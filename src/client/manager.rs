//! Cross-domain coordination for client remount and resource cleanup.

use super::{
    ActivateOutcome, CleanupReason, ClientCleanup, ClientId, ClientLifecycleConfig,
    ClientLifecycleConfigError, ClientLifecycleError, ClientRegistry, ClientSession, ClientState,
    ClientTick, HeartbeatOutcome,
};
use crate::object::reclamation::CatalogTick;
use crate::object::{PendingWriteRevoker, WriteAdmission, WriteOwner};
use crate::segment::error::{AttachError, SegmentStateError};
use crate::segment::stats::SegmentState;
use crate::segment::{SegmentId, SegmentIncarnation, SegmentPool, SegmentSpec};
use parking_lot::Mutex;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::sync::Arc;
use thiserror::Error;

const GRACEFUL_UNMOUNT_RETRY_TICKS: u64 = 100;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientCleanupReport {
    pub completed_sessions: usize,
    pub revoked_pending_writes: usize,
    pub invalidated_segments: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GracefulUnmountReport {
    pub completed: usize,
    pub stale_or_cancelled: usize,
    pub retried: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentUnmountOutcome {
    Unmounted,
    AlreadyAbsent,
}

#[derive(Debug, Error)]
pub enum ClientManagerError {
    #[error(transparent)]
    Lifecycle(#[from] ClientLifecycleError),
    #[error(transparent)]
    Attach(#[from] AttachError),
    #[error(transparent)]
    SegmentState(#[from] SegmentStateError),
    #[error("active client remount does not match its mounted segment set")]
    ActiveRemountConflict,
    #[error("client manager slot is inconsistent with the active registry session")]
    InconsistentSlot,
    #[error("segment {0} is unavailable in its current lifecycle state")]
    SegmentUnavailable(SegmentId),
    #[error("failed to roll back remounted segment {segment}")]
    Rollback {
        segment: SegmentId,
        #[source]
        source: SegmentStateError,
    },
}

/// Resource coordination around the synchronous [`ClientRegistry`].
///
/// Resource-changing operations take a per-client lock. Heartbeat remains a
/// short registry update and never allocates a manager slot for an unknown
/// client.
#[derive(Clone)]
pub struct ClientManager {
    inner: Arc<ClientManagerInner>,
}

struct ClientManagerInner {
    registry: ClientRegistry,
    pool: Arc<SegmentPool>,
    pending_write_revoker: PendingWriteRevoker,
    slots: Mutex<HashMap<ClientId, Arc<ClientSlot>>>,
    graceful_unmounts: Mutex<GracefulUnmountQueue>,
}

struct ClientSlot {
    mount_state: Mutex<ClientMountState>,
}

#[derive(Default)]
struct ClientMountState {
    session: Option<ClientSession>,
    segments: BTreeSet<SegmentId>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct GracefulUnmountKey {
    session: ClientSession,
    segment: SegmentId,
}

#[derive(Clone)]
struct PendingGracefulUnmount {
    deadline: ClientTick,
    incarnation: SegmentIncarnation,
    revision: u64,
    claimed: bool,
}

struct GracefulUnmountJob {
    key: GracefulUnmountKey,
    incarnation: SegmentIncarnation,
    revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GracefulDeadlineRecord {
    deadline: ClientTick,
    revision: u64,
    key: GracefulUnmountKey,
}

impl Ord for GracefulDeadlineRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.revision.cmp(&other.revision))
            .then_with(|| {
                self.key
                    .session
                    .client_id()
                    .cmp(&other.key.session.client_id())
            })
            .then_with(|| {
                self.key
                    .session
                    .generation()
                    .cmp(&other.key.session.generation())
            })
            .then_with(|| self.key.segment.cmp(&other.key.segment))
    }
}

impl PartialOrd for GracefulDeadlineRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct GracefulUnmountQueue {
    deadlines: BinaryHeap<Reverse<GracefulDeadlineRecord>>,
    pending: HashMap<GracefulUnmountKey, PendingGracefulUnmount>,
    next_revision: u64,
    cancelled_since_report: usize,
}

impl GracefulUnmountQueue {
    fn schedule(
        &mut self,
        key: GracefulUnmountKey,
        incarnation: SegmentIncarnation,
        deadline: ClientTick,
    ) {
        if let Some(pending) = self.pending.get(&key)
            && pending.incarnation.same_as(&incarnation)
            && (pending.claimed || pending.deadline <= deadline)
        {
            return;
        }
        self.insert(key, incarnation, deadline);
    }

    fn cancel(&mut self, key: GracefulUnmountKey) {
        if self
            .pending
            .remove(&key)
            .is_some_and(|pending| !pending.claimed)
        {
            self.cancelled_since_report = self.cancelled_since_report.saturating_add(1);
        }
    }

    fn cancel_session(&mut self, session: ClientSession) {
        let cancelled = self
            .pending
            .iter()
            .filter(|(key, pending)| key.session == session && !pending.claimed)
            .count();
        self.pending.retain(|key, _| key.session != session);
        self.cancelled_since_report = self.cancelled_since_report.saturating_add(cancelled);
    }

    fn contains(&self, key: GracefulUnmountKey, incarnation: &SegmentIncarnation) -> bool {
        self.pending
            .get(&key)
            .is_some_and(|pending| pending.incarnation.same_as(incarnation))
    }

    fn next_deadline(&mut self) -> Option<ClientTick> {
        self.prune_stale_deadlines();
        self.deadlines.peek().map(|record| record.0.deadline)
    }

    fn claim_due(&mut self, now: ClientTick, budget: usize) -> Vec<GracefulUnmountJob> {
        let mut jobs = Vec::new();
        while jobs.len() < budget {
            self.prune_stale_deadlines();
            let Some(record) = self.deadlines.peek().map(|record| record.0) else {
                break;
            };
            if record.deadline > now {
                break;
            }
            self.deadlines.pop();
            let Some(pending) = self.pending.get_mut(&record.key) else {
                continue;
            };
            if pending.revision != record.revision
                || pending.deadline != record.deadline
                || pending.claimed
            {
                continue;
            }
            pending.claimed = true;
            jobs.push(GracefulUnmountJob {
                key: record.key,
                incarnation: pending.incarnation.clone(),
                revision: record.revision,
            });
        }
        jobs
    }

    fn take_cancelled(&mut self) -> usize {
        std::mem::take(&mut self.cancelled_since_report)
    }

    fn finish(&mut self, job: &GracefulUnmountJob) {
        if self
            .pending
            .get(&job.key)
            .is_some_and(|pending| pending.revision == job.revision)
        {
            self.pending.remove(&job.key);
        }
    }

    fn retry(&mut self, job: GracefulUnmountJob, deadline: ClientTick) -> bool {
        if self
            .pending
            .get(&job.key)
            .is_none_or(|pending| pending.revision != job.revision)
        {
            return false;
        }
        self.insert(job.key, job.incarnation, deadline);
        true
    }

    fn insert(
        &mut self,
        key: GracefulUnmountKey,
        incarnation: SegmentIncarnation,
        deadline: ClientTick,
    ) {
        self.next_revision = self.next_revision.wrapping_add(1).max(1);
        let revision = self.next_revision;
        self.pending.insert(
            key,
            PendingGracefulUnmount {
                deadline,
                incarnation,
                revision,
                claimed: false,
            },
        );
        self.deadlines.push(Reverse(GracefulDeadlineRecord {
            deadline,
            revision,
            key,
        }));
    }

    fn prune_stale_deadlines(&mut self) {
        while let Some(record) = self.deadlines.peek().map(|record| record.0) {
            let current = self.pending.get(&record.key);
            if current.is_some_and(|pending| {
                pending.revision == record.revision && pending.deadline == record.deadline
            }) {
                break;
            }
            self.deadlines.pop();
        }
    }
}

impl ClientManager {
    pub fn new(pool: Arc<SegmentPool>, pending_write_revoker: PendingWriteRevoker) -> Self {
        Self::from_registry(pool, pending_write_revoker, ClientRegistry::new())
    }

    pub fn with_config(
        pool: Arc<SegmentPool>,
        pending_write_revoker: PendingWriteRevoker,
        config: ClientLifecycleConfig,
    ) -> Result<Self, ClientLifecycleConfigError> {
        Ok(Self::from_registry(
            pool,
            pending_write_revoker,
            ClientRegistry::with_config(config)?,
        ))
    }

    fn from_registry(
        pool: Arc<SegmentPool>,
        pending_write_revoker: PendingWriteRevoker,
        registry: ClientRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(ClientManagerInner {
                registry,
                pool,
                pending_write_revoker,
                slots: Mutex::new(HashMap::new()),
                graceful_unmounts: Mutex::new(GracefulUnmountQueue::default()),
            }),
        }
    }

    /// Returns whether two handles drive the same client and graceful-work state.
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn heartbeat(&self, client_id: ClientId, now: ClientTick) -> HeartbeatOutcome {
        self.inner.registry.heartbeat(client_id, now)
    }

    /// Resolves the exact active session identity used to finish or revoke a
    /// previously admitted object write.
    pub fn write_owner(&self, client_id: ClientId) -> Result<WriteOwner, ClientLifecycleError> {
        self.inner
            .registry
            .active_session(client_id)
            .map(WriteOwner::for_session)
    }

    /// Creates a generation-bound admission fence for a new object write.
    pub fn write_admission(
        &self,
        client_id: ClientId,
    ) -> Result<WriteAdmission, ClientLifecycleError> {
        self.inner
            .registry
            .active_session_guard(client_id)
            .map(WriteAdmission::guarded)
    }

    /// Fences the requested sessions and performs one batched logical cleanup.
    pub fn drain_sessions(
        &self,
        sessions: impl IntoIterator<Item = ClientSession>,
        reason: CleanupReason,
        now: CatalogTick,
    ) -> Result<ClientCleanupReport, ClientManagerError> {
        let mut cleanups = Vec::new();
        for session in sessions {
            if let Some(cleanup) = self.inner.registry.begin_drain(session, reason)? {
                cleanups.push(cleanup);
            }
        }
        self.cleanup_batch(cleanups, now)
    }

    /// Claims one bounded batch of due sessions and completes their logical
    /// resource cleanup.
    pub fn run_cleanup_step(
        &self,
        client_now: ClientTick,
        catalog_now: CatalogTick,
    ) -> Result<ClientCleanupReport, ClientManagerError> {
        let config = self.inner.registry.config();
        let cleanups = self
            .inner
            .registry
            .claim_due_cleanups(client_now, config.cleanup_scan_budget());
        self.cleanup_batch(cleanups, catalog_now)
    }

    /// Mounts one segment for an active client, or atomically establishes the
    /// first client session when the client is absent.
    pub fn mount_segment(
        &self,
        client_id: ClientId,
        spec: SegmentSpec,
        now: ClientTick,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId.into());
        }
        let segment_id = spec.identity().id();
        if spec.identity().owner() != client_id {
            return Err(SegmentStateError::OwnerMismatch {
                segment: segment_id,
                expected: spec.identity().owner(),
                actual: client_id,
            }
            .into());
        }

        let slot = self.slot(client_id);
        let result = {
            let mut state = slot.mount_state.lock();
            match self.inner.registry.state(client_id) {
                None => self.mount_segment_absent(client_id, spec, now, &mut state),
                Some(ClientState::Active) => {
                    self.mount_segment_active(client_id, spec, now, &mut state)
                }
                Some(state) => Err(ClientLifecycleError::CleanupInProgress { state }.into()),
            }
        };
        if result.is_err() {
            self.prune_unused_slot(client_id, &slot);
        }
        result
    }

    /// Immediately quiesces and removes one mounted segment. A missing segment
    /// is an idempotent success and does not require a live client session.
    pub fn unmount_segment(
        &self,
        client_id: ClientId,
        segment_id: SegmentId,
    ) -> Result<SegmentUnmountOutcome, ClientManagerError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId.into());
        }
        if segment_id.is_nil() {
            return Err(SegmentStateError::NotFound(segment_id).into());
        }
        let Some(segment) = self.inner.pool.segment(segment_id) else {
            return Ok(SegmentUnmountOutcome::AlreadyAbsent);
        };
        let expected_owner = segment.spec().identity().owner();
        if expected_owner != client_id {
            return Err(SegmentStateError::OwnerMismatch {
                segment: segment_id,
                expected: expected_owner,
                actual: client_id,
            }
            .into());
        }

        let session = self.inner.registry.active_session(client_id)?;
        let slot = self
            .existing_slot(client_id)
            .ok_or(ClientManagerError::InconsistentSlot)?;
        let mut state = slot.mount_state.lock();
        if state.session != Some(session) || !state.segments.contains(&segment_id) {
            return Err(ClientManagerError::InconsistentSlot);
        }

        self.inner.pool.quiesce(client_id, segment_id)?;
        self.inner.pool.remove(client_id, segment_id)?;
        state.segments.remove(&segment_id);
        self.inner
            .graceful_unmounts
            .lock()
            .cancel(GracefulUnmountKey {
                session,
                segment: segment_id,
            });
        Ok(SegmentUnmountOutcome::Unmounted)
    }

    /// Stops new placement immediately and defers logical removal until the
    /// earliest requested deadline for this session/segment incarnation.
    pub fn schedule_graceful_unmount(
        &self,
        client_id: ClientId,
        segment_id: SegmentId,
        deadline: ClientTick,
    ) -> Result<(), ClientManagerError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId.into());
        }
        if segment_id.is_nil() {
            return Err(SegmentStateError::NotFound(segment_id).into());
        }
        let segment = self
            .inner
            .pool
            .segment(segment_id)
            .ok_or(SegmentStateError::NotFound(segment_id))?;
        let expected_owner = segment.spec().identity().owner();
        if expected_owner != client_id {
            return Err(SegmentStateError::OwnerMismatch {
                segment: segment_id,
                expected: expected_owner,
                actual: client_id,
            }
            .into());
        }
        let session = self.inner.registry.active_session(client_id)?;
        let slot = self
            .existing_slot(client_id)
            .ok_or(ClientManagerError::InconsistentSlot)?;
        let state = slot.mount_state.lock();
        if state.session != Some(session) || !state.segments.contains(&segment_id) {
            return Err(SegmentStateError::NotFound(segment_id).into());
        }
        let incarnation = segment.incarnation();
        let key = GracefulUnmountKey {
            session,
            segment: segment_id,
        };
        let mut queue = self.inner.graceful_unmounts.lock();
        match segment.stats().state {
            SegmentState::Accepting => self.inner.pool.quiesce(client_id, segment_id)?,
            SegmentState::Quiesced if queue.contains(key, &incarnation) => {}
            SegmentState::Quiesced | SegmentState::Removed => {
                return Err(ClientManagerError::SegmentUnavailable(segment_id));
            }
        }
        queue.schedule(key, incarnation, deadline);
        Ok(())
    }

    /// Executes one bounded batch of due graceful removals. Unexpected segment
    /// transition failures stay scheduled for a short retry.
    pub fn run_graceful_unmount_step(&self, now: ClientTick) -> GracefulUnmountReport {
        let budget = self.inner.registry.config().cleanup_scan_budget();
        let (jobs, cancelled) = {
            let mut queue = self.inner.graceful_unmounts.lock();
            let jobs = queue.claim_due(now, budget);
            (jobs, queue.take_cancelled())
        };
        let mut report = GracefulUnmountReport {
            stale_or_cancelled: cancelled,
            ..GracefulUnmountReport::default()
        };
        for job in jobs {
            match self.complete_graceful_unmount(&job) {
                Ok(true) => {
                    self.inner.graceful_unmounts.lock().finish(&job);
                    report.completed += 1;
                }
                Ok(false) => {
                    self.inner.graceful_unmounts.lock().finish(&job);
                    report.stale_or_cancelled += 1;
                }
                Err(error) => {
                    log::warn!(
                        target: "cakemaster::client::manager",
                        client_id:% = job.key.session.client_id(),
                        generation = job.key.session.generation(),
                        segment_id:% = job.key.segment,
                        error:% = error;
                        "graceful segment unmount failed and will be retried"
                    );
                    if self
                        .inner
                        .graceful_unmounts
                        .lock()
                        .retry(job, now.saturating_add(GRACEFUL_UNMOUNT_RETRY_TICKS))
                    {
                        report.retried += 1;
                    } else {
                        report.stale_or_cancelled += 1;
                    }
                }
            }
        }
        report
    }

    pub fn next_graceful_unmount_deadline(&self) -> Option<ClientTick> {
        self.inner.graceful_unmounts.lock().next_deadline()
    }

    /// Executes generation-fenced cleanup work with one batched segment-pool
    /// invalidation. Physical allocations remain owned by outstanding handles.
    fn cleanup_batch(
        &self,
        cleanups: impl IntoIterator<Item = ClientCleanup>,
        now: CatalogTick,
    ) -> Result<ClientCleanupReport, ClientManagerError> {
        let cleanups: Vec<_> = cleanups.into_iter().collect();
        let mut report = ClientCleanupReport::default();
        let mut ready = Vec::with_capacity(cleanups.len());

        for cleanup in cleanups {
            let session = cleanup.session();
            self.inner.graceful_unmounts.lock().cancel_session(session);
            let slot = self.existing_slot(session.client_id());
            if let Some(slot) = &slot {
                let mut state = slot.mount_state.lock();
                match state.session {
                    Some(current) if current == session => {
                        state.session = None;
                        state.segments.clear();
                    }
                    None => {}
                    Some(_) => return Err(ClientManagerError::InconsistentSlot),
                }
            }
            ready.push((cleanup, slot));
        }

        report.revoked_pending_writes = self
            .inner
            .pending_write_revoker
            .revoke_sessions(ready.iter().map(|(cleanup, _)| cleanup.session()), now);

        report.invalidated_segments = self.inner.pool.invalidate_owners(
            ready
                .iter()
                .map(|(cleanup, _)| cleanup.session().client_id()),
        );

        // Keep every claim unfinished until slot pruning completes. Any early
        // error drops the claims and requeues the logical cleanup for retry.
        for (cleanup, slot) in &ready {
            if let Some(slot) = slot {
                self.prune_unused_slot(cleanup.session().client_id(), slot);
            }
        }
        for (cleanup, _) in ready {
            cleanup.finish()?;
            report.completed_sessions += 1;
        }
        Ok(report)
    }

    /// Atomically establishes the resource/session relationship for one
    /// client, rolling back only resources changed by this remount attempt.
    pub fn remount(
        &self,
        client_id: ClientId,
        segments: Vec<SegmentSpec>,
        now: ClientTick,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId.into());
        }
        let slot = self.slot(client_id);
        let result = {
            let mut state = slot.mount_state.lock();
            self.remount_locked(client_id, segments, now, &mut state)
        };
        if result.is_err() {
            self.prune_unused_slot(client_id, &slot);
        }
        result
    }

    fn slot(&self, client_id: ClientId) -> Arc<ClientSlot> {
        let mut slots = self.inner.slots.lock();
        slots
            .entry(client_id)
            .or_insert_with(|| {
                Arc::new(ClientSlot {
                    mount_state: Mutex::new(ClientMountState::default()),
                })
            })
            .clone()
    }

    fn existing_slot(&self, client_id: ClientId) -> Option<Arc<ClientSlot>> {
        self.inner.slots.lock().get(&client_id).cloned()
    }

    fn prune_unused_slot(&self, client_id: ClientId, slot: &Arc<ClientSlot>) {
        if slot.mount_state.lock().session.is_some() {
            return;
        }
        let mut slots = self.inner.slots.lock();
        if Arc::strong_count(slot) == 2
            && slots
                .get(&client_id)
                .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            slots.remove(&client_id);
        }
    }

    fn remount_locked(
        &self,
        client_id: ClientId,
        segments: Vec<SegmentSpec>,
        now: ClientTick,
        state: &mut ClientMountState,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        match self.inner.registry.state(client_id) {
            Some(ClientState::Active) => {
                return self.remount_active(client_id, &segments, now, state);
            }
            Some(state) => {
                return Err(ClientLifecycleError::CleanupInProgress { state }.into());
            }
            None => {}
        }

        let mut attached = Vec::new();
        let mut quiesced_existing = Vec::new();
        for spec in &segments {
            let outcome = match self.inner.pool.attach_quiesced(spec.clone()) {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.rollback(client_id, &attached, &[])?;
                    return Err(error.into());
                }
            };
            let segment = outcome.segment();
            if outcome.is_new() {
                attached.push(segment.id());
            } else if segment.stats().state == SegmentState::Quiesced {
                quiesced_existing.push(segment.id());
            }
        }

        let mut to_reactivate = attached.clone();
        to_reactivate.extend_from_slice(&quiesced_existing);
        if let Err(error) = self.inner.pool.reactivate_many(client_id, &to_reactivate) {
            self.rollback(client_id, &attached, &quiesced_existing)?;
            return Err(error.into());
        }

        // Make the registry session visible only after all mounted resources
        // are accepting. From this point onward the transaction cannot fail,
        // so write admission never observes a half-mounted active session.
        let outcome = match self.inner.registry.activate(client_id, now) {
            Ok(ActivateOutcome::Activated(session)) => ActivateOutcome::Activated(session),
            Ok(ActivateOutcome::AlreadyActive(_)) => {
                self.rollback(client_id, &attached, &quiesced_existing)?;
                return Err(ClientManagerError::InconsistentSlot);
            }
            Err(error) => {
                self.rollback(client_id, &attached, &quiesced_existing)?;
                return Err(error.into());
            }
        };

        state.session = Some(outcome.session());
        state.segments = segments
            .iter()
            .map(|segment| segment.identity().id())
            .collect();
        Ok(outcome)
    }

    fn remount_active(
        &self,
        client_id: ClientId,
        segments: &[SegmentSpec],
        now: ClientTick,
        state: &ClientMountState,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        let session = self.inner.registry.active_session(client_id)?;
        if state.session != Some(session) {
            return Err(ClientManagerError::InconsistentSlot);
        }
        let requested: BTreeSet<_> = segments
            .iter()
            .map(|segment| segment.identity().id())
            .collect();
        if requested != state.segments
            || segments.iter().any(|spec| {
                self.inner
                    .pool
                    .segment(spec.identity().id())
                    .is_none_or(|segment| segment.spec() != spec)
            })
        {
            return Err(ClientManagerError::ActiveRemountConflict);
        }
        Ok(self.inner.registry.activate(client_id, now)?)
    }

    fn mount_segment_active(
        &self,
        client_id: ClientId,
        spec: SegmentSpec,
        now: ClientTick,
        state: &mut ClientMountState,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        let session = self.inner.registry.active_session(client_id)?;
        if state.session != Some(session) {
            return Err(ClientManagerError::InconsistentSlot);
        }
        let segment_id = spec.identity().id();
        if state.segments.contains(&segment_id) {
            let segment = self
                .inner
                .pool
                .segment(segment_id)
                .ok_or(ClientManagerError::InconsistentSlot)?;
            if segment.spec() != &spec {
                return Err(ClientManagerError::ActiveRemountConflict);
            }
            if segment.stats().state != SegmentState::Accepting {
                return Err(ClientManagerError::SegmentUnavailable(segment_id));
            }
            return Ok(self.inner.registry.activate(client_id, now)?);
        }

        let outcome = self.inner.pool.attach_quiesced(spec)?;
        if !outcome.is_new() {
            return Err(ClientManagerError::InconsistentSlot);
        }
        if let Err(source) = self.inner.pool.reactivate(client_id, segment_id) {
            self.rollback(client_id, &[segment_id], &[])?;
            return Err(source.into());
        }
        let activation = match self.inner.registry.activate(client_id, now) {
            Ok(activation @ ActivateOutcome::AlreadyActive(current)) if current == session => {
                activation
            }
            Ok(_) => {
                self.rollback(client_id, &[segment_id], &[])?;
                return Err(ClientManagerError::InconsistentSlot);
            }
            Err(error) => {
                self.rollback(client_id, &[segment_id], &[])?;
                return Err(error.into());
            }
        };
        state.segments.insert(segment_id);
        Ok(activation)
    }

    fn mount_segment_absent(
        &self,
        client_id: ClientId,
        spec: SegmentSpec,
        now: ClientTick,
        state: &mut ClientMountState,
    ) -> Result<ActivateOutcome, ClientManagerError> {
        let outcome = self.inner.pool.attach_quiesced(spec)?;
        let segment_id = outcome.segment().id();
        let attached = outcome.is_new().then_some(segment_id);
        let activation = match self.inner.registry.activate(client_id, now) {
            Ok(activation @ ActivateOutcome::Activated(_)) => activation,
            Ok(ActivateOutcome::AlreadyActive(_)) => {
                self.rollback(client_id, attached.as_slice(), &[])?;
                return Err(ClientManagerError::InconsistentSlot);
            }
            Err(error) => {
                self.rollback(client_id, attached.as_slice(), &[])?;
                return Err(error.into());
            }
        };
        if let Err(source) = self.inner.pool.reactivate(client_id, segment_id) {
            let registry_rollback = self
                .inner
                .registry
                .rollback_activation(activation.session());
            self.rollback(client_id, attached.as_slice(), &[])?;
            registry_rollback?;
            return Err(source.into());
        }

        state.session = Some(activation.session());
        state.segments.insert(segment_id);
        Ok(activation)
    }

    fn complete_graceful_unmount(
        &self,
        job: &GracefulUnmountJob,
    ) -> Result<bool, SegmentStateError> {
        let client_id = job.key.session.client_id();
        let Some(slot) = self.existing_slot(client_id) else {
            return Ok(false);
        };
        let mut state = slot.mount_state.lock();
        if state.session != Some(job.key.session) || !state.segments.contains(&job.key.segment) {
            return Ok(false);
        }
        let Some(segment) = self.inner.pool.segment(job.key.segment) else {
            return Ok(false);
        };
        if !job.incarnation.matches(&segment) {
            return Ok(false);
        }
        match segment.stats().state {
            SegmentState::Accepting => {
                self.inner.pool.quiesce(client_id, job.key.segment)?;
                return Err(SegmentStateError::StillAccepting(job.key.segment));
            }
            SegmentState::Quiesced => {}
            SegmentState::Removed => return Ok(false),
        }
        self.inner.pool.remove(client_id, job.key.segment)?;
        state.segments.remove(&job.key.segment);
        Ok(true)
    }

    fn rollback(
        &self,
        client_id: ClientId,
        attached: &[SegmentId],
        reactivated: &[SegmentId],
    ) -> Result<(), ClientManagerError> {
        let mut first_error = None;
        for &segment in reactivated.iter().rev() {
            if let Err(source) = self.inner.pool.quiesce(client_id, segment) {
                first_error.get_or_insert(ClientManagerError::Rollback { segment, source });
            }
        }
        for &segment in attached.iter().rev() {
            if let Err(source) = self.inner.pool.quiesce(client_id, segment) {
                first_error.get_or_insert(ClientManagerError::Rollback { segment, source });
                continue;
            }
            if let Err(source) = self.inner.pool.remove(client_id, segment) {
                first_error.get_or_insert(ClientManagerError::Rollback { segment, source });
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectManager;
    use crate::segment::{MemoryRegion, SegmentIdentity, TransportEndpoint, TransportProtocol};

    const CLIENT: ClientId = ClientId::new(91, 92);
    const SEGMENT: SegmentId = SegmentId::new(93, 94);

    fn segment() -> SegmentSpec {
        SegmentSpec::memory(
            SegmentIdentity::new(SEGMENT, CLIENT, "incarnation-fence"),
            MemoryRegion::new(0x6_0000_0000, 4096),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        )
    }

    #[test]
    fn claimed_old_job_cannot_remove_same_session_reuse() {
        let pool = Arc::new(SegmentPool::new());
        let objects = ObjectManager::new(pool.clone());
        let clients = ClientManager::new(pool.clone(), objects.pending_write_revoker());
        clients
            .mount_segment(CLIENT, segment(), ClientTick::ZERO)
            .unwrap();
        clients
            .schedule_graceful_unmount(CLIENT, SEGMENT, ClientTick::new(10))
            .unwrap();
        let job = clients
            .inner
            .graceful_unmounts
            .lock()
            .claim_due(ClientTick::new(10), 1)
            .pop()
            .unwrap();

        clients.unmount_segment(CLIENT, SEGMENT).unwrap();
        clients
            .mount_segment(CLIENT, segment(), ClientTick::new(1))
            .unwrap();
        assert!(!clients.complete_graceful_unmount(&job).unwrap());
        assert_eq!(
            pool.segment(SEGMENT).unwrap().stats().state,
            SegmentState::Accepting
        );
    }

    #[test]
    fn claimed_old_job_cannot_cross_a_new_client_generation() {
        let pool = Arc::new(SegmentPool::new());
        let objects = ObjectManager::new(pool.clone());
        let clients = ClientManager::with_config(
            pool.clone(),
            objects.pending_write_revoker(),
            ClientLifecycleConfig::new(4)
                .with_ttl(5)
                .with_cleanup_scan_budget(4),
        )
        .unwrap();
        clients
            .mount_segment(CLIENT, segment(), ClientTick::ZERO)
            .unwrap();
        clients
            .schedule_graceful_unmount(CLIENT, SEGMENT, ClientTick::new(10))
            .unwrap();
        let job = clients
            .inner
            .graceful_unmounts
            .lock()
            .claim_due(ClientTick::new(10), 1)
            .pop()
            .unwrap();

        clients
            .run_cleanup_step(ClientTick::new(10), CatalogTick::new(10))
            .unwrap();
        clients
            .mount_segment(CLIENT, segment(), ClientTick::new(10))
            .unwrap();
        assert!(!clients.complete_graceful_unmount(&job).unwrap());
        assert_eq!(
            pool.segment(SEGMENT).unwrap().stats().state,
            SegmentState::Accepting
        );
    }
}
