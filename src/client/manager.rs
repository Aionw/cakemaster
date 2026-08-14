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
use crate::segment::{SegmentId, SegmentPool, SegmentSpec};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientCleanupReport {
    pub completed_sessions: usize,
    pub revoked_pending_writes: usize,
    pub invalidated_segments: usize,
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
}

struct ClientSlot {
    mount_state: Mutex<ClientMountState>,
}

#[derive(Default)]
struct ClientMountState {
    session: Option<ClientSession>,
    segments: BTreeSet<SegmentId>,
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
            }),
        }
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
