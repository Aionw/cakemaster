//! Server-side coordination for client remount, heartbeat, and expiry fencing.

use crate::MasterClock;
use cakemaster::client::{
    ActivateOutcome, CleanupReason, ClientCleanup, ClientId, ClientLifecycleError, ClientRegistry,
    ClientSession, ClientState, HeartbeatOutcome,
};
use cakemaster::segment::error::{AttachError, SegmentStateError};
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{SegmentId, SegmentPool, SegmentSpec};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

pub const DEFAULT_MASTER_VIEW_VERSION: i64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientPing {
    view_version: i64,
    heartbeat: HeartbeatOutcome,
}

impl ClientPing {
    pub const fn view_version(self) -> i64 {
        self.view_version
    }

    pub const fn heartbeat(self) -> HeartbeatOutcome {
        self.heartbeat
    }
}

#[derive(Debug, Error)]
pub enum ClientRuntimeError {
    #[error(transparent)]
    Lifecycle(#[from] ClientLifecycleError),
    #[error(transparent)]
    Attach(#[from] AttachError),
    #[error(transparent)]
    SegmentState(#[from] SegmentStateError),
    #[error("active client remount does not match its mounted segment set")]
    ActiveRemountConflict,
    #[error("client runtime slot is inconsistent with the active registry session")]
    InconsistentSlot,
    #[error("failed to roll back remounted segment {segment}")]
    Rollback {
        segment: SegmentId,
        #[source]
        source: SegmentStateError,
    },
}

/// Async coordination around the synchronous [`ClientRegistry`].
///
/// Only remount takes a per-client async lock. Ping remains a short registry
/// update and never allocates a runtime slot for an unknown client.
#[derive(Clone)]
pub struct ClientRuntime {
    inner: Arc<ClientRuntimeInner>,
}

struct ClientRuntimeInner {
    registry: Arc<ClientRegistry>,
    pool: Arc<SegmentPool>,
    clock: MasterClock,
    view_version: i64,
    slots: Mutex<HashMap<ClientId, Arc<ClientSlot>>>,
}

struct ClientSlot {
    lifecycle: Mutex<ClientSlotState>,
}

#[derive(Default)]
struct ClientSlotState {
    session: Option<ClientSession>,
    segments: BTreeSet<SegmentId>,
}

impl ClientRuntime {
    pub fn new(pool: Arc<SegmentPool>, clock: MasterClock) -> Self {
        Self::with_registry(
            pool,
            Arc::new(ClientRegistry::new()),
            clock,
            DEFAULT_MASTER_VIEW_VERSION,
        )
    }

    pub fn with_registry(
        pool: Arc<SegmentPool>,
        registry: Arc<ClientRegistry>,
        clock: MasterClock,
        view_version: i64,
    ) -> Self {
        Self {
            inner: Arc::new(ClientRuntimeInner {
                registry,
                pool,
                clock,
                view_version,
                slots: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn registry(&self) -> &Arc<ClientRegistry> {
        &self.inner.registry
    }

    pub fn pool(&self) -> &Arc<SegmentPool> {
        &self.inner.pool
    }

    pub fn clock(&self) -> &MasterClock {
        &self.inner.clock
    }

    pub fn view_version(&self) -> i64 {
        self.inner.view_version
    }

    pub async fn slot_count(&self) -> usize {
        self.inner.slots.lock().await.len()
    }

    pub fn ping(&self, client_id: ClientId) -> ClientPing {
        ClientPing {
            view_version: self.inner.view_version,
            heartbeat: self
                .inner
                .registry
                .heartbeat(client_id, self.inner.clock.client_now()),
        }
    }

    /// Fences due sessions and returns cleanup work without performing any
    /// resource-manager calls under the registry lock.
    pub fn maintenance(&self) -> Vec<ClientCleanup> {
        let config = self.inner.registry.config();
        self.inner
            .registry
            .maintenance(self.inner.clock.client_now(), config.maintenance_budget())
    }

    /// Atomically establishes the resource/session relationship for one
    /// client, rolling back only resources changed by this remount attempt.
    pub async fn remount(
        &self,
        client_id: ClientId,
        segments: Vec<SegmentSpec>,
    ) -> Result<ActivateOutcome, ClientRuntimeError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId.into());
        }
        let slot = self.slot(client_id).await;
        let result = {
            let mut state = slot.lifecycle.lock().await;
            self.remount_locked(client_id, segments, &mut state)
        };
        if result.is_err() {
            self.prune_unused_slot(client_id, &slot).await;
        }
        result
    }

    async fn slot(&self, client_id: ClientId) -> Arc<ClientSlot> {
        let mut slots = self.inner.slots.lock().await;
        slots
            .entry(client_id)
            .or_insert_with(|| {
                Arc::new(ClientSlot {
                    lifecycle: Mutex::new(ClientSlotState::default()),
                })
            })
            .clone()
    }

    async fn prune_unused_slot(&self, client_id: ClientId, slot: &Arc<ClientSlot>) {
        if slot.lifecycle.lock().await.session.is_some() {
            return;
        }
        let mut slots = self.inner.slots.lock().await;
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
        state: &mut ClientSlotState,
    ) -> Result<ActivateOutcome, ClientRuntimeError> {
        match self.inner.registry.state(client_id) {
            Some(ClientState::Active) => {
                return self.remount_active(client_id, &segments, state);
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

        let outcome = match self
            .inner
            .registry
            .activate(client_id, self.inner.clock.client_now())
        {
            Ok(ActivateOutcome::Activated(session)) => ActivateOutcome::Activated(session),
            Ok(ActivateOutcome::AlreadyActive(_)) => {
                self.rollback(client_id, &attached, &[])?;
                return Err(ClientRuntimeError::InconsistentSlot);
            }
            Err(error) => {
                self.rollback(client_id, &attached, &[])?;
                return Err(error.into());
            }
        };

        let mut to_reactivate = attached.clone();
        to_reactivate.extend_from_slice(&quiesced_existing);
        if let Err(error) = self.inner.pool.reactivate_many(client_id, &to_reactivate) {
            let session = outcome.session();
            self.inner
                .registry
                .begin_drain(session, CleanupReason::ServerShutdown)?;
            self.rollback(client_id, &attached, &quiesced_existing)?;
            self.inner.registry.finish_cleanup(session)?;
            return Err(error.into());
        }

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
        state: &ClientSlotState,
    ) -> Result<ActivateOutcome, ClientRuntimeError> {
        let session = self.inner.registry.active_session(client_id)?;
        if state.session != Some(session) {
            return Err(ClientRuntimeError::InconsistentSlot);
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
            return Err(ClientRuntimeError::ActiveRemountConflict);
        }
        Ok(self
            .inner
            .registry
            .activate(client_id, self.inner.clock.client_now())?)
    }

    fn rollback(
        &self,
        client_id: ClientId,
        attached: &[SegmentId],
        reactivated: &[SegmentId],
    ) -> Result<(), ClientRuntimeError> {
        let mut first_error = None;
        for &segment in reactivated.iter().rev() {
            if let Err(source) = self.inner.pool.quiesce(client_id, segment) {
                first_error.get_or_insert(ClientRuntimeError::Rollback { segment, source });
            }
        }
        for &segment in attached.iter().rev() {
            if let Err(source) = self.inner.pool.quiesce(client_id, segment) {
                first_error.get_or_insert(ClientRuntimeError::Rollback { segment, source });
                continue;
            }
            if let Err(source) = self.inner.pool.remove(client_id, segment) {
                first_error.get_or_insert(ClientRuntimeError::Rollback { segment, source });
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}
