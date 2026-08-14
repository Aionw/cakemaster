use super::ClientId;
use super::config::ClientLifecycleConfig;
use super::error::{ClientLifecycleConfigError, ClientLifecycleError};
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientTick(u64);

impl ClientTick {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn saturating_add(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }
}

/// One server-side incarnation of a client.
///
/// Cleanup and future mailbox/task ownership use the complete session rather
/// than a bare [`ClientId`], so work from an older incarnation cannot affect a
/// newly activated client with the same ID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientSession {
    client_id: ClientId,
    generation: u64,
}

impl ClientSession {
    pub const fn client_id(self) -> ClientId {
        self.client_id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Cloneable fencing token for operations owned by one client incarnation.
/// The registry invalidates it before emitting cleanup work.
#[derive(Clone)]
pub(crate) struct ClientSessionGuard {
    inner: Arc<ClientSessionGuardInner>,
}

struct ClientSessionGuardInner {
    session: ClientSession,
    active: AtomicBool,
}

impl ClientSessionGuard {
    pub(crate) fn session(&self) -> ClientSession {
        self.inner.session
    }

    pub(crate) fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::Acquire)
    }

    fn invalidate(&self) {
        self.inner.active.store(false, Ordering::Release);
    }
}

impl fmt::Debug for ClientSessionGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSessionGuard")
            .field("session", &self.session())
            .field("active", &self.is_active())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ClientState {
    Active,
    Draining,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivateOutcome {
    Activated(ClientSession),
    AlreadyActive(ClientSession),
}

impl ActivateOutcome {
    pub const fn session(self) -> ClientSession {
        match self {
            Self::Activated(session) | Self::AlreadyActive(session) => session,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatOutcome {
    Alive(ClientSession),
    NeedRemount,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CleanupReason {
    GracefulUnmount,
    HeartbeatExpired,
    ServerShutdown,
}

/// Exclusive ownership of one fenced session's cleanup.
///
/// Dropping an unfinished claim makes it available to the next maintenance
/// round. This keeps cancellation and transient runtime failures from losing
/// cleanup work while ensuring only one worker can clean a session at a time.
pub struct ClientCleanup {
    registry: Option<Arc<ClientRegistryInner>>,
    session: ClientSession,
    reason: CleanupReason,
}

impl ClientCleanup {
    pub const fn session(&self) -> ClientSession {
        self.session
    }

    pub const fn reason(&self) -> CleanupReason {
        self.reason
    }

    /// Completes cleanup and removes the fenced session from the registry.
    pub fn finish(mut self) -> Result<(), ClientLifecycleError> {
        self.registry
            .as_ref()
            .expect("unfinished cleanup retains its registry")
            .finish_cleanup(self.session)?;
        self.registry = None;
        Ok(())
    }
}

impl fmt::Debug for ClientCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCleanup")
            .field("session", &self.session)
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl Drop for ClientCleanup {
    fn drop(&mut self) {
        if let Some(registry) = &self.registry {
            registry.release_cleanup(self.session);
        }
    }
}

/// Synchronous client session registry.
///
/// State transitions, generation allocation, and the deadline index share one
/// mutex. The lock is never exposed and this component performs no external
/// cleanup itself; callers execute returned [`ClientCleanup`] values after the
/// method returns.
#[derive(Clone)]
pub struct ClientRegistry {
    inner: Arc<ClientRegistryInner>,
}

struct ClientRegistryInner {
    config: ClientLifecycleConfig,
    state: Mutex<RegistryState>,
}

struct RegistryState {
    entries: HashMap<ClientId, ClientEntry>,
    deadlines: BinaryHeap<Reverse<DeadlineRecord>>,
    cleanup_ready: VecDeque<ClientSession>,
    next_generation: u64,
}

struct ClientEntry {
    session: ClientSession,
    guard: ClientSessionGuard,
    phase: ClientPhase,
    expires_at: ClientTick,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientPhase {
    Active,
    CleanupPending {
        reason: CleanupReason,
        claim: CleanupClaimState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupClaimState {
    Ready,
    Queued,
    Running,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeadlineRecord {
    expires_at: ClientTick,
    client_id: ClientId,
    generation: u64,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self::with_config(ClientLifecycleConfig::default())
            .expect("the default ClientRegistry configuration is valid")
    }

    pub fn with_config(config: ClientLifecycleConfig) -> Result<Self, ClientLifecycleConfigError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(ClientRegistryInner {
                config,
                state: Mutex::new(RegistryState {
                    entries: HashMap::with_capacity(config.max_clients),
                    deadlines: BinaryHeap::with_capacity(config.max_clients),
                    cleanup_ready: VecDeque::new(),
                    next_generation: 1,
                }),
            }),
        })
    }

    pub fn config(&self) -> ClientLifecycleConfig {
        self.inner.config
    }

    pub fn len(&self) -> usize {
        self.inner.state.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.state.lock().entries.is_empty()
    }

    pub fn state(&self, client_id: ClientId) -> Option<ClientState> {
        self.inner
            .state
            .lock()
            .entries
            .get(&client_id)
            .map(ClientEntry::state)
    }

    /// Establishes a session after the caller has completed register/remount.
    ///
    /// Repeating activation for a still-live client is idempotent and also
    /// refreshes its deadline. A session that has reached its deadline is
    /// fenced before this method reports cleanup in progress.
    pub fn activate(
        &self,
        client_id: ClientId,
        now: ClientTick,
    ) -> Result<ActivateOutcome, ClientLifecycleError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId);
        }

        let mut registry = self.inner.state.lock();
        if let Some(entry) = registry.entries.get_mut(&client_id) {
            if !entry.is_active() {
                return Err(ClientLifecycleError::CleanupInProgress {
                    state: entry.state(),
                });
            }
            if entry.expire_if_due(now) {
                return Err(ClientLifecycleError::CleanupInProgress {
                    state: ClientState::Expired,
                });
            }
            entry.refresh(now, self.inner.config.ttl_ticks);
            return Ok(ActivateOutcome::AlreadyActive(entry.session));
        }

        if registry.entries.len() >= self.inner.config.max_clients {
            return Err(ClientLifecycleError::CapacityExceeded {
                max_clients: self.inner.config.max_clients,
            });
        }

        let generation = registry.allocate_generation()?;
        let session = ClientSession {
            client_id,
            generation,
        };
        let guard = ClientSessionGuard {
            inner: Arc::new(ClientSessionGuardInner {
                session,
                active: AtomicBool::new(true),
            }),
        };
        let expires_at = now.saturating_add(self.inner.config.ttl_ticks);
        registry.entries.insert(
            client_id,
            ClientEntry {
                session,
                guard,
                phase: ClientPhase::Active,
                expires_at,
            },
        );
        registry.deadlines.push(Reverse(DeadlineRecord {
            expires_at,
            client_id,
            generation,
        }));
        Ok(ActivateOutcome::Activated(session))
    }

    /// Refreshes the current active session, or requests remount without
    /// allocating state for unknown IDs.
    pub fn heartbeat(&self, client_id: ClientId, now: ClientTick) -> HeartbeatOutcome {
        let mut registry = self.inner.state.lock();
        let Some(entry) = registry.entries.get_mut(&client_id) else {
            return HeartbeatOutcome::NeedRemount;
        };
        if !entry.is_active() || entry.expire_if_due(now) {
            return HeartbeatOutcome::NeedRemount;
        }

        entry.refresh(now, self.inner.config.ttl_ticks);
        HeartbeatOutcome::Alive(entry.session)
    }

    pub fn active_session(
        &self,
        client_id: ClientId,
    ) -> Result<ClientSession, ClientLifecycleError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId);
        }
        self.inner
            .state
            .lock()
            .entries
            .get(&client_id)
            .filter(|entry| entry.is_active())
            .map(|entry| entry.session)
            .ok_or(ClientLifecycleError::ClientNotActive)
    }

    /// Returns the generation-bound fencing token used by a new operation.
    pub(crate) fn active_session_guard(
        &self,
        client_id: ClientId,
    ) -> Result<ClientSessionGuard, ClientLifecycleError> {
        if client_id.is_nil() {
            return Err(ClientLifecycleError::NilClientId);
        }
        self.inner
            .state
            .lock()
            .entries
            .get(&client_id)
            .filter(|entry| entry.is_active())
            .map(|entry| entry.guard.clone())
            .ok_or(ClientLifecycleError::ClientNotActive)
    }

    /// Removes an exact session that was published by a resource transaction
    /// which could not complete. Stale deadline records are discarded by the
    /// normal generation check when they become due.
    pub(crate) fn rollback_activation(
        &self,
        session: ClientSession,
    ) -> Result<(), ClientLifecycleError> {
        let mut registry = self.inner.state.lock();
        let entry = registry
            .entries
            .get(&session.client_id)
            .ok_or(ClientLifecycleError::ClientNotActive)?;
        entry.ensure_session(session)?;
        if !entry.is_active() {
            return Err(ClientLifecycleError::CleanupInProgress {
                state: entry.state(),
            });
        }
        entry.guard.invalidate();
        registry.entries.remove(&session.client_id);
        Ok(())
    }

    /// Fences a session and claims its cleanup when no worker owns it.
    pub fn begin_drain(
        &self,
        session: ClientSession,
        reason: CleanupReason,
    ) -> Result<Option<ClientCleanup>, ClientLifecycleError> {
        let mut registry = self.inner.state.lock();
        let Some(entry) = registry.entries.get_mut(&session.client_id) else {
            return Err(ClientLifecycleError::ClientNotActive);
        };
        entry.ensure_session(session)?;

        if entry.is_active() {
            entry.fence(reason);
        }

        let cleanup = entry.claim_cleanup(CleanupClaimState::Ready);
        Ok(cleanup.map(|(session, reason)| self.cleanup_claim(session, reason)))
    }

    /// Processes at most `budget` due or stale deadline records.
    ///
    /// Heartbeats update only the entry. When its original heap record becomes
    /// due, this scan either requeues the single latest deadline or fences and
    /// returns the session for cleanup. The configured per-round budget is a
    /// hard per-call cap; callers may pass a smaller budget for this round.
    pub fn claim_due_cleanups(&self, now: ClientTick, budget: usize) -> Vec<ClientCleanup> {
        let mut registry = self.inner.state.lock();
        let mut cleanups = Vec::new();
        let budget = budget.min(self.inner.config.cleanup_scan_budget);

        let mut processed = 0;
        while processed < budget {
            let Some(session) = registry.cleanup_ready.pop_front() else {
                break;
            };
            processed += 1;
            if let Some(entry) = registry.entries.get_mut(&session.client_id)
                && entry.session == session
                && let Some((session, reason)) = entry.claim_cleanup(CleanupClaimState::Queued)
            {
                cleanups.push(self.cleanup_claim(session, reason));
            }
        }

        while processed < budget {
            let Some(Reverse(record)) = registry.deadlines.peek().copied() else {
                break;
            };
            if record.expires_at > now {
                break;
            }
            registry.deadlines.pop();
            processed += 1;

            let mut replacement = None;
            if let Some(entry) = registry.entries.get_mut(&record.client_id)
                && entry.session.generation == record.generation
            {
                match entry.phase {
                    ClientPhase::Active if now < entry.expires_at => {
                        replacement = Some(DeadlineRecord {
                            expires_at: entry.expires_at,
                            client_id: record.client_id,
                            generation: record.generation,
                        });
                    }
                    ClientPhase::Active => {
                        entry.mark_expired();
                        if let Some((session, reason)) =
                            entry.claim_cleanup(CleanupClaimState::Ready)
                        {
                            cleanups.push(self.cleanup_claim(session, reason));
                        }
                    }
                    ClientPhase::CleanupPending {
                        reason: CleanupReason::HeartbeatExpired,
                        ..
                    } => {
                        if let Some((session, reason)) =
                            entry.claim_cleanup(CleanupClaimState::Ready)
                        {
                            cleanups.push(self.cleanup_claim(session, reason));
                        }
                    }
                    ClientPhase::CleanupPending { .. } => {}
                }
            }
            if let Some(replacement) = replacement {
                registry.deadlines.push(Reverse(replacement));
            }
        }

        cleanups
    }
    fn cleanup_claim(&self, session: ClientSession, reason: CleanupReason) -> ClientCleanup {
        ClientCleanup {
            registry: Some(Arc::clone(&self.inner)),
            session,
            reason,
        }
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistryState {
    fn allocate_generation(&mut self) -> Result<u64, ClientLifecycleError> {
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or(ClientLifecycleError::GenerationExhausted)?;
        Ok(generation)
    }
}

impl ClientRegistryInner {
    fn finish_cleanup(&self, session: ClientSession) -> Result<(), ClientLifecycleError> {
        let mut registry = self.state.lock();
        let Some(entry) = registry.entries.get(&session.client_id) else {
            return Ok(());
        };
        entry.ensure_session(session)?;
        if !entry.cleanup_is_running() {
            return Err(ClientLifecycleError::CleanupNotStarted);
        }
        registry.entries.remove(&session.client_id);
        Ok(())
    }

    fn release_cleanup(&self, session: ClientSession) {
        let mut registry = self.state.lock();
        let should_retry = registry
            .entries
            .get_mut(&session.client_id)
            .is_some_and(|entry| entry.session == session && entry.queue_running_cleanup());
        if should_retry {
            registry.cleanup_ready.push_back(session);
        }
    }
}

impl ClientEntry {
    fn state(&self) -> ClientState {
        match self.phase {
            ClientPhase::Active => ClientState::Active,
            ClientPhase::CleanupPending {
                reason: CleanupReason::HeartbeatExpired,
                ..
            } => ClientState::Expired,
            ClientPhase::CleanupPending { .. } => ClientState::Draining,
        }
    }

    fn is_active(&self) -> bool {
        self.phase == ClientPhase::Active
    }

    fn ensure_session(&self, session: ClientSession) -> Result<(), ClientLifecycleError> {
        if self.session == session {
            Ok(())
        } else {
            Err(ClientLifecycleError::StaleSession)
        }
    }

    fn refresh(&mut self, now: ClientTick, ttl_ticks: u64) {
        self.expires_at = self.expires_at.max(now.saturating_add(ttl_ticks));
    }

    fn expire_if_due(&mut self, now: ClientTick) -> bool {
        if now < self.expires_at {
            return false;
        }
        self.mark_expired();
        true
    }

    fn mark_expired(&mut self) {
        debug_assert!(self.is_active());
        self.phase = ClientPhase::CleanupPending {
            reason: CleanupReason::HeartbeatExpired,
            claim: CleanupClaimState::Ready,
        };
        self.guard.invalidate();
    }

    fn fence(&mut self, reason: CleanupReason) {
        debug_assert!(self.is_active());
        self.phase = ClientPhase::CleanupPending {
            reason,
            claim: CleanupClaimState::Ready,
        };
        self.guard.invalidate();
    }

    fn claim_cleanup(
        &mut self,
        expected: CleanupClaimState,
    ) -> Option<(ClientSession, CleanupReason)> {
        let ClientPhase::CleanupPending { reason, claim } = &mut self.phase else {
            return None;
        };
        if *claim != expected {
            return None;
        }
        *claim = CleanupClaimState::Running;
        Some((self.session, *reason))
    }

    fn cleanup_is_running(&self) -> bool {
        matches!(
            self.phase,
            ClientPhase::CleanupPending {
                claim: CleanupClaimState::Running,
                ..
            }
        )
    }

    fn queue_running_cleanup(&mut self) -> bool {
        let ClientPhase::CleanupPending { claim, .. } = &mut self.phase else {
            return false;
        };
        if *claim != CleanupClaimState::Running {
            return false;
        }
        *claim = CleanupClaimState::Queued;
        true
    }
}
