use super::ClientId;
use super::config::ClientLifecycleConfig;
use super::error::{ClientLifecycleConfigError, ClientLifecycleError};
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientCleanup {
    session: ClientSession,
    reason: CleanupReason,
}

impl ClientCleanup {
    pub const fn session(self) -> ClientSession {
        self.session
    }

    pub const fn reason(self) -> CleanupReason {
        self.reason
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
    next_generation: u64,
}

struct ClientEntry {
    session: ClientSession,
    state: ClientState,
    expires_at: ClientTick,
    cleanup_reason: Option<CleanupReason>,
    cleanup_issued: bool,
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
            .map(|entry| entry.state)
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
            if entry.state != ClientState::Active {
                return Err(ClientLifecycleError::CleanupInProgress { state: entry.state });
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
        let expires_at = now.saturating_add(self.inner.config.ttl_ticks);
        registry.entries.insert(
            client_id,
            ClientEntry {
                session,
                state: ClientState::Active,
                expires_at,
                cleanup_reason: None,
                cleanup_issued: false,
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
        if entry.state != ClientState::Active || entry.expire_if_due(now) {
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
            .filter(|entry| entry.state == ClientState::Active)
            .map(|entry| entry.session)
            .ok_or(ClientLifecycleError::ClientNotActive)
    }

    /// Fences a session and emits cleanup work exactly once.
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

        if entry.state == ClientState::Active {
            entry.state = match reason {
                CleanupReason::HeartbeatExpired => ClientState::Expired,
                CleanupReason::GracefulUnmount | CleanupReason::ServerShutdown => {
                    ClientState::Draining
                }
            };
            entry.cleanup_reason = Some(reason);
        }

        Ok(entry.issue_cleanup())
    }

    /// Processes at most `budget` due or stale deadline records.
    ///
    /// Heartbeats update only the entry. When its original heap record becomes
    /// due, maintenance either requeues the single latest deadline or fences
    /// and returns the session for cleanup. The configured maintenance budget
    /// is a hard per-call cap; callers may pass a smaller budget for this round.
    pub fn maintenance(&self, now: ClientTick, budget: usize) -> Vec<ClientCleanup> {
        let mut registry = self.inner.state.lock();
        let mut cleanups = Vec::new();
        let budget = budget.min(self.inner.config.maintenance_budget);

        for _ in 0..budget {
            let Some(Reverse(record)) = registry.deadlines.peek().copied() else {
                break;
            };
            if record.expires_at > now {
                break;
            }
            registry.deadlines.pop();

            let mut replacement = None;
            if let Some(entry) = registry.entries.get_mut(&record.client_id)
                && entry.session.generation == record.generation
            {
                match entry.state {
                    ClientState::Active if now < entry.expires_at => {
                        replacement = Some(DeadlineRecord {
                            expires_at: entry.expires_at,
                            client_id: record.client_id,
                            generation: record.generation,
                        });
                    }
                    ClientState::Active => {
                        entry.mark_expired();
                        if let Some(cleanup) = entry.issue_cleanup() {
                            cleanups.push(cleanup);
                        }
                    }
                    ClientState::Expired => {
                        if let Some(cleanup) = entry.issue_cleanup() {
                            cleanups.push(cleanup);
                        }
                    }
                    ClientState::Draining => {}
                }
            }
            if let Some(replacement) = replacement {
                registry.deadlines.push(Reverse(replacement));
            }
        }

        cleanups
    }

    /// Removes a fenced session after all external resource cleanup succeeds.
    ///
    /// Finishing an already removed session is idempotent. If a newer session
    /// with the same client ID exists, the generation mismatch is rejected.
    pub fn finish_cleanup(&self, session: ClientSession) -> Result<(), ClientLifecycleError> {
        let mut registry = self.inner.state.lock();
        let Some(entry) = registry.entries.get(&session.client_id) else {
            return Ok(());
        };
        entry.ensure_session(session)?;
        if entry.state == ClientState::Active || !entry.cleanup_issued {
            return Err(ClientLifecycleError::CleanupNotStarted);
        }
        registry.entries.remove(&session.client_id);
        Ok(())
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

impl ClientEntry {
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
        self.state = ClientState::Expired;
        self.cleanup_reason = Some(CleanupReason::HeartbeatExpired);
    }

    fn issue_cleanup(&mut self) -> Option<ClientCleanup> {
        if self.cleanup_issued {
            return None;
        }
        let reason = self.cleanup_reason?;
        self.cleanup_issued = true;
        Some(ClientCleanup {
            session: self.session,
            reason,
        })
    }
}
