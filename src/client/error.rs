//! Errors returned by client lifecycle operations.

use super::lifecycle::ClientState;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClientLifecycleConfigError {
    #[error("`ttl_ticks` must be greater than zero")]
    ZeroTtl,
    #[error("`max_clients` must be greater than zero")]
    ZeroMaxClients,
    #[error("`maintenance_budget` must be greater than zero")]
    ZeroMaintenanceBudget,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClientLifecycleError {
    #[error("client ID must not be nil")]
    NilClientId,
    #[error("client registry capacity of {max_clients} entries has been reached")]
    CapacityExceeded { max_clients: usize },
    #[error("client cleanup is already in progress ({state:?})")]
    CleanupInProgress { state: ClientState },
    #[error("client does not have an active session")]
    ClientNotActive,
    #[error("client cleanup has not been started")]
    CleanupNotStarted,
    #[error("client session generation is stale")]
    StaleSession,
    #[error("client session generation space is exhausted")]
    GenerationExhausted,
}
