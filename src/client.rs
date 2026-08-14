//! Client identity, session fencing, heartbeat, and cleanup lifecycle.
//!
//! [`ClientRegistry`] contains no async or resource-management calls. It
//! fences expired or draining sessions under one short lock and returns
//! generation-tagged [`ClientCleanup`] work. [`ClientManager`] coordinates
//! the resulting segment and object cleanup outside that registry lock.

pub mod config;
pub mod error;
mod lifecycle;
mod manager;

pub use crate::segment::ClientId;
pub use config::ClientLifecycleConfig;
pub use error::{ClientLifecycleConfigError, ClientLifecycleError};
pub(crate) use lifecycle::ClientSessionGuard;
pub use lifecycle::{
    ActivateOutcome, CleanupReason, ClientCleanup, ClientRegistry, ClientSession, ClientState,
    ClientTick, HeartbeatOutcome,
};
pub use manager::{
    ClientCleanupReport, ClientManager, ClientManagerError, GracefulUnmountReport,
    SegmentUnmountOutcome,
};
