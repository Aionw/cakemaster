//! Client identity, session fencing, heartbeat, and cleanup lifecycle.
//!
//! [`ClientRegistry`] deliberately contains no async or resource-management
//! calls. It fences expired or draining sessions under one short lock and
//! returns generation-tagged [`ClientCleanup`] work for the server runtime to
//! execute afterwards.

pub mod config;
pub mod error;
mod lifecycle;

pub use crate::segment::ClientId;
pub use config::ClientLifecycleConfig;
pub use error::{ClientLifecycleConfigError, ClientLifecycleError};
pub use lifecycle::{
    ActivateOutcome, CleanupReason, ClientCleanup, ClientRegistry, ClientSession, ClientState,
    ClientTick, HeartbeatOutcome,
};
