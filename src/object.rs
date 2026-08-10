//! Object identity, publication, lookup, and incremental reclamation.
//!
//! A put moves through `claim_put -> PutClaim::stage -> publish`. Dropping a
//! claim rolls it back, while dropping tickets or read handles never releases
//! live storage prematurely. Lookups pin immutable object versions with an
//! `Arc`; `collect_step` only detaches expired versions and releases their
//! reservations after the final pin disappears.
//!
//! The root index stores stable slots rather than object payloads. Eviction is
//! therefore a slot-local compare-and-swap followed by bounded queue work; it
//! never performs a stop-the-world map scan.

mod catalog;
pub mod config;
mod content;
pub mod diagnostics;
pub mod error;
mod identity;
pub mod reclamation;
mod replica;
mod write;

pub use catalog::{ObjectCatalog, ObjectHandle, ObjectRead, PutClaim, PutTicket};
pub use config::ObjectCatalogConfig;
pub use content::{ObjectContent, ObjectKind};
pub use identity::{NamespaceId, ObjectIdentity, ObjectKey, ObjectLookup};
pub use replica::{MemoryReplica, NofReplica, ReplicaId, ReplicaLease, ReplicaSet};
pub use write::{ObjectCommit, WriteId, WriteOwner};
