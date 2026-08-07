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
mod types;

pub use catalog::{
    CatalogIndexConfig, CollectReport, LookupError, ObjectCatalog, ObjectCatalogConfig,
    ObjectCatalogConfigError, ObjectCatalogStats, ObjectHandle, ObjectLeasePolicy, ObjectRead,
    PublishError, PutClaim, PutError, PutTicket, ReclamationPolicy, RemoveError, RevokeError,
    StageError,
};
pub use types::{
    CatalogTick, CollectBudget, MemoryReplica, NamespaceId, ObjectCommit, ObjectContent,
    ObjectIdentity, ObjectKey, ObjectKind, ObjectLookup, ReclaimReason, ReclaimTarget, ReplicaId,
    ReplicaLease, ReplicaSet, WriteId, WriteOwner,
};
