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
mod eviction;
mod identity;
mod manager;
pub mod reclamation;
mod replica;
mod tenant;
mod write;

pub use catalog::{
    LiveReplicaView, ObjectCatalog, ObjectHandle, ObjectRead, PutClaim, PutTicket, ReplicaSetView,
};
pub use config::ObjectCatalogConfig;
pub use content::{ObjectContent, ObjectKind};
pub use eviction::{
    DEFAULT_ALLOCATION_FAILURE_BUDGET, MemoryEvictionConfig, MemoryEvictionConfigError,
    MemoryEvictionStats,
};
pub use identity::{NamespaceId, ObjectIdentity, ObjectKey, ObjectLookup};
pub use manager::{
    AllocatedReplica, ObjectManager, ObjectManagerMaintenance, ObjectPutPlan, PendingWriteRevoker,
    ReplicaSelector, StartedPut,
};
pub use reclamation::ReclaimFilter;
pub use replica::{DirectReplica, LocalSsdReplica, ReplicaId, ReplicaLease, ReplicaSet};
pub use tenant::{
    ResolvedTenant, TenantAdminError, TenantCatalog, TenantConfig, TenantConfigError,
    TenantGetError, TenantId, TenantIdError, TenantObjectError, TenantObjectManager,
    TenantObjectManagerCreateError, TenantPolicy, TenantPutRequest, TenantQuotaLimits,
    TenantQuotaSnapshot, TenantRemoveError, TenantResourceClass, TenantSnapshot,
};
pub use write::{ObjectCommit, WriteAdmission, WriteId, WriteOwner};
