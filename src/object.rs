//! Object identity, publication, lookup, and incremental reclamation.
//!
//! A write moves through `begin_write -> WriteClaim::stage -> commit`. Dropping
//! a claim aborts it, while dropping transaction tokens or read handles never
//! releases live storage prematurely. Lookups pin immutable committed versions
//! with an `Arc`; `collect_step` releases retired reservations only after their
//! lease and final local pin are gone.
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
    LiveReplicaView, ObjectCatalog, ObjectHandle, ObjectRead, ReplicaSetView, WriteClaim,
    WriteTransaction,
};
pub use config::{
    DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS, DEFAULT_EXPECTED_OBJECTS,
    DEFAULT_MAX_SOFT_PIN_TTL_TICKS, DEFAULT_SOFT_PIN_TTL_TICKS, ObjectCatalogConfig,
    ObjectPinRequest, SoftPinAction,
};
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
pub(crate) use replica::{ReplicaSnapshot, ReplicaSnapshotSet};
pub use tenant::{
    ResolvedTenant, TenantAdminError, TenantCatalog, TenantConfig, TenantConfigError,
    TenantGetError, TenantId, TenantIdError, TenantObjectError, TenantObjectManager,
    TenantObjectManagerCreateError, TenantPolicy, TenantPutRequest, TenantQuotaLimits,
    TenantQuotaSnapshot, TenantRemoveError, TenantResourceClass, TenantSnapshot,
};
pub use write::{ObjectCommit, TransactionId, VersionId, WriteAdmission, WriteMode, WriteOwner};
