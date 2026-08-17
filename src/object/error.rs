//! Errors returned by object-catalog operations.

use super::reclamation::CatalogTick;
use super::replica::ReplicaId;
use crate::segment::ReplicaClass;
use crate::segment::error::ReserveError;
use crate::segment::placement::PlacementError;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectCatalogConfigError {
    #[error("`expected_objects` must be greater than zero")]
    ZeroExpectedObjects,
    #[error("`lease_ttl_ticks` must be greater than zero")]
    ZeroLeaseTtl,
    #[error(
        "`lease_refresh_ticks` ({lease_refresh_ticks}) must not exceed `lease_ttl_ticks` ({lease_ttl_ticks})"
    )]
    LeaseRefreshExceedsTtl {
        lease_ttl_ticks: u64,
        lease_refresh_ticks: u64,
    },
    #[error("`pending_timeout_ticks` must be greater than zero")]
    ZeroPendingTimeout,
    #[error("`max_retired_bytes` must be greater than zero")]
    ZeroMaxRetiredBytes,
    #[error(
        "default soft-pin TTL ({default_soft_pin_ttl_ticks}) must not exceed the maximum ({max_soft_pin_ttl_ticks})"
    )]
    DefaultSoftPinTtlExceedsMaximum {
        default_soft_pin_ttl_ticks: u64,
        max_soft_pin_ttl_ticks: u64,
    },
    #[error("soft-pin TTL is only valid with the enable action")]
    SoftPinTtlRequiresEnable,
    #[error(
        "soft-pin TTL ({soft_pin_ttl_ticks}) exceeds the configured maximum ({max_soft_pin_ttl_ticks})"
    )]
    SoftPinTtlExceedsMaximum {
        soft_pin_ttl_ticks: u64,
        max_soft_pin_ttl_ticks: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeginError {
    #[error("object key must not be empty")]
    EmptyKey,
    #[error("object pin request is invalid: {0}")]
    InvalidPinRequest(ObjectCatalogConfigError),
    #[error("object already exists")]
    AlreadyExists,
    #[error("an object write is already in progress")]
    WriteInProgress,
    #[error("object reclamation backlog limit has been reached")]
    ReclamationBacklog,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StageError {
    #[error("object catalog is no longer available")]
    CatalogDropped,
    #[error("object write claim was lost")]
    ClaimLost,
    #[error("the client session that owns the write is no longer active")]
    OwnerInactive,
    #[error("object content size must not be zero")]
    ZeroSize,
    #[error("object must have at least one replica")]
    NoReplicas,
    #[error(
        "replica {} has capacity {capacity_bytes} bytes, but {required_bytes} bytes are required",
        .replica.get()
    )]
    ReplicaTooSmall {
        replica: ReplicaId,
        required_bytes: u64,
        capacity_bytes: u64,
    },
    #[error("replica {} belongs to an invalidated segment", .replica.get())]
    ReplicaInvalidated { replica: ReplicaId },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CommitError {
    #[error("write transaction belongs to another object catalog")]
    ForeignCatalog,
    #[error("object write transaction no longer exists")]
    TransactionGone,
    #[error("object write transaction is not staged")]
    NotStaged,
    #[error("object commit metadata conflicts with the pending publication")]
    CommitConflict,
    #[error("one or more object replicas belong to an invalidated segment")]
    ReplicasInvalidated,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AbortError {
    #[error("write transaction belongs to another object catalog")]
    ForeignCatalog,
    #[error("object write transaction no longer exists")]
    TransactionGone,
    #[error("object write transaction has already committed")]
    AlreadyCommitted,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LookupError {
    #[error("object was not found")]
    NotFound,
    #[error("object is not ready")]
    NotReady,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RemoveError {
    #[error("object was not found")]
    NotFound,
    #[error("object is not ready")]
    NotReady,
    #[error("object is leased until catalog tick {}", .expires_at.get())]
    Leased { expires_at: CatalogTick },
    #[error("object is hard-pinned")]
    HardPinned,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectRemoveError {
    #[error("object was not found")]
    NotFound,
    #[error("object is not ready")]
    NotReady,
    #[error("object is leased until catalog tick {}", .expires_at.get())]
    Leased { expires_at: CatalogTick },
    #[error("object is hard-pinned")]
    HardPinned,
}

impl From<RemoveError> for ObjectRemoveError {
    fn from(error: RemoveError) -> Self {
        match error {
            RemoveError::NotFound => Self::NotFound,
            RemoveError::NotReady => Self::NotReady,
            RemoveError::Leased { expires_at } => Self::Leased { expires_at },
            RemoveError::HardPinned => Self::HardPinned,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectManagerError {
    #[error("object put plan is invalid")]
    InvalidPlan,
    #[error("object already exists or has a write in progress")]
    AlreadyExists,
    #[error("no suitable replicas are available")]
    NoAvailableReplicas,
    #[error("object or pending write was not found")]
    NotFound,
    #[error("pending write belongs to another owner")]
    IllegalOwner,
    #[error("pending write uses {actual:?} replicas, not {requested:?}")]
    ReplicaClassMismatch {
        requested: ReplicaClass,
        actual: ReplicaClass,
    },
    #[error("object write is no longer valid")]
    InvalidWrite,
    #[error("object manager invariant failed")]
    Internal,
}

impl From<BeginError> for ObjectManagerError {
    fn from(error: BeginError) -> Self {
        match error {
            BeginError::EmptyKey | BeginError::InvalidPinRequest(_) => Self::InvalidPlan,
            BeginError::AlreadyExists | BeginError::WriteInProgress => Self::AlreadyExists,
            BeginError::ReclamationBacklog => Self::NoAvailableReplicas,
        }
    }
}

impl From<PlacementError> for ObjectManagerError {
    fn from(error: PlacementError) -> Self {
        let mapped = match error {
            PlacementError::ZeroSize
            | PlacementError::ZeroReplicas
            | PlacementError::Reserve(ReserveError::ZeroSize) => Self::InvalidPlan,
            PlacementError::InsufficientReplicas { .. }
            | PlacementError::Reserve(
                ReserveError::NotAccepting(_)
                | ReserveError::OutOfSpace(_)
                | ReserveError::NotFound(_),
            ) => Self::NoAvailableReplicas,
            PlacementError::Reserve(
                ReserveError::ForeignCandidate
                | ReserveError::NotDirectlyAllocatable(_)
                | ReserveError::AddressOverflow(_),
            ) => Self::Internal,
        };
        if mapped == Self::Internal {
            log::error!(
                target: "cakemaster::object::manager",
                source_error:% = error;
                "placement error violated an object manager invariant"
            );
        }
        mapped
    }
}

impl From<StageError> for ObjectManagerError {
    fn from(error: StageError) -> Self {
        let mapped = match error {
            StageError::ZeroSize => Self::InvalidPlan,
            StageError::NoReplicas | StageError::ReplicaInvalidated { .. } => {
                Self::NoAvailableReplicas
            }
            StageError::CatalogDropped
            | StageError::ClaimLost
            | StageError::ReplicaTooSmall { .. } => Self::Internal,
            StageError::OwnerInactive => Self::InvalidWrite,
        };
        if mapped == Self::Internal {
            log::error!(
                target: "cakemaster::object::manager",
                source_error:% = error;
                "catalog staging error violated an object manager invariant"
            );
        }
        mapped
    }
}
