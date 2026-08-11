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
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PutError {
    #[error("object key must not be empty")]
    EmptyKey,
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
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PublishError {
    #[error("put ticket belongs to another object catalog")]
    ForeignCatalog,
    #[error("object write no longer exists")]
    ObjectGone,
    #[error("object publication is already in progress")]
    PublicationInProgress,
    #[error("object is not pending publication")]
    NotPending,
    #[error("object commit metadata conflicts with the pending publication")]
    CommitConflict,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RevokeError {
    #[error("put ticket belongs to another object catalog")]
    ForeignCatalog,
    #[error("object write no longer exists")]
    ObjectGone,
    #[error("object has already been published")]
    AlreadyPublished,
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

impl From<PutError> for ObjectManagerError {
    fn from(error: PutError) -> Self {
        match error {
            PutError::EmptyKey => Self::InvalidPlan,
            PutError::AlreadyExists | PutError::WriteInProgress => Self::AlreadyExists,
            PutError::ReclamationBacklog => Self::NoAvailableReplicas,
        }
    }
}

impl From<PlacementError> for ObjectManagerError {
    fn from(error: PlacementError) -> Self {
        match error {
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
        }
    }
}

impl From<StageError> for ObjectManagerError {
    fn from(error: StageError) -> Self {
        match error {
            StageError::ZeroSize => Self::InvalidPlan,
            StageError::NoReplicas => Self::NoAvailableReplicas,
            StageError::CatalogDropped
            | StageError::ClaimLost
            | StageError::ReplicaTooSmall { .. } => Self::Internal,
        }
    }
}
