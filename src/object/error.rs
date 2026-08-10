//! Errors returned by object-catalog operations.

use super::reclamation::CatalogTick;
use super::replica::ReplicaId;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectCatalogConfigError {
    ZeroExpectedObjects,
    ZeroLeaseTtl,
    RefreshExceedsLease,
    ZeroPendingTimeout,
    ZeroRetiredLimit,
}

impl fmt::Display for ObjectCatalogConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroExpectedObjects => "expected object count must not be zero",
            Self::ZeroLeaseTtl => "object lease TTL must not be zero",
            Self::RefreshExceedsLease => "lease refresh threshold must not exceed the lease TTL",
            Self::ZeroPendingTimeout => "pending object timeout must not be zero",
            Self::ZeroRetiredLimit => "retired byte limit must not be zero",
        })
    }
}

impl std::error::Error for ObjectCatalogConfigError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutError {
    EmptyKey,
    AlreadyExists,
    WriteInProgress,
    ReclamationBacklog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageError {
    CatalogDropped,
    ClaimLost,
    ZeroSize,
    NoReplicas,
    ReplicaTooSmall {
        replica: ReplicaId,
        required_bytes: u64,
        capacity_bytes: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishError {
    ForeignCatalog,
    ObjectGone,
    PublicationInProgress,
    NotPending,
    CommitConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevokeError {
    ForeignCatalog,
    ObjectGone,
    AlreadyPublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LookupError {
    NotFound,
    NotReady,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveError {
    NotFound,
    NotReady,
    Leased { expires_at: CatalogTick },
}

macro_rules! impl_error {
    ($type:ty) => {
        impl std::error::Error for $type {}

        impl fmt::Display for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{self:?}")
            }
        }
    };
}

impl_error!(PutError);
impl_error!(StageError);
impl_error!(PublishError);
impl_error!(RevokeError);
impl_error!(LookupError);
impl_error!(RemoveError);
