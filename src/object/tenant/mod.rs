//! Tenant isolation and per-resource quota policy for the object manager.

mod manager;
mod quota;
mod registry;

use super::error::ObjectManagerError;
use super::identity::NamespaceId;
use crate::segment::ReplicaClass;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

pub use manager::{
    TenantCatalog, TenantGetError, TenantObjectManager, TenantObjectManagerCreateError,
    TenantPutRequest, TenantRemoveError,
};
pub(crate) use quota::QuotaReservationGuard;
pub(crate) use quota::TenantQuotaCharge;
pub use registry::ResolvedTenant;

/// External tenant identity. Its internal representation is deliberately
/// opaque so callers cannot depend on the namespace encoding.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantId(Arc<str>);

impl TenantId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, TenantIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(TenantIdError::Empty);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<&str> for TenantId {
    type Error = TenantIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(Arc::<str>::from(value))
    }
}

impl TryFrom<String> for TenantId {
    type Error = TenantIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(Arc::<str>::from(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TenantResourceClass {
    Memory,
    Nof,
}

impl TenantResourceClass {
    pub const fn replica_class(self) -> ReplicaClass {
        match self {
            Self::Memory => ReplicaClass::Memory,
            Self::Nof => ReplicaClass::Nof,
        }
    }

    pub fn from_replica_class(replica_class: ReplicaClass) -> Option<Self> {
        match replica_class {
            ReplicaClass::Memory => Some(Self::Memory),
            ReplicaClass::Nof => Some(Self::Nof),
            _ => None,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Memory => 0,
            Self::Nof => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TenantQuotaLimits {
    memory_bytes: u64,
    nof_bytes: u64,
}

impl TenantQuotaLimits {
    pub const fn new(memory_bytes: u64, nof_bytes: u64) -> Self {
        Self {
            memory_bytes,
            nof_bytes,
        }
    }

    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    pub const fn nof_bytes(self) -> u64 {
        self.nof_bytes
    }

    pub const fn for_class(self, class: TenantResourceClass) -> u64 {
        match class {
            TenantResourceClass::Memory => self.memory_bytes,
            TenantResourceClass::Nof => self.nof_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TenantPolicy {
    quota: TenantQuotaLimits,
}

impl TenantPolicy {
    pub const fn new(quota: TenantQuotaLimits) -> Self {
        Self { quota }
    }

    pub const fn quota(self) -> TenantQuotaLimits {
        self.quota
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TenantConfig {
    /// Compatibility mode: every external tenant ID maps to the default
    /// namespace and quota accounting is bypassed.
    #[default]
    Single,
    Multi {
        initial_policies: Vec<(TenantId, TenantPolicy)>,
    },
}

impl TenantConfig {
    pub fn multi(initial_policies: Vec<(TenantId, TenantPolicy)>) -> Self {
        Self::Multi { initial_policies }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TenantQuotaSnapshot {
    pub requested_bytes: u64,
    pub effective_bytes: u64,
    pub demand_bytes: u64,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
    pub retiring_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantSnapshot {
    pub id: TenantId,
    pub namespace: NamespaceId,
    pub generation: u64,
    pub policy: TenantPolicy,
    pub memory: TenantQuotaSnapshot,
    pub nof: TenantQuotaSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TenantIdError {
    #[error("tenant ID must not be empty")]
    Empty,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TenantConfigError {
    #[error("tenant `{0}` appears more than once in the initial policy list")]
    DuplicateTenant(TenantId),
    #[error("tenant namespace space is exhausted")]
    NamespaceExhausted,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TenantAdminError {
    #[error("tenant administration is unavailable in single-tenant mode")]
    SingleTenantMode,
    #[error("tenant is not registered")]
    TenantNotRegistered,
    #[error("tenant still has reserved, used, or retiring quota demand")]
    TenantNotEmpty,
    #[error("tenant namespace space is exhausted")]
    NamespaceExhausted,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TenantObjectError {
    #[error("tenant is not registered")]
    TenantNotRegistered,
    #[error("resolved tenant belongs to another manager or generation")]
    InvalidTenantHandle,
    #[error(
        "tenant {class:?} quota exceeded: requested {requested_bytes}, demand {demand_bytes}, effective limit {effective_bytes}"
    )]
    TenantQuotaExceeded {
        class: TenantResourceClass,
        requested_bytes: u64,
        demand_bytes: u64,
        effective_bytes: u64,
    },
    #[error(transparent)]
    Object(#[from] ObjectManagerError),
}
