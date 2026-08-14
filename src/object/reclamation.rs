//! Incremental object-catalog reclamation controls and reports.

use super::NamespaceId;
use crate::segment::ReplicaClass;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogTick(u64);

impl CatalogTick {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn saturating_add(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectBudget {
    pub(super) max_candidates: usize,
    pub(super) max_reclaims: usize,
    pub(super) max_empty_slots: usize,
}

impl CollectBudget {
    pub const fn new(max_candidates: usize, max_reclaims: usize, max_empty_slots: usize) -> Self {
        Self {
            max_candidates,
            max_reclaims,
            max_empty_slots,
        }
    }

    pub const fn max_candidates(self) -> usize {
        self.max_candidates
    }

    pub const fn max_reclaims(self) -> usize {
        self.max_reclaims
    }

    pub const fn max_empty_slots(self) -> usize {
        self.max_empty_slots
    }
}

impl Default for CollectBudget {
    fn default() -> Self {
        Self::new(64, 64, 16)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CollectReport {
    pub busy: bool,
    pub scanned_candidates: usize,
    pub expired_pending: usize,
    pub invalidated_pending: usize,
    pub invalidated_published: usize,
    pub retired_objects: usize,
    pub retired_bytes: u64,
    pub reclaimed_objects: usize,
    pub reclaimed_bytes: u64,
    pub removed_empty_slots: usize,
    pub scoped_retired_objects: usize,
    pub scoped_retired_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclaimFilter {
    Any,
    Scope {
        namespace: NamespaceId,
        replica_class: ReplicaClass,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReclaimTarget {
    pub filter: ReclaimFilter,
    pub bytes: u64,
}
