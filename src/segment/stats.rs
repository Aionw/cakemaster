//! Segment operational-state and capacity diagnostics.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentState {
    /// Published to placement and accepting new resource reservations.
    Accepting,
    /// Hidden from placement but still logically readable.
    Quiesced,
    /// Logically invalid. Outstanding resource handles may still defer the
    /// physical release of allocator state.
    Removed,
}

impl SegmentState {
    pub const fn is_accepting(self) -> bool {
        matches!(self, Self::Accepting)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentSpaceStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub largest_free_region_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentUsageStats {
    /// Reservations that still hold capacity in this segment.
    pub active_allocations: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStats {
    pub space: SegmentSpaceStats,
    pub usage: SegmentUsageStats,
    pub state: SegmentState,
}

/// Aggregate physical space for one replica class.
///
/// Both accepting and quiesced
/// segments are included because quiescing placement does not release the
/// underlying allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaClassSpaceStats {
    pub generation: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub largest_free_region_bytes: u64,
}
