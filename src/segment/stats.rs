//! Segment lifecycle and capacity diagnostics.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentState {
    Accepting,
    Quiesced,
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
pub struct SegmentReservationStats {
    pub live: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStats {
    pub space: SegmentSpaceStats,
    pub reservations: SegmentReservationStats,
    pub state: SegmentState,
}
