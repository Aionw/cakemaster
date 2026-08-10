//! Segment-pool allocator configuration.

pub const DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT: u32 = 128 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPoolConfig {
    pub(super) max_allocator_nodes_per_segment: u32,
}

impl SegmentPoolConfig {
    pub const fn new(max_allocator_nodes_per_segment: u32) -> Self {
        Self {
            max_allocator_nodes_per_segment,
        }
    }
}

impl Default for SegmentPoolConfig {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT)
    }
}
