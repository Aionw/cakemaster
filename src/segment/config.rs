//! Segment-pool allocator configuration.

pub const DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT: u32 = 128 * 1024;
pub const DEFAULT_ALLOCATOR_SHARDS: usize = 1;
pub(super) const MIN_ALLOCATOR_NODES_PER_SEGMENT: u32 = 3;
pub(super) const MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE: u32 = u32::MAX - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPoolConfig {
    pub(super) max_allocator_nodes_per_segment: u32,
    pub(super) allocator_shards: usize,
    pub(super) allocator_shard_index: Option<usize>,
}

impl SegmentPoolConfig {
    pub const fn new(max_allocator_nodes_per_segment: u32) -> Self {
        Self {
            max_allocator_nodes_per_segment,
            allocator_shards: DEFAULT_ALLOCATOR_SHARDS,
            allocator_shard_index: None,
        }
    }

    /// Splits each direct-memory resource into disjoint allocator extents.
    /// Empty extents may move between metadata shard owners under pressure.
    pub const fn with_allocator_shards(mut self, allocator_shards: usize) -> Self {
        self.allocator_shards = allocator_shards;
        self.allocator_shard_index = None;
        self
    }

    pub(crate) const fn for_allocator_shard(
        mut self,
        allocator_shard_index: usize,
        allocator_shards: usize,
    ) -> Self {
        self.allocator_shards = allocator_shards;
        self.allocator_shard_index = Some(allocator_shard_index);
        self
    }

    /// Returns the fixed metadata-node budget created for each direct-memory
    /// allocator.
    pub const fn max_allocator_nodes_per_segment(self) -> u32 {
        self.max_allocator_nodes_per_segment
    }

    pub const fn allocator_shards(self) -> usize {
        self.allocator_shards
    }
}

impl Default for SegmentPoolConfig {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT)
    }
}
