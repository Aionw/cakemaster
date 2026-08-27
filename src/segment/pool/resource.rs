use crate::segment::descriptor::MemoryRegion;
use crate::segment::error::{AttachError, LocalSsdError, ReserveError};
use crate::segment::lifetime::SegmentLease;
use crate::segment::local_ssd::{AdmissionFailure, LocalSsdCapacity, LocalSsdStats, OffloadPermit};
use crate::segment::offset_allocator::ByteAllocator;
use crate::segment::reservation::Reservation;
use crate::segment::spec::{
    CxlArenaId, CxlArenaSpec, ReplicaClass, SegmentConfiguration, SegmentSpec,
};
use crate::segment::stats::SegmentSpaceStats;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum CandidateCapability {
    Direct(ReplicaClass),
    Offload,
}

/// Resource interface consumed by a logical segment entry.
///
/// The catalog only mounts this interface. Concrete segment kinds and shared
/// physical-resource rules stay in this module.
pub(super) enum MountedResource {
    Range(ShardedByteAllocator),
    LocalSsd(LocalSsdCapacity),
}

#[derive(Default)]
pub(super) struct ResourceRegistry {
    cxl_arenas: HashMap<CxlArenaId, CxlArenaResource>,
}

struct CxlArenaResource {
    spec: CxlArenaSpec,
    allocator: ShardedByteAllocator,
    attached_segments: usize,
}

#[derive(Clone)]
pub(super) struct ShardedByteAllocator {
    extents: ExtentStore,
}

#[derive(Clone)]
enum ExtentStore {
    Concurrent(Arc<Mutex<Vec<AllocatorExtent>>>),
    /// The vector is mutated only by one metadata-shard mailbox. ArcSwap keeps
    /// CXL logical-segment aliases cheap without an allocation-path mutex.
    ShardLocal(Arc<ArcSwap<Vec<AllocatorExtent>>>),
}

/// One independently allocatable range whose ownership may move between
/// metadata shards only while the range is completely empty.
#[derive(Clone)]
struct AllocatorExtent {
    allocator: ByteAllocator,
    owner: usize,
}

pub(crate) struct TransferableExtent {
    extent: AllocatorExtent,
}

impl ShardedByteAllocator {
    fn new(
        capacity: u64,
        shard_count: usize,
        owned_shard: Option<usize>,
        max_allocator_nodes: u32,
    ) -> Self {
        debug_assert_ne!(shard_count, 0);
        let shard_count_u32 = u32::try_from(shard_count).expect("allocator shard count fits u32");
        let base_nodes = max_allocator_nodes / shard_count_u32;
        let node_remainder = max_allocator_nodes % shard_count_u32;
        let shard_count = u64::from(shard_count_u32);
        let base_capacity = capacity / shard_count;
        let remainder = capacity % shard_count;
        let extents = (0..shard_count)
            .filter(|index| owned_shard.is_none_or(|owned| owned == *index as usize))
            .map(|index| {
                let arena_capacity = base_capacity + u64::from(index < remainder);
                let arena_nodes = base_nodes + u32::from(index < u64::from(node_remainder));
                let base = index
                    .checked_mul(base_capacity)
                    .and_then(|base| base.checked_add(index.min(remainder)))
                    .expect("allocator shard ranges fit the resource");
                let allocator = ByteAllocator::new_at(base, arena_capacity, arena_nodes);
                AllocatorExtent {
                    allocator,
                    owner: usize::try_from(index).expect("allocator extent index fits usize"),
                }
            })
            .collect();
        let extents = match owned_shard {
            Some(_) => ExtentStore::ShardLocal(Arc::new(ArcSwap::from_pointee(extents))),
            None => ExtentStore::Concurrent(Arc::new(Mutex::new(extents))),
        };
        Self { extents }
    }

    fn may_satisfy(&self, shard: usize, bytes: u64) -> bool {
        match &self.extents {
            ExtentStore::Concurrent(extents) => extents
                .lock()
                .iter()
                .any(|extent| extent.owner == shard && extent.allocator.may_satisfy(bytes)),
            ExtentStore::ShardLocal(extents) => extents
                .load()
                .iter()
                .any(|extent| extent.owner == shard && extent.allocator.may_satisfy(bytes)),
        }
    }

    fn allocate(
        &self,
        shard: usize,
        bytes: u64,
    ) -> Option<crate::segment::offset_allocator::OffsetAllocationHandle> {
        self.allocate_owned(shard, bytes)
    }

    fn allocate_owned(
        &self,
        shard: usize,
        bytes: u64,
    ) -> Option<crate::segment::offset_allocator::OffsetAllocationHandle> {
        match &self.extents {
            ExtentStore::Concurrent(extents) => extents
                .lock()
                .iter()
                .find_map(|extent| extent.allocate(shard, bytes)),
            ExtentStore::ShardLocal(extents) => extents
                .load()
                .iter()
                .find_map(|extent| extent.allocate(shard, bytes)),
        }
    }

    fn stats_for_shard(&self, shard: usize) -> SegmentSpaceStats {
        match &self.extents {
            ExtentStore::Concurrent(extents) => {
                allocator_stats(extents.lock().iter().filter(|extent| extent.owner == shard))
            }
            ExtentStore::ShardLocal(extents) => {
                allocator_stats(extents.load().iter().filter(|extent| extent.owner == shard))
            }
        }
    }

    fn stats(&self) -> SegmentSpaceStats {
        match &self.extents {
            ExtentStore::Concurrent(extents) => allocator_stats(extents.lock().iter()),
            ExtentStore::ShardLocal(extents) => allocator_stats(extents.load().iter()),
        }
    }

    fn take_empty_extent(&self, shard: usize, minimum_bytes: u64) -> Option<TransferableExtent> {
        match &self.extents {
            ExtentStore::Concurrent(extents) => {
                let mut extents = extents.lock();
                let position = empty_extent_position(&extents, shard, minimum_bytes)?;
                Some(TransferableExtent {
                    extent: extents.remove(position),
                })
            }
            ExtentStore::ShardLocal(extents) => {
                let current = extents.load_full();
                let position = empty_extent_position(&current, shard, minimum_bytes)?;
                let mut next = current.as_ref().clone();
                let extent = next.remove(position);
                extents.store(Arc::new(next));
                Some(TransferableExtent { extent })
            }
        }
    }

    fn add_extent(&self, shard: usize, mut extent: TransferableExtent) {
        extent.extent.owner = shard;
        match &self.extents {
            ExtentStore::Concurrent(extents) => extents.lock().push(extent.extent),
            ExtentStore::ShardLocal(extents) => {
                let current = extents.load_full();
                let mut next = current.as_ref().clone();
                next.push(extent.extent);
                extents.store(Arc::new(next));
            }
        }
    }
}

fn allocator_stats<'a>(
    extents: impl IntoIterator<Item = &'a AllocatorExtent>,
) -> SegmentSpaceStats {
    extents
        .into_iter()
        .fold(empty_space_stats(), |total, extent| {
            add_allocator_stats(total, extent.allocator.stats())
        })
}

fn empty_extent_position(
    extents: &[AllocatorExtent],
    shard: usize,
    minimum_bytes: u64,
) -> Option<usize> {
    extents.iter().position(|extent| {
        extent.owner == shard
            && extent.allocator.may_satisfy(minimum_bytes)
            && extent.allocator.stats().live_allocations == 0
    })
}

impl AllocatorExtent {
    fn allocate(
        &self,
        shard: usize,
        bytes: u64,
    ) -> Option<crate::segment::offset_allocator::OffsetAllocationHandle> {
        if self.owner != shard || !self.allocator.may_satisfy(bytes) {
            return None;
        }
        self.allocator.allocate_after_precheck(bytes)
    }
}

const fn empty_space_stats() -> SegmentSpaceStats {
    SegmentSpaceStats {
        capacity_bytes: 0,
        used_bytes: 0,
        available_bytes: 0,
        largest_free_region_bytes: 0,
    }
}

fn add_allocator_stats(
    mut total: SegmentSpaceStats,
    stats: crate::segment::offset_allocator::AllocatorStats,
) -> SegmentSpaceStats {
    total.capacity_bytes = total.capacity_bytes.saturating_add(stats.capacity);
    total.used_bytes = total.used_bytes.saturating_add(stats.used_bytes);
    total.available_bytes = total.available_bytes.saturating_add(stats.available_bytes);
    total.largest_free_region_bytes = total
        .largest_free_region_bytes
        .max(stats.largest_free_region);
    total
}

impl ResourceRegistry {
    pub(super) fn mount<'a>(
        &mut self,
        spec: &SegmentSpec,
        existing: impl IntoIterator<Item = &'a SegmentSpec>,
        max_allocator_nodes: u32,
        allocator_shards: usize,
        allocator_shard_index: Option<usize>,
    ) -> Result<MountedResource, AttachError> {
        validate_resource_conflicts(spec, existing)?;
        match spec.configuration() {
            SegmentConfiguration::Memory { region, .. }
            | SegmentConfiguration::Nof { region, .. } => {
                Ok(MountedResource::Range(ShardedByteAllocator::new(
                    region.size(),
                    allocator_shards,
                    allocator_shard_index,
                    max_allocator_nodes,
                )))
            }
            SegmentConfiguration::LocalSsd {
                initial_offload_enabled,
            } => Ok(MountedResource::LocalSsd(LocalSsdCapacity::new(
                *initial_offload_enabled,
            ))),
            SegmentConfiguration::Cxl { arena, .. } => self.mount_cxl(
                arena,
                allocator_shards,
                allocator_shard_index,
                max_allocator_nodes,
            ),
        }
    }

    pub(super) fn unmount(&mut self, spec: &SegmentSpec) {
        // This drops only catalog ownership. Outstanding allocation/lease
        // handles retain their Arc-backed allocator state until RAII cleanup.
        let Some(arena) = spec.cxl_arena() else {
            return;
        };
        let remove = {
            let resource = self
                .cxl_arenas
                .get_mut(arena.id())
                .expect("mounted CXL segments retain a registered arena");
            resource.attached_segments = resource
                .attached_segments
                .checked_sub(1)
                .expect("CXL arena attachment accounting remains balanced");
            resource.attached_segments == 0
        };
        if remove {
            self.cxl_arenas.remove(arena.id());
        }
    }

    fn mount_cxl(
        &mut self,
        arena: &CxlArenaSpec,
        allocator_shards: usize,
        allocator_shard_index: Option<usize>,
        max_allocator_nodes: u32,
    ) -> Result<MountedResource, AttachError> {
        if let Some(resource) = self.cxl_arenas.get_mut(arena.id()) {
            if &resource.spec != arena {
                return Err(AttachError::ConflictingCxlArena {
                    arena: arena.id().clone(),
                });
            }
            resource.attached_segments += 1;
            return Ok(MountedResource::Range(resource.allocator.clone()));
        }

        let allocator = ShardedByteAllocator::new(
            arena.capacity_bytes(),
            allocator_shards,
            allocator_shard_index,
            max_allocator_nodes,
        );
        self.cxl_arenas.insert(
            arena.id().clone(),
            CxlArenaResource {
                spec: arena.clone(),
                allocator: allocator.clone(),
                attached_segments: 1,
            },
        );
        Ok(MountedResource::Range(allocator))
    }
}

impl MountedResource {
    pub(super) const fn supports_direct_reservation(&self) -> bool {
        matches!(self, Self::Range(_))
    }

    pub(super) const fn supports_offload(&self) -> bool {
        matches!(self, Self::LocalSsd(_))
    }

    pub(super) fn may_satisfy(&self, allocator_shard: usize, bytes: u64) -> bool {
        match self {
            Self::Range(allocator) => allocator.may_satisfy(allocator_shard, bytes),
            Self::LocalSsd(_) => false,
        }
    }

    pub(super) fn candidate_capability(
        &self,
        replica_class: ReplicaClass,
    ) -> Option<CandidateCapability> {
        match self {
            Self::Range(_) => Some(CandidateCapability::Direct(replica_class)),
            Self::LocalSsd(capacity) if capacity.offload_enabled() => {
                Some(CandidateCapability::Offload)
            }
            Self::LocalSsd(_) => None,
        }
    }

    pub(super) fn reserve(
        &self,
        segment: Arc<SegmentSpec>,
        segment_lease: SegmentLease,
        allocator_shard: usize,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        let id = segment.identity().id();
        let Self::Range(allocator) = self else {
            return Err(ReserveError::NotDirectlyAllocatable(id));
        };
        let allocation = allocator
            .allocate(allocator_shard, bytes)
            .ok_or(ReserveError::OutOfSpace(id))?;
        let buffer_address = segment
            .direct_region()
            .expect("direct resources retain a byte range")
            .base()
            .checked_add(allocation.offset())
            .ok_or(ReserveError::AddressOverflow(id))?;

        Ok(Reservation {
            allocation,
            segment,
            segment_lease,
            region: MemoryRegion::new(buffer_address, bytes),
        })
    }

    pub(super) fn report_capacity(
        &self,
        segment: crate::segment::SegmentId,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        let Self::LocalSsd(capacity) = self else {
            return Err(LocalSsdError::NotLocalSsd(segment));
        };
        capacity.report(capacity_bytes);
        Ok(())
    }

    pub(super) fn set_offload_enabled(
        &self,
        segment: crate::segment::SegmentId,
        enabled: bool,
    ) -> Result<(), LocalSsdError> {
        let Self::LocalSsd(capacity) = self else {
            return Err(LocalSsdError::NotLocalSsd(segment));
        };
        capacity.set_offload_enabled(enabled);
        Ok(())
    }

    pub(super) fn admit_offload(
        &self,
        segment: Arc<SegmentSpec>,
        segment_lease: SegmentLease,
        bytes: u64,
    ) -> Result<OffloadPermit, LocalSsdError> {
        let id = segment.identity().id();
        let Self::LocalSsd(capacity) = self else {
            return Err(LocalSsdError::NotLocalSsd(id));
        };
        let allocation = capacity.admit(bytes).map_err(|error| match error {
            AdmissionFailure::OffloadDisabled => LocalSsdError::OffloadDisabled(id),
            AdmissionFailure::CapacityNotReported => LocalSsdError::CapacityNotReported(id),
            AdmissionFailure::OutOfSpace => LocalSsdError::OutOfSpace(id),
        })?;
        Ok(OffloadPermit::new(allocation, segment_lease, segment))
    }

    pub(super) fn local_ssd_stats(&self) -> Option<LocalSsdStats> {
        match self {
            Self::Range(_) => None,
            Self::LocalSsd(capacity) => Some(capacity.stats()),
        }
    }

    pub(super) fn space_stats(&self) -> SegmentSpaceStats {
        match self {
            Self::Range(allocator) => allocator.stats(),
            Self::LocalSsd(capacity) => {
                let stats = capacity.stats();
                SegmentSpaceStats {
                    capacity_bytes: stats.capacity_bytes,
                    used_bytes: stats.admitted_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: 0,
                }
            }
        }
    }

    pub(super) fn space_stats_for_shard(&self, allocator_shard: usize) -> SegmentSpaceStats {
        match self {
            Self::Range(allocator) => allocator.stats_for_shard(allocator_shard),
            Self::LocalSsd(capacity) => {
                let stats = capacity.stats();
                SegmentSpaceStats {
                    capacity_bytes: stats.capacity_bytes,
                    used_bytes: stats.admitted_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: 0,
                }
            }
        }
    }

    pub(super) fn take_empty_extent(
        &self,
        allocator_shard: usize,
        minimum_bytes: u64,
    ) -> Option<TransferableExtent> {
        match self {
            Self::Range(allocator) => allocator.take_empty_extent(allocator_shard, minimum_bytes),
            Self::LocalSsd(_) => None,
        }
    }

    pub(super) fn add_extent(&self, allocator_shard: usize, extent: TransferableExtent) -> bool {
        match self {
            Self::Range(allocator) => {
                allocator.add_extent(allocator_shard, extent);
                true
            }
            Self::LocalSsd(_) => false,
        }
    }
}

fn validate_resource_conflicts<'a>(
    spec: &SegmentSpec,
    existing: impl IntoIterator<Item = &'a SegmentSpec>,
) -> Result<(), AttachError> {
    match spec.configuration() {
        SegmentConfiguration::Memory { region, transport } => {
            let end = region.end().ok_or(AttachError::AddressOverflow)?;
            if let Some(existing) = existing.into_iter().find_map(|existing| {
                let SegmentConfiguration::Memory {
                    region: existing_region,
                    transport: existing_transport,
                } = existing.configuration()
                else {
                    return None;
                };
                (existing.identity().owner() == spec.identity().owner()
                    && existing_transport == transport
                    && region.base()
                        < existing_region
                            .end()
                            .expect("mounted memory ranges are valid")
                    && existing_region.base() < end)
                    .then(|| existing.identity().id())
            }) {
                return Err(AttachError::OverlappingAddressRange { existing });
            }
        }
        SegmentConfiguration::Nof { transport, .. } => {
            if let Some(existing) = existing.into_iter().find_map(|existing| {
                let SegmentConfiguration::Nof {
                    transport: existing_transport,
                    ..
                } = existing.configuration()
                else {
                    return None;
                };
                (existing_transport == transport).then(|| existing.identity().id())
            }) {
                return Err(AttachError::DuplicateNofEndpoint { existing });
            }
        }
        SegmentConfiguration::LocalSsd { .. } => {
            if let Some(existing) = existing.into_iter().find_map(|existing| {
                matches!(
                    existing.configuration(),
                    SegmentConfiguration::LocalSsd { .. }
                )
                .then(|| existing.identity())
                .filter(|identity| identity.owner() == spec.identity().owner())
                .map(|identity| identity.id())
            }) {
                return Err(AttachError::DuplicateLocalSsdOwner { existing });
            }
        }
        SegmentConfiguration::Cxl { .. } => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ExtentStore, ShardedByteAllocator};

    #[test]
    fn complete_layout_keeps_extents_disjoint() {
        let allocator = ShardedByteAllocator::new(4096, 2, None, 128);
        assert_eq!(allocator.stats_for_shard(0).capacity_bytes, 2048);
        assert_eq!(allocator.stats_for_shard(1).capacity_bytes, 2048);

        let first = allocator.allocate(0, 2048).unwrap();
        let second = allocator.allocate(1, 1024).unwrap();
        assert_eq!(first.offset(), 0);
        assert_eq!(second.offset(), 2048);

        drop(second);
        drop(first);
        assert_eq!(allocator.stats().available_bytes, 4096);
    }

    #[test]
    fn local_layout_materializes_only_its_owned_extent() {
        let allocator = ShardedByteAllocator::new(4096, 2, Some(1), 128);
        assert!(matches!(&allocator.extents, ExtentStore::ShardLocal(_)));
        assert_eq!(allocator.stats_for_shard(0).capacity_bytes, 0);
        assert_eq!(allocator.stats_for_shard(1).capacity_bytes, 2048);
        assert!(allocator.allocate(0, 1).is_none());

        let allocation = allocator.allocate(1, 2048).unwrap();
        assert_eq!(allocation.offset(), 2048);
        drop(allocation);
        assert_eq!(allocator.stats().available_bytes, 2048);
    }
}
