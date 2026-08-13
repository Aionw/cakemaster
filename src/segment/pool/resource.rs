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
    Range(ByteAllocator),
    LocalSsd(LocalSsdCapacity),
}

#[derive(Default)]
pub(super) struct ResourceRegistry {
    cxl_arenas: HashMap<CxlArenaId, CxlArenaResource>,
}

struct CxlArenaResource {
    spec: CxlArenaSpec,
    allocator: ByteAllocator,
    attached_segments: usize,
}

impl ResourceRegistry {
    pub(super) fn mount<'a>(
        &mut self,
        spec: &SegmentSpec,
        existing: impl IntoIterator<Item = &'a SegmentSpec>,
        max_allocator_nodes: u32,
    ) -> Result<MountedResource, AttachError> {
        validate_resource_conflicts(spec, existing)?;
        match spec.configuration() {
            SegmentConfiguration::Memory { region, .. }
            | SegmentConfiguration::Nof { region, .. } => Ok(MountedResource::Range(
                ByteAllocator::new(region.size(), max_allocator_nodes),
            )),
            SegmentConfiguration::LocalSsd {
                initial_offload_enabled,
            } => Ok(MountedResource::LocalSsd(LocalSsdCapacity::new(
                *initial_offload_enabled,
            ))),
            SegmentConfiguration::Cxl { arena, .. } => self.mount_cxl(arena, max_allocator_nodes),
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

        let allocator = ByteAllocator::new(arena.capacity_bytes(), max_allocator_nodes);
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

    pub(super) fn may_satisfy(&self, bytes: u64) -> bool {
        match self {
            Self::Range(allocator) => allocator.may_satisfy(bytes),
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
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        let id = segment.identity().id();
        let Self::Range(allocator) = self else {
            return Err(ReserveError::NotDirectlyAllocatable(id));
        };
        let allocation = allocator
            .allocate_after_precheck(bytes)
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
            Self::Range(allocator) => {
                let stats = allocator.stats();
                SegmentSpaceStats {
                    capacity_bytes: stats.capacity,
                    used_bytes: stats.used_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: stats.largest_free_region,
                }
            }
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
