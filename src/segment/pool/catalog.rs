use super::{
    AttachOutcome, DirectCandidate, OffloadSnapshot, OffloadTarget, PoolSnapshot, SegmentHandle,
};
use crate::segment::descriptor::MemoryRegion;
use crate::segment::error::{AttachError, LocalSsdError, ReserveError, SegmentStateError};
use crate::segment::identity::{ClientId, SegmentId};
use crate::segment::local_ssd::{AdmissionFailure, LocalSsdCapacity, LocalSsdStats, OffloadPermit};
use crate::segment::offset_allocator::ByteAllocator;
use crate::segment::reservation::Reservation;
use crate::segment::spec::{
    CxlArenaId, CxlArenaSpec, ReplicaClass, SegmentConfiguration, SegmentSpec,
};
use crate::segment::stats::{SegmentSpaceStats, SegmentState, SegmentStats, SegmentUsageStats};
use crate::segment::usage::UsageTracker;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Catalog {
    pool_id: u64,
    segments: HashMap<SegmentId, Arc<SegmentEntry>>,
    resources: ResourceRegistry,
    indexes: CandidateIndexes,
}

#[derive(Default)]
struct ResourceRegistry {
    cxl_arenas: HashMap<CxlArenaId, CxlArenaResource>,
}

struct CxlArenaResource {
    spec: CxlArenaSpec,
    capacity: ByteAllocator,
    attached_segments: usize,
}

#[derive(Default)]
struct CandidateIndexes {
    direct_generation: u64,
    direct: HashMap<ReplicaClass, Arc<[DirectCandidate]>>,
    offload_generation: u64,
    offload: Arc<[OffloadTarget]>,
}

pub(super) struct SegmentEntry {
    spec: Arc<SegmentSpec>,
    state: Mutex<SegmentState>,
    capacity: CapacityHandle,
    usage: UsageTracker,
}

enum CapacityHandle {
    Range(ByteAllocator),
    LocalSsd(LocalSsdCapacity),
}

#[derive(Clone, Copy)]
enum CandidateCapability {
    Direct(ReplicaClass),
    Offload,
}

impl Catalog {
    pub(super) fn new(pool_id: u64) -> Self {
        Self {
            pool_id,
            segments: HashMap::new(),
            resources: ResourceRegistry::default(),
            indexes: CandidateIndexes::default(),
        }
    }

    pub(super) fn attach(
        &mut self,
        spec: SegmentSpec,
        max_allocator_nodes: u32,
    ) -> Result<AttachOutcome, AttachError> {
        if let Some(existing) = self.segment(spec.identity().id()) {
            return if existing.spec() == &spec {
                Ok(AttachOutcome::AlreadyAttached(existing))
            } else {
                Err(AttachError::ConflictingSegmentId(spec.identity().id()))
            };
        }

        self.validate_resource_conflicts(&spec)?;
        let capacity = self.resources.bind(&spec, max_allocator_nodes)?;
        let entry = Arc::new(SegmentEntry::new(Arc::new(spec), capacity));
        let segment = self.handle(entry.clone());
        self.segments.insert(segment.id(), entry);
        self.rebuild_indexes();
        Ok(AttachOutcome::Attached(segment))
    }

    pub(super) fn snapshot(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        PoolSnapshot {
            generation: self.indexes.direct_generation,
            replica_class,
            candidates: self
                .indexes
                .direct
                .get(&replica_class)
                .cloned()
                .unwrap_or_else(|| Arc::from([])),
        }
    }

    pub(super) fn offload_snapshot(&self) -> OffloadSnapshot {
        OffloadSnapshot {
            generation: self.indexes.offload_generation,
            targets: self.indexes.offload.clone(),
        }
    }

    pub(super) fn segment(&self, id: SegmentId) -> Option<SegmentHandle> {
        self.segments
            .get(&id)
            .cloned()
            .map(|entry| self.handle(entry))
    }

    pub(super) fn len(&self) -> usize {
        self.segments.len()
    }

    pub(super) fn report_local_ssd_capacity(
        &self,
        owner: ClientId,
        id: SegmentId,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        self.owned_local_ssd_entry(owner, id)?
            .report_local_ssd_capacity(capacity_bytes)
    }

    pub(super) fn set_local_ssd_offload_enabled(
        &mut self,
        owner: ClientId,
        id: SegmentId,
        enabled: bool,
    ) -> Result<(), LocalSsdError> {
        self.owned_local_ssd_entry(owner, id)?
            .set_local_ssd_offload_enabled(enabled)?;
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn quiesce(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        self.owned_entry(owner, id)?.quiesce();
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn reactivate(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        self.owned_entry(owner, id)?.reactivate();
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn remove(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        let entry = self.owned_entry(owner, id)?;
        entry.prepare_remove()?;
        let removed = self
            .segments
            .remove(&id)
            .expect("owned entries remain registered while the catalog is write-locked");
        self.resources.unbind(removed.spec());
        self.rebuild_indexes();
        Ok(())
    }

    fn handle(&self, entry: Arc<SegmentEntry>) -> SegmentHandle {
        SegmentHandle {
            pool_id: self.pool_id,
            entry,
        }
    }

    fn owned_entry(
        &self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<Arc<SegmentEntry>, SegmentStateError> {
        let entry = self
            .segments
            .get(&id)
            .cloned()
            .ok_or(SegmentStateError::NotFound(id))?;
        if entry.spec.identity().owner() != owner {
            return Err(SegmentStateError::OwnerMismatch {
                segment: id,
                expected: entry.spec.identity().owner(),
                actual: owner,
            });
        }
        Ok(entry)
    }

    fn owned_local_ssd_entry(
        &self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<Arc<SegmentEntry>, LocalSsdError> {
        let entry = self
            .segments
            .get(&id)
            .cloned()
            .ok_or(LocalSsdError::NotFound(id))?;
        let expected = entry.spec.identity().owner();
        if expected != owner {
            return Err(LocalSsdError::OwnerMismatch {
                segment: id,
                expected,
                actual: owner,
            });
        }
        if !entry.is_local_ssd_capacity() {
            return Err(LocalSsdError::NotLocalSsd(id));
        }
        Ok(entry)
    }

    fn validate_resource_conflicts(&self, spec: &SegmentSpec) -> Result<(), AttachError> {
        match spec.configuration() {
            SegmentConfiguration::Memory { region, transport } => {
                let end = region.end().ok_or(AttachError::AddressOverflow)?;
                if let Some(existing) = self.segments.values().find_map(|entry| {
                    let SegmentConfiguration::Memory {
                        region: existing_region,
                        transport: existing_transport,
                    } = entry.spec.configuration()
                    else {
                        return None;
                    };
                    (entry.spec.identity().owner() == spec.identity().owner()
                        && existing_transport == transport
                        && region.base()
                            < existing_region.end().expect("attached segments are valid")
                        && existing_region.base() < end)
                        .then(|| entry.spec.identity().id())
                }) {
                    return Err(AttachError::OverlappingAddressRange { existing });
                }
            }
            SegmentConfiguration::Nof { transport, .. } => {
                if let Some(existing) = self.segments.values().find_map(|entry| {
                    let SegmentConfiguration::Nof {
                        transport: existing_transport,
                        ..
                    } = entry.spec.configuration()
                    else {
                        return None;
                    };
                    (existing_transport == transport).then(|| entry.spec.identity().id())
                }) {
                    return Err(AttachError::DuplicateNofEndpoint { existing });
                }
            }
            SegmentConfiguration::LocalSsd { .. } => {
                if let Some(existing) = self.segments.values().find_map(|entry| {
                    matches!(
                        entry.spec.configuration(),
                        SegmentConfiguration::LocalSsd { .. }
                    )
                    .then(|| entry.spec.identity())
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

    fn rebuild_indexes(&mut self) {
        let mut direct: HashMap<_, Vec<_>> = HashMap::new();
        let mut offload = Vec::new();

        for entry in self.segments.values() {
            let Some(capability) = entry.candidate_capability() else {
                continue;
            };
            let segment = self.handle(entry.clone());
            match capability {
                CandidateCapability::Direct(replica_class) => direct
                    .entry(replica_class)
                    .or_default()
                    .push(DirectCandidate { segment }),
                CandidateCapability::Offload => offload.push(OffloadTarget { segment }),
            }
        }

        for candidates in direct.values_mut() {
            candidates.sort_unstable_by_key(|candidate| candidate.id());
        }
        offload.sort_unstable_by_key(|target| target.id());

        if !same_direct_index(&direct, &self.indexes.direct) {
            self.indexes.direct = direct
                .into_iter()
                .map(|(class, candidates)| (class, Arc::from(candidates)))
                .collect();
            self.indexes.direct_generation = self.indexes.direct_generation.wrapping_add(1);
        }
        if !same_offload_index(&offload, &self.indexes.offload) {
            self.indexes.offload = Arc::from(offload);
            self.indexes.offload_generation = self.indexes.offload_generation.wrapping_add(1);
        }
    }
}

impl ResourceRegistry {
    fn bind(
        &mut self,
        spec: &SegmentSpec,
        max_allocator_nodes: u32,
    ) -> Result<CapacityHandle, AttachError> {
        match spec.configuration() {
            SegmentConfiguration::Cxl { arena, .. } => {
                if let Some(resource) = self.cxl_arenas.get_mut(arena.id()) {
                    if &resource.spec != arena {
                        return Err(AttachError::ConflictingCxlArena {
                            arena: arena.id().clone(),
                        });
                    }
                    resource.attached_segments += 1;
                    return Ok(CapacityHandle::Range(resource.capacity.clone()));
                }

                let capacity = ByteAllocator::new(arena.capacity_bytes(), max_allocator_nodes);
                self.cxl_arenas.insert(
                    arena.id().clone(),
                    CxlArenaResource {
                        spec: arena.clone(),
                        capacity: capacity.clone(),
                        attached_segments: 1,
                    },
                );
                Ok(CapacityHandle::Range(capacity))
            }
            SegmentConfiguration::LocalSsd {
                initial_offload_enabled,
            } => Ok(CapacityHandle::LocalSsd(LocalSsdCapacity::new(
                *initial_offload_enabled,
            ))),
            SegmentConfiguration::Memory { region, .. }
            | SegmentConfiguration::Nof { region, .. } => Ok(CapacityHandle::Range(
                ByteAllocator::new(region.size(), max_allocator_nodes),
            )),
        }
    }

    fn unbind(&mut self, spec: &SegmentSpec) {
        let Some(arena) = spec.cxl_arena() else {
            return;
        };
        let remove = {
            let resource = self
                .cxl_arenas
                .get_mut(arena.id())
                .expect("attached CXL segments retain a registered arena");
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
}

impl SegmentEntry {
    fn new(spec: Arc<SegmentSpec>, capacity: CapacityHandle) -> Self {
        Self {
            spec,
            state: Mutex::new(SegmentState::Accepting),
            capacity,
            usage: UsageTracker::new(),
        }
    }

    pub(super) fn spec(&self) -> &SegmentSpec {
        &self.spec
    }

    pub(super) const fn is_range_capacity(&self) -> bool {
        matches!(&self.capacity, CapacityHandle::Range(_))
    }

    pub(super) const fn is_local_ssd_capacity(&self) -> bool {
        matches!(&self.capacity, CapacityHandle::LocalSsd(_))
    }

    fn candidate_capability(&self) -> Option<CandidateCapability> {
        if !self.state.lock().is_accepting() {
            return None;
        }
        match &self.capacity {
            CapacityHandle::Range(_) => {
                Some(CandidateCapability::Direct(self.spec.replica_class()))
            }
            CapacityHandle::LocalSsd(capacity) if capacity.offload_enabled() => {
                Some(CandidateCapability::Offload)
            }
            CapacityHandle::LocalSsd(_) => None,
        }
    }

    #[inline]
    pub(super) fn reserve(&self, bytes: u64) -> Result<Reservation, ReserveError> {
        let CapacityHandle::Range(capacity) = &self.capacity else {
            return Err(ReserveError::NotDirectlyAllocatable(
                self.spec.identity().id(),
            ));
        };
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        if !capacity.may_satisfy(bytes) {
            return Err(ReserveError::OutOfSpace(self.spec.identity().id()));
        }

        let state = self.state.lock();
        if !state.is_accepting() {
            return Err(ReserveError::NotAccepting(self.spec.identity().id()));
        }
        let allocation = capacity
            .allocate_after_precheck(bytes)
            .ok_or(ReserveError::OutOfSpace(self.spec.identity().id()))?;
        let buffer_address = self
            .spec
            .direct_region()
            .expect("range capacity retains a direct byte range")
            .base()
            .checked_add(allocation.offset())
            .ok_or(ReserveError::AddressOverflow(self.spec.identity().id()))?;
        let usage = self.usage.acquire();
        drop(state);

        Ok(Reservation {
            allocation,
            segment: self.spec.clone(),
            _usage: usage,
            region: MemoryRegion::new(buffer_address, bytes),
        })
    }

    pub(super) fn report_local_ssd_capacity(
        &self,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        let CapacityHandle::LocalSsd(capacity) = &self.capacity else {
            return Err(LocalSsdError::NotLocalSsd(self.spec.identity().id()));
        };
        capacity.report(capacity_bytes);
        Ok(())
    }

    pub(super) fn set_local_ssd_offload_enabled(&self, enabled: bool) -> Result<(), LocalSsdError> {
        let CapacityHandle::LocalSsd(capacity) = &self.capacity else {
            return Err(LocalSsdError::NotLocalSsd(self.spec.identity().id()));
        };
        capacity.set_offload_enabled(enabled);
        Ok(())
    }

    pub(super) fn admit_offload(&self, bytes: u64) -> Result<OffloadPermit, LocalSsdError> {
        let id = self.spec.identity().id();
        let CapacityHandle::LocalSsd(capacity) = &self.capacity else {
            return Err(LocalSsdError::NotLocalSsd(id));
        };
        if bytes == 0 {
            return Err(LocalSsdError::ZeroSize);
        }

        let state = self.state.lock();
        if !state.is_accepting() {
            return Err(LocalSsdError::NotAccepting(id));
        }
        let allocation = capacity.admit(bytes).map_err(|error| match error {
            AdmissionFailure::OffloadDisabled => LocalSsdError::OffloadDisabled(id),
            AdmissionFailure::CapacityNotReported => LocalSsdError::CapacityNotReported(id),
            AdmissionFailure::OutOfSpace => LocalSsdError::OutOfSpace(id),
        })?;
        let permit = OffloadPermit::new(allocation, self.usage.acquire(), self.spec.clone());
        drop(state);
        Ok(permit)
    }

    pub(super) fn local_ssd_stats(&self) -> Option<LocalSsdStats> {
        match &self.capacity {
            CapacityHandle::Range(_) => None,
            CapacityHandle::LocalSsd(capacity) => Some(capacity.stats()),
        }
    }

    fn quiesce(&self) {
        let mut state = self.state.lock();
        if state.is_accepting() {
            *state = SegmentState::Quiesced;
        }
    }

    fn reactivate(&self) {
        let mut state = self.state.lock();
        if *state == SegmentState::Quiesced {
            *state = SegmentState::Accepting;
        }
    }

    fn prepare_remove(&self) -> Result<(), SegmentStateError> {
        let mut state = self.state.lock();
        let active_allocations = self.usage.active_allocations();
        match *state {
            SegmentState::Accepting => {
                Err(SegmentStateError::StillAccepting(self.spec.identity().id()))
            }
            SegmentState::Quiesced if active_allocations != 0 => Err(SegmentStateError::Busy {
                segment: self.spec.identity().id(),
                active_allocations,
            }),
            SegmentState::Quiesced => {
                *state = SegmentState::Removed;
                Ok(())
            }
            SegmentState::Removed => Ok(()),
        }
    }

    pub(super) fn stats(&self) -> SegmentStats {
        let state = *self.state.lock();
        let space = match &self.capacity {
            CapacityHandle::Range(capacity) => {
                let stats = capacity.stats();
                SegmentSpaceStats {
                    capacity_bytes: stats.capacity,
                    used_bytes: stats.used_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: stats.largest_free_region,
                }
            }
            CapacityHandle::LocalSsd(capacity) => {
                let stats = capacity.stats();
                SegmentSpaceStats {
                    capacity_bytes: stats.capacity_bytes,
                    used_bytes: stats.admitted_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: 0,
                }
            }
        };
        SegmentStats {
            space,
            usage: SegmentUsageStats {
                active_allocations: self.usage.active_allocations(),
            },
            state,
        }
    }
}

fn same_direct_index(
    left: &HashMap<ReplicaClass, Vec<DirectCandidate>>,
    right: &HashMap<ReplicaClass, Arc<[DirectCandidate]>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(class, candidates)| {
            right.get(class).is_some_and(|current| {
                candidates.len() == current.len()
                    && candidates
                        .iter()
                        .zip(current.iter())
                        .all(|(left, right)| Arc::ptr_eq(&left.entry, &right.entry))
            })
        })
}

fn same_offload_index(left: &[OffloadTarget], right: &[OffloadTarget]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| Arc::ptr_eq(&left.entry, &right.entry))
}
