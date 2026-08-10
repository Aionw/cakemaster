use super::{AttachOutcome, PoolSnapshot, SegmentCandidate};
use crate::segment::descriptor::MemoryRegion;
use crate::segment::error::{AttachError, LifecycleError, ReserveError};
use crate::segment::identity::{ClientId, SegmentId};
use crate::segment::offset_allocator::ByteAllocator;
use crate::segment::reservation::{Reservation, ReservationCounter};
use crate::segment::spec::{CxlArenaId, CxlArenaSpec, ReplicaClass, SegmentSpec};
use crate::segment::stats::{
    SegmentReservationStats, SegmentSpaceStats, SegmentState, SegmentStats,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(super) struct Catalog {
    pool_id: u64,
    generation: u64,
    segments: HashMap<SegmentId, Arc<Segment>>,
    cxl_arenas: HashMap<CxlArenaId, CxlArena>,
    allocatable: HashMap<ReplicaClass, Arc<[SegmentCandidate]>>,
}

struct CxlArena {
    spec: CxlArenaSpec,
    allocator: ByteAllocator,
}

pub(super) struct Segment {
    pool_id: u64,
    spec: Arc<SegmentSpec>,
    phase: Mutex<SegmentState>,
    allocator: ByteAllocator,
    live_reservations: Arc<AtomicU64>,
    accepting: AtomicBool,
}

struct CatalogMutation<'a> {
    catalog: &'a mut Catalog,
}

impl Catalog {
    pub(super) fn new(pool_id: u64) -> Self {
        Self {
            pool_id,
            generation: 0,
            segments: HashMap::new(),
            cxl_arenas: HashMap::new(),
            allocatable: HashMap::new(),
        }
    }

    pub(super) fn attach(
        &mut self,
        spec: SegmentSpec,
        max_allocator_nodes: u32,
    ) -> Result<AttachOutcome, AttachError> {
        if let Some(existing) = self.candidate(spec.identity().id()) {
            return if existing.spec() == &spec {
                Ok(AttachOutcome::AlreadyAttached(existing))
            } else {
                Err(AttachError::ConflictingSegmentId(spec.identity().id()))
            };
        }

        self.validate_resource_conflicts(&spec)?;

        let allocator = self.allocator_for(&spec, max_allocator_nodes)?;
        let candidate = self.mutation().insert(spec, allocator);
        Ok(AttachOutcome::Attached(candidate))
    }

    pub(super) fn snapshot(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        PoolSnapshot {
            generation: self.generation,
            replica_class,
            candidates: self
                .allocatable
                .get(&replica_class)
                .cloned()
                .unwrap_or_else(|| Arc::from([])),
        }
    }

    pub(super) fn candidate(&self, id: SegmentId) -> Option<SegmentCandidate> {
        self.segments.get(&id).map(|segment| SegmentCandidate {
            pool_id: self.pool_id,
            segment: segment.clone(),
        })
    }

    pub(super) fn len(&self) -> usize {
        self.segments.len()
    }

    pub(super) fn quiesce(&mut self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        self.mutation().quiesce(owner, id)
    }

    pub(super) fn reactivate(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), LifecycleError> {
        self.mutation().reactivate(owner, id)
    }

    pub(super) fn remove(&mut self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        self.mutation().remove(owner, id)
    }

    fn validate_resource_conflicts(&self, spec: &SegmentSpec) -> Result<(), AttachError> {
        match spec {
            SegmentSpec::Memory(spec) => {
                let end = spec.region().end().ok_or(AttachError::AddressOverflow)?;
                if let Some(existing) = self.segments.values().find_map(|segment| {
                    let existing = segment.spec.memory()?;
                    (existing.identity().owner() == spec.identity().owner()
                        && existing.transport() == spec.transport()
                        && spec.region().base()
                            < existing
                                .region()
                                .end()
                                .expect("attached segments are valid")
                        && existing.region().base() < end)
                        .then(|| existing.identity().id())
                }) {
                    return Err(AttachError::OverlappingAddressRange { existing });
                }
            }
            SegmentSpec::Nof(spec) => {
                if let Some(existing) = self.segments.values().find_map(|segment| {
                    let existing = segment.spec.nof()?;
                    (existing.transport() == spec.transport()).then(|| existing.identity().id())
                }) {
                    return Err(AttachError::DuplicateNofEndpoint { existing });
                }
            }
            SegmentSpec::Cxl(_) => {}
        }
        Ok(())
    }

    fn allocator_for(
        &mut self,
        spec: &SegmentSpec,
        max_allocator_nodes: u32,
    ) -> Result<ByteAllocator, AttachError> {
        let SegmentSpec::Cxl(cxl) = spec else {
            return Ok(ByteAllocator::new(
                spec.direct_region().size(),
                max_allocator_nodes,
            ));
        };

        if let Some(arena) = self.cxl_arenas.get(cxl.arena().id()) {
            if &arena.spec != cxl.arena() {
                return Err(AttachError::ConflictingCxlArena {
                    arena: cxl.arena().id().clone(),
                });
            }
            return Ok(arena.allocator.clone());
        }

        let allocator = ByteAllocator::new(cxl.arena().capacity_bytes(), max_allocator_nodes);
        self.cxl_arenas.insert(
            cxl.arena().id().clone(),
            CxlArena {
                spec: cxl.arena().clone(),
                allocator: allocator.clone(),
            },
        );
        Ok(allocator)
    }

    fn mutation(&mut self) -> CatalogMutation<'_> {
        CatalogMutation { catalog: self }
    }

    fn rebuild_snapshot(&mut self) {
        let mut candidates_by_class: HashMap<_, Vec<_>> = HashMap::new();
        for segment in self
            .segments
            .values()
            .filter(|segment| segment.accepting.load(Ordering::Acquire))
        {
            candidates_by_class
                .entry(segment.spec.replica_class())
                .or_default()
                .push(SegmentCandidate {
                    pool_id: self.pool_id,
                    segment: segment.clone(),
                });
        }
        for candidates in candidates_by_class.values_mut() {
            candidates.sort_unstable_by_key(SegmentCandidate::id);
        }

        let unchanged = candidates_by_class.len() == self.allocatable.len()
            && candidates_by_class
                .iter()
                .all(|(replica_class, candidates)| {
                    self.allocatable.get(replica_class).is_some_and(|current| {
                        candidates.len() == current.len()
                            && candidates
                                .iter()
                                .zip(current.iter())
                                .all(|(candidate, current)| {
                                    Arc::ptr_eq(&candidate.segment, &current.segment)
                                })
                    })
                });
        if unchanged {
            return;
        }
        self.allocatable = candidates_by_class
            .into_iter()
            .map(|(replica_class, candidates)| (replica_class, Arc::from(candidates)))
            .collect();
        self.generation = self.generation.wrapping_add(1);
    }
}

impl CatalogMutation<'_> {
    fn insert(&mut self, spec: SegmentSpec, allocator: ByteAllocator) -> SegmentCandidate {
        let spec = Arc::new(spec);
        let segment = Arc::new(Segment::new(self.catalog.pool_id, spec, allocator));
        let candidate = SegmentCandidate {
            pool_id: self.catalog.pool_id,
            segment: segment.clone(),
        };
        let id = segment.spec.identity().id();
        self.catalog.segments.insert(id, segment);
        candidate
    }

    fn quiesce(&mut self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let segment = self.owned_segment(owner, id)?;
        segment.quiesce();
        Ok(())
    }

    fn reactivate(&mut self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let segment = self.owned_segment(owner, id)?;
        segment.reactivate();
        Ok(())
    }

    fn remove(&mut self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let segment = self.owned_segment(owner, id)?;
        segment.prepare_remove()?;
        self.catalog.segments.remove(&id);
        Ok(())
    }

    fn owned_segment(
        &self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<Arc<Segment>, LifecycleError> {
        let segment = self
            .catalog
            .segments
            .get(&id)
            .cloned()
            .ok_or(LifecycleError::NotFound(id))?;
        if segment.spec.identity().owner() != owner {
            return Err(LifecycleError::OwnerMismatch {
                segment: id,
                expected: segment.spec.identity().owner(),
                actual: owner,
            });
        }
        Ok(segment)
    }
}

impl Drop for CatalogMutation<'_> {
    fn drop(&mut self) {
        self.catalog.rebuild_snapshot();
    }
}

impl Segment {
    fn new(pool_id: u64, spec: Arc<SegmentSpec>, allocator: ByteAllocator) -> Self {
        Self {
            pool_id,
            spec,
            phase: Mutex::new(SegmentState::Accepting),
            allocator,
            live_reservations: Arc::new(AtomicU64::new(0)),
            accepting: AtomicBool::new(true),
        }
    }

    pub(super) const fn pool_id(&self) -> u64 {
        self.pool_id
    }

    pub(super) fn spec(&self) -> &SegmentSpec {
        &self.spec
    }

    #[inline]
    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, ReserveError> {
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ReserveError::NotAccepting(self.spec.identity().id()));
        }
        if !self.allocator.may_satisfy(bytes) {
            return Err(ReserveError::OutOfSpace(self.spec.identity().id()));
        }

        let phase = self.phase.lock();
        if *phase != SegmentState::Accepting {
            return Err(ReserveError::NotAccepting(self.spec.identity().id()));
        }

        let allocation = self
            .allocator
            .allocate_after_precheck(bytes)
            .ok_or(ReserveError::OutOfSpace(self.spec.identity().id()))?;
        let buffer_address = match self
            .spec
            .direct_region()
            .base()
            .checked_add(allocation.offset())
        {
            Some(address) => address,
            None => return Err(ReserveError::AddressOverflow(self.spec.identity().id())),
        };
        let counter = ReservationCounter::acquire(self.live_reservations.clone());
        drop(phase);

        Ok(Reservation {
            allocation,
            segment: self.spec.clone(),
            _counter: counter,
            region: MemoryRegion::new(buffer_address, bytes),
        })
    }

    fn quiesce(&self) {
        let mut phase = self.phase.lock();
        if *phase == SegmentState::Accepting {
            *phase = SegmentState::Quiesced;
            self.accepting.store(false, Ordering::Release);
        }
    }

    fn reactivate(&self) {
        let mut phase = self.phase.lock();
        if *phase == SegmentState::Quiesced {
            *phase = SegmentState::Accepting;
            self.accepting.store(true, Ordering::Release);
        }
    }

    fn prepare_remove(&self) -> Result<(), LifecycleError> {
        let mut phase = self.phase.lock();
        let live_allocations = self.live_reservations.load(Ordering::Acquire);
        match *phase {
            SegmentState::Accepting => {
                Err(LifecycleError::StillAccepting(self.spec.identity().id()))
            }
            SegmentState::Quiesced if live_allocations != 0 => Err(LifecycleError::Busy {
                segment: self.spec.identity().id(),
                live_allocations,
            }),
            SegmentState::Quiesced => {
                *phase = SegmentState::Removed;
                self.accepting.store(false, Ordering::Release);
                Ok(())
            }
            SegmentState::Removed => Ok(()),
        }
    }

    pub(super) fn stats(&self) -> SegmentStats {
        let state = *self.phase.lock();
        let stats = self.allocator.stats();
        SegmentStats {
            space: SegmentSpaceStats {
                capacity_bytes: stats.capacity,
                used_bytes: stats.used_bytes,
                available_bytes: stats.available_bytes,
                largest_free_region_bytes: stats.largest_free_region,
            },
            reservations: SegmentReservationStats {
                live: self.live_reservations.load(Ordering::Acquire),
            },
            state,
        }
    }
}
