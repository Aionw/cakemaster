use super::config::{
    MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE, MIN_ALLOCATOR_NODES_PER_SEGMENT, SegmentPoolConfig,
};
use super::descriptor::{MemoryRegion, MemorySegmentSpec};
use super::error::{AttachError, LifecycleError, PoolConfigError, ReserveError};
use super::identity::{ClientId, SegmentId};
use super::offset_allocator::ByteAllocator;
use super::reservation::Reservation;
use super::stats::{SegmentReservationStats, SegmentSpaceStats, SegmentState, SegmentStats};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

pub struct SegmentPool {
    pool_id: u64,
    config: SegmentPoolConfig,
    catalog: RwLock<Catalog>,
}

struct Catalog {
    generation: u64,
    segments: HashMap<SegmentId, Arc<Segment>>,
    allocatable: Arc<[SegmentCandidate]>,
}

struct Segment {
    pool_id: u64,
    spec: Arc<MemorySegmentSpec>,
    phase: Mutex<SegmentState>,
    allocator: ByteAllocator,
    accepting: AtomicBool,
}

#[derive(Clone)]
pub struct SegmentCandidate {
    pool_id: u64,
    segment: Arc<Segment>,
}

impl SegmentCandidate {
    pub fn id(&self) -> SegmentId {
        self.segment.spec.identity().id()
    }

    pub fn spec(&self) -> &MemorySegmentSpec {
        &self.segment.spec
    }

    pub fn stats(&self) -> SegmentStats {
        self.segment.stats()
    }
}

impl fmt::Debug for SegmentCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentCandidate")
            .field("id", &self.id())
            .field("name", &self.spec().identity().name())
            .field("stats", &self.stats())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    generation: u64,
    candidates: Arc<[SegmentCandidate]>,
}

impl PoolSnapshot {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn candidates(&self) -> &[SegmentCandidate] {
        &self.candidates
    }

    pub fn iter(&self) -> std::slice::Iter<'_, SegmentCandidate> {
        self.candidates.iter()
    }

    pub fn len(&self) -> usize {
        self.candidates.len()
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }
}

#[derive(Clone, Debug)]
pub enum AttachOutcome {
    Attached(SegmentCandidate),
    AlreadyAttached(SegmentCandidate),
}

impl AttachOutcome {
    pub fn candidate(&self) -> &SegmentCandidate {
        match self {
            Self::Attached(candidate) | Self::AlreadyAttached(candidate) => candidate,
        }
    }

    pub const fn is_new(&self) -> bool {
        matches!(self, Self::Attached(_))
    }
}

impl SegmentPool {
    pub fn new() -> Self {
        Self::with_config(SegmentPoolConfig::default())
            .expect("the default SegmentPool configuration is valid")
    }

    pub fn with_config(config: SegmentPoolConfig) -> Result<Self, PoolConfigError> {
        let max_nodes = config.max_allocator_nodes_per_segment;
        if !(MIN_ALLOCATOR_NODES_PER_SEGMENT..MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE)
            .contains(&max_nodes)
        {
            return Err(PoolConfigError {
                max_allocator_nodes_per_segment: max_nodes,
            });
        }

        Ok(Self {
            pool_id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            config,
            catalog: RwLock::new(Catalog {
                generation: 0,
                segments: HashMap::new(),
                allocatable: Arc::from([]),
            }),
        })
    }

    pub fn attach(&self, spec: MemorySegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        let mut catalog = self.catalog.write();

        if let Some(existing) = catalog.segments.get(&spec.identity().id()) {
            let candidate = SegmentCandidate {
                pool_id: self.pool_id,
                segment: existing.clone(),
            };
            return if existing.spec.as_ref() == &spec {
                Ok(AttachOutcome::AlreadyAttached(candidate))
            } else {
                Err(AttachError::ConflictingSegmentId(spec.identity().id()))
            };
        }

        let end = spec.region().end().ok_or(AttachError::AddressOverflow)?;
        for existing in catalog.segments.values() {
            let existing_spec = existing.spec.as_ref();
            if existing_spec.identity().owner() == spec.identity().owner()
                && existing_spec.transport() == spec.transport()
                && spec.region().base()
                    < existing_spec
                        .region()
                        .end()
                        .expect("attached segments are valid")
                && existing_spec.region().base() < end
            {
                return Err(AttachError::OverlappingAddressRange {
                    existing: existing_spec.identity().id(),
                });
            }
        }

        let spec = Arc::new(spec);
        let segment = Arc::new(Segment::new(
            self.pool_id,
            spec,
            self.config.max_allocator_nodes_per_segment,
        ));
        let candidate = SegmentCandidate {
            pool_id: self.pool_id,
            segment: segment.clone(),
        };
        catalog
            .segments
            .insert(segment.spec.identity().id(), segment);
        rebuild_snapshot(&mut catalog, self.pool_id);

        Ok(AttachOutcome::Attached(candidate))
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        let catalog = self.catalog.read();
        PoolSnapshot {
            generation: catalog.generation,
            candidates: catalog.allocatable.clone(),
        }
    }

    pub fn candidate(&self, id: SegmentId) -> Option<SegmentCandidate> {
        let catalog = self.catalog.read();
        catalog.segments.get(&id).map(|segment| SegmentCandidate {
            pool_id: self.pool_id,
            segment: segment.clone(),
        })
    }

    pub fn stats(&self, id: SegmentId) -> Option<SegmentStats> {
        self.candidate(id).map(|candidate| candidate.stats())
    }

    pub fn len(&self) -> usize {
        self.catalog.read().segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn reserve(
        &self,
        candidate: &SegmentCandidate,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        if candidate.pool_id != self.pool_id || candidate.segment.pool_id != self.pool_id {
            return Err(ReserveError::ForeignCandidate);
        }
        candidate.segment.reserve(bytes)
    }

    pub fn reserve_on(&self, id: SegmentId, bytes: u64) -> Result<Reservation, ReserveError> {
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        let segment = self
            .catalog
            .read()
            .segments
            .get(&id)
            .cloned()
            .ok_or(ReserveError::NotFound(id))?;
        segment.reserve(bytes)
    }

    pub fn quiesce(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = self.catalog.write();
        let segment = owned_segment(&catalog, owner, id)?;
        segment.quiesce();
        rebuild_snapshot(&mut catalog, self.pool_id);
        Ok(())
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = self.catalog.write();
        let segment = owned_segment(&catalog, owner, id)?;
        segment.reactivate();
        rebuild_snapshot(&mut catalog, self.pool_id);
        Ok(())
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = self.catalog.write();
        let segment = owned_segment(&catalog, owner, id)?;
        segment.prepare_remove()?;
        catalog.segments.remove(&id);
        rebuild_snapshot(&mut catalog, self.pool_id);
        Ok(())
    }
}

impl Default for SegmentPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Segment {
    fn new(pool_id: u64, spec: Arc<MemorySegmentSpec>, max_allocator_nodes: u32) -> Self {
        let allocator = ByteAllocator::new(spec.clone(), max_allocator_nodes);
        Self {
            pool_id,
            spec,
            phase: Mutex::new(SegmentState::Accepting),
            allocator,
            accepting: AtomicBool::new(true),
        }
    }

    #[inline]
    fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, ReserveError> {
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
        let buffer_address = match self.spec.region().base().checked_add(allocation.offset()) {
            Some(address) => address,
            None => return Err(ReserveError::AddressOverflow(self.spec.identity().id())),
        };
        drop(phase);

        Ok(Reservation {
            allocation,
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
        let live_allocations = self.allocator.stats().live_allocations;
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

    fn stats(&self) -> SegmentStats {
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
                live: stats.live_allocations,
            },
            state,
        }
    }
}

fn validate_spec(spec: &MemorySegmentSpec) -> Result<(), AttachError> {
    if spec.identity().id().is_nil() {
        return Err(AttachError::NilSegmentId);
    }
    if spec.identity().owner().is_nil() {
        return Err(AttachError::NilOwnerId);
    }
    if spec.identity().name().is_empty() {
        return Err(AttachError::EmptyName);
    }
    if spec.region().base() == 0 {
        return Err(AttachError::ZeroBaseAddress);
    }
    if spec.transport().endpoint().is_empty() {
        return Err(AttachError::EmptyTransportEndpoint);
    }
    let protocol = spec.transport().protocol().as_str();
    if let Err(source) = protocol.parse::<super::transport::TransportProtocol>() {
        return Err(AttachError::InvalidTransportProtocol {
            protocol: Arc::from(protocol),
            source,
        });
    }
    if spec.topology().host_id().is_some_and(str::is_empty) {
        return Err(AttachError::EmptyHostId);
    }
    if spec.region().size() == 0 {
        return Err(AttachError::ZeroSize);
    }
    spec.region().end().ok_or(AttachError::AddressOverflow)?;
    Ok(())
}

fn owned_segment(
    catalog: &Catalog,
    owner: ClientId,
    id: SegmentId,
) -> Result<Arc<Segment>, LifecycleError> {
    let segment = catalog
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

fn rebuild_snapshot(catalog: &mut Catalog, pool_id: u64) {
    let mut candidates: Vec<_> = catalog
        .segments
        .values()
        .filter(|segment| segment.accepting.load(Ordering::Acquire))
        .map(|segment| SegmentCandidate {
            pool_id,
            segment: segment.clone(),
        })
        .collect();
    candidates.sort_unstable_by_key(SegmentCandidate::id);
    catalog.allocatable = Arc::from(candidates);
    catalog.generation = catalog.generation.wrapping_add(1);
}
