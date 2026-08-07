use super::offset_allocator::{ByteAllocator, OffsetAllocationHandle};
use super::types::{
    ClientId, MemoryDescriptor, MemoryDescriptorRef, MemoryRegion, MemorySegmentSpec, SegmentId,
    SegmentReservationStats, SegmentSpaceStats, SegmentState, SegmentStats,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub const DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT: u32 = 128 * 1024;

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPoolConfig {
    max_allocator_nodes_per_segment: u32,
}

impl SegmentPoolConfig {
    pub const fn new(max_allocator_nodes_per_segment: u32) -> Self {
        Self {
            max_allocator_nodes_per_segment,
        }
    }

    pub const fn max_allocator_nodes_per_segment(self) -> u32 {
        self.max_allocator_nodes_per_segment
    }
}

impl Default for SegmentPoolConfig {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolConfigError {
    max_allocator_nodes_per_segment: u32,
}

impl fmt::Display for PoolConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "max allocator nodes per segment must be in 3..{}, got {}",
            u32::MAX - 1,
            self.max_allocator_nodes_per_segment
        )
    }
}

impl std::error::Error for PoolConfigError {}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttachError {
    NilSegmentId,
    NilOwnerId,
    EmptyName,
    InvalidBase,
    EmptyTransportEndpoint,
    InvalidTransportProtocol,
    EmptyHostId,
    ZeroSize,
    AddressOverflow,
    ConflictingSegmentId(SegmentId),
    OverlappingAddressRange { existing: SegmentId },
}

impl fmt::Display for AttachError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NilSegmentId => formatter.write_str("segment id must not be nil"),
            Self::NilOwnerId => formatter.write_str("segment owner id must not be nil"),
            Self::EmptyName => formatter.write_str("segment name must not be empty"),
            Self::InvalidBase => {
                formatter.write_str("memory segment base address must not be zero")
            }
            Self::EmptyTransportEndpoint => {
                formatter.write_str("transport endpoint must not be empty")
            }
            Self::InvalidTransportProtocol => {
                formatter.write_str("transport protocol is not a valid identifier")
            }
            Self::EmptyHostId => formatter.write_str("host id must not be empty when present"),
            Self::ZeroSize => formatter.write_str("segment size must not be zero"),
            Self::AddressOverflow => formatter.write_str("segment address range overflows u64"),
            Self::ConflictingSegmentId(id) => {
                write!(
                    formatter,
                    "segment id {id} is already attached with different metadata"
                )
            }
            Self::OverlappingAddressRange { existing } => write!(
                formatter,
                "segment address range overlaps existing segment {existing} in the same address space"
            ),
        }
    }
}

impl std::error::Error for AttachError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleError {
    NotFound(SegmentId),
    OwnerMismatch {
        segment: SegmentId,
        expected: ClientId,
        actual: ClientId,
    },
    StillAccepting(SegmentId),
    Busy {
        segment: SegmentId,
        live_allocations: u64,
    },
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(formatter, "segment {id} was not found"),
            Self::OwnerMismatch {
                segment,
                expected,
                actual,
            } => write!(
                formatter,
                "segment {segment} belongs to client {expected}, not {actual}"
            ),
            Self::StillAccepting(id) => {
                write!(formatter, "segment {id} must be quiesced before removal")
            }
            Self::Busy {
                segment,
                live_allocations,
            } => write!(
                formatter,
                "segment {segment} still has {live_allocations} live allocations"
            ),
        }
    }
}

impl std::error::Error for LifecycleError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReserveError {
    ZeroSize,
    ForeignCandidate,
    NotFound(SegmentId),
    NotAccepting(SegmentId),
    OutOfSpace(SegmentId),
    AddressOverflow(SegmentId),
}

impl fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSize => formatter.write_str("reservation size must not be zero"),
            Self::ForeignCandidate => {
                formatter.write_str("segment candidate belongs to another pool")
            }
            Self::NotFound(id) => write!(formatter, "segment {id} was not found"),
            Self::NotAccepting(id) => {
                write!(formatter, "segment {id} is not accepting reservations")
            }
            Self::OutOfSpace(id) => write!(formatter, "segment {id} has no suitable free range"),
            Self::AddressOverflow(id) => {
                write!(formatter, "allocated address overflowed in segment {id}")
            }
        }
    }
}

impl std::error::Error for ReserveError {}

pub struct Reservation {
    allocation: OffsetAllocationHandle,
    region: MemoryRegion,
}

impl Reservation {
    pub fn segment_id(&self) -> SegmentId {
        self.allocation.spec().identity().id()
    }

    pub const fn offset(&self) -> u64 {
        self.allocation.offset()
    }

    pub const fn requested_bytes(&self) -> u64 {
        self.allocation.requested_bytes()
    }

    pub const fn reserved_bytes(&self) -> u64 {
        self.allocation.reserved_bytes()
    }

    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub fn descriptor(&self) -> MemoryDescriptorRef<'_> {
        MemoryDescriptorRef::new(self.region, self.allocation.spec().transport())
    }

    pub fn owned_descriptor(&self) -> MemoryDescriptor {
        self.descriptor().to_owned()
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reservation")
            .field("segment_id", &self.segment_id())
            .field("offset", &self.offset())
            .field("region", &self.region)
            .field("reserved_bytes", &self.reserved_bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}

impl SegmentPool {
    pub fn new() -> Self {
        Self::with_config(SegmentPoolConfig::default())
            .expect("the default SegmentPool configuration is valid")
    }

    pub fn with_config(config: SegmentPoolConfig) -> Result<Self, PoolConfigError> {
        let max_nodes = config.max_allocator_nodes_per_segment;
        if !(3..u32::MAX - 1).contains(&max_nodes) {
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
        let mut catalog = write_lock(&self.catalog);

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
        let catalog = read_lock(&self.catalog);
        PoolSnapshot {
            generation: catalog.generation,
            candidates: catalog.allocatable.clone(),
        }
    }

    pub fn candidate(&self, id: SegmentId) -> Option<SegmentCandidate> {
        let catalog = read_lock(&self.catalog);
        catalog.segments.get(&id).map(|segment| SegmentCandidate {
            pool_id: self.pool_id,
            segment: segment.clone(),
        })
    }

    pub fn stats(&self, id: SegmentId) -> Option<SegmentStats> {
        self.candidate(id).map(|candidate| candidate.stats())
    }

    pub fn len(&self) -> usize {
        read_lock(&self.catalog).segments.len()
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
        let segment = read_lock(&self.catalog)
            .segments
            .get(&id)
            .cloned()
            .ok_or(ReserveError::NotFound(id))?;
        segment.reserve(bytes)
    }

    pub fn quiesce(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = write_lock(&self.catalog);
        let segment = owned_segment(&catalog, owner, id)?;
        segment.quiesce();
        rebuild_snapshot(&mut catalog, self.pool_id);
        Ok(())
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = write_lock(&self.catalog);
        let segment = owned_segment(&catalog, owner, id)?;
        segment.reactivate();
        rebuild_snapshot(&mut catalog, self.pool_id);
        Ok(())
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        let mut catalog = write_lock(&self.catalog);
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

        let phase = mutex_lock(&self.phase);
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
        let mut phase = mutex_lock(&self.phase);
        if *phase == SegmentState::Accepting {
            *phase = SegmentState::Quiesced;
            self.accepting.store(false, Ordering::Release);
        }
    }

    fn reactivate(&self) {
        let mut phase = mutex_lock(&self.phase);
        if *phase == SegmentState::Quiesced {
            *phase = SegmentState::Accepting;
            self.accepting.store(true, Ordering::Release);
        }
    }

    fn prepare_remove(&self) -> Result<(), LifecycleError> {
        let mut phase = mutex_lock(&self.phase);
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
        let state = *mutex_lock(&self.phase);
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
        return Err(AttachError::InvalidBase);
    }
    if spec.transport().endpoint().is_empty() {
        return Err(AttachError::EmptyTransportEndpoint);
    }
    if spec
        .transport()
        .protocol()
        .as_str()
        .parse::<super::types::TransportProtocol>()
        .is_err()
    {
        return Err(AttachError::InvalidTransportProtocol);
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

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[inline(always)]
fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
