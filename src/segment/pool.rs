mod catalog;
mod entry;

use self::catalog::Catalog;
use self::entry::SegmentEntry;
use super::config::{
    MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE, MIN_ALLOCATOR_NODES_PER_SEGMENT, SegmentPoolConfig,
};
use super::error::{AttachError, PoolConfigError, ReserveError, SegmentStateError};
use super::identity::{ClientId, SegmentId};
use super::reservation::Reservation;
use super::spec::{ReplicaClass, SegmentSpec};
use super::stats::ReplicaClassSpaceStats;
use super::stats::SegmentStats;
use parking_lot::RwLock;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

pub struct SegmentPool {
    pool_id: u64,
    config: SegmentPoolConfig,
    catalog: RwLock<Catalog>,
    direct_capacity_epoch: AtomicU64,
    invalidation_epoch: AtomicU64,
}

/// Metadata and runtime-state handle for one logical segment.
///
/// This handle does not represent an allocation and therefore never blocks
/// removal by itself. Pass it to [`SegmentPool::reserve`] to reserve capacity.
#[derive(Clone)]
pub struct SegmentHandle {
    pool_id: u64,
    entry: Arc<SegmentEntry>,
}

/// Weak identity for one concrete segment attachment.
///
/// Keeping this token alive does not retain the segment resource. It is used
/// by deferred lifecycle work to fence a later attachment that reuses the same
/// logical segment id.
#[derive(Clone)]
pub(crate) struct SegmentIncarnation {
    pool_id: u64,
    entry: Weak<SegmentEntry>,
}

impl SegmentHandle {
    pub fn id(&self) -> SegmentId {
        self.entry.spec().identity().id()
    }

    pub fn spec(&self) -> &SegmentSpec {
        self.entry.spec()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.spec().replica_class()
    }

    pub fn stats(&self) -> SegmentStats {
        self.entry.stats()
    }

    pub(crate) fn incarnation(&self) -> SegmentIncarnation {
        SegmentIncarnation {
            pool_id: self.pool_id,
            entry: Arc::downgrade(&self.entry),
        }
    }
}

impl SegmentIncarnation {
    pub(crate) fn matches(&self, segment: &SegmentHandle) -> bool {
        self.pool_id == segment.pool_id
            && Weak::ptr_eq(&self.entry, &Arc::downgrade(&segment.entry))
    }

    pub(crate) fn same_as(&self, other: &Self) -> bool {
        self.pool_id == other.pool_id && Weak::ptr_eq(&self.entry, &other.entry)
    }
}

impl fmt::Debug for SegmentHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentHandle")
            .field("id", &self.id())
            .field("name", &self.spec().identity().name())
            .field("stats", &self.stats())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    generation: u64,
    replica_class: ReplicaClass,
    candidates: Arc<[SegmentHandle]>,
}

/// Capacity currently accepting direct allocations for one replica class.
///
/// Each mounted memory segment owns independent capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaClassCapacity {
    generation: u64,
    replica_class: ReplicaClass,
    capacity_bytes: u64,
}

impl ReplicaClassCapacity {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn replica_class(self) -> ReplicaClass {
        self.replica_class
    }

    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }
}

impl PoolSnapshot {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn replica_class(&self) -> ReplicaClass {
        self.replica_class
    }

    pub fn candidates(&self) -> &[SegmentHandle] {
        &self.candidates
    }

    pub fn iter(&self) -> std::slice::Iter<'_, SegmentHandle> {
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
    Attached(SegmentHandle),
    AlreadyAttached(SegmentHandle),
}

impl AttachOutcome {
    pub fn segment(&self) -> &SegmentHandle {
        match self {
            Self::Attached(segment) | Self::AlreadyAttached(segment) => segment,
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

        let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            pool_id,
            config,
            catalog: RwLock::new(Catalog::new(pool_id)),
            direct_capacity_epoch: AtomicU64::new(0),
            invalidation_epoch: AtomicU64::new(0),
        })
    }

    pub fn attach(&self, spec: SegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        self.update_direct_catalog(|catalog| {
            catalog.attach(spec, self.config.max_allocator_nodes_per_segment)
        })
    }

    /// Attaches a new segment without publishing it to accepting snapshots.
    /// Existing idempotent mounts retain their current state.
    pub fn attach_quiesced(&self, spec: SegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        let owner = spec.identity().owner();
        let id = spec.identity().id();
        self.update_direct_catalog(|catalog| {
            let outcome = catalog.attach(spec, self.config.max_allocator_nodes_per_segment)?;
            if outcome.is_new() {
                catalog
                    .quiesce(owner, id)
                    .expect("a newly attached segment remains owned while catalog-locked");
            }
            Ok(outcome)
        })
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        self.snapshot_for(ReplicaClass::Memory)
    }

    pub fn snapshot_for(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        self.catalog.read().snapshot(replica_class)
    }

    /// Returns the physical capacity behind the current accepting snapshot.
    pub fn capacity_for(&self, replica_class: ReplicaClass) -> ReplicaClassCapacity {
        summarize_capacity(self.catalog.read().snapshot(replica_class))
    }

    /// Returns aggregate physical space for all mounted segments in a class.
    ///
    /// Unlike [`Self::capacity_for`], this includes quiesced segments so a
    /// placement-state transition cannot create a false memory-pressure spike.
    pub fn space_for(&self, replica_class: ReplicaClass) -> ReplicaClassSpaceStats {
        self.catalog.read().space_for(replica_class)
    }

    /// Cheap change token for callers that cache accepting capacities.
    pub fn direct_capacity_epoch(&self) -> u64 {
        self.direct_capacity_epoch.load(Ordering::Acquire)
    }

    /// Cheap change token for consumers that retire objects whose segment
    /// lifetime was explicitly invalidated.
    pub(crate) fn invalidation_epoch(&self) -> u64 {
        self.invalidation_epoch.load(Ordering::Acquire)
    }

    pub fn segment(&self, id: SegmentId) -> Option<SegmentHandle> {
        self.catalog.read().segment(id)
    }

    pub fn stats(&self, id: SegmentId) -> Option<SegmentStats> {
        self.segment(id).map(|segment| segment.stats())
    }

    pub fn len(&self) -> usize {
        self.catalog.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn reserve(
        &self,
        candidate: &SegmentHandle,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        if candidate.pool_id != self.pool_id {
            return Err(ReserveError::ForeignCandidate);
        }
        candidate.entry.reserve(bytes)
    }

    pub fn reserve_on(&self, id: SegmentId, bytes: u64) -> Result<Reservation, ReserveError> {
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        let segment = self.segment(id).ok_or(ReserveError::NotFound(id))?;
        self.reserve(&segment, bytes)
    }

    pub fn quiesce(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.quiesce(owner, id))
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.reactivate(owner, id))
    }

    /// Validates ownership for the whole batch before publishing any segment.
    pub fn reactivate_many(
        &self,
        owner: ClientId,
        ids: &[SegmentId],
    ) -> Result<(), SegmentStateError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.update_direct_catalog(|catalog| catalog.reactivate_many(owner, ids))
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.remove(owner, id))?;
        self.invalidation_epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Logically invalidates every segment owned by a fenced client and removes
    /// them from placement and lookup. Outstanding reservations retain their
    /// physical allocation until dropped, but become unusable immediately.
    /// Repeating cleanup for an owner with no mounted segments is a no-op.
    pub fn invalidate_owner(&self, owner: ClientId) -> usize {
        self.invalidate_owners(std::iter::once(owner))
    }

    /// Invalidates many owners under one catalog lock and publishes one new
    /// placement snapshot. Duplicate and already-cleaned owners are ignored.
    pub fn invalidate_owners(&self, owners: impl IntoIterator<Item = ClientId>) -> usize {
        let invalidated = self.catalog.write().invalidate_owners(owners);
        if invalidated != 0 {
            self.direct_capacity_epoch.fetch_add(1, Ordering::Release);
            self.invalidation_epoch.fetch_add(1, Ordering::Release);
        }
        invalidated
    }

    fn update_direct_catalog<T, E>(
        &self,
        operation: impl FnOnce(&mut Catalog) -> Result<T, E>,
    ) -> Result<T, E> {
        let result = {
            let mut catalog = self.catalog.write();
            operation(&mut catalog)?
        };
        self.direct_capacity_epoch.fetch_add(1, Ordering::Release);
        Ok(result)
    }
}

fn summarize_capacity(snapshot: PoolSnapshot) -> ReplicaClassCapacity {
    let mut capacity_bytes = 0_u64;
    for candidate in snapshot.iter() {
        capacity_bytes = capacity_bytes.saturating_add(candidate.stats().space.capacity_bytes);
    }
    ReplicaClassCapacity {
        generation: snapshot.generation(),
        replica_class: snapshot.replica_class(),
        capacity_bytes,
    }
}

impl Default for SegmentPool {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_spec(spec: &SegmentSpec) -> Result<(), AttachError> {
    if spec.identity().id().is_nil() {
        return Err(AttachError::NilSegmentId);
    }
    if spec.identity().owner().is_nil() {
        return Err(AttachError::NilOwnerId);
    }
    if spec.identity().name().is_empty() {
        return Err(AttachError::EmptyName);
    }
    if matches!(spec.transport().protocol().as_str(), "cxl" | "nvmeof") {
        return Err(AttachError::IncompatibleTransportProtocol {
            protocol: spec.transport().protocol().clone(),
        });
    }
    if spec.region().base() == 0 {
        return Err(AttachError::ZeroBaseAddress);
    }
    validate_direct_range(spec.region(), spec.transport())
}

fn validate_direct_range(
    region: super::descriptor::MemoryRegion,
    transport: &super::transport::TransportEndpoint,
) -> Result<(), AttachError> {
    if transport.endpoint().is_empty() {
        return Err(AttachError::EmptyTransportEndpoint);
    }
    let protocol = transport.protocol().as_str();
    if let Err(source) = protocol.parse::<super::transport::TransportProtocol>() {
        return Err(AttachError::InvalidTransportProtocol {
            protocol: Arc::from(protocol),
            source,
        });
    }
    if region.size() == 0 {
        return Err(AttachError::ZeroSize);
    }
    region.end().ok_or(AttachError::AddressOverflow)?;
    Ok(())
}
