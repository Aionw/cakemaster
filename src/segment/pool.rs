mod catalog;
mod entry;
mod resource;

use self::catalog::Catalog;
use self::entry::SegmentEntry;
use self::resource::TransferableExtent;
use super::config::{
    MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE, MIN_ALLOCATOR_NODES_PER_SEGMENT, SegmentPoolConfig,
};
use super::error::{AttachError, LocalSsdError, PoolConfigError, ReserveError, SegmentStateError};
use super::identity::{ClientId, SegmentId};
use super::local_ssd::{LocalSsdStats, OffloadPermit};
use super::reservation::Reservation;
use super::spec::{
    ReplicaClass, SegmentConfiguration, SegmentKind, SegmentResourceId, SegmentSpec,
};
use super::stats::ReplicaClassSpaceStats;
use super::stats::SegmentStats;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

pub struct SegmentPool {
    pool_id: u64,
    config: SegmentPoolConfig,
    catalog: RwLock<Catalog>,
    direct_capacity_epoch: AtomicU64,
    invalidation_epoch: AtomicU64,
    topology_sink: OnceLock<SegmentTopologySink>,
}

type SegmentTopologySink = Arc<dyn Fn(SegmentTopologyEvent) + Send + Sync>;

#[derive(Clone, Debug)]
pub(crate) enum SegmentTopologyEvent {
    Attach {
        spec: SegmentSpec,
        state: crate::segment::stats::SegmentState,
    },
    ReportLocalSsdCapacity {
        owner: ClientId,
        id: SegmentId,
        capacity_bytes: u64,
    },
    SetLocalSsdOffloadEnabled {
        owner: ClientId,
        id: SegmentId,
        enabled: bool,
    },
    Quiesce {
        owner: ClientId,
        id: SegmentId,
    },
    Reactivate {
        owner: ClientId,
        ids: Vec<SegmentId>,
    },
    Remove {
        owner: ClientId,
        id: SegmentId,
    },
    InvalidateOwners {
        owners: Vec<ClientId>,
    },
}

pub(crate) struct SegmentExtentTransfer {
    segment: SegmentId,
    extent: TransferableExtent,
}

/// Metadata and runtime-state handle for one logical segment.
///
/// This handle does not represent an allocation and therefore never blocks
/// removal by itself. Convert it to a capability-specific candidate before
/// reserving capacity.
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

    pub fn kind(&self) -> SegmentKind {
        self.spec().kind()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.spec().replica_class()
    }

    pub fn resource_id(&self) -> SegmentResourceId {
        self.spec().resource_id()
    }

    pub fn stats(&self) -> SegmentStats {
        self.entry.stats()
    }

    pub fn local_ssd_stats(&self) -> Option<LocalSsdStats> {
        self.entry.local_ssd_stats()
    }

    pub(crate) fn incarnation(&self) -> SegmentIncarnation {
        SegmentIncarnation {
            pool_id: self.pool_id,
            entry: Arc::downgrade(&self.entry),
        }
    }

    pub fn direct_candidate(&self) -> Option<DirectCandidate> {
        self.entry
            .supports_direct_reservation()
            .then(|| DirectCandidate {
                segment: self.clone(),
            })
    }

    pub fn offload_target(&self) -> Option<OffloadTarget> {
        self.entry.supports_offload().then(|| OffloadTarget {
            segment: self.clone(),
        })
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

/// Capability token accepted by synchronous range reservation APIs.
#[derive(Clone, Debug)]
pub struct DirectCandidate {
    segment: SegmentHandle,
}

impl Deref for DirectCandidate {
    type Target = SegmentHandle;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

/// Capability token accepted by asynchronous LocalSSD admission APIs.
#[derive(Clone, Debug)]
pub struct OffloadTarget {
    segment: SegmentHandle,
}

impl Deref for OffloadTarget {
    type Target = SegmentHandle;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    generation: u64,
    replica_class: ReplicaClass,
    candidates: Arc<[DirectCandidate]>,
}

/// Capacity currently accepting direct allocations for one replica class.
///
/// Logical segments that share a physical resource (for example, CXL mounts)
/// are counted once.
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

    pub fn candidates(&self) -> &[DirectCandidate] {
        &self.candidates
    }

    pub fn iter(&self) -> std::slice::Iter<'_, DirectCandidate> {
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
pub struct OffloadSnapshot {
    generation: u64,
    targets: Arc<[OffloadTarget]>,
}

impl OffloadSnapshot {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn targets(&self) -> &[OffloadTarget] {
        &self.targets
    }

    pub fn iter(&self) -> std::slice::Iter<'_, OffloadTarget> {
        self.targets.iter()
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
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

    pub fn direct_candidate(&self) -> Option<DirectCandidate> {
        self.segment().direct_candidate()
    }

    pub fn offload_target(&self) -> Option<OffloadTarget> {
        self.segment().offload_target()
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
            || config.allocator_shards == 0
            || usize::try_from(max_nodes).unwrap_or(usize::MAX)
                / (MIN_ALLOCATOR_NODES_PER_SEGMENT as usize)
                < config.allocator_shards
            || config
                .allocator_shard_index
                .is_some_and(|index| index >= config.allocator_shards)
        {
            return Err(PoolConfigError {
                max_allocator_nodes_per_segment: max_nodes,
                allocator_shards: config.allocator_shards,
            });
        }

        let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            pool_id,
            config,
            catalog: RwLock::new(Catalog::new(pool_id)),
            direct_capacity_epoch: AtomicU64::new(0),
            invalidation_epoch: AtomicU64::new(0),
            topology_sink: OnceLock::new(),
        })
    }

    pub(crate) const fn config(&self) -> SegmentPoolConfig {
        self.config
    }

    pub(crate) const fn instance_id(&self) -> u64 {
        self.pool_id
    }

    /// Installs the single production topology mirror. Initial state is sent
    /// before the method returns, and every later mutation is published only
    /// after the control catalog has committed it.
    pub(crate) fn install_topology_sink(
        &self,
        sink: impl Fn(SegmentTopologyEvent) + Send + Sync + 'static,
    ) {
        let sink: SegmentTopologySink = Arc::new(sink);
        assert!(
            self.topology_sink.set(sink.clone()).is_ok(),
            "segment topology sink is installed once"
        );
        for event in self.catalog.read().topology_events() {
            sink(event);
        }
    }

    fn publish_topology(&self, event: SegmentTopologyEvent) {
        if let Some(sink) = self.topology_sink.get() {
            sink(event);
        }
    }

    pub fn attach(&self, spec: SegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        let event_spec = spec.clone();
        let outcome = self.update_direct_catalog(|catalog| {
            catalog.attach(
                spec,
                self.config.max_allocator_nodes_per_segment,
                self.config.allocator_shards,
                self.config.allocator_shard_index,
            )
        })?;
        if outcome.is_new() {
            self.publish_topology(SegmentTopologyEvent::Attach {
                spec: event_spec,
                state: crate::segment::stats::SegmentState::Accepting,
            });
        }
        Ok(outcome)
    }

    /// Attaches a new segment without publishing it to accepting snapshots.
    /// Existing idempotent mounts retain their current state.
    pub fn attach_quiesced(&self, spec: SegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        let owner = spec.identity().owner();
        let id = spec.identity().id();
        let event_spec = spec.clone();
        let outcome = self.update_direct_catalog(|catalog| {
            let outcome = catalog.attach(
                spec,
                self.config.max_allocator_nodes_per_segment,
                self.config.allocator_shards,
                self.config.allocator_shard_index,
            )?;
            if outcome.is_new() {
                catalog
                    .quiesce(owner, id)
                    .expect("a newly attached segment remains owned while catalog-locked");
            }
            Ok(outcome)
        })?;
        if outcome.is_new() {
            self.publish_topology(SegmentTopologyEvent::Attach {
                spec: event_spec,
                state: crate::segment::stats::SegmentState::Quiesced,
            });
        }
        Ok(outcome)
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        self.snapshot_for(ReplicaClass::Memory)
    }

    pub fn snapshot_for(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        self.catalog.read().snapshot(replica_class)
    }

    /// Returns the physical capacity behind the current accepting snapshot.
    pub fn capacity_for(&self, replica_class: ReplicaClass) -> ReplicaClassCapacity {
        self.capacities_for(&[replica_class])
            .pop()
            .expect("one requested replica class produces one capacity result")
    }

    /// Computes multiple class capacities from one catalog snapshot lock.
    pub fn capacities_for(&self, replica_classes: &[ReplicaClass]) -> Vec<ReplicaClassCapacity> {
        let catalog = self.catalog.read();
        replica_classes
            .iter()
            .map(|replica_class| summarize_capacity(catalog.snapshot(*replica_class)))
            .collect()
    }

    /// Returns aggregate physical space for all mounted segments in a class.
    ///
    /// Unlike [`Self::capacity_for`], this includes quiesced segments so a
    /// placement-state transition cannot create a false memory-pressure spike.
    pub fn space_for(&self, replica_class: ReplicaClass) -> ReplicaClassSpaceStats {
        self.catalog.read().space_for(replica_class)
    }

    pub(crate) fn space_for_shard(
        &self,
        replica_class: ReplicaClass,
        allocator_shard: usize,
    ) -> ReplicaClassSpaceStats {
        self.catalog
            .read()
            .space_for_shard(replica_class, Some(allocator_shard))
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

    pub fn offload_snapshot(&self) -> OffloadSnapshot {
        self.catalog.read().offload_snapshot()
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
        candidate: &DirectCandidate,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        self.reserve_for_shard(candidate, 0, bytes)
    }

    #[inline]
    pub(crate) fn reserve_for_shard(
        &self,
        candidate: &DirectCandidate,
        allocator_shard: usize,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        if candidate.pool_id != self.pool_id {
            return Err(ReserveError::ForeignCandidate);
        }
        candidate.entry.reserve(allocator_shard, bytes)
    }

    pub fn reserve_on(&self, id: SegmentId, bytes: u64) -> Result<Reservation, ReserveError> {
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        let segment = self.segment(id).ok_or(ReserveError::NotFound(id))?;
        let candidate = segment
            .direct_candidate()
            .ok_or(ReserveError::NotDirectlyAllocatable(id))?;
        self.reserve(&candidate, bytes)
    }

    pub(crate) fn take_empty_extent(
        &self,
        replica_class: ReplicaClass,
        allocator_shard: usize,
        minimum_bytes: u64,
    ) -> Option<SegmentExtentTransfer> {
        self.catalog
            .read()
            .take_empty_extent(replica_class, allocator_shard, minimum_bytes)
            .map(|(segment, extent)| SegmentExtentTransfer { segment, extent })
    }

    pub(crate) fn add_extent(&self, allocator_shard: usize, transfer: SegmentExtentTransfer) {
        self.catalog
            .read()
            .add_extent(transfer.segment, allocator_shard, transfer.extent);
        self.direct_capacity_epoch.fetch_add(1, Ordering::Release);
    }

    pub fn report_local_ssd_capacity(
        &self,
        owner: ClientId,
        id: SegmentId,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        self.catalog
            .read()
            .report_local_ssd_capacity(owner, id, capacity_bytes)?;
        self.publish_topology(SegmentTopologyEvent::ReportLocalSsdCapacity {
            owner,
            id,
            capacity_bytes,
        });
        Ok(())
    }

    pub fn set_local_ssd_offload_enabled(
        &self,
        owner: ClientId,
        id: SegmentId,
        enabled: bool,
    ) -> Result<(), LocalSsdError> {
        self.catalog
            .write()
            .set_local_ssd_offload_enabled(owner, id, enabled)?;
        self.publish_topology(SegmentTopologyEvent::SetLocalSsdOffloadEnabled {
            owner,
            id,
            enabled,
        });
        Ok(())
    }

    pub fn admit_offload(
        &self,
        target: &OffloadTarget,
        bytes: u64,
    ) -> Result<OffloadPermit, LocalSsdError> {
        if target.pool_id != self.pool_id {
            return Err(LocalSsdError::ForeignCandidate);
        }
        target.entry.admit_offload(bytes)
    }

    pub fn quiesce(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.quiesce(owner, id))?;
        self.publish_topology(SegmentTopologyEvent::Quiesce { owner, id });
        Ok(())
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.reactivate(owner, id))?;
        self.publish_topology(SegmentTopologyEvent::Reactivate {
            owner,
            ids: vec![id],
        });
        Ok(())
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
        self.update_direct_catalog(|catalog| catalog.reactivate_many(owner, ids))?;
        self.publish_topology(SegmentTopologyEvent::Reactivate {
            owner,
            ids: ids.to_vec(),
        });
        Ok(())
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.update_direct_catalog(|catalog| catalog.remove(owner, id))?;
        self.invalidation_epoch.fetch_add(1, Ordering::Release);
        self.publish_topology(SegmentTopologyEvent::Remove { owner, id });
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
        let owners = owners.into_iter().collect::<Vec<_>>();
        let invalidated = self
            .catalog
            .write()
            .invalidate_owners(owners.iter().copied());
        if invalidated != 0 {
            self.direct_capacity_epoch.fetch_add(1, Ordering::Release);
            self.invalidation_epoch.fetch_add(1, Ordering::Release);
            self.publish_topology(SegmentTopologyEvent::InvalidateOwners { owners });
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
    let mut resources = HashSet::with_capacity(snapshot.len());
    let mut capacity_bytes = 0_u64;
    for candidate in snapshot.iter() {
        if resources.insert(candidate.resource_id()) {
            capacity_bytes = capacity_bytes.saturating_add(candidate.stats().space.capacity_bytes);
        }
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
    match spec.configuration() {
        SegmentConfiguration::Memory { region, transport } => {
            if matches!(transport.protocol().as_str(), "cxl" | "nvmeof") {
                return Err(AttachError::IncompatibleTransportProtocol {
                    kind: SegmentKind::Memory,
                    protocol: transport.protocol().clone(),
                });
            }
            if region.base() == 0 {
                return Err(AttachError::ZeroBaseAddress);
            }
            validate_direct_range(*region, transport)
        }
        SegmentConfiguration::Nof { region, transport } => {
            validate_direct_range(*region, transport)
        }
        SegmentConfiguration::Cxl { arena, .. } => {
            if arena.id().as_str().is_empty() {
                return Err(AttachError::EmptyCxlArenaId);
            }
            if arena.capacity_bytes() == 0 {
                return Err(AttachError::ZeroSize);
            }
            Ok(())
        }
        SegmentConfiguration::LocalSsd { .. } => Ok(()),
    }
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
