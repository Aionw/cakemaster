mod catalog;
mod entry;
mod resource;

use self::catalog::Catalog;
use self::entry::SegmentEntry;
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
use super::stats::SegmentStats;
use parking_lot::RwLock;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

pub struct SegmentPool {
    pool_id: u64,
    config: SegmentPoolConfig,
    catalog: RwLock<Catalog>,
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
        })
    }

    pub fn attach(&self, spec: SegmentSpec) -> Result<AttachOutcome, AttachError> {
        validate_spec(&spec)?;
        self.catalog
            .write()
            .attach(spec, self.config.max_allocator_nodes_per_segment)
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        self.snapshot_for(ReplicaClass::Memory)
    }

    pub fn snapshot_for(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        self.catalog.read().snapshot(replica_class)
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
        let candidate = segment
            .direct_candidate()
            .ok_or(ReserveError::NotDirectlyAllocatable(id))?;
        self.reserve(&candidate, bytes)
    }

    pub fn report_local_ssd_capacity(
        &self,
        owner: ClientId,
        id: SegmentId,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        self.catalog
            .read()
            .report_local_ssd_capacity(owner, id, capacity_bytes)
    }

    pub fn set_local_ssd_offload_enabled(
        &self,
        owner: ClientId,
        id: SegmentId,
        enabled: bool,
    ) -> Result<(), LocalSsdError> {
        self.catalog
            .write()
            .set_local_ssd_offload_enabled(owner, id, enabled)
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
        self.catalog.write().quiesce(owner, id)
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.catalog.write().reactivate(owner, id)
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), SegmentStateError> {
        self.catalog.write().remove(owner, id)
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
