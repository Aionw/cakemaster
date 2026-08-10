mod catalog;

use self::catalog::{Catalog, Segment};
use super::config::{
    MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE, MIN_ALLOCATOR_NODES_PER_SEGMENT, SegmentPoolConfig,
};
use super::error::{AttachError, LifecycleError, PoolConfigError, ReserveError};
use super::identity::{ClientId, SegmentId};
use super::reservation::Reservation;
use super::spec::{
    MemorySegmentSpec, NofSegmentSpec, ReplicaClass, SegmentKind, SegmentMetadata,
    SegmentResourceId, SegmentSpec,
};
use super::stats::SegmentStats;
use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

pub struct SegmentPool {
    pool_id: u64,
    config: SegmentPoolConfig,
    catalog: RwLock<Catalog>,
}

#[derive(Clone)]
pub struct SegmentCandidate {
    pool_id: u64,
    segment: Arc<Segment>,
}

impl SegmentCandidate {
    pub fn id(&self) -> SegmentId {
        self.segment.spec().identity().id()
    }

    pub fn spec(&self) -> &SegmentSpec {
        self.segment.spec()
    }

    pub fn kind(&self) -> SegmentKind {
        self.segment.spec().kind()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.segment.spec().replica_class()
    }

    pub fn resource_id(&self) -> SegmentResourceId {
        self.segment.spec().resource_id()
    }

    pub fn memory_spec(&self) -> Option<&MemorySegmentSpec> {
        self.segment.spec().memory()
    }

    pub fn nof_spec(&self) -> Option<&NofSegmentSpec> {
        self.segment.spec().nof()
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
    replica_class: ReplicaClass,
    candidates: Arc<[SegmentCandidate]>,
}

impl PoolSnapshot {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn replica_class(&self) -> ReplicaClass {
        self.replica_class
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

        let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            pool_id,
            config,
            catalog: RwLock::new(Catalog::new(pool_id)),
        })
    }

    pub fn attach(&self, spec: impl Into<SegmentSpec>) -> Result<AttachOutcome, AttachError> {
        let spec = spec.into();
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

    pub fn candidate(&self, id: SegmentId) -> Option<SegmentCandidate> {
        self.catalog.read().candidate(id)
    }

    pub fn stats(&self, id: SegmentId) -> Option<SegmentStats> {
        self.candidate(id).map(|candidate| candidate.stats())
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
        candidate: &SegmentCandidate,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        if candidate.pool_id != self.pool_id || candidate.segment.pool_id() != self.pool_id {
            return Err(ReserveError::ForeignCandidate);
        }
        candidate.segment.reserve(bytes)
    }

    pub fn reserve_on(&self, id: SegmentId, bytes: u64) -> Result<Reservation, ReserveError> {
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        let candidate = self.candidate(id).ok_or(ReserveError::NotFound(id))?;
        self.reserve(&candidate, bytes)
    }

    pub fn quiesce(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        self.catalog.write().quiesce(owner, id)
    }

    pub fn reactivate(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        self.catalog.write().reactivate(owner, id)
    }

    pub fn remove(&self, owner: ClientId, id: SegmentId) -> Result<(), LifecycleError> {
        self.catalog.write().remove(owner, id)
    }
}

impl Default for SegmentPool {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_spec(spec: &SegmentSpec) -> Result<(), AttachError> {
    match spec {
        SegmentSpec::Memory(spec) => validate_memory_spec(spec),
        SegmentSpec::Nof(spec) => validate_nof_spec(spec),
    }
}

fn validate_memory_spec(spec: &MemorySegmentSpec) -> Result<(), AttachError> {
    validate_metadata(spec.metadata())?;
    if spec.region().base() == 0 {
        return Err(AttachError::ZeroBaseAddress);
    }
    validate_direct_range(spec.region(), spec.transport())
}

fn validate_nof_spec(spec: &NofSegmentSpec) -> Result<(), AttachError> {
    validate_metadata(spec.metadata())?;
    validate_direct_range(spec.region(), spec.transport())
}

fn validate_metadata(metadata: &SegmentMetadata) -> Result<(), AttachError> {
    if metadata.identity().id().is_nil() {
        return Err(AttachError::NilSegmentId);
    }
    if metadata.identity().owner().is_nil() {
        return Err(AttachError::NilOwnerId);
    }
    if metadata.identity().name().is_empty() {
        return Err(AttachError::EmptyName);
    }
    if metadata.topology().host_id().is_some_and(str::is_empty) {
        return Err(AttachError::EmptyHostId);
    }
    Ok(())
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
