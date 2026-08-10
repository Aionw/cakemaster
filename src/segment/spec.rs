use super::descriptor::{MemoryRegion, SegmentTopology};
use super::identity::{SegmentId, SegmentIdentity};
use super::transport::{TransportEndpoint, TransportProtocol};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentMetadata {
    identity: SegmentIdentity,
    topology: SegmentTopology,
}

impl SegmentMetadata {
    pub fn new(identity: SegmentIdentity) -> Self {
        Self {
            identity,
            topology: SegmentTopology::default(),
        }
    }

    pub fn with_topology(mut self, topology: SegmentTopology) -> Self {
        self.topology = topology;
        self
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        &self.identity
    }

    pub const fn topology(&self) -> &SegmentTopology {
        &self.topology
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemorySegmentSpec {
    metadata: SegmentMetadata,
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl MemorySegmentSpec {
    pub fn new(
        identity: SegmentIdentity,
        region: MemoryRegion,
        transport: TransportEndpoint,
    ) -> Self {
        Self {
            metadata: SegmentMetadata::new(identity),
            region,
            transport,
        }
    }

    pub fn with_topology(mut self, topology: SegmentTopology) -> Self {
        self.metadata = self.metadata.with_topology(topology);
        self
    }

    pub const fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        self.metadata.identity()
    }

    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }

    pub const fn topology(&self) -> &SegmentTopology {
        self.metadata.topology()
    }
}

/// An allocatable byte range in an NVMe-oF namespace.
///
/// Unlike a process memory segment, `region.base()` is a namespace offset and
/// zero is a valid start. The transport is fixed to `nvmeof` so the storage
/// kind cannot silently drift from its wire descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofSegmentSpec {
    metadata: SegmentMetadata,
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl NofSegmentSpec {
    pub fn new(
        identity: SegmentIdentity,
        region: MemoryRegion,
        endpoint: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            metadata: SegmentMetadata::new(identity),
            region,
            transport: TransportEndpoint::new(TransportProtocol::NvmeOf, endpoint),
        }
    }

    pub fn with_topology(mut self, topology: SegmentTopology) -> Self {
        self.metadata = self.metadata.with_topology(topology);
        self
    }

    pub const fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        self.metadata.identity()
    }

    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }

    pub const fn topology(&self) -> &SegmentTopology {
        self.metadata.topology()
    }
}

/// Stable identity of one physical CXL allocation arena.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CxlArenaId(Arc<str>);

impl CxlArenaId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Physical capacity shared by every logical CXL segment that names this
/// arena.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CxlArenaSpec {
    id: CxlArenaId,
    capacity_bytes: u64,
}

impl CxlArenaSpec {
    pub const fn new(id: CxlArenaId, capacity_bytes: u64) -> Self {
        Self { id, capacity_bytes }
    }

    pub const fn id(&self) -> &CxlArenaId {
        &self.id
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }
}

/// A client-visible logical mount backed by a shared CXL arena.
///
/// Reservations allocate offsets from the arena. The resulting descriptor is
/// a memory descriptor with protocol `cxl` and the logical segment name as its
/// endpoint, matching Mooncake's wire semantics without conflating CXL with a
/// regular process address range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CxlSegmentSpec {
    metadata: SegmentMetadata,
    arena: CxlArenaSpec,
    transport: TransportEndpoint,
}

impl CxlSegmentSpec {
    pub fn new(identity: SegmentIdentity, arena: CxlArenaSpec) -> Self {
        let endpoint = Arc::<str>::from(identity.name());
        Self {
            metadata: SegmentMetadata::new(identity),
            arena,
            transport: TransportEndpoint::new(TransportProtocol::Cxl, endpoint),
        }
    }

    pub fn with_topology(mut self, topology: SegmentTopology) -> Self {
        self.metadata = self.metadata.with_topology(topology);
        self
    }

    pub const fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        self.metadata.identity()
    }

    pub const fn arena(&self) -> &CxlArenaSpec {
        &self.arena
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }

    pub const fn topology(&self) -> &SegmentTopology {
        self.metadata.topology()
    }
}

/// Concrete storage implementation mounted in a [`SegmentPool`](super::SegmentPool).
///
/// A segment kind describes how capacity is provided. It is deliberately
/// separate from [`ReplicaClass`]: for example, CXL is a distinct segment
/// backend but still produces memory replicas on the Mooncake wire protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SegmentKind {
    Memory,
    Cxl,
    Nof,
}

/// Replica family produced by a segment.
///
/// Placement operates on replica classes, while attachment and lifecycle
/// management operate on concrete [`SegmentKind`] values.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ReplicaClass {
    Memory,
    Nof,
}

/// Identity of the physical capacity provider behind a logical segment.
///
/// Dedicated memory segments use their segment id. Shared backends such as
/// CXL use an arena identity so placement does not mistake two logical mounts
/// for independent capacity or failure domains.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SegmentResourceId {
    Dedicated(SegmentId),
    CxlArena(CxlArenaId),
    NofNamespace(TransportEndpoint),
}

/// Type-safe specification for every segment backend known to the pool.
///
/// New backends extend this enum and implement their construction and
/// validation at the backend boundary; the catalog only consumes the common
/// identity, topology, kind, and replica-class views below.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SegmentSpec {
    Memory(MemorySegmentSpec),
    Cxl(CxlSegmentSpec),
    Nof(NofSegmentSpec),
}

impl SegmentSpec {
    pub const fn kind(&self) -> SegmentKind {
        match self {
            Self::Memory(_) => SegmentKind::Memory,
            Self::Cxl(_) => SegmentKind::Cxl,
            Self::Nof(_) => SegmentKind::Nof,
        }
    }

    pub const fn replica_class(&self) -> ReplicaClass {
        match self {
            Self::Memory(_) | Self::Cxl(_) => ReplicaClass::Memory,
            Self::Nof(_) => ReplicaClass::Nof,
        }
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        self.metadata().identity()
    }

    pub const fn topology(&self) -> &SegmentTopology {
        self.metadata().topology()
    }

    pub fn resource_id(&self) -> SegmentResourceId {
        match self {
            Self::Memory(spec) => SegmentResourceId::Dedicated(spec.identity().id()),
            Self::Cxl(spec) => SegmentResourceId::CxlArena(spec.arena().id().clone()),
            Self::Nof(spec) => SegmentResourceId::NofNamespace(spec.transport().clone()),
        }
    }

    pub const fn metadata(&self) -> &SegmentMetadata {
        match self {
            Self::Memory(spec) => spec.metadata(),
            Self::Cxl(spec) => spec.metadata(),
            Self::Nof(spec) => spec.metadata(),
        }
    }

    pub const fn memory(&self) -> Option<&MemorySegmentSpec> {
        match self {
            Self::Memory(spec) => Some(spec),
            Self::Cxl(_) | Self::Nof(_) => None,
        }
    }

    pub const fn cxl(&self) -> Option<&CxlSegmentSpec> {
        match self {
            Self::Cxl(spec) => Some(spec),
            Self::Memory(_) | Self::Nof(_) => None,
        }
    }

    pub const fn nof(&self) -> Option<&NofSegmentSpec> {
        match self {
            Self::Memory(_) | Self::Cxl(_) => None,
            Self::Nof(spec) => Some(spec),
        }
    }

    pub(crate) const fn direct_region(&self) -> MemoryRegion {
        match self {
            Self::Memory(spec) => spec.region(),
            Self::Cxl(spec) => MemoryRegion::new(0, spec.arena().capacity_bytes()),
            Self::Nof(spec) => spec.region(),
        }
    }
}

impl From<MemorySegmentSpec> for SegmentSpec {
    fn from(spec: MemorySegmentSpec) -> Self {
        Self::Memory(spec)
    }
}

impl From<NofSegmentSpec> for SegmentSpec {
    fn from(spec: NofSegmentSpec) -> Self {
        Self::Nof(spec)
    }
}

impl From<CxlSegmentSpec> for SegmentSpec {
    fn from(spec: CxlSegmentSpec) -> Self {
        Self::Cxl(spec)
    }
}
