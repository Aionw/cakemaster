use super::descriptor::MemoryRegion;
use super::identity::{ClientId, SegmentId, SegmentIdentity};
use super::transport::{TransportEndpoint, TransportProtocol};
use std::sync::Arc;

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

/// Concrete storage implementation mounted in a [`SegmentPool`](super::SegmentPool).
///
/// A segment kind describes how capacity is provided. It is deliberately
/// separate from [`ReplicaClass`]: CXL is a distinct segment kind but still
/// produces memory replicas on the Mooncake wire protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SegmentKind {
    Memory,
    Cxl,
    Nof,
    LocalSsd,
}

/// Replica family produced by a segment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ReplicaClass {
    Memory,
    Nof,
    LocalSsd,
}

/// Identity of the physical capacity provider behind a logical segment.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SegmentResourceId {
    Dedicated(SegmentId),
    CxlArena(CxlArenaId),
    NofNamespace(TransportEndpoint),
    LocalSsd(ClientId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SegmentConfiguration {
    Memory {
        region: MemoryRegion,
        transport: TransportEndpoint,
    },
    Cxl {
        arena: CxlArenaSpec,
        transport: TransportEndpoint,
    },
    Nof {
        region: MemoryRegion,
        transport: TransportEndpoint,
    },
    LocalSsd {
        initial_offload_enabled: bool,
    },
}

/// Immutable mount facts for one logical segment.
///
/// Runtime state such as acceptance/removal state, allocator usage, heartbeat
/// capacity, and offload admission lives in the catalog entry instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentSpec {
    identity: SegmentIdentity,
    configuration: SegmentConfiguration,
}

impl SegmentSpec {
    pub fn memory(
        identity: SegmentIdentity,
        region: MemoryRegion,
        transport: TransportEndpoint,
    ) -> Self {
        Self {
            identity,
            configuration: SegmentConfiguration::Memory { region, transport },
        }
    }

    /// Creates an allocatable range in an NVMe-oF namespace.
    ///
    /// `region.base()` is a namespace offset, so zero is valid. The protocol
    /// is fixed here to prevent the storage kind drifting from its descriptor.
    pub fn nof(
        identity: SegmentIdentity,
        region: MemoryRegion,
        endpoint: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            identity,
            configuration: SegmentConfiguration::Nof {
                region,
                transport: TransportEndpoint::new(TransportProtocol::NvmeOf, endpoint),
            },
        }
    }

    /// Creates a client-visible logical mount backed by a shared CXL arena.
    pub fn cxl(identity: SegmentIdentity, arena: CxlArenaSpec) -> Self {
        let endpoint = Arc::<str>::from(identity.name());
        Self {
            identity,
            configuration: SegmentConfiguration::Cxl {
                arena,
                transport: TransportEndpoint::new(TransportProtocol::Cxl, endpoint),
            },
        }
    }

    /// Creates a client-local SSD target managed by asynchronous offload.
    ///
    /// Capacity is intentionally absent and must be reported by heartbeat.
    pub fn local_ssd(identity: SegmentIdentity, initial_offload_enabled: bool) -> Self {
        Self {
            identity,
            configuration: SegmentConfiguration::LocalSsd {
                initial_offload_enabled,
            },
        }
    }

    pub const fn identity(&self) -> &SegmentIdentity {
        &self.identity
    }

    pub const fn kind(&self) -> SegmentKind {
        match &self.configuration {
            SegmentConfiguration::Memory { .. } => SegmentKind::Memory,
            SegmentConfiguration::Cxl { .. } => SegmentKind::Cxl,
            SegmentConfiguration::Nof { .. } => SegmentKind::Nof,
            SegmentConfiguration::LocalSsd { .. } => SegmentKind::LocalSsd,
        }
    }

    pub const fn replica_class(&self) -> ReplicaClass {
        match &self.configuration {
            SegmentConfiguration::Memory { .. } | SegmentConfiguration::Cxl { .. } => {
                ReplicaClass::Memory
            }
            SegmentConfiguration::Nof { .. } => ReplicaClass::Nof,
            SegmentConfiguration::LocalSsd { .. } => ReplicaClass::LocalSsd,
        }
    }

    pub fn resource_id(&self) -> SegmentResourceId {
        match &self.configuration {
            SegmentConfiguration::Memory { .. } => {
                SegmentResourceId::Dedicated(self.identity().id())
            }
            SegmentConfiguration::Cxl { arena, .. } => {
                SegmentResourceId::CxlArena(arena.id().clone())
            }
            SegmentConfiguration::Nof { transport, .. } => {
                SegmentResourceId::NofNamespace(transport.clone())
            }
            SegmentConfiguration::LocalSsd { .. } => {
                SegmentResourceId::LocalSsd(self.identity().owner())
            }
        }
    }

    /// Returns the mounted range for Memory and NoF. CXL capacity belongs to
    /// its arena and LocalSSD has no direct range.
    pub const fn region(&self) -> Option<MemoryRegion> {
        match &self.configuration {
            SegmentConfiguration::Memory { region, .. }
            | SegmentConfiguration::Nof { region, .. } => Some(*region),
            SegmentConfiguration::Cxl { .. } | SegmentConfiguration::LocalSsd { .. } => None,
        }
    }

    pub const fn transport(&self) -> Option<&TransportEndpoint> {
        match &self.configuration {
            SegmentConfiguration::Memory { transport, .. }
            | SegmentConfiguration::Cxl { transport, .. }
            | SegmentConfiguration::Nof { transport, .. } => Some(transport),
            SegmentConfiguration::LocalSsd { .. } => None,
        }
    }

    pub const fn cxl_arena(&self) -> Option<&CxlArenaSpec> {
        match &self.configuration {
            SegmentConfiguration::Cxl { arena, .. } => Some(arena),
            SegmentConfiguration::Memory { .. }
            | SegmentConfiguration::Nof { .. }
            | SegmentConfiguration::LocalSsd { .. } => None,
        }
    }

    pub const fn initial_offload_enabled(&self) -> Option<bool> {
        match &self.configuration {
            SegmentConfiguration::LocalSsd {
                initial_offload_enabled,
            } => Some(*initial_offload_enabled),
            SegmentConfiguration::Memory { .. }
            | SegmentConfiguration::Cxl { .. }
            | SegmentConfiguration::Nof { .. } => None,
        }
    }

    pub(crate) const fn configuration(&self) -> &SegmentConfiguration {
        &self.configuration
    }

    pub(crate) const fn direct_region(&self) -> Option<MemoryRegion> {
        match &self.configuration {
            SegmentConfiguration::Memory { region, .. }
            | SegmentConfiguration::Nof { region, .. } => Some(*region),
            SegmentConfiguration::Cxl { arena, .. } => {
                Some(MemoryRegion::new(0, arena.capacity_bytes()))
            }
            SegmentConfiguration::LocalSsd { .. } => None,
        }
    }
}
