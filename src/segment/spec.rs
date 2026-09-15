use super::{MemoryRegion, SegmentIdentity, TransportEndpoint};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReplicaClass {
    Memory,
}

/// Immutable registration of a dedicated memory range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentSpec {
    identity: SegmentIdentity,
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl SegmentSpec {
    pub fn memory(
        identity: SegmentIdentity,
        region: MemoryRegion,
        transport: TransportEndpoint,
    ) -> Self {
        Self {
            identity,
            region,
            transport,
        }
    }
    pub const fn identity(&self) -> &SegmentIdentity {
        &self.identity
    }
    pub const fn region(&self) -> MemoryRegion {
        self.region
    }
    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }

    pub const fn replica_class(&self) -> ReplicaClass {
        ReplicaClass::Memory
    }
}
