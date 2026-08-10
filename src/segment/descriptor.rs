use super::transport::TransportEndpoint;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MemoryRegion {
    base: u64,
    size: u64,
}

impl MemoryRegion {
    pub const fn new(base: u64, size: u64) -> Self {
        Self { base, size }
    }

    pub const fn base(self) -> u64 {
        self.base
    }

    pub const fn size(self) -> u64 {
        self.size
    }

    pub const fn end(self) -> Option<u64> {
        self.base.checked_add(self.size)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SegmentTopology {
    host_id: Option<Arc<str>>,
}

impl SegmentTopology {
    pub fn on_host(host_id: impl Into<Arc<str>>) -> Self {
        Self {
            host_id: Some(host_id.into()),
        }
    }

    pub fn host_id(&self) -> Option<&str> {
        self.host_id.as_deref()
    }

    pub(crate) fn host_id_arc(&self) -> Option<Arc<str>> {
        self.host_id.clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDescriptor {
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl MemoryDescriptor {
    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }
}

/// Zero-copy view of a memory descriptor owned by a live reservation.
///
/// Allocation hot paths should use this view. Call [`Self::to_owned`] only at
/// an ownership boundary such as an RPC queue or persistent metadata record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryDescriptorRef<'a> {
    region: MemoryRegion,
    transport: &'a TransportEndpoint,
}

impl<'a> MemoryDescriptorRef<'a> {
    pub(crate) const fn new(region: MemoryRegion, transport: &'a TransportEndpoint) -> Self {
        Self { region, transport }
    }

    pub const fn region(self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(self) -> &'a TransportEndpoint {
        self.transport
    }

    pub fn to_owned(self) -> MemoryDescriptor {
        MemoryDescriptor {
            region: self.region,
            transport: self.transport.clone(),
        }
    }
}
