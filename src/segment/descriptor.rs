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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofDescriptor {
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl NofDescriptor {
    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofDescriptorRef<'a> {
    region: MemoryRegion,
    transport: &'a TransportEndpoint,
}

impl<'a> NofDescriptorRef<'a> {
    pub(crate) const fn new(region: MemoryRegion, transport: &'a TransportEndpoint) -> Self {
        Self { region, transport }
    }

    pub const fn region(self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(self) -> &'a TransportEndpoint {
        self.transport
    }

    pub fn to_owned(self) -> NofDescriptor {
        NofDescriptor {
            region: self.region,
            transport: self.transport.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReservationDescriptor {
    Memory(MemoryDescriptor),
    Nof(NofDescriptor),
}

impl ReservationDescriptor {
    pub const fn region(&self) -> MemoryRegion {
        match self {
            Self::Memory(descriptor) => descriptor.region(),
            Self::Nof(descriptor) => descriptor.region(),
        }
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        match self {
            Self::Memory(descriptor) => descriptor.transport(),
            Self::Nof(descriptor) => descriptor.transport(),
        }
    }

    pub const fn memory(&self) -> Option<&MemoryDescriptor> {
        match self {
            Self::Memory(descriptor) => Some(descriptor),
            Self::Nof(_) => None,
        }
    }

    pub const fn nof(&self) -> Option<&NofDescriptor> {
        match self {
            Self::Memory(_) => None,
            Self::Nof(descriptor) => Some(descriptor),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReservationDescriptorRef<'a> {
    Memory(MemoryDescriptorRef<'a>),
    Nof(NofDescriptorRef<'a>),
}

impl<'a> ReservationDescriptorRef<'a> {
    pub const fn region(self) -> MemoryRegion {
        match self {
            Self::Memory(descriptor) => descriptor.region(),
            Self::Nof(descriptor) => descriptor.region(),
        }
    }

    pub const fn transport(self) -> &'a TransportEndpoint {
        match self {
            Self::Memory(descriptor) => descriptor.transport(),
            Self::Nof(descriptor) => descriptor.transport(),
        }
    }

    pub const fn memory(self) -> Option<MemoryDescriptorRef<'a>> {
        match self {
            Self::Memory(descriptor) => Some(descriptor),
            Self::Nof(_) => None,
        }
    }

    pub const fn nof(self) -> Option<NofDescriptorRef<'a>> {
        match self {
            Self::Memory(_) => None,
            Self::Nof(descriptor) => Some(descriptor),
        }
    }

    pub fn to_owned(self) -> ReservationDescriptor {
        match self {
            Self::Memory(descriptor) => ReservationDescriptor::Memory(descriptor.to_owned()),
            Self::Nof(descriptor) => ReservationDescriptor::Nof(descriptor.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSsdDescriptor {
    client_id: super::identity::ClientId,
    object_size: u64,
    transport_endpoint: Arc<str>,
}

impl LocalSsdDescriptor {
    pub const fn client_id(&self) -> super::identity::ClientId {
        self.client_id
    }

    pub const fn object_size(&self) -> u64 {
        self.object_size
    }

    pub fn transport_endpoint(&self) -> &str {
        &self.transport_endpoint
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalSsdDescriptorRef<'a> {
    client_id: super::identity::ClientId,
    object_size: u64,
    transport_endpoint: &'a str,
}

impl<'a> LocalSsdDescriptorRef<'a> {
    pub(crate) const fn new(
        client_id: super::identity::ClientId,
        object_size: u64,
        transport_endpoint: &'a str,
    ) -> Self {
        Self {
            client_id,
            object_size,
            transport_endpoint,
        }
    }

    pub const fn client_id(self) -> super::identity::ClientId {
        self.client_id
    }

    pub const fn object_size(self) -> u64 {
        self.object_size
    }

    pub const fn transport_endpoint(self) -> &'a str {
        self.transport_endpoint
    }

    pub fn to_owned(self) -> LocalSsdDescriptor {
        LocalSsdDescriptor {
            client_id: self.client_id,
            object_size: self.object_size,
            transport_endpoint: Arc::from(self.transport_endpoint),
        }
    }
}
