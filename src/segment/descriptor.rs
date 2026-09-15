use super::transport::TransportEndpoint;

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

/// Owned descriptor for a memory reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeDescriptor {
    region: MemoryRegion,
    transport: TransportEndpoint,
}

impl RangeDescriptor {
    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }
}

/// Borrowed hot-path view of a [`RangeDescriptor`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeDescriptorRef<'a> {
    region: MemoryRegion,
    transport: &'a TransportEndpoint,
}

impl<'a> RangeDescriptorRef<'a> {
    pub(crate) const fn new(region: MemoryRegion, transport: &'a TransportEndpoint) -> Self {
        Self { region, transport }
    }

    pub const fn region(self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(self) -> &'a TransportEndpoint {
        self.transport
    }

    pub fn to_owned(self) -> RangeDescriptor {
        RangeDescriptor {
            region: self.region,
            transport: self.transport.clone(),
        }
    }
}

pub type ReservationDescriptor = RangeDescriptor;
pub type ReservationDescriptorRef<'a> = RangeDescriptorRef<'a>;
