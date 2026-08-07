use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

macro_rules! uuid_pair_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name {
            high: u64,
            low: u64,
        }

        impl $name {
            pub const NIL: Self = Self { high: 0, low: 0 };

            pub const fn new(high: u64, low: u64) -> Self {
                Self { high, low }
            }

            pub const fn high(self) -> u64 {
                self.high
            }

            pub const fn low(self) -> u64 {
                self.low
            }

            pub const fn is_nil(self) -> bool {
                self.high == 0 && self.low == 0
            }
        }

        impl From<u128> for $name {
            fn from(value: u128) -> Self {
                Self {
                    high: (value >> 64) as u64,
                    low: value as u64,
                }
            }
        }

        impl From<$name> for u128 {
            fn from(value: $name) -> Self {
                (u128::from(value.high) << 64) | u128::from(value.low)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:016x}{:016x}", self.high, self.low)
            }
        }
    };
}

uuid_pair_id!(SegmentId);
uuid_pair_id!(ClientId);

/// Transfer protocol used to reach a memory segment.
///
/// The common protocols are strongly typed so placement and capability checks
/// do not depend on string literals. `Custom` keeps the core compatible with
/// transport plugins that are not known at compile time.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TransportProtocol {
    Tcp,
    Rdma,
    Cxl,
    Custom(Arc<str>),
}

impl TransportProtocol {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Tcp => "tcp",
            Self::Rdma => "rdma",
            Self::Cxl => "cxl",
            Self::Custom(protocol) => protocol,
        }
    }
}

impl fmt::Display for TransportProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TransportProtocol {
    type Err = ParseTransportProtocolError;

    fn from_str(protocol: &str) -> Result<Self, Self::Err> {
        if protocol.is_empty()
            || !protocol.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte)
            })
        {
            return Err(ParseTransportProtocolError);
        }

        Ok(match protocol {
            "tcp" => Self::Tcp,
            "rdma" => Self::Rdma,
            "cxl" => Self::Cxl,
            custom => Self::Custom(Arc::from(custom)),
        })
    }
}

impl TryFrom<&str> for TransportProtocol {
    type Error = ParseTransportProtocolError;

    fn try_from(protocol: &str) -> Result<Self, Self::Error> {
        protocol.parse()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParseTransportProtocolError;

impl fmt::Display for ParseTransportProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport protocol must be a non-empty lowercase ASCII identifier")
    }
}

impl std::error::Error for ParseTransportProtocolError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TransportEndpoint {
    protocol: TransportProtocol,
    endpoint: Arc<str>,
}

impl TransportEndpoint {
    pub fn new(protocol: TransportProtocol, endpoint: impl Into<Arc<str>>) -> Self {
        Self {
            protocol,
            endpoint: endpoint.into(),
        }
    }

    pub const fn protocol(&self) -> &TransportProtocol {
        &self.protocol
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentIdentity {
    id: SegmentId,
    owner: ClientId,
    name: Arc<str>,
}

impl SegmentIdentity {
    pub fn new(id: SegmentId, owner: ClientId, name: impl Into<Arc<str>>) -> Self {
        Self {
            id,
            owner,
            name: name.into(),
        }
    }

    pub const fn id(&self) -> SegmentId {
        self.id
    }

    pub const fn owner(&self) -> ClientId {
        self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

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
pub struct MemorySegmentSpec {
    identity: SegmentIdentity,
    region: MemoryRegion,
    transport: TransportEndpoint,
    topology: SegmentTopology,
}

impl MemorySegmentSpec {
    pub fn new(
        identity: SegmentIdentity,
        region: MemoryRegion,
        transport: TransportEndpoint,
    ) -> Self {
        Self {
            identity,
            region,
            transport,
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

    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub const fn transport(&self) -> &TransportEndpoint {
        &self.transport
    }

    pub const fn topology(&self) -> &SegmentTopology {
        &self.topology
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentState {
    Accepting,
    Quiesced,
    Removed,
}

impl SegmentState {
    pub const fn is_accepting(self) -> bool {
        matches!(self, Self::Accepting)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentSpaceStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub largest_free_region_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentReservationStats {
    pub live: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStats {
    pub space: SegmentSpaceStats,
    pub reservations: SegmentReservationStats,
    pub state: SegmentState,
}
