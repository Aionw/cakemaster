use super::error::ParseTransportProtocolError;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

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
    NvmeOf,
    Custom(Arc<str>),
}

impl TransportProtocol {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Tcp => "tcp",
            Self::Rdma => "rdma",
            Self::Cxl => "cxl",
            Self::NvmeOf => "nvmeof",
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
            "nvmeof" => Self::NvmeOf,
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
