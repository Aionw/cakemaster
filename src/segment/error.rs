//! Errors returned by segment-pool operations.

use super::identity::{ClientId, SegmentId};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParseTransportProtocolError;

impl fmt::Display for ParseTransportProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport protocol must be a non-empty lowercase ASCII identifier")
    }
}

impl std::error::Error for ParseTransportProtocolError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolConfigError {
    pub(super) max_allocator_nodes_per_segment: u32,
}

impl fmt::Display for PoolConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "max allocator nodes per segment must be in 3..{}, got {}",
            u32::MAX - 1,
            self.max_allocator_nodes_per_segment
        )
    }
}

impl std::error::Error for PoolConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttachError {
    NilSegmentId,
    NilOwnerId,
    EmptyName,
    InvalidBase,
    EmptyTransportEndpoint,
    InvalidTransportProtocol,
    EmptyHostId,
    ZeroSize,
    AddressOverflow,
    ConflictingSegmentId(SegmentId),
    OverlappingAddressRange { existing: SegmentId },
}

impl fmt::Display for AttachError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NilSegmentId => formatter.write_str("segment id must not be nil"),
            Self::NilOwnerId => formatter.write_str("segment owner id must not be nil"),
            Self::EmptyName => formatter.write_str("segment name must not be empty"),
            Self::InvalidBase => {
                formatter.write_str("memory segment base address must not be zero")
            }
            Self::EmptyTransportEndpoint => {
                formatter.write_str("transport endpoint must not be empty")
            }
            Self::InvalidTransportProtocol => {
                formatter.write_str("transport protocol is not a valid identifier")
            }
            Self::EmptyHostId => formatter.write_str("host id must not be empty when present"),
            Self::ZeroSize => formatter.write_str("segment size must not be zero"),
            Self::AddressOverflow => formatter.write_str("segment address range overflows u64"),
            Self::ConflictingSegmentId(id) => {
                write!(
                    formatter,
                    "segment id {id} is already attached with different metadata"
                )
            }
            Self::OverlappingAddressRange { existing } => write!(
                formatter,
                "segment address range overlaps existing segment {existing} in the same address space"
            ),
        }
    }
}

impl std::error::Error for AttachError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleError {
    NotFound(SegmentId),
    OwnerMismatch {
        segment: SegmentId,
        expected: ClientId,
        actual: ClientId,
    },
    StillAccepting(SegmentId),
    Busy {
        segment: SegmentId,
        live_allocations: u64,
    },
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(formatter, "segment {id} was not found"),
            Self::OwnerMismatch {
                segment,
                expected,
                actual,
            } => write!(
                formatter,
                "segment {segment} belongs to client {expected}, not {actual}"
            ),
            Self::StillAccepting(id) => {
                write!(formatter, "segment {id} must be quiesced before removal")
            }
            Self::Busy {
                segment,
                live_allocations,
            } => write!(
                formatter,
                "segment {segment} still has {live_allocations} live allocations"
            ),
        }
    }
}

impl std::error::Error for LifecycleError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReserveError {
    ZeroSize,
    ForeignCandidate,
    NotFound(SegmentId),
    NotAccepting(SegmentId),
    OutOfSpace(SegmentId),
    AddressOverflow(SegmentId),
}

impl fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSize => formatter.write_str("reservation size must not be zero"),
            Self::ForeignCandidate => {
                formatter.write_str("segment candidate belongs to another pool")
            }
            Self::NotFound(id) => write!(formatter, "segment {id} was not found"),
            Self::NotAccepting(id) => {
                write!(formatter, "segment {id} is not accepting reservations")
            }
            Self::OutOfSpace(id) => write!(formatter, "segment {id} has no suitable free range"),
            Self::AddressOverflow(id) => {
                write!(formatter, "allocated address overflowed in segment {id}")
            }
        }
    }
}

impl std::error::Error for ReserveError {}
