//! Errors returned by segment-pool operations.

use super::config::{MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE, MIN_ALLOCATOR_NODES_PER_SEGMENT};
use super::identity::{ClientId, SegmentId};
use super::spec::{CxlArenaId, SegmentKind};
use super::transport::TransportProtocol;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(
    "transport protocol must be non-empty and contain only lowercase ASCII letters, digits, `_`, or `-`"
)]
pub struct ParseTransportProtocolError;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(
    "`max_allocator_nodes_per_segment` must be in {MIN_ALLOCATOR_NODES_PER_SEGMENT}..{MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE}, got {max_allocator_nodes_per_segment}"
)]
pub struct PoolConfigError {
    pub(super) max_allocator_nodes_per_segment: u32,
}

impl PoolConfigError {
    pub const fn max_allocator_nodes_per_segment(&self) -> u32 {
        self.max_allocator_nodes_per_segment
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AttachError {
    #[error("segment id must not be nil")]
    NilSegmentId,
    #[error("segment owner id must not be nil")]
    NilOwnerId,
    #[error("segment name must not be empty")]
    EmptyName,
    #[error("memory segment base address must not be zero")]
    ZeroBaseAddress,
    #[error("transport endpoint must not be empty")]
    EmptyTransportEndpoint,
    #[error("invalid transport protocol `{protocol}`")]
    InvalidTransportProtocol {
        protocol: Arc<str>,
        #[source]
        source: ParseTransportProtocolError,
    },
    #[error("transport protocol {protocol:?} is incompatible with {kind:?} segments")]
    IncompatibleTransportProtocol {
        kind: SegmentKind,
        protocol: TransportProtocol,
    },
    #[error("host id must not be empty when present")]
    EmptyHostId,
    #[error("segment size must not be zero")]
    ZeroSize,
    #[error("segment address range overflows u64")]
    AddressOverflow,
    #[error("segment id {0} is already attached with different metadata")]
    ConflictingSegmentId(SegmentId),
    #[error("segment address range overlaps existing segment {existing} in the same address space")]
    OverlappingAddressRange { existing: SegmentId },
    #[error("NVMe-oF namespace endpoint is already attached as segment {existing}")]
    DuplicateNofEndpoint { existing: SegmentId },
    #[error("CXL arena id must not be empty")]
    EmptyCxlArenaId,
    #[error("CXL arena {arena:?} is already registered with different capacity")]
    ConflictingCxlArena { arena: CxlArenaId },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LifecycleError {
    #[error("segment {0} was not found")]
    NotFound(SegmentId),
    #[error("segment {segment} belongs to client {expected}, not {actual}")]
    OwnerMismatch {
        segment: SegmentId,
        expected: ClientId,
        actual: ClientId,
    },
    #[error("segment {0} must be quiesced before removal")]
    StillAccepting(SegmentId),
    #[error("segment {segment} still has {live_allocations} live allocations")]
    Busy {
        segment: SegmentId,
        live_allocations: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReserveError {
    #[error("reservation size must not be zero")]
    ZeroSize,
    #[error("segment candidate belongs to another pool")]
    ForeignCandidate,
    #[error("segment {0} was not found")]
    NotFound(SegmentId),
    #[error("segment {0} is not accepting reservations")]
    NotAccepting(SegmentId),
    #[error("segment {0} has no suitable free range")]
    OutOfSpace(SegmentId),
    #[error("allocated address overflowed in segment {0}")]
    AddressOverflow(SegmentId),
}
