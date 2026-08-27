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
    "invalid segment-pool configuration: max allocator nodes {max_allocator_nodes_per_segment} must be in {MIN_ALLOCATOR_NODES_PER_SEGMENT}..{MAX_ALLOCATOR_NODES_PER_SEGMENT_EXCLUSIVE} and provide at least {MIN_ALLOCATOR_NODES_PER_SEGMENT} nodes per allocator shard; allocator shards {allocator_shards} must be positive"
)]
pub struct PoolConfigError {
    pub(super) max_allocator_nodes_per_segment: u32,
    pub(super) allocator_shards: usize,
}

impl PoolConfigError {
    pub const fn max_allocator_nodes_per_segment(&self) -> u32 {
        self.max_allocator_nodes_per_segment
    }

    pub const fn allocator_shards(&self) -> usize {
        self.allocator_shards
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
    #[error("client already has LocalSSD segment {existing}")]
    DuplicateLocalSsdOwner { existing: SegmentId },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SegmentStateError {
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
    #[error("segment {segment} still has {active_allocations} active allocations")]
    Busy {
        segment: SegmentId,
        active_allocations: u64,
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
    #[error("segment {0} does not support direct reservations")]
    NotDirectlyAllocatable(SegmentId),
    #[error("segment {0} is not accepting reservations")]
    NotAccepting(SegmentId),
    #[error("segment {0} has no suitable free range")]
    OutOfSpace(SegmentId),
    #[error("allocated address overflowed in segment {0}")]
    AddressOverflow(SegmentId),
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalSsdError {
    #[error("offload size must not be zero")]
    ZeroSize,
    #[error("LocalSSD object transport endpoint must not be empty")]
    EmptyTransportEndpoint,
    #[error("segment candidate belongs to another pool")]
    ForeignCandidate,
    #[error("segment {0} was not found")]
    NotFound(SegmentId),
    #[error("segment {segment} belongs to client {expected}, not {actual}")]
    OwnerMismatch {
        segment: SegmentId,
        expected: ClientId,
        actual: ClientId,
    },
    #[error("segment {0} is not a LocalSSD offload target")]
    NotLocalSsd(SegmentId),
    #[error("LocalSSD segment {0} is not accepting offloads")]
    NotAccepting(SegmentId),
    #[error("LocalSSD segment {0} has offloading disabled")]
    OffloadDisabled(SegmentId),
    #[error("LocalSSD segment {0} has not reported capacity")]
    CapacityNotReported(SegmentId),
    #[error("LocalSSD segment {0} has insufficient reported capacity")]
    OutOfSpace(SegmentId),
}
