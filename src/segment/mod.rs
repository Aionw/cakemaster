//! Memory segment registration, placement, and range reservations.

mod offset_allocator;
mod placement;
mod pool;
mod types;

pub use placement::{
    AllocationSpec, FailureDomain, FreeCapacityPolicy, FulfillmentPolicy, PlacementConstraints,
    PlacementError, PlacementPolicy, PlacementRequest, ReplicaAllocator, ReplicaPolicy,
    ReservationSet,
};
pub use pool::{
    AttachError, AttachOutcome, DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT, LifecycleError,
    PoolConfigError, PoolSnapshot, Reservation, ReserveError, SegmentCandidate, SegmentPool,
    SegmentPoolConfig,
};
pub use types::{
    ClientId, MemoryDescriptor, MemoryDescriptorRef, MemoryRegion, MemorySegmentSpec,
    ParseTransportProtocolError, SegmentId, SegmentIdentity, SegmentReservationStats,
    SegmentSpaceStats, SegmentState, SegmentStats, SegmentTopology, TransportEndpoint,
    TransportProtocol,
};
