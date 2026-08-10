//! Heterogeneous segment registration, placement, offload admission, and
//! lifecycle management.

pub mod config;
mod descriptor;
pub mod error;
mod identity;
mod local_ssd;
mod offset_allocator;
pub mod placement;
mod pool;
mod reservation;
mod spec;
pub mod stats;
mod transport;
mod usage;

pub use config::SegmentPoolConfig;
pub use descriptor::{
    LocalSsdDescriptor, LocalSsdDescriptorRef, MemoryRegion, RangeDescriptor, RangeDescriptorRef,
    ReservationDescriptor, ReservationDescriptorRef,
};
pub use identity::{ClientId, SegmentId, SegmentIdentity};
pub use local_ssd::{LocalSsdLease, LocalSsdStats, OffloadPermit};
pub use pool::{
    AttachOutcome, DirectCandidate, OffloadSnapshot, OffloadTarget, PoolSnapshot, SegmentHandle,
    SegmentPool,
};
pub use reservation::Reservation;
pub use spec::{
    CxlArenaId, CxlArenaSpec, ReplicaClass, SegmentKind, SegmentResourceId, SegmentSpec,
};
pub use transport::{TransportEndpoint, TransportProtocol};
