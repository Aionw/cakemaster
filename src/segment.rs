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

pub use config::SegmentPoolConfig;
pub use descriptor::{
    LocalSsdDescriptor, LocalSsdDescriptorRef, MemoryDescriptor, MemoryDescriptorRef, MemoryRegion,
    NofDescriptor, NofDescriptorRef, ReservationDescriptor, ReservationDescriptorRef,
    SegmentTopology,
};
pub use identity::{ClientId, SegmentId, SegmentIdentity};
pub use local_ssd::{LocalSsdLease, LocalSsdStats, OffloadPermit};
pub use pool::{AttachOutcome, OffloadSnapshot, PoolSnapshot, SegmentCandidate, SegmentPool};
pub use reservation::Reservation;
pub use spec::{
    CxlArenaId, CxlArenaSpec, CxlSegmentSpec, LocalSsdSegmentSpec, MemorySegmentSpec,
    NofSegmentSpec, ReplicaClass, SegmentKind, SegmentMetadata, SegmentResourceId, SegmentSpec,
};
pub use transport::{TransportEndpoint, TransportProtocol};
