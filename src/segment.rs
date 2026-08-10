//! Memory segment registration, placement, and range reservations.

pub mod config;
mod descriptor;
pub mod error;
mod identity;
mod offset_allocator;
pub mod placement;
mod pool;
mod reservation;
mod spec;
pub mod stats;
mod transport;

pub use config::SegmentPoolConfig;
pub use descriptor::{
    MemoryDescriptor, MemoryDescriptorRef, MemoryRegion, NofDescriptor, NofDescriptorRef,
    ReservationDescriptor, ReservationDescriptorRef, SegmentTopology,
};
pub use identity::{ClientId, SegmentId, SegmentIdentity};
pub use pool::{AttachOutcome, PoolSnapshot, SegmentCandidate, SegmentPool};
pub use reservation::Reservation;
pub use spec::{
    CxlArenaId, CxlArenaSpec, CxlSegmentSpec, MemorySegmentSpec, NofSegmentSpec, ReplicaClass,
    SegmentKind, SegmentMetadata, SegmentResourceId, SegmentSpec,
};
pub use transport::{TransportEndpoint, TransportProtocol};
