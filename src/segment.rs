//! Memory segment registration, placement, and
//! lifecycle management.

pub mod config;
mod descriptor;
pub mod error;
mod identity;
mod lifetime;
mod offset_allocator;
pub mod placement;
mod pool;
mod reservation;
mod spec;
pub mod stats;
mod transport;

pub use config::SegmentPoolConfig;
pub use descriptor::{
    MemoryRegion, RangeDescriptor, RangeDescriptorRef, ReservationDescriptor,
    ReservationDescriptorRef,
};
pub use identity::{ClientId, SegmentId, SegmentIdentity};
pub(crate) use pool::SegmentIncarnation;
pub use pool::{AttachOutcome, PoolSnapshot, ReplicaClassCapacity, SegmentHandle, SegmentPool};
pub use reservation::Reservation;
pub use spec::{ReplicaClass, SegmentSpec};
pub use stats::ReplicaClassSpaceStats;
pub use transport::{TransportEndpoint, TransportProtocol};
