use super::descriptor::{
    MemoryRegion, RangeDescriptorRef, ReservationDescriptor, ReservationDescriptorRef,
};
use super::identity::SegmentId;
use super::lifetime::SegmentLease;
use super::offset_allocator::OffsetAllocationHandle;
use super::spec::{ReplicaClass, SegmentSpec};
use std::fmt;
use std::sync::Arc;

pub struct Reservation {
    pub(super) allocation: OffsetAllocationHandle,
    pub(super) segment: Arc<SegmentSpec>,
    pub(super) segment_lease: SegmentLease,
    pub(super) region: MemoryRegion,
}

impl Reservation {
    pub fn segment_id(&self) -> SegmentId {
        self.segment.identity().id()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.segment.replica_class()
    }

    /// Whether the mounted segment incarnation that issued this reservation
    /// is still logically valid.
    pub fn is_live(&self) -> bool {
        self.segment_lease.is_live()
    }

    pub const fn offset(&self) -> u64 {
        self.allocation.offset()
    }

    pub const fn requested_bytes(&self) -> u64 {
        self.allocation.requested_bytes()
    }

    pub const fn reserved_bytes(&self) -> u64 {
        self.allocation.reserved_bytes()
    }

    pub const fn region(&self) -> MemoryRegion {
        self.region
    }

    pub fn descriptor(&self) -> ReservationDescriptorRef<'_> {
        let descriptor = RangeDescriptorRef::new(
            self.region,
            self.segment
                .transport()
                .expect("direct reservations retain a transport endpoint"),
        );
        match self.segment.replica_class() {
            ReplicaClass::Memory => ReservationDescriptorRef::Memory(descriptor),
            ReplicaClass::Nof => ReservationDescriptorRef::Nof(descriptor),
            ReplicaClass::LocalSsd => {
                unreachable!("LocalSSD capacity cannot produce direct reservations")
            }
        }
    }

    pub fn owned_descriptor(&self) -> ReservationDescriptor {
        self.descriptor().to_owned()
    }

    pub(crate) fn release_batch(reservations: Vec<Self>) {
        let mut allocations = Vec::with_capacity(reservations.len());
        let mut segment_leases = Vec::with_capacity(reservations.len());
        for reservation in reservations {
            allocations.push(reservation.allocation);
            segment_leases.push(reservation.segment_lease);
        }
        OffsetAllocationHandle::release_batch(allocations);
        drop(segment_leases);
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reservation")
            .field("segment_id", &self.segment_id())
            .field("offset", &self.offset())
            .field("region", &self.region)
            .field("reserved_bytes", &self.reserved_bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}
