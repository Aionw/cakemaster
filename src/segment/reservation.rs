use super::descriptor::{
    MemoryDescriptorRef, MemoryRegion, NofDescriptorRef, ReservationDescriptor,
    ReservationDescriptorRef,
};
use super::identity::SegmentId;
use super::offset_allocator::OffsetAllocationHandle;
use super::spec::{ReplicaClass, SegmentSpec};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct ReservationCounter {
    live: Arc<AtomicU64>,
}

impl ReservationCounter {
    pub(super) fn acquire(live: Arc<AtomicU64>) -> Self {
        live.fetch_add(1, Ordering::AcqRel);
        Self { live }
    }
}

impl Drop for ReservationCounter {
    fn drop(&mut self) {
        let previous = self.live.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0, "reservation counts must remain balanced");
    }
}

pub struct Reservation {
    pub(super) allocation: OffsetAllocationHandle,
    pub(super) segment: Arc<SegmentSpec>,
    pub(super) _counter: ReservationCounter,
    pub(super) region: MemoryRegion,
}

impl Reservation {
    pub fn segment_id(&self) -> SegmentId {
        self.segment.identity().id()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.segment.replica_class()
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
        match self.segment.as_ref() {
            SegmentSpec::Memory(spec) => ReservationDescriptorRef::Memory(
                MemoryDescriptorRef::new(self.region, spec.transport()),
            ),
            SegmentSpec::Cxl(spec) => ReservationDescriptorRef::Memory(MemoryDescriptorRef::new(
                self.region,
                spec.transport(),
            )),
            SegmentSpec::Nof(spec) => {
                ReservationDescriptorRef::Nof(NofDescriptorRef::new(self.region, spec.transport()))
            }
        }
    }

    pub fn owned_descriptor(&self) -> ReservationDescriptor {
        self.descriptor().to_owned()
    }

    pub(crate) fn release_batch(reservations: Vec<Self>) {
        let mut allocations = Vec::with_capacity(reservations.len());
        let mut counters = Vec::with_capacity(reservations.len());
        for reservation in reservations {
            allocations.push(reservation.allocation);
            counters.push(reservation._counter);
        }
        OffsetAllocationHandle::release_batch(allocations);
        drop(counters);
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
