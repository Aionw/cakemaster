use super::descriptor::{MemoryDescriptor, MemoryDescriptorRef, MemoryRegion};
use super::identity::SegmentId;
use super::offset_allocator::OffsetAllocationHandle;
use super::spec::SegmentSpec;
use std::fmt;
use std::sync::Arc;

pub struct Reservation {
    pub(super) allocation: OffsetAllocationHandle,
    pub(super) segment: Arc<SegmentSpec>,
    pub(super) region: MemoryRegion,
}

impl Reservation {
    pub fn segment_id(&self) -> SegmentId {
        self.segment.identity().id()
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

    pub fn descriptor(&self) -> MemoryDescriptorRef<'_> {
        let spec = self
            .segment
            .memory()
            .expect("memory is the only direct segment backend");
        MemoryDescriptorRef::new(self.region, spec.transport())
    }

    pub fn owned_descriptor(&self) -> MemoryDescriptor {
        self.descriptor().to_owned()
    }

    pub(crate) fn release_batch(reservations: Vec<Self>) {
        OffsetAllocationHandle::release_batch(
            reservations
                .into_iter()
                .map(|reservation| reservation.allocation),
        );
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
