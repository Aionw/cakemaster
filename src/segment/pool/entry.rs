use crate::segment::MemoryRegion;
use crate::segment::error::{ReserveError, SegmentStateError};
use crate::segment::lifetime::SegmentLifetime;
use crate::segment::offset_allocator::ByteAllocator;
use crate::segment::reservation::Reservation;
use crate::segment::spec::SegmentSpec;
use crate::segment::stats::{SegmentState, SegmentStats, SegmentUsageStats};
use parking_lot::Mutex;
use std::sync::Arc;

pub(super) struct SegmentEntry {
    spec: Arc<SegmentSpec>,
    state: Mutex<SegmentState>,
    allocator: ByteAllocator,
    lifetime: SegmentLifetime,
}

impl SegmentEntry {
    pub(super) fn new(spec: Arc<SegmentSpec>, allocator: ByteAllocator) -> Self {
        Self {
            spec,
            state: Mutex::new(SegmentState::Accepting),
            allocator,
            lifetime: SegmentLifetime::new(),
        }
    }

    pub(super) fn spec(&self) -> &SegmentSpec {
        &self.spec
    }

    pub(super) fn is_accepting(&self) -> bool {
        self.state.lock().is_accepting()
    }

    #[inline]
    pub(super) fn reserve(&self, bytes: u64) -> Result<Reservation, ReserveError> {
        let id = self.spec.identity().id();
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        if !self.allocator.may_satisfy(bytes) {
            return Err(ReserveError::OutOfSpace(id));
        }

        let state = self.state.lock();
        if !state.is_accepting() {
            return Err(ReserveError::NotAccepting(id));
        }
        let allocation = self
            .allocator
            .allocate_after_precheck(bytes)
            .ok_or(ReserveError::OutOfSpace(id))?;
        let address = self
            .spec
            .region()
            .base()
            .checked_add(allocation.offset())
            .ok_or(ReserveError::AddressOverflow(id))?;
        let reservation = Reservation {
            allocation,
            segment: self.spec.clone(),
            segment_lease: self.lifetime.acquire(),
            region: MemoryRegion::new(address, bytes),
        };
        drop(state);
        Ok(reservation)
    }

    pub(super) fn quiesce(&self) {
        let mut state = self.state.lock();
        if state.is_accepting() {
            *state = SegmentState::Quiesced;
        }
    }

    pub(super) fn reactivate(&self) {
        let mut state = self.state.lock();
        if *state == SegmentState::Quiesced {
            *state = SegmentState::Accepting;
        }
    }

    pub(super) fn prepare_remove(&self) -> Result<(), SegmentStateError> {
        let mut state = self.state.lock();
        match *state {
            SegmentState::Accepting => {
                Err(SegmentStateError::StillAccepting(self.spec.identity().id()))
            }
            SegmentState::Quiesced => {
                self.lifetime.invalidate();
                *state = SegmentState::Removed;
                Ok(())
            }
            SegmentState::Removed => Ok(()),
        }
    }

    /// Immediately fences this incarnation regardless of its accepting state.
    /// Used by owner cleanup after the client session has already been fenced.
    pub(super) fn invalidate(&self) {
        let mut state = self.state.lock();
        self.lifetime.invalidate();
        *state = SegmentState::Removed;
    }

    pub(super) fn stats(&self) -> SegmentStats {
        let state = *self.state.lock();
        SegmentStats {
            space: {
                let stats = self.allocator.stats();
                crate::segment::stats::SegmentSpaceStats {
                    capacity_bytes: stats.capacity,
                    used_bytes: stats.used_bytes,
                    available_bytes: stats.available_bytes,
                    largest_free_region_bytes: stats.largest_free_region,
                }
            },
            usage: SegmentUsageStats {
                active_allocations: self.lifetime.active_leases(),
            },
            state,
        }
    }
}
