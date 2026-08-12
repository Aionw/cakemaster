use super::resource::{CandidateCapability, MountedResource};
use crate::segment::error::{LocalSsdError, ReserveError, SegmentStateError};
use crate::segment::lifetime::SegmentLifetime;
use crate::segment::local_ssd::{LocalSsdStats, OffloadPermit};
use crate::segment::reservation::Reservation;
use crate::segment::spec::SegmentSpec;
use crate::segment::stats::{SegmentState, SegmentStats, SegmentUsageStats};
use parking_lot::Mutex;
use std::sync::Arc;

pub(super) struct SegmentEntry {
    spec: Arc<SegmentSpec>,
    state: Mutex<SegmentState>,
    resource: MountedResource,
    lifetime: SegmentLifetime,
}

impl SegmentEntry {
    pub(super) fn new(spec: Arc<SegmentSpec>, resource: MountedResource) -> Self {
        Self {
            spec,
            state: Mutex::new(SegmentState::Accepting),
            resource,
            lifetime: SegmentLifetime::new(),
        }
    }

    pub(super) fn spec(&self) -> &SegmentSpec {
        &self.spec
    }

    pub(super) fn supports_direct_reservation(&self) -> bool {
        self.resource.supports_direct_reservation()
    }

    pub(super) fn supports_offload(&self) -> bool {
        self.resource.supports_offload()
    }

    pub(super) fn candidate_capability(&self) -> Option<CandidateCapability> {
        if !self.state.lock().is_accepting() {
            return None;
        }
        self.resource
            .candidate_capability(self.spec.replica_class())
    }

    #[inline]
    pub(super) fn reserve(&self, bytes: u64) -> Result<Reservation, ReserveError> {
        let id = self.spec.identity().id();
        if !self.supports_direct_reservation() {
            return Err(ReserveError::NotDirectlyAllocatable(id));
        }
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        if !self.resource.may_satisfy(bytes) {
            return Err(ReserveError::OutOfSpace(id));
        }

        let state = self.state.lock();
        if !state.is_accepting() {
            return Err(ReserveError::NotAccepting(id));
        }
        let result = self
            .resource
            .reserve(self.spec.clone(), self.lifetime.acquire(), bytes);
        drop(state);
        result
    }

    pub(super) fn report_local_ssd_capacity(
        &self,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        self.resource
            .report_capacity(self.spec.identity().id(), capacity_bytes)
    }

    pub(super) fn set_local_ssd_offload_enabled(&self, enabled: bool) -> Result<(), LocalSsdError> {
        self.resource
            .set_offload_enabled(self.spec.identity().id(), enabled)
    }

    pub(super) fn admit_offload(&self, bytes: u64) -> Result<OffloadPermit, LocalSsdError> {
        let id = self.spec.identity().id();
        if !self.supports_offload() {
            return Err(LocalSsdError::NotLocalSsd(id));
        }
        if bytes == 0 {
            return Err(LocalSsdError::ZeroSize);
        }

        let state = self.state.lock();
        if !state.is_accepting() {
            return Err(LocalSsdError::NotAccepting(id));
        }
        let result = self
            .resource
            .admit_offload(self.spec.clone(), self.lifetime.acquire(), bytes);
        drop(state);
        result
    }

    pub(super) fn local_ssd_stats(&self) -> Option<LocalSsdStats> {
        self.resource.local_ssd_stats()
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
            space: self.resource.space_stats(),
            usage: SegmentUsageStats {
                active_allocations: self.lifetime.active_leases(),
            },
            state,
        }
    }
}
