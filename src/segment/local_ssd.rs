use super::descriptor::{LocalSsdDescriptor, LocalSsdDescriptorRef};
use super::error::LocalSsdError;
use super::identity::SegmentId;
use super::reservation::ReservationCounter;
use super::spec::SegmentSpec;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

#[derive(Clone)]
pub(crate) struct LocalSsdCapacity {
    shared: Arc<SharedCapacity>,
}

struct SharedCapacity {
    state: Mutex<CapacityState>,
}

struct CapacityState {
    offload_enabled: bool,
    reported_capacity_bytes: Option<u64>,
    pending_bytes: u64,
    committed_bytes: u64,
}

pub(crate) enum AdmissionFailure {
    OffloadDisabled,
    CapacityNotReported,
    OutOfSpace,
}

pub(crate) struct LocalSsdAllocation {
    shared: Arc<SharedCapacity>,
    bytes: u64,
    committed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalSsdStats {
    pub offload_enabled: bool,
    pub capacity_reported: bool,
    pub capacity_bytes: u64,
    pub admitted_bytes: u64,
    pub available_bytes: u64,
    pub pending_bytes: u64,
    pub committed_bytes: u64,
}

pub struct OffloadPermit {
    allocation: Option<LocalSsdAllocation>,
    counter: Option<ReservationCounter>,
    segment: Arc<SegmentSpec>,
}

pub struct LocalSsdLease {
    allocation: LocalSsdAllocation,
    _counter: ReservationCounter,
    segment: Arc<SegmentSpec>,
    transport_endpoint: Arc<str>,
}

impl LocalSsdCapacity {
    pub(crate) fn new(offload_enabled: bool) -> Self {
        Self {
            shared: Arc::new(SharedCapacity {
                state: Mutex::new(CapacityState {
                    offload_enabled,
                    reported_capacity_bytes: None,
                    pending_bytes: 0,
                    committed_bytes: 0,
                }),
            }),
        }
    }

    pub(crate) fn set_offload_enabled(&self, enabled: bool) {
        self.shared.state.lock().offload_enabled = enabled;
    }

    pub(crate) fn offload_enabled(&self) -> bool {
        self.shared.state.lock().offload_enabled
    }

    pub(crate) fn report(&self, capacity_bytes: u64) {
        self.shared.state.lock().reported_capacity_bytes = Some(capacity_bytes);
    }

    pub(crate) fn admit(&self, bytes: u64) -> Result<LocalSsdAllocation, AdmissionFailure> {
        let mut state = self.shared.state.lock();
        if !state.offload_enabled {
            return Err(AdmissionFailure::OffloadDisabled);
        }
        let capacity = state
            .reported_capacity_bytes
            .ok_or(AdmissionFailure::CapacityNotReported)?;
        let admitted = state.pending_bytes.saturating_add(state.committed_bytes);
        if admitted
            .checked_add(bytes)
            .is_none_or(|next| next > capacity)
        {
            return Err(AdmissionFailure::OutOfSpace);
        }
        state.pending_bytes += bytes;
        drop(state);
        Ok(LocalSsdAllocation {
            shared: self.shared.clone(),
            bytes,
            committed: false,
        })
    }

    pub(crate) fn stats(&self) -> LocalSsdStats {
        let state = self.shared.state.lock();
        let capacity_bytes = state.reported_capacity_bytes.unwrap_or(0);
        let admitted_bytes = state.pending_bytes.saturating_add(state.committed_bytes);
        LocalSsdStats {
            offload_enabled: state.offload_enabled,
            capacity_reported: state.reported_capacity_bytes.is_some(),
            capacity_bytes,
            admitted_bytes,
            available_bytes: capacity_bytes.saturating_sub(admitted_bytes),
            pending_bytes: state.pending_bytes,
            committed_bytes: state.committed_bytes,
        }
    }
}

impl LocalSsdAllocation {
    fn commit(&mut self) {
        debug_assert!(!self.committed);
        let mut state = self.shared.state.lock();
        state.pending_bytes = state
            .pending_bytes
            .checked_sub(self.bytes)
            .expect("LocalSSD permits keep pending byte accounting balanced");
        state.committed_bytes += self.bytes;
        self.committed = true;
    }
}

impl Drop for LocalSsdAllocation {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        let accounted = if self.committed {
            &mut state.committed_bytes
        } else {
            &mut state.pending_bytes
        };
        *accounted = accounted
            .checked_sub(self.bytes)
            .expect("LocalSSD leases keep byte accounting balanced");
    }
}

impl OffloadPermit {
    pub(crate) fn new(
        allocation: LocalSsdAllocation,
        live: Arc<AtomicU64>,
        segment: Arc<SegmentSpec>,
    ) -> Self {
        Self {
            allocation: Some(allocation),
            counter: Some(ReservationCounter::acquire(live)),
            segment,
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment.identity().id()
    }

    pub fn bytes(&self) -> u64 {
        self.allocation
            .as_ref()
            .expect("live permits contain an allocation")
            .bytes
    }

    pub fn commit(
        mut self,
        transport_endpoint: impl Into<Arc<str>>,
    ) -> Result<LocalSsdLease, LocalSsdError> {
        let transport_endpoint = transport_endpoint.into();
        if transport_endpoint.is_empty() {
            return Err(LocalSsdError::EmptyTransportEndpoint);
        }
        let mut allocation = self
            .allocation
            .take()
            .expect("live permits contain an allocation");
        allocation.commit();
        Ok(LocalSsdLease {
            allocation,
            _counter: self.counter.take().expect("live permits contain a counter"),
            segment: self.segment.clone(),
            transport_endpoint,
        })
    }

    pub fn abort(self) {
        drop(self);
    }
}

impl fmt::Debug for OffloadPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OffloadPermit")
            .field("segment_id", &self.segment_id())
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl LocalSsdLease {
    pub fn segment_id(&self) -> SegmentId {
        self.segment.identity().id()
    }

    pub const fn bytes(&self) -> u64 {
        self.allocation.bytes
    }

    pub fn transport_endpoint(&self) -> &str {
        &self.transport_endpoint
    }

    pub fn descriptor(&self) -> LocalSsdDescriptorRef<'_> {
        let spec = self
            .segment
            .local_ssd()
            .expect("LocalSSD leases retain a LocalSSD segment");
        LocalSsdDescriptorRef::new(
            spec.identity().owner(),
            self.bytes(),
            self.transport_endpoint(),
        )
    }

    pub fn owned_descriptor(&self) -> LocalSsdDescriptor {
        self.descriptor().to_owned()
    }
}

impl fmt::Debug for LocalSsdLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSsdLease")
            .field("segment_id", &self.segment_id())
            .field("bytes", &self.bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}
