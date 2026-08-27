use super::resource::{CandidateCapability, MountedResource, TransferableExtent};
use crate::segment::error::{LocalSsdError, ReserveError, SegmentStateError};
use crate::segment::lifetime::SegmentLifetime;
use crate::segment::local_ssd::{LocalSsdStats, OffloadPermit};
use crate::segment::reservation::Reservation;
use crate::segment::spec::SegmentSpec;
use crate::segment::stats::{SegmentState, SegmentStats, SegmentUsageStats};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

const STATE_ACCEPTING: u8 = 0;
const STATE_QUIESCED: u8 = 1;
const STATE_REMOVED: u8 = 2;

pub(super) struct SegmentEntry {
    spec: Arc<SegmentSpec>,
    state: EntryState,
    resource: MountedResource,
    lifetime: SegmentLifetime,
}

enum EntryState {
    Concurrent(Mutex<SegmentState>),
    /// Production metadata shards serialize reservations and topology changes
    /// through one mailbox, so an atomic state is sufficient and no gate is
    /// needed around the resource operation.
    ShardLocal(AtomicU8),
}

impl SegmentEntry {
    pub(super) fn new(
        spec: Arc<SegmentSpec>,
        resource: MountedResource,
        shard_local: bool,
    ) -> Self {
        Self {
            spec,
            state: if shard_local {
                EntryState::ShardLocal(AtomicU8::new(STATE_ACCEPTING))
            } else {
                EntryState::Concurrent(Mutex::new(SegmentState::Accepting))
            },
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
        if !self.state.load().is_accepting() {
            return None;
        }
        self.resource
            .candidate_capability(self.spec.replica_class())
    }

    #[inline]
    pub(super) fn reserve(
        &self,
        allocator_shard: usize,
        bytes: u64,
    ) -> Result<Reservation, ReserveError> {
        let id = self.spec.identity().id();
        if !self.supports_direct_reservation() {
            return Err(ReserveError::NotDirectlyAllocatable(id));
        }
        if bytes == 0 {
            return Err(ReserveError::ZeroSize);
        }
        if !self.resource.may_satisfy(allocator_shard, bytes) {
            return Err(ReserveError::OutOfSpace(id));
        }

        self.state
            .reserve(|| {
                self.resource.reserve(
                    self.spec.clone(),
                    self.lifetime.acquire(),
                    allocator_shard,
                    bytes,
                )
            })
            .ok_or(ReserveError::NotAccepting(id))?
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

        self.state
            .reserve(|| {
                self.resource
                    .admit_offload(self.spec.clone(), self.lifetime.acquire(), bytes)
            })
            .ok_or(LocalSsdError::NotAccepting(id))?
    }

    pub(super) fn local_ssd_stats(&self) -> Option<LocalSsdStats> {
        self.resource.local_ssd_stats()
    }

    pub(super) fn quiesce(&self) {
        self.state.quiesce();
    }

    pub(super) fn reactivate(&self) {
        self.state.reactivate();
    }

    pub(super) fn prepare_remove(&self) -> Result<(), SegmentStateError> {
        if self.state.prepare_remove(|| self.lifetime.invalidate()) {
            Ok(())
        } else {
            Err(SegmentStateError::StillAccepting(self.spec.identity().id()))
        }
    }

    /// Immediately fences this incarnation regardless of its accepting state.
    /// Used by owner cleanup after the client session has already been fenced.
    pub(super) fn invalidate(&self) {
        self.lifetime.invalidate();
        self.state.remove();
    }

    pub(super) fn stats(&self) -> SegmentStats {
        let state = self.state.load();
        SegmentStats {
            space: self.resource.space_stats(),
            usage: SegmentUsageStats {
                active_allocations: self.lifetime.active_leases(),
            },
            state,
        }
    }

    pub(super) fn space_for_shard(
        &self,
        allocator_shard: usize,
    ) -> crate::segment::stats::SegmentSpaceStats {
        self.resource.space_stats_for_shard(allocator_shard)
    }

    pub(super) fn take_empty_extent(
        &self,
        allocator_shard: usize,
        minimum_bytes: u64,
    ) -> Option<TransferableExtent> {
        self.state
            .load()
            .is_accepting()
            .then(|| {
                self.resource
                    .take_empty_extent(allocator_shard, minimum_bytes)
            })
            .flatten()
    }

    pub(super) fn add_extent(&self, allocator_shard: usize, extent: TransferableExtent) -> bool {
        self.state.load().is_accepting() && self.resource.add_extent(allocator_shard, extent)
    }
}

impl EntryState {
    fn load(&self) -> SegmentState {
        match self {
            Self::Concurrent(state) => *state.lock(),
            Self::ShardLocal(state) => decode_state(state.load(Ordering::Acquire)),
        }
    }

    fn reserve<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        match self {
            Self::Concurrent(state) => {
                let state = state.lock();
                if !state.is_accepting() {
                    return None;
                }
                let result = operation();
                drop(state);
                Some(result)
            }
            Self::ShardLocal(state) => {
                (state.load(Ordering::Acquire) == STATE_ACCEPTING).then(operation)
            }
        }
    }

    fn quiesce(&self) {
        match self {
            Self::Concurrent(state) => {
                let mut state = state.lock();
                if state.is_accepting() {
                    *state = SegmentState::Quiesced;
                }
            }
            Self::ShardLocal(state) => {
                let _ = state.compare_exchange(
                    STATE_ACCEPTING,
                    STATE_QUIESCED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
    }

    fn reactivate(&self) {
        match self {
            Self::Concurrent(state) => {
                let mut state = state.lock();
                if *state == SegmentState::Quiesced {
                    *state = SegmentState::Accepting;
                }
            }
            Self::ShardLocal(state) => {
                let _ = state.compare_exchange(
                    STATE_QUIESCED,
                    STATE_ACCEPTING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
    }

    fn prepare_remove(&self, invalidate: impl FnOnce()) -> bool {
        match self {
            Self::Concurrent(state) => {
                let mut state = state.lock();
                match *state {
                    SegmentState::Accepting => false,
                    SegmentState::Quiesced => {
                        invalidate();
                        *state = SegmentState::Removed;
                        true
                    }
                    SegmentState::Removed => true,
                }
            }
            Self::ShardLocal(state) => match state.load(Ordering::Acquire) {
                STATE_ACCEPTING => false,
                STATE_QUIESCED => {
                    invalidate();
                    state.store(STATE_REMOVED, Ordering::Release);
                    true
                }
                STATE_REMOVED => true,
                invalid => unreachable!("invalid shard-local segment state {invalid}"),
            },
        }
    }

    fn remove(&self) {
        match self {
            Self::Concurrent(state) => *state.lock() = SegmentState::Removed,
            Self::ShardLocal(state) => state.store(STATE_REMOVED, Ordering::Release),
        }
    }
}

fn decode_state(state: u8) -> SegmentState {
    match state {
        STATE_ACCEPTING => SegmentState::Accepting,
        STATE_QUIESCED => SegmentState::Quiesced,
        STATE_REMOVED => SegmentState::Removed,
        invalid => unreachable!("invalid shard-local segment state {invalid}"),
    }
}
