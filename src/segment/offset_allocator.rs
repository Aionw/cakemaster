use offset_allocator::{Allocation, Allocator};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Byte-oriented, thread-safe adapter around the open-source
/// `offset-allocator` crate.
///
/// The upstream allocator uses `u32` units. Segments larger than 4 GiB use the
/// smallest power-of-two byte quantum that can represent their full range.
/// Allocation, binning, and coalescing remain owned by the upstream crate.
#[derive(Clone)]
pub(crate) struct ByteAllocator {
    shared: Arc<SharedAllocator>,
}

struct SharedAllocator {
    state: Mutex<AllocatorState>,
    quantum_shift: u32,
    managed_capacity: u64,
    base_offset: u64,
    /// Conservative upper bound for the largest request the binning algorithm
    /// can currently satisfy. A stale high value only takes the slow path;
    /// allocation failures tighten it while frees raise it as needed.
    largest_free_region_hint: AtomicU64,
}

struct AllocatorState {
    inner: Allocator,
    used_bytes: u64,
    live_allocations: u64,
}

/// Move-only RAII allocation returned by [`ByteAllocator`].
///
/// This is the Rust equivalent of Mooncake's `OffsetAllocationHandle`: the
/// range is returned to the allocator when the handle is dropped.
pub(crate) struct OffsetAllocationHandle {
    owner: Arc<SharedAllocator>,
    allocation: Option<Allocation>,
    offset: u64,
    requested_bytes: u64,
    reserved_bytes: u64,
}

struct PendingRelease {
    owner: Arc<SharedAllocator>,
    allocation: Allocation,
    reserved_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocatorStats {
    pub(crate) capacity: u64,
    pub(crate) used_bytes: u64,
    pub(crate) available_bytes: u64,
    pub(crate) largest_free_region: u64,
    pub(crate) live_allocations: u64,
}

impl ByteAllocator {
    #[cfg(test)]
    pub(crate) fn new(capacity: u64, max_allocator_nodes: u32) -> Self {
        Self::new_at(0, capacity, max_allocator_nodes)
    }

    pub(crate) fn new_at(base_offset: u64, capacity: u64, max_allocator_nodes: u32) -> Self {
        debug_assert_ne!(capacity, 0);
        debug_assert!(max_allocator_nodes >= 3);
        debug_assert!(max_allocator_nodes < u32::MAX - 1);

        let mut quantum_shift = 0;
        while (capacity >> quantum_shift) > u64::from(u32::MAX) {
            quantum_shift += 1;
        }

        let capacity_units = (capacity >> quantum_shift) as u32;
        let managed_capacity = u64::from(capacity_units) << quantum_shift;
        let inner = Allocator::with_max_allocs(capacity_units, max_allocator_nodes);
        let initial_report = inner.storage_report();

        Self {
            shared: Arc::new(SharedAllocator {
                state: Mutex::new(AllocatorState {
                    inner,
                    used_bytes: 0,
                    live_allocations: 0,
                }),
                quantum_shift,
                managed_capacity,
                base_offset,
                largest_free_region_hint: AtomicU64::new(
                    u64::from(initial_report.largest_free_region) << quantum_shift,
                ),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn allocate(&self, requested_bytes: u64) -> Option<OffsetAllocationHandle> {
        if !self.may_satisfy(requested_bytes) {
            return None;
        }
        self.allocate_after_precheck(requested_bytes)
    }

    #[inline]
    pub(crate) fn may_satisfy(&self, requested_bytes: u64) -> bool {
        requested_bytes != 0
            && requested_bytes <= self.shared.managed_capacity
            && requested_bytes <= self.shared.largest_free_region_hint.load(Ordering::Relaxed)
    }

    /// Performs the authoritative, mutex-protected allocation after the caller
    /// has checked [`Self::may_satisfy`]. The hint is only an optimization, so
    /// this operation can still fail after a successful precheck.
    #[inline]
    pub(crate) fn allocate_after_precheck(
        &self,
        requested_bytes: u64,
    ) -> Option<OffsetAllocationHandle> {
        debug_assert!(requested_bytes != 0);
        debug_assert!(requested_bytes <= self.shared.managed_capacity);

        let quantum = 1_u64 << self.shared.quantum_shift;
        let allocation_units =
            requested_bytes / quantum + u64::from(!requested_bytes.is_multiple_of(quantum));
        let allocation_units = u32::try_from(allocation_units).ok()?;

        let (allocation, reserved_bytes) = {
            let mut state = self.shared.state.lock();
            let Some(allocation) = state.inner.allocate(allocation_units) else {
                self.shared.refresh_largest_free_region(&state);
                return None;
            };
            let reserved_units = state.inner.allocation_size(allocation);
            let reserved_bytes = u64::from(reserved_units) << self.shared.quantum_shift;
            state.used_bytes += reserved_bytes;
            state.live_allocations += 1;
            (allocation, reserved_bytes)
        };

        Some(OffsetAllocationHandle {
            owner: self.shared.clone(),
            allocation: Some(allocation),
            offset: self
                .shared
                .base_offset
                .checked_add(u64::from(allocation.offset) << self.shared.quantum_shift)
                .expect("allocator shard offsets fit in the mounted resource"),
            requested_bytes,
            reserved_bytes,
        })
    }

    pub(crate) fn stats(&self) -> AllocatorStats {
        let state = self.shared.state.lock();
        let report = state.inner.storage_report();
        AllocatorStats {
            capacity: self.shared.managed_capacity,
            used_bytes: state.used_bytes,
            available_bytes: u64::from(report.total_free_space) << self.shared.quantum_shift,
            largest_free_region: u64::from(report.largest_free_region) << self.shared.quantum_shift,
            live_allocations: state.live_allocations,
        }
    }
}

impl SharedAllocator {
    fn release(&self, allocation: Allocation, reserved_bytes: u64) {
        self.release_many(std::iter::once((allocation, reserved_bytes)));
    }

    fn release_many(&self, releases: impl IntoIterator<Item = (Allocation, u64)>) {
        let mut state = self.state.lock();
        let mut allocation_count = 0_u64;
        let mut released_bytes = 0_u64;
        for (allocation, reserved_bytes) in releases {
            state.inner.free(allocation);
            allocation_count += 1;
            released_bytes += reserved_bytes;
        }
        state.live_allocations = state
            .live_allocations
            .checked_sub(allocation_count)
            .expect("allocation handles guarantee balanced release");
        state.used_bytes = state
            .used_bytes
            .checked_sub(released_bytes)
            .expect("allocation handles guarantee balanced byte accounting");
        if allocation_count != 0
            && self.largest_free_region_hint.load(Ordering::Relaxed) < self.managed_capacity
        {
            self.refresh_largest_free_region(&state);
        }
    }

    fn refresh_largest_free_region(&self, state: &AllocatorState) {
        let report = state.inner.storage_report();
        self.largest_free_region_hint.store(
            u64::from(report.largest_free_region) << self.quantum_shift,
            Ordering::Relaxed,
        );
    }
}

impl OffsetAllocationHandle {
    #[inline]
    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub(crate) const fn requested_bytes(&self) -> u64 {
        self.requested_bytes
    }

    #[inline]
    pub(crate) const fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    pub(crate) fn release_batch(handles: impl IntoIterator<Item = Self>) {
        let mut releases = handles
            .into_iter()
            .map(Self::into_pending_release)
            .collect::<Vec<_>>();
        releases.sort_unstable_by_key(|release| Arc::as_ptr(&release.owner) as usize);

        let mut group_start = 0;
        while group_start < releases.len() {
            let owner_ptr = Arc::as_ptr(&releases[group_start].owner);
            let mut group_end = group_start + 1;
            while group_end < releases.len()
                && std::ptr::eq(Arc::as_ptr(&releases[group_end].owner), owner_ptr)
            {
                group_end += 1;
            }
            releases[group_start].owner.release_many(
                releases[group_start..group_end]
                    .iter()
                    .map(|release| (release.allocation, release.reserved_bytes)),
            );
            group_start = group_end;
        }
    }

    fn into_pending_release(mut self) -> PendingRelease {
        PendingRelease {
            owner: self.owner.clone(),
            allocation: self
                .allocation
                .take()
                .expect("live allocation handles contain an allocation"),
            reserved_bytes: self.reserved_bytes,
        }
    }
}

impl Drop for OffsetAllocationHandle {
    fn drop(&mut self) {
        if let Some(allocation) = self.allocation.take() {
            self.owner.release(allocation, self.reserved_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ByteAllocator;

    fn allocator(capacity: u64, max_allocator_nodes: u32) -> ByteAllocator {
        ByteAllocator::new(capacity, max_allocator_nodes)
    }

    #[test]
    fn handles_release_and_coalesce_on_drop() {
        let allocator = allocator(4096, 1024);
        let first = allocator.allocate(128).unwrap();
        let second = allocator.allocate(256).unwrap();
        let third = allocator.allocate(64).unwrap();

        assert_eq!(first.offset(), 0);
        assert_eq!(second.offset(), 128);
        assert_eq!(third.offset(), 384);

        drop(second);
        drop(first);
        drop(third);

        let stats = allocator.stats();
        assert_eq!(stats.available_bytes, 4096);
        assert_eq!(stats.live_allocations, 0);
        assert_eq!(allocator.allocate(4096).unwrap().offset(), 0);
    }

    #[test]
    fn scales_large_segments_with_a_power_of_two_quantum() {
        let capacity = 16_u64 << 30;
        let allocator = allocator(capacity, 1024);
        assert_eq!(allocator.stats().capacity, capacity);

        let one_byte = allocator.allocate(1).unwrap();
        assert_eq!(one_byte.requested_bytes(), 1);
        assert_eq!(one_byte.reserved_bytes(), 8);
        assert_eq!(one_byte.offset(), 0);

        drop(one_byte);
        assert_eq!(allocator.stats().available_bytes, capacity);
    }

    #[test]
    fn failed_request_hint_is_raised_after_a_free() {
        let allocator = allocator(4096, 1024);
        let pressure = allocator.allocate(3072).unwrap();

        assert!(allocator.allocate(2048).is_none());
        assert!(!allocator.may_satisfy(2048));

        drop(pressure);
        assert!(allocator.may_satisfy(4096));
        assert_eq!(allocator.allocate(4096).unwrap().offset(), 0);
    }

    #[test]
    fn batch_release_groups_handles_by_allocator_and_coalesces() {
        let first = allocator(4096, 1024);
        let second = allocator(8192, 1024);
        let releases = vec![
            first.allocate(128).unwrap(),
            second.allocate(512).unwrap(),
            first.allocate(256).unwrap(),
            second.allocate(1024).unwrap(),
        ];

        super::OffsetAllocationHandle::release_batch(releases);

        assert_eq!(first.stats().live_allocations, 0);
        assert_eq!(first.stats().available_bytes, 4096);
        assert_eq!(second.stats().live_allocations, 0);
        assert_eq!(second.stats().available_bytes, 8192);
        assert_eq!(first.allocate(4096).unwrap().offset(), 0);
        assert_eq!(second.allocate(8192).unwrap().offset(), 0);
    }
}
