use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

/// Lifecycle control for one mounted segment incarnation.
///
/// The catalog owns the lifetime while each resource-bearing allocation owns
/// a [`SegmentLease`]. Logical invalidation is independent from physical
/// reclamation: leases observe the former and keep the latter alive.
pub(crate) struct SegmentLifetime {
    state: Arc<LifetimeState>,
}

struct LifetimeState {
    live: AtomicBool,
}

/// A resource-bearing reference to one mounted segment incarnation.
///
/// This type is deliberately not `Clone`: each lease corresponds to one
/// allocation counted by [`SegmentLifetime::active_leases`].
pub(crate) struct SegmentLease {
    state: Arc<LifetimeState>,
}

/// Non-owning liveness view used by immutable object metadata snapshots.
///
/// Unlike [`SegmentLease`], this observer neither keeps the mounted segment
/// incarnation alive nor contributes to its active allocation count.
#[derive(Clone)]
pub(crate) struct SegmentLiveness {
    state: Weak<LifetimeState>,
}

impl SegmentLifetime {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(LifetimeState {
                live: AtomicBool::new(true),
            }),
        }
    }

    pub(crate) fn acquire(&self) -> SegmentLease {
        SegmentLease {
            state: self.state.clone(),
        }
    }

    pub(crate) fn active_leases(&self) -> u64 {
        let active = Arc::strong_count(&self.state).saturating_sub(1);
        u64::try_from(active).unwrap_or(u64::MAX)
    }

    /// Fences every lease issued by this mounted incarnation.
    ///
    /// Dropping the leases still owns physical reclamation; invalidation only
    /// prevents their logical use.
    pub(crate) fn invalidate(&self) {
        self.state.live.store(false, Ordering::Release);
    }
}

impl SegmentLease {
    pub(crate) fn is_live(&self) -> bool {
        self.state.live.load(Ordering::Acquire)
    }

    pub(crate) fn observer(&self) -> SegmentLiveness {
        SegmentLiveness {
            state: Arc::downgrade(&self.state),
        }
    }
}

impl SegmentLiveness {
    pub(crate) fn is_live(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| state.live.load(Ordering::Acquire))
    }
}
