use std::sync::Arc;

/// Tracks resource-bearing operations associated with one logical segment.
///
/// Only [`UsageToken`] clones the anchor. Catalog handles and placement
/// snapshots do not, so stale metadata handles never block segment removal.
pub(crate) struct UsageTracker {
    anchor: Arc<UsageAnchor>,
}

struct UsageAnchor;

pub(crate) struct UsageToken {
    _anchor: Arc<UsageAnchor>,
}

impl UsageTracker {
    pub(crate) fn new() -> Self {
        Self {
            anchor: Arc::new(UsageAnchor),
        }
    }

    pub(crate) fn acquire(&self) -> UsageToken {
        UsageToken {
            _anchor: self.anchor.clone(),
        }
    }

    pub(crate) fn active_allocations(&self) -> u64 {
        let active = Arc::strong_count(&self.anchor).saturating_sub(1);
        u64::try_from(active).unwrap_or(u64::MAX)
    }
}
