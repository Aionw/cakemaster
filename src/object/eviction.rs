//! Production memory-watermark eviction policy and diagnostics.

use super::diagnostics::ObjectCatalogStats;
use super::reclamation::{CollectBudget, CollectReport};
use crate::segment::ReplicaClassSpaceStats;
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;

const WATERMARK_SCALE: u64 = 1_000_000;
const MAX_ALLOCATION_RETRIES: usize = 3;

/// Default bounded work allowed after one allocation failure.
pub const DEFAULT_ALLOCATION_FAILURE_BUDGET: CollectBudget = CollectBudget::new(64, 64, 0);

/// Validated production memory-eviction policy.
///
/// Ratios are converted to integer parts-per-million at construction so all
/// threshold calculations use integer bytes and remain stable near a boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryEvictionConfig {
    high_watermark_ppm: u32,
    low_watermark_ppm: u32,
    allocation_failure_budget: CollectBudget,
    allocation_retry_limit: usize,
}

impl MemoryEvictionConfig {
    /// Creates a policy with `0 < low < high < 1`.
    pub fn new(
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> Result<Self, MemoryEvictionConfigError> {
        let high_watermark_ppm = ratio_to_ppm(high_watermark_ratio)
            .ok_or(MemoryEvictionConfigError::InvalidHighWatermark)?;
        let low_watermark_ppm = ratio_to_ppm(low_watermark_ratio)
            .ok_or(MemoryEvictionConfigError::InvalidLowWatermark)?;
        if low_watermark_ppm >= high_watermark_ppm {
            return Err(MemoryEvictionConfigError::LowNotBelowHigh);
        }
        Ok(Self {
            high_watermark_ppm,
            low_watermark_ppm,
            allocation_failure_budget: DEFAULT_ALLOCATION_FAILURE_BUDGET,
            allocation_retry_limit: 1,
        })
    }

    /// Sets bounded synchronous collection and retry limits for allocation
    /// failures. At least one candidate and reclaim are required, and retries
    /// are capped so configuration cannot turn the request path into a loop.
    pub fn with_allocation_failure_policy(
        mut self,
        budget: CollectBudget,
        retry_limit: usize,
    ) -> Result<Self, MemoryEvictionConfigError> {
        if budget.max_candidates() == 0 {
            return Err(MemoryEvictionConfigError::ZeroFailureCandidates);
        }
        if budget.max_reclaims() == 0 {
            return Err(MemoryEvictionConfigError::ZeroFailureReclaims);
        }
        if retry_limit > MAX_ALLOCATION_RETRIES {
            return Err(MemoryEvictionConfigError::TooManyAllocationRetries);
        }
        self.allocation_failure_budget = budget;
        self.allocation_retry_limit = retry_limit;
        Ok(self)
    }

    pub fn high_watermark_ratio(self) -> f64 {
        f64::from(self.high_watermark_ppm) / WATERMARK_SCALE as f64
    }

    pub fn low_watermark_ratio(self) -> f64 {
        f64::from(self.low_watermark_ppm) / WATERMARK_SCALE as f64
    }

    pub const fn allocation_failure_budget(self) -> CollectBudget {
        self.allocation_failure_budget
    }

    pub const fn allocation_retry_limit(self) -> usize {
        self.allocation_retry_limit
    }

    pub(crate) fn high_bytes(self, capacity_bytes: u64) -> u64 {
        scale_bytes(capacity_bytes, self.high_watermark_ppm)
    }

    pub(crate) fn low_bytes(self, capacity_bytes: u64) -> u64 {
        scale_bytes(capacity_bytes, self.low_watermark_ppm)
    }
}

impl Default for MemoryEvictionConfig {
    fn default() -> Self {
        Self::new(0.90, 0.80).expect("the default memory watermarks are valid")
    }
}

/// Invalid memory-watermark or allocation-failure controls.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MemoryEvictionConfigError {
    #[error("memory eviction high watermark must be finite and in (0, 1)")]
    InvalidHighWatermark,
    #[error("memory eviction low watermark must be finite and in (0, 1)")]
    InvalidLowWatermark,
    #[error("memory eviction low watermark must be strictly below the high watermark")]
    LowNotBelowHigh,
    #[error("allocation-failure eviction must scan at least one candidate")]
    ZeroFailureCandidates,
    #[error("allocation-failure eviction must attempt at least one physical reclaim")]
    ZeroFailureReclaims,
    #[error("allocation-failure retry limit must not exceed {MAX_ALLOCATION_RETRIES}")]
    TooManyAllocationRetries,
}

/// Snapshot of watermark state and cumulative controller activity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryEvictionStats {
    pub active: bool,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub maximum_used_bytes: u64,
    /// Maximum sampled physical usage ratio in parts per million.
    pub maximum_used_ratio_ppm: u64,
    pub available_bytes: u64,
    pub high_watermark_bytes: u64,
    pub low_watermark_bytes: u64,
    pub pending_bytes: u64,
    pub live_bytes: u64,
    pub retired_bytes: u64,
    pub reclaim_debt_bytes: u64,
    pub requested_reclaim_debt_bytes: u64,
    pub watermark_reclaim_debt_bytes: u64,
    pub trigger_events: u64,
    pub controller_steps: u64,
    pub busy_steps: u64,
    pub retired_objects: u64,
    pub retired_bytes_total: u64,
    pub reclaimed_objects: u64,
    pub reclaimed_bytes_total: u64,
    pub allocation_failures: u64,
    pub allocation_retries: u64,
    pub allocation_retry_successes: u64,
    pub wakeups: u64,
}

#[derive(Clone)]
pub(crate) struct MemoryEvictionController {
    inner: Arc<MemoryEvictionInner>,
}

struct MemoryEvictionInner {
    config: MemoryEvictionConfig,
    notify: Arc<Notify>,
    state: Mutex<MemoryEvictionStats>,
}

impl MemoryEvictionController {
    pub(crate) fn new(config: MemoryEvictionConfig) -> Self {
        Self {
            inner: Arc::new(MemoryEvictionInner {
                config,
                notify: Arc::new(Notify::new()),
                state: Mutex::new(MemoryEvictionStats::default()),
            }),
        }
    }

    pub(crate) fn config(&self) -> MemoryEvictionConfig {
        self.inner.config
    }

    pub(crate) fn notify(&self) -> Arc<Notify> {
        self.inner.notify.clone()
    }

    pub(crate) fn stats(&self) -> MemoryEvictionStats {
        *self.inner.state.lock()
    }

    /// Samples physical usage and returns the absolute bytes still required to
    /// reach low watermark. Retired bytes are deliberately not subtracted:
    /// they still own allocator capacity until the RAII reclaim succeeds.
    pub(crate) fn prepare_step(
        &self,
        space: ReplicaClassSpaceStats,
        catalog: ObjectCatalogStats,
    ) -> u64 {
        let config = self.inner.config;
        let high_bytes = config.high_bytes(space.capacity_bytes);
        let low_bytes = config.low_bytes(space.capacity_bytes);
        let mut state = self.inner.state.lock();
        if space.capacity_bytes == 0 || state.active && space.used_bytes <= low_bytes {
            state.active = false;
        } else if !state.active && space.used_bytes > high_bytes {
            state.active = true;
            state.trigger_events = state.trigger_events.saturating_add(1);
        }
        update_sample(&mut state, space, catalog, high_bytes, low_bytes);
        if state.active {
            space.used_bytes.saturating_sub(low_bytes)
        } else {
            0
        }
    }

    pub(crate) fn finish_step(
        &self,
        space: ReplicaClassSpaceStats,
        catalog: ObjectCatalogStats,
        report: CollectReport,
    ) -> bool {
        let high_bytes = self.inner.config.high_bytes(space.capacity_bytes);
        let low_bytes = self.inner.config.low_bytes(space.capacity_bytes);
        let mut state = self.inner.state.lock();
        if space.capacity_bytes == 0 || space.used_bytes <= low_bytes {
            state.active = false;
        }
        state.controller_steps = state.controller_steps.saturating_add(1);
        state.busy_steps = state.busy_steps.saturating_add(u64::from(report.busy));
        state.retired_objects = state
            .retired_objects
            .saturating_add(report.retired_objects as u64);
        state.retired_bytes_total = state
            .retired_bytes_total
            .saturating_add(report.retired_bytes);
        state.reclaimed_objects = state
            .reclaimed_objects
            .saturating_add(report.reclaimed_objects as u64);
        state.reclaimed_bytes_total = state
            .reclaimed_bytes_total
            .saturating_add(report.reclaimed_bytes);
        update_sample(&mut state, space, catalog, high_bytes, low_bytes);
        state.active
    }

    pub(crate) fn record_allocation_failure(&self) {
        let mut state = self.inner.state.lock();
        state.allocation_failures = state.allocation_failures.saturating_add(1);
        state.wakeups = state.wakeups.saturating_add(1);
        drop(state);
        self.inner.notify.notify_one();
    }

    pub(crate) fn refresh_catalog(&self, catalog: ObjectCatalogStats) {
        let mut state = self.inner.state.lock();
        state.pending_bytes = catalog.pending_bytes;
        state.live_bytes = catalog.live_bytes;
        state.retired_bytes = catalog.retired_bytes;
        state.reclaim_debt_bytes = catalog.reclaim_debt;
        state.requested_reclaim_debt_bytes = catalog.requested_reclaim_debt;
        state.watermark_reclaim_debt_bytes = catalog.watermark_reclaim_debt;
    }

    pub(crate) fn record_allocation_retry(&self) {
        let mut state = self.inner.state.lock();
        state.allocation_retries = state.allocation_retries.saturating_add(1);
    }

    pub(crate) fn record_allocation_retry_success(&self) {
        let mut state = self.inner.state.lock();
        state.allocation_retry_successes = state.allocation_retry_successes.saturating_add(1);
    }
}

fn update_sample(
    state: &mut MemoryEvictionStats,
    space: ReplicaClassSpaceStats,
    catalog: ObjectCatalogStats,
    high_bytes: u64,
    low_bytes: u64,
) {
    state.capacity_bytes = space.capacity_bytes;
    state.used_bytes = space.used_bytes;
    state.maximum_used_bytes = state.maximum_used_bytes.max(space.used_bytes);
    if space.capacity_bytes != 0 {
        let used_ratio_ppm = u64::try_from(
            u128::from(space.used_bytes) * u128::from(WATERMARK_SCALE)
                / u128::from(space.capacity_bytes),
        )
        .unwrap_or(u64::MAX);
        state.maximum_used_ratio_ppm = state.maximum_used_ratio_ppm.max(used_ratio_ppm);
    }
    state.available_bytes = space.available_bytes;
    state.high_watermark_bytes = high_bytes;
    state.low_watermark_bytes = low_bytes;
    state.pending_bytes = catalog.pending_bytes;
    state.live_bytes = catalog.live_bytes;
    state.retired_bytes = catalog.retired_bytes;
    state.reclaim_debt_bytes = catalog.reclaim_debt;
    state.requested_reclaim_debt_bytes = catalog.requested_reclaim_debt;
    state.watermark_reclaim_debt_bytes = catalog.watermark_reclaim_debt;
}

fn ratio_to_ppm(ratio: f64) -> Option<u32> {
    if !ratio.is_finite() || !(0.0..1.0).contains(&ratio) || ratio == 0.0 {
        return None;
    }
    let scaled = (ratio * WATERMARK_SCALE as f64).round();
    (scaled > 0.0 && scaled < WATERMARK_SCALE as f64).then_some(scaled as u32)
}

fn scale_bytes(bytes: u64, ratio_ppm: u32) -> u64 {
    let scaled = u128::from(bytes) * u128::from(ratio_ppm);
    u64::try_from(scaled / u128::from(WATERMARK_SCALE))
        .expect("a ratio below one cannot overflow the input byte count")
}
