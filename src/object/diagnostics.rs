//! Read-only object-catalog diagnostics.

/// Flat metrics projection of the catalog's grouped internal state.
///
/// Keeping metric names at one level makes logs and exporters ergonomic;
/// mutation ownership remains separated inside `CatalogInner`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCatalogStats {
    // Stable index and write lifecycle.
    pub slots: usize,
    pub claims: usize,
    pub pending_objects: usize,
    pub published_objects: usize,
    pub pending_bytes: u64,
    pub live_bytes: u64,
    pub retired_bytes: u64,
    // Reclaim pressure from independent producers.
    /// Largest outstanding global reclaim request after coalescing sources.
    pub reclaim_debt: u64,
    /// Explicit/admin reclaim debt, excluding the production watermark target.
    pub requested_reclaim_debt: u64,
    /// Physical bytes still required by the production watermark controller.
    pub watermark_reclaim_debt: u64,
    // Bounded collector queue diagnostics.
    pub pending_candidates: usize,
    pub liveness_scan_remaining: usize,
    pub young_candidates: usize,
    pub protected_candidates: usize,
    pub retired_candidates: usize,
    pub empty_slot_candidates: usize,
}
