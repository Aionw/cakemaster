//! Read-only object-catalog diagnostics.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCatalogStats {
    pub slots: usize,
    pub claims: usize,
    pub pending_objects: usize,
    pub published_objects: usize,
    pub pending_bytes: u64,
    pub live_bytes: u64,
    pub retired_bytes: u64,
    pub reclaim_debt: u64,
    pub pending_candidates: usize,
    pub soft_pin_candidates: usize,
    pub liveness_scan_remaining: usize,
    pub young_candidates: usize,
    pub protected_candidates: usize,
    pub retired_candidates: usize,
    pub empty_slot_candidates: usize,
}
