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
}
