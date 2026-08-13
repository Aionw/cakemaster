//! Object-catalog sizing, lease, and reclamation configuration.

use super::error::ObjectCatalogConfigError;

/// Complete configuration for an [`ObjectCatalog`](super::ObjectCatalog).
///
/// Builder methods consume and return the configuration, so partially updated
/// values cannot be observed through shared mutable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectCatalogConfig {
    pub(super) expected_objects: usize,
    pub(super) lease_ttl_ticks: u64,
    pub(super) lease_refresh_ticks: u64,
    pub(super) pending_timeout_ticks: u64,
    pub(super) empty_slot_grace_ticks: u64,
    pub(super) max_retired_bytes: u64,
}

impl ObjectCatalogConfig {
    pub const fn new(expected_objects: usize) -> Self {
        Self {
            expected_objects,
            lease_ttl_ticks: 10_000,
            lease_refresh_ticks: 5_000,
            pending_timeout_ticks: 30_000,
            empty_slot_grace_ticks: 60_000,
            max_retired_bytes: 1_u64 << 30,
        }
    }

    pub const fn with_lease(mut self, ttl_ticks: u64, refresh_ticks: u64) -> Self {
        self.lease_ttl_ticks = ttl_ticks;
        self.lease_refresh_ticks = refresh_ticks;
        self
    }

    pub const fn with_pending_timeout(mut self, ticks: u64) -> Self {
        self.pending_timeout_ticks = ticks;
        self
    }

    pub const fn with_empty_slot_grace(mut self, ticks: u64) -> Self {
        self.empty_slot_grace_ticks = ticks;
        self
    }

    pub const fn with_max_retired_bytes(mut self, bytes: u64) -> Self {
        self.max_retired_bytes = bytes;
        self
    }

    pub(super) fn validate(self) -> Result<(), ObjectCatalogConfigError> {
        if self.expected_objects == 0 {
            return Err(ObjectCatalogConfigError::ZeroExpectedObjects);
        }
        if self.lease_ttl_ticks == 0 {
            return Err(ObjectCatalogConfigError::ZeroLeaseTtl);
        }
        if self.lease_refresh_ticks > self.lease_ttl_ticks {
            return Err(ObjectCatalogConfigError::LeaseRefreshExceedsTtl {
                lease_ttl_ticks: self.lease_ttl_ticks,
                lease_refresh_ticks: self.lease_refresh_ticks,
            });
        }
        if self.pending_timeout_ticks == 0 {
            return Err(ObjectCatalogConfigError::ZeroPendingTimeout);
        }
        if self.max_retired_bytes == 0 {
            return Err(ObjectCatalogConfigError::ZeroMaxRetiredBytes);
        }
        Ok(())
    }
}

impl Default for ObjectCatalogConfig {
    fn default() -> Self {
        Self::new(64 * 1024)
    }
}
