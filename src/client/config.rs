//! Client-registry TTL, capacity, and cleanup-scan configuration.

use super::error::ClientLifecycleConfigError;

/// Complete configuration for a [`ClientRegistry`](super::ClientRegistry).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientLifecycleConfig {
    pub(super) ttl_ticks: u64,
    pub(super) max_clients: usize,
    pub(super) cleanup_scan_budget: usize,
}

impl ClientLifecycleConfig {
    pub const fn new(max_clients: usize) -> Self {
        Self {
            ttl_ticks: 10_000,
            max_clients,
            cleanup_scan_budget: 256,
        }
    }

    pub const fn with_ttl(mut self, ttl_ticks: u64) -> Self {
        self.ttl_ticks = ttl_ticks;
        self
    }

    pub const fn with_cleanup_scan_budget(mut self, cleanup_scan_budget: usize) -> Self {
        self.cleanup_scan_budget = cleanup_scan_budget;
        self
    }

    pub const fn ttl_ticks(self) -> u64 {
        self.ttl_ticks
    }

    pub const fn max_clients(self) -> usize {
        self.max_clients
    }

    pub const fn cleanup_scan_budget(self) -> usize {
        self.cleanup_scan_budget
    }

    pub(super) fn validate(self) -> Result<(), ClientLifecycleConfigError> {
        if self.ttl_ticks == 0 {
            return Err(ClientLifecycleConfigError::ZeroTtl);
        }
        if self.max_clients == 0 {
            return Err(ClientLifecycleConfigError::ZeroMaxClients);
        }
        if self.cleanup_scan_budget == 0 {
            return Err(ClientLifecycleConfigError::ZeroCleanupScanBudget);
        }
        Ok(())
    }
}

impl Default for ClientLifecycleConfig {
    fn default() -> Self {
        Self::new(64 * 1024)
    }
}
