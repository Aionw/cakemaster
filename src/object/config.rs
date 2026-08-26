//! Object-catalog sizing, lease, and reclamation configuration.

use super::error::ObjectCatalogConfigError;

/// Upstream-compatible default soft-pin lifetime (30 minutes).
pub const DEFAULT_SOFT_PIN_TTL_TICKS: u64 = 30 * 60 * 1_000;
/// Upstream-compatible maximum request-level soft-pin lifetime (24 hours).
pub const DEFAULT_MAX_SOFT_PIN_TTL_TICKS: u64 = 24 * 60 * 60 * 1_000;
/// Upstream permits soft-pinned objects as a last-resort eviction candidate.
pub const DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS: bool = true;

/// Requested change to an object's committed soft pin.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SoftPinAction {
    /// Keep the currently committed deadline, if any.
    #[default]
    Preserve,
    /// Install a new deadline when the write commits.
    Enable,
    /// Clear the committed deadline when the write commits.
    Disable,
}

/// Pin policy carried by one put or upsert transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectPinRequest {
    soft_pin_action: SoftPinAction,
    soft_pin_ttl_ticks: Option<u64>,
    with_hard_pin: bool,
}

impl ObjectPinRequest {
    pub const fn new(
        soft_pin_action: SoftPinAction,
        soft_pin_ttl_ticks: Option<u64>,
        with_hard_pin: bool,
    ) -> Self {
        Self {
            soft_pin_action,
            soft_pin_ttl_ticks,
            with_hard_pin,
        }
    }

    pub const fn soft_pin_action(self) -> SoftPinAction {
        self.soft_pin_action
    }

    pub const fn soft_pin_ttl_ticks(self) -> Option<u64> {
        self.soft_pin_ttl_ticks
    }

    pub const fn with_hard_pin(self) -> bool {
        self.with_hard_pin
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ResolvedObjectPinRequest {
    pub(super) soft_pin: ResolvedSoftPinRequest,
    pub(super) with_hard_pin: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ResolvedSoftPinRequest {
    pub(super) action: SoftPinAction,
    pub(super) ttl_ticks: u64,
}

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
    pub(super) default_soft_pin_ttl_ticks: u64,
    pub(super) max_soft_pin_ttl_ticks: u64,
    pub(super) allow_evict_soft_pinned_objects: bool,
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
            default_soft_pin_ttl_ticks: DEFAULT_SOFT_PIN_TTL_TICKS,
            max_soft_pin_ttl_ticks: DEFAULT_MAX_SOFT_PIN_TTL_TICKS,
            allow_evict_soft_pinned_objects: DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS,
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

    pub const fn with_soft_pin_ttl(mut self, default_ticks: u64, max_ticks: u64) -> Self {
        self.default_soft_pin_ttl_ticks = default_ticks;
        self.max_soft_pin_ttl_ticks = max_ticks;
        self
    }

    pub const fn with_soft_pin_eviction(mut self, allow: bool) -> Self {
        self.allow_evict_soft_pinned_objects = allow;
        self
    }

    pub const fn default_soft_pin_ttl_ticks(self) -> u64 {
        self.default_soft_pin_ttl_ticks
    }

    pub const fn max_soft_pin_ttl_ticks(self) -> u64 {
        self.max_soft_pin_ttl_ticks
    }

    pub const fn allow_evict_soft_pinned_objects(self) -> bool {
        self.allow_evict_soft_pinned_objects
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
        if self.default_soft_pin_ttl_ticks > self.max_soft_pin_ttl_ticks {
            return Err(ObjectCatalogConfigError::DefaultSoftPinTtlExceedsMaximum {
                default_soft_pin_ttl_ticks: self.default_soft_pin_ttl_ticks,
                max_soft_pin_ttl_ticks: self.max_soft_pin_ttl_ticks,
            });
        }
        Ok(())
    }

    pub(super) fn resolve_pin_request(
        self,
        request: ObjectPinRequest,
    ) -> Result<ResolvedObjectPinRequest, ObjectCatalogConfigError> {
        let ttl_ticks = match request.soft_pin_action {
            SoftPinAction::Preserve | SoftPinAction::Disable => {
                if request.soft_pin_ttl_ticks.is_some() {
                    return Err(ObjectCatalogConfigError::SoftPinTtlRequiresEnable);
                }
                0
            }
            SoftPinAction::Enable => {
                let ttl_ticks = request
                    .soft_pin_ttl_ticks
                    .unwrap_or(self.default_soft_pin_ttl_ticks);
                if ttl_ticks > self.max_soft_pin_ttl_ticks {
                    return Err(ObjectCatalogConfigError::SoftPinTtlExceedsMaximum {
                        soft_pin_ttl_ticks: ttl_ticks,
                        max_soft_pin_ttl_ticks: self.max_soft_pin_ttl_ticks,
                    });
                }
                ttl_ticks
            }
        };
        Ok(ResolvedObjectPinRequest {
            soft_pin: ResolvedSoftPinRequest {
                action: request.soft_pin_action,
                ttl_ticks,
            },
            with_hard_pin: request.with_hard_pin,
        })
    }
}

impl Default for ObjectCatalogConfig {
    fn default() -> Self {
        Self::new(64 * 1024)
    }
}
