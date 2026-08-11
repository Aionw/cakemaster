//! Process-local monotonic time shared by server runtime components.

use cakemaster::object::reclamation::CatalogTick;
use std::time::Instant;

/// A cloneable time origin for catalog leases, deadlines, and maintenance.
///
/// All components that operate on the same object manager should receive a
/// clone of one `MasterClock`; independently created clocks do not share a
/// comparable [`CatalogTick`] domain.
#[derive(Clone, Debug)]
pub struct MasterClock {
    epoch: Instant,
}

impl MasterClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    pub fn now(&self) -> CatalogTick {
        let millis = self.epoch.elapsed().as_millis();
        CatalogTick::new(u64::try_from(millis).unwrap_or(u64::MAX))
    }
}

impl Default for MasterClock {
    fn default() -> Self {
        Self::new()
    }
}
