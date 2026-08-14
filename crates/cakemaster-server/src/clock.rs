//! Process-local monotonic time shared by server runtime components.

use cakemaster::client::ClientTick;
use cakemaster::object::reclamation::CatalogTick;
use tokio::time::Instant;

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
        CatalogTick::new(self.elapsed_millis())
    }

    pub fn client_now(&self) -> ClientTick {
        ClientTick::new(self.elapsed_millis())
    }

    fn elapsed_millis(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

impl Default for MasterClock {
    fn default() -> Self {
        Self::new()
    }
}
