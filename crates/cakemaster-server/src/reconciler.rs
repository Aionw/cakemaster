//! Periodic convergence of client and object lifecycle state.

use crate::MasterClock;
use cakemaster::client::{
    ClientCleanupReport, ClientManager, ClientManagerError, GracefulUnmountReport,
};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{ObjectManager, ObjectManagerMaintenance, TenantObjectManager};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

/// Default delay between bounded reconciliation steps.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_millis(100);
/// Default candidate, reclaim, and empty-slot budgets for each object step.
pub const DEFAULT_OBJECT_COLLECTION_BUDGET: CollectBudget = CollectBudget::new(256, 256, 64);

/// Scheduling interval and object-collection budget for a [`MasterReconciler`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MasterReconcileConfig {
    interval: Duration,
    object_budget: CollectBudget,
}

impl MasterReconcileConfig {
    /// Creates a configuration, rejecting an interval that cannot make progress.
    pub const fn new(
        interval: Duration,
        object_budget: CollectBudget,
    ) -> Result<Self, MasterReconcileConfigError> {
        if interval.is_zero() {
            return Err(MasterReconcileConfigError::ZeroInterval);
        }
        Ok(Self {
            interval,
            object_budget,
        })
    }

    /// Returns the delay between reconciliation steps.
    pub const fn interval(self) -> Duration {
        self.interval
    }

    /// Returns the object collection budget applied to each step.
    pub const fn object_budget(self) -> CollectBudget {
        self.object_budget
    }
}

impl Default for MasterReconcileConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_RECONCILE_INTERVAL,
            object_budget: DEFAULT_OBJECT_COLLECTION_BUDGET,
        }
    }
}

/// Invalid [`MasterReconcileConfig`] values.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MasterReconcileConfigError {
    #[error("master reconcile interval must be greater than zero")]
    ZeroInterval,
}

/// Complete outcome of one client-and-object convergence step.
#[derive(Debug)]
pub struct ReconcileStepReport {
    /// Client cleanup outcome. Object collection still runs when this is an error.
    pub client_cleanup: Result<ClientCleanupReport, ClientManagerError>,
    /// Due segment-level graceful removals processed after client cleanup.
    pub graceful_unmount: GracefulUnmountReport,
    /// Object retirement and reclamation work completed by this step.
    pub object_collection: ObjectManagerMaintenance,
}

trait ObjectReconcileBackend: Send + Sync {
    fn reconcile_objects(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
    ) -> ObjectManagerMaintenance;
}

impl ObjectReconcileBackend for ObjectManager {
    fn reconcile_objects(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
    ) -> ObjectManagerMaintenance {
        self.maintenance(now, budget)
    }
}

impl ObjectReconcileBackend for TenantObjectManager {
    fn reconcile_objects(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
    ) -> ObjectManagerMaintenance {
        self.maintenance(now, budget)
    }
}

/// Periodically converges client sessions, segment membership, and object state.
///
/// The reconciler does not spawn itself. Its owner runs [`Self::run_until`]
/// alongside the RPC server and supplies the same shutdown lifecycle.
#[derive(Clone)]
pub struct MasterReconciler {
    clients: ClientManager,
    objects: Arc<dyn ObjectReconcileBackend>,
    clock: MasterClock,
    reconcile_notify: Arc<Notify>,
    config: MasterReconcileConfig,
}

impl MasterReconciler {
    pub(crate) fn for_object_manager(
        clients: ClientManager,
        objects: Arc<ObjectManager>,
        clock: MasterClock,
        reconcile_notify: Arc<Notify>,
        config: MasterReconcileConfig,
    ) -> Self {
        Self {
            clients,
            objects,
            clock,
            reconcile_notify,
            config,
        }
    }

    pub(crate) fn for_tenant_object_manager(
        clients: ClientManager,
        objects: Arc<TenantObjectManager>,
        clock: MasterClock,
        reconcile_notify: Arc<Notify>,
        config: MasterReconcileConfig,
    ) -> Self {
        Self {
            clients,
            objects,
            clock,
            reconcile_notify,
            config,
        }
    }

    /// Returns this reconciler's immutable scheduling and budget configuration.
    pub const fn config(&self) -> MasterReconcileConfig {
        self.config
    }

    /// Runs one bounded convergence step.
    ///
    /// Object collection always runs, even when client cleanup reports an
    /// error. An unfinished client cleanup claim is requeued by its Drop path.
    pub fn reconcile_once(&self) -> ReconcileStepReport {
        let client_now = self.clock.client_now();
        let catalog_now = self.clock.now();
        let client_cleanup = self.clients.run_cleanup_step(client_now, catalog_now);
        let graceful_unmount = self.clients.run_graceful_unmount_step(client_now);
        let object_collection = self
            .objects
            .reconcile_objects(catalog_now, self.config.object_budget());
        ReconcileStepReport {
            client_cleanup,
            graceful_unmount,
            object_collection,
        }
    }

    /// Runs bounded convergence steps until the supplied shutdown future wins.
    pub async fn run_until<F>(self, shutdown: F)
    where
        F: Future<Output = ()>,
    {
        let first_tick = Instant::now() + self.config.interval();
        let mut interval = tokio::time::interval_at(first_tick, self.config.interval());
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::pin!(shutdown);

        loop {
            let notified = self.reconcile_notify.notified();
            let deadline = self.clients.next_graceful_unmount_deadline();
            let deadline_wait = async {
                match deadline {
                    Some(deadline) => {
                        tokio::time::sleep(self.clock.delay_until_client_tick(deadline)).await
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(notified);
            tokio::pin!(deadline_wait);
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    self.reconcile_and_log();
                },
                _ = &mut deadline_wait => self.reconcile_and_log(),
                _ = &mut notified => {},
            }
        }
    }

    fn reconcile_and_log(&self) {
        let report = self.reconcile_once();
        if let Err(error) = &report.client_cleanup {
            log::error!(
                target: "cakemaster_server::reconciler",
                cleanup_error:% = error;
                "master reconciliation client cleanup failed"
            );
        }
    }
}
