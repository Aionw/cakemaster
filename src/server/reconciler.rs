//! Periodic convergence of client and object lifecycle state.

use super::MasterClock;
use crate::client::{
    ClientCleanupReport, ClientManager, ClientManagerError, GracefulUnmountReport,
};
use crate::object::reclamation::{CatalogTick, CollectBudget};
use crate::object::{ObjectManager, ObjectManagerMaintenance, TenantObjectManager};
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
    memory_eviction_notify: Option<Arc<Notify>>,
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
        let memory_eviction_notify = objects.memory_eviction_notify();
        Self {
            clients,
            objects,
            clock,
            reconcile_notify,
            memory_eviction_notify,
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
        let memory_eviction_notify = objects.memory_eviction_notify();
        Self {
            clients,
            objects,
            clock,
            reconcile_notify,
            memory_eviction_notify,
            config,
        }
    }

    /// Returns this reconciler's immutable scheduling and budget configuration.
    pub const fn config(&self) -> MasterReconcileConfig {
        self.config
    }

    /// Returns the shared client lifecycle manager driven by this reconciler.
    pub const fn client_manager(&self) -> &ClientManager {
        &self.clients
    }

    /// Returns the shared monotonic clock used by this reconciler.
    pub const fn clock(&self) -> &MasterClock {
        &self.clock
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
            let eviction_notified = async {
                match &self.memory_eviction_notify {
                    Some(notify) => notify.notified().await,
                    None => std::future::pending::<()>().await,
                }
            };
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
            tokio::pin!(eviction_notified);
            tokio::pin!(deadline_wait);
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    self.reconcile_and_reschedule().await;
                },
                _ = &mut deadline_wait => self.reconcile_and_reschedule().await,
                _ = &mut eviction_notified => self.reconcile_and_reschedule().await,
                _ = &mut notified => self.reconcile_and_reschedule().await,
            }
        }
    }

    async fn reconcile_and_reschedule(&self) {
        if self.reconcile_and_log() {
            tokio::task::yield_now().await;
            if let Some(notify) = &self.memory_eviction_notify {
                notify.notify_one();
            }
        }
    }

    fn reconcile_and_log(&self) -> bool {
        let report = self.reconcile_once();
        if let Err(error) = &report.client_cleanup {
            log::error!(
                target: "cakemaster::server::reconciler",
                cleanup_error:% = error;
                "master reconciliation client cleanup failed"
            );
        }
        let client_cleanup = report.client_cleanup.as_ref().ok();
        let catalog = report.object_collection.catalog;
        let memory_eviction = report.object_collection.memory_eviction;
        let did_work = client_cleanup.is_some_and(|report| {
            report.completed_sessions != 0
                || report.revoked_pending_writes != 0
                || report.invalidated_segments != 0
        }) || report.graceful_unmount.completed != 0
            || report.graceful_unmount.stale_or_cancelled != 0
            || report.graceful_unmount.retried != 0
            || report.object_collection.expired_writes != 0
            || catalog.expired_soft_pins != 0
            || catalog.invalidated_pending != 0
            || catalog.invalidated_published != 0
            || catalog.pruned_objects != 0
            || catalog.reclaimed_objects != 0
            || catalog.removed_empty_slots != 0
            || memory_eviction.is_some_and(|stats| stats.active);
        if did_work {
            log::debug!(
                target: "cakemaster::server::reconciler",
                completed_sessions = client_cleanup.map_or(0, |report| report.completed_sessions),
                revoked_pending_writes = client_cleanup.map_or(0, |report| report.revoked_pending_writes),
                invalidated_segments = client_cleanup.map_or(0, |report| report.invalidated_segments),
                graceful_unmount_completed = report.graceful_unmount.completed,
                graceful_unmount_retried = report.graceful_unmount.retried,
                expired_writes = report.object_collection.expired_writes,
                expired_soft_pins = catalog.expired_soft_pins,
                invalidated_pending = catalog.invalidated_pending,
                invalidated_published = catalog.invalidated_published,
                pruned_objects = catalog.pruned_objects,
                reclaimed_objects = catalog.reclaimed_objects,
                reclaimed_bytes = catalog.reclaimed_bytes,
                removed_empty_slots = catalog.removed_empty_slots;
                "master reconciliation completed work"
            );
            if let Some(stats) = memory_eviction {
                log::debug!(
                    target: "cakemaster::server::eviction",
                    active = stats.active,
                    capacity_bytes = stats.capacity_bytes,
                    used_bytes = stats.used_bytes,
                    maximum_used_bytes = stats.maximum_used_bytes,
                    maximum_used_ratio_ppm = stats.maximum_used_ratio_ppm,
                    high_watermark_bytes = stats.high_watermark_bytes,
                    low_watermark_bytes = stats.low_watermark_bytes,
                    live_bytes = stats.live_bytes,
                    retired_bytes = stats.retired_bytes,
                    reclaim_debt_bytes = stats.reclaim_debt_bytes,
                    requested_reclaim_debt_bytes = stats.requested_reclaim_debt_bytes,
                    allocation_reclaim_debt_bytes = stats.allocation_reclaim_debt_bytes,
                    watermark_reclaim_debt_bytes = stats.watermark_reclaim_debt_bytes,
                    trigger_events = stats.trigger_events,
                    allocation_failures = stats.allocation_failures;
                    "memory eviction controller sampled pressure"
                );
            }
        }
        memory_eviction.is_some_and(|stats| stats.active)
            && !catalog.busy
            && (catalog.reclaimed_objects != 0 || catalog.pruned_objects != 0)
    }
}
