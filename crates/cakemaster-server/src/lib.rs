//! RPC adapters for Cakemaster domain services.

mod client_task_queue;
mod clock;
mod object_catalog_rpc;
mod reconciler;

pub use client_task_queue::{ClientTaskQueue, ClientTaskQueueError, ClientTaskRx, ClientTaskTx};
pub use clock::MasterClock;
pub use object_catalog_rpc::{DEFAULT_MASTER_VIEW_VERSION, ObjectCatalogRpcService};
pub use reconciler::{
    DEFAULT_OBJECT_COLLECTION_BUDGET, DEFAULT_RECONCILE_INTERVAL, MasterReconcileConfig,
    MasterReconcileConfigError, MasterReconciler, ReconcileStepReport,
};
