//! RPC adapters for Cakemaster domain services.

mod client_task_queue;
mod clock;
mod reconciler;
pub mod rpc;
mod runtime;

pub use client_task_queue::{ClientTaskQueue, ClientTaskQueueError, ClientTaskRx, ClientTaskTx};
pub use clock::MasterClock;
pub use reconciler::{
    DEFAULT_OBJECT_COLLECTION_BUDGET, DEFAULT_RECONCILE_INTERVAL, MasterReconcileConfig,
    MasterReconcileConfigError, MasterReconciler, ReconcileStepReport,
};
pub use rpc::{DEFAULT_MASTER_VIEW_VERSION, ObjectCatalogRpcService};
pub use runtime::{
    BoundMooncakeServer, DEFAULT_MOONCAKE_LISTEN_ADDR, MooncakeServerBindError,
    MooncakeServerBuildError, MooncakeServerComposition, MooncakeServerConfig,
};
