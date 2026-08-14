//! RPC adapters for Cakemaster domain services.

mod client_task_queue;
mod clock;
mod object_catalog_rpc;

pub use client_task_queue::{ClientTaskQueue, ClientTaskQueueError, ClientTaskRx, ClientTaskTx};
pub use clock::MasterClock;
pub use object_catalog_rpc::{DEFAULT_MASTER_VIEW_VERSION, ObjectCatalogRpcService};
