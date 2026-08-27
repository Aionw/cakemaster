//! Production composition and lifecycle for the supported Mooncake RPC subset.

use super::{
    DEFAULT_MASTER_VIEW_VERSION, MasterClock, MasterReconcileConfig, MasterReconciler,
    ObjectCatalogRpcService, ShardedObjectManager,
};
use crate::client::{ClientLifecycleConfig, ClientLifecycleConfigError, ClientManager};
use crate::mooncake::WrappedMasterServiceServer;
use crate::object::error::ObjectCatalogConfigError;
use crate::object::{MemoryEvictionConfig, ObjectCatalogConfig};
use crate::segment::error::PoolConfigError;
use crate::segment::{SegmentPool, SegmentPoolConfig};
use coro_rpc::{CompioLocalTask, RegisterError, ServerConfig};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;

/// Conservative loopback default for the production Mooncake RPC listener.
pub const DEFAULT_MOONCAKE_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50051);
pub const DEFAULT_METADATA_SHARDS: usize = 1;

/// Complete in-process configuration for the production Mooncake server.
///
/// The default keeps all metadata in memory, exposes only loopback, and uses
/// the validated defaults of the core managers. Callers can replace each core
/// configuration without introducing a second manager into the composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MooncakeServerConfig {
    listen_addr: SocketAddr,
    segment_pool: SegmentPoolConfig,
    object_catalog: ObjectCatalogConfig,
    memory_eviction: MemoryEvictionConfig,
    client_lifecycle: ClientLifecycleConfig,
    reconcile: MasterReconcileConfig,
    view_version: i64,
    access_log: bool,
    metadata_shards: usize,
}

impl MooncakeServerConfig {
    pub const fn with_listen_addr(mut self, listen_addr: SocketAddr) -> Self {
        self.listen_addr = listen_addr;
        self
    }

    pub const fn with_segment_pool(mut self, segment_pool: SegmentPoolConfig) -> Self {
        self.segment_pool = segment_pool;
        self
    }

    pub const fn with_object_catalog(mut self, object_catalog: ObjectCatalogConfig) -> Self {
        self.object_catalog = object_catalog;
        self
    }

    pub const fn with_memory_eviction(mut self, memory_eviction: MemoryEvictionConfig) -> Self {
        self.memory_eviction = memory_eviction;
        self
    }

    pub const fn with_client_lifecycle(mut self, client_lifecycle: ClientLifecycleConfig) -> Self {
        self.client_lifecycle = client_lifecycle;
        self
    }

    pub const fn with_reconcile(mut self, reconcile: MasterReconcileConfig) -> Self {
        self.reconcile = reconcile;
        self
    }

    pub const fn with_view_version(mut self, view_version: i64) -> Self {
        self.view_version = view_version;
        self
    }

    pub const fn with_access_log(mut self, enabled: bool) -> Self {
        self.access_log = enabled;
        self
    }

    pub const fn with_metadata_shards(mut self, metadata_shards: usize) -> Self {
        self.metadata_shards = metadata_shards;
        self
    }

    pub const fn listen_addr(self) -> SocketAddr {
        self.listen_addr
    }

    pub const fn segment_pool(self) -> SegmentPoolConfig {
        self.segment_pool
    }

    pub const fn object_catalog(self) -> ObjectCatalogConfig {
        self.object_catalog
    }

    pub const fn memory_eviction(self) -> MemoryEvictionConfig {
        self.memory_eviction
    }

    pub const fn client_lifecycle(self) -> ClientLifecycleConfig {
        self.client_lifecycle
    }

    pub const fn reconcile(self) -> MasterReconcileConfig {
        self.reconcile
    }

    pub const fn view_version(self) -> i64 {
        self.view_version
    }

    pub const fn access_log(self) -> bool {
        self.access_log
    }

    pub const fn metadata_shards(self) -> usize {
        self.metadata_shards
    }

    /// Builds one explicit manager/service/reconciler object graph.
    pub fn build(self) -> Result<MooncakeServerComposition, MooncakeServerBuildError> {
        MooncakeServerComposition::new(self)
    }
}

impl Default for MooncakeServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_MOONCAKE_LISTEN_ADDR,
            segment_pool: SegmentPoolConfig::default(),
            object_catalog: ObjectCatalogConfig::default(),
            memory_eviction: MemoryEvictionConfig::default(),
            client_lifecycle: ClientLifecycleConfig::default(),
            reconcile: MasterReconcileConfig::default(),
            view_version: DEFAULT_MASTER_VIEW_VERSION,
            access_log: false,
            metadata_shards: DEFAULT_METADATA_SHARDS,
        }
    }
}

/// Errors produced while validating and constructing the production graph.
#[derive(Debug, Error)]
pub enum MooncakeServerBuildError {
    #[error("invalid segment pool configuration: {0}")]
    SegmentPool(#[from] PoolConfigError),
    #[error("invalid object catalog configuration: {0}")]
    ObjectCatalog(#[from] ObjectCatalogConfigError),
    #[error("invalid client lifecycle configuration: {0}")]
    ClientLifecycle(#[from] ClientLifecycleConfigError),
}

/// Errors produced while registering routes or binding the TCP listener.
#[derive(Debug, Error)]
pub enum MooncakeServerBindError {
    #[error("failed to register Mooncake RPC routes: {0}")]
    Register(#[from] RegisterError),
    #[error("failed to bind Mooncake RPC listener: {0}")]
    Listen(#[from] io::Error),
}

/// Inspectable, not-yet-bound production object graph.
///
/// Keeping this stage public makes manager and lifecycle wiring directly
/// testable without starting a subprocess.
pub struct MooncakeServerComposition {
    config: MooncakeServerConfig,
    pool: Arc<SegmentPool>,
    manager: Arc<ShardedObjectManager>,
    clock: MasterClock,
    service: ObjectCatalogRpcService<ShardedObjectManager>,
    reconciler: MasterReconciler,
}

impl MooncakeServerComposition {
    fn new(config: MooncakeServerConfig) -> Result<Self, MooncakeServerBuildError> {
        let pool = Arc::new(SegmentPool::with_config(
            config.segment_pool().with_allocator_shards(1),
        )?);
        let manager = Arc::new(ShardedObjectManager::new(
            pool.clone(),
            config.object_catalog(),
            config.memory_eviction(),
            config.metadata_shards(),
        )?);
        let clock = MasterClock::new();
        let service = ObjectCatalogRpcService::new_sharded_with_client_config(
            manager.clone(),
            config.client_lifecycle(),
            clock.clone(),
            config.view_version(),
        )?;
        let reconciler = service.reconciler(config.reconcile());
        Ok(Self {
            config,
            pool,
            manager,
            clock,
            service,
            reconciler,
        })
    }

    pub const fn config(&self) -> MooncakeServerConfig {
        self.config
    }

    pub const fn pool(&self) -> &Arc<SegmentPool> {
        &self.pool
    }

    pub const fn manager(&self) -> &Arc<ShardedObjectManager> {
        &self.manager
    }

    pub const fn clock(&self) -> &MasterClock {
        &self.clock
    }

    pub const fn service(&self) -> &ObjectCatalogRpcService<ShardedObjectManager> {
        &self.service
    }

    pub const fn reconciler(&self) -> &MasterReconciler {
        &self.reconciler
    }

    /// Registers the supported `WrappedMasterService` routes and binds TCP.
    pub async fn bind(self) -> Result<BoundMooncakeServer, MooncakeServerBindError> {
        let clients = self.service.client_manager().clone();
        let rpc_server = WrappedMasterServiceServer::new(self.service)
            .into_rpc_server_with_config(
                ServerConfig::default().with_access_log(self.config.access_log()),
            )?;
        let server = rpc_server.bind_compio(self.config.listen_addr())?;
        let local_addr = server.local_addr()?;
        let mut tasks = server.into_shard_tasks(self.manager.shard_count())?;
        let shard_zero_server = tasks.remove(0);
        let reconciler = self.reconciler;
        let shard_zero: CompioLocalTask = Box::new(move || {
            let server = shard_zero_server();
            Box::pin(async move {
                tokio::pin!(server);
                let reconcile = reconciler.run_compio();
                tokio::pin!(reconcile);
                tokio::select! {
                    result = &mut server => result,
                    () = &mut reconcile => Err(io::Error::other(
                        "Compio reconciler stopped unexpectedly"
                    )),
                }
            })
        });
        tasks.insert(0, shard_zero);
        let server_exits = self.manager.start_runtime_tasks(tasks)?;
        Ok(BoundMooncakeServer {
            local_addr,
            server_exits,
            pool: self.pool,
            manager: self.manager,
            clients,
            clock: self.clock,
        })
    }
}

/// Bound production runtime with explicit shared-state handles.
pub struct BoundMooncakeServer {
    local_addr: SocketAddr,
    server_exits: Vec<tokio::sync::oneshot::Receiver<io::Result<()>>>,
    pool: Arc<SegmentPool>,
    manager: Arc<ShardedObjectManager>,
    clients: ClientManager,
    clock: MasterClock,
}

impl BoundMooncakeServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    pub const fn pool(&self) -> &Arc<SegmentPool> {
        &self.pool
    }

    pub const fn manager(&self) -> &Arc<ShardedObjectManager> {
        &self.manager
    }

    pub const fn client_manager(&self) -> &ClientManager {
        &self.clients
    }

    pub const fn clock(&self) -> &MasterClock {
        &self.clock
    }

    /// Runs the RPC listener and reconciler concurrently until shutdown.
    ///
    /// Completion of any participant broadcasts shutdown to the other one.
    /// This future returns only after the server has closed and joined every
    /// connection task and the reconciler loop has completely exited.
    pub async fn run_until<F>(self, shutdown: F) -> io::Result<()>
    where
        F: Future<Output = ()>,
    {
        let mut server_exits = FuturesUnordered::new();
        for exit in self.server_exits {
            server_exits.push(exit);
        }
        tokio::pin!(shutdown);

        enum FirstExit {
            Shutdown,
            Server(io::Result<()>),
        }

        let first = tokio::select! {
            _ = &mut shutdown => FirstExit::Shutdown,
            result = server_exits.next() => FirstExit::Server(match result {
                Some(Ok(result)) => result,
                Some(Err(_)) => Err(io::Error::other("Compio shard RPC task stopped unexpectedly")),
                None => Err(io::Error::other("Compio shard RPC tasks were not started")),
            }),
        };
        self.manager.stop_runtime_tasks().await;

        match first {
            FirstExit::Shutdown => Ok(()),
            FirstExit::Server(server_result) => server_result,
        }
    }
}
