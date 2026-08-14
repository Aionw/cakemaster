//! Production composition and lifecycle for the supported Mooncake RPC subset.

use super::{
    DEFAULT_MASTER_VIEW_VERSION, MasterClock, MasterReconcileConfig, MasterReconciler,
    ObjectCatalogRpcService,
};
use crate::client::{ClientLifecycleConfig, ClientLifecycleConfigError, ClientManager};
use crate::mooncake::WrappedMasterServiceServer;
use crate::object::error::ObjectCatalogConfigError;
use crate::object::{ObjectCatalogConfig, ObjectManager};
use crate::segment::error::PoolConfigError;
use crate::segment::{SegmentPool, SegmentPoolConfig};
use coro_rpc::{BoundRpcServer, RegisterError};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

/// Conservative loopback default for the production Mooncake RPC listener.
pub const DEFAULT_MOONCAKE_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50051);

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
    client_lifecycle: ClientLifecycleConfig,
    reconcile: MasterReconcileConfig,
    view_version: i64,
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

    pub const fn listen_addr(self) -> SocketAddr {
        self.listen_addr
    }

    pub const fn segment_pool(self) -> SegmentPoolConfig {
        self.segment_pool
    }

    pub const fn object_catalog(self) -> ObjectCatalogConfig {
        self.object_catalog
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
            client_lifecycle: ClientLifecycleConfig::default(),
            reconcile: MasterReconcileConfig::default(),
            view_version: DEFAULT_MASTER_VIEW_VERSION,
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
    manager: Arc<ObjectManager>,
    clock: MasterClock,
    service: ObjectCatalogRpcService,
    reconciler: MasterReconciler,
}

impl MooncakeServerComposition {
    fn new(config: MooncakeServerConfig) -> Result<Self, MooncakeServerBuildError> {
        let pool = Arc::new(SegmentPool::with_config(config.segment_pool())?);
        let manager = Arc::new(ObjectManager::with_config(
            pool.clone(),
            config.object_catalog(),
        )?);
        let clock = MasterClock::new();
        let service = ObjectCatalogRpcService::new_with_client_config(
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

    pub const fn manager(&self) -> &Arc<ObjectManager> {
        &self.manager
    }

    pub const fn clock(&self) -> &MasterClock {
        &self.clock
    }

    pub const fn service(&self) -> &ObjectCatalogRpcService {
        &self.service
    }

    pub const fn reconciler(&self) -> &MasterReconciler {
        &self.reconciler
    }

    /// Registers the supported `WrappedMasterService` routes and binds TCP.
    pub async fn bind(self) -> Result<BoundMooncakeServer, MooncakeServerBindError> {
        let clients = self.service.client_manager().clone();
        let rpc_server = WrappedMasterServiceServer::new(self.service).into_rpc_server()?;
        let server = rpc_server.bind(self.config.listen_addr()).await?;
        Ok(BoundMooncakeServer {
            server,
            reconciler: self.reconciler,
            pool: self.pool,
            manager: self.manager,
            clients,
            clock: self.clock,
        })
    }
}

/// Bound production runtime with explicit shared-state handles.
pub struct BoundMooncakeServer {
    server: BoundRpcServer,
    reconciler: MasterReconciler,
    pool: Arc<SegmentPool>,
    manager: Arc<ObjectManager>,
    clients: ClientManager,
    clock: MasterClock,
}

impl BoundMooncakeServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.server.local_addr()
    }

    pub const fn pool(&self) -> &Arc<SegmentPool> {
        &self.pool
    }

    pub const fn manager(&self) -> &Arc<ObjectManager> {
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut server = Box::pin(
            self.server
                .run_until(shutdown_requested(shutdown_rx.clone())),
        );
        let mut reconciler = Box::pin(self.reconciler.run_until(shutdown_requested(shutdown_rx)));
        tokio::pin!(shutdown);

        enum FirstExit {
            Shutdown,
            Server(io::Result<()>),
            Reconciler,
        }

        let first = tokio::select! {
            _ = &mut shutdown => FirstExit::Shutdown,
            result = &mut server => FirstExit::Server(result),
            () = &mut reconciler => FirstExit::Reconciler,
        };
        shutdown_tx.send_replace(true);

        match first {
            FirstExit::Shutdown => {
                let (server_result, ()) = tokio::join!(&mut server, &mut reconciler);
                server_result
            }
            FirstExit::Server(server_result) => {
                reconciler.await;
                server_result
            }
            FirstExit::Reconciler => server.await,
        }
    }
}

async fn shutdown_requested(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}
