use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Semaphore, mpsc};

use crate::error::RpcErrorCode;
use crate::function_id;
use crate::protocol::{
    FrameError, FrameLimits, RequestFrame, ResponseFrame, read_request, write_response,
};
use crate::struct_pack::{StructPack, deserialize, serialize};

/// Server-side limits and connection settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub frame_limits: FrameLimits,
    pub max_in_flight_per_connection: usize,
    pub response_queue_capacity: usize,
    pub tcp_nodelay: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            frame_limits: FrameLimits::default(),
            max_in_flight_per_connection: 256,
            response_queue_capacity: 256,
            tcp_nodelay: true,
        }
    }
}

/// Metadata and attachment belonging to one incoming request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub sequence: u32,
    pub function_id: u32,
    pub attachment: Vec<u8>,
    pub peer_addr: SocketAddr,
}

/// A successful handler value with an optional response attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponse<T> {
    pub value: T,
    pub attachment: Vec<u8>,
}

impl<T> RpcResponse<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            attachment: Vec::new(),
        }
    }

    pub fn with_attachment(value: T, attachment: Vec<u8>) -> Self {
        Self { value, attachment }
    }
}

/// An error intentionally returned by an RPC handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFailure {
    pub code: u16,
    pub message: String,
}

impl RpcFailure {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code: if code == 0 {
                RpcErrorCode::RpcThrowException as u16
            } else {
                code
            },
            message: message.into(),
        }
    }

    pub fn standard(code: RpcErrorCode) -> Self {
        Self::new(code as u16, code.message())
    }
}

impl fmt::Display for RpcFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RPC failure {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcFailure {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    DuplicateRoute {
        route_id: u32,
        existing_name: String,
        new_name: String,
    },
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateRoute {
                route_id,
                existing_name,
                new_name,
            } => write!(
                f,
                "route ID {route_id:#010x} is already registered as {existing_name:?}, cannot register {new_name:?}"
            ),
        }
    }
}

impl std::error::Error for RegisterError {}

struct EncodedResponse {
    body: Vec<u8>,
    attachment: Vec<u8>,
}

type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<EncodedResponse, RpcFailure>> + Send + 'static>>;

trait ErasedHandler: Send + Sync {
    fn call(&self, body: Vec<u8>, context: RequestContext) -> HandlerFuture;
}

struct TypedHandler<A, R, F> {
    function: F,
    marker: PhantomData<fn(A) -> R>,
}

impl<A, R, F, Fut> ErasedHandler for TypedHandler<A, R, F>
where
    A: StructPack + Send + 'static,
    R: StructPack + Send + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Vec<u8>, _context: RequestContext) -> HandlerFuture {
        let arguments = match deserialize::<A>(&body) {
            Ok(arguments) => arguments,
            Err(error) => {
                return Box::pin(async move {
                    Err(RpcFailure::new(
                        RpcErrorCode::InvalidRpcArguments as u16,
                        format!("invalid rpc arg: {error}"),
                    ))
                });
            }
        };
        let future = (self.function)(arguments);
        Box::pin(async move {
            let value = future.await?;
            let body = serialize(&value).map_err(|error| {
                RpcFailure::new(
                    RpcErrorCode::InvalidRpcResult as u16,
                    format!("failed to serialize RPC result: {error}"),
                )
            })?;
            Ok(EncodedResponse {
                body,
                attachment: Vec::new(),
            })
        })
    }
}

struct ContextHandler<A, R, F> {
    function: F,
    marker: PhantomData<fn(A) -> R>,
}

impl<A, R, F, Fut> ErasedHandler for ContextHandler<A, R, F>
where
    A: StructPack + Send + 'static,
    R: StructPack + Send + 'static,
    F: Fn(A, RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Vec<u8>, context: RequestContext) -> HandlerFuture {
        let arguments = match deserialize::<A>(&body) {
            Ok(arguments) => arguments,
            Err(error) => {
                return Box::pin(async move {
                    Err(RpcFailure::new(
                        RpcErrorCode::InvalidRpcArguments as u16,
                        format!("invalid rpc arg: {error}"),
                    ))
                });
            }
        };
        let future = (self.function)(arguments, context);
        Box::pin(async move {
            let response = future.await?;
            let body = serialize(&response.value).map_err(|error| {
                RpcFailure::new(
                    RpcErrorCode::InvalidRpcResult as u16,
                    format!("failed to serialize RPC result: {error}"),
                )
            })?;
            Ok(EncodedResponse {
                body,
                attachment: response.attachment,
            })
        })
    }
}

struct NoArgsHandler<R, F> {
    function: F,
    marker: PhantomData<fn() -> R>,
}

impl<R, F, Fut> ErasedHandler for NoArgsHandler<R, F>
where
    R: StructPack + Send + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Vec<u8>, _context: RequestContext) -> HandlerFuture {
        if !body.is_empty() {
            return Box::pin(async {
                Err(RpcFailure::standard(RpcErrorCode::InvalidRpcArguments))
            });
        }
        let future = (self.function)();
        Box::pin(async move {
            let value = future.await?;
            let body = serialize(&value).map_err(|error| {
                RpcFailure::new(
                    RpcErrorCode::InvalidRpcResult as u16,
                    format!("failed to serialize RPC result: {error}"),
                )
            })?;
            Ok(EncodedResponse {
                body,
                attachment: Vec::new(),
            })
        })
    }
}

struct NoArgsContextHandler<R, F> {
    function: F,
    marker: PhantomData<fn() -> R>,
}

impl<R, F, Fut> ErasedHandler for NoArgsContextHandler<R, F>
where
    R: StructPack + Send + 'static,
    F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Vec<u8>, context: RequestContext) -> HandlerFuture {
        if !body.is_empty() {
            return Box::pin(async {
                Err(RpcFailure::standard(RpcErrorCode::InvalidRpcArguments))
            });
        }
        let future = (self.function)(context);
        Box::pin(async move {
            let response = future.await?;
            let body = serialize(&response.value).map_err(|error| {
                RpcFailure::new(
                    RpcErrorCode::InvalidRpcResult as u16,
                    format!("failed to serialize RPC result: {error}"),
                )
            })?;
            Ok(EncodedResponse {
                body,
                attachment: response.attachment,
            })
        })
    }
}

#[derive(Clone)]
struct Route {
    name: String,
    handler: Arc<dyn ErasedHandler>,
}

/// Tokio coro_rpc server and typed route registry.
pub struct RpcServer {
    config: ServerConfig,
    routes: HashMap<u32, Route>,
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServer {
    pub fn new() -> Self {
        Self::with_config(ServerConfig::default())
    }

    pub fn with_config(config: ServerConfig) -> Self {
        Self {
            config,
            routes: HashMap::new(),
        }
    }

    pub fn register<A, R, F, Fut>(
        &mut self,
        function_name: impl Into<String>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        A: StructPack + Send + 'static,
        R: StructPack + Send + 'static,
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
    {
        let name = function_name.into();
        let route_id = function_id(&name);
        self.insert(
            route_id,
            name,
            Arc::new(TypedHandler::<A, R, F> {
                function,
                marker: PhantomData,
            }),
        )
    }

    pub fn register_with_context<A, R, F, Fut>(
        &mut self,
        function_name: impl Into<String>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        A: StructPack + Send + 'static,
        R: StructPack + Send + 'static,
        F: Fn(A, RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
    {
        let name = function_name.into();
        let route_id = function_id(&name);
        self.insert(
            route_id,
            name,
            Arc::new(ContextHandler::<A, R, F> {
                function,
                marker: PhantomData,
            }),
        )
    }

    pub fn register_no_args<R, F, Fut>(
        &mut self,
        function_name: impl Into<String>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        R: StructPack + Send + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
    {
        let name = function_name.into();
        let route_id = function_id(&name);
        self.insert(
            route_id,
            name,
            Arc::new(NoArgsHandler::<R, F> {
                function,
                marker: PhantomData,
            }),
        )
    }

    pub fn register_no_args_with_context<R, F, Fut>(
        &mut self,
        function_name: impl Into<String>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        R: StructPack + Send + 'static,
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
    {
        let name = function_name.into();
        let route_id = function_id(&name);
        self.insert(
            route_id,
            name,
            Arc::new(NoArgsContextHandler::<R, F> {
                function,
                marker: PhantomData,
            }),
        )
    }

    fn insert(
        &mut self,
        route_id: u32,
        name: String,
        handler: Arc<dyn ErasedHandler>,
    ) -> Result<&mut Self, RegisterError> {
        if let Some(existing) = self.routes.get(&route_id) {
            return Err(RegisterError::DuplicateRoute {
                route_id,
                existing_name: existing.name.clone(),
                new_name: name,
            });
        }
        self.routes.insert(route_id, Route { name, handler });
        Ok(self)
    }

    pub async fn bind(self, address: impl ToSocketAddrs) -> io::Result<BoundRpcServer> {
        let listener = TcpListener::bind(address).await?;
        Ok(BoundRpcServer {
            listener,
            config: self.config,
            routes: Arc::new(self.routes),
        })
    }

    pub async fn serve(self, address: impl ToSocketAddrs) -> io::Result<()> {
        self.bind(address).await?.run().await
    }
}

/// A bound server, useful for discovering an OS-assigned port before serving.
pub struct BoundRpcServer {
    listener: TcpListener,
    config: ServerConfig,
    routes: Arc<HashMap<u32, Route>>,
}

impl BoundRpcServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> io::Result<()> {
        self.run_until(std::future::pending::<()>()).await
    }

    pub async fn run_until<F>(self, shutdown: F) -> io::Result<()>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                accepted = self.listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    stream.set_nodelay(self.config.tcp_nodelay)?;
                    let routes = self.routes.clone();
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        let _ = serve_connection(stream, peer_addr, routes, config).await;
                    });
                }
            }
        }
    }
}

async fn serve_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    routes: Arc<HashMap<u32, Route>>,
    config: ServerConfig,
) -> Result<(), FrameError> {
    let (mut reader, mut writer) = stream.into_split();
    let (responses, mut response_rx) =
        mpsc::channel::<ResponseFrame>(config.response_queue_capacity.max(1));
    let writer_task = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            write_response(&mut writer, &response).await?;
        }
        Ok::<_, io::Error>(())
    });
    let permits = Arc::new(Semaphore::new(config.max_in_flight_per_connection.max(1)));

    loop {
        let request = match read_request(&mut reader, config.frame_limits).await {
            Ok(request) => request,
            Err(FrameError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore is never closed");
        let route = routes.get(&request.header.function_id).cloned();
        let response_tx = responses.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let response = dispatch_request(request, peer_addr, route).await;
            let _ = response_tx.send(response).await;
        });
    }

    drop(responses);
    let _ = writer_task.await;
    Ok(())
}

async fn dispatch_request(
    request: RequestFrame,
    peer_addr: SocketAddr,
    route: Option<Route>,
) -> ResponseFrame {
    let sequence = request.header.sequence;
    let Some(route) = route else {
        return failure_frame(
            sequence,
            RpcFailure::new(
                RpcErrorCode::FunctionNotRegistered as u16,
                "function not registered",
            ),
        );
    };
    let context = RequestContext {
        sequence,
        function_id: request.header.function_id,
        attachment: request.attachment,
        peer_addr,
    };
    match route.handler.call(request.body, context).await {
        Ok(response) => match ResponseFrame::new(sequence, 0, response.body, response.attachment) {
            Ok(frame) => frame,
            Err(_) => failure_frame(
                sequence,
                RpcFailure::standard(RpcErrorCode::MessageTooLarge),
            ),
        },
        Err(error) => failure_frame(sequence, error),
    }
}

fn failure_frame(sequence: u32, failure: RpcFailure) -> ResponseFrame {
    let (wire_code, body) = if failure.code >= 255 {
        let body = serialize(&(failure.code, failure.message))
            .expect("a small extended RPC error is always serializable");
        (255, body)
    } else {
        let body =
            serialize(&failure.message).expect("a small standard RPC error is always serializable");
        (failure.code as u8, body)
    };
    ResponseFrame::new(sequence, wire_code, body, Vec::new())
        .expect("a small RPC error frame always fits in u32 lengths")
}
