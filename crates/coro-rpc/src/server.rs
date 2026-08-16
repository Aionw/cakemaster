use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, Sink, Stream};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_service::Service;

use crate::error::RpcErrorCode;
use crate::method::{RpcMethod, RpcNoArgsMethod};
use crate::protocol::{FrameError, FrameLimits, RequestFrame, ResponseFrame, ServerCodec};
use crate::struct_pack::{
    StructPack, deserialize, deserialize_with_type_hash, serialize, serialize_with_type_hash,
};

const DRIVER_POLL_BUDGET: usize = 1024;
const SERVER_CONNECTION_LOG_TARGET: &str = "coro_rpc::server::connection";
const SERVER_REQUEST_LOG_TARGET: &str = "coro_rpc::server::request";

tokio::task_local! {
    static CURRENT_REQUEST_CONTEXT: RequestContext;
}

/// Server-side limits and connection settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub frame_limits: FrameLimits,
    pub max_in_flight_per_connection: usize,
    pub tcp_nodelay: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            frame_limits: FrameLimits::default(),
            max_in_flight_per_connection: 256,
            tcp_nodelay: true,
        }
    }
}

/// Metadata and attachment belonging to one incoming request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub sequence: u32,
    pub function_id: u32,
    pub attachment: Bytes,
    pub peer_addr: SocketAddr,
}

/// Returns the context of the RPC request currently polling this task.
///
/// Generated services that do not accept attachments can use this accessor for
/// diagnostics without changing their public method signatures. The context is
/// available only while a server handler future is being polled.
pub fn current_request_context() -> Option<RequestContext> {
    CURRENT_REQUEST_CONTEXT.try_with(Clone::clone).ok()
}

/// A successful handler value with an optional response attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponse<T> {
    pub value: T,
    pub attachment: Bytes,
}

impl<T> RpcResponse<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            attachment: Bytes::new(),
        }
    }

    pub fn with_attachment(value: T, attachment: impl Into<Bytes>) -> Self {
        Self {
            value,
            attachment: attachment.into(),
        }
    }
}

/// An error intentionally returned by an RPC handler.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("RPC failure {code}: {message}")]
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegisterError {
    #[error(
        "route ID {route_id:#010x} is already registered as {existing_name:?}, cannot register {new_name:?}"
    )]
    DuplicateRoute {
        route_id: u32,
        existing_name: String,
        new_name: String,
    },
}

type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<ResponseFrame, Infallible>> + Send + 'static>>;

trait ErasedHandler: Send + Sync {
    fn call(&self, body: Bytes, context: RequestContext) -> ResponseFuture;
}

struct TypedHandler<A, R, F> {
    function: F,
    request_type_hash: u32,
    response_type_hash: u32,
    marker: PhantomData<fn(A) -> R>,
}

impl<A, R, F, Fut> ErasedHandler for TypedHandler<A, R, F>
where
    A: StructPack + Send + 'static,
    R: StructPack + Send + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Bytes, context: RequestContext) -> ResponseFuture {
        let sequence = context.sequence;
        let arguments = match deserialize_with_type_hash::<A>(&body, self.request_type_hash) {
            Ok(arguments) => arguments,
            Err(error) => {
                return ready_failure(
                    sequence,
                    RpcFailure::new(
                        RpcErrorCode::InvalidRpcArguments as u16,
                        format!("invalid rpc arg: {error}"),
                    ),
                );
            }
        };
        let future = (self.function)(arguments);
        let response_type_hash = self.response_type_hash;
        Box::pin(CURRENT_REQUEST_CONTEXT.scope(context, async move {
            Ok(match future.await {
                Ok(value) => success_frame(sequence, &value, Bytes::new(), response_type_hash),
                Err(error) => failure_frame(sequence, error),
            })
        }))
    }
}

struct ContextHandler<A, R, F> {
    function: F,
    request_type_hash: u32,
    response_type_hash: u32,
    marker: PhantomData<fn(A) -> R>,
}

impl<A, R, F, Fut> ErasedHandler for ContextHandler<A, R, F>
where
    A: StructPack + Send + 'static,
    R: StructPack + Send + 'static,
    F: Fn(A, RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Bytes, context: RequestContext) -> ResponseFuture {
        let sequence = context.sequence;
        let arguments = match deserialize_with_type_hash::<A>(&body, self.request_type_hash) {
            Ok(arguments) => arguments,
            Err(error) => {
                return ready_failure(
                    sequence,
                    RpcFailure::new(
                        RpcErrorCode::InvalidRpcArguments as u16,
                        format!("invalid rpc arg: {error}"),
                    ),
                );
            }
        };
        let future = (self.function)(arguments, context.clone());
        let response_type_hash = self.response_type_hash;
        Box::pin(CURRENT_REQUEST_CONTEXT.scope(context, async move {
            Ok(match future.await {
                Ok(response) => success_frame(
                    sequence,
                    &response.value,
                    response.attachment,
                    response_type_hash,
                ),
                Err(error) => failure_frame(sequence, error),
            })
        }))
    }
}

struct NoArgsHandler<R, F> {
    function: F,
    response_type_hash: u32,
    marker: PhantomData<fn() -> R>,
}

impl<R, F, Fut> ErasedHandler for NoArgsHandler<R, F>
where
    R: StructPack + Send + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Bytes, context: RequestContext) -> ResponseFuture {
        let sequence = context.sequence;
        if !body.is_empty() {
            return ready_failure(
                sequence,
                RpcFailure::standard(RpcErrorCode::InvalidRpcArguments),
            );
        }
        let future = (self.function)();
        let response_type_hash = self.response_type_hash;
        Box::pin(CURRENT_REQUEST_CONTEXT.scope(context, async move {
            Ok(match future.await {
                Ok(value) => success_frame(sequence, &value, Bytes::new(), response_type_hash),
                Err(error) => failure_frame(sequence, error),
            })
        }))
    }
}

struct NoArgsContextHandler<R, F> {
    function: F,
    response_type_hash: u32,
    marker: PhantomData<fn() -> R>,
}

impl<R, F, Fut> ErasedHandler for NoArgsContextHandler<R, F>
where
    R: StructPack + Send + 'static,
    F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
{
    fn call(&self, body: Bytes, context: RequestContext) -> ResponseFuture {
        let sequence = context.sequence;
        if !body.is_empty() {
            return ready_failure(
                sequence,
                RpcFailure::standard(RpcErrorCode::InvalidRpcArguments),
            );
        }
        let future = (self.function)(context.clone());
        let response_type_hash = self.response_type_hash;
        Box::pin(CURRENT_REQUEST_CONTEXT.scope(context, async move {
            Ok(match future.await {
                Ok(response) => success_frame(
                    sequence,
                    &response.value,
                    response.attachment,
                    response_type_hash,
                ),
                Err(error) => failure_frame(sequence, error),
            })
        }))
    }
}

struct Route {
    name: &'static str,
    handler: Box<dyn ErasedHandler>,
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
        method: RpcMethod<A, R>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        A: StructPack + Send + 'static,
        R: StructPack + Send + 'static,
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
    {
        self.insert(
            method.route_id(),
            method.name(),
            Box::new(TypedHandler::<A, R, F> {
                function,
                request_type_hash: method.request_type_hash(),
                response_type_hash: method.response_type_hash(),
                marker: PhantomData,
            }),
        )
    }

    pub fn register_with_context<A, R, F, Fut>(
        &mut self,
        method: RpcMethod<A, R>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        A: StructPack + Send + 'static,
        R: StructPack + Send + 'static,
        F: Fn(A, RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
    {
        self.insert(
            method.route_id(),
            method.name(),
            Box::new(ContextHandler::<A, R, F> {
                function,
                request_type_hash: method.request_type_hash(),
                response_type_hash: method.response_type_hash(),
                marker: PhantomData,
            }),
        )
    }

    pub fn register_no_args<R, F, Fut>(
        &mut self,
        method: RpcNoArgsMethod<R>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        R: StructPack + Send + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, RpcFailure>> + Send + 'static,
    {
        self.insert(
            method.route_id(),
            method.name(),
            Box::new(NoArgsHandler::<R, F> {
                function,
                response_type_hash: method.response_type_hash(),
                marker: PhantomData,
            }),
        )
    }

    pub fn register_no_args_with_context<R, F, Fut>(
        &mut self,
        method: RpcNoArgsMethod<R>,
        function: F,
    ) -> Result<&mut Self, RegisterError>
    where
        R: StructPack + Send + 'static,
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RpcResponse<R>, RpcFailure>> + Send + 'static,
    {
        self.insert(
            method.route_id(),
            method.name(),
            Box::new(NoArgsContextHandler::<R, F> {
                function,
                response_type_hash: method.response_type_hash(),
                marker: PhantomData,
            }),
        )
    }

    fn insert(
        &mut self,
        route_id: u32,
        name: &'static str,
        handler: Box<dyn ErasedHandler>,
    ) -> Result<&mut Self, RegisterError> {
        if let Some(existing) = self.routes.get(&route_id) {
            return Err(RegisterError::DuplicateRoute {
                route_id,
                existing_name: existing.name.to_owned(),
                new_name: name.to_owned(),
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
        let cancellation = CancellationToken::new();
        let connections = TaskTracker::new();
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                _ = &mut shutdown => break Ok(()),
                accepted = self.listener.accept() => {
                    let (stream, peer_addr) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => break Err(error),
                    };
                    if let Err(error) = stream.set_nodelay(self.config.tcp_nodelay) {
                        log::error!(
                            target: SERVER_CONNECTION_LOG_TARGET,
                            peer_addr:% = peer_addr,
                            error:% = error;
                            "failed to configure accepted RPC connection"
                        );
                        break Err(error);
                    }
                    log::debug!(
                        target: SERVER_CONNECTION_LOG_TARGET,
                        peer_addr:% = peer_addr;
                        "accepted RPC connection"
                    );
                    let connection = ServerConnection::new(
                        stream,
                        peer_addr,
                        self.routes.clone(),
                        &self.config,
                    );
                    let connection_cancellation = cancellation.child_token();
                    connections.spawn(async move {
                        tokio::select! {
                            _ = connection_cancellation.cancelled() => {
                                log::debug!(
                                    target: SERVER_CONNECTION_LOG_TARGET,
                                    peer_addr:% = peer_addr;
                                    "RPC connection cancelled during server shutdown"
                                );
                            }
                            result = AssertUnwindSafe(connection).catch_unwind() => match result {
                                Ok(Ok(())) => log::debug!(
                                    target: SERVER_CONNECTION_LOG_TARGET,
                                    peer_addr:% = peer_addr;
                                    "RPC connection closed"
                                ),
                                Ok(Err(error)) => log::warn!(
                                    target: SERVER_CONNECTION_LOG_TARGET,
                                    peer_addr:% = peer_addr,
                                    error:% = error;
                                    "RPC connection failed"
                                ),
                                Err(panic) => log::error!(
                                    target: SERVER_CONNECTION_LOG_TARGET,
                                    peer_addr:% = peer_addr,
                                    panic_message = panic_message(&panic);
                                    "RPC connection task panicked"
                                ),
                            }
                        }
                    });
                }
            }
        };

        cancellation.cancel();
        connections.close();
        connections.wait().await;
        result
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[derive(Clone)]
struct RpcService {
    peer_addr: SocketAddr,
    routes: Arc<HashMap<u32, Route>>,
    request_logging: RequestLogging,
}

#[derive(Clone, Copy)]
struct RequestLogging {
    trace: bool,
    debug: bool,
    warn: bool,
}

impl RequestLogging {
    fn capture() -> Self {
        Self {
            trace: log::log_enabled!(target: SERVER_REQUEST_LOG_TARGET, log::Level::Trace),
            debug: log::log_enabled!(target: SERVER_REQUEST_LOG_TARGET, log::Level::Debug),
            warn: log::log_enabled!(target: SERVER_REQUEST_LOG_TARGET, log::Level::Warn),
        }
    }
}

impl Service<RequestFrame> for RpcService {
    type Response = ResponseFrame;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestFrame) -> Self::Future {
        let sequence = request.header.sequence;
        let function_id = request.header.function_id;
        let Some(route) = self.routes.get(&function_id) else {
            log::warn!(
                target: SERVER_REQUEST_LOG_TARGET,
                peer_addr:% = self.peer_addr,
                sequence = sequence,
                function_id = function_id;
                "RPC function is not registered"
            );
            return ready_failure(
                sequence,
                RpcFailure::new(
                    RpcErrorCode::FunctionNotRegistered as u16,
                    "function not registered",
                ),
            );
        };
        let method = route.name;
        if self.request_logging.trace {
            log::trace!(
                target: SERVER_REQUEST_LOG_TARGET,
                peer_addr:% = self.peer_addr,
                sequence = sequence,
                function_id = function_id,
                method = method,
                body_bytes = request.body.len(),
                attachment_bytes = request.attachment.len();
                "RPC request received"
            );
        }
        let context = RequestContext {
            sequence,
            function_id,
            attachment: request.attachment,
            peer_addr: self.peer_addr,
        };
        let response = route.handler.call(request.body, context);
        let peer_addr = self.peer_addr;
        let request_logging = self.request_logging;
        // Successful request timing is a DEBUG diagnostic. Keeping the clock
        // reads out of the default INFO/WARN hot path matters for tiny,
        // heavily-pipelined RPCs; failed requests still retain every routing
        // and error field at WARN.
        let started_at = request_logging.debug.then(Instant::now);
        Box::pin(async move {
            let response = match response.await {
                Ok(response) => response,
                Err(never) => match never {},
            };
            let error_code = response.header.error_code;
            if error_code == 0 && request_logging.debug {
                let elapsed_micros = started_at
                    .expect("debug request logging captures a start time")
                    .elapsed()
                    .as_micros();
                log::debug!(
                    target: SERVER_REQUEST_LOG_TARGET,
                    peer_addr:% = peer_addr,
                    sequence = sequence,
                    function_id = function_id,
                    method = method,
                    elapsed_micros = elapsed_micros;
                    "RPC request completed"
                );
            } else if error_code != 0 && request_logging.warn {
                let (rpc_error_code, rpc_error_message) = response_failure_details(&response);
                if let Some(started_at) = started_at {
                    log::warn!(
                        target: SERVER_REQUEST_LOG_TARGET,
                        peer_addr:% = peer_addr,
                        sequence = sequence,
                        function_id = function_id,
                        method = method,
                        rpc_error_code = rpc_error_code,
                        rpc_error_message:? = rpc_error_message,
                        elapsed_micros = started_at.elapsed().as_micros();
                        "RPC request failed"
                    );
                } else {
                    log::warn!(
                        target: SERVER_REQUEST_LOG_TARGET,
                        peer_addr:% = peer_addr,
                        sequence = sequence,
                        function_id = function_id,
                        method = method,
                        rpc_error_code = rpc_error_code,
                        rpc_error_message:? = rpc_error_message;
                        "RPC request failed"
                    );
                }
            }
            Ok(response)
        })
    }
}

struct ServerConnection {
    transport: Framed<TcpStream, ServerCodec>,
    service: RpcService,
    in_flight: FuturesUnordered<ResponseFuture>,
    pending_responses: VecDeque<ResponseFrame>,
    buffered_responses: usize,
    max_in_flight: usize,
    read_closed: bool,
}

impl ServerConnection {
    fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        routes: Arc<HashMap<u32, Route>>,
        config: &ServerConfig,
    ) -> Self {
        Self {
            transport: Framed::new(stream, ServerCodec::new(config.frame_limits)),
            service: RpcService {
                peer_addr,
                routes,
                request_logging: RequestLogging::capture(),
            },
            in_flight: FuturesUnordered::new(),
            pending_responses: VecDeque::new(),
            buffered_responses: 0,
            max_in_flight: config.max_in_flight_per_connection.max(1),
            read_closed: false,
        }
    }

    fn active_requests(&self) -> usize {
        self.in_flight.len() + self.pending_responses.len() + self.buffered_responses
    }
}

impl Future for ServerConnection {
    type Output = Result<(), FrameError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut budget = DRIVER_POLL_BUDGET;

        loop {
            let mut progressed = false;

            while !this.read_closed && this.active_requests() < this.max_in_flight && budget > 0 {
                match this.service.poll_ready(context) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(never)) => match never {},
                    Poll::Pending => break,
                }
                match Pin::new(&mut this.transport).poll_next(context) {
                    Poll::Ready(Some(Ok(request))) => {
                        this.in_flight.push(this.service.call(request));
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                    Poll::Ready(None) => {
                        this.read_closed = true;
                        progressed = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            while !this.in_flight.is_empty() && budget > 0 {
                match Pin::new(&mut this.in_flight).poll_next(context) {
                    Poll::Ready(Some(Ok(response))) => {
                        this.pending_responses.push_back(response);
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(never))) => match never {},
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }

            while !this.pending_responses.is_empty() && budget > 0 {
                match Pin::new(&mut this.transport).poll_ready(context) {
                    Poll::Ready(Ok(())) => {
                        let response = this
                            .pending_responses
                            .pop_front()
                            .expect("response queue was checked as non-empty");
                        Pin::new(&mut this.transport).start_send(response)?;
                        this.buffered_responses += 1;
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => break,
                }
            }

            if this.buffered_responses != 0 {
                match Pin::new(&mut this.transport).poll_flush(context) {
                    Poll::Ready(Ok(())) => {
                        this.buffered_responses = 0;
                        progressed = true;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => {}
                }
            }

            if this.read_closed && this.active_requests() == 0 {
                return Poll::Ready(Ok(()));
            }
            if budget == 0 {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

fn ready_failure(sequence: u32, failure: RpcFailure) -> ResponseFuture {
    Box::pin(std::future::ready(Ok(failure_frame(sequence, failure))))
}

fn response_failure_details(response: &ResponseFrame) -> (u16, Option<String>) {
    if response.header.error_code == 255 {
        return deserialize::<(u16, String)>(&response.body)
            .map(|(code, message)| (code, Some(message)))
            .unwrap_or((u16::from(response.header.error_code), None));
    }
    let message = deserialize::<String>(&response.body).ok();
    (u16::from(response.header.error_code), message)
}

fn success_frame<R: StructPack>(
    sequence: u32,
    value: &R,
    attachment: Bytes,
    response_type_hash: u32,
) -> ResponseFrame {
    match serialize_with_type_hash(value, response_type_hash) {
        Ok(body) => ResponseFrame::new(sequence, 0, body, attachment).unwrap_or_else(|_| {
            failure_frame(
                sequence,
                RpcFailure::standard(RpcErrorCode::MessageTooLarge),
            )
        }),
        Err(error) => failure_frame(
            sequence,
            RpcFailure::new(
                RpcErrorCode::InvalidRpcResult as u16,
                format!("failed to serialize RPC result: {error}"),
            ),
        ),
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
    ResponseFrame::new(sequence, wire_code, body, Bytes::new())
        .expect("a small RPC error frame always fits in u32 lengths")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_context_is_scoped_to_the_handler_future() {
        assert!(current_request_context().is_none());
        let expected = RequestContext {
            sequence: 17,
            function_id: 23,
            attachment: Bytes::from_static(b"diagnostic"),
            peer_addr: "127.0.0.1:4123".parse().unwrap(),
        };
        CURRENT_REQUEST_CONTEXT
            .scope(expected.clone(), async {
                assert_eq!(
                    current_request_context().unwrap().sequence,
                    expected.sequence
                );
                tokio::task::yield_now().await;
                assert_eq!(
                    current_request_context().unwrap().peer_addr,
                    expected.peer_addr
                );
            })
            .await;
        assert!(current_request_context().is_none());
    }

    #[test]
    fn failure_details_decode_standard_and_extended_codes() {
        let standard = failure_frame(
            1,
            RpcFailure::new(RpcErrorCode::InvalidRpcArguments as u16, "bad request"),
        );
        assert_eq!(
            response_failure_details(&standard),
            (
                RpcErrorCode::InvalidRpcArguments as u16,
                Some("bad request".to_owned())
            )
        );

        let extended = failure_frame(2, RpcFailure::new(1001, "application failure"));
        assert_eq!(
            response_failure_details(&extended),
            (1001, Some("application failure".to_owned()))
        );
    }
}
