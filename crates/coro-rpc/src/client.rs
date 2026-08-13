use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Sink, Stream};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::error::{RemoteError, RpcError};
use crate::method::{RpcMethod, RpcNoArgsMethod};
use crate::protocol::{ClientCodec, FrameError, FrameLimits, RequestFrame, ResponseFrame};
use crate::struct_pack::{
    StructPack, deserialize, deserialize_with_type_hash, serialize_with_type_hash,
};

const DRIVER_POLL_BUDGET: usize = 1024;

/// Configuration for a multiplexed coro_rpc connection.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Option<Duration>,
    pub frame_limits: FrameLimits,
    pub tcp_nodelay: bool,
    pub max_in_flight_requests: usize,
    pub pending_request_buffer: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Some(Duration::from_secs(5)),
            frame_limits: FrameLimits::default(),
            tcp_nodelay: true,
            max_in_flight_requests: 1024,
            pending_request_buffer: 1024,
        }
    }
}

/// A decoded RPC value and the optional out-of-band coro_rpc attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcReply<T> {
    pub value: T,
    pub attachment: Bytes,
}

type PendingSender = oneshot::Sender<Result<ResponseFrame, RpcError>>;

struct DispatchRequest {
    frame: RequestFrame,
    completion: PendingSender,
}

struct ClientInner {
    requests: mpsc::Sender<DispatchRequest>,
    cancellations: mpsc::UnboundedSender<u32>,
    closed: Arc<AtomicBool>,
    next_sequence: AtomicU32,
    config: ClientConfig,
}

/// A cloneable, pipelined Tokio coro_rpc client.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<ClientInner>,
}

impl RpcClient {
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self, RpcError> {
        Self::connect_with_config(address, ClientConfig::default()).await
    }

    pub async fn connect_with_config(
        address: impl ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, RpcError> {
        let stream = TcpStream::connect(address).await.map_err(RpcError::io)?;
        stream
            .set_nodelay(config.tcp_nodelay)
            .map_err(RpcError::io)?;

        let (request_tx, request_rx) = mpsc::channel(config.pending_request_buffer.max(1));
        let (cancellation_tx, cancellation_rx) = mpsc::unbounded_channel();
        let closed = Arc::new(AtomicBool::new(false));
        let connection =
            ClientConnection::new(stream, request_rx, cancellation_rx, closed.clone(), &config);
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            inner: Arc::new(ClientInner {
                requests: request_tx,
                cancellations: cancellation_tx,
                closed,
                next_sequence: AtomicU32::new(0),
                config,
            }),
        })
    }

    pub async fn call<A, R>(&self, method: RpcMethod<A, R>, arguments: &A) -> Result<R, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        Ok(self
            .call_with_attachment(method, arguments, Bytes::new())
            .await?
            .value)
    }

    pub async fn call_with_attachment<A, R>(
        &self,
        method: RpcMethod<A, R>,
        arguments: &A,
        attachment: impl Into<Bytes>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        let body = Bytes::from(serialize_with_type_hash(
            arguments,
            method.request_type_hash(),
        )?);
        let response = self
            .send(method.route_id(), body, attachment.into())
            .await?;
        decode_response(response, method.response_type_hash())
    }

    pub async fn call_no_args<R>(&self, method: RpcNoArgsMethod<R>) -> Result<R, RpcError>
    where
        R: StructPack,
    {
        Ok(self
            .call_no_args_with_attachment(method, Bytes::new())
            .await?
            .value)
    }

    pub async fn call_no_args_with_attachment<R>(
        &self,
        method: RpcNoArgsMethod<R>,
        attachment: impl Into<Bytes>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        R: StructPack,
    {
        let response = self
            .send(method.route_id(), Bytes::new(), attachment.into())
            .await?;
        decode_response(response, method.response_type_hash())
    }

    async fn send(
        &self,
        route_id: u32,
        body: Bytes,
        attachment: Bytes,
    ) -> Result<ResponseFrame, RpcError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RpcError::ConnectionClosed);
        }
        if body.len() > self.inner.config.frame_limits.max_body_len
            || attachment.len() > self.inner.config.frame_limits.max_attachment_len
        {
            return Err(RpcError::RequestTooLarge);
        }

        let sequence = self.inner.next_sequence.fetch_add(1, Ordering::Relaxed);
        let frame = RequestFrame::new(sequence, route_id, body, attachment)
            .map_err(|_| RpcError::RequestTooLarge)?;
        let (completion, response) = oneshot::channel();
        let mut response = ResponseGuard {
            sequence,
            response,
            cancellations: self.inner.cancellations.clone(),
            cancel_on_drop: true,
        };

        let dispatch = async {
            self.inner
                .requests
                .send(DispatchRequest { frame, completion })
                .await
                .map_err(|_| RpcError::ConnectionClosed)?;
            response.receive().await
        };
        if let Some(timeout) = self.inner.config.request_timeout {
            match tokio::time::timeout(timeout, dispatch).await {
                Ok(result) => result,
                Err(_) => Err(RpcError::TimedOut),
            }
        } else {
            dispatch.await
        }
    }
}

struct ResponseGuard {
    sequence: u32,
    response: oneshot::Receiver<Result<ResponseFrame, RpcError>>,
    cancellations: mpsc::UnboundedSender<u32>,
    cancel_on_drop: bool,
}

impl ResponseGuard {
    async fn receive(&mut self) -> Result<ResponseFrame, RpcError> {
        let response = (&mut self.response)
            .await
            .unwrap_or(Err(RpcError::ConnectionClosed));
        self.cancel_on_drop = false;
        response
    }
}

impl Drop for ResponseGuard {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.response.close();
            let _ = self.cancellations.send(self.sequence);
        }
    }
}

fn decode_response<R: StructPack>(
    frame: ResponseFrame,
    response_type_hash: u32,
) -> Result<RpcReply<R>, RpcError> {
    match frame.header.error_code {
        0 => Ok(RpcReply {
            value: deserialize_with_type_hash::<R>(&frame.body, response_type_hash)?,
            attachment: frame.attachment,
        }),
        255 => {
            let (code, message) = deserialize::<(u16, String)>(&frame.body)?;
            Err(RpcError::Remote(RemoteError { code, message }))
        }
        code => {
            let message = deserialize::<String>(&frame.body)?;
            Err(RpcError::Remote(RemoteError {
                code: u16::from(code),
                message,
            }))
        }
    }
}

struct ClientConnection {
    transport: Framed<TcpStream, ClientCodec>,
    requests: mpsc::Receiver<DispatchRequest>,
    cancellations: mpsc::UnboundedReceiver<u32>,
    in_flight: HashMap<u32, PendingSender>,
    pending_request: Option<DispatchRequest>,
    max_in_flight: usize,
    needs_flush: bool,
    requests_closed: bool,
    closed: Arc<AtomicBool>,
}

impl ClientConnection {
    fn new(
        stream: TcpStream,
        requests: mpsc::Receiver<DispatchRequest>,
        cancellations: mpsc::UnboundedReceiver<u32>,
        closed: Arc<AtomicBool>,
        config: &ClientConfig,
    ) -> Self {
        Self {
            transport: Framed::new(stream, ClientCodec::new(config.frame_limits)),
            requests,
            cancellations,
            in_flight: HashMap::with_capacity(config.max_in_flight_requests),
            pending_request: None,
            max_in_flight: config.max_in_flight_requests.max(1),
            needs_flush: false,
            requests_closed: false,
            closed,
        }
    }

    fn finish(&mut self, error: RpcError) -> Poll<Result<(), RpcError>> {
        self.closed.store(true, Ordering::Release);
        for (_, completion) in self.in_flight.drain() {
            let _ = completion.send(Err(error.clone()));
        }
        if let Some(request) = self.pending_request.take() {
            let _ = request.completion.send(Err(error.clone()));
        }
        while let Ok(request) = self.requests.try_recv() {
            let _ = request.completion.send(Err(error.clone()));
        }
        Poll::Ready(Err(error))
    }
}

impl Future for ClientConnection {
    type Output = Result<(), RpcError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut budget = DRIVER_POLL_BUDGET;

        loop {
            let mut progressed = false;

            while budget > 0 {
                match this.cancellations.poll_recv(context) {
                    Poll::Ready(Some(sequence)) => {
                        this.in_flight.remove(&sequence);
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }

            while this.in_flight.len() < this.max_in_flight && budget > 0 {
                if this.pending_request.is_none() && !this.requests_closed {
                    match this.requests.poll_recv(context) {
                        Poll::Ready(Some(request)) => {
                            this.pending_request = Some(request);
                            progressed = true;
                        }
                        Poll::Ready(None) => {
                            this.requests_closed = true;
                            progressed = true;
                        }
                        Poll::Pending => {}
                    }
                }

                let Some(request) = this.pending_request.as_ref() else {
                    break;
                };
                let sequence = request.frame.header.sequence;
                if request.completion.is_closed() {
                    this.pending_request.take();
                    budget -= 1;
                    progressed = true;
                    continue;
                }
                if this.in_flight.contains_key(&sequence) {
                    let request = this
                        .pending_request
                        .take()
                        .expect("pending request was checked as present");
                    let _ = request.completion.send(Err(RpcError::SerialNumberConflict));
                    budget -= 1;
                    progressed = true;
                    continue;
                }

                match Pin::new(&mut this.transport).poll_ready(context) {
                    Poll::Ready(Ok(())) => {
                        let request = this
                            .pending_request
                            .take()
                            .expect("pending request was checked as present");
                        this.in_flight.insert(sequence, request.completion);
                        if let Err(error) = Pin::new(&mut this.transport).start_send(request.frame)
                        {
                            return this.finish(rpc_error_from_frame(error));
                        }
                        this.needs_flush = true;
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(Err(error)) => {
                        return this.finish(rpc_error_from_frame(error));
                    }
                    Poll::Pending => break,
                }
            }

            if this.needs_flush {
                match Pin::new(&mut this.transport).poll_flush(context) {
                    Poll::Ready(Ok(())) => {
                        this.needs_flush = false;
                        progressed = true;
                    }
                    Poll::Ready(Err(error)) => {
                        return this.finish(rpc_error_from_frame(error));
                    }
                    Poll::Pending => {}
                }
            }

            while budget > 0 {
                match Pin::new(&mut this.transport).poll_next(context) {
                    Poll::Ready(Some(Ok(frame))) => {
                        if let Some(completion) = this.in_flight.remove(&frame.header.sequence) {
                            let _ = completion.send(Ok(frame));
                        }
                        budget -= 1;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(error))) => {
                        return this.finish(rpc_error_from_frame(error));
                    }
                    Poll::Ready(None) => return this.finish(RpcError::ConnectionClosed),
                    Poll::Pending => break,
                }
            }

            if this.requests_closed
                && this.pending_request.is_none()
                && this.in_flight.is_empty()
                && !this.needs_flush
            {
                this.closed.store(true, Ordering::Release);
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

impl Drop for ClientConnection {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}

fn rpc_error_from_frame(error: FrameError) -> RpcError {
    match error {
        FrameError::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            RpcError::ConnectionClosed
        }
        FrameError::Io(error) => RpcError::io(error),
        FrameError::Protocol(error) => RpcError::Protocol(error.to_string()),
    }
}
