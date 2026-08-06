use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, oneshot};
use tokio::task::AbortHandle;

use crate::error::{RemoteError, RpcError};
use crate::function_id;
use crate::protocol::{
    FrameError, FrameLimits, RequestFrame, ResponseFrame, read_response, write_request,
};
use crate::struct_pack::{StructPack, deserialize, serialize};

/// Configuration for a multiplexed coro_rpc connection.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Option<Duration>,
    pub frame_limits: FrameLimits,
    pub tcp_nodelay: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Some(Duration::from_secs(5)),
            frame_limits: FrameLimits::default(),
            tcp_nodelay: true,
        }
    }
}

/// A decoded RPC value and the optional out-of-band coro_rpc attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcReply<T> {
    pub value: T,
    pub attachment: Vec<u8>,
}

type PendingSender = oneshot::Sender<Result<ResponseFrame, RpcError>>;
type PendingMap = Arc<StdMutex<HashMap<u32, PendingSender>>>;

struct ClientInner {
    writer: Mutex<OwnedWriteHalf>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    next_sequence: AtomicU32,
    config: ClientConfig,
    reader_abort: StdMutex<Option<AbortHandle>>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(abort) = self
            .reader_abort
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            abort.abort();
        }
        fail_all_pending(&self.pending, RpcError::ConnectionClosed);
    }
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
        let (reader, writer) = stream.into_split();
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(ClientInner {
            writer: Mutex::new(writer),
            pending: pending.clone(),
            closed: closed.clone(),
            next_sequence: AtomicU32::new(0),
            config: config.clone(),
            reader_abort: StdMutex::new(None),
        });

        let task = tokio::spawn(reader_loop(reader, pending, closed, config.frame_limits));
        *inner
            .reader_abort
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task.abort_handle());
        Ok(Self { inner })
    }

    /// Calls a function using the MD5 route ID of its exact C++ qualified name.
    pub async fn call<A, R>(&self, function_name: &str, arguments: &A) -> Result<R, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        Ok(self
            .call_with_attachment(function_name, arguments, Vec::new())
            .await?
            .value)
    }

    /// Calls a function by an already calculated route ID.
    pub async fn call_id<A, R>(&self, route_id: u32, arguments: &A) -> Result<R, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        Ok(self
            .call_id_with_attachment(route_id, arguments, Vec::new())
            .await?
            .value)
    }

    /// Calls a C++ function that has no parameters.
    ///
    /// coro_rpc represents a no-argument request as an empty body, which is
    /// distinct from a single `std::monostate` argument.
    pub async fn call_no_args<R>(&self, function_name: &str) -> Result<R, RpcError>
    where
        R: StructPack,
    {
        Ok(self
            .call_no_args_id_with_attachment(function_id(function_name), Vec::new())
            .await?
            .value)
    }

    pub async fn call_no_args_id<R>(&self, route_id: u32) -> Result<R, RpcError>
    where
        R: StructPack,
    {
        Ok(self
            .call_no_args_id_with_attachment(route_id, Vec::new())
            .await?
            .value)
    }

    pub async fn call_with_attachment<A, R>(
        &self,
        function_name: &str,
        arguments: &A,
        attachment: Vec<u8>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        self.call_id_with_attachment(function_id(function_name), arguments, attachment)
            .await
    }

    pub async fn call_id_with_attachment<A, R>(
        &self,
        route_id: u32,
        arguments: &A,
        attachment: Vec<u8>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        A: StructPack,
        R: StructPack,
    {
        let body = serialize(arguments)?;
        let response = self.send(route_id, body, attachment).await?;
        decode_response(response)
    }

    pub async fn call_no_args_with_attachment<R>(
        &self,
        function_name: &str,
        attachment: Vec<u8>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        R: StructPack,
    {
        self.call_no_args_id_with_attachment(function_id(function_name), attachment)
            .await
    }

    pub async fn call_no_args_id_with_attachment<R>(
        &self,
        route_id: u32,
        attachment: Vec<u8>,
    ) -> Result<RpcReply<R>, RpcError>
    where
        R: StructPack,
    {
        let response = self.send(route_id, Vec::new(), attachment).await?;
        decode_response(response)
    }

    async fn send(
        &self,
        route_id: u32,
        body: Vec<u8>,
        attachment: Vec<u8>,
    ) -> Result<ResponseFrame, RpcError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RpcError::ConnectionClosed);
        }
        if body.len() > self.inner.config.frame_limits.max_body_len
            || attachment.len() > self.inner.config.frame_limits.max_attachment_len
        {
            return Err(RpcError::RequestTooLarge);
        }

        let (sender, receiver) = oneshot::channel();
        let sequence = self.reserve_sequence(sender)?;
        let frame = match RequestFrame::new(sequence, route_id, body, attachment) {
            Ok(frame) => frame,
            Err(_) => {
                remove_pending(&self.inner.pending, sequence);
                return Err(RpcError::RequestTooLarge);
            }
        };

        let write_result = {
            let mut writer = self.inner.writer.lock().await;
            write_request(&mut *writer, &frame).await
        };
        if let Err(error) = write_result {
            self.inner.closed.store(true, Ordering::Release);
            fail_all_pending(&self.inner.pending, RpcError::io(&error));
            return Err(RpcError::io(error));
        }

        if let Some(timeout) = self.inner.config.request_timeout {
            match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(RpcError::ConnectionClosed),
                Err(_) => {
                    remove_pending(&self.inner.pending, sequence);
                    Err(RpcError::TimedOut)
                }
            }
        } else {
            receiver.await.unwrap_or(Err(RpcError::ConnectionClosed))
        }
    }

    fn reserve_sequence(&self, sender: PendingSender) -> Result<u32, RpcError> {
        // A collision is possible only after wrapping 2^32 requests while an
        // old request with the same number is still in flight.
        let sequence = self.inner.next_sequence.fetch_add(1, Ordering::Relaxed);
        let mut pending = self
            .inner
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RpcError::ConnectionClosed);
        }
        if pending.contains_key(&sequence) {
            return Err(RpcError::SerialNumberConflict);
        }
        pending.insert(sequence, sender);
        Ok(sequence)
    }
}

fn decode_response<R: StructPack>(frame: ResponseFrame) -> Result<RpcReply<R>, RpcError> {
    match frame.header.error_code {
        0 => Ok(RpcReply {
            value: deserialize::<R>(&frame.body)?,
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

async fn reader_loop(
    mut reader: OwnedReadHalf,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    limits: FrameLimits,
) {
    loop {
        match read_response(&mut reader, limits).await {
            Ok(frame) => {
                let sender = pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&frame.header.sequence);
                // A timed-out call may legitimately leave a late response.
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(frame));
                }
            }
            Err(error) => {
                closed.store(true, Ordering::Release);
                let error = match error {
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
                };
                fail_all_pending(&pending, error);
                return;
            }
        }
    }
}

fn remove_pending(pending: &PendingMap, sequence: u32) {
    pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&sequence);
}

fn fail_all_pending(pending: &PendingMap, error: RpcError) {
    let senders = {
        let mut pending = pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err(error.clone()));
    }
}
