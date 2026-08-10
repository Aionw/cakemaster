//! Per-client Tokio task channels for the Master server.
//!
//! Create one channel pair per client. Master-side producers keep the
//! [`ClientTaskTx`], while the RPC service keeps the [`ClientTaskRx`] and drains
//! it when that client fetches work. Completion reports travel through a
//! separate RPC path and are intentionally outside this queue abstraction.

use thiserror::Error;
use tokio::sync::mpsc;

/// Factory and owner for one client's task channel pair.
#[derive(Debug)]
pub struct ClientTaskQueue<T> {
    tx: ClientTaskTx<T>,
    rx: ClientTaskRx<T>,
}

impl<T> ClientTaskQueue<T> {
    /// Creates a bounded per-client task channel.
    pub fn new(capacity: usize) -> Result<Self, ClientTaskQueueError> {
        if capacity == 0 {
            return Err(ClientTaskQueueError::ZeroCapacity);
        }
        let (tx, rx) = mpsc::channel(capacity);
        Ok(Self {
            tx: ClientTaskTx { inner: tx },
            rx: ClientTaskRx { inner: rx },
        })
    }

    /// Splits the queue into the Master producer and RPC consumer endpoints.
    pub fn into_parts(self) -> (ClientTaskTx<T>, ClientTaskRx<T>) {
        (self.tx, self.rx)
    }
}

/// Master-side producer for one client's outbound tasks.
#[derive(Debug)]
pub struct ClientTaskTx<T> {
    inner: mpsc::Sender<T>,
}

impl<T> Clone for ClientTaskTx<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> ClientTaskTx<T> {
    /// Sends a task, asynchronously waiting for per-client queue capacity.
    pub async fn send(&self, task: T) -> Result<(), mpsc::error::SendError<T>> {
        self.inner.send(task).await
    }

    /// Returns the number of tasks that can be sent without waiting.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Waits until the RPC consumer endpoint is closed.
    pub async fn closed(&self) {
        self.inner.closed().await;
    }
}

/// Server-side consumer drained by one client's fetch RPC handler.
#[derive(Debug)]
pub struct ClientTaskRx<T> {
    inner: mpsc::Receiver<T>,
}

impl<T> ClientTaskRx<T> {
    /// Receives the next task, waiting until a producer sends or closes.
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await
    }

    /// Receives at least one and up to `limit` currently available tasks.
    ///
    /// The call is cancellation-safe and returns an empty vector only after all
    /// producers have closed and the buffered tasks have been drained.
    pub async fn recv_many(&mut self, limit: usize) -> Result<Vec<T>, ClientTaskQueueError> {
        if limit == 0 {
            return Err(ClientTaskQueueError::ZeroBatchSize);
        }
        let mut tasks = Vec::with_capacity(limit);
        self.inner.recv_many(&mut tasks, limit).await;
        Ok(tasks)
    }

    /// Prevents new sends while leaving buffered tasks available to drain.
    pub fn close(&mut self) {
        self.inner.close();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClientTaskQueueError {
    #[error("client task queue capacity must be greater than zero")]
    ZeroCapacity,
    #[error("receive batch size must be greater than zero")]
    ZeroBatchSize,
}
