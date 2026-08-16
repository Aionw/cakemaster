//! A Tokio implementation of the yalantinglibs `coro_rpc` v0 wire protocol.
//!
//! The crate deliberately keeps the transport framing and the `struct_pack`
//! codec public. This makes it possible to use the interoperable pieces without
//! adopting the supplied client or server abstraction.

mod client;
mod error;
mod hash;
mod method;
pub mod protocol;
mod server;
pub mod struct_pack;

pub use bytes::Bytes;
pub use client::{ClientConfig, RpcClient, RpcReply};
pub use error::{RemoteError, RpcError, RpcErrorCode};
pub use hash::function_id;
pub use method::{RpcMethod, RpcNoArgsMethod};
pub use server::{
    BoundRpcServer, RegisterError, RequestContext, RpcFailure, RpcResponse, RpcServer,
    ServerConfig, current_request_context,
};
pub use struct_pack::{ByteString, StructPack, StructPackError};
pub use tokio::net::ToSocketAddrs;
