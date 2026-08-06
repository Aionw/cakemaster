//! A Tokio implementation of the yalantinglibs `coro_rpc` v0 wire protocol.
//!
//! The crate deliberately keeps the transport framing and the `struct_pack`
//! codec public. This makes it possible to use the interoperable pieces without
//! adopting the supplied client or server abstraction.

mod client;
mod error;
mod hash;
pub mod protocol;
mod server;
pub mod struct_pack;

pub use client::{ClientConfig, RpcClient, RpcReply};
pub use error::{RemoteError, RpcError, RpcErrorCode};
pub use hash::function_id;
pub use server::{
    BoundRpcServer, RegisterError, RequestContext, RpcFailure, RpcResponse, RpcServer, ServerConfig,
};
pub use struct_pack::{ByteString, StructPack, StructPackError};
