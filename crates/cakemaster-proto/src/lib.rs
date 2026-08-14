//! RPC contracts generated from Cakemaster's Thrift IDL files.
//!
//! The generated namespaces are exposed once from this crate so applications,
//! tests, and tools all compile against the same Rust types.

/// Mooncake Store RPC handshake version at the fixed upstream wire baseline.
///
/// This is the exact `MOONCAKE_STORE_VERSION` used by Mooncake commit
/// `5c0724d22e7f04513a3453c8b6642a5a21b80b47`.
pub const MOONCAKE_STORE_VERSION: &str = "2.0.0";

include!(concat!(env!("OUT_DIR"), "/cakemaster_rpc.rs"));
include!(concat!(env!("OUT_DIR"), "/mooncake_master_rpc.rs"));
