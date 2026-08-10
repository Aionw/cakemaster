//! RPC contracts generated from Cakemaster's Thrift IDL files.
//!
//! The generated namespaces are exposed once from this crate so applications,
//! tests, and tools all compile against the same Rust types.

include!(concat!(env!("OUT_DIR"), "/cakemaster_rpc.rs"));
include!(concat!(env!("OUT_DIR"), "/mooncake_master_rpc.rs"));
