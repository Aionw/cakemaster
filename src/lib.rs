//! Core cakemaster domain components.

pub mod client;
pub mod object;
mod proto;
pub mod segment;
pub mod server;

pub use proto::{MOONCAKE_STORE_VERSION, api, mooncake};
