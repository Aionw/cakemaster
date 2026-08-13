use std::marker::PhantomData;

use crate::function_id;
use crate::struct_pack::{StructPack, type_hash};

/// Static wire metadata for one typed coro_rpc method.
///
/// Construct a method once and reuse it for server registration and client
/// calls. Route and schema hashes are consequently outside the request path.
#[derive(Debug)]
pub struct RpcMethod<Request, Response> {
    name: &'static str,
    route_id: u32,
    request_type_hash: u32,
    response_type_hash: u32,
    marker: PhantomData<fn(Request) -> Response>,
}

impl<Request, Response> RpcMethod<Request, Response>
where
    Request: StructPack,
    Response: StructPack,
{
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            route_id: function_id(name),
            request_type_hash: type_hash::<Request>(),
            response_type_hash: type_hash::<Response>(),
            marker: PhantomData,
        }
    }
}

impl<Request, Response> RpcMethod<Request, Response> {
    /// Constructor used by generated stubs whose wire metadata was calculated
    /// from the IDL at build time.
    #[doc(hidden)]
    pub const fn from_generated_parts(
        name: &'static str,
        route_id: u32,
        request_type_hash: u32,
        response_type_hash: u32,
    ) -> Self {
        Self {
            name,
            route_id,
            request_type_hash,
            response_type_hash,
            marker: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn route_id(&self) -> u32 {
        self.route_id
    }

    pub(crate) const fn request_type_hash(&self) -> u32 {
        self.request_type_hash
    }

    pub(crate) const fn response_type_hash(&self) -> u32 {
        self.response_type_hash
    }
}

impl<Request, Response> Copy for RpcMethod<Request, Response> {}

impl<Request, Response> Clone for RpcMethod<Request, Response> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Static wire metadata for a coro_rpc method whose request body is empty.
#[derive(Debug)]
pub struct RpcNoArgsMethod<Response> {
    name: &'static str,
    route_id: u32,
    response_type_hash: u32,
    marker: PhantomData<fn() -> Response>,
}

impl<Response> RpcNoArgsMethod<Response>
where
    Response: StructPack,
{
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            route_id: function_id(name),
            response_type_hash: type_hash::<Response>(),
            marker: PhantomData,
        }
    }
}

impl<Response> RpcNoArgsMethod<Response> {
    /// Constructor used by generated stubs whose wire metadata was calculated
    /// from the IDL at build time.
    #[doc(hidden)]
    pub const fn from_generated_parts(
        name: &'static str,
        route_id: u32,
        response_type_hash: u32,
    ) -> Self {
        Self {
            name,
            route_id,
            response_type_hash,
            marker: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn route_id(&self) -> u32 {
        self.route_id
    }

    pub(crate) const fn response_type_hash(&self) -> u32 {
        self.response_type_hash
    }
}

impl<Response> Copy for RpcNoArgsMethod<Response> {}

impl<Response> Clone for RpcNoArgsMethod<Response> {
    fn clone(&self) -> Self {
        *self
    }
}
