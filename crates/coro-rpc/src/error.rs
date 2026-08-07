use std::fmt;

use thiserror::Error;

use crate::struct_pack::StructPackError;

/// Standard error codes declared by yalantinglibs `coro_rpc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RpcErrorCode {
    Ok = 0,
    IoError = 1,
    NotConnected = 2,
    TimedOut = 3,
    InvalidRpcArguments = 4,
    AddressInUse = 5,
    BadAddress = 6,
    OpenError = 7,
    ListenError = 8,
    OperationCanceled = 9,
    RpcThrowException = 10,
    FunctionNotRegistered = 11,
    ProtocolError = 12,
    UnknownProtocolVersion = 13,
    MessageTooLarge = 14,
    ServerHasRun = 15,
    InvalidRpcResult = 16,
    SerialNumberConflict = 17,
}

impl RpcErrorCode {
    pub fn from_u16(value: u16) -> Option<Self> {
        Some(match value {
            0 => Self::Ok,
            1 => Self::IoError,
            2 => Self::NotConnected,
            3 => Self::TimedOut,
            4 => Self::InvalidRpcArguments,
            5 => Self::AddressInUse,
            6 => Self::BadAddress,
            7 => Self::OpenError,
            8 => Self::ListenError,
            9 => Self::OperationCanceled,
            10 => Self::RpcThrowException,
            11 => Self::FunctionNotRegistered,
            12 => Self::ProtocolError,
            13 => Self::UnknownProtocolVersion,
            14 => Self::MessageTooLarge,
            15 => Self::ServerHasRun,
            16 => Self::InvalidRpcResult,
            17 => Self::SerialNumberConflict,
            _ => return None,
        })
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::IoError => "io error",
            Self::NotConnected => "not connected",
            Self::TimedOut => "time out",
            Self::InvalidRpcArguments => "invalid rpc arg",
            Self::AddressInUse => "address in use",
            Self::BadAddress => "bad address",
            Self::OpenError => "open error",
            Self::ListenError => "listen error",
            Self::OperationCanceled => "operation canceled",
            Self::RpcThrowException => "rpc throw exception",
            Self::FunctionNotRegistered => "function not registered",
            Self::ProtocolError => "protocol error",
            Self::UnknownProtocolVersion => "unknown protocol version",
            Self::MessageTooLarge => "message too large",
            Self::ServerHasRun => "server has run",
            Self::InvalidRpcResult => "invalid rpc result",
            Self::SerialNumberConflict => "serial number conflict",
        }
    }
}

/// An error returned by the remote RPC handler.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("remote RPC error {code}: {message}")]
pub struct RemoteError {
    pub code: u16,
    pub message: String,
}

/// Errors produced by [`crate::RpcClient`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RpcError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("struct_pack error: {0}")]
    Codec(#[from] StructPackError),
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error("RPC request timed out")]
    TimedOut,
    #[error("RPC connection closed")]
    ConnectionClosed,
    #[error("RPC request is too large")]
    RequestTooLarge,
    #[error("RPC sequence number conflict")]
    SerialNumberConflict,
}

impl RpcError {
    pub(crate) fn io(error: impl fmt::Display) -> Self {
        Self::Io(error.to_string())
    }
}
