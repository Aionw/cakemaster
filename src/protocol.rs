//! coro_rpc v0 frame headers and Tokio I/O helpers.

use std::fmt;
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAGIC: u8 = 21;
pub const VERSION: u8 = 0;
pub const STRUCT_PACK_SERIALIZATION: u8 = 0;
pub const REQUEST_HEADER_LEN: usize = 20;
pub const RESPONSE_HEADER_LEN: usize = 16;

/// Limits applied before allocating buffers for an incoming frame.
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    pub max_body_len: usize,
    pub max_attachment_len: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_body_len: 64 * 1024 * 1024,
            max_attachment_len: 256 * 1024 * 1024,
        }
    }
}

/// A request header with the exact yalantinglibs field layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    pub magic: u8,
    pub version: u8,
    pub serialize_type: u8,
    pub msg_type: u8,
    pub sequence: u32,
    pub function_id: u32,
    pub body_len: u32,
    pub attachment_len: u32,
}

impl RequestHeader {
    pub fn new(sequence: u32, function_id: u32, body_len: u32, attachment_len: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            serialize_type: STRUCT_PACK_SERIALIZATION,
            msg_type: 0,
            sequence,
            function_id,
            body_len,
            attachment_len,
        }
    }

    pub fn encode(self) -> [u8; REQUEST_HEADER_LEN] {
        let mut bytes = [0_u8; REQUEST_HEADER_LEN];
        bytes[0] = self.magic;
        bytes[1] = self.version;
        bytes[2] = self.serialize_type;
        bytes[3] = self.msg_type;
        bytes[4..8].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.function_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.body_len.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.attachment_len.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() != REQUEST_HEADER_LEN {
            return Err(ProtocolError::InvalidHeaderLength {
                expected: REQUEST_HEADER_LEN,
                actual: bytes.len(),
            });
        }
        let header = Self {
            magic: bytes[0],
            version: bytes[1],
            serialize_type: bytes[2],
            msg_type: bytes[3],
            sequence: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            function_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            body_len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            attachment_len: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        };
        validate_common(header.magic, header.version)?;
        if header.serialize_type != STRUCT_PACK_SERIALIZATION {
            return Err(ProtocolError::UnsupportedSerialization(
                header.serialize_type,
            ));
        }
        Ok(header)
    }
}

/// A response header with the exact yalantinglibs field layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub magic: u8,
    pub version: u8,
    pub error_code: u8,
    pub msg_type: u8,
    pub sequence: u32,
    pub body_len: u32,
    pub attachment_len: u32,
}

impl ResponseHeader {
    pub fn new(sequence: u32, error_code: u8, body_len: u32, attachment_len: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            error_code,
            msg_type: 0,
            sequence,
            body_len,
            attachment_len,
        }
    }

    pub fn encode(self) -> [u8; RESPONSE_HEADER_LEN] {
        let mut bytes = [0_u8; RESPONSE_HEADER_LEN];
        bytes[0] = self.magic;
        bytes[1] = self.version;
        bytes[2] = self.error_code;
        bytes[3] = self.msg_type;
        bytes[4..8].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.body_len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.attachment_len.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() != RESPONSE_HEADER_LEN {
            return Err(ProtocolError::InvalidHeaderLength {
                expected: RESPONSE_HEADER_LEN,
                actual: bytes.len(),
            });
        }
        let header = Self {
            magic: bytes[0],
            version: bytes[1],
            error_code: bytes[2],
            msg_type: bytes[3],
            sequence: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            body_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            attachment_len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        };
        validate_common(header.magic, header.version)?;
        Ok(header)
    }
}

fn validate_common(magic: u8, version: u8) -> Result<(), ProtocolError> {
    if magic != MAGIC {
        return Err(ProtocolError::InvalidMagic(magic));
    }
    if version > VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidHeaderLength { expected: usize, actual: usize },
    InvalidMagic(u8),
    UnsupportedVersion(u8),
    UnsupportedSerialization(u8),
    BodyTooLarge { length: u32, limit: usize },
    AttachmentTooLarge { length: u32, limit: usize },
    LengthOverflow,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHeaderLength { expected, actual } => {
                write!(f, "header is {actual} bytes, expected {expected}")
            }
            Self::InvalidMagic(value) => write!(f, "invalid magic {value}, expected {MAGIC}"),
            Self::UnsupportedVersion(value) => {
                write!(f, "unsupported protocol version {value}")
            }
            Self::UnsupportedSerialization(value) => {
                write!(f, "unsupported serialization type {value}")
            }
            Self::BodyTooLarge { length, limit } => {
                write!(f, "body length {length} exceeds limit {limit}")
            }
            Self::AttachmentTooLarge { length, limit } => {
                write!(f, "attachment length {length} exceeds limit {limit}")
            }
            Self::LengthOverflow => f.write_str("frame length overflow"),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    Protocol(ProtocolError),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Protocol(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ProtocolError> for FrameError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFrame {
    pub header: RequestHeader,
    pub body: Vec<u8>,
    pub attachment: Vec<u8>,
}

impl RequestFrame {
    pub fn new(
        sequence: u32,
        function_id: u32,
        body: Vec<u8>,
        attachment: Vec<u8>,
    ) -> Result<Self, ProtocolError> {
        let body_len = u32::try_from(body.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        let attachment_len =
            u32::try_from(attachment.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        Ok(Self {
            header: RequestHeader::new(sequence, function_id, body_len, attachment_len),
            body,
            attachment,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseFrame {
    pub header: ResponseHeader,
    pub body: Vec<u8>,
    pub attachment: Vec<u8>,
}

impl ResponseFrame {
    pub fn new(
        sequence: u32,
        error_code: u8,
        body: Vec<u8>,
        attachment: Vec<u8>,
    ) -> Result<Self, ProtocolError> {
        let body_len = u32::try_from(body.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        let attachment_len =
            u32::try_from(attachment.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        Ok(Self {
            header: ResponseHeader::new(sequence, error_code, body_len, attachment_len),
            body,
            attachment,
        })
    }
}

fn validate_lengths(
    body_len: u32,
    attachment_len: u32,
    limits: FrameLimits,
) -> Result<usize, ProtocolError> {
    if body_len as usize > limits.max_body_len {
        return Err(ProtocolError::BodyTooLarge {
            length: body_len,
            limit: limits.max_body_len,
        });
    }
    if attachment_len as usize > limits.max_attachment_len {
        return Err(ProtocolError::AttachmentTooLarge {
            length: attachment_len,
            limit: limits.max_attachment_len,
        });
    }
    (body_len as usize)
        .checked_add(attachment_len as usize)
        .ok_or(ProtocolError::LengthOverflow)
}

pub async fn read_request<R>(
    reader: &mut R,
    limits: FrameLimits,
) -> Result<RequestFrame, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = [0_u8; REQUEST_HEADER_LEN];
    reader.read_exact(&mut bytes).await?;
    let header = RequestHeader::decode(&bytes)?;
    let total = validate_lengths(header.body_len, header.attachment_len, limits)?;
    let mut payload = vec![0_u8; total];
    reader.read_exact(&mut payload).await?;
    let body_len = header.body_len as usize;
    let attachment = payload.split_off(body_len);
    Ok(RequestFrame {
        header,
        body: payload,
        attachment,
    })
}

pub async fn write_request<W>(writer: &mut W, frame: &RequestFrame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&frame.header.encode()).await?;
    writer.write_all(&frame.body).await?;
    writer.write_all(&frame.attachment).await?;
    writer.flush().await
}

pub async fn read_response<R>(
    reader: &mut R,
    limits: FrameLimits,
) -> Result<ResponseFrame, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = [0_u8; RESPONSE_HEADER_LEN];
    reader.read_exact(&mut bytes).await?;
    let header = ResponseHeader::decode(&bytes)?;
    let total = validate_lengths(header.body_len, header.attachment_len, limits)?;
    let mut payload = vec![0_u8; total];
    reader.read_exact(&mut payload).await?;
    let body_len = header.body_len as usize;
    let attachment = payload.split_off(body_len);
    Ok(ResponseFrame {
        header,
        body: payload,
        attachment,
    })
}

pub async fn write_response<W>(writer: &mut W, frame: &ResponseFrame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&frame.header.encode()).await?;
    writer.write_all(&frame.body).await?;
    writer.write_all(&frame.attachment).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_header_golden_bytes() {
        let header = RequestHeader::new(0x0102_0304, 0xcbb1_1ed8, 9, 3);
        assert_eq!(
            header.encode(),
            [
                0x15, 0, 0, 0, 4, 3, 2, 1, 0xd8, 0x1e, 0xb1, 0xcb, 9, 0, 0, 0, 3, 0, 0, 0,
            ]
        );
        assert_eq!(RequestHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn response_header_golden_bytes() {
        let header = ResponseHeader::new(0x0102_0304, 0, 9, 3);
        assert_eq!(
            header.encode(),
            [0x15, 0, 0, 0, 4, 3, 2, 1, 9, 0, 0, 0, 3, 0, 0, 0]
        );
        assert_eq!(ResponseHeader::decode(&header.encode()).unwrap(), header);
    }
}
