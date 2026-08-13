//! coro_rpc v0 frame headers and Tokio codecs.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

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
        let mut fields = &bytes[4..];
        let header = Self {
            magic: bytes[0],
            version: bytes[1],
            serialize_type: bytes[2],
            msg_type: bytes[3],
            sequence: fields.get_u32_le(),
            function_id: fields.get_u32_le(),
            body_len: fields.get_u32_le(),
            attachment_len: fields.get_u32_le(),
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
        let mut fields = &bytes[4..];
        let header = Self {
            magic: bytes[0],
            version: bytes[1],
            error_code: bytes[2],
            msg_type: bytes[3],
            sequence: fields.get_u32_le(),
            body_len: fields.get_u32_le(),
            attachment_len: fields.get_u32_le(),
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("header is {actual} bytes, expected {expected}")]
    InvalidHeaderLength { expected: usize, actual: usize },
    #[error("invalid magic {0}, expected {MAGIC}")]
    InvalidMagic(u8),
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported serialization type {0}")]
    UnsupportedSerialization(u8),
    #[error("body length {length} exceeds limit {limit}")]
    BodyTooLarge { length: u32, limit: usize },
    #[error("attachment length {length} exceeds limit {limit}")]
    AttachmentTooLarge { length: u32, limit: usize },
    #[error("{field} length {actual} does not match header value {declared}")]
    LengthMismatch {
        field: &'static str,
        declared: u32,
        actual: usize,
    },
    #[error("frame length overflow")]
    LengthOverflow,
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFrame {
    pub header: RequestHeader,
    pub body: Bytes,
    pub attachment: Bytes,
}

impl RequestFrame {
    pub fn new(
        sequence: u32,
        function_id: u32,
        body: impl Into<Bytes>,
        attachment: impl Into<Bytes>,
    ) -> Result<Self, ProtocolError> {
        let body = body.into();
        let attachment = attachment.into();
        let (body_len, attachment_len) = payload_lengths(&body, &attachment)?;
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
    pub body: Bytes,
    pub attachment: Bytes,
}

impl ResponseFrame {
    pub fn new(
        sequence: u32,
        error_code: u8,
        body: impl Into<Bytes>,
        attachment: impl Into<Bytes>,
    ) -> Result<Self, ProtocolError> {
        let body = body.into();
        let attachment = attachment.into();
        let (body_len, attachment_len) = payload_lengths(&body, &attachment)?;
        Ok(Self {
            header: ResponseHeader::new(sequence, error_code, body_len, attachment_len),
            body,
            attachment,
        })
    }
}

fn payload_lengths(body: &Bytes, attachment: &Bytes) -> Result<(u32, u32), ProtocolError> {
    Ok((
        u32::try_from(body.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        u32::try_from(attachment.len()).map_err(|_| ProtocolError::LengthOverflow)?,
    ))
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

fn has_header(source: &mut BytesMut, header_len: usize) -> bool {
    if source.len() >= header_len {
        return true;
    }
    source.reserve(header_len - source.len());
    false
}

fn decode_payload(
    source: &mut BytesMut,
    header_len: usize,
    body_len: u32,
    attachment_len: u32,
    limits: FrameLimits,
) -> Result<Option<(Bytes, Bytes)>, FrameError> {
    let payload_len = validate_lengths(body_len, attachment_len, limits)?;
    let frame_len = header_len
        .checked_add(payload_len)
        .ok_or(ProtocolError::LengthOverflow)?;
    if source.len() < frame_len {
        source.reserve(frame_len - source.len());
        return Ok(None);
    }

    source.advance(header_len);
    let body = source.split_to(body_len as usize).freeze();
    let attachment = source.split_to(attachment_len as usize).freeze();
    Ok(Some((body, attachment)))
}

fn require_complete_at_eof<T>(
    source: &BytesMut,
    decoded: Option<T>,
) -> Result<Option<T>, FrameError> {
    if let Some(frame) = decoded {
        return Ok(Some(frame));
    }
    if source.is_empty() {
        return Ok(None);
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!(
            "connection closed with {} bytes of an incomplete coro_rpc frame",
            source.len()
        ),
    )
    .into())
}

fn encode_parts(
    header: &[u8],
    declared_body_len: u32,
    declared_attachment_len: u32,
    body: &Bytes,
    attachment: &Bytes,
    destination: &mut BytesMut,
) -> Result<(), FrameError> {
    validate_encoded_length("body", declared_body_len, body.len())?;
    validate_encoded_length("attachment", declared_attachment_len, attachment.len())?;

    let frame_len = header
        .len()
        .checked_add(body.len())
        .and_then(|length| length.checked_add(attachment.len()))
        .ok_or(ProtocolError::LengthOverflow)?;
    destination.reserve(frame_len);
    destination.put_slice(header);
    destination.put_slice(body);
    destination.put_slice(attachment);
    Ok(())
}

fn validate_encoded_length(
    field: &'static str,
    declared: u32,
    actual: usize,
) -> Result<(), ProtocolError> {
    if usize::try_from(declared).ok() != Some(actual) {
        return Err(ProtocolError::LengthMismatch {
            field,
            declared,
            actual,
        });
    }
    Ok(())
}

/// A streaming Tokio codec for coro_rpc request frames.
#[derive(Debug, Clone, Copy)]
pub struct RequestCodec {
    limits: FrameLimits,
}

impl RequestCodec {
    pub fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }
}

impl Default for RequestCodec {
    fn default() -> Self {
        Self::new(FrameLimits::default())
    }
}

/// A streaming Tokio codec for coro_rpc response frames.
#[derive(Debug, Clone, Copy)]
pub struct ResponseCodec {
    limits: FrameLimits,
}

impl ResponseCodec {
    pub fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }
}

impl Default for ResponseCodec {
    fn default() -> Self {
        Self::new(FrameLimits::default())
    }
}

impl Decoder for RequestCodec {
    type Item = RequestFrame;
    type Error = FrameError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !has_header(source, REQUEST_HEADER_LEN) {
            return Ok(None);
        }
        let header = RequestHeader::decode(&source[..REQUEST_HEADER_LEN])?;
        let Some((body, attachment)) = decode_payload(
            source,
            REQUEST_HEADER_LEN,
            header.body_len,
            header.attachment_len,
            self.limits,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(RequestFrame {
            header,
            body,
            attachment,
        }))
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let decoded = self.decode(source)?;
        require_complete_at_eof(source, decoded)
    }
}

impl Encoder<RequestFrame> for RequestCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        frame: RequestFrame,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode_parts(
            &frame.header.encode(),
            frame.header.body_len,
            frame.header.attachment_len,
            &frame.body,
            &frame.attachment,
            destination,
        )
    }
}

impl Decoder for ResponseCodec {
    type Item = ResponseFrame;
    type Error = FrameError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !has_header(source, RESPONSE_HEADER_LEN) {
            return Ok(None);
        }
        let header = ResponseHeader::decode(&source[..RESPONSE_HEADER_LEN])?;
        let Some((body, attachment)) = decode_payload(
            source,
            RESPONSE_HEADER_LEN,
            header.body_len,
            header.attachment_len,
            self.limits,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(ResponseFrame {
            header,
            body,
            attachment,
        }))
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let decoded = self.decode(source)?;
        require_complete_at_eof(source, decoded)
    }
}

impl Encoder<ResponseFrame> for ResponseCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        frame: ResponseFrame,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode_parts(
            &frame.header.encode(),
            frame.header.body_len,
            frame.header.attachment_len,
            &frame.body,
            &frame.attachment,
            destination,
        )
    }
}

/// Client-side bidirectional codec: requests are encoded and responses decoded.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientCodec {
    requests: RequestCodec,
    responses: ResponseCodec,
}

impl ClientCodec {
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            requests: RequestCodec::new(limits),
            responses: ResponseCodec::new(limits),
        }
    }
}

impl Decoder for ClientCodec {
    type Item = ResponseFrame;
    type Error = FrameError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.responses.decode(source)
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.responses.decode_eof(source)
    }
}

impl Encoder<RequestFrame> for ClientCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        frame: RequestFrame,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.requests.encode(frame, destination)
    }
}

/// Server-side bidirectional codec: requests are decoded and responses encoded.
#[derive(Debug, Clone, Copy, Default)]
pub struct ServerCodec {
    requests: RequestCodec,
    responses: ResponseCodec,
}

impl ServerCodec {
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            requests: RequestCodec::new(limits),
            responses: ResponseCodec::new(limits),
        }
    }
}

impl Decoder for ServerCodec {
    type Item = RequestFrame;
    type Error = FrameError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.requests.decode(source)
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.requests.decode_eof(source)
    }
}

impl Encoder<ResponseFrame> for ServerCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        frame: ResponseFrame,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.responses.encode(frame, destination)
    }
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

    #[test]
    fn request_codec_buffers_fragmented_frames() {
        let expected =
            RequestFrame::new(7, 42, b"request body".to_vec(), b"attachment".to_vec()).unwrap();
        let mut encoded = BytesMut::new();
        RequestCodec::default()
            .encode(expected.clone(), &mut encoded)
            .unwrap();

        let tail = encoded.split_off(9);
        let mut codec = RequestCodec::default();
        assert_eq!(codec.decode(&mut encoded).unwrap(), None);
        encoded.extend_from_slice(&tail);
        assert_eq!(codec.decode(&mut encoded).unwrap(), Some(expected));
        assert!(encoded.is_empty());
    }

    #[test]
    fn response_codec_decodes_pipelined_frames() {
        let first = ResponseFrame::new(1, 0, b"one".to_vec(), Bytes::new()).unwrap();
        let second = ResponseFrame::new(2, 11, b"two".to_vec(), b"extra".to_vec()).unwrap();
        let mut encoded = BytesMut::new();
        let mut codec = ResponseCodec::default();
        codec.encode(first.clone(), &mut encoded).unwrap();
        codec.encode(second.clone(), &mut encoded).unwrap();

        assert_eq!(codec.decode(&mut encoded).unwrap(), Some(first));
        assert_eq!(codec.decode(&mut encoded).unwrap(), Some(second));
        assert_eq!(codec.decode(&mut encoded).unwrap(), None);
    }

    #[test]
    fn codec_rejects_oversized_and_incomplete_frames() {
        let limits = FrameLimits {
            max_body_len: 3,
            max_attachment_len: 3,
        };
        let mut oversized = BytesMut::from(&RequestHeader::new(1, 2, 4, 0).encode()[..]);
        let error = RequestCodec::new(limits)
            .decode(&mut oversized)
            .unwrap_err();
        assert!(matches!(
            error,
            FrameError::Protocol(ProtocolError::BodyTooLarge {
                length: 4,
                limit: 3
            })
        ));

        let mut incomplete = BytesMut::from(&ResponseHeader::new(1, 0, 1, 0).encode()[..]);
        let error = ResponseCodec::default()
            .decode_eof(&mut incomplete)
            .unwrap_err();
        assert!(matches!(
            error,
            FrameError::Io(ref error) if error.kind() == io::ErrorKind::UnexpectedEof
        ));
    }
}
