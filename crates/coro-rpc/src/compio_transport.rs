//! Native Compio framing for server-side coro_rpc connections.

use std::io;

use bytes::Bytes;
use compio::buf::{IoBuf, IoBufMut, Slice};
use compio::io::framed::codec::{Decoder, Encoder};
use compio::io::framed::frame::{Frame, Framer};

use crate::protocol::{
    FrameError, FrameLimits, MAGIC, ProtocolError, REQUEST_HEADER_LEN, RESPONSE_HEADER_LEN,
    RequestFrame, RequestHeader, ResponseFrame, validate_lengths,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ServerFramer {
    limits: FrameLimits,
}

impl ServerFramer {
    pub(crate) const fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }
}

impl<B: IoBufMut> Framer<B> for ServerFramer {
    fn enclose(&mut self, _buffer: &mut B) {}

    fn extract(&mut self, buffer: &Slice<B>) -> io::Result<Option<Frame>> {
        let bytes = buffer.as_init();
        if let Some(&magic) = bytes.first()
            && magic != MAGIC
        {
            return Err(invalid_data(ProtocolError::InvalidMagic(magic)));
        }
        if bytes.len() < REQUEST_HEADER_LEN {
            return Ok(None);
        }
        let header = RequestHeader::decode(&bytes[..REQUEST_HEADER_LEN]).map_err(invalid_data)?;
        let payload_len = validate_lengths(header.body_len, header.attachment_len, self.limits)
            .map_err(invalid_data)?;
        let frame_len = REQUEST_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| invalid_data(ProtocolError::LengthOverflow))?;
        Ok((bytes.len() >= frame_len).then(|| Frame::new(0, frame_len, 0)))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerCodec;

impl<B: IoBuf> Decoder<RequestFrame, B> for ServerCodec {
    type Error = FrameError;

    fn decode(&mut self, buffer: &Slice<B>) -> Result<RequestFrame, Self::Error> {
        let bytes = buffer.as_init();
        let header = RequestHeader::decode(&bytes[..REQUEST_HEADER_LEN])?;
        let body_end = REQUEST_HEADER_LEN
            .checked_add(header.body_len as usize)
            .ok_or(ProtocolError::LengthOverflow)?;
        let attachment_end = body_end
            .checked_add(header.attachment_len as usize)
            .ok_or(ProtocolError::LengthOverflow)?;
        if bytes.len() != attachment_end {
            return Err(ProtocolError::LengthMismatch {
                field: "frame",
                declared: u32::try_from(attachment_end)
                    .map_err(|_| ProtocolError::LengthOverflow)?,
                actual: bytes.len(),
            }
            .into());
        }
        Ok(RequestFrame {
            header,
            body: Bytes::copy_from_slice(&bytes[REQUEST_HEADER_LEN..body_end]),
            attachment: Bytes::copy_from_slice(&bytes[body_end..attachment_end]),
        })
    }
}

impl<B: IoBufMut> Encoder<ResponseFrame, B> for ServerCodec {
    type Error = FrameError;

    fn encode(&mut self, frame: ResponseFrame, buffer: &mut B) -> Result<(), Self::Error> {
        validate_encoded_len("body", frame.header.body_len, frame.body.len())?;
        validate_encoded_len(
            "attachment",
            frame.header.attachment_len,
            frame.attachment.len(),
        )?;
        let frame_len = RESPONSE_HEADER_LEN
            .checked_add(frame.body.len())
            .and_then(|len| len.checked_add(frame.attachment.len()))
            .ok_or(ProtocolError::LengthOverflow)?;
        buffer.reserve(frame_len).map_err(reserve_error)?;
        buffer
            .extend_from_slice(&frame.header.encode())
            .map_err(reserve_error)?;
        buffer
            .extend_from_slice(&frame.body)
            .map_err(reserve_error)?;
        buffer
            .extend_from_slice(&frame.attachment)
            .map_err(reserve_error)?;
        Ok(())
    }
}

fn validate_encoded_len(
    field: &'static str,
    declared: u32,
    actual: usize,
) -> Result<(), ProtocolError> {
    if usize::try_from(declared).ok() == Some(actual) {
        return Ok(());
    }
    Err(ProtocolError::LengthMismatch {
        field,
        declared,
        actual,
    })
}

fn invalid_data(error: ProtocolError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn reserve_error(error: impl std::fmt::Display) -> FrameError {
    io::Error::other(error.to_string()).into()
}
