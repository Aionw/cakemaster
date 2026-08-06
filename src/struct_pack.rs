//! The portable subset of yalantinglibs `struct_pack` used by coro_rpc.
//!
//! The implementation covers fixed-width primitives, strings, byte strings,
//! vectors, arrays, maps, sets, options, expected values (`Result`), tuples and
//! reflected structs. C++ structs should use `YLT_REFL` so their representation
//! is field-based rather than ABI/padding-based.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::hash::md5_hash32;

const TYPE_INT32: u8 = 1;
const TYPE_UINT32: u8 = 2;
const TYPE_INT64: u8 = 3;
const TYPE_UINT64: u8 = 4;
const TYPE_INT8: u8 = 5;
const TYPE_UINT8: u8 = 6;
const TYPE_INT16: u8 = 7;
const TYPE_UINT16: u8 = 8;
const TYPE_BOOL: u8 = 11;
const TYPE_CHAR8: u8 = 12;
const TYPE_CHAR32: u8 = 14;
const TYPE_FLOAT32: u8 = 17;
const TYPE_FLOAT64: u8 = 18;
const TYPE_STRING: u8 = 128;
const TYPE_ARRAY: u8 = 129;
const TYPE_MAP: u8 = 130;
const TYPE_SET: u8 = 131;
const TYPE_CONTAINER: u8 = 132;
const TYPE_OPTIONAL: u8 = 133;
const TYPE_EXPECTED: u8 = 135;
const TYPE_MONOSTATE: u8 = 250;
const TYPE_STRUCT: u8 = 253;
const TYPE_END: u8 = 255;

const META_COMPATIBLE_SIZE_MASK: u8 = 0b0000_0011;
const META_TYPE_LITERAL: u8 = 0b0000_0100;
const META_LENGTH_WIDTH_MASK: u8 = 0b0001_1000;
const META_RESERVED_MASK: u8 = 0b1110_0000;

const DEFAULT_CONTAINER_LIMIT: usize = 64 * 1024 * 1024;

/// Errors produced while encoding or decoding a struct_pack value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructPackError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidTypeHash { expected: u32, actual: u32 },
    InvalidTypeLiteral,
    InvalidUtf8,
    InvalidBool(u8),
    InvalidChar(u32),
    ContainerTooLarge { length: u64, limit: usize },
    LengthOverflow,
    TrailingBytes(usize),
    InvalidMetadata(&'static str),
    DeclaredSizeMismatch { declared: u64, actual: usize },
}

impl fmt::Display for StructPackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => {
                write!(f, "need {needed} bytes, only {remaining} remain")
            }
            Self::InvalidTypeHash { expected, actual } => write!(
                f,
                "type hash mismatch (expected {expected:#010x}, got {actual:#010x})"
            ),
            Self::InvalidTypeLiteral => f.write_str("full type literal does not match"),
            Self::InvalidUtf8 => f.write_str("C++ string is not valid UTF-8"),
            Self::InvalidBool(value) => write!(f, "invalid bool byte {value}"),
            Self::InvalidChar(value) => write!(f, "invalid char32 value {value:#x}"),
            Self::ContainerTooLarge { length, limit } => {
                write!(f, "container length {length} exceeds limit {limit}")
            }
            Self::LengthOverflow => f.write_str("container length cannot be represented"),
            Self::TrailingBytes(count) => write!(f, "{count} trailing bytes remain"),
            Self::InvalidMetadata(message) => write!(f, "invalid metadata: {message}"),
            Self::DeclaredSizeMismatch { declared, actual } => write!(
                f,
                "compatible-data size declares {declared} bytes, actual size is {actual}"
            ),
        }
    }
}

impl std::error::Error for StructPackError {}

/// A C++ `std::string` that may contain arbitrary non-UTF-8 bytes.
///
/// `String` and `ByteString` intentionally have the same struct_pack type hash.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteString(pub Vec<u8>);

impl From<Vec<u8>> for ByteString {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<String> for ByteString {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl AsRef<[u8]> for ByteString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Trait implemented by values that have a portable struct_pack schema.
pub trait StructPack: Sized {
    /// Appends the yalantinglibs type literal for this value.
    #[doc(hidden)]
    fn append_type_literal(output: &mut Vec<u8>);

    /// Returns the largest dynamically sized container in this value.
    #[doc(hidden)]
    fn max_container_len(&self) -> usize {
        0
    }

    /// Encodes only the data payload; the public [`serialize`] function adds
    /// the struct_pack hash and metadata.
    #[doc(hidden)]
    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError>;

    /// Decodes only the data payload.
    #[doc(hidden)]
    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError>;
}

/// Returns the canonical struct_pack type literal for `T`.
pub fn type_literal<T: StructPack>() -> Vec<u8> {
    let mut literal = Vec::new();
    T::append_type_literal(&mut literal);
    literal
}

/// Returns the struct_pack type hash for `T`, with the metadata flag cleared.
pub fn type_hash<T: StructPack>() -> u32 {
    md5_hash32(&type_literal::<T>()) & 0xffff_fffe
}

/// Serializes a value in release-compatible struct_pack form.
pub fn serialize<T: StructPack>(value: &T) -> Result<Vec<u8>, StructPackError> {
    let max_length = value.max_container_len();
    let length_width = width_for_length(max_length);
    let has_metadata = length_width != 1;
    let hash = type_hash::<T>() | u32::from(has_metadata);

    let mut output = Vec::new();
    output.extend_from_slice(&hash.to_le_bytes());
    if has_metadata {
        output.push(width_tag(length_width) << 3);
    }

    let mut encoder = Encoder {
        output: &mut output,
        length_width,
    };
    value.encode_payload(&mut encoder)?;
    Ok(output)
}

/// Deserializes a value, limiting any individual container to 64 Mi elements.
pub fn deserialize<T: StructPack>(input: &[u8]) -> Result<T, StructPackError> {
    deserialize_with_limit(input, DEFAULT_CONTAINER_LIMIT)
}

/// Deserializes a value with a caller-provided per-container element limit.
pub fn deserialize_with_limit<T: StructPack>(
    input: &[u8],
    container_limit: usize,
) -> Result<T, StructPackError> {
    let mut decoder = Decoder {
        input,
        position: 0,
        length_width: 1,
        container_limit,
    };

    let encoded_hash = decoder.read_u32()?;
    let expected_hash = type_hash::<T>();
    if encoded_hash & 0xffff_fffe != expected_hash {
        return Err(StructPackError::InvalidTypeHash {
            expected: expected_hash,
            actual: encoded_hash & 0xffff_fffe,
        });
    }

    let mut declared_total_size = None;
    if encoded_hash & 1 != 0 {
        let metadata = decoder.read_u8()?;
        if metadata & META_RESERVED_MASK != 0 {
            return Err(StructPackError::InvalidMetadata("reserved bits are set"));
        }

        declared_total_size = match metadata & META_COMPATIBLE_SIZE_MASK {
            0 => None,
            1 => Some(u64::from(decoder.read_u16()?)),
            2 => Some(u64::from(decoder.read_u32()?)),
            3 => Some(decoder.read_u64()?),
            _ => unreachable!(),
        };

        if metadata & META_TYPE_LITERAL != 0 {
            let expected_literal = type_literal::<T>();
            let actual_literal = decoder.read_exact(expected_literal.len())?;
            let terminator = decoder.read_u8()?;
            if actual_literal != expected_literal || terminator != 0 {
                return Err(StructPackError::InvalidTypeLiteral);
            }
        }

        decoder.length_width = match (metadata & META_LENGTH_WIDTH_MASK) >> 3 {
            0 => 1,
            1 => 2,
            2 => 4,
            3 => 8,
            _ => unreachable!(),
        };
    }

    if let Some(declared) = declared_total_size
        && declared != input.len() as u64
    {
        return Err(StructPackError::DeclaredSizeMismatch {
            declared,
            actual: input.len(),
        });
    }

    let value = T::decode_payload(&mut decoder)?;
    let remaining = decoder.remaining();
    // Trailing bytes are exactly how struct_pack carries newer `compatible<T>`
    // fields to an older schema. They are accepted only when a total-size field
    // explicitly marks the message as compatible data.
    if remaining != 0 && declared_total_size.is_none() {
        return Err(StructPackError::TrailingBytes(remaining));
    }
    Ok(value)
}

fn width_for_length(length: usize) -> u8 {
    if length < 1 << 8 {
        1
    } else if length < 1 << 16 {
        2
    } else if (length as u128) < (1_u128 << 32) {
        4
    } else {
        8
    }
}

fn width_tag(width: u8) -> u8 {
    match width {
        1 => 0,
        2 => 1,
        4 => 2,
        8 => 3,
        _ => unreachable!("validated struct_pack length width"),
    }
}

fn append_size_literal(mut size: usize, output: &mut Vec<u8>) {
    while size >= 127 {
        output.push((size % 127 + 1) as u8);
        size /= 127;
    }
    output.push((size + 129) as u8);
}

/// Payload encoder exposed for implementations generated by
/// [`crate::impl_struct_pack!`].
#[doc(hidden)]
pub struct Encoder<'a> {
    output: &'a mut Vec<u8>,
    length_width: u8,
}

impl Encoder<'_> {
    fn write(&mut self, bytes: &[u8]) {
        self.output.extend_from_slice(bytes);
    }

    fn write_len(&mut self, length: usize) -> Result<(), StructPackError> {
        match self.length_width {
            1 => self
                .output
                .push(u8::try_from(length).map_err(|_| StructPackError::LengthOverflow)?),
            2 => self.output.extend_from_slice(
                &u16::try_from(length)
                    .map_err(|_| StructPackError::LengthOverflow)?
                    .to_le_bytes(),
            ),
            4 => self.output.extend_from_slice(
                &u32::try_from(length)
                    .map_err(|_| StructPackError::LengthOverflow)?
                    .to_le_bytes(),
            ),
            8 => self.output.extend_from_slice(
                &u64::try_from(length)
                    .map_err(|_| StructPackError::LengthOverflow)?
                    .to_le_bytes(),
            ),
            _ => unreachable!("validated struct_pack length width"),
        }
        Ok(())
    }
}

/// Payload decoder exposed for implementations generated by
/// [`crate::impl_struct_pack!`].
#[doc(hidden)]
pub struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
    length_width: u8,
    container_limit: usize,
}

impl<'a> Decoder<'a> {
    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], StructPackError> {
        if length > self.remaining() {
            return Err(StructPackError::UnexpectedEof {
                needed: length,
                remaining: self.remaining(),
            });
        }
        let start = self.position;
        self.position += length;
        Ok(&self.input[start..self.position])
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], StructPackError> {
        Ok(self
            .read_exact(N)?
            .try_into()
            .expect("slice length was checked"))
    }

    fn read_u8(&mut self) -> Result<u8, StructPackError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, StructPackError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, StructPackError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, StructPackError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_len(&mut self) -> Result<usize, StructPackError> {
        let length = match self.length_width {
            1 => u64::from(self.read_u8()?),
            2 => u64::from(self.read_u16()?),
            4 => u64::from(self.read_u32()?),
            8 => self.read_u64()?,
            _ => unreachable!("validated struct_pack length width"),
        };
        if length > self.container_limit as u64 {
            return Err(StructPackError::ContainerTooLarge {
                length,
                limit: self.container_limit,
            });
        }
        usize::try_from(length).map_err(|_| StructPackError::LengthOverflow)
    }
}

macro_rules! impl_integer {
    ($type:ty, $id:expr) => {
        impl StructPack for $type {
            fn append_type_literal(output: &mut Vec<u8>) {
                output.push($id);
            }

            fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
                encoder.write(&self.to_le_bytes());
                Ok(())
            }

            fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
                Ok(<$type>::from_le_bytes(decoder.read_array()?))
            }
        }
    };
}

impl_integer!(i8, TYPE_INT8);
impl_integer!(u8, TYPE_UINT8);
impl_integer!(i16, TYPE_INT16);
impl_integer!(u16, TYPE_UINT16);
impl_integer!(i32, TYPE_INT32);
impl_integer!(u32, TYPE_UINT32);
impl_integer!(i64, TYPE_INT64);
impl_integer!(u64, TYPE_UINT64);
impl_integer!(f32, TYPE_FLOAT32);
impl_integer!(f64, TYPE_FLOAT64);

impl StructPack for bool {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_BOOL);
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write(&[u8::from(*self)]);
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        match decoder.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(StructPackError::InvalidBool(value)),
        }
    }
}

impl StructPack for char {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_CHAR32);
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write(&u32::from(*self).to_le_bytes());
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let value = decoder.read_u32()?;
        char::from_u32(value).ok_or(StructPackError::InvalidChar(value))
    }
}

impl StructPack for () {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_MONOSTATE);
    }

    fn encode_payload(&self, _encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        Ok(())
    }

    fn decode_payload(_decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        Ok(())
    }
}

fn append_cpp_string_literal(output: &mut Vec<u8>) {
    output.extend_from_slice(&[TYPE_STRING, TYPE_CHAR8]);
}

impl StructPack for String {
    fn append_type_literal(output: &mut Vec<u8>) {
        append_cpp_string_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.len()
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write_len(self.len())?;
        encoder.write(self.as_bytes());
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let length = decoder.read_len()?;
        let bytes = decoder.read_exact(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| StructPackError::InvalidUtf8)
    }
}

impl StructPack for ByteString {
    fn append_type_literal(output: &mut Vec<u8>) {
        append_cpp_string_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.0.len()
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write_len(self.0.len())?;
        encoder.write(&self.0);
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let length = decoder.read_len()?;
        Ok(Self(decoder.read_exact(length)?.to_vec()))
    }
}

impl<T: StructPack> StructPack for Vec<T> {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_CONTAINER);
        T::append_type_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.iter()
            .fold(self.len(), |max, value| max.max(value.max_container_len()))
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write_len(self.len())?;
        for value in self {
            value.encode_payload(encoder)?;
        }
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let length = decoder.read_len()?;
        let mut values = Vec::with_capacity(length.min(4096));
        for _ in 0..length {
            values.push(T::decode_payload(decoder)?);
        }
        Ok(values)
    }
}

impl<T: StructPack, const N: usize> StructPack for [T; N] {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_ARRAY);
        T::append_type_literal(output);
        append_size_literal(N, output);
    }

    fn max_container_len(&self) -> usize {
        self.iter()
            .map(StructPack::max_container_len)
            .max()
            .unwrap_or(0)
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        for value in self {
            value.encode_payload(encoder)?;
        }
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let mut values = Vec::with_capacity(N);
        for _ in 0..N {
            values.push(T::decode_payload(decoder)?);
        }
        values
            .try_into()
            .map_err(|_| StructPackError::LengthOverflow)
    }
}

impl<T: StructPack> StructPack for Option<T> {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_OPTIONAL);
        T::append_type_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.as_ref().map_or(0, StructPack::max_container_len)
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write(&[u8::from(self.is_some())]);
        if let Some(value) = self {
            value.encode_payload(encoder)?;
        }
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        match decoder.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(T::decode_payload(decoder)?)),
            value => Err(StructPackError::InvalidBool(value)),
        }
    }
}

impl<T: StructPack, E: StructPack> StructPack for Result<T, E> {
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_EXPECTED);
        T::append_type_literal(output);
        E::append_type_literal(output);
    }

    fn max_container_len(&self) -> usize {
        match self {
            Ok(value) => value.max_container_len(),
            Err(error) => error.max_container_len(),
        }
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        match self {
            Ok(value) => {
                encoder.write(&[1]);
                value.encode_payload(encoder)
            }
            Err(error) => {
                encoder.write(&[0]);
                error.encode_payload(encoder)
            }
        }
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        match decoder.read_u8()? {
            0 => Ok(Err(E::decode_payload(decoder)?)),
            1 => Ok(Ok(T::decode_payload(decoder)?)),
            value => Err(StructPackError::InvalidBool(value)),
        }
    }
}

impl<K, V> StructPack for BTreeMap<K, V>
where
    K: StructPack + Ord,
    V: StructPack,
{
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_MAP);
        K::append_type_literal(output);
        V::append_type_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.iter().fold(self.len(), |max, (key, value)| {
            max.max(key.max_container_len())
                .max(value.max_container_len())
        })
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write_len(self.len())?;
        for (key, value) in self {
            key.encode_payload(encoder)?;
            value.encode_payload(encoder)?;
        }
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let length = decoder.read_len()?;
        let mut values = BTreeMap::new();
        for _ in 0..length {
            values.insert(K::decode_payload(decoder)?, V::decode_payload(decoder)?);
        }
        Ok(values)
    }
}

impl<T> StructPack for BTreeSet<T>
where
    T: StructPack + Ord,
{
    fn append_type_literal(output: &mut Vec<u8>) {
        output.push(TYPE_SET);
        T::append_type_literal(output);
    }

    fn max_container_len(&self) -> usize {
        self.iter()
            .fold(self.len(), |max, value| max.max(value.max_container_len()))
    }

    fn encode_payload(&self, encoder: &mut Encoder<'_>) -> Result<(), StructPackError> {
        encoder.write_len(self.len())?;
        for value in self {
            value.encode_payload(encoder)?;
        }
        Ok(())
    }

    fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
        let length = decoder.read_len()?;
        let mut values = BTreeSet::new();
        for _ in 0..length {
            values.insert(T::decode_payload(decoder)?);
        }
        Ok(values)
    }
}

macro_rules! impl_tuple {
    ($(($type:ident, $index:tt)),+ $(,)?) => {
        impl<$($type: StructPack),+> StructPack for ($($type,)+) {
            fn append_type_literal(output: &mut Vec<u8>) {
                output.push(TYPE_STRUCT);
                $($type::append_type_literal(output);)+
                output.push(TYPE_END);
            }

            fn max_container_len(&self) -> usize {
                let mut maximum = 0;
                $(maximum = maximum.max(self.$index.max_container_len());)+
                maximum
            }

            fn encode_payload(
                &self,
                encoder: &mut Encoder<'_>,
            ) -> Result<(), StructPackError> {
                $(self.$index.encode_payload(encoder)?;)+
                Ok(())
            }

            fn decode_payload(decoder: &mut Decoder<'_>) -> Result<Self, StructPackError> {
                Ok(($($type::decode_payload(decoder)?,)+))
            }
        }
    };
}

impl_tuple!((A, 0));
impl_tuple!((A, 0), (B, 1));
impl_tuple!((A, 0), (B, 1), (C, 2));
impl_tuple!((A, 0), (B, 1), (C, 2), (D, 3));
impl_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4));
impl_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5));
impl_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6));
impl_tuple!(
    (A, 0),
    (B, 1),
    (C, 2),
    (D, 3),
    (E, 4),
    (F, 5),
    (G, 6),
    (H, 7)
);

/// Internal constants used by [`crate::impl_struct_pack!`].
#[doc(hidden)]
pub mod __private {
    pub const TYPE_STRUCT: u8 = super::TYPE_STRUCT;
    pub const TYPE_END: u8 = super::TYPE_END;
}

/// Implements [`StructPack`] for a named-field Rust struct.
///
/// Field order is the wire order and must match the order passed to C++
/// `YLT_REFL`.
///
/// ```
/// use cakemaster::impl_struct_pack;
///
/// struct Person {
///     id: i32,
///     name: String,
/// }
/// impl_struct_pack!(Person { id: i32, name: String });
/// ```
#[macro_export]
macro_rules! impl_struct_pack {
    ($type:ty { $($field:ident : $field_type:ty),+ $(,)? }) => {
        impl $crate::struct_pack::StructPack for $type {
            fn append_type_literal(output: &mut ::std::vec::Vec<u8>) {
                output.push($crate::struct_pack::__private::TYPE_STRUCT);
                $(
                    <$field_type as $crate::struct_pack::StructPack>::append_type_literal(output);
                )+
                output.push($crate::struct_pack::__private::TYPE_END);
            }

            fn max_container_len(&self) -> usize {
                let mut maximum = 0usize;
                $(
                    maximum = maximum.max(
                        <$field_type as $crate::struct_pack::StructPack>::max_container_len(
                            &self.$field,
                        ),
                    );
                )+
                maximum
            }

            fn encode_payload(
                &self,
                encoder: &mut $crate::struct_pack::Encoder<'_>,
            ) -> ::std::result::Result<(), $crate::struct_pack::StructPackError> {
                $(
                    <$field_type as $crate::struct_pack::StructPack>::encode_payload(
                        &self.$field,
                        encoder,
                    )?;
                )+
                Ok(())
            }

            fn decode_payload(
                decoder: &mut $crate::struct_pack::Decoder<'_>,
            ) -> ::std::result::Result<Self, $crate::struct_pack::StructPackError> {
                Ok(Self {
                    $(
                        $field: <$field_type as $crate::struct_pack::StructPack>::decode_payload(
                            decoder,
                        )?,
                    )+
                })
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Person {
        id: i32,
        name: String,
    }
    crate::impl_struct_pack!(Person {
        id: i32,
        name: String
    });

    #[test]
    fn hashes_match_upstream() {
        assert_eq!(type_literal::<i32>(), vec![1]);
        assert_eq!(type_hash::<i32>(), 0x55a5_4008);
        assert_eq!(type_literal::<String>(), vec![0x80, 0x0c]);
        assert_eq!(type_hash::<String>(), 0x9dcf_fa76);
        assert_eq!(type_hash::<(i32, String)>(), 0x85a8_fde6);
        assert_eq!(type_hash::<Person>(), type_hash::<(i32, String)>());
    }

    #[test]
    fn round_trips_reflected_struct() {
        let value = Person {
            id: 42,
            name: "Betty".to_owned(),
        };
        let bytes = serialize(&value).unwrap();
        assert_eq!(deserialize::<Person>(&bytes).unwrap(), value);
    }

    #[test]
    fn uses_two_byte_lengths_at_256() {
        let value = "x".repeat(300);
        let bytes = serialize(&value).unwrap();
        assert_eq!(&bytes[..7], &[0x77, 0xfa, 0xcf, 0x9d, 0x08, 0x2c, 0x01]);
        assert_eq!(deserialize::<String>(&bytes).unwrap(), value);
    }
}
