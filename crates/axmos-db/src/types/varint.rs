use std::{
    cmp::Ordering,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Read,
};

use crate::types::{SerializationError, SerializationResult};

/// Maximum bytes needed to encode any i64 value (ZigZag + base-128).
pub const MAX_VARINT_LEN: usize = 10;

// VarInt stands for variable-length integer.
//
// It is a special type that is encoded using 10 bytes, and depending on the value, will occupy more or less space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarInt<'a>(&'a [u8]);

impl<'a> VarInt<'a> {
    /// Create a VarInt from encoded bytes (borrows the bytes)
    pub(crate) fn from_encoded_bytes(bytes: &'a [u8]) -> SerializationResult<(Self, usize)> {
        let mut bytes_read = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if i >= MAX_VARINT_LEN {
                return Err(SerializationError::InvalidVarIntPrefix);
            };
            bytes_read += 1;
            if (b & 0x80) == 0 {
                return Ok((VarInt(&bytes[..bytes_read]), bytes_read));
            }
        }
        Err(SerializationError::InvalidVarIntPrefix)
    }

    /// Create a VarInt from encoded bytes (unchecked variant)
    pub(crate) fn from_encoded_bytes_unchecked(bytes: &'a [u8]) -> (Self, usize) {
        let mut bytes_read = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if i >= MAX_VARINT_LEN {
                panic!("Invalid VarInt prefix");
            };
            bytes_read += 1;
            if (b & 0x80) == 0 {
                return (VarInt(&bytes[..bytes_read]), bytes_read);
            }
        }
        panic!("Invalid VarInt prefix");
    }

    /// Returns the encoded size for a value.
    pub(crate) fn encoded_size(value: i64) -> usize {
        let mut v = Self::encode_zigzag(value);
        let mut size = 0;
        loop {
            size += 1;
            v >>= 7;
            if v == 0 {
                break;
            }
        }
        size
    }

    pub(crate) fn read_buf<R: Read>(reader: &mut R) -> SerializationResult<Box<[u8]>> {
        let mut buf = [0u8; 1];
        let mut bytes = Vec::new();

        for _ in 0..MAX_VARINT_LEN {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                return Err(SerializationError::UnexpectedEof);
            }

            let b = buf[0];
            bytes.push(b);

            if (b & 0x80) == 0 {
                return Ok(bytes.into_boxed_slice());
            }
        }

        Err(SerializationError::InvalidVarIntPrefix)
    }

    /// Decode the value from the stored bytes
    pub(crate) fn value(&self) -> i64 {
        let mut result: u64 = 0;
        let mut shift = 0;

        for &b in self.0.iter() {
            result |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Self::decode_zigzag(result);
            }
            shift += 7;
        }
        unreachable!("Invalid varint bytes")
    }

    /// Returns the number of bytes this varint occupies
    pub(crate) fn size(&self) -> usize {
        self.0.len()
    }

    /// Get the raw encoded bytes
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0
    }

    /// Convert from i64 to u64 using ZigZag encoding
    #[inline]
    pub(crate) fn encode_zigzag(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }

    /// Convert from u64 to i64 using ZigZag decoding
    #[inline]
    pub(crate) fn decode_zigzag(value: u64) -> i64 {
        ((value >> 1) as i64) ^ (-((value & 1) as i64))
    }

    /// Encode an i64 value to bytes
    pub(crate) fn encode(value: i64, buffer: &mut [u8; MAX_VARINT_LEN]) -> &[u8] {
        let mut encoded = Self::encode_zigzag(value);
        let mut idx = 0;

        loop {
            let mut byte = (encoded & 0x7F) as u8;
            encoded >>= 7;
            if encoded != 0 {
                byte |= 0x80;
            }
            buffer[idx] = byte;
            idx += 1;
            if encoded == 0 {
                break;
            }
        }

        &buffer[..idx]
    }
}

/// Conversions between VarInt and the different signed and unsigned integer primitive types.
///
/// Note that VarInt is a signed integer type, so the conversion will be lossy if you cast to an unsigned integer.
///
/// Converts a varint value to a [isize].
impl<'a> From<VarInt<'a>> for isize {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as isize
    }
}

/// Converts a varint value to a [i32].
impl<'a> From<VarInt<'a>> for i32 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as i32
    }
}

/// Converts a varint value to a [i64].
impl<'a> From<VarInt<'a>> for i64 {
    fn from(value: VarInt<'a>) -> Self {
        value.value()
    }
}

/// Converts a varint value to a [i16].
impl<'a> From<VarInt<'a>> for i16 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as i16
    }
}

/// Converts a varint value to a [i8].
impl<'a> From<VarInt<'a>> for i8 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as i8
    }
}

/// Converts a varint value to a [usize].
impl<'a> From<VarInt<'a>> for usize {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as usize
    }
}

/// Converts a varint value to a [u32].
impl<'a> From<VarInt<'a>> for u32 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as u32
    }
}

/// Converts a varint value to a [u64].
impl<'a> From<VarInt<'a>> for u64 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as u64
    }
}

/// Converts a varint value to a [u16].
impl<'a> From<VarInt<'a>> for u16 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as u16
    }
}

/// Converts a varint value to a [u8].
impl<'a> From<VarInt<'a>> for u8 {
    fn from(value: VarInt<'a>) -> Self {
        value.value() as u8
    }
}

impl<'a> PartialOrd for VarInt<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for VarInt<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

impl<'a> Display for VarInt<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "VarInt: {}", self.value())
    }
}
