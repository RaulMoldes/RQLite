use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;

pub const MAX_VARINT_LEN: usize = 9;

// VarInt stands for variable-length integer.
//
// It is a special type that is encoded using 8 bytes, and depending on the value, will occupy more or less space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarInt<'a>(&'a [u8]);

impl<'a> VarInt<'a> {
    /// Create a VarInt from encoded bytes (borrows the bytes)
    pub fn from_encoded_bytes(bytes: &'a [u8]) -> std::io::Result<(Self, usize)> {
        // Validate and find the end of the varint
        let mut bytes_read = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if i >= MAX_VARINT_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "VarInt too long",
                ));
            }
            bytes_read += 1;
            if (b & 0x80) == 0 {
                return Ok((VarInt(&bytes[..bytes_read]), bytes_read));
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete varint",
        ))
    }

    pub fn encoded_size(value: i64) -> usize {
        let mut v = value as u64;
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

    pub fn read_buf<R: std::io::Read>(reader: &mut R) -> std::io::Result<Box<[u8]>> {
        let mut buf = [0u8; 1];
        let mut bytes = Vec::new();

        for i in 0..MAX_VARINT_LEN {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "EOF antes de completar varint",
                ));
            }

            let b = buf[0];
            bytes.push(b);

            if (b & 0x80) == 0 {
                return Ok(bytes.into_boxed_slice());
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "VarInt too long",
        ))
    }

    /// Decode the value from the stored bytes
    pub fn value(&self) -> i64 {
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
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Get the raw encoded bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0
    }

    /// Convert from i64 to u64 using ZigZag encoding
    #[inline]
    fn encode_zigzag(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }

    /// Convert from u64 to i64 using ZigZag decoding
    #[inline]
    fn decode_zigzag(value: u64) -> i64 {
        ((value >> 1) as i64) ^ (-((value & 1) as i64))
    }

    /// Encode an i64 value to bytes
    pub fn encode(value: i64, buffer: &mut [u8; MAX_VARINT_LEN]) -> &[u8] {
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

impl<'a> TryFrom<VarInt<'a>> for usize {
    type Error = std::io::Error;

    fn try_from(varint: VarInt<'a>) -> Result<Self, Self::Error> {
        let value = varint.value();
        if value < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot convert negative VarInt to usize",
            ))
        } else {
            Ok(value as usize)
        }
    }
}

impl<'a> TryFrom<VarInt<'a>> for u32 {
    type Error = std::io::Error;

    fn try_from(varint: VarInt<'a>) -> Result<Self, Self::Error> {
        let value = varint.value();
        if value < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot convert negative VarInt to u32",
            ))
        } else {
            Ok(value as u32)
        }
    }
}

impl<'a> TryFrom<VarInt<'a>> for u64 {
    type Error = std::io::Error;

    fn try_from(varint: VarInt<'a>) -> Result<Self, Self::Error> {
        let value = varint.value();
        if value < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot convert negative VarInt to u64",
            ))
        } else {
            Ok(value as u64)
        }
    }
}

impl<'a> PartialOrd for VarInt<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for VarInt<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by decoded values, not by bytes
        self.value().cmp(&other.value())
    }
}

impl<'a> fmt::Display for VarInt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VarInt: {}", self.value())
    }
}
