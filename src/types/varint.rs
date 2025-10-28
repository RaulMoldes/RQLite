use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;

const MAX_VARINT_LEN: usize = 9;

// VarInt stands for variable-length integer.
//
// It is a special type that is encoded using 8 bytes, and depending on the value, will occupy more or less space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarInt(pub i64);

impl TryFrom<VarInt> for usize {
    type Error = &'static str;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative VarInt to usize")
        } else {
            Ok(value.0 as usize)
        }
    }
}

impl VarInt {
    /// Returns the number of bytes this varint will occupy when serialized.
    pub fn size(&self) -> usize {
        let mut value = Self::encode_zigzag(self.0);
        let mut size = 1;
        while value >= 0x80 {
            value >>= 7;
            size += 1;
        }
        size
    }
}

impl TryFrom<VarInt> for u32 {
    type Error = &'static str;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative VarInt to usize")
        } else {
            Ok(value.0 as u32)
        }
    }
}

impl TryFrom<VarInt> for u64 {
    type Error = &'static str;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative VarInt to usize")
        } else {
            Ok(value.0 as u64)
        }
    }
}

impl VarInt {
    /// Convert from i64 to u64 using ZigZag encoding (https://docs.rs/residua-zigzag/latest/zigzag/)
    #[inline]
    fn encode_zigzag(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }

    /// Convert from u64 to i64 using ZigZag decoding
    #[inline]
    fn decode_zigzag(value: u64) -> i64 {
        ((value >> 1) as i64) ^ (-((value & 1) as i64))
    }
}

impl PartialOrd for VarInt {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VarInt {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "VarInt: {}", self.0)?;
        Ok(())
    }
}

impl VarInt {
    /// Serializes a VarInt to a byte vector.
    pub fn to_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MAX_VARINT_LEN);
        let mut value = Self::encode_zigzag(self.0);

        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // Set continuation bit
            }
            bytes.push(byte);
            if value == 0 {
                break;
            }
        }
        bytes
    }

    /// Deserializes a [VarInt] from an slice of bytes
    pub fn from_bytes(bytes: &[u8]) -> std::io::Result<(Self, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut bytes_read = 0;

        for (i, &b) in bytes.iter().enumerate() {
            if i >= MAX_VARINT_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "VarInt too long",
                ));
            }

            result |= ((b & 0x7F) as u64) << shift;
            bytes_read += 1;

            if (b & 0x80) == 0 {
                return Ok((VarInt(Self::decode_zigzag(result)), bytes_read));
            }

            shift += 7;
            if shift >= 64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "VarInt shift overflow",
                ));
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete varint",
        ))
    }
}

impl From<VarInt> for VarIntBytes {
    fn from(v: VarInt) -> VarIntBytes {
        VarIntBytes(v.to_bytes())
    }
}

impl TryFrom<&[u8]> for VarInt {
    type Error = std::io::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (varint, _) = VarInt::from_bytes(bytes)?;
        Ok(varint)
    }
}

pub struct VarIntBytes(Vec<u8>);

impl AsRef<[u8]> for VarIntBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<&VarInt> for VarIntBytes {
    fn from(v: &VarInt) -> Self {
        VarIntBytes(v.to_bytes())
    }
}

crate::impl_arithmetic_ops!(VarInt);
