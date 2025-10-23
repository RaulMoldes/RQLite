
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
const MAX_VARINT_LEN: usize = 9;

// Varint stands for variable-length integer.
//
// It is a special type that is encoded using 8 bytes, and depending on the value, will occupy more or less space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Varint(pub i64);

impl TryFrom<Varint> for usize {
    type Error = &'static str;

    fn try_from(value: Varint) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative Varint to usize")
        } else {
            Ok(value.0 as usize)
        }
    }
}

impl TryFrom<Varint> for u32 {
    type Error = &'static str;

    fn try_from(value: Varint) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative Varint to usize")
        } else {
            Ok(value.0 as u32)
        }
    }
}

impl TryFrom<Varint> for u64 {
    type Error = &'static str;

    fn try_from(value: Varint) -> Result<Self, Self::Error> {
        if value.0 < 0 {
            Err("cannot convert negative Varint to usize")
        } else {
            Ok(value.0 as u64)
        }
    }
}

impl Varint {
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


impl PartialOrd for Varint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Varint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl fmt::Display for Varint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "VarInt: {}", self.0)?;
        Ok(())
    }
}


impl Varint {
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
                    "Varint too long",
                ));
            }

            result |= ((b & 0x7F) as u64) << shift;
            bytes_read += 1;

            if (b & 0x80) == 0 {
                return Ok((Varint(Self::decode_zigzag(result)), bytes_read));
            }

            shift += 7;
            if shift >= 64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Varint shift overflow",
                ));
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete varint",
        ))
    }
}


impl From<Varint> for Vec<u8> {
    fn from(v: Varint) -> Vec<u8> {
        v.to_bytes()
    }
}

impl TryFrom<&[u8]> for Varint {
    type Error = std::io::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (varint, _) = Varint::from_bytes(bytes)?;
        Ok(varint)
    }
}

crate::impl_arithmetic_ops!(Varint);
