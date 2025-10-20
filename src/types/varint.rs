use crate::serialization::Serializable;
use crate::types::{DataType, DataTypeMarker};
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::io::{self, Read, Write};
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

impl DataType for Varint {
    /// Returns the RQLite type Varint
    fn _type_of(&self) -> DataTypeMarker {
        DataTypeMarker::VarInt
    }

    // Computes the serialized size of a Varint.
    fn size_of(&self) -> u16 {
        let mut value = Self::encode_zigzag(self.0);
        let mut size = 0;

        loop {
            size += 1;
            value >>= 7;
            if value == 0 {
                break;
            }
        }

        size
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

impl Serializable for Varint {
    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        // ZigZag-encode
        let mut value = Self::encode_zigzag(self.0);

        // Write byte by byte adding the continuation bit
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;

            if value != 0 {
                byte |= 0x80; // Set continuation bit
            }

            writer.write_all(&[byte])?;

            if value == 0 {
                break;
            }
        }

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut result: u64 = 0;
        let mut shift = 0;

        for _ in 0..MAX_VARINT_LEN {
            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte)?;
            let b = byte[0];

            // Extract the first 7 bits of data
            result |= ((b & 0x7F) as u64) << shift;

            // If there is no continuation bit, terminate
            if (b & 0x80) == 0 {
                // Decode using ZigZag decoding
                return Ok(Varint(Self::decode_zigzag(result)));
            }

            shift += 7;

            // Prevent overflow
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Varint shift overflow",
                ));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Varint too long",
        ))
    }
}

crate::impl_arithmetic_ops!(Varint);
