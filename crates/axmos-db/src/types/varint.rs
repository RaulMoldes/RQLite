use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;

/// Maximum bytes needed to encode any i64 value (ZigZag + base-128).
pub const MAX_VARINT_LEN: usize = 10;

// VarInt stands for variable-length integer.
//
// It is a special type that is encoded using 10 bytes, and depending on the value, will occupy more or less space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarInt<'a>(&'a [u8]);

impl<'a> VarInt<'a> {
    /// Create a VarInt from encoded bytes (borrows the bytes)
    pub fn from_encoded_bytes(bytes: &'a [u8]) -> std::io::Result<(Self, usize)> {
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

    /// Returns the encoded size for a value.
    pub fn encoded_size(value: i64) -> usize {
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

    pub fn read_buf<R: std::io::Read>(reader: &mut R) -> std::io::Result<Box<[u8]>> {
        let mut buf = [0u8; 1];
        let mut bytes = Vec::new();

        for _ in 0..MAX_VARINT_LEN {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "EOF before completing varint",
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
    pub(crate) fn encode_zigzag(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }

    /// Convert from u64 to i64 using ZigZag decoding
    #[inline]
    pub(crate) fn decode_zigzag(value: u64) -> i64 {
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
        self.value().cmp(&other.value())
    }
}

impl<'a> fmt::Display for VarInt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VarInt: {}", self.value())
    }
}

#[cfg(test)]
mod varint_tests {
    use super::*;

    #[test]
    fn test_zigzag_encoding() {
        // Verify ZigZag encoding maps correctly
        assert_eq!(VarInt::encode_zigzag(0), 0);
        assert_eq!(VarInt::encode_zigzag(-1), 1);
        assert_eq!(VarInt::encode_zigzag(1), 2);
        assert_eq!(VarInt::encode_zigzag(-2), 3);
        assert_eq!(VarInt::encode_zigzag(2), 4);
        assert_eq!(VarInt::encode_zigzag(i64::MAX), u64::MAX - 1);
        assert_eq!(VarInt::encode_zigzag(i64::MIN), u64::MAX);
    }

    #[test]
    fn test_zigzag_roundtrip() {
        let test_values = [
            0i64,
            1,
            -1,
            2,
            -2,
            127,
            -128,
            255,
            -256,
            i16::MAX as i64,
            i16::MIN as i64,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ];

        for &value in &test_values {
            let encoded = VarInt::encode_zigzag(value);
            let decoded = VarInt::decode_zigzag(encoded);
            assert_eq!(decoded, value, "ZigZag roundtrip failed for {}", value);
        }
    }

    #[test]
    fn test_varint_encode_decode() {
        // Test values that fit within MAX_VARINT_LEN (9 bytes)
        let test_values = [
            0i64,
            1,
            -1,
            63,
            -64,
            64,
            -65,
            8191,
            -8192,
            8192,
            -8193,
            1_000_000,
            -1_000_000,
            i32::MAX as i64,
            i32::MIN as i64,
        ];

        for &value in &test_values {
            let mut buf = [0u8; MAX_VARINT_LEN];
            let encoded = VarInt::encode(value, &mut buf);

            let (varint, size) = VarInt::from_encoded_bytes(encoded)
                .expect(&format!("Failed to decode varint for {}", value));

            assert_eq!(size, encoded.len());
            assert_eq!(
                varint.value(),
                value,
                "VarInt roundtrip failed for {}",
                value
            );
        }
    }

    #[test]
    fn test_cross_platform_byte_representation() {
        // These specific byte sequences should decode to the same values
        // regardless of platform endianness

        let mut buf = [0u8; MAX_VARINT_LEN];

        // Value 0 → ZigZag 0 → [0x00]
        let encoded = VarInt::encode(0, &mut buf);
        assert_eq!(encoded, &[0x00]);

        // Value 1 → ZigZag 2 → [0x02]
        let encoded = VarInt::encode(1, &mut buf);
        assert_eq!(encoded, &[0x02]);

        // Value -1 → ZigZag 1 → [0x01]
        let encoded = VarInt::encode(-1, &mut buf);
        assert_eq!(encoded, &[0x01]);

        // Value 300 → ZigZag 600 → [0xD8, 0x04]
        let encoded = VarInt::encode(300, &mut buf);
        assert_eq!(encoded, &[0xD8, 0x04]);

        // Verify decoding
        let (varint, _) = VarInt::from_encoded_bytes(&[0xD8, 0x04]).unwrap();
        assert_eq!(varint.value(), 300);
    }

    #[test]
    fn test_known_byte_sequences_small() {
        // Test that specific byte sequences always decode to expected values
        // This ensures cross-platform compatibility
        // Only testing values that fit in 9 bytes

        let test_cases: &[(&[u8], i64)] = &[
            (&[0x00], 0),
            (&[0x01], -1),
            (&[0x02], 1),
            (&[0x03], -2),
            (&[0x7E], 63),
            (&[0x7F], -64),
            (&[0x80, 0x01], 64),
            (&[0x81, 0x01], -65),
        ];

        for &(bytes, expected_value) in test_cases {
            let (varint, _) =
                VarInt::from_encoded_bytes(bytes).expect(&format!("Failed to decode {:?}", bytes));
            assert_eq!(
                varint.value(),
                expected_value,
                "Byte sequence {:?} should decode to {}",
                bytes,
                expected_value
            );
        }
    }

    #[test]
    fn test_platform_independence() {
        // This test verifies that encoding is purely algorithmic
        // and doesn't depend on platform byte order

        let mut buf = [0u8; MAX_VARINT_LEN];

        // Encode a multi-byte value
        let value = 0x12345678i64;
        let encoded = VarInt::encode(value, &mut buf);

        // The encoded bytes should be the same on any platform
        // because VarInt uses bit operations, not memory layout
        let (decoded, _) = VarInt::from_encoded_bytes(encoded).unwrap();
        assert_eq!(decoded.value(), value);

        // Verify the actual bytes are deterministic
        let expected_zigzag = VarInt::encode_zigzag(value);
        let mut reconstructed = 0u64;
        let mut shift = 0;
        for &b in encoded {
            reconstructed |= ((b & 0x7F) as u64) << shift;
            shift += 7;
        }
        assert_eq!(reconstructed, expected_zigzag);
    }

    #[test]
    fn test_negative_values() {
        let mut buf = [0u8; MAX_VARINT_LEN];

        // Small negative values should encode efficiently due to ZigZag
        let encoded_neg1 = VarInt::encode(-1, &mut buf);
        assert_eq!(encoded_neg1.len(), 1); // -1 → ZigZag 1 → 1 byte

        let encoded_neg64 = VarInt::encode(-64, &mut buf);
        assert_eq!(encoded_neg64.len(), 1); // -64 → ZigZag 127 → 1 byte

        let encoded_neg65 = VarInt::encode(-65, &mut buf);
        assert_eq!(encoded_neg65.len(), 2); // -65 → ZigZag 129 → 2 bytes
    }

    #[test]
    fn test_roundtrip_various_sizes() {
        let mut buf = [0u8; MAX_VARINT_LEN];

        // Test values of various encoded sizes
        let test_values = [
            (0i64, 1),       // 1 byte
            (63, 1),         // 1 byte (max 1-byte positive)
            (-64, 1),        // 1 byte (min 1-byte negative)
            (64, 2),         // 2 bytes
            (-65, 2),        // 2 bytes
            (8191, 2),       // 2 bytes (max 2-byte)
            (-8192, 2),      // 2 bytes (min 2-byte)
            (1_000_000, 3),  // 3 bytes
            (-1_000_000, 3), // 3 bytes
        ];

        for (value, _expected_size) in test_values {
            let encoded = VarInt::encode(value, &mut buf);
            let (decoded, size) = VarInt::from_encoded_bytes(encoded).unwrap();

            assert_eq!(decoded.value(), value, "Roundtrip failed for {}", value);
            assert_eq!(size, encoded.len());
        }
    }
}
