//! # VarInt Module
//!
//! This module implements encoding and decoding of variable-length integers
//! (varint) used in the SQLite file format.
//!
//!
//! Varint is a method of encoding integers using a variable number of bytes.
//! It is commonly used in databases and serialization formats to save space.
//!

use std::io::{self, Read, Write};

/// MAX SIZE FOR A VARINT. A varint occupies 8 bytes for values up to 2^56 - 1.
/// For values larger than this, it occupies 9 bytes.
/// The maximum value that can be represented in 8 bytes is 2^56 - 1.
/// The 9th byte is used to store the sign bit of the 64-bit integer.
/// The maximum value that can be represented in 9 bytes is 2^64 - 1.
pub const MAX_VARINT_SIZE: usize = 9;

/// Encodes a signed 64-bit integer as a varint.
///
/// # Parameters
/// * `value` - The signed integer to encode.
/// * `writer` - The destination where the encoded bytes will be written (must implement `Write`).
///
/// # Errors
/// Returns an error if there are issues writing to the destination.
///
/// # Returns
/// The number of bytes written to the destination.
#[allow(unused_assignments)]
pub fn encode_varint<W: Write>(value: i64, writer: &mut W) -> io::Result<usize> {
    let mut uvalue = value as u64;

    // Initialize the number of bytes written
    // to 0. This will be updated as we write bytes.
    let mut bytes_written = 0;

    // For small values (1 byte)
    // If the value is less than or equal to 127, we can write it directly.
    // The first bit is the sign bit, so we can use the lower 7 bits.
    if uvalue <= 0x7F {
        writer.write_all(&[uvalue as u8])?;
        return Ok(1);
    }

    // For larger values, we need to encode them in multiple bytes.
    // THis is the case for values that are larger than 127, but can be fit in 8 bytes.
    // We will use the first 7 bits of each byte to store the value, and the 8th bit
    // as a continuation bit. The continuation bit is set to 1 if there are more bytes
    // to read, and 0 if this is the last byte.
    if uvalue <= 0xFFFFFFFFFFFFFF {
        let mut buffer = [0u8; 8];
        let mut i = 0;

        // Encode the value in 7-bit chunks
        // The first 7 bits are stored in the first byte, the next 7 bits in the second byte, and so on.
        // The last byte will not have the continuation bit set.
        while uvalue >= 0x80 {
            // While there are more than 7 bits to encode
            // Set the continuation bit (0x80) and store the lower 7 bits
            // The lower 7 bits are obtained by ANDing with 0x7F (127 in decimal). (01111111)
            // The continuation bit is set by ORing with 0x80 (128 in decimal). (10000000)
            // The value is then shifted right by 7 bits to process the next chunk.
            // The first byte will have the continuation bit set, and the lower 7 bits of the value.
            buffer[i] = 0x80 | (uvalue & 0x7F) as u8;
            uvalue >>= 7;
            i += 1;
        }

        // The last byte will not have the continuation bit set.
        buffer[i] = uvalue as u8;

        // Write the buffer to the destination.
        writer.write_all(&buffer[0..=i])?;
        bytes_written = i + 1;
    } else {
        // For values larger than 8 bytes, we need to use 9 bytes.
        // The first 8 bytes will have the continuation bit set, and the last byte will not.
        // The last byte will contain the sign bit of the 64-bit integer.
        let mut buffer = [0u8; 9];

        // The first 8 bytes will have the continuation bit set.
        // The first 7 bits are stored in the first byte, the next 7 bits in the second byte, and so on.
        for i in 0..8 {
            buffer[i] = 0x80 | ((uvalue >> (7 * i)) & 0x7F) as u8;
        }

        // The last byte will not have the continuation bit set.
        buffer[8] = (uvalue >> 56) as u8;

        writer.write_all(&buffer)?;
        bytes_written = 9;
    }

    Ok(bytes_written)
}

/// Decodes a varint from a source. The source must implement the `Read` trait.
/// # Parameters
/// * `reader` - The source from which the varint will be read.
///
/// # Returns
/// A tuple containing the decoded signed integer and the number of bytes read.
///
/// # Errors
/// Returns an error if the varint is invalid or if there are issues reading from the source.
///
pub fn decode_varint<R: Read>(reader: &mut R) -> io::Result<(i64, usize)> {
    // Initialize the result to 0, the shift to 0, and the number of bytes read to 0.
    // The result will be a 64-bit unsigned integer, but we will return it as a signed integer.
    // The shift will be used to shift the bits to the correct position.
    // The number of bytes read will be used to keep track of how many bytes we have read.
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut bytes_read = 0;

    for _ in 0..MAX_VARINT_SIZE {
        // Till 9 bytes
        // Read the first byte from the source.
        // The first byte will have the continuation bit set if there are more bytes to read.
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        bytes_read += 1;

        // Extract the lower 7 bits from the byte and shift them to the correct position.
        // The lower 7 bits are obtained by ANDing with 0x7F (127 in decimal).
        // Then we shift the value to the left by the number of bits we have already read.

        let value = (byte[0] & 0x7F) as u64;
        result |= value << shift;

        // If the continuation bit is not set, we can stop reading.
        // The continuation bit is the 8th bit of the byte, which is obtained by ANDing with 0x80 (128 in decimal).
        if byte[0] & 0x80 == 0 {
            return Ok((result as i64, bytes_read));
        }

        // If the continuation bit is set, we need to read the next byte.
        // We shift the value to the left by 7 bits to make room for the next byte.
        shift += 7;

        // For the ninth byte, we need to check if it is the last byte.
        // The last byte will not have the continuation bit set, and it will contain the sign bit of the 64-bit integer.
        // The last byte will be shifted 56 bits (8 bytes * 7 bits) to the left.
        if bytes_read == 8 {
            let mut last_byte = [0u8; 1];
            reader.read_exact(&mut last_byte)?;
            bytes_read += 1;

            // Shift the last byte to the left by 56 bits and add it to the result.
            result |= (last_byte[0] as u64) << 56;
            return Ok((result as i64, bytes_read));
        }
    }

    // Return an error if we have read 9 bytes and the continuation bit is still set.
    // This means that the varint is invalid or too long.
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Too many bytes read for varint",
    ))
}

/// Helpers to calculate the size of a varint.
/// This function calculates the size of a varint for a given signed integer.
/// The size is determined by the number of bytes needed to encode the integer.
/// # Parameters
/// * `value` - The signed integer to calculate the size for.
///
/// # Returns
/// The size of the varint in bytes.

pub fn varint_size(value: i64) -> usize {
    let uvalue = value as u64;

    if uvalue <= 0x7F {
        1
    } else if uvalue <= 0x3FFF {
        2
    } else if uvalue <= 0x1FFFFF {
        3
    } else if uvalue <= 0xFFFFFFF {
        4
    } else if uvalue <= 0x7FFFFFFFF {
        5
    } else if uvalue <= 0x3FFFFFFFFFF {
        6
    } else if uvalue <= 0x1FFFFFFFFFFFF {
        7
    } else if uvalue <= 0xFFFFFFFFFFFFFF {
        8
    } else {
        9
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varint_encode_decode_small() {
        // Small values (1 byte)
        // These values can be encoded in a single byte.
        let test_values = [0, 1, 42, 127];

        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();

            assert_eq!(bytes_written, 1);
            assert_eq!(buffer.len(), 1);

            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();

            assert_eq!(decoded, value);
            assert_eq!(bytes_read, 1);
        }
    }

    #[test]
    fn test_varint_encode_decode_medium() {
        // Median values (2-3 bytes)
        let test_values = [128, 255, 256, 16383, 16384, 65535, 65536];

        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();

            assert!(bytes_written > 1);
            assert_eq!(buffer.len(), bytes_written);

            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();

            assert_eq!(decoded, value);
            assert_eq!(bytes_read, bytes_written);
        }
    }

    #[test]
    fn test_varint_encode_decode_large() {
        // BIg values (4-9 bytes)
        let test_values = [
            2_i64.pow(32) - 1, // Maximum u32 value
            2_i64.pow(32),     // MInimu 5 byte value
            2_i64.pow(56) - 1, // Maximum u56 value
            2_i64.pow(56),     // Mimimum 9 byte value
            i64::MAX,          // Maximum i64 value
        ];

        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();

            let expected_size = varint_size(value);
            assert_eq!(bytes_written, expected_size);
            assert_eq!(buffer.len(), expected_size);

            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();

            assert_eq!(decoded, value);
            assert_eq!(bytes_read, expected_size);
        }
    }

    #[test]
    fn test_varint_size() {
        // Test the varint_size function
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16383), 2);
        assert_eq!(varint_size(16384), 3);
        assert_eq!(varint_size(2097151), 3);
        assert_eq!(varint_size(2097152), 4);
        assert_eq!(varint_size(268435455), 4);
        assert_eq!(varint_size(268435456), 5);
        assert_eq!(varint_size(34359738367), 5);
        assert_eq!(varint_size(34359738368), 6);
        assert_eq!(varint_size(4398046511103), 6);
        assert_eq!(varint_size(4398046511104), 7);
        assert_eq!(varint_size(562949953421311), 7);
        assert_eq!(varint_size(562949953421312), 8);
        assert_eq!(varint_size(72057594037927935), 8);
        assert_eq!(varint_size(72057594037927936), 9);
        assert_eq!(varint_size(i64::MAX), 9);
    }
}
