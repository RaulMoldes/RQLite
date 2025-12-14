//! Comparators for B+ tree key ordering.
//!
//! This module provides comparators that handle platform-native byte order
//! for numeric types. Since types are serialized using the platform's native
//! endianness (via `as_slice!`), comparators must detect and dispatch to the
//! appropriate comparison logic.
//!
//! We provide the following comparators:
//!
//! - [`NumericComparator`]: Compares numeric types stored in **native** byte order
//! - [`FixedSizeBytesComparator`]: Lexicographic comparison (byte-by-byte)
//! - [`VarlenComparator`]: Variable-length with VarInt prefix, lexicographic data

use crate::{types::VarInt, varint::MAX_VARINT_LEN};

use std::{
    cmp::Ordering,
    io::{self, Error as IoError, ErrorKind},
    mem, usize,
};

/// Detects if the platform is little-endian at compile time.
#[cfg(target_endian = "little")]
const IS_LITTLE_ENDIAN: bool = true;

#[cfg(target_endian = "big")]
const IS_LITTLE_ENDIAN: bool = false;

/// Subtracts two variable length byte keys.
/// Returns the `diff` in bytes with VarInt length prefix.
/// Assumes that lhs > rhs (lexicographically).
pub(crate) fn subtract_bytes(lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
    let (longer, shorter) = if lhs.len() >= rhs.len() {
        (lhs, rhs)
    } else {
        (rhs, lhs)
    };

    let max_len = longer.len();
    let mut buf = [0; MAX_VARINT_LEN];
    let max_len_varint = VarInt::encode(max_len as i64, &mut buf);

    let total_len = max_len + max_len_varint.len();
    let mut result: Vec<u8> = vec![0u8; total_len];
    result[0..max_len_varint.len()].copy_from_slice(max_len_varint);

    let mut borrow = 0;

    for i in (0..max_len).rev() {
        let lhs_byte = longer[i] as i16;
        let rhs_byte = if i >= max_len - shorter.len() {
            shorter[i - (max_len - shorter.len())] as i16
        } else {
            0
        };

        let mut diff = lhs_byte - rhs_byte - borrow;
        if diff < 0 {
            diff += 256;
            borrow = 1;
        } else {
            borrow = 0;
        }

        result[max_len_varint.len() + i] = diff as u8;
    }

    if borrow != 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "lhs must be greater than rhs",
        ));
    }

    // Remove leading zeros from the data section
    let data_start = max_len_varint.len();
    let mut first_non_zero = 0;

    while first_non_zero < max_len && result[data_start + first_non_zero] == 0 {
        first_non_zero += 1;
    }

    if first_non_zero == max_len {
        first_non_zero = max_len - 1;
    }

    let data_len = max_len - first_non_zero;

    if first_non_zero > 0 {
        let mut new_buf = [0; MAX_VARINT_LEN];
        let new_len_varint = VarInt::encode(data_len as i64, &mut new_buf);
        let new_varint_len = new_len_varint.len();
        let new_total_len = new_varint_len + data_len;

        result.copy_within(
            data_start + first_non_zero..data_start + max_len,
            new_varint_len,
        );

        result[0..new_varint_len].copy_from_slice(new_len_varint);
        result.truncate(new_total_len);
    }

    Ok(result.into_boxed_slice())
}

pub(crate) trait Ranger: Comparator {
    /// Computes the byte difference between two keys.
    /// Returns `|lhs - rhs|` in a format comparable by this comparator.
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>>;
}

pub(crate) trait Comparator {
    /// Compares two keys.
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering>;

    /// Returns the size of a key from its byte representation.
    fn key_size(&self, data: &[u8]) -> io::Result<usize>;

    /// Returns true if keys have a fixed size.
    fn is_fixed_size(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DynComparator {
    Variable(VarlenComparator),
    StrictNumeric(NumericComparator),
    FixedSizeBytes(FixedSizeBytesComparator),
    SignedNumeric(SignedNumericComparator),
    Composite(CompositeComparator),
}

impl Comparator for DynComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        match self {
            Self::FixedSizeBytes(c) => c.compare(lhs, rhs),
            Self::Variable(c) => c.compare(lhs, rhs),
            Self::StrictNumeric(c) => c.compare(lhs, rhs),
            Self::SignedNumeric(c) => c.compare(lhs, rhs),
            Self::Composite(c) => c.compare(lhs, rhs),
        }
    }

    fn is_fixed_size(&self) -> bool {
        match self {
            Self::FixedSizeBytes(c) => c.is_fixed_size(),
            Self::Variable(c) => c.is_fixed_size(),
            Self::StrictNumeric(c) => c.is_fixed_size(),
            Self::SignedNumeric(c) => c.is_fixed_size(),
            Self::Composite(c) => c.is_fixed_size(),
        }
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        match self {
            Self::FixedSizeBytes(c) => c.key_size(data),
            Self::Variable(c) => c.key_size(data),
            Self::StrictNumeric(c) => c.key_size(data),
            Self::SignedNumeric(c) => c.key_size(data),
            Self::Composite(c) => c.key_size(data),
        }
    }
}

impl Ranger for DynComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        match self {
            Self::FixedSizeBytes(c) => c.range_bytes(lhs, rhs),
            Self::Variable(c) => c.range_bytes(lhs, rhs),
            Self::StrictNumeric(c) => c.range_bytes(lhs, rhs),
            Self::SignedNumeric(c) => c.range_bytes(lhs, rhs),
            Self::Composite(c) => c.range_bytes(lhs, rhs),
        }
    }
}

/// Comparator for numeric types stored in platform-native byte order.
///
/// This comparator reconstructs numeric values from bytes before comparison,
/// automatically handling the platform's endianness.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NumericComparator(usize);

impl NumericComparator {
    pub(crate) fn with_type<T>() -> Self {
        Self(mem::size_of::<T>())
    }

    pub(crate) fn for_size(size: usize) -> Self {
        Self(size)
    }

    /// Reads bytes as u64 using platform-native byte order.
    #[inline]
    pub fn read_native_u64(&self, bytes: &[u8]) -> u64 {
        let mut value: u64 = 0;

        if IS_LITTLE_ENDIAN {
            // Little-endian: LSB first
            for i in 0..self.0.min(bytes.len()) {
                value |= (bytes[i] as u64) << (8 * i);
            }
        } else {
            // Big-endian: MSB first
            for i in 0..self.0.min(bytes.len()) {
                value |= (bytes[i] as u64) << (8 * (self.0 - 1 - i));
            }
        }

        value
    }

    /// Writes u64 to bytes using platform-native byte order.
    #[inline]
    fn write_native_u64(&self, value: u64, buf: &mut [u8]) {
        if IS_LITTLE_ENDIAN {
            for i in 0..self.0 {
                buf[i] = ((value >> (8 * i)) & 0xFF) as u8;
            }
        } else {
            for i in 0..self.0 {
                buf[i] = ((value >> (8 * (self.0 - 1 - i))) & 0xFF) as u8;
            }
        }
    }
}

impl Comparator for NumericComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        let a = self.read_native_u64(lhs);
        let b = self.read_native_u64(rhs);
        Ok(a.cmp(&b))
    }

    fn key_size(&self, _data: &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}

impl Ranger for NumericComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        let a = self.read_native_u64(lhs);
        let b = self.read_native_u64(rhs);

        let (greater, lesser) = if a >= b { (a, b) } else { (b, a) };
        let diff = greater - lesser;

        let mut diff_bytes = vec![0u8; self.0];
        self.write_native_u64(diff, &mut diff_bytes);

        Ok(diff_bytes.into_boxed_slice())
    }
}

/// Comparator for signed numeric types stored in platform-native byte order.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SignedNumericComparator(usize);

impl SignedNumericComparator {
    pub(crate) fn with_type<T>() -> Self {
        Self(mem::size_of::<T>())
    }

    pub(crate) fn for_size(size: usize) -> Self {
        Self(size)
    }

    /// Reads bytes as i64 using platform-native byte order.
    #[inline]
    fn read_native_i64(&self, bytes: &[u8]) -> i64 {
        let mut value: u64 = 0;
        let size = self.0.min(8).min(bytes.len());

        if IS_LITTLE_ENDIAN {
            for i in 0..size {
                value |= (bytes[i] as u64) << (8 * i);
            }
        } else {
            for i in 0..size {
                value |= (bytes[i] as u64) << (8 * (size - 1 - i));
            }
        }

        // Sign extend based on actual size
        let bits = size * 8;
        if bits == 0 {
            return 0;
        }

        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            // For 8-byte values, no extension needed - just reinterpret
            if bits >= 64 {
                value as i64
            } else {
                // Extend sign bits for smaller types
                let mask = !((1u64 << bits) - 1);
                (value | mask) as i64
            }
        } else {
            value as i64
        }
    }

    /// Writes i64 to bytes using platform-native byte order.
    #[inline]
    fn write_native_i64(&self, value: i64, buf: &mut [u8]) {
        let uvalue = value as u64;
        if IS_LITTLE_ENDIAN {
            for i in 0..self.0 {
                buf[i] = ((uvalue >> (8 * i)) & 0xFF) as u8;
            }
        } else {
            for i in 0..self.0 {
                buf[i] = ((uvalue >> (8 * (self.0 - 1 - i))) & 0xFF) as u8;
            }
        }
    }
}

impl Comparator for SignedNumericComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        let a = self.read_native_i64(lhs);
        let b = self.read_native_i64(rhs);
        Ok(a.cmp(&b))
    }

    fn key_size(&self, _data: &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}

impl Ranger for SignedNumericComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        let a = self.read_native_i64(lhs);
        let b = self.read_native_i64(rhs);

        // For signed types, compute absolute difference
        let diff = (a - b).unsigned_abs();

        let mut diff_bytes = vec![0u8; self.0];
        // Write as unsigned (difference is always positive)
        if IS_LITTLE_ENDIAN {
            for i in 0..self.0 {
                diff_bytes[i] = ((diff >> (8 * i)) & 0xFF) as u8;
            }
        } else {
            for i in 0..self.0 {
                diff_bytes[i] = ((diff >> (8 * (self.0 - 1 - i))) & 0xFF) as u8;
            }
        }

        Ok(diff_bytes.into_boxed_slice())
    }
}

/// Comparator for fixed-size byte arrays using lexicographic ordering.
///
/// This comparator compares bytes from left to right (big-endian style),
/// which is useful for:
/// - UUIDs
/// - Fixed-length strings
/// - Any type where lexicographic order is desired
#[derive(Debug, Clone, Copy)]
pub(crate) struct FixedSizeBytesComparator(usize);

impl FixedSizeBytesComparator {
    pub(crate) fn with_type<T>() -> Self {
        Self(std::mem::size_of::<T>())
    }

    pub(crate) fn for_size(size: usize) -> Self {
        Self(size)
    }
}

impl Comparator for FixedSizeBytesComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        Ok(lhs[..self.0].cmp(&rhs[..self.0]))
    }

    fn key_size(&self, _data: &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}

impl Ranger for FixedSizeBytesComparator {
    /// Computes arithmetic difference treating bytes as big-endian.
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        // Determine greater/lesser lexicographically
        let (greater, lesser) = if lhs[..self.0] >= rhs[..self.0] {
            (&lhs[..self.0], &rhs[..self.0])
        } else {
            (&rhs[..self.0], &lhs[..self.0])
        };

        let mut result = vec![0u8; self.0];
        let mut borrow: i16 = 0;

        // Subtract right-to-left (LSB first in memory for big-endian interpretation)
        for i in (0..self.0).rev() {
            let g = greater[i] as i16;
            let l = lesser[i] as i16;
            let mut diff = g - l - borrow;

            if diff < 0 {
                diff += 256;
                borrow = 1;
            } else {
                borrow = 0;
            }

            result[i] = diff as u8;
        }

        Ok(result.into_boxed_slice())
    }
}

/// Comparator for variable-length data with VarInt length prefix.
///
/// Format: `[VarInt length][data bytes]`
/// Comparison is lexicographic on the data portion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VarlenComparator;

impl Comparator for VarlenComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        if lhs.as_ptr() == rhs.as_ptr() && lhs.len() == rhs.len() {
            return Ok(Ordering::Equal);
        }

        let (left_length, left_offset) = VarInt::from_encoded_bytes(lhs)?;
        let lhs_len: usize = left_length.try_into()?;
        let left_data = &lhs[left_offset..left_offset + lhs_len];

        let (right_length, right_offset) = VarInt::from_encoded_bytes(rhs)?;
        let rhs_len: usize = right_length.try_into()?;
        let right_data = &rhs[right_offset..right_offset + rhs_len];

        let min_len = lhs_len.min(rhs_len);

        if min_len > 8 {
            // Compare in 8-byte chunks for efficiency
            let chunk_count = min_len / 8;
            for i in 0..chunk_count {
                let lhs_chunk = &left_data[i * 8..(i + 1) * 8];
                let rhs_chunk = &right_data[i * 8..(i + 1) * 8];

                // Use big-endian for lexicographic ordering
                let lhs_u64 = u64::from_be_bytes(lhs_chunk.try_into().unwrap());
                let rhs_u64 = u64::from_be_bytes(rhs_chunk.try_into().unwrap());

                match lhs_u64.cmp(&rhs_u64) {
                    Ordering::Equal => continue,
                    other => return Ok(other),
                }
            }

            // Compare remaining bytes
            for i in (chunk_count * 8)..min_len {
                match left_data[i].cmp(&right_data[i]) {
                    Ordering::Equal => continue,
                    other => return Ok(other),
                }
            }
        } else {
            // Compare small payloads byte by byte
            for i in 0..min_len {
                match left_data[i].cmp(&right_data[i]) {
                    Ordering::Equal => continue,
                    other => return Ok(other),
                }
            }
        }

        // If all compared bytes are equal, decide by length
        Ok(lhs_len.cmp(&rhs_len))
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        let (len, offset) = VarInt::from_encoded_bytes(data)?;
        let len_usize: usize = len.try_into()?;
        Ok(offset + len_usize)
    }
}

impl Ranger for VarlenComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        let (left_length, left_offset) = VarInt::from_encoded_bytes(lhs)?;
        let lhs_len: usize = left_length.try_into()?;
        let left_data = &lhs[left_offset..left_offset + lhs_len];

        let (right_length, right_offset) = VarInt::from_encoded_bytes(rhs)?;
        let rhs_len: usize = right_length.try_into()?;
        let right_data = &rhs[right_offset..right_offset + rhs_len];

        match self.compare(lhs, rhs)? {
            Ordering::Equal => Ok(Box::from([])),
            Ordering::Greater => subtract_bytes(left_data, right_data),
            Ordering::Less => subtract_bytes(right_data, left_data),
        }
    }
}

/// Comparator for composite keys with multiple typed columns.
/// Chains individual comparators and compares column by column.
#[derive(Debug, Clone)]
pub(crate) struct CompositeComparator {
    /// Comparators for each key column, in order
    comparators: Vec<DynComparator>,
}

impl CompositeComparator {
    pub(crate) fn new(comparators: Vec<DynComparator>) -> Self {
        Self { comparators }
    }

    /// Creates a composite comparator from a schema's key columns.
    pub(crate) fn from_schema(schema: &crate::database::schema::Schema) -> Self {
        let comparators: Vec<DynComparator> =
            schema.iter_keys().map(|col| col.comparator()).collect();
        Self { comparators }
    }

    /// Returns the number of key columns.
    pub(crate) fn num_keys(&self) -> usize {
        self.comparators.len()
    }
}

impl Comparator for CompositeComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = &lhs[lhs_offset..];
            let rhs_key = &rhs[rhs_offset..];

            match comparator.compare(lhs_key, rhs_key)? {
                Ordering::Equal => {
                    // Move to next column
                    lhs_offset += comparator.key_size(lhs_key)?;
                    rhs_offset += comparator.key_size(rhs_key)?;
                }
                other => return Ok(other),
            }
        }

        Ok(Ordering::Equal)
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        let mut offset = 0usize;

        for comparator in &self.comparators {
            let key_data = &data[offset..];
            offset += comparator.key_size(key_data)?;
        }

        Ok(offset)
    }

    fn is_fixed_size(&self) -> bool {
        self.comparators.iter().all(|c| c.is_fixed_size())
    }
}

impl Ranger for CompositeComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        // For composite keys, we compute the range based on the first differing column
        // This is useful for range scans where we want to estimate distance
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = &lhs[lhs_offset..];
            let rhs_key = &rhs[rhs_offset..];

            let lhs_size = comparator.key_size(lhs_key)?;
            let rhs_size = comparator.key_size(rhs_key)?;

            match comparator.compare(lhs_key, rhs_key)? {
                Ordering::Equal => {
                    lhs_offset += lhs_size;
                    rhs_offset += rhs_size;
                }
                _ => {
                    // Return range for the first differing column
                    return comparator.range_bytes(lhs_key, rhs_key);
                }
            }
        }

        // All columns equal
        Ok(Box::from([]))
    }
}
