//! Comparators for B+ tree key ordering.
//!
//! This module provides comparators that handle platform-native byte order
//! for numeric types. Since types are serialized using the platform's native
//! endianness (via `bytemuck_slice!`), comparators must detect and dispatch to the
//! appropriate comparison logic.
//!
//! We provide the following comparators:
//!
//! - [`NumericComparator`]: Compares numeric types stored in **native** byte order
//! - [`FixedSizeBytesComparator`]: Lexicographic comparison (byte-by-byte)
//! - [`VarlenComparator`]: Variable-length with VarInt prefix, lexicographic data

use crate::{
    schema::stats::Selectivity, tree::cell_ops::KeyBytes, types::VarInt, varint::MAX_VARINT_LEN,
};

use std::{
    cmp::Ordering,
    io::{self, Error as IoError, ErrorKind},
    usize,
};

pub(crate) mod fixed;
pub(crate) mod numeric;
pub(crate) mod varlen;

pub(crate) use numeric::{NumericComparator, SignedNumericComparator};
pub(crate) use varlen::VarlenComparator;

pub(crate) use fixed::FixedSizeBytesComparator;

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
    fn range_bytes(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Box<[u8]>>;

    /// Selectivity range
    ///
    /// From a value range, represented by two byte values, return the estimated selectivity
    fn selectivity_range(&self, min: KeyBytes<'_>, max: KeyBytes<'_>) -> io::Result<Selectivity> {
        return Ok(Selectivity::Uncomputable);
    }
}

pub(crate) trait Comparator {
    /// Compares two keys.
    fn compare(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Ordering>;

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
    fn compare(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Ordering> {
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
    fn range_bytes(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Box<[u8]>> {
        match self {
            Self::FixedSizeBytes(c) => c.range_bytes(lhs, rhs),
            Self::Variable(c) => c.range_bytes(lhs, rhs),
            Self::StrictNumeric(c) => c.range_bytes(lhs, rhs),
            Self::SignedNumeric(c) => c.range_bytes(lhs, rhs),
            Self::Composite(c) => c.range_bytes(lhs, rhs),
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

    /// Returns the number of key columns.
    pub(crate) fn num_keys(&self) -> usize {
        self.comparators.len()
    }
}

impl Comparator for CompositeComparator {
    fn compare(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Ordering> {
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = KeyBytes::from(&lhs[lhs_offset..]);
            let rhs_key = KeyBytes::from(&rhs[rhs_offset..]);

            match comparator.compare(lhs_key, rhs_key)? {
                Ordering::Equal => {
                    // Move to next column
                    lhs_offset += comparator.key_size(&lhs_key)?;
                    rhs_offset += comparator.key_size(&rhs_key)?;
                }
                other => return Ok(other),
            }
        }

        Ok(Ordering::Equal)
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        let mut offset = 0usize;

        for comparator in &self.comparators {
            let key_data = KeyBytes::from(&data[offset..]);
            offset += comparator.key_size(&key_data)?;
        }

        Ok(offset)
    }

    fn is_fixed_size(&self) -> bool {
        self.comparators.iter().all(|c| c.is_fixed_size())
    }
}

impl Ranger for CompositeComparator {
    fn range_bytes(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Box<[u8]>> {
        // For composite keys, we compute the range based on the first differing column
        // This is useful for range scans where we want to estimate distance
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = KeyBytes::from(&lhs[lhs_offset..]);
            let rhs_key = KeyBytes::from(&rhs[rhs_offset..]);

            let lhs_size = comparator.key_size(&lhs_key)?;
            let rhs_size = comparator.key_size(&rhs_key)?;

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
