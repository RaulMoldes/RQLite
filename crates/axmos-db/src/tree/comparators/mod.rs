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
    usize,
};

pub(crate) mod fixed;
pub(crate) mod numeric;
pub(crate) mod varlen;

pub(crate) use varlen::VarlenComparator;

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
