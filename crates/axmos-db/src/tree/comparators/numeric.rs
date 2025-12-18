use std::{cmp::Ordering, io, mem, usize};

use super::{Comparator, IS_LITTLE_ENDIAN, Ranger};
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
