use std::{
    cmp::Ordering,
    io,
    usize
};

use super::{Comparator, Ranger};




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
