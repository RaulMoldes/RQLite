use super::{Comparator, Ranger};
use crate::{schema::stats::Selectivity, tree::cell_ops::KeyBytes};
use std::{cmp::Ordering, io, usize};

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
    fn compare(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Ordering> {
        Ok(lhs[..self.0].cmp(&rhs[..self.0]))
    }

    fn key_size(&self, _data:  &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}

impl Ranger for FixedSizeBytesComparator {
    /// Computes arithmetic difference treating bytes as big-endian.
    fn range_bytes(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Box<[u8]>> {
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

    fn selectivity_range(&self, min: KeyBytes<'_>, max: KeyBytes<'_>) -> io::Result<Selectivity> {
        // Interpret as big-endian unsigned integers for range calculation
        let range_bytes = self.range_bytes(min, max)?;

        // Convert range to u128 for calculation (supports up to 16 bytes)
        let mut range_val: u128 = 0;
        for &byte in range_bytes.iter() {
            range_val = (range_val << 8) | byte as u128;
        }

        // Max possible range: 2^(size*8) - 1
        let max_possible: u128 = if self.0 >= 16 {
            u128::MAX
        } else {
            (1u128 << (self.0 * 8)) - 1
        };

        if max_possible == 0 {
            return Ok(Selectivity::Uncomputable);
        }

        let selectivity = range_val as f64 / max_possible as f64;
        Ok(Selectivity::from(selectivity))
    }
}
