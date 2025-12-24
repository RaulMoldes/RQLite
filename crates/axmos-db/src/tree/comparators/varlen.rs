use super::{Comparator, Ranger, subtract_bytes};
use crate::schema::stats::Selectivity;
use crate::types::VarInt;
use crate:: tree::cell_ops::KeyBytes;
use std::{cmp::Ordering, io, usize};

/// Comparator for variable-length data with VarInt length prefix.
///
/// Format: `[VarInt length][data bytes]`
/// Comparison is lexicographic on the data portion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VarlenComparator;

impl Comparator for VarlenComparator {
    fn compare(&self, lhs: KeyBytes<'_>, rhs: KeyBytes<'_>) -> io::Result<Ordering> {
        if lhs.as_ptr() == rhs.as_ptr() && lhs.len() == rhs.len() {
            return Ok(Ordering::Equal);
        }

        let (left_length, left_offset) = VarInt::from_encoded_bytes_unchecked(lhs.as_ref());
        let lhs_len: usize = left_length.into();
        let left_data = &lhs[left_offset..left_offset + lhs_len];

        let (right_length, right_offset) = VarInt::from_encoded_bytes_unchecked(rhs.as_ref());
        let rhs_len: usize = right_length.into();
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

    fn key_size(&self, data:  &[u8]) -> io::Result<usize> {
        let (len, offset) = VarInt::from_encoded_bytes_unchecked(data.as_ref());
        let len_usize: usize = len.into();
        Ok(offset + len_usize)
    }
}

impl Ranger for VarlenComparator {
    /// Computes the range (difference between two variable length keys).
    ///
    /// The keys must be encoded with a VarInt prefix which indicates key length followed by the actual data content (see [crate::types::Blob])
    fn range_bytes(&self, lhs: KeyBytes<'_>, rhs:KeyBytes<'_>) -> io::Result<Box<[u8]>> {
        let (left_length, left_offset) = VarInt::from_encoded_bytes_unchecked(&lhs);
        let lhs_len: usize = left_length.into();
        let left_data = &lhs[left_offset..left_offset + lhs_len];

        let (right_length, right_offset) = VarInt::from_encoded_bytes_unchecked(&rhs);
        let rhs_len: usize = right_length.into();
        let right_data = &rhs[right_offset..right_offset + rhs_len];

        match self.compare(lhs, rhs)? {
            Ordering::Equal => Ok(Box::from([])),
            Ordering::Greater => subtract_bytes(left_data, right_data),
            Ordering::Less => subtract_bytes(right_data, left_data),
        }
    }

    /// For variable length data, we use the difference bytes to estimate selectivity
    ///
    /// As it is not easy to operate with variable length data (how the fuck do you divide an string), we compare using first few bytes as approximation
    fn selectivity_range(&self, min: KeyBytes<'_>, max: KeyBytes<'_>) -> io::Result<Selectivity> {
        let range_bytes = self.range_bytes(min, max)?;

        if range_bytes.is_empty() {
            return Ok(Selectivity::Empty);
        }

        // Decode the VarInt prefix to get actual data
        let (len, offset) = VarInt::from_encoded_bytes_unchecked(&range_bytes);
        let data_len: usize = len.into();

        if data_len == 0 {
            return Ok(Selectivity::Empty);
        }

        let data = &range_bytes[offset..offset + data_len.min(range_bytes.len() - offset)];

        // Use first 8 bytes as approximation for selectivity
        let sample_bytes = data_len.min(8);
        let mut range_val: u64 = 0;
        for i in 0..sample_bytes.min(data.len()) {
            range_val = (range_val << 8) | data[i] as u64;
        }

        // Shift to normalize if we read fewer than 8 bytes
        if sample_bytes < 8 {
            range_val <<= (8 - sample_bytes) * 8;
        }

        // Max is u64::MAX for 8 bytes
        let selectivity = range_val as f64 / u64::MAX as f64;
        Ok(Selectivity::from(selectivity))
    }
}
