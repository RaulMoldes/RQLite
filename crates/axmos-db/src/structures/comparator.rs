use crate::types::VarInt;

use std::{cmp::Ordering, io, usize};

pub(crate) trait Comparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering>;
    fn key_size(&self, data: &[u8]) -> io::Result<usize>;
    fn is_fixed_size(&self) -> bool {
        false
    }
}

pub(crate) enum DynComparator {
    Variable(VarlenComparator),
    StrictNumeric(NumericComparator),
    FixedSizeBytes(FixedSizeBytesComparator),
}

pub(crate) struct NumericComparator(usize);

impl Comparator for DynComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        match self {
            Self::FixedSizeBytes(c) => c.compare(lhs, rhs),
            Self::Variable(c) => c.compare(lhs, rhs),
            Self::StrictNumeric(c) => c.compare(lhs, rhs),
        }
    }

    fn is_fixed_size(&self) -> bool {
        match self {
            Self::FixedSizeBytes(c) => c.is_fixed_size(),
            Self::Variable(c) => c.is_fixed_size(),
            Self::StrictNumeric(c) => c.is_fixed_size(),
        }
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        match self {
            Self::FixedSizeBytes(c) => c.key_size(data),
            Self::Variable(c) => c.key_size(data),
            Self::StrictNumeric(c) => c.key_size(data),
        }
    }
}

pub(crate) struct VarlenComparator;

impl Comparator for VarlenComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        if lhs.as_ptr() == rhs.as_ptr() && lhs.len() == rhs.len() {
            return Ok(Ordering::Equal);
        }

        // Deserialize VarInt lengths
        let (left_length, left_offset) = VarInt::from_encoded_bytes(lhs)?;
        let lhs_len: usize = left_length.try_into()?;
        let left_data = &lhs[left_offset..left_offset + lhs_len];

        let (right_length, right_offset) = VarInt::from_encoded_bytes(rhs)?;
        let rhs_len: usize = right_length.try_into()?;
        let right_data = &rhs[right_offset..right_offset + rhs_len];

        let min_len = lhs_len.min(rhs_len);

        if min_len > 8 {
            // Compare in 8-byte chunks for big byte arrays
            let chunk_count = min_len / 8;
            for i in 0..chunk_count {
                let lhs_chunk = &left_data[i * 8..(i + 1) * 8];
                let rhs_chunk = &right_data[i * 8..(i + 1) * 8];

                // Ahhh yes.. overnight bugs.
                // [from_ne_bytes] does not preserve lexicographical ordering of strings.
                // for correctness we use [from_be_bytes]
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

        // If all compared bytes are equal, decide by total length
        Ok(lhs_len.cmp(&rhs_len))
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        let (len, offset) = VarInt::from_encoded_bytes(data)?;
        let len_usize: usize = len.try_into().unwrap();
        Ok(offset + len_usize)
    }
}

pub(crate) struct FixedSizeBytesComparator(usize);

impl FixedSizeBytesComparator {
    pub fn with_type<T>() -> Self {
        Self(std::mem::size_of::<T>())
    }

    pub fn for_size(size: usize) -> Self {
        Self(size)
    }
}

impl Comparator for FixedSizeBytesComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        Ok(lhs[..self.0].cmp(&rhs[..self.0]))
    }

    fn key_size(&self, __data: &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}

impl NumericComparator {
    pub fn with_type<T>() -> Self {
        Self(std::mem::size_of::<T>())
    }

    pub fn for_size(size: usize) -> Self {
        Self(size)
    }
}

impl Comparator for NumericComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        let mut a: u64 = 0;
        let mut b: u64 = 0;

        for i in 0..self.0 {
            a |= (lhs[i] as u64) << (8 * i);
            b |= (rhs[i] as u64) << (8 * i);
        }

        Ok(a.cmp(&b))
    }

    fn key_size(&self, __data: &[u8]) -> io::Result<usize> {
        Ok(self.0)
    }

    fn is_fixed_size(&self) -> bool {
        true
    }
}
