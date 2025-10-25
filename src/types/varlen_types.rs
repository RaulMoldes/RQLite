use crate::types::SizedType;
use crate::{types::VarInt, TextEncoding};
use std::cmp::Ordering;
use std::cmp::{Ord, PartialOrd};
use std::io;
use std::mem::MaybeUninit;

// Helper function for string encoding
fn encode_str(s: &str, encoding: TextEncoding) -> Vec<u8> {
    match encoding {
        TextEncoding::Utf8 => s.as_bytes().to_vec(),
        TextEncoding::Utf16be => s.encode_utf16().flat_map(|ch| ch.to_be_bytes()).collect(),
        TextEncoding::Utf16le => s.encode_utf16().flat_map(|ch| ch.to_le_bytes()).collect(),
    }
}

/// Returns the number of bytes we need to store the length of a [`VARCHAR`] or [`TEXT`] types.
pub(crate) fn utf8_length_prefix_bytes(max_characters: usize) -> usize {
    let varint = VarInt(max_characters as i64);
    varint.size()
}

/// Returns the number of bytes needed to store the length prefix for UTF-16 encoding.
pub(crate) fn utf16_length_prefix_bytes(max_characters: usize) -> usize {
    let max_bytes = max_characters * 4;
    let varint = VarInt(max_bytes as i64);
    varint.size()
}

pub(crate) fn decode_utf16le(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();
    String::from_utf16(&units).unwrap()
}

pub(crate) fn decode_utf16be(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
        .collect();
    String::from_utf16(&units).unwrap()
}

#[derive(Debug)]
pub struct VarlenType<'a> {
    /// The raw serialized data including length prefix
    raw_data: &'a [u8],
    /// Cached offset where the actual data starts (after length prefix)
    data_offset: usize,
    /// Cached length of the data
    cached_length: Option<usize>,
}

impl<'a> VarlenType<'a> {
    pub(crate) fn empty() -> Self {
        static EMPTY: [u8; 1] = [0]; // VarInt encoding of 0
        Self {
            raw_data: &EMPTY,
            data_offset: 1,
            cached_length: Some(0),
        }
    }

    /// Creates a VarlenType from raw serialized data (length prefix + data)
    pub(crate) fn from_raw(raw_data: &'a [u8]) -> io::Result<Self> {
        let (varint, offset) = VarInt::from_bytes(raw_data)?;
        let length: usize = varint
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            raw_data,
            data_offset: offset,
            cached_length: Some(length),
        })
    }

    /// Get the length of the data
    pub fn length(&self) -> usize {
        if let Some(len) = self.cached_length {
            len
        } else {
            // Parse length on demand if not cached
            let (varint, _) = VarInt::from_bytes(self.raw_data).unwrap_or((VarInt(0), 0));
            varint.try_into().unwrap_or(0)
        }
    }

    pub(crate) fn boxed(data: &[u8]) -> Box<[u8]> {
        let length_varint = VarInt(data.len() as i64);
        let length_bytes = length_varint.to_bytes();
        let total_len = length_bytes.len() + data.len();

        // Create an uninitialized Box<[MaybeUninit<u8>]>
        let mut boxed_uninit: Box<[MaybeUninit<u8>]> = Box::new_uninit_slice(total_len);

        // Write the data in the slice
        unsafe {
            let slice = &mut *boxed_uninit;
            for (i, b) in length_bytes.iter().enumerate() {
                slice[i].write(*b);
            }
            for (i, b) in data.iter().enumerate() {
                slice[length_bytes.len() + i].write(*b);
            }

            boxed_uninit.assume_init()
        }
    }

    /// Get the actual data (excluding length prefix)
    pub fn data(&self) -> &[u8] {
        let len = self.length();
        let end = self.data_offset + len;
        if end > self.raw_data.len() {
            &self.raw_data[self.data_offset..]
        } else {
            &self.raw_data[self.data_offset..end]
        }
    }

    /// Get raw serialized data (including length prefix)
    pub fn raw_data(&self) -> &[u8] {
        self.raw_data
    }

    pub fn is_empty(&self) -> bool {
        self.length() == 0
    }

    pub(crate) fn to_string(&self, encoding: TextEncoding) -> String {
        let data = self.data();
        match encoding {
            TextEncoding::Utf8 => String::from_utf8(data.to_vec()).unwrap_or_default(),
            TextEncoding::Utf16le => decode_utf16le(data),
            TextEncoding::Utf16be => decode_utf16be(data),
        }
    }
}

impl<'a> std::fmt::Display for VarlenType<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VarlenType: Length {} bytes", self.length())
    }
}

impl<'a> PartialEq for VarlenType<'a> {
    /// Zero-copy equality check with early exit
    fn eq(&self, other: &Self) -> bool {
        // Fast path: check length first
        let self_len = self.length();
        if self_len != other.length() {
            return false;
        }

        // Fast path: if they point to the same memory
        if self.raw_data.as_ptr() == other.raw_data.as_ptr()
            && self.data_offset == other.data_offset
        {
            return true;
        }

        let self_data = self.data();
        let other_data = other.data();

        // Use slice comparison which is optimized
        self_data == other_data
    }
}

impl<'a> Eq for VarlenType<'a> {}

impl<'a> Ord for VarlenType<'a> {
    /// Zero-copy comparison that compares byte-by-byte without materializing the full data
    /// This is optimized for early exit when differences are found
    fn cmp(&self, other: &Self) -> Ordering {
        // Fast path: if they point to the same memory, they're equal
        if self.raw_data.as_ptr() == other.raw_data.as_ptr()
            && self.data_offset == other.data_offset
            && self.length() == other.length()
        {
            return Ordering::Equal;
        }

        let self_len = self.length();
        let other_len = other.length();

        // Get data slices
        let self_data = self.data();
        let other_data = other.data();

        // Compare byte by byte up to the minimum length
        let min_len = self_len.min(other_len);

        // Use chunks for better performance on longer strings
        if min_len > 8 {
            // Compare in 8-byte chunks for better performance
            let chunk_count = min_len / 8;
            for i in 0..chunk_count {
                let self_chunk = &self_data[i * 8..(i + 1) * 8];
                let other_chunk = &other_data[i * 8..(i + 1) * 8];

                // Convert to u64 for fast comparison
                let self_u64 = u64::from_ne_bytes(self_chunk.try_into().unwrap());
                let other_u64 = u64::from_ne_bytes(other_chunk.try_into().unwrap());

                match self_u64.cmp(&other_u64) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }

            // Compare remaining bytes
            for i in (chunk_count * 8)..min_len {
                match self_data[i].cmp(&other_data[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
        } else {
            // For short strings, compare byte by byte
            for i in 0..min_len {
                match self_data[i].cmp(&other_data[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
        }

        // If all compared bytes are equal, the shorter one comes first
        self_len.cmp(&other_len)
    }
}

impl<'a> PartialOrd for VarlenType<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> AsRef<[u8]> for VarlenType<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl<'a> From<&VarlenType<'a>> for String {
    fn from(varlen: &VarlenType<'a>) -> Self {
        varlen.to_string(TextEncoding::Utf8)
    }
}

/// Owned version of VarlenType
#[derive(Debug, Clone)]
pub struct Blob {
    /// Stores the complete serialized form (length prefix + data) as a boxed slice.
    serialized: Box<[u8]>,
    /// Cached offset where data starts
    data_offset: usize,
    /// Cached data length
    cached_length: usize,
}

impl Blob {
    /// Create a new Blob from raw data (will add length prefix)
    pub fn new(data: &[u8]) -> Self {
        let serialized = VarlenType::boxed(data);
        let (varint, offset) = VarInt::from_bytes(&serialized).unwrap();
        let length: usize = varint.try_into().unwrap_or(0);

        Self {
            serialized,
            data_offset: offset,
            cached_length: length,
        }
    }

    /// Get the data (excluding length prefix)
    pub fn data(&self) -> &[u8] {
        let end = self.data_offset + self.cached_length;
        if end > self.serialized.len() {
            &self.serialized[self.data_offset..]
        } else {
            &self.serialized[self.data_offset..end]
        }
    }

    /// Get a VarlenType view of this Blob
    pub fn as_varlen(&self) -> VarlenType<'_> {
        VarlenType::from_raw(&self.serialized[..]).unwrap()
    }

    /// Get the serialized data (including length prefix)
    pub fn serialized(&self) -> &[u8] {
        &self.serialized
    }

    /// Get the length of the data (excluding prefix)
    pub fn length(&self) -> usize {
        self.cached_length
    }

    /// Get total size in memory (including length prefix)
    pub fn total_size(&self) -> usize {
        self.serialized.len()
    }

    /// Clone the data portion (without length prefix) into a Vec
    pub fn data_to_vec(&self) -> Vec<u8> {
        self.data().to_vec()
    }

    /// Consume self and return the boxed slice
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        self.serialized
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        // If lengths differ, they're not equal
        if self.cached_length != other.cached_length {
            return false;
        }
        // Compare the actual data
        self.data() == other.data()
    }
}

impl Eq for Blob {}

impl Ord for Blob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Use the optimized zero-copy comparison
        self.as_varlen().cmp(&other.as_varlen())
    }
}

impl PartialOrd for Blob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Borrow implementation <[u8]>
impl std::borrow::Borrow<[u8]> for Blob {
    fn borrow(&self) -> &[u8] {
        self.data()
    }
}

// For testing and convenience
impl From<u32> for Blob {
    fn from(value: u32) -> Self {
        Blob::new(&value.to_be_bytes())
    }
}

impl From<&str> for Blob {
    fn from(value: &str) -> Self {
        Blob::new(value.as_bytes())
    }
}

impl From<String> for Blob {
    fn from(value: String) -> Self {
        Blob::new(value.as_bytes())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(value: Vec<u8>) -> Self {
        Blob::new(&value)
    }
}

impl TryFrom<&[u8]> for Blob {
    type Error = std::io::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Blob::new(value))
    }
}

#[cfg(test)]
mod varlen_tests {
    use super::*;

    #[test]
    fn test_varlen_empty() {
        let v = VarlenType::empty();
        assert_eq!(v.length(), 0);
        assert!(v.data().is_empty());
        assert_eq!(v.to_string(TextEncoding::Utf8), "");
    }

    #[test]
    fn test_varlen_from_raw() {
        let data = b"hello";
        let serialized = VarlenType::boxed(data);
        let v = VarlenType::from_raw(&serialized).unwrap();
        assert_eq!(v.length(), data.len());
        assert_eq!(v.data(), data);
        assert_eq!(v.to_string(TextEncoding::Utf8), "hello");
    }

    #[test]
    fn test_varlen_cmp() {
        let box_a = VarlenType::boxed(b"abc");
        let a = VarlenType::from_raw(&box_a).unwrap();
        let box_b = VarlenType::boxed(b"abd");
        let b = VarlenType::from_raw(&box_b).unwrap();
        assert!(a.cmp(&b) == std::cmp::Ordering::Less);
        let box_c = VarlenType::boxed(b"abc");
        let c = VarlenType::from_raw(&box_c).unwrap();
        assert!(a.eq(&c));
    }

    #[test]
    fn test_blob_new_and_data() {
        let data = b"foobar";
        let blob = Blob::new(data);
        assert_eq!(blob.data(), data);
        assert_eq!(blob.length(), data.len());
        assert_eq!(blob.as_varlen().data(), data);
    }

    #[test]
    fn test_blob_from_various() {
        let b1 = Blob::from(42u32);
        assert_eq!(b1.length(), 4);
        assert_eq!(b1.data(), &42u32.to_be_bytes());

        let b2 = Blob::from("hello");
        assert_eq!(b2.length(), 5);
        assert_eq!(b2.data(), b"hello");

        let b3 = Blob::from(vec![1, 2, 3]);
        assert_eq!(b3.length(), 3);
        assert_eq!(b3.data(), &[1, 2, 3]);
    }

    #[test]
    fn test_blob_ord() {
        let a = Blob::from("abc");
        let b = Blob::from("abd");
        assert!(a < b);
        let c = Blob::from("abc");
        assert_eq!(a, c);
    }

    #[test]
    fn test_blob_serialization_roundtrip() {
        let original = b"some random data";
        let blob = Blob::new(original);
        let serialized = blob.serialized();
        let varlen = VarlenType::from_raw(serialized).unwrap();
        assert_eq!(varlen.data(), original);
    }
}
