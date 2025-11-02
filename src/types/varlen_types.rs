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

/// Zero-copy view over variable-length data with length prefix
/// Optimized for use as database index keys
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

    pub(crate) fn from_raw_unchecked(
        raw_data: &'a [u8],
        offset: usize,
        len: usize,
    ) -> VarlenType<'a> {
        assert!(offset <= raw_data.len(), "offset out of range");
        assert!(offset + len <= raw_data.len(), "offset + len out of");

        VarlenType {
            raw_data,
            data_offset: offset,
            cached_length: Some(len),
        }
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

    /// Helper to create a boxed slice with length prefix
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

    /// Convert to owned Blob
    pub fn to_blob(&self) -> Blob {
        Blob(self.data().to_vec().into_boxed_slice())
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

impl<'a> TryFrom<&'a [u8]> for VarlenType<'a> {
    type Error = std::io::Error;

    /// Tries to create a `VarlenType` from a raw byte slice (length prefix + data).
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        Self::from_raw(value)
    }
}

impl<'a> TryFrom<&'a str> for VarlenType<'a> {
    type Error = std::io::Error;

    /// Creates a UTF-8 encoded VarlenType from a &str.
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let encoded = value.as_bytes();
        let boxed = Self::boxed(encoded);
        // Safety: Box<[u8]> lives for the duration of 'static here, we reborrow as &'a
        let slice: &'a [u8] = unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&boxed) };
        Self::from_raw(slice)
    }
}

impl TryFrom<String> for VarlenType<'static> {
    type Error = std::io::Error;

    /// Consumes a String and creates a VarlenType with owned bytes.
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bytes = value.into_bytes();
        let boxed = Self::boxed(&bytes);
        let slice: &'static [u8] = Box::leak(boxed);
        Self::from_raw(slice)
    }
}

impl<'a> From<&VarlenType<'a>> for String {
    fn from(varlen: &VarlenType<'a>) -> Self {
        varlen.to_string(TextEncoding::Utf8)
    }
}

impl<'a> AsRef<[u8]> for VarlenType<'a> {
    fn as_ref(&self) -> &[u8] {
        let length_varint = VarInt(self.length() as i64);
        let length_bytes = length_varint.to_bytes();

        let expected_len = length_bytes.len() + self.length();
        if self.raw_data.len() == expected_len
            && &self.raw_data[0..length_bytes.len()] == length_bytes.as_slice()
        {
            self.raw_data
        } else {
            let mut buf = Vec::with_capacity(expected_len);
            buf.extend_from_slice(&length_bytes);
            buf.extend_from_slice(self.data());
            buf.leak()
        }
    }
}
/// Owned wrapper for variable-length data
/// Just holds the raw bytes (Excluding length prefix)
#[derive(Debug, Clone)]
pub struct Blob(Box<[u8]>);

impl Blob {
    /// Create a new Blob from raw data
    pub fn new(data: &[u8]) -> Self {
        Blob(VarlenType::boxed(data))
    }

    /// Get a VarlenType view of this Blob
    pub fn as_varlen(&self) -> VarlenType<'_> {
        VarlenType::from_raw(&self.0).unwrap()
    }

    /// Get the raw serialized bytes (including length prefix)
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume self and return the boxed slice
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        self.0
    }

    /// Get the length of the data (excluding prefix)
    pub fn length(&self) -> usize {
        self.as_varlen().length()
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

// Comparisons delegate to VarlenType for optimized zero-copy comparison
impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.as_varlen().eq(&other.as_varlen())
    }
}

impl Eq for Blob {}

impl Ord for Blob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_varlen().cmp(&other.as_varlen())
    }
}

impl PartialOrd for Blob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Conversion from VarlenType to Blob
impl<'a> From<VarlenType<'a>> for Blob {
    fn from(varlen: VarlenType<'a>) -> Self {
        varlen.to_blob()
    }
}

impl<'a> From<&VarlenType<'a>> for Blob {
    fn from(varlen: &VarlenType<'a>) -> Self {
        varlen.to_blob()
    }
}

// Convenience conversions
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

impl From<&[u8]> for Blob {
    fn from(value: &[u8]) -> Self {
        Blob::new(value)
    }
}

impl std::borrow::Borrow<[u8]> for Blob {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl std::fmt::Display for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob: {} bytes", self.length())
    }
}

#[cfg(test)]
mod tests {
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
    fn test_varlen_serialization() {
        let data = [1, 2, 3, 4, 5, 6, 7, 8];
        let boxed = VarlenType::boxed(&data);
        let vlen = VarlenType::from_raw(&boxed).unwrap();
        let bytes: &[u8] = vlen.as_ref();
        let vlen_deserialized = VarlenType::try_from(bytes).unwrap();
        assert_eq!(vlen_deserialized, vlen)
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
    fn test_blob_simple() {
        let data = b"test data";
        let data_len = VarInt(data.len() as i64);
        let mut bytes = data_len.to_bytes();
        bytes.extend_from_slice(data);
        let blob = Blob::new(data);

        assert_eq!(blob.as_ref(), bytes);
        assert_eq!(blob.length(), data.len());

        // Test VarlenType view
        let varlen = blob.as_varlen();
        assert_eq!(varlen.data(), data);
    }

    #[test]
    fn test_blob_comparisons() {
        let a = Blob::from("abc");
        let b = Blob::from("abd");
        let c = Blob::from("abc");

        assert!(a < b);
        assert_eq!(a, c);
        assert_ne!(a, b);
    }


}
