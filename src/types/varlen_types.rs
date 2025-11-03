use crate::{types::VarInt, TextEncoding};

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

/// Borrowed wrapper for variable-length data
/// Holds a reference to the raw bytes (including length prefix)
#[derive(Debug, Clone, Copy)]
pub struct BlobRef<'a>(&'a [u8]);

impl<'a> AsRef<[u8]> for BlobRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> BlobRef<'a> {
    /// Create a new Blob from bytes that already include the length prefix
    pub fn from_encoded_bytes(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Parse a Blob from a byte slice, returning the Blob and bytes consumed
    pub fn parse(bytes: &'a [u8]) -> (Self, usize) {
        let (len_varint, len_bytes) = VarInt::from_encoded_bytes(bytes);
        let data_len: usize = len_varint.try_into().unwrap();
        let total_size = len_bytes + data_len;
        (Self(&bytes[..total_size]), total_size)
    }

    /// Get just the data portion (excluding length prefix)
    pub fn data(&self) -> &[u8] {
        let (len_varint, offset) = VarInt::from_encoded_bytes(self.0);
        let len: usize = len_varint.try_into().unwrap();
        &self.0[offset..offset + len]
    }

    /// Get the length of the data (excluding prefix)
    pub fn length(&self) -> usize {
        let (len_varint, _) = VarInt::from_encoded_bytes(self.0);
        len_varint.try_into().unwrap()
    }

    pub fn data_offset(&self) -> usize {
        let (_, offset) = VarInt::from_encoded_bytes(self.0);
        offset
    }

    pub fn to_string(self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => String::from_utf8(self.data().to_vec()).unwrap(),
            TextEncoding::Utf16be => {
                assert!(
                    self.length() % 2 == 0,
                    "UTF-16BE blob length must be multiple of 2"
                );
                decode_utf16be(self.data())
            }
            TextEncoding::Utf16le => {
                assert!(
                    self.length() % 2 == 0,
                    "UTF-16LE blob length must be multiple of 2"
                );
                decode_utf16le(self.data())
            }
        }
    }

    pub fn as_utf8(&self) -> &[u8] {
        self.data()
    }

    pub fn as_str(&self, encoding: TextEncoding) -> &str {
        match encoding {
            TextEncoding::Utf8 => std::str::from_utf8(self.as_utf8()).unwrap(),
            _ => panic!("Can only convert to string directly using utf8 encoding"),
        }
    }

    /// Total size including the length prefix
    pub fn total_size(&self) -> usize {
        self.0.len()
    }
}
