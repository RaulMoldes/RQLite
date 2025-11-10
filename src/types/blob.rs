use crate::types::varint::MAX_VARINT_LEN;
use crate::{types::VarInt, TextEncoding};
use std::cmp::PartialEq;

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

#[derive(Debug, Clone)]
pub struct Blob(Box<[u8]>);

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Blob {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl Blob {
    /// Create a new Blob from bytes that already include the length prefix
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(bytes.to_vec().into_boxed_slice())
    }

    /// Get just the data portion
    pub fn data(&self) -> &[u8] {
        self.as_ref()
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.data().len()
    }

    pub fn to_string(&self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => String::from_utf8(self.data().to_vec()).unwrap(),
            TextEncoding::Utf16be => {
                assert!(
                    self.len() % 2 == 0,
                    "UTF-16BE blob length must be multiple of 2"
                );
                decode_utf16be(self.data())
            }
            TextEncoding::Utf16le => {
                assert!(
                    self.len() % 2 == 0,
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

    pub fn content(&self) -> &[u8] {
        let data = self.data();
        let (_, offset) = VarInt::from_encoded_bytes(data).unwrap();
        &self.data()[offset..]
    }

    pub fn content_mut(&mut self) -> &mut [u8] {
        let data = self.data();
        let (_, offset) = VarInt::from_encoded_bytes(data).unwrap();
        &mut self.data_mut()[offset..]
    }
}

#[derive(Debug)]
pub struct BlobRef<'a>(&'a [u8]);

impl<'a> AsRef<[u8]> for BlobRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> BlobRef<'a> {
    /// Create a new Blob from bytes that already include the length prefix
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Get just the data portion (excluding length prefix)
    pub fn data(&self) -> &[u8] {
        self.0
    }

    pub fn content(&self) -> &[u8] {
        let data = self.data();
        let (_, offset) = VarInt::from_encoded_bytes(data).unwrap();
        &self.data()[offset..]
    }

    /// Get the length of the data (excluding prefix)
    pub fn len(&self) -> usize {
        self.data().len()
    }

    pub fn to_string(&self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => String::from_utf8(self.data().to_vec()).unwrap(),
            TextEncoding::Utf16be => {
                assert!(
                    self.len() % 2 == 0,
                    "UTF-16BE blob length must be multiple of 2"
                );
                decode_utf16be(self.data())
            }
            TextEncoding::Utf16le => {
                assert!(
                    self.len() % 2 == 0,
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

    pub fn to_blob(&self) -> Blob {
        Blob(self.0.to_vec().into_boxed_slice())
    }
}

impl<'a> AsRef<BlobRef<'a>> for Blob {
    fn as_ref(&self) -> &BlobRef<'a> {
        unsafe { &*(self.0.as_ref() as *const [u8] as *const BlobRef<'_>) }
    }
}

impl<'a> AsMut<BlobRefMut<'a>> for Blob {
    fn as_mut(&mut self) -> &mut BlobRefMut<'a> {
        unsafe { &mut *(self.0.as_mut() as *mut [u8] as *mut BlobRefMut<'_>) }
    }
}

/// Borrowed wrapper for variable-length data
/// Holds a reference to the raw bytes (including length prefix)
#[derive(Debug)]
pub struct BlobRefMut<'a>(&'a mut [u8]);

impl<'a> AsRef<[u8]> for BlobRefMut<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> AsMut<[u8]> for BlobRefMut<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0
    }
}

impl<'a> BlobRefMut<'a> {
    /// Create a new Blob from bytes that already include the length prefix
    pub fn from_bytes(bytes: &'a mut [u8]) -> Self {
        Self(bytes)
    }

    /// Get just the data portion (excluding length prefix)
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.0
    }

    /// Get just the data portion (excluding length prefix)
    pub fn data(&self) -> &[u8] {
        self.0
    }

    /// Get the length of the data (excluding prefix)
    pub fn len(&self) -> usize {
        self.data().len()
    }

    pub fn to_string(&self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => String::from_utf8(self.data().to_vec()).unwrap(),
            TextEncoding::Utf16be => {
                assert!(
                    self.len() % 2 == 0,
                    "UTF-16BE blob length must be multiple of 2"
                );
                decode_utf16be(self.data())
            }
            TextEncoding::Utf16le => {
                assert!(
                    self.len() % 2 == 0,
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

    pub fn to_blob(&self) -> Blob {
        Blob(self.0.to_vec().into_boxed_slice())
    }

    pub fn content(&self) -> &[u8] {
        let data = self.data();
        let (_, offset) = VarInt::from_encoded_bytes(data).unwrap();
        &self.data()[offset..]
    }

    pub fn content_mut(&mut self) -> &mut [u8] {
        let data = self.data();
        let (_, offset) = VarInt::from_encoded_bytes(data).unwrap();
        &mut self.data_mut()[offset..]
    }
}

impl From<&str> for Blob {
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        let mut buffer = [0u8; MAX_VARINT_LEN];
        let vlen = VarInt::encode(bytes.len() as i64, &mut buffer);
        let mut blob_buffer = vlen.to_vec();
        blob_buffer.extend_from_slice(bytes);
        Blob(blob_buffer.into_boxed_slice())
    }
}

impl From<&[u8]> for Blob {
    fn from(value: &[u8]) -> Self {
        let mut buffer = [0u8; MAX_VARINT_LEN];
        let vlen = VarInt::encode(value.len() as i64, &mut buffer);
        let mut blob_buffer = vlen.to_vec();
        blob_buffer.extend_from_slice(value);

        Blob(blob_buffer.into_boxed_slice())
    }
}

impl From<&[u8; 8]> for Blob {
    fn from(value: &[u8; 8]) -> Self {
        let mut buffer = [0u8; MAX_VARINT_LEN];
        let vlen = VarInt::encode(value.len() as i64, &mut buffer);
        let mut blob_buffer = vlen.to_vec();
        blob_buffer.extend_from_slice(value);

        Blob(blob_buffer.into_boxed_slice())
    }
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl<'a> PartialEq for BlobRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'a> PartialEq for BlobRefMut<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'a> PartialEq<BlobRef<'a>> for Blob {
    fn eq(&self, other: &BlobRef<'a>) -> bool {
        self.0.as_ref() == other.0
    }
}

impl<'a> PartialEq<Blob> for BlobRef<'a> {
    fn eq(&self, other: &Blob) -> bool {
        self.0 == other.0.as_ref()
    }
}

impl<'a> PartialEq<BlobRefMut<'a>> for Blob {
    fn eq(&self, other: &BlobRefMut<'a>) -> bool {
        self.0.as_ref() == other.0
    }
}

impl<'a> PartialEq<Blob> for BlobRefMut<'a> {
    fn eq(&self, other: &Blob) -> bool {
        self.0 == other.0.as_ref()
    }
}

impl<'a, 'b> PartialEq<BlobRefMut<'b>> for BlobRef<'a> {
    fn eq(&self, other: &BlobRefMut<'b>) -> bool {
        self.0 == other.0
    }
}

impl<'a, 'b> PartialEq<BlobRef<'b>> for BlobRefMut<'a> {
    fn eq(&self, other: &BlobRef<'b>) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<&[u8]> for Blob {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0.as_ref() == *other
    }
}

impl<'a> PartialEq<&[u8]> for BlobRef<'a> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0 == *other
    }
}

impl<'a> PartialEq<&[u8]> for BlobRefMut<'a> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Blob> for &[u8] {
    fn eq(&self, other: &Blob) -> bool {
        *self == other.0.as_ref()
    }
}

impl<'a> PartialEq<BlobRef<'a>> for &[u8] {
    fn eq(&self, other: &BlobRef<'a>) -> bool {
        *self == other.0
    }
}

impl<'a> PartialEq<BlobRefMut<'a>> for &[u8] {
    fn eq(&self, other: &BlobRefMut<'a>) -> bool {
        *self == other.0
    }
}

impl Eq for Blob {}
impl<'a> Eq for BlobRef<'a> {}
impl<'a> Eq for BlobRefMut<'a> {}
