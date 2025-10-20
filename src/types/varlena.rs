//! Types module.
use super::{DataType, DataTypeMarker, Splittable};
use crate::serialization::Serializable;
use crate::TextEncoding;
use std::ops::{Index, Range, RangeFrom, RangeFull, RangeTo};

use std::cmp::Ordering;
use std::cmp::{Ord, PartialOrd};

// Helper function for string encoding
fn encode_str(s: &str, encoding: TextEncoding) -> Vec<u8> {
    match encoding {
        TextEncoding::Utf8 => s.as_bytes().to_vec(),
        TextEncoding::Utf16be => s.encode_utf16().flat_map(|ch| ch.to_be_bytes()).collect(),
        TextEncoding::Utf16le => s.encode_utf16().flat_map(|ch| ch.to_le_bytes()).collect(),
    }
}

#[derive(Debug, Clone)]
pub struct VarlenaType {
    length: u16,
    data: Vec<u8>,
    encoding: Option<TextEncoding>,
}

impl Splittable for VarlenaType {
    fn merge_with(&mut self, other: Self) {
        self.data.extend(other.data);
        self.length += other.length;

        if self.encoding != other.encoding {
            self.encoding = None
        }
    }

    fn split_at(&mut self, offset: u16) -> Self {
        let new_data = self.data.split_off(offset as usize);
        let new_length = self.length - offset;
        self.length = offset;

        Self {
            data: new_data,
            length: new_length,
            encoding: self.encoding,
        }
    }
}

impl VarlenaType {
    pub(crate) fn is_allocated(&self) -> bool {
        self.data.capacity() == self.length as usize
    }

    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            data: Vec::new(),
            encoding: None,
        }
    }

    pub(crate) fn from_string(s: impl AsRef<str>) -> Self {
        Self::from_str(s.as_ref(), TextEncoding::Utf8)
    }

    pub(crate) fn from_blob(data: impl Into<Vec<u8>>) -> Self {
        let data = data.into();
        let length = data.len() as u16;
        Self {
            length,
            data,
            encoding: None,
        }
    }

    pub(crate) fn with_capacity(size: u16, encoding: Option<TextEncoding>) -> Self {
        let len = size as usize;
        Self {
            data: Vec::with_capacity(len),
            length: 0, // Start from 0 since capacity does not mean data.
            encoding,
        }
    }

    pub(crate) fn from_raw_bytes(data: &[u8], encoding: Option<TextEncoding>) -> Self {
        let len = data.len() as u16;
        Self {
            data: data.to_vec(),
            length: len,
            encoding,
        }
    }
    pub(crate) fn from_str(s: &str, encoding: TextEncoding) -> Self {
        match encoding {
            TextEncoding::Utf8 => {
                let data = s.as_bytes().to_vec();
                let length = data.len() as u16;
                Self {
                    length,
                    data,
                    encoding: Some(encoding),
                }
            }
            TextEncoding::Utf16be => {
                let mut data = Vec::new();
                for ch in s.encode_utf16() {
                    data.extend_from_slice(&ch.to_be_bytes());
                }
                let length = data.len() as u16;
                Self {
                    length,
                    data,
                    encoding: Some(encoding),
                }
            }
            TextEncoding::Utf16le => {
                let mut data = Vec::new();
                for ch in s.encode_utf16() {
                    data.extend_from_slice(&ch.to_le_bytes());
                }
                let length = data.len() as u16;
                Self {
                    length,
                    data,
                    encoding: Some(encoding),
                }
            }
        }
    }

    /// Returns the data as a string if it's a String type
    pub(crate) fn as_string(&self) -> Option<String> {
        if let Some(enconding) = self.encoding {
            match enconding {
                TextEncoding::Utf8 => {
                    return String::from_utf8(self.data.clone()).ok();
                }
                TextEncoding::Utf16le => {
                    let u16_data: Vec<u16> = self
                        .data
                        .chunks(2)
                        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                        .collect();
                    return String::from_utf16(&u16_data).ok();
                }
                TextEncoding::Utf16be => {
                    let u16_data: Vec<u16> = self
                        .data
                        .chunks(2)
                        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                        .collect();
                    return String::from_utf16(&u16_data).ok();
                }
            }
        }
        None
    }

    pub(crate) fn total_size_bytes(&self) -> u16 {
        3 + self.data.len() as u16
    }

    pub(crate) fn effective_size(&self) -> usize {
        self.data.len()
    }

    /// Returns the data as bytes
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn len(&self) -> usize {
        self.length as usize
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub(crate) fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub(crate) fn encoding(&self) -> Option<TextEncoding> {
        self.encoding
    }

    pub(crate) fn is_string(&self) -> bool {
        self.encoding.is_some()
    }

    pub(crate) fn is_blob(&self) -> bool {
        self.encoding.is_none()
    }

    // Type checking methods
    pub(crate) fn is_utf8(&self) -> bool {
        matches!(self.encoding, Some(TextEncoding::Utf8))
    }

    pub(crate) fn is_utf16(&self) -> bool {
        matches!(
            self.encoding,
            Some(TextEncoding::Utf16be | TextEncoding::Utf16le)
        )
    }

    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.length = 0;
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(len);
            self.length = len as u16;
        }
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub(crate) fn extend_from_slice(&mut self, data: &[u8]) -> Result<(), &'static str> {
        let new_len = self.length as usize + data.len();
        if new_len > u16::MAX as usize {
            return Err("Data would exceed maximum length");
        }

        self.data.extend_from_slice(data);
        self.length = new_len as u16;
        Ok(())
    }

    pub fn extend_from_str(&mut self, s: &str) -> Result<(), &'static str> {
        let encoding = self.encoding.ok_or("Cannot write string to blob")?;
        let bytes = encode_str(s, encoding);
        self.extend_from_slice(&bytes)
    }
}

impl DataType for VarlenaType {
    fn _type_of(&self) -> DataTypeMarker {
        if self.encoding.is_some() {
            DataTypeMarker::Text
        } else {
            DataTypeMarker::Blob
        }
    }

    fn size_of(&self) -> u16 {
        self.total_size_bytes()
    }
}

impl std::fmt::Display for VarlenaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(enc) = self.encoding {
            writeln!(f, " String: Encoding {}; Length {}", enc, self.length)?;
        } else {
            writeln!(f, " Blob: Length {}", self.length)?;
        }

        Ok(())
    }
}

impl PartialEq for VarlenaType {
    fn eq(&self, other: &Self) -> bool {
        if let Some(encoding) = self.encoding {
            if let Some(other_encoding) = other.encoding {
                return self.data == other.data && encoding == other_encoding;
            }
        }

        self.data == other.data && self.encoding.is_none() && other.encoding.is_none()
    }
}

impl Eq for VarlenaType {}

impl Ord for VarlenaType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl PartialOrd for VarlenaType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Serializable for VarlenaType {
    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        // 1. Write encoding marker (1 byte)
        // If encoding is Some, it's a String, otherwise it's a Blob
        let encoding_marker = match self.encoding {
            None => 0u8,                        // Blob
            Some(TextEncoding::Utf8) => 1u8,    // UTF-8 String
            Some(TextEncoding::Utf16le) => 2u8, // UTF-16LE String
            Some(TextEncoding::Utf16be) => 3u8, // UTF-16BE String
        };
        writer.write_all(&[encoding_marker])?;

        // 2. Write length as varint
        writer.write_all(&self.length.to_be_bytes())?;

        // 3. Write data
        writer.write_all(&self.data)?;

        Ok(())
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        // 1. Read encoding marker (1 byte)
        let mut encoding_marker = [0u8; 1];
        reader.read_exact(&mut encoding_marker)?;

        let encoding = match encoding_marker[0] {
            0 => None,                        // Blob
            1 => Some(TextEncoding::Utf8),    // UTF-8 String
            2 => Some(TextEncoding::Utf16le), // UTF-16LE String
            3 => Some(TextEncoding::Utf16be), // UTF-16BE String
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid encoding marker: {}", encoding_marker[0]),
                ))
            }
        };

        // 2. Read length as byte
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer)?;
        let length = u16::from_be_bytes([buffer[0], buffer[1]]);

        // 3. Read data
        let data_len = length as usize;
        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        Ok(Self {
            length,
            data,
            encoding,
        })
    }
}

// Implement Index for convenient byte access
impl Index<usize> for VarlenaType {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl Index<Range<usize>> for VarlenaType {
    type Output = [u8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        &self.data[range]
    }
}

impl Index<RangeFull> for VarlenaType {
    type Output = [u8];

    fn index(&self, _: RangeFull) -> &Self::Output {
        &self.data[..]
    }
}

impl Index<RangeFrom<usize>> for VarlenaType {
    type Output = [u8];

    fn index(&self, range: RangeFrom<usize>) -> &Self::Output {
        &self.data[range]
    }
}

impl Index<RangeTo<usize>> for VarlenaType {
    type Output = [u8];

    fn index(&self, range: RangeTo<usize>) -> &Self::Output {
        &self.data[range]
    }
}

// Implement AsRef for convenience
impl AsRef<[u8]> for VarlenaType {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

// Implement From traits for easy construction
impl From<String> for VarlenaType {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<&str> for VarlenaType {
    fn from(s: &str) -> Self {
        Self::from_string(s)
    }
}

impl From<Vec<u8>> for VarlenaType {
    fn from(data: Vec<u8>) -> Self {
        Self::from_blob(data)
    }
}

impl From<&[u8]> for VarlenaType {
    fn from(data: &[u8]) -> Self {
        Self::from_blob(data.to_vec())
    }
}

// TryFrom for fallible conversions
impl TryFrom<VarlenaType> for String {
    type Error = String;

    fn try_from(value: VarlenaType) -> Result<Self, Self::Error> {
        value
            .as_string()
            .ok_or_else(|| "Cannot convert blob to string".to_string())
    }
}
