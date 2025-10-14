//! Types module.
use super::{varint::Varint, RQLiteType, RQLiteTypeMarker, Splittable};
use crate::serialization::Serializable;
use crate::TextEncoding;

use std::cmp::Ordering;
use std::cmp::{Ord, PartialOrd};

#[derive(Debug, Clone)]
pub struct VarlenaType {
    length: Varint,
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

    fn split_at(&mut self, offset: usize) -> Self {
        let new_data = self.data.split_off(offset);
        let new_length = self.length - Varint(offset as i64);
        self.length = Varint(offset as i64);

        Self {
            data: new_data,
            length: new_length,
            encoding: self.encoding,
        }
    }
}

impl VarlenaType {
    pub(crate) fn from_raw_bytes(data: &[u8], encoding: Option<TextEncoding>) -> Self {
        let len = data.len() as i64;
        Self {
            data: data.to_vec(),
            length: Varint(len),
            encoding,
        }
    }
    pub(crate) fn from_str(s: &str, encoding: TextEncoding) -> Self {
        match encoding {
            TextEncoding::Utf8 => {
                let data = s.as_bytes().to_vec();
                let length = Varint(data.len() as i64);
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
                let length = Varint(data.len() as i64);
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
                let length = Varint(data.len() as i64);
                Self {
                    length,
                    data,
                    encoding: Some(encoding),
                }
            }
        }
    }

    /// Returns the data as a string if it's a String type
    pub fn as_string(&self) -> Option<String> {
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

    pub fn total_size_bytes(&self) -> usize {
        1 + self.length.size_of() + self.data.len()
    }

    pub fn effective_size(&self) -> usize {
        self.data.len()
    }

    /// Returns the data as bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl RQLiteType for VarlenaType {
    fn _type_of(&self) -> RQLiteTypeMarker {
        if self.encoding.is_some() {
            RQLiteTypeMarker::String
        } else {
            RQLiteTypeMarker::Blob
        }
    }

    fn size_of(&self) -> usize {
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
        self.length.write_to(writer)?;

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

        // 2. Read length as varint
        let length = Varint::read_from(reader)?;

        // 3. Read data
        let data_len = length.0 as usize;
        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        Ok(Self {
            length,
            data,
            encoding,
        })
    }
}
