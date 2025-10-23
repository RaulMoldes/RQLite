//! Types module.
use crate::TextEncoding;
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


/// Returns the number of bytes we need to store the length of a [`VARCHAR`] or [`TEXT`] types.
/// As long as they are encoded in utf8.
///
/// UTF-8 encodes each character using anywhere from 1 to 4 bytes. So
/// considering the worst case scenario where every single character in a string
/// is 4 bytes, a [`VARCHAR(255)`] type would require 2 bytes to store 255
/// characters unlike ASCII which only requires 1 byte.
pub(crate) fn utf8_length_prefix_bytes(max_characters: usize) -> usize {
    match max_characters {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
    }
}

/// Returns the number of bytes needed to store the length prefix for UTF-16 encoding.
///
/// UTF-16 encodes characters using either 2 or 4 bytes (1 or 2 code units).
/// In the worst case, each character requires 2 code units (4 bytes).
///
/// For UTF-16LE and UTF-16BE the calculation is the same since both
/// use the same encoding scheme, just with different byte order.
pub(crate) fn utf16_length_prefix_bytes(max_characters: usize) -> usize {
    // Each character can be up to 4 bytes (2 UTF-16 code units)
    let max_bytes = max_characters * 4;

    match max_bytes {
        0..=255 => 1,     // 1 byte can store up to 255 bytes
        256..=65535 => 2, // 2 bytes can store up to 65,535 bytes
        _ => 4,           // 4 bytes for larger values
    }
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
    length: u16,
    data: &'a [u8],
}

impl<'a> VarlenType<'a> {
    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            data: &[],
        }
    }

     pub(crate) fn from_slice(buf: &'a [u8], start_pos: usize, length: usize) -> Self {
        let end_pos = start_pos + length;

        // Ensure no to excede buffer size
        if end_pos > buf.len() {
            // If we have overflow the buffer, take only till the end.
            let actual_length = buf.len().saturating_sub(start_pos);
            Self {
                length: actual_length as u16,
                data: &buf[start_pos..],
            }
        } else {
            Self {
                length: length as u16,
                data: &buf[start_pos..end_pos],
            }
        }
    }

    pub(crate) fn to_string(&self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => {
                String::from_utf8(self.data.to_vec()).unwrap_or_default()
            }
            TextEncoding::Utf16le => decode_utf16le(self.data),
            TextEncoding::Utf16be => decode_utf16be(self.data),
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn length(&self) -> u16 {
        self.length
    }

}


impl<'a>  std::fmt::Display for VarlenType<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, " Blob: Length {}", self.length)?;
        Ok(())
    }
}

impl<'a> PartialEq for VarlenType<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<'a> Eq for VarlenType<'a> {}

impl<'a> Ord for VarlenType<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(other.data)
    }
}

impl<'a> PartialOrd for VarlenType<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


// Implement AsRef for convenience
impl<'a> AsRef<[u8]> for VarlenType<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}



/// Convert to string using the default encoding type.
impl<'a> From<&VarlenType<'a>> for String {
    fn from(varlen: &VarlenType<'a>) -> Self {
        // By default the encoding will be utf8
        varlen.to_string(TextEncoding::Utf8)
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct Blob(Vec<u8>);

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for Blob {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl<'a> ToOwned for VarlenType<'a>{
    type Owned = Blob;
    fn to_owned(&self) -> Self::Owned {
        Blob(self.as_ref().to_vec())
    }
}

impl<'a> std::borrow::Borrow<VarlenType<'a>> for Blob {
    fn borrow(&self) -> &VarlenType<'a> {
        unsafe {
            std::mem::transmute::<&VarlenType, &VarlenType<'a>>(
                &VarlenType {
                    length: self.0.len() as u16,
                    data: &self.0,
                }
            )
        }
    }
}



// Just for testing purposes
impl From<u32> for Blob {
    fn from(value: u32) -> Self {
        let vec = value.to_be_bytes();
        VarlenType::from_slice(&vec, 0, 4).to_owned()
    }
}
