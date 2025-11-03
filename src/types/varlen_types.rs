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

/// Owned wrapper for variable-length data
/// Just holds the raw bytes (Excluding length prefix)
#[derive(Debug, Clone)]
pub struct Blob(Box<[u8]>);

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Blob {
    /// Create a new Blob from raw data
    pub fn new(data: &[u8]) -> Self {
        let len_bytes = VarInt::from(data.len()).to_bytes();
        let mut buffer = Vec::with_capacity(len_bytes.len() + data.len());
        buffer.extend_from_slice(&len_bytes);
        buffer.extend_from_slice(data);
        Self(buffer.into_boxed_slice())
    }

    pub fn data(&self) -> &[u8] {
        let (len, offset) = VarInt::from_bytes(self.0.as_ref());
        let len_usize: usize = len.try_into().unwrap();
        &self.as_ref()[offset..offset + len_usize]
    }

    /// Get the length of the data (excluding prefix)
    pub fn length(&self) -> usize {
        let (len, offset) = VarInt::from_bytes(self.0.as_ref());
        len.try_into().unwrap()
    }

    pub fn data_offset(&self) -> usize {
        let (len, offset) = VarInt::from_bytes(self.0.as_ref());
        offset
    }

    pub fn to_string(&self, encoding: TextEncoding) -> String {
        match encoding {
            TextEncoding::Utf8 => {
                String::from_utf8(self.data().to_vec()).unwrap()
            }
            TextEncoding::Utf16be => {
                assert!(self.length().is_power_of_two(), "UTF-16BE blob length must be multiple of 2");
                let u16_iter = self
                    .data()
                    .chunks_exact(2)
                    .map(|pair| u16::from_be_bytes([pair[0], pair[1]]));
                String::from_utf16(&u16_iter.collect::<Vec<_>>()).unwrap()
            }
            TextEncoding::Utf16le => {
                assert!(self.length().is_power_of_two(), "UTF-16LE blob length must be multiple of 2");
                let u16_iter = self
                    .data()
                    .chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]));
                String::from_utf16(&u16_iter.collect::<Vec<_>>()).unwrap()
            }
        }
    }


    pub fn as_utf8(&self) -> &[u8] {
        self.data()
    }


    pub fn as_str(&self, encoding: TextEncoding) -> &str {
        match encoding {
            TextEncoding::Utf8 => std::str::from_utf8(self.as_utf8()).unwrap(),
            _ => panic!("Can only convert to string directly using utf8 encoding")
        }
    }

    pub fn as_utf16(&self, encoding: TextEncoding) -> &[u16] {
        match encoding {
        TextEncoding::Utf16be => {
            assert!(self.length() % 2 == 0);
             unsafe {
                std::slice::from_raw_parts(self.data().as_ptr() as *const u16, self.length()/2)
            }
        }
        TextEncoding::Utf16le => {
            assert!(self.length() % 2 == 0);
            unsafe {
                std::slice::from_raw_parts(self.data().as_ptr() as *const u16, self.length()/2)
            }
        }
        TextEncoding::Utf8 => panic!("Use [as_utf8()] for UTF-8"),
    }

    }
}

impl From<(String, TextEncoding)> for Blob {
    fn from(value: (String, TextEncoding)) -> Self {
        match value.1 {
            TextEncoding::Utf8 => Self::new(value.0.as_bytes()),
            TextEncoding::Utf16be => {
                let buf: Box<[u8]> = value
                    .0
                    .encode_utf16()
                    .flat_map(|ch| ch.to_be_bytes())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();

                Self(buf)
            }
            TextEncoding::Utf16le =>  {
                let buf: Box<[u8]> = value
                    .0
                    .encode_utf16()
                    .flat_map(|ch| ch.to_le_bytes())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();

                Self(buf)
            },
        }
    }
}
