use crate::serialization::Serializable;
use crate::types::{DataType, DataTypeMarker};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Char(char);

impl Char {
    pub const NULL: Self = Self('\0');
    pub const MAX: Self = Self(char::MAX);
    pub const REPLACEMENT: Self = Self(char::REPLACEMENT_CHARACTER);

    pub const fn new(value: char) -> Self {
        Self(value)
    }

    pub const fn value(self) -> char {
        self.0
    }

    /// Create from a byte (ASCII)
    pub const fn from_ascii(byte: u8) -> Option<Self> {
        if byte <= 127 {
            Some(Self(byte as char))
        } else {
            None
        }
    }

    /// Try to create from a u32 code point
    pub fn from_u32(code: u32) -> Option<Self> {
        char::from_u32(code).map(Self)
    }

    /// Get the Unicode code point
    pub const fn to_u32(self) -> u32 {
        self.0 as u32
    }

    /// Convert to UTF-8 bytes
    pub fn to_utf8_bytes(self) -> Vec<u8> {
        let mut buf = vec![0; self.0.len_utf8()];
        self.0.encode_utf8(&mut buf);
        buf
    }

    /// Check if this is an ASCII character
    pub const fn is_ascii(self) -> bool {
        self.0.is_ascii()
    }

    /// Get ASCII value if this is an ASCII character
    pub const fn to_ascii(self) -> Option<u8> {
        if self.is_ascii() {
            Some(self.0 as u8)
        } else {
            None
        }
    }

    pub const fn is_whitespace(self) -> bool {
        self.0.is_whitespace()
    }

    pub const fn is_lowercase(self) -> bool {
        self.0.is_lowercase()
    }

    pub const fn is_uppercase(self) -> bool {
        self.0.is_uppercase()
    }

    pub fn to_lowercase(self) -> impl Iterator<Item = Char> {
        self.0.to_lowercase().map(Char)
    }

    pub fn to_uppercase(self) -> impl Iterator<Item = Char> {
        self.0.to_uppercase().map(Char)
    }

    pub const fn to_ascii_lowercase(self) -> Self {
        Self(self.0.to_ascii_lowercase())
    }

    pub const fn to_ascii_uppercase(self) -> Self {
        Self(self.0.to_ascii_uppercase())
    }

    pub fn to_digit(self, radix: u32) -> Option<u32> {
        self.0.to_digit(radix)
    }

    pub const fn len_utf8(self) -> usize {
        self.0.len_utf8()
    }

    pub const fn len_utf16(self) -> usize {
        self.0.len_utf16()
    }
}

impl DataType for Char {
    fn _type_of(&self) -> DataTypeMarker {
        DataTypeMarker::Char
    }

    fn size_of(&self) -> u16 {
        4 // Stored as UTF-32 (4 bytes)
    }
}

impl std::fmt::Display for Char {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Char: '{}'", self.0.escape_default())
    }
}

impl Serializable for Char {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        // Read as UTF-32 (4 bytes, big-endian)
        let mut bytes = [0u8; 4];
        reader.read_exact(&mut bytes)?;
        let code_point = u32::from_be_bytes(bytes);

        char::from_u32(code_point).map(Char).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Unicode code point: 0x{code_point}"),
            )
        })
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        // Write as UTF-32 (4 bytes, big-endian)
        let bytes = (self.0 as u32).to_be_bytes();
        writer.write_all(&bytes)?;
        Ok(())
    }
}

// Conversions
impl From<char> for Char {
    fn from(value: char) -> Self {
        Self(value)
    }
}

impl From<Char> for char {
    fn from(c: Char) -> Self {
        c.0
    }
}

impl TryFrom<u32> for Char {
    type Error = std::char::CharTryFromError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        char::try_from(value).map(Self)
    }
}

impl From<Char> for u32 {
    fn from(c: Char) -> Self {
        c.0 as u32
    }
}

impl TryFrom<u8> for Char {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value <= 127 {
            Ok(Self(value as char))
        } else {
            Err("Value is not valid ASCII")
        }
    }
}

// Allow pattern matching with char literals
impl PartialEq<char> for Char {
    fn eq(&self, other: &char) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Char> for char {
    fn eq(&self, other: &Char) -> bool {
        *self == other.0
    }
}
