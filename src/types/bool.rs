use crate::serialization::Serializable;
use crate::types::{Byte, DataType, DataTypeMarker};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bool(bool);

impl Bool {
    pub const TRUE: Self = Self(true);
    pub const FALSE: Self = Self(false);

    pub const fn new(value: bool) -> Self {
        Self(value)
    }

    pub const fn value(self) -> bool {
        self.0
    }

    pub const fn is_true(self) -> bool {
        self.0
    }

    pub const fn is_false(self) -> bool {
        !self.0
    }

    pub fn then<T>(self, f: impl FnOnce() -> T) -> Option<T> {
        if self.0 {
            Some(f())
        } else {
            None
        }
    }

    pub fn then_some<T>(self, value: T) -> Option<T> {
        if self.0 {
            Some(value)
        } else {
            None
        }
    }

    /// Convert to byte representation (0x00 for false, 0x01 for true)
    pub const fn to_byte(self) -> Byte {
        if self.0 {
            Byte::TRUE
        } else {
            Byte::FALSE
        }
    }

    /// Create from byte (0 is false, non-zero is true)
    pub const fn from_byte(byte: Byte) -> Self {
        Self(byte.0 != 0)
    }
}

impl DataType for Bool {
    fn _type_of(&self) -> DataTypeMarker {
        DataTypeMarker::Boolean
    }

    fn size_of(&self) -> u16 {
        1 // Stored as a single byte
    }
}

impl std::fmt::Display for Bool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bool: {}", if self.0 { "true" } else { "false" })
    }
}

impl Serializable for Bool {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        Ok(Bool(byte[0] != 0))
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        let byte = if self.0 { 0x01u8 } else { 0x00u8 };
        writer.write_all(&[byte])?;
        Ok(())
    }
}

// Logical operations for Bool
impl std::ops::Not for Bool {
    type Output = Self;

    fn not(self) -> Self::Output {
        Bool(!self.0)
    }
}

impl std::ops::BitAnd for Bool {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Bool(self.0 & rhs.0)
    }
}

impl std::ops::BitOr for Bool {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Bool(self.0 | rhs.0)
    }
}

impl std::ops::BitXor for Bool {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Bool(self.0 ^ rhs.0)
    }
}

// Conversions
impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl From<Bool> for bool {
    fn from(b: Bool) -> Self {
        b.0
    }
}

impl From<Byte> for Bool {
    fn from(byte: Byte) -> Self {
        Self::from_byte(byte)
    }
}

impl From<Bool> for Byte {
    fn from(b: Bool) -> Self {
        b.to_byte()
    }
}
