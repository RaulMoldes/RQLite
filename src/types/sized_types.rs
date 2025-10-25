use crate::{float_scalar, scalar};

scalar! {
    pub struct Int8(i8);
}

scalar! {
    pub struct Int16(i16);
}

scalar! {
    pub struct Int32(i32);
}

scalar! {
    pub struct Int64(i64);
}

scalar! {
    pub struct UInt8(u8);
}

scalar! {
    pub struct UInt16(u16);
}

scalar! {
    pub struct UInt32(u32);
}

scalar! {
    pub struct UInt64(u64);
}

float_scalar! {
    pub struct Float32(f32);
}

float_scalar! {
    pub struct Float64(f64);
}

pub type Byte = UInt8;
pub type Char = UInt8;
pub type Bool = UInt8;

impl Char {
    pub const NULL: Self = Self(b'\0');
    pub const MAX: Self = Self(0xFF);
    pub const REPLACEMENT: Self = Self(0x1A);

    pub const fn from_ascii(byte: u8) -> Option<Self> {
        if byte <= 127 {
            Some(Self(byte))
        } else {
            None
        }
    }

    pub const fn is_ascii(self) -> bool {
        self.0 <= 127
    }

    pub const fn to_ascii(self) -> Option<u8> {
        if self.is_ascii() {
            Some(self.0)
        } else {
            None
        }
    }

    pub fn is_whitespace(self) -> bool {
        matches!(self.0, b' ' | b'\t' | b'\n' | b'\r' | 0x0C) // form feed
    }

    pub const fn is_lowercase(self) -> bool {
        self.0.is_ascii_lowercase()
    }

    pub const fn is_uppercase(self) -> bool {
        self.0.is_ascii_uppercase()
    }

    pub const fn to_ascii_lowercase(self) -> Self {
        Self(self.0.to_ascii_lowercase())
    }

    pub const fn to_ascii_uppercase(self) -> Self {
        Self(self.0.to_ascii_uppercase())
    }

    pub fn to_digit(self, radix: u32) -> Option<u32> {
        if radix > 36 {
            return None;
        }

        let c = char::from(self.0);
        if c.is_digit(radix) {
            c.to_digit(radix)
        } else {
            None
        }
    }

    pub const fn len_utf8(self) -> usize {
        1
    }

    pub const fn len_utf16(self) -> usize {
        1
    }

    pub const fn is_alphabetic(self) -> bool {
        self.0.is_ascii_alphabetic()
    }

    pub const fn is_alphanumeric(self) -> bool {
        self.0.is_ascii_alphanumeric()
    }

    pub const fn is_digit(self) -> bool {
        self.0.is_ascii_digit()
    }

    pub const fn is_hex_digit(self) -> bool {
        self.0.is_ascii_hexdigit()
    }

    pub const fn as_byte(self) -> u8 {
        self.0
    }

    pub fn to_char(self) -> char {
        self.0 as char
    }
}

impl Bool {
    pub const TRUE: Self = Self(1);
    pub const FALSE: Self = Self(0);

    pub const fn new(value: bool) -> Self {
        if value {
            Self::TRUE
        } else {
            Self::FALSE
        }
    }

    pub const fn value(self) -> bool {
        self.0 != 0
    }

    pub const fn is_true(self) -> bool {
        self.0 != 0
    }

    pub const fn is_false(self) -> bool {
        self.0 == 0
    }

    pub fn then<T>(self, f: impl FnOnce() -> T) -> Option<T> {
        if self.is_true() {
            Some(f())
        } else {
            None
        }
    }

    pub fn then_some<T>(self, value: T) -> Option<T> {
        if self.is_true() {
            Some(value)
        } else {
            None
        }
    }
    pub const fn from_bool(value: bool) -> Self {
        Self::new(value)
    }

    pub const fn to_bool(self) -> bool {
        self.value()
    }

    pub const fn from_u8(value: u8) -> Self {
        Self(if value != 0 { 1 } else { 0 })
    }

    pub const fn to_u8(self) -> u8 {
        self.0
    }
}

// Implementaciones de traits útiles
impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Bool::new(value)
    }
}

impl From<Bool> for bool {
    fn from(value: Bool) -> bool {
        value.value()
    }
}

impl From<char> for Char {
    fn from(c: char) -> Self {
        // Solo toma el byte menos significativo
        Self(c as u8)
    }
}

impl From<Char> for char {
    fn from(c: Char) -> char {
        c.0 as char
    }
}
