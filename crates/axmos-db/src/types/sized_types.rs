//! Fixed-size numeric types

use crate::{
    checked_conversion, direct_axmos_cast, float, signed_integer, to_blob,
    types::{Blob, Date, DateTime, core::AxmosCastable},
    unsigned_integer,
};

signed_integer! {

    pub struct Int8(i8);
}

signed_integer! {

    pub struct Int16(i16);
}

signed_integer! {

    pub struct Int32(i32);
}

signed_integer! {

    pub struct Int64(i64);
}

unsigned_integer! {

    pub struct UInt8(u8);
}

unsigned_integer! {

    pub struct UInt16(u16);
}

unsigned_integer! {

    pub struct UInt32(u32);
}

unsigned_integer! {

    pub struct UInt64(u64);
}

float! {
    pub struct Float32(f32);
}

float! {
    pub struct Float64(f64);
}

impl From<Float32> for f64 {
    fn from(value: Float32) -> Self {
        value.0 as f64
    }
}

// Additional UInt8 methods for character/boolean handling
// We cannot create raw char/bool wrappers because their internal bit representation may be illegal (due to bytemuck Pod trait: https://docs.rs/bytemuck/latest/bytemuck/trait.Pod.html)
impl UInt8 {
    pub const TRUE: Self = Self(1);
    pub const FALSE: Self = Self(0);
    pub const NULL: Self = Self(b'\0');
    pub const MAX: Self = Self(0xFF);
    pub const REPLACEMENT: Self = Self(0x1A);

    pub const fn from_ascii(byte: u8) -> Option<Self> {
        if byte <= 127 { Some(Self(byte)) } else { None }
    }

    pub const fn is_ascii(self) -> bool {
        self.0 <= 127
    }

    pub const fn is_true(self) -> bool {
        self.0 == Self::TRUE.0
    }

    pub const fn is_false(self) -> bool {
        !self.is_true()
    }

    pub const fn to_ascii(self) -> Option<u8> {
        if self.is_ascii() { Some(self.0) } else { None }
    }

    pub fn is_whitespace(self) -> bool {
        matches!(self.0, b' ' | b'\t' | b'\n' | b'\r' | 0x0C)
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

    pub const fn as_byte(self) -> u8 {
        self.0
    }

    pub fn to_char(self) -> char {
        self.0 as char
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
}

impl From<bool> for UInt8 {
    fn from(value: bool) -> Self {
        if value { Self::TRUE } else { Self::FALSE }
    }
}

impl From<UInt8> for bool {
    fn from(value: UInt8) -> Self {
        value != UInt8::FALSE
    }
}

impl From<UInt8> for char {
    fn from(value: UInt8) -> Self {
        value.to_char()
    }
}

direct_axmos_cast!(
    Int8[i8]  => Int16[i16], Int32[i32], Int64[i64], UInt8[u8], UInt16[u16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    Int16[i16]  => Int8[i8], Int32[i32], Int64[i64], UInt8[u8], UInt16[u16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    Int32[i32]  => Int8[i8], Int16[i16], Int64[i64], UInt8[u8], UInt16[u16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    Int64[i64]  => Int8[i8], Int16[i16], Int32[i32], UInt8[u8], UInt16[u16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    UInt8[u8]  => Int16[i16], Int32[i32], Int64[i64], Int8[i8], UInt16[u16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    UInt16[u16]  => Int8[i8], Int32[i32], Int64[i64], UInt8[u8], Int16[i16], UInt32[u32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    UInt32[u32]  => Int8[i8], Int16[i16], Int64[i64], UInt8[u8], UInt16[u16], Int32[i32], UInt64[u64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    UInt64[u64]  => Int8[i8], Int16[i16], Int32[i32], UInt8[u8], UInt16[u16], UInt32[u32], Int64[i64], Float32[f32], Float64[f64], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    Float32[f32]  => Int8[i8], Int16[i16], Int64[i64], UInt8[u8], UInt16[u16], Int32[i32], UInt64[u64], UInt32[u32], DateTime[u64], Date[u32]
);

direct_axmos_cast!(
    Float64[f64]  => Int8[i8], Int16[i16], Int32[i32], UInt8[u8], UInt16[u16], UInt32[u32], Int64[i64], UInt64[u64], DateTime[u64], Date[u32]
);

checked_conversion!(f64 => f32);
checked_conversion!(f32 => f64);

impl AxmosCastable<Float32> for Float64 {
    fn can_cast(&self) -> bool {
        true
    }

    fn try_cast(&self) -> Option<Float32> {
        f64_to_f32_checked(self.0).map(Float32::from)
    }
}

impl AxmosCastable<Float64> for Float32 {
    fn can_cast(&self) -> bool {
        true
    }

    fn try_cast(&self) -> Option<Float64> {
        f32_to_f64_checked(self.0).map(Float64::from)
    }
}

to_blob!(
    UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64
);
