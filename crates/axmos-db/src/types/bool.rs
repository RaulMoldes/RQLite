use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Not},
};

use crate::{
    blob::Blob,
    core::{
        BooleanType, DeserializableType, FixedSizeType, Hashable, RefTrait, SerializableType,
        TypeCast, TypeRef,
    },
    types::{
        SerializationError, SerializationResult,
        numeric::{Float32, Float64, Int32, Int64, UInt32, UInt64},
    },
};

/// We cannot implement [bytemuck] [Pod] or [Zeroable] for rust's bool type, as it can hold illegal representations. At first, I was goin to create a custom type using u8, but it overcomplicates everything. So for now, boolean types cannot be converted to references.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Default, Hash)]
pub struct Bool(pub bool);

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

    pub const fn and(self, other: Self) -> Self {
        Self(self.0 && other.0)
    }

    pub const fn or(self, other: Self) -> Self {
        Self(self.0 || other.0)
    }

    pub const fn not(self) -> Self {
        Self(!self.0)
    }

    pub const fn xor(self, other: Self) -> Self {
        Self(self.0 ^ other.0)
    }
}

impl BooleanType for Bool {}
impl FixedSizeType for Bool {}

impl Display for Bool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.0 {
            f.write_str("TRUE")
        } else {
            f.write_str("FALSE")
        }
    }
}

impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

// Bool cannot be Pod because bool has illegal bit patterns.
// Instead, we manually implement DeserializableType:

/// Reference type for Bool that reads from a u8
#[derive(Debug, Clone, Copy)]
pub struct BoolRef<'a>(&'a u8);

impl<'a> BoolRef<'a> {
    pub fn value(&self) -> bool {
        *self.0 != 0
    }

    pub fn to_owned(&self) -> Bool {
        Bool(*self.0 != 0)
    }
}

impl<'a> AsRef<[u8]> for BoolRef<'a> {
    fn as_ref(&self) -> &[u8] {
        std::slice::from_ref(self.0)
    }
}

impl<'a> TypeRef<'a> for BoolRef<'a> {
    type Owned = Bool;
    fn to_owned(&self) -> Bool {
        BoolRef::to_owned(self)
    }

    fn as_slice(&self) -> &[u8] {
        &[]
    }
}

impl RefTrait for Bool {
    type Ref<'a> = BoolRef<'a>;
}

impl DeserializableType for Bool {
    fn reinterpret_cast(buffer: &[u8]) -> SerializationResult<(Self::Ref<'_>, usize)> {
        if buffer.is_empty() {
            return Err(SerializationError::UnexpectedEof);
        }
        Ok((BoolRef(&buffer[0]), 1))
    }

    fn deserialize(buffer: &[u8], cursor: usize) -> SerializationResult<(Self::Ref<'_>, usize)> {
        let (data_ref, bytes_read) = Self::reinterpret_cast(&buffer[cursor..])?;
        Ok((data_ref, cursor + bytes_read))
    }
}

impl From<Bool> for bool {
    fn from(value: Bool) -> Self {
        value.0
    }
}

// Standard boolean operators
impl Not for Bool {
    type Output = Self;
    fn not(self) -> Self {
        Bool::not(self)
    }
}

impl BitAnd for Bool {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        self.and(rhs)
    }
}

impl BitOr for Bool {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        self.or(rhs)
    }
}

impl BitXor for Bool {
    type Output = Self;
    fn bitxor(self, rhs: Self) -> Self {
        self.xor(rhs)
    }
}

impl BitAndAssign for Bool {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = self.and(rhs);
    }
}

impl BitOrAssign for Bool {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = self.or(rhs);
    }
}

impl BitXorAssign for Bool {
    fn bitxor_assign(&mut self, rhs: Self) {
        *self = self.xor(rhs);
    }
}

impl TypeCast<Bool> for Float32 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0.0))
    }
}

impl TypeCast<Bool> for Float64 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0.0))
    }
}

impl TypeCast<Bool> for Int32 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0))
    }
}

impl TypeCast<Bool> for Int64 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0))
    }
}

impl TypeCast<Bool> for UInt32 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0))
    }
}

impl TypeCast<Bool> for UInt64 {
    fn try_cast(&self) -> Option<Bool> {
        Some(Bool(self.0 != 0))
    }
}

impl TypeCast<Int32> for Bool {
    fn try_cast(&self) -> Option<Int32> {
        Some(Int32(self.0 as i32))
    }
}

impl TypeCast<Int64> for Bool {
    fn try_cast(&self) -> Option<Int64> {
        Some(Int64(self.0 as i64))
    }
}

impl TypeCast<UInt32> for Bool {
    fn try_cast(&self) -> Option<UInt32> {
        Some(UInt32(self.0 as u32))
    }
}

impl TypeCast<UInt64> for Bool {
    fn try_cast(&self) -> Option<UInt64> {
        Some(UInt64(self.0 as u64))
    }
}

impl TypeCast<Float32> for Bool {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as u8 as f32))
    }
}

impl TypeCast<Float64> for Bool {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as u8 as f64))
    }
}

impl TypeCast<Blob> for Bool {
    fn try_cast(&self) -> Option<Blob> {
        Some(Blob::from_unencoded_slice(&[self.0 as u8]))
    }
}

impl Hashable for Bool {
    fn hash128(&self) -> u128 {
        self.0 as u8 as u128
    }

    fn hash64(&self) -> u64 {
        self.0 as u8 as u64
    }
}

impl SerializableType for Bool {
    fn write_to(&self, writer: &mut [u8], cursor: usize) -> SerializationResult<usize> {
        writer[cursor..].copy_from_slice(&[self.0 as u8]);
        Ok(cursor + 1)
    }
}
