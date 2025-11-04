//! Types module.
use std::cmp::Ord;
use std::mem::size_of;

pub mod blob;
pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod varint;

pub use date::{Date, DateRef, DateRefMut};
pub use datetime::{DateTime, DateTimeRef, DateTimeRefMut};
pub use id::*;
pub use sized_types::{
    Float32, Float32Ref, Float32RefMut, Float64, Float64Ref, Float64RefMut, Int16, Int16Ref,
    Int16RefMut, Int32, Int32Ref, Int32RefMut, Int64, Int64Ref, Int64RefMut, Int8, Int8Ref,
    Int8RefMut, UInt16, UInt16Ref, UInt16RefMut, UInt32, UInt32Ref, UInt32RefMut, UInt64,
    UInt64Ref, UInt64RefMut, UInt8, UInt8Ref, UInt8RefMut,
};

pub use blob::{Blob, BlobRef, BlobRefMut};
pub use varint::VarInt;

use crate::def_data_type;
#[cfg(test)]
mod tests;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataTypeKind {
    SmallInt,
    HalfInt,
    Int,
    BigInt,
    SmallUInt,
    HalfUInt,
    UInt,
    BigUInt,
    Float,
    Double,
    Byte,
    Char,
    Boolean,
    Date,
    DateTime,
    Blob,
    Text,
}

impl DataTypeKind {
    pub fn is_fixed_size(&self) -> bool {
        !matches!(self, DataTypeKind::Blob | DataTypeKind::Text)
    }

    pub fn size(&self) -> Option<usize> {
        match self {
            DataTypeKind::SmallInt => Some(size_of::<i8>()),
            DataTypeKind::HalfInt => Some(size_of::<i16>()),
            DataTypeKind::Int => Some(size_of::<i32>()),
            DataTypeKind::BigInt => Some(size_of::<i64>()),
            DataTypeKind::SmallUInt
            | DataTypeKind::Byte
            | DataTypeKind::Char
            | DataTypeKind::Boolean => Some(size_of::<u8>()),
            DataTypeKind::HalfUInt => Some(size_of::<u16>()),
            DataTypeKind::UInt | DataTypeKind::Date => Some(size_of::<u32>()),
            DataTypeKind::BigUInt | DataTypeKind::DateTime => Some(size_of::<u64>()),
            DataTypeKind::Float => Some(size_of::<f32>()),
            DataTypeKind::Double => Some(size_of::<f64>()),
            DataTypeKind::Blob | DataTypeKind::Text => None,
        }
    }
}

pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

def_data_type!(DataTypeRef, Ref);
def_data_type!(DataTypeRefMut, RefMut);
def_data_type!(DataType, Owned);

impl AsRef<[u8]> for DataType {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Null => &[],
            Self::SmallInt(p) => p.as_ref(),
            Self::HalfInt(p) => p.as_ref(),
            Self::Int(p) => p.as_ref(),
            Self::BigInt(p) => p.as_ref(),
            Self::SmallUInt(p) => p.as_ref(),
            Self::HalfUInt(p) => p.as_ref(),
            Self::UInt(p) => p.as_ref(),
            Self::BigUInt(p) => p.as_ref(),
            Self::Float(p) => p.as_ref(),
            Self::Double(p) => p.as_ref(),
            Self::Byte(p) | Self::Char(p) | Self::Boolean(p) => p.as_ref(),
            Self::Date(p) => p.as_ref(),
            Self::DateTime(p) => p.as_ref(),
            Self::Blob(b) | Self::Text(b) => b.as_ref(),
        }
    }
}

impl<'a> AsRef<[u8]> for DataTypeRef<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Null => &[],
            Self::SmallInt(p) => p.as_ref(),
            Self::HalfInt(p) => p.as_ref(),
            Self::Int(p) => p.as_ref(),
            Self::BigInt(p) => p.as_ref(),
            Self::SmallUInt(p) => p.as_ref(),
            Self::HalfUInt(p) => p.as_ref(),
            Self::UInt(p) => p.as_ref(),
            Self::BigUInt(p) => p.as_ref(),
            Self::Float(p) => p.as_ref(),
            Self::Double(p) => p.as_ref(),
            Self::Byte(p) | Self::Char(p) | Self::Boolean(p) => p.as_ref(),
            Self::Date(p) => p.as_ref(),
            Self::DateTime(p) => p.as_ref(),
            Self::Blob(b) | Self::Text(b) => b.as_ref(),
        }
    }
}

impl DataTypeKind {
    pub fn alignment(&self) -> usize {
        match self {
            DataTypeKind::BigInt | DataTypeKind::BigUInt | DataTypeKind::DateTime => 8,
            DataTypeKind::Int | DataTypeKind::UInt | DataTypeKind::Float | DataTypeKind::Date => 4,
            DataTypeKind::HalfInt | DataTypeKind::HalfUInt => 2,
            DataTypeKind::SmallInt
            | DataTypeKind::SmallUInt
            | DataTypeKind::Byte
            | DataTypeKind::Char
            | DataTypeKind::Boolean => 1,
            DataTypeKind::Double => 8,
            DataTypeKind::Blob | DataTypeKind::Text => 1,
        }
    }
}
