//! Types module.
use std::cmp::Ord;
use std::mem::size_of;

pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod special_types;
pub mod varlen_types;

pub use date::Date;
pub use datetime::DateTime;
pub use id::*;
pub use sized_types::{
    Bool, Byte, Char, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
};

pub use special_types::VarInt;
pub use varlen_types::Blob;

#[cfg(test)]
mod tests;

pub(crate) enum DataType {
    Null,
    VarInt(VarInt),
    SmallInt(Int8),
    HalfInt(Int16),
    Int(Int32),
    BigInt(Int64),
    SmallUInt(UInt8),
    HalfUInt(UInt16),
    UInt(UInt32),
    BigUInt(UInt64),
    Float(Float32),
    Double(Float64),
    Byte(Byte),
    Blob(Blob),
    Varchar(Blob),
    Text(Blob),
    Date(Date),
    Char(Char),
    Boolean(Bool),
    DateTime(DateTime),
    RowId(RowId),
    PageId(PageId),
    TxId(TxId),
    LogId(LogId),
}

impl DataType {
    fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            DataType::BigUInt(_)
                | DataType::BigInt(_)
                | DataType::SmallInt(_)
                | DataType::SmallUInt(_)
                | DataType::UInt(_)
                | DataType::Int(_)
                | DataType::HalfInt(_)
                | DataType::HalfUInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
                | DataType::Char(_)
                | DataType::Boolean(_)
                | DataType::Byte(_)
                | DataType::DateTime(_)
                | DataType::Date(_)
                | DataType::LogId(_)
                | DataType::PageId(_)
                | DataType::RowId(_)
                | DataType::TxId(_)
        )
    }

    fn size(&self) -> usize {
        match self {
            // Nulls and variable integers
            DataType::Null => 0,
            DataType::VarInt(v) => v.size(),

            // Signed integers
            DataType::SmallInt(_) => size_of::<i8>(),
            DataType::HalfInt(_) => size_of::<i16>(),
            DataType::Int(_) => size_of::<i32>(),
            DataType::BigInt(_) => size_of::<i64>(),

            // Unsigned integers
            DataType::SmallUInt(_) => size_of::<u8>(),
            DataType::HalfUInt(_) => size_of::<u16>(),
            DataType::UInt(_) => size_of::<u32>(),
            DataType::BigUInt(_) => size_of::<u64>(),

            // Floating point
            DataType::Float(_) => size_of::<f32>(),
            DataType::Double(_) => size_of::<f64>(),

            // Id types (4bytes)
            DataType::RowId(_) | DataType::PageId(_) | DataType::LogId(_) | DataType::TxId(_) => {
                size_of::<u32>()
            }

            // Scalar 1-byte values
            DataType::Byte(_) | DataType::Char(_) | DataType::Boolean(_) => size_of::<u8>(),

            // Date/time values
            DataType::Date(_) => size_of::<u32>(),
            DataType::DateTime(_) => size_of::<u64>(),

            // Variable-length types (no prefix)
            DataType::Blob(b) | DataType::Varchar(b) | DataType::Text(b) => b.as_ref().len(),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }
}
pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

pub trait SizedType: Ord + Eq + PartialEq + PartialOrd {
    fn size(&self) -> usize;
}
