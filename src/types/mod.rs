//! Types module.
use std::cmp::Ord;


pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod varlen_types;
pub mod special_types;

pub use id::*;
pub use sized_types::{
    Bool, Byte, Char, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
};
pub use date::Date;
pub use datetime::DateTime;


pub use special_types::Varint;
pub use varlen_types::Blob;

#[cfg(test)]
mod tests;

pub(crate) enum DataType {
    Null,
    VarInt(Varint),
    SmallInt(Int8),
    HalfInt(Int16),
    Int(Int32),
    Bigint(Int64),
    SmallUInt(UInt8),
    HalfUInt(UInt16),
    UInt(UInt32),
    BigUInt(UInt64),
    Float(Float32),
    Double(Float64),
    Byte(Byte),
    Bool(Bool),
    Blob(Blob),
    Varchar(Blob),
    Text(Blob),
    Date(Date),
    Char(Char),
    Boolean(UInt8),
    DateTime(DateTime),
}

pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}
