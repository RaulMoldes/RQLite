//! Types module.
use crate::serialization::Serializable;

use std::cmp::Ord;
use std::fmt::Display;

pub mod bool;
pub mod byte;
pub mod char;
pub mod date;
pub mod datetime;
pub mod id;
pub mod numeric;
pub mod varchar;
pub mod varint;
pub mod varlena;
pub use byte::Byte;
pub use date::Date;

#[allow(unused_imports)]
pub use datetime::DateTime;
pub use id::*;
#[allow(unused_imports)]
pub use numeric::*;
#[allow(unused_imports)]
pub use varchar::Varchar;
#[allow(unused_imports)]
pub use varint::Varint;
pub use varlena::VarlenaType;

#[cfg(test)]
mod tests;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DataTypeMarker {
    Null = 0,
    VarInt = 1,
    SmallInt = 2,
    HalfInt = 3,
    Int = 4,
    Bigint = 5,
    SmallUInt = 6,
    HalfUInt = 7,
    UInt = 8,
    BigUInt = 9,
    Float = 10,
    Double = 11,
    Byte = 12,
    Blob = 13,
    Varchar = 14,
    Text = 15,
    Key = 16,
    Date = 17,
    Char = 18,
    Boolean = 19,
    DateTime = 20,
}

pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

pub(crate) trait DataType: Serializable + Ord + PartialEq + Eq + Display {
    fn _type_of(&self) -> DataTypeMarker {
        DataTypeMarker::Null
    }

    fn size_of(&self) -> u16;
}

pub(crate) trait Splittable {
    fn split_at(&mut self, offset: u16) -> Self;
    fn merge_with(&mut self, other: Self);
}
