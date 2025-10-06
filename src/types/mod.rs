//! Types module.
use crate::serialization::Serializable;

use std::cmp::Ord;
use std::fmt::Display;

pub mod byte;
pub mod float32;
pub mod float64;
pub mod page_id;
pub mod row_id;
pub mod varint;
pub mod varlena;

use crate::impl_arithmetic_ops;
pub use byte::Byte;
pub use float32::Float32;
pub use float64::Float64;
pub use page_id::PageId;
pub use row_id::RowId;
pub use varint::Varint;
pub use varlena::VarlenaType;

#[cfg(test)]
mod tests;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RQLiteTypeMarker {
    /// 0: NULL
    Null = 0,
    /// 1: Varint (Variable-sized integer)
    Integer = 1,
    /// 2: 32 bit floating point number (single precision)
    Float32 = 2,
    /// 2: 64 bit floating point number (IEEE 754)
    Float64 = 3,
    /// 3: Byte type: used to represent booleans.
    Byte = 4,
    /// 12: BLOB with variable length specified by the following varint
    Blob = 5,
    /// 13: String with variable length specified by the following varint
    String = 6,
    /// Key: special type for page and row ids.
    Key = 7,
}

pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

pub(crate) trait RQLiteType: Serializable + Ord + PartialEq + Eq + Display {
    fn _type_of(&self) -> RQLiteTypeMarker {
        RQLiteTypeMarker::Null
    }

    fn size_of(&self) -> usize;
}

pub(crate) trait Splittable {
    fn split_at(&mut self, offset: usize) -> Self;
    fn merge_with(&mut self, other: Self);
}

impl_arithmetic_ops!(Float32);
impl_arithmetic_ops!(Float64);
impl_arithmetic_ops!(Varint);
