//! Types module.
mod core;

use core::{AxmosValueType, AxmosValueTypeRef, AxmosValueTypeRefMut};

use axmos_derive::AxmosDataType;
use std::{cmp::Ord, fmt};

pub mod blob;
pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod varint;

use crate::TextEncoding;
pub use blob::{Blob, BlobRef, BlobRefMut};
pub use date::{Date, DateRef, DateRefMut};
pub use datetime::{DateTime, DateTimeRef, DateTimeRefMut};
pub use id::*;
pub use sized_types::{
    Float32, Float32Ref, Float32RefMut, Float64, Float64Ref, Float64RefMut, Int8, Int8Ref,
    Int8RefMut, Int16, Int16Ref, Int16RefMut, Int32, Int32Ref, Int32RefMut, Int64, Int64Ref,
    Int64RefMut, UInt8, UInt8Ref, UInt8RefMut, UInt16, UInt16Ref, UInt16RefMut, UInt32, UInt32Ref,
    UInt32RefMut, UInt64, UInt64Ref, UInt64RefMut,
};
pub use varint::VarInt;
#[cfg(test)]
mod tests;

#[derive(AxmosDataType, Debug, PartialEq, Clone)]
pub enum DataType {
    #[null]
    Null,

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

    Byte(UInt8),

    Blob(Blob),

    Text(Blob),

    Date(Date),

    Char(UInt8),

    Boolean(UInt8),

    DateTime(DateTime),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "Null"),

            DataType::SmallInt(v) => write!(f, "{}", v),
            DataType::HalfInt(v) => write!(f, "{}", v),
            DataType::Int(v) => write!(f, "{}", v),
            DataType::BigInt(v) => write!(f, "{}", v),

            DataType::SmallUInt(v) => write!(f, "{}", v),
            DataType::HalfUInt(v) => write!(f, "{}", v),
            DataType::UInt(v) => write!(f, "{}", v),
            DataType::BigUInt(v) => write!(f, "{}", v),

            DataType::Float(v) => write!(f, "{}", v),
            DataType::Double(v) => write!(f, "{}", v),

            DataType::Byte(v) => write!(f, "Byte({})", u8::from(*v)),
            DataType::Boolean(v) => write!(f, "Bool({})", bool::from(*v)),
            DataType::Char(v) => write!(f, "Char({})", char::from(*v)),

            DataType::Blob(b) => {
                let content = b.content();
                if content.len() <= 16 {
                    write!(f, "Blob({:02X?})", content)
                } else {
                    write!(f, "Blob({:02X?}... len={})", &content[..16], content.len())
                }
            }

            DataType::Text(b) => {
                let txt = b.to_string(TextEncoding::Utf8);
                write!(f, "Text(\"{}\")", txt)
            }

            DataType::Date(d) => write!(f, "Date({})", d.to_iso_string()),
            DataType::DateTime(dt) => write!(f, "DateTime({})", dt.to_iso_string()),
        }
    }
}
