//! Types module.
pub(crate) mod core;

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    hash::Hash,
};

use axmos_derive::AxmosDataType;

pub mod blob;
pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod varint;

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

#[derive(AxmosDataType, Debug, Clone, Default)]
pub enum DataType {
    #[default]
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

    #[non_arith]
    #[non_copy]
    Blob(Blob),

    #[non_arith]
    #[non_copy]
    Text(Blob),

    Date(Date),

    Char(UInt8),

    Boolean(UInt8),

    DateTime(DateTime),
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
                let txt = b.to_string();
                write!(f, "Text(\"{}\")", txt)
            }

            DataType::Date(d) => write!(f, "Date({})", d.to_iso_string()),
            DataType::DateTime(dt) => write!(f, "DateTime({})", dt.to_iso_string()),
        }
    }
}

/// Macro for creating DataType values with less boilerplate
#[macro_export]
macro_rules! dt {
    (int $v:expr) => {
        $crate::types::DataType::Int(($v).into())
    };
    (bigint $v:expr) => {
        $crate::types::DataType::BigInt(($v).into())
    };
    (uint $v:expr) => {
        $crate::types::DataType::UInt(($v).into())
    };
    (biguint $v:expr) => {
        $crate::types::DataType::BigUInt(($v).into())
    };
    (double $v:expr) => {
        $crate::types::DataType::Double(($v).into())
    };
    (float $v:expr) => {
        $crate::types::DataType::Float(($v).into())
    };
    (text $v:expr) => {
        $crate::types::DataType::Text(($v).into())
    };
    (bool $v:expr) => {
        $crate::types::DataType::Boolean(if $v {
            $crate::types::UInt8::TRUE
        } else {
            $crate::types::UInt8::FALSE
        })
    };
    (null) => {
        $crate::types::DataType::Null
    };
    (blob $v:expr) => {
        $crate::types::DataType::Blob(($v).into())
    };
    (datetime $v:expr) => {
        $crate::types::DataType::DateTime(($v).into())
    };
}
