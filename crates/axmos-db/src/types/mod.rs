//! Types module.
mod core;

use core::{AxmosCastable, AxmosValueType, AxmosValueTypeRef, AxmosValueTypeRefMut};

use axmos_derive::AxmosDataType;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;
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

    #[non_hash]
    Float(Float32),
    #[non_hash]
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

impl Eq for DataType {}

// Custom PartialEq and PartialOrd implementations.
impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::Null, DataType::Null) => true,

            (DataType::Blob(a), DataType::Blob(b)) => a == b,
            (DataType::Text(a), DataType::Text(b)) => a == b,
            (DataType::Text(a), DataType::Blob(b)) => a == b,
            (DataType::Blob(a), DataType::Text(b)) => a == b,
            (num_a, num_b) if num_a.is_numeric() && num_b.is_numeric() => {
                num_a.to_f64() == num_b.to_f64()
            }

            // Para el mismo tipo específico
            (DataType::SmallInt(a), DataType::SmallInt(b)) => a == b,
            (DataType::HalfInt(a), DataType::HalfInt(b)) => a == b,
            (DataType::Int(a), DataType::Int(b)) => a == b,
            (DataType::BigInt(a), DataType::BigInt(b)) => a == b,
            (DataType::SmallUInt(a), DataType::SmallUInt(b)) => a == b,
            (DataType::HalfUInt(a), DataType::HalfUInt(b)) => a == b,
            (DataType::UInt(a), DataType::UInt(b)) => a == b,
            (DataType::BigUInt(a), DataType::BigUInt(b)) => a == b,
            (DataType::Float(a), DataType::Float(b)) => a == b,
            (DataType::Double(a), DataType::Double(b)) => a == b,
            (DataType::Byte(a), DataType::Byte(b)) => a == b,
            (DataType::Boolean(a), DataType::Boolean(b)) => a == b,
            (DataType::Char(a), DataType::Char(b)) => a == b,
            (DataType::Date(a), DataType::Date(b)) => a == b,
            (DataType::DateTime(a), DataType::DateTime(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DataType::Null, DataType::Null) => Some(Ordering::Equal),
            (DataType::Null, _) => Some(Ordering::Less),
            (_, DataType::Null) => Some(Ordering::Greater),

            (DataType::Blob(a), DataType::Blob(b)) => a.partial_cmp(b),
            (DataType::Text(a), DataType::Text(b)) => a.partial_cmp(b),
            (DataType::Text(a), DataType::Blob(b)) => a.partial_cmp(b),
            (DataType::Blob(a), DataType::Text(b)) => a.partial_cmp(b),

            (num_a, num_b) if num_a.is_numeric() && num_b.is_numeric() => {
                num_a.to_f64().partial_cmp(&num_b.to_f64())
            }

            (DataType::SmallInt(a), DataType::SmallInt(b)) => a.partial_cmp(b),
            (DataType::HalfInt(a), DataType::HalfInt(b)) => a.partial_cmp(b),
            (DataType::Int(a), DataType::Int(b)) => a.partial_cmp(b),
            (DataType::BigInt(a), DataType::BigInt(b)) => a.partial_cmp(b),
            (DataType::SmallUInt(a), DataType::SmallUInt(b)) => a.partial_cmp(b),
            (DataType::HalfUInt(a), DataType::HalfUInt(b)) => a.partial_cmp(b),
            (DataType::UInt(a), DataType::UInt(b)) => a.partial_cmp(b),
            (DataType::BigUInt(a), DataType::BigUInt(b)) => a.partial_cmp(b),
            (DataType::Float(a), DataType::Float(b)) => a.partial_cmp(b),
            (DataType::Double(a), DataType::Double(b)) => a.partial_cmp(b),
            (DataType::Byte(a), DataType::Byte(b)) => a.partial_cmp(b),
            (DataType::Boolean(a), DataType::Boolean(b)) => a.partial_cmp(b),
            (DataType::Char(a), DataType::Char(b)) => a.partial_cmp(b),
            (DataType::Date(a), DataType::Date(b)) => a.partial_cmp(b),
            (DataType::DateTime(a), DataType::DateTime(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the enum discriminant first.
        mem::discriminant(self).hash(state);

        match self {
            DataType::Null => {}
            DataType::SmallInt(v) => i8::from(*v).hash(state),
            DataType::HalfInt(v) => i16::from(*v).hash(state),
            DataType::Int(v) => i32::from(*v).hash(state),
            DataType::BigInt(v) => i64::from(*v).hash(state),
            DataType::SmallUInt(v) => u8::from(*v).hash(state),
            DataType::HalfUInt(v) => u16::from(*v).hash(state),
            DataType::UInt(v) => u32::from(*v).hash(state),
            DataType::BigUInt(v) => u64::from(*v).hash(state),

            // IMPORTANT: For floats we need to handle the possibility of NaNs appearing
            DataType::Float(v) => {
                let f = f32::from(*v);
                if f.is_nan() {
                    0x7FC00000u32.hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            DataType::Double(v) => {
                let f = f64::from(*v);
                if f.is_nan() {
                    // All nans should have the same hashed representation
                    0x7FF8000000000000u64.hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }

            DataType::Byte(v) => u8::from(*v).hash(state),
            DataType::Boolean(v) => bool::from(*v).hash(state),
            DataType::Char(v) => char::from(*v).hash(state),

            DataType::Blob(b) => b.content().hash(state),
            DataType::Text(b) => b.content().hash(state),
            DataType::Date(d) => u32::from(*d).hash(state),
            DataType::DateTime(dt) => u64::from(*dt).hash(state),
        }
    }
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

#[cfg(test)]
mod kind_enum_tests {
    use super::*;

    #[test]
    fn test_datatype_kind_methods() {
        // Test is_numeric()
        assert!(!DataTypeKind::Null.is_numeric());
        assert!(DataTypeKind::SmallInt.is_numeric()); // Int8
        assert!(DataTypeKind::HalfInt.is_numeric()); // Int16
        assert!(DataTypeKind::Int.is_numeric()); // Int32
        assert!(DataTypeKind::BigInt.is_numeric()); // Int64
        assert!(DataTypeKind::SmallUInt.is_numeric()); // UInt8
        assert!(DataTypeKind::HalfUInt.is_numeric()); // UInt16
        assert!(DataTypeKind::UInt.is_numeric()); // UInt32
        assert!(DataTypeKind::BigUInt.is_numeric()); // UInt64
        assert!(DataTypeKind::Float.is_numeric()); // Float32
        assert!(DataTypeKind::Double.is_numeric()); // Float64
        assert!(DataTypeKind::Byte.is_numeric()); // UInt8
        assert!(!DataTypeKind::Blob.is_numeric());
        assert!(!DataTypeKind::Text.is_numeric());
        assert!(DataTypeKind::Date.is_numeric()); // Date
        assert!(DataTypeKind::Char.is_numeric()); // UInt8
        assert!(DataTypeKind::Boolean.is_numeric()); // UInt8
        assert!(DataTypeKind::DateTime.is_numeric()); // DateTime

        // Test is_fixed_size()
        assert!(DataTypeKind::Null.is_fixed_size()); // Size 0
        assert!(DataTypeKind::SmallInt.is_fixed_size()); // 1 byte
        assert!(DataTypeKind::HalfInt.is_fixed_size()); // 2 bytes
        assert!(DataTypeKind::Int.is_fixed_size()); // 4 bytes
        assert!(DataTypeKind::BigInt.is_fixed_size()); // 8 bytes
        assert!(DataTypeKind::SmallUInt.is_fixed_size()); // 1 byte
        assert!(DataTypeKind::HalfUInt.is_fixed_size()); // 2 bytes
        assert!(DataTypeKind::UInt.is_fixed_size()); // 4 bytes
        assert!(DataTypeKind::BigUInt.is_fixed_size()); // 8 bytes
        assert!(DataTypeKind::Float.is_fixed_size()); // 4 bytes
        assert!(DataTypeKind::Double.is_fixed_size()); // 8 bytes
        assert!(DataTypeKind::Byte.is_fixed_size()); // 1 byte
        assert!(!DataTypeKind::Blob.is_fixed_size()); // Variable size
        assert!(!DataTypeKind::Text.is_fixed_size()); // Variable size
        assert!(DataTypeKind::Date.is_fixed_size()); // 4 bytes
        assert!(DataTypeKind::Char.is_fixed_size()); // 1 byte
        assert!(DataTypeKind::Boolean.is_fixed_size()); // 1 byte
        assert!(DataTypeKind::DateTime.is_fixed_size()); // 8 bytes
    }

    #[test]
    fn test_datatype_kind_is_signed() {
        assert!(!DataTypeKind::Null.is_signed());
        assert!(DataTypeKind::SmallInt.is_signed()); // Int8 signed
        assert!(DataTypeKind::HalfInt.is_signed()); // Int16 signed
        assert!(DataTypeKind::Int.is_signed()); // Int32 signed
        assert!(DataTypeKind::BigInt.is_signed()); // Int64 signed
        assert!(!DataTypeKind::SmallUInt.is_signed()); // UInt8 unsigned
        assert!(!DataTypeKind::HalfUInt.is_signed()); // UInt16 unsigned
        assert!(!DataTypeKind::UInt.is_signed()); // UInt32 unsigned
        assert!(!DataTypeKind::BigUInt.is_signed()); // UInt64 unsigned
        assert!(DataTypeKind::Float.is_signed()); // Float32 signed
        assert!(DataTypeKind::Double.is_signed()); // Float64 signed
        assert!(!DataTypeKind::Byte.is_signed()); // UInt8 (Byte) unsigned
        assert!(!DataTypeKind::Blob.is_signed());
        assert!(!DataTypeKind::Text.is_signed());
        assert!(!DataTypeKind::Date.is_signed()); // Date not signed
        assert!(!DataTypeKind::Char.is_signed()); // UInt8 (Char) unsigned
        assert!(!DataTypeKind::Boolean.is_signed()); // UInt8 (Boolean) unsigned
        assert!(!DataTypeKind::DateTime.is_signed()); // DateTime is unsigned
    }
}
