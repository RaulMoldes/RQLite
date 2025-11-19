//! Types module.
use axmos_derive::DataType;
use std::cmp::Ord;
use std::mem::size_of;

pub mod blob;
pub mod date;
pub mod datetime;
pub mod id;
pub mod sized_types;
pub mod varint;

use crate::repr_enum;
use crate::sql::ast::Expr;
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

use crate::database::errors::TypeError;

#[cfg(test)]
mod tests;



pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

#[derive(Debug, PartialEq, Clone, DataType)]
pub enum DataType {
    Null,

    #[fixed]
    SmallInt(Int8),

    #[fixed]
    HalfInt(Int16),

    #[fixed]
    Int(Int32),

    #[fixed]
    BigInt(Int64),

    #[fixed]
    SmallUInt(UInt8),

    #[fixed]
    HalfUInt(UInt16),

    #[fixed]
    UInt(UInt32),

    #[fixed]
    BigUInt(UInt64),

    #[fixed]
    Float(Float32),

    #[fixed]
    Double(Float64),

    #[fixed]
    Byte(UInt8),

    #[dynamic]
    Blob(Blob),

    #[dynamic]
    Text(Blob),

    #[fixed]
    Date(Date),

    #[fixed]
    Char(UInt8),

    #[fixed]
    Boolean(UInt8),

    #[fixed]
    DateTime(DateTime),
}

impl TryFrom<(DataTypeKind, Expr)> for DataType {
    type Error = TypeError;

    fn try_from(value: (DataTypeKind, Expr)) -> Result<Self, Self::Error> {
        use DataTypeKind as K;
        use Expr as E;

        match value {

            (K::Null, _) | (_, E::Null) => Ok(DataType::Null),
            (K::Boolean, E::Boolean(b)) => Ok(DataType::Boolean(UInt8::from(b as u8))),
            (K::Boolean, E::Number(n)) => Ok(DataType::Boolean(UInt8::from((n != 0.0) as u8))),
            (K::Boolean, E::String(s)) => match s.to_ascii_uppercase().as_str() {
                "TRUE" => Ok(DataType::Boolean(UInt8::TRUE)),
                "FALSE" => Ok(DataType::Boolean(UInt8::FALSE)),
                _ => Err(TypeError::Other),
            },
            (K::SmallInt, E::Number(n)) => Ok(DataType::SmallInt(Int8::from(n as i8))),
            (K::HalfInt, E::Number(n)) => Ok(DataType::HalfInt(Int16::from(n as i16))),
            (K::Int, E::Number(n)) => Ok(DataType::Int(Int32::from(n as i32))),
            (K::BigInt, E::Number(n)) => Ok(DataType::BigInt(Int64::from(n as i64))),
            (K::SmallUInt, E::Number(n)) => Ok(DataType::SmallUInt(UInt8::from(n as u8))),
            (K::HalfUInt, E::Number(n)) => Ok(DataType::HalfUInt(UInt16::from(n as u16))),
            (K::UInt, E::Number(n)) => Ok(DataType::UInt(UInt32::from(n as u32))),
            (K::BigUInt, E::Number(n)) => Ok(DataType::BigUInt(UInt64::from(n as u64))),
            (K::Float, E::Number(n)) => Ok(DataType::Float(Float32::from(n as f32))),
            (K::Double, E::Number(n)) => Ok(DataType::Double(Float64::from(n))),
            (K::Text, E::String(s)) => Ok(DataType::Text(Blob::from(s.as_bytes()))),
            (K::Blob, E::String(s)) => Ok(DataType::Blob(Blob::from(s.as_bytes()))),
            (K::Text, E::Number(n)) => Ok(DataType::Text(Blob::from(&n.to_ne_bytes()))),
            (K::Blob, E::Number(n)) => Ok(DataType::Blob(Blob::from(&n.to_ne_bytes()))),
            (K::Char, E::String(s)) if s.len() == 1 => {
                Ok(DataType::Char(UInt8::from(s.as_bytes()[0])))
            }
            (K::Char, E::Number(n)) => Ok(DataType::Char(UInt8::from(n as u8))),
            (K::Date, E::String(s)) => {
                let parsed = Date::parse_iso(&s).map_err(|e| TypeError::Other)?;
                Ok(DataType::Date(parsed))
            }
            (K::Date, E::Number(n)) => Ok(DataType::Date(Date::from(n as u32))),
            (K::DateTime, E::String(s)) => {
                let parsed = DateTime::parse_iso(&s).map_err(|e| TypeError::Other)?;
                Ok(DataType::DateTime(parsed))
            }
            (K::DateTime, E::Number(n)) => Ok(DataType::DateTime(DateTime::from(n as u64))),
            (K::SmallInt, E::String(s)) => Ok(DataType::SmallInt(Int8::from(
                s.parse::<i8>()?,
            ))),
            (K::HalfInt, E::String(s)) => Ok(DataType::HalfInt(Int16::from(
                s.parse::<i16>()?,
            ))),
            (K::Int, E::String(s)) => Ok(DataType::Int(Int32::from(
                s.parse::<i32>()?,
            ))),
            (K::BigInt, E::String(s)) => Ok(DataType::BigInt(Int64::from(
                s.parse::<i64>()?,
            ))),
            (K::SmallUInt, E::String(s)) => Ok(DataType::SmallUInt(UInt8::from(
                s.parse::<u8>()?,
            ))),
            (K::HalfUInt, E::String(s)) => Ok(DataType::HalfUInt(UInt16::from(
                s.parse::<u16>()?,
            ))),
            (K::UInt, E::String(s)) => Ok(DataType::UInt(UInt32::from(
                s.parse::<u32>()?,
            ))),
            (K::BigUInt, E::String(s)) => Ok(DataType::BigUInt(UInt64::from(
                s.parse::<u64>()?,
            ))),
            (K::Float, E::String(s)) => Ok(DataType::Float(Float32::from(
                s.parse::<f32>()?,
            ))),
            (K::Double, E::String(s)) => Ok(DataType::Double(Float64::from(
                s.parse::<f64>()?,
            ))),
            (kind, expr) => Err(TypeError::UnexpectedDataType(kind)),
        }
    }
}

pub fn reinterpret_cast<'a>(
    dtype: DataTypeKind,
    buffer: &'a [u8],
) -> std::io::Result<(DataTypeRef<'a>, usize)> {
    match dtype {
        DataTypeKind::Null => Ok((DataTypeRef::Null, 0)),
        DataTypeKind::Blob => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;

            let value = DataTypeRef::Blob(BlobRef::from_raw_bytes(
                &buffer[cursor..cursor + offset + len_usize],
            ));
            cursor += len_usize + offset;
            Ok((value, cursor))
        }

        DataTypeKind::Text => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;

            let value = DataTypeRef::Text(BlobRef::from_raw_bytes(
                &buffer[cursor..cursor + offset + len_usize],
            ));

            cursor += len_usize + offset;
            Ok((value, cursor))
        }
        DataTypeKind::BigInt => {
            let mut cursor = 0;
            let value = Int64Ref::try_from(&buffer[cursor..])?;
            cursor += Int64Ref::SIZE;
            Ok((DataTypeRef::BigInt(value), cursor))
        }
        DataTypeKind::Int => {
            let mut cursor = 0;
            let value = Int32Ref::try_from(&buffer[cursor..])?;
            cursor += Int32Ref::SIZE;
            Ok((DataTypeRef::Int(value), cursor))
        }
        DataTypeKind::HalfInt => {
            let mut cursor = 0;
            let value = Int16Ref::try_from(&buffer[cursor..])?;
            cursor += Int16Ref::SIZE;
            Ok((DataTypeRef::HalfInt(value), cursor))
        }
        DataTypeKind::SmallInt => {
            let mut cursor = 0;
            let value = Int8Ref::try_from(&buffer[cursor..])?;
            cursor += Int8Ref::SIZE;
            Ok((DataTypeRef::SmallInt(value), cursor))
        }
        DataTypeKind::BigUInt => {
            let mut cursor = 0;
            let value = UInt64Ref::try_from(&buffer[cursor..])?;
            cursor += UInt64Ref::SIZE;
            Ok((DataTypeRef::BigUInt(value), cursor))
        }
        DataTypeKind::UInt => {
            let mut cursor = 0;
            let value = UInt32Ref::try_from(&buffer[cursor..])?;
            cursor += UInt32Ref::SIZE;
            Ok((DataTypeRef::UInt(value), cursor))
        }
        DataTypeKind::HalfUInt => {
            let mut cursor = 0;
            let value = UInt16Ref::try_from(&buffer[cursor..])?;
            cursor += UInt16Ref::SIZE;
            Ok((DataTypeRef::HalfUInt(value), cursor))
        }
        DataTypeKind::SmallUInt => {
            let mut cursor = 0;
            let value = UInt8Ref::try_from(&buffer[cursor..])?;
            cursor += UInt8Ref::SIZE;
            Ok((DataTypeRef::SmallUInt(value), cursor))
        }
        DataTypeKind::Float => {
            let mut cursor = 0;
            let value = Float32Ref::try_from(&buffer[cursor..])?;
            cursor += Float32Ref::SIZE;
            Ok((DataTypeRef::Float(value), cursor))
        }
        DataTypeKind::Double => {
            let mut cursor = 0;
            let value = Float64Ref::try_from(&buffer[cursor..])?;
            cursor += Float64Ref::SIZE;
            Ok((DataTypeRef::Double(value), cursor))
        }
        DataTypeKind::Boolean => {
            let mut cursor = 0;
            let value = UInt8Ref::try_from(&buffer[cursor..])?;
            cursor += UInt8Ref::SIZE;
            Ok((DataTypeRef::Boolean(value), cursor))
        }
        DataTypeKind::Char => {
            let mut cursor = 0;
            let value = UInt8Ref::try_from(&buffer[cursor..])?;
            cursor += UInt8Ref::SIZE;
            Ok((DataTypeRef::Char(value), cursor))
        }
        DataTypeKind::Byte => {
            let mut cursor = 0;
            let value = UInt8Ref::try_from(&buffer[cursor..])?;
            cursor += UInt8Ref::SIZE;
            Ok((DataTypeRef::Byte(value), cursor))
        }
        DataTypeKind::Date => {
            let mut cursor = 0;
            let value = DateRef::try_from(&buffer[cursor..])?;
            cursor += DateRef::SIZE;
            Ok((DataTypeRef::Date(value), cursor))
        }
        DataTypeKind::DateTime => {
            let mut cursor = 0;
            let value = DateTimeRef::try_from(&buffer[cursor..])?;
            cursor += DateTimeRef::SIZE;
            Ok((DataTypeRef::DateTime(value), cursor))
        }
    }
}

pub fn reinterpret_cast_mut<'a>(
    dtype: DataTypeKind,
    buffer: &'a mut [u8],
) -> std::io::Result<(DataTypeRefMut<'a>, usize)> {
    match dtype {
        DataTypeKind::Null => Ok((DataTypeRefMut::Null, 0)),
        DataTypeKind::Blob => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;

            let value = DataTypeRefMut::Blob(BlobRefMut::from_raw_bytes(
                &mut buffer[cursor..cursor + offset + len_usize],
            ));
            cursor += len_usize + offset;

            Ok((value, cursor))
        }

        DataTypeKind::Text => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;
            cursor += offset;

            let value = DataTypeRefMut::Text(BlobRefMut::from_raw_bytes(
                &mut buffer[cursor..cursor + offset + len_usize],
            ));
            cursor += len_usize + offset;

            Ok((value, cursor))
        }
        DataTypeKind::BigInt => {
            let mut cursor = 0;
            let value = Int64RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Int64RefMut::SIZE;
            Ok((DataTypeRefMut::BigInt(value), cursor))
        }
        DataTypeKind::Int => {
            let mut cursor = 0;
            let value = Int32RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Int32RefMut::SIZE;
            Ok((DataTypeRefMut::Int(value), cursor))
        }
        DataTypeKind::HalfInt => {
            let mut cursor = 0;
            let value = Int16RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Int16RefMut::SIZE;
            Ok((DataTypeRefMut::HalfInt(value), cursor))
        }
        DataTypeKind::SmallInt => {
            let mut cursor = 0;
            let value = Int8RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Int8RefMut::SIZE;
            Ok((DataTypeRefMut::SmallInt(value), cursor))
        }
        DataTypeKind::BigUInt => {
            let mut cursor = 0;
            let value = UInt64RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt64RefMut::SIZE;
            Ok((DataTypeRefMut::BigUInt(value), cursor))
        }
        DataTypeKind::UInt => {
            let mut cursor = 0;
            let value = UInt32RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt32RefMut::SIZE;
            Ok((DataTypeRefMut::UInt(value), cursor))
        }
        DataTypeKind::HalfUInt => {
            let mut cursor = 0;
            let value = UInt16RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt16RefMut::SIZE;
            Ok((DataTypeRefMut::HalfUInt(value), cursor))
        }
        DataTypeKind::SmallUInt => {
            let mut cursor = 0;
            let value = UInt8RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt8RefMut::SIZE;
            Ok((DataTypeRefMut::SmallUInt(value), cursor))
        }
        DataTypeKind::Float => {
            let mut cursor = 0;
            let value = Float32RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Float32RefMut::SIZE;
            Ok((DataTypeRefMut::Float(value), cursor))
        }
        DataTypeKind::Double => {
            let mut cursor = 0;
            let value = Float64RefMut::try_from(&mut buffer[cursor..])?;
            cursor += Float64RefMut::SIZE;
            Ok((DataTypeRefMut::Double(value), cursor))
        }
        DataTypeKind::Boolean => {
            let mut cursor = 0;
            let value = UInt8RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt8RefMut::SIZE;
            Ok((DataTypeRefMut::Boolean(value), cursor))
        }
        DataTypeKind::Char => {
            let mut cursor = 0;
            let value = UInt8RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt8RefMut::SIZE;
            Ok((DataTypeRefMut::Char(value), cursor))
        }
        DataTypeKind::Byte => {
            let mut cursor = 0;
            let value = UInt8RefMut::try_from(&mut buffer[cursor..])?;
            cursor += UInt8RefMut::SIZE;
            Ok((DataTypeRefMut::Byte(value), cursor))
        }
        DataTypeKind::Date => {
            let mut cursor = 0;
            let value = DateRefMut::try_from(&mut buffer[cursor..])?;
            cursor += DateRefMut::SIZE;
            Ok((DataTypeRefMut::Date(value), cursor))
        }
        DataTypeKind::DateTime => {
            let mut cursor = 0;
            let value = DateTimeRefMut::try_from(&mut buffer[cursor..])?;
            cursor += DateTimeRefMut::SIZE;
            Ok((DataTypeRefMut::DateTime(value), cursor))
        }
    }
}





// TODO: Maybe we can include this in the derive macro to make all this cleaner overall.
repr_enum!(
pub enum DataTypeKind: u8 {
    SmallInt = 0,
    HalfInt = 1,
    Int = 2,
    BigInt = 3,
    SmallUInt = 4,
    HalfUInt = 5,
    UInt = 6,
    BigUInt = 7,
    Float = 8,
    Double = 9,
    Byte = 10,
    Char = 11,
    Boolean = 12,
    Date = 13,
    DateTime = 14,
    Blob = 15,
    Text = 16,
    Null = 17
}

);

impl DataTypeKind {
    pub fn is_fixed_size(&self) -> bool {
        !matches!(self, DataTypeKind::Blob | DataTypeKind::Text)
    }

    pub fn size(&self) -> Option<usize> {
        match self {
            DataTypeKind::Null => Some(0),
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

    pub fn can_be_coerced(&self, other: DataTypeKind) -> bool {
        if std::mem::discriminant(self) == std::mem::discriminant(&other) {
            return true;
        }

        if matches!(self, DataTypeKind::Null) {
            return true;
        }

        if matches!(
            other,
            DataTypeKind::Null | DataTypeKind::Blob | DataTypeKind::Text
        ) {
            return true;
        }

        false
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataTypeKind::SmallInt
                | DataTypeKind::HalfInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::SmallUInt
                | DataTypeKind::HalfUInt
                | DataTypeKind::UInt
                | DataTypeKind::BigUInt
                | DataTypeKind::Float
                | DataTypeKind::Double
                | DataTypeKind::Date
                | DataTypeKind::DateTime
        )
    }
}
