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
    Float32, Float32Ref, Float32RefMut, Float64, Float64Ref, Float64RefMut, Int8, Int8Ref,
    Int8RefMut, Int16, Int16Ref, Int16RefMut, Int32, Int32Ref, Int32RefMut, Int64, Int64Ref,
    Int64RefMut, UInt8, UInt8Ref, UInt8RefMut, UInt16, UInt16Ref, UInt16RefMut, UInt32, UInt32Ref,
    UInt32RefMut, UInt64, UInt64Ref, UInt64RefMut,
};

use crate::sql::ast::Expr;
use crate::{def_data_type, repr_enum};
pub use blob::{Blob, BlobRef, BlobRefMut};

use crate::structures::bplustree::{FixedSizeComparator, VarlenComparator};
pub use varint::VarInt;
#[cfg(test)]
mod tests;

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
}

pub(crate) trait Key:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + std::hash::Hash
{
    fn new_key() -> Self;
}

def_data_type!(DataTypeRef, Ref);
def_data_type!(DataTypeRefMut, RefMut);
def_data_type!(DataType, Owned);

impl DataType {
    pub fn datatype(&self) -> DataTypeKind {
        match self {
            DataType::SmallInt(_) => DataTypeKind::SmallInt,
            DataType::HalfInt(_) => DataTypeKind::HalfInt,
            DataType::Int(_) => DataTypeKind::Int,
            DataType::BigInt(_) => DataTypeKind::BigInt,
            DataType::SmallUInt(_) => DataTypeKind::SmallUInt,
            DataType::HalfUInt(_) => DataTypeKind::HalfUInt,
            DataType::UInt(_) => DataTypeKind::UInt,
            DataType::BigUInt(_) => DataTypeKind::BigUInt,
            DataType::Float(_) => DataTypeKind::Float,
            DataType::Double(_) => DataTypeKind::Double,
            DataType::Byte(_) => DataTypeKind::Byte,
            DataType::Char(_) => DataTypeKind::Char,
            DataType::Boolean(_) => DataTypeKind::Boolean,
            DataType::Date(_) => DataTypeKind::Date,
            DataType::DateTime(_) => DataTypeKind::DateTime,
            DataType::Text(_) => DataTypeKind::Text,
            DataType::Blob(_) => DataTypeKind::Blob,
            DataType::Null => DataTypeKind::Null,
        }
    }
    pub fn matches(&self, other: DataTypeKind) -> bool {
        match (self, other) {
            (DataType::SmallInt(_), DataTypeKind::SmallInt) => true,
            (DataType::HalfInt(_), DataTypeKind::HalfInt) => true,
            (DataType::Int(_), DataTypeKind::Int) => true,
            (DataType::BigInt(_), DataTypeKind::BigInt) => true,
            (DataType::SmallUInt(_), DataTypeKind::SmallUInt) => true,
            (DataType::HalfUInt(_), DataTypeKind::HalfUInt) => true,
            (DataType::UInt(_), DataTypeKind::UInt) => true,
            (DataType::BigUInt(_), DataTypeKind::BigUInt) => true,
            (DataType::Float(_), DataTypeKind::Float) => true,
            (DataType::Double(_), DataTypeKind::Double) => true,
            (DataType::Byte(_), DataTypeKind::Byte) => true,
            (DataType::Char(_), DataTypeKind::Char) => true,
            (DataType::Boolean(_), DataTypeKind::Boolean) => true,
            (DataType::Date(_), DataTypeKind::Date) => true,
            (DataType::DateTime(_), DataTypeKind::DateTime) => true,
            (DataType::Text(_), DataTypeKind::Text) => true,
            (DataType::Blob(_), DataTypeKind::Blob) => true,
            (DataType::Null, _) => true, // NULL matches any type
            _ => false,
        }
    }
}

impl DataType {
    pub fn from_expr(expr: Expr, kind: DataTypeKind) -> DataType {
        match (kind, expr) {
            (DataTypeKind::Null, _) | (_, Expr::Null) => DataType::Null,
            (DataTypeKind::Boolean, Expr::Boolean(b)) => DataType::Boolean(UInt8::from(b as u8)),
            (DataTypeKind::Boolean, Expr::Number(n)) => {
                DataType::Boolean(UInt8::from((n != 0.0) as u8))
            }
            (DataTypeKind::Boolean, Expr::String(n)) => {
                if n == "TRUE" {
                    DataType::Boolean(UInt8::TRUE)
                } else if n == "FALSE" {
                    DataType::Boolean(UInt8::FALSE)
                } else {
                    panic!("Invalid expression for parsing boolean type: {n}");
                }
            }
            (DataTypeKind::SmallInt, Expr::Number(n)) => DataType::SmallInt(Int8::from(n as i8)),
            (DataTypeKind::SmallInt, Expr::String(n)) => {
                DataType::SmallInt(Int8::from(n.parse::<i8>().unwrap()))
            }
            (DataTypeKind::HalfInt, Expr::Number(n)) => DataType::HalfInt(Int16::from(n as i16)),
            (DataTypeKind::HalfInt, Expr::String(n)) => {
                DataType::HalfInt(Int16::from(n.parse::<i16>().unwrap()))
            }
            (DataTypeKind::Int, Expr::Number(n)) => DataType::Int(Int32::from(n as i32)),
            (DataTypeKind::Int, Expr::String(n)) => {
                DataType::Int(Int32::from(n.parse::<i32>().unwrap()))
            }
            (DataTypeKind::BigInt, Expr::Number(n)) => DataType::BigInt(Int64::from(n as i64)),
            (DataTypeKind::BigInt, Expr::String(n)) => {
                DataType::BigInt(Int64::from(n.parse::<i64>().unwrap()))
            }
            (DataTypeKind::SmallUInt, Expr::Number(n)) => DataType::SmallUInt(UInt8::from(n as u8)),
            (DataTypeKind::SmallUInt, Expr::String(n)) => {
                DataType::SmallUInt(UInt8::from(n.parse::<u8>().unwrap()))
            }
            (DataTypeKind::HalfUInt, Expr::Number(n)) => DataType::HalfUInt(UInt16::from(n as u16)),
            (DataTypeKind::HalfUInt, Expr::String(n)) => {
                DataType::HalfUInt(UInt16::from(n.parse::<u16>().unwrap()))
            }
            (DataTypeKind::UInt, Expr::Number(n)) => DataType::UInt(UInt32::from(n as u32)),
            (DataTypeKind::UInt, Expr::String(n)) => {
                DataType::UInt(UInt32::from(n.parse::<u32>().unwrap()))
            }
            (DataTypeKind::BigUInt, Expr::Number(n)) => DataType::BigUInt(UInt64::from(n as u64)),
            (DataTypeKind::BigUInt, Expr::String(n)) => {
                DataType::BigUInt(UInt64::from(n.parse::<u64>().unwrap()))
            }
            (DataTypeKind::Float, Expr::Number(n)) => DataType::Float(Float32::from(n as f32)),
            (DataTypeKind::Float, Expr::String(n)) => {
                DataType::Float(Float32::from(n.parse::<f32>().unwrap()))
            }
            (DataTypeKind::Double, Expr::Number(n)) => DataType::Double(Float64::from(n)),
            (DataTypeKind::Double, Expr::String(n)) => {
                DataType::Double(Float64::from(n.parse::<f64>().unwrap()))
            }
            (DataTypeKind::Text, Expr::String(s)) => DataType::Text(Blob::from(s.as_bytes())),
            (DataTypeKind::Blob, Expr::String(s)) => DataType::Blob(Blob::from(s.as_bytes())),
            (DataTypeKind::Text, Expr::Number(s)) => DataType::Text(Blob::from(&s.to_ne_bytes())),
            (DataTypeKind::Blob, Expr::Number(s)) => DataType::Blob(Blob::from(&s.to_ne_bytes())),
            (DataTypeKind::Char, Expr::String(s)) if s.len() == 1 => {
                DataType::Char(UInt8::from(s.as_bytes()[0]))
            }
            (DataTypeKind::Char, Expr::Number(n)) => DataType::Char(UInt8::from((n) as u8)),
            (DataTypeKind::Date, Expr::String(s)) => DataType::Date(
                Date::parse_iso(&s)
                    .unwrap_or_else(|_| panic!("Invalid date format for value '{}'", s)),
            ),
            (DataTypeKind::Date, Expr::Number(f)) => DataType::Date(Date::from(f as u32)),
            (DataTypeKind::DateTime, Expr::String(s)) => DataType::DateTime(
                DateTime::parse_iso(&s)
                    .unwrap_or_else(|_| panic!("Invalid datetime format for value '{}'", s)),
            ),
            (DataTypeKind::DateTime, Expr::Number(f)) => {
                DataType::DateTime(DateTime::from(f as u64))
            }
            (kind, expr) => panic!(
                "Cannot convert expression {:?} into data type {:?}",
                expr, kind
            ),
        }
    }
}

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
            DataTypeKind::Null => 0,
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

impl DataTypeKind {
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

        let self_align = self.alignment();
        let other_align = other.alignment();
        self_align <= other_align
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
