use crate::types::{
    BlobRef, BlobRefMut, DataType, DataTypeKind, DataTypeRef, DataTypeRefMut, DateRef, DateRefMut,
    DateTimeRef, DateTimeRefMut, Float32Ref, Float32RefMut, Float64Ref, Float64RefMut, Int16Ref,
    Int16RefMut, Int32, Float64, Int32Ref, Int32RefMut, Int64Ref, Int64RefMut, Int8Ref, Int8RefMut,
    UInt16Ref, UInt16RefMut, UInt32Ref, UInt32RefMut, UInt64Ref, UInt64RefMut, UInt8, UInt8Ref,
    UInt8RefMut, VarInt,
};
use crate::TextEncoding;
use std::convert::AsRef;
use std::ptr::null;

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<ColumnDef>,
}

fn align_cursor(cursor: usize, align: usize) -> usize {
    cursor.next_multiple_of(align)
}

impl Schema {
    fn alignment(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.dtype.alignment())
            .max()
            .unwrap_or(1)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnDef {
    pub(crate) name: String,
    pub(crate) dtype: DataTypeKind,
    pub(crate) nullable: bool,
}

fn reinterpret_cast<'a>(
    dtype: DataTypeKind,
    buffer: &'a [u8],
) -> std::io::Result<(DataTypeRef<'a>, usize)> {
    match dtype {
        DataTypeKind::Blob => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;
            cursor += offset;

            let value = DataTypeRef::Blob(BlobRef::from_bytes(&buffer[cursor..cursor + len_usize]));
            cursor += len_usize;
            Ok((value, cursor))
        }

        DataTypeKind::Text => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;
            cursor += offset;

            let value = DataTypeRef::Text(BlobRef::from_bytes(&buffer[cursor..cursor + len_usize]));
            cursor += len_usize;
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

fn reinterpret_cast_mut<'a>(
    dtype: DataTypeKind,
    buffer: &'a mut [u8],
) -> std::io::Result<(DataTypeRefMut<'a>, usize)> {
    match dtype {
        DataTypeKind::Blob => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;
            cursor += offset;

            let value = DataTypeRefMut::Blob(BlobRefMut::from_bytes(
                &mut buffer[cursor..cursor + len_usize],
            ));
            cursor += len_usize;

            Ok((value, cursor))
        }

        DataTypeKind::Text => {
            let mut cursor = 0;
            let (len_varint, offset) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let len_usize: usize = len_varint.try_into()?;
            cursor += offset;

            let value = DataTypeRefMut::Text(BlobRefMut::from_bytes(
                &mut buffer[cursor..cursor + len_usize],
            ));
            cursor += len_usize;

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

struct Tuple<'a> {
    data: &'a mut [u8],
    offsets: Vec<usize>,
}

impl<'a> Tuple<'a> {
    fn read(buffer: &'a mut [u8], schema: &Schema) -> std::io::Result<Tuple<'a>> {
        let mut tuple = Tuple {
            data: buffer,
            offsets: Vec::with_capacity(schema.columns.len()),
        };
        let null_bitmap_size = schema.columns.len().div_ceil(8);
        let mut cursor = null_bitmap_size;
        for (i, col) in schema.columns.iter().enumerate() {
            tuple.offsets.push(cursor);
            let is_null = if col.nullable {
                tuple.is_null(i, schema)
            } else {
                false
            };

            if !is_null {
                let (value, read_bytes) = reinterpret_cast(col.dtype, &tuple.data[cursor..])?;
                cursor += read_bytes;
            };
        }

        Ok(tuple)
    }

    fn is_null(&self, col_idx: usize, schema: &Schema) -> bool {
        let byte_idx = col_idx / 8;
        let bit_idx = col_idx % 8;
        let null_bitmap_size = schema.columns.len().div_ceil(8);
        let bitmap = &self.data[0..null_bitmap_size];
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    fn value(&self, schema: &Schema, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = schema.columns[index].dtype;
        let is_null = if schema.columns[index].nullable {
            self.is_null(index, schema)
        } else {
            false
        };

        if !is_null {
            let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    fn value_mut(&mut self, schema: &Schema, index: usize) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = schema.columns[index].dtype;
        let is_null = if schema.columns[index].nullable {
            self.is_null(index, schema)
        } else {
            false
        };

        if !is_null {
            let (value, _) = reinterpret_cast_mut(dtype, &mut self.data[self.offsets[index]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRefMut::Null)
        }
    }

    fn key(&self, schema: &Schema) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = schema.columns[0].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[0]..])?;
        Ok(value)
    }

    fn key_mut(&mut self, schema: &Schema) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = schema.columns[0].dtype;
        let (value, _) = reinterpret_cast_mut(dtype, &mut self.data[self.offsets[0]..])?;
        Ok(value)
    }

    fn bitmap(&self, schema: &Schema) -> &[u8] {
        let bitmap_len = schema.columns.len();
        &self.data[0..bitmap_len]
    }

    fn bitmap_mut(&mut self, schema: &Schema) -> &mut [u8] {
        let bitmap_len = schema.columns.len();
        &mut self.data[0..bitmap_len]
    }
}

pub struct TupleDisplay<'t, 's, 'a> {
    tuple: &'t Tuple<'a>,
    schema: &'s Schema,
}

impl<'a> Tuple<'a> {
    pub fn display<'s>(&'a self, schema: &'s Schema) -> TupleDisplay<'a, 's, 'a> {
        TupleDisplay {
            tuple: self,
            schema,
        }
    }
}

impl<'t, 'b, 'a> std::fmt::Display for TupleDisplay<'t, 'b, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tuple[")?;

        for (i, col_def) in self.schema.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}: ", col_def.name)?;

            match self.tuple.value(self.schema, i) {
                Ok(value) => match value {
                    DataTypeRef::Null => write!(f, "NULL")?,
                    DataTypeRef::SmallInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::HalfInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Int(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::BigInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::SmallUInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::HalfUInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::UInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::BigUInt(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Float(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Double(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Byte(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Char(v) => write!(f, "'{}'", v.to_owned().to_char())?,
                    DataTypeRef::Boolean(v) => write!(f, "{}", v.to_owned() != 0)?,
                    DataTypeRef::Date(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::DateTime(v) => write!(f, "{}", v.to_owned())?,
                    DataTypeRef::Blob(b) => write!(f, "Blob({} bytes)", b.len())?,
                    DataTypeRef::Text(b) => write!(f, "\"{}\"", b.as_str(TextEncoding::Utf8))?,
                },
                Err(e) => {
                    write!(f, "<error: {e}>")?;
                }
            }
        }

        write!(f, "]")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn test_tuple_serialization() {
        // Create an schema with columns
        let schema = Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    dtype: DataTypeKind::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "age".into(),
                    dtype: DataTypeKind::SmallUInt,
                    nullable: false,
                },
                ColumnDef {
                    name: "letter".into(),
                    dtype: DataTypeKind::Char,
                    nullable: true,
                }, // make it nullable
                ColumnDef {
                    name: "balance".into(),
                    dtype: DataTypeKind::Double,
                    nullable: false,
                },
                ColumnDef {
                    name: "bonus".into(),
                    dtype: DataTypeKind::Double,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    dtype: DataTypeKind::Text,
                    nullable: false,
                },
            ],
        };

        // Example data
        let id: i32 = 42;
        let age: u8 = 33;
        let letter: u8 = 65;
        let balance: f64 = 1234.5678;
        let bonus: f64 = 99.9;
        let name : &str = "Test tuple";
        let null_bitmap: [u8; 1] = [0b00000100];

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&null_bitmap);
        buffer.extend_from_slice(&id.to_le_bytes());
        buffer.extend_from_slice(&age.to_le_bytes());

        buffer.extend_from_slice(&balance.to_le_bytes());
        buffer.extend_from_slice(&bonus.to_le_bytes());

        // Test text serialization using varint to store the prefix.
        let mut length_buffer = [0u8; 9];
        let length_bytes = VarInt::encode(name.len() as i64, &mut  length_buffer);

        let len: usize = VarInt::from_encoded_bytes(length_bytes).unwrap().0.try_into().unwrap();

        debug_assert!(len == name.len());



        buffer.extend_from_slice(length_bytes);
        buffer.extend_from_slice(name.as_bytes());

        dbg!(buffer.len());



        let tuple = Tuple::read(&mut buffer, &schema).unwrap();
        println!("{}", tuple.display(&schema));


        let id_ref = tuple.value(&schema, 0).unwrap();
        assert_eq!(id_ref.to_owned(), DataType::Int(Int32(id)));

        let age_ref = tuple.value(&schema, 1).unwrap();
        assert_eq!(age_ref.to_owned(), DataType::SmallUInt(UInt8(age)));

        let letter_ref = tuple.value(&schema, 2).unwrap();
        assert!(matches!(letter_ref, DataTypeRef::Null));

        let balance_ref = tuple.value(&schema, 3).unwrap();
        assert_eq!(balance_ref.to_owned(), DataType::Double(Float64(balance)));

        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert_eq!(bonus_ref.to_owned(), DataType::Double(Float64(bonus)));

        println!("{}", tuple.display(&schema));
    }
}
