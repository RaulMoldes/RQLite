use std::ptr::null;

use crate::types::varint::MAX_VARINT_LEN;
use crate::types::{
    BlobRef, BlobRefMut, DataType, DataTypeKind, DataTypeRef, DataTypeRefMut, DateRef, DateRefMut,
    DateTimeRef, DateTimeRefMut, Float32Ref, Float32RefMut, Float64Ref, Float64RefMut, Int16Ref,
    Int16RefMut, Int32, Int32Ref, Int32RefMut, Int64Ref, Int64RefMut, Int8Ref, Int8RefMut,
    UInt16Ref, UInt16RefMut, UInt32Ref, UInt32RefMut, UInt64Ref, UInt64RefMut, UInt8Ref,
    UInt8RefMut, VarInt,
};
use crate::TextEncoding;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Schema {
    pub(crate) columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn write_to(self) -> std::io::Result<Vec<u8>> {
        let mut out_buffer =
            Vec::with_capacity(self.columns.len() * std::mem::size_of::<ColumnDef>());

        let mut varint_buf = [0u8; MAX_VARINT_LEN];
        let buffer = VarInt::encode(self.columns.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);
        for column in self.columns {
            out_buffer.extend_from_slice(&[column.dtype as u8]);
            let mut varint_buf = [0u8; MAX_VARINT_LEN];
            let buffer = VarInt::encode(column.name.len() as i64, &mut varint_buf);
            out_buffer.extend_from_slice(buffer);
            out_buffer.extend_from_slice(&column.name.as_bytes());
        }
        Ok(out_buffer)
    }

    pub fn read_from(bytes: &[u8]) -> std::io::Result<Self> {
        let (column_count, offset) = VarInt::from_encoded_bytes(bytes)?;
        let column_count_usize = column_count
            .try_into()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut cursor = offset;
        let mut columns = Vec::with_capacity(column_count_usize);

        while columns.len() < column_count_usize {
            // Verificar que tenemos bytes suficientes
            if cursor >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for schema",
                ));
            }

            // Leer dtype
            let dtype_byte = bytes[cursor];
            let dtype = DataTypeKind::from_repr(dtype_byte)?;
            cursor += 1;

            let (name_len, name_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let name_len_usize: usize = name_len.try_into()?;
            cursor += name_len_offset;

            if cursor + name_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for column name",
                ));
            }

            let name_bytes = &bytes[cursor..cursor + name_len_usize];
            let name = String::from_utf8(name_bytes.to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += name_len_usize;

            columns.push(ColumnDef { dtype, name });
        }

        Ok(Schema { columns })
    }
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

impl From<&[ColumnDef]> for Schema {
    fn from(value: &[ColumnDef]) -> Self {
        Self {
            columns: value.to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnDef {
    pub(crate) dtype: DataTypeKind,
    pub(crate) name: String,
}

impl ColumnDef {
    pub fn new(dtype: DataTypeKind, name: &str) -> Self {
        Self {
            dtype,
            name: name.to_string(),
        }
    }
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

pub struct Tuple<'a> {
    data: &'a mut [u8],
    offsets: Vec<usize>,
    key_len: usize,
}

impl<'a> Tuple<'a> {
    pub fn read(buffer: &'a mut [u8], schema: &Schema) -> std::io::Result<Tuple<'a>> {
        let mut tuple = Tuple {
            data: buffer,
            offsets: Vec::with_capacity(schema.columns.len()),
            key_len: 0,
        };

        // Prepend always the key.
        if let Some(key) = schema.columns.first() {
            tuple.offsets.push(0);
            let (value, read_bytes) = reinterpret_cast(key.dtype, &tuple.data[0..])?;
            tuple.key_len = read_bytes;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot construct tuple from empty data",
            ));
        };

        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let mut cursor = tuple.key_len + null_bitmap_size;
        for (i, col) in schema.columns.iter().skip(1).enumerate() {
            tuple.offsets.push(cursor);
            // i+1 because we skipped the first column (key) but is_null expects the actual column index
            let is_null = tuple.is_null(i + 1, schema);

            if !is_null {
                let (value, read_bytes) = reinterpret_cast(col.dtype, &tuple.data[cursor..])?;
                cursor += read_bytes;
            };
        }

        Ok(tuple)
    }

    pub fn num_fields(&self) -> usize {
        self.offsets.len()
    }

    fn is_null(&self, mut col_idx: usize, schema: &Schema) -> bool {
        if col_idx == 0 {
            return false;
        } else {
            col_idx -= 1;
        };

        let byte_idx = col_idx / 8;
        let bit_idx = col_idx % 8;
        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let bitmap = &self.data[self.key_len..self.key_len + null_bitmap_size];
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn value(&self, schema: &Schema, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = schema.columns[index].dtype;

        let is_null = self.is_null(index, schema);

        if !is_null {
            let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    fn value_mut(&mut self, schema: &Schema, index: usize) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = schema.columns[index].dtype;
        let is_null = self.is_null(index, schema);

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
        &self.data[self.key_len..self.key_len + bitmap_len]
    }

    fn bitmap_mut(&mut self, schema: &Schema) -> &mut [u8] {
        let bitmap_len = schema.columns.len();
        &mut self.data[self.key_len..self.key_len + bitmap_len]
    }

    /// Update a value in place
    pub fn update_value<F>(
        &mut self,
        schema: &Schema,
        index: usize,
        updater: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(DataTypeRefMut<'_>),
    {
        let value = self.value_mut(schema, index)?;
        updater(value);
        Ok(())
    }

    pub fn set_value(
        &mut self,
        schema: &Schema,
        index: usize,
        value: DataType,
    ) -> std::io::Result<()> {
        let dtype = schema.columns[index].dtype;
        let offset = self.offsets[index];

        match (dtype, value) {
            (DataTypeKind::SmallInt, DataType::SmallInt(v)) => {
                let mut ref_mut = Int8RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::HalfInt, DataType::HalfInt(v)) => {
                let mut ref_mut = Int16RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Int, DataType::Int(v)) => {
                let mut ref_mut = Int32RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::BigInt, DataType::BigInt(v)) => {
                let mut ref_mut = Int64RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::SmallUInt, DataType::SmallUInt(v)) => {
                let mut ref_mut = UInt8RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Byte, DataType::Byte(v)) => {
                let mut ref_mut = UInt8RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Char, DataType::Char(v)) => {
                let mut ref_mut = UInt8RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Boolean, DataType::Boolean(v)) => {
                let mut ref_mut = UInt8RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::HalfUInt, DataType::HalfUInt(v)) => {
                let mut ref_mut = UInt16RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::UInt, DataType::UInt(v)) => {
                let mut ref_mut = UInt32RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Date, DataType::Date(v)) => {
                let mut ref_mut = UInt32RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::DateTime, DataType::DateTime(v)) => {
                let mut ref_mut = UInt64RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::BigUInt, DataType::BigUInt(v)) => {
                let mut ref_mut = UInt64RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Float, DataType::Float(v)) => {
                let mut ref_mut = Float32RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Double, DataType::Double(v)) => {
                let mut ref_mut = Float64RefMut::from_bytes(&mut self.data[offset..])?;
                ref_mut.set(v.0);
            }
            (DataTypeKind::Text, DataType::Text(v)) => {
                let (len_varint, varint_size) = VarInt::from_encoded_bytes(&self.data[offset..])?;
                let existing_len: usize = len_varint.try_into()?;

                let (len_new_varint, new_varint_size) =
                    VarInt::from_encoded_bytes(&self.data[offset..])?;
                let new_len: usize = len_new_varint.try_into()?;

                if (new_len + new_varint_size) <= (existing_len + varint_size) {
                    // New value fits
                    self.data[offset..offset + v.len()].copy_from_slice(v.data());
                    // Pad the remaining space with zeros
                    if v.len() < existing_len {
                        self.data[offset + v.len()..offset + (existing_len + varint_size)].fill(0);
                    }
                } else {
                    // New value is too large for the allocated space
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "New text value ({} bytes) exceeds allocated space ({} bytes)",
                            v.len(),
                            existing_len
                        ),
                    ));
                }
            }
            (DataTypeKind::Blob, DataType::Blob(v)) => {
                let (len_varint, varint_size) = VarInt::from_encoded_bytes(&self.data[offset..])?;
                let existing_len: usize = len_varint.try_into()?;

                let (len_new_varint, new_varint_size) =
                    VarInt::from_encoded_bytes(&self.data[offset..])?;
                let new_len: usize = len_new_varint.try_into()?;

                if (new_len + new_varint_size) <= (existing_len + varint_size) {
                    // New value fits
                    self.data[offset..offset + v.len()].copy_from_slice(v.data());
                    // Pad the remaining space with zeros
                    if v.len() < existing_len {
                        self.data[offset + v.len()..offset + (existing_len + varint_size)].fill(0);
                    }
                } else {
                    // New value is too large for the allocated space
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "New text value ({} bytes) exceeds allocated space ({} bytes)",
                            v.len(),
                            existing_len
                        ),
                    ));
                }
            }

            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Type mismatch",
                ))
            }
        }
        Ok(())
    }

    pub fn set_null(&mut self, schema: &Schema, index: usize) {
        assert!(index > 0, "Cannot set the key to NULL");

        // Fix: adjust index for the bitmap (subtract 1 since key is not in bitmap)
        let byte_idx = (index - 1) / 8;
        let bit_idx = (index - 1) % 8;
        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let bitmap = &mut self.data[self.key_len..self.key_len + null_bitmap_size];
        bitmap[byte_idx] |= 1 << bit_idx;
    }

    pub fn clear_null(&mut self, schema: &Schema, index: usize) {
        // Fix: adjust index for the bitmap (subtract 1 since key is not in bitmap)
        let byte_idx = (index - 1) / 8;
        let bit_idx = (index - 1) % 8;
        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let bitmap = &mut self.data[self.key_len..self.key_len + null_bitmap_size];
        bitmap[byte_idx] &= !(1 << bit_idx);
    }
}

#[derive(Debug)]
pub struct TupleRef<'a> {
    data: &'a [u8],
    offsets: Vec<usize>,
    key_len: usize,
}

impl<'a> TupleRef<'a> {
    pub fn read(buffer: &'a [u8], schema: &Schema) -> std::io::Result<TupleRef<'a>> {
        let mut tuple = TupleRef {
            data: buffer,
            offsets: Vec::with_capacity(schema.columns.len()),
            key_len: 0,
        };

        // Prepend always the key.
        if let Some(key) = schema.columns.first() {
            tuple.offsets.push(0);
            let (value, read_bytes) = reinterpret_cast(key.dtype, &tuple.data[0..])?;
            tuple.key_len = read_bytes;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot construct tuple from empty data",
            ));
        };

        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let mut cursor = tuple.key_len + null_bitmap_size;
        for (i, col) in schema.columns.iter().skip(1).enumerate() {
            tuple.offsets.push(cursor);
            // i+1 because we skipped the first column (key) but is_null expects the actual column index
            let is_null = tuple.is_null(i + 1, schema);

            if !is_null {
                let (value, read_bytes) = reinterpret_cast(col.dtype, &tuple.data[cursor..])?;
                cursor += read_bytes;
            };
        }

        Ok(tuple)
    }

    pub fn num_fields(&self) -> usize {
        self.offsets.len()
    }

    fn is_null(&self, mut col_idx: usize, schema: &Schema) -> bool {
        if col_idx == 0 {
            return false;
        } else {
            col_idx -= 1;
        };

        let byte_idx = col_idx / 8;
        let bit_idx = col_idx % 8;
        let null_bitmap_size = (schema.columns.len() - 1).div_ceil(8);
        let bitmap = &self.data[self.key_len..self.key_len + null_bitmap_size];
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn value(&self, schema: &Schema, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = schema.columns[index].dtype;
        // Fix: use index directly, not index - 1
        let is_null = self.is_null(index, schema);

        if !is_null {
            let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    fn key(&self, schema: &Schema) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = schema.columns[0].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[0]..])?;
        Ok(value)
    }

    fn bitmap(&self, schema: &Schema) -> &[u8] {
        let bitmap_len = schema.columns.len();
        &self.data[self.key_len..self.key_len + bitmap_len]
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

pub(crate) fn tuple(values: &[DataType], schema: &Schema) -> std::io::Result<Box<[u8]>> {
    if values.len() > schema.columns.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Too many values: {} values for {} columns",
                values.len(),
                schema.columns.len()
            ),
        ));
    }

    for (i, (value, col_def)) in values.iter().zip(schema.columns.iter()).enumerate() {
        if !value.matches(col_def.dtype) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Type mismatch at column {}: expected {:?}, got {:?}",
                    i, col_def.dtype, value
                ),
            ));
        }
    }

    let key_size = values.first().map(|v| v.size()).unwrap_or(0);
    let null_bitmap_size = schema.columns.len().saturating_sub(1).div_ceil(8);

    let mut total_size = key_size + null_bitmap_size;
    for value in values.iter().skip(1) {
        if !matches!(value, DataType::Null) {
            total_size += value.size();
        }
    }
    // Single allocation with exact size
    let mut buffer: Box<[std::mem::MaybeUninit<u8>]> = Box::new_uninit_slice(total_size);

    // Safe initialization
    unsafe {
        // Initialize null bitmap to 0
        let ptr = buffer.as_mut_ptr() as *mut u8;
        let mut cursor = 0;

        // Write the key first.
        if let Some(value) = values.first() {
            let data = value.as_ref();
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
            cursor += data.len();
        }

        std::ptr::write_bytes(ptr.add(cursor), 0, null_bitmap_size);
        let bitmap_start = cursor;
        cursor += null_bitmap_size;

        // Write values
        for (col_idx, col_def) in schema.columns.iter().enumerate().skip(1) {
            let value_opt = values.get(col_idx);
            match value_opt {
                Some(DataType::Null) | None => {
                    // Set null bit
                    let i = col_idx - 1; // offset because bitmap covers only non-key columns

                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    *ptr.add(bitmap_start + byte_idx) |= 1 << bit_idx;
                }
                Some(value) => {
                    let data = value.as_ref();
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                    cursor += data.len();
                }
            }
        }

        Ok(Box::from_raw(Box::into_raw(buffer) as *mut [u8]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Blob, DataType, Float64, Int32, UInt8};

    fn create_schema() -> Schema {
        Schema::from(
            [
                ColumnDef::new(DataTypeKind::Int, "id"),
                ColumnDef::new(DataTypeKind::Text, "name"),
                ColumnDef::new(DataTypeKind::Boolean, "active"),
                ColumnDef::new(DataTypeKind::Double, "balance"),
                ColumnDef::new(DataTypeKind::Double, "bonus"),
                ColumnDef::new(DataTypeKind::Text, "description"),
            ]
            .as_ref(),
        )
    }

    fn create_blob(content: &str) -> Vec<u8> {
        let mut len_buf = [0u8; 9];
        let mut blob = VarInt::encode(content.len() as i64, &mut len_buf).to_vec();
        blob.extend_from_slice(content.as_bytes());
        blob
    }

    #[test]
    fn test_serialization() {
        let schema = create_schema();

        let id = 42;
        let name = "Test tuple";
        let active: Option<bool> = None;
        let balance = 1234.5678;
        let bonus = 99.9;
        let description = "Initial description";

        let values = vec![
            DataType::Int(Int32(id)),
            DataType::Text(Blob::from(name)),
            DataType::Null,
            DataType::Double(Float64(balance)),
            DataType::Double(Float64(bonus)),
            DataType::Text(Blob::from(description)),
        ];

        let tuple_buffer = tuple(&values, &schema).unwrap();

        let mut tuple_bytes = tuple_buffer.to_vec();
        let tuple = Tuple::read(&mut tuple_bytes, &schema).unwrap();

        println!("{}", tuple.display(&schema));

        let id_ref = tuple.value(&schema, 0).unwrap();
        assert_eq!(id_ref.as_ref(), &id.to_le_bytes());

        let name_ref = tuple.value(&schema, 1).unwrap();
        assert_eq!(name_ref.as_ref(), name.as_bytes());

        let active_ref = tuple.value(&schema, 2).unwrap();
        assert!(matches!(active_ref, DataTypeRef::Null));

        let balance_ref = tuple.value(&schema, 3).unwrap();
        assert_eq!(balance_ref.as_ref(), &balance.to_le_bytes());

        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert_eq!(bonus_ref.as_ref(), &bonus.to_le_bytes());
    }

    #[test]
    fn test_updates() {
        let schema = create_schema();

        let id = 42;
        let name = "Test tuple";
        let balance = 1234.5678;
        let bonus = 99.9;
        let description = "Initial description";

        let values = vec![
            DataType::Int(Int32(id)),
            DataType::Text(Blob::from(name)),
            DataType::Null,
            DataType::Double(Float64(balance)),
            DataType::Double(Float64(bonus)),
            DataType::Text(Blob::from(description)),
        ];

        let tuple_buffer = tuple(&values, &schema).unwrap();
        let mut buffer_vec = tuple_buffer.to_vec();

        let mut tuple = Tuple::read(&mut buffer_vec, &schema).unwrap();

        println!("Before update:\n{}", tuple.display(&schema));

        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert_eq!(bonus_ref.as_ref(), &bonus.to_le_bytes());

        let new_bonus = 100.0;
        tuple
            .set_value(&schema, 4, DataType::Double(Float64(new_bonus)))
            .unwrap();

        let new_description = "Updated description";
        let mut blob = create_blob(new_description);
        tuple
            .set_value(&schema, 5, DataType::Text(Blob::from_bytes(blob.as_mut())))
            .unwrap();

        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert_eq!(bonus_ref.as_ref(), &new_bonus.to_le_bytes());

        let desc_ref = tuple.value(&schema, 5).unwrap();
        assert_eq!(desc_ref.as_ref(), new_description.as_bytes());

        tuple.set_null(&schema, 4);
        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert!(matches!(bonus_ref, DataTypeRef::Null));

        tuple.clear_null(&schema, 4);
        let bonus_ref = tuple.value(&schema, 4).unwrap();
        assert_eq!(bonus_ref.as_ref(), &new_bonus.to_le_bytes());

        println!("After update:\n{}", tuple.display(&schema));
    }
    #[test]
    fn test_tuple_creation() {
        let schema = Schema::from(
            [
                ColumnDef::new(DataTypeKind::Int, "id"),
                ColumnDef::new(DataTypeKind::Text, "name"),
                ColumnDef::new(DataTypeKind::Boolean, "active"),
            ]
            .as_ref(),
        );

        let values = vec![
            DataType::Int(Int32(42)),
            DataType::Text(Blob::from("Alice")),
            DataType::Boolean(UInt8(1)),
        ];

        let tuple_buffer = tuple(&values, &schema).unwrap();

        dbg!(&tuple_buffer);
        let expected_size = 1 + 4 + (1 + 5) + 1;
        assert!(tuple_buffer.len() >= expected_size);

        let mut buffer_vec = tuple_buffer.to_vec();
        let tuple_ref = Tuple::read(&mut buffer_vec, &schema).unwrap();

        match tuple_ref.value(&schema, 0).unwrap() {
            DataTypeRef::Int(v) => assert_eq!(v.to_owned().0, 42),
            _ => panic!("Wrong type for id"),
        }
    }
}
