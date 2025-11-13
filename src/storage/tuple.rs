use crate::database::schema::Schema;
use crate::types::{
    reinterpret_cast, reinterpret_cast_mut, DataType, DataTypeKind, DataTypeRef, DataTypeRefMut,
    Float32RefMut, Float64RefMut, Int16RefMut, Int32RefMut, Int64RefMut, Int8RefMut, UInt16RefMut,
    UInt32RefMut, UInt64RefMut, UInt8RefMut, VarInt,
};
use crate::TextEncoding;

pub struct TupleRefMut<'a, 'b> {
    data: &'a mut [u8],
    schema: &'b Schema,
    offsets: Vec<usize>,
    key_len: usize,
}

impl<'a, 'b> TupleRefMut<'a, 'b> {
    pub fn read(buffer: &'a mut [u8], schema: &'b Schema) -> std::io::Result<TupleRefMut<'a, 'b>> {
        let mut tuple = TupleRefMut {
            data: buffer,
            schema,
            offsets: Vec::with_capacity(schema.columns.len()),
            key_len: 0,
        };

        let mut cursor = 0;
        for key in schema.iter_keys() {
            tuple.offsets.push(cursor);
            let (value, read_bytes) = reinterpret_cast(key.dtype, &tuple.data[cursor..])?;
            tuple.key_len += read_bytes;
            cursor += read_bytes;
        }

        let null_bitmap_size = schema.values().len().div_ceil(8);
        cursor += null_bitmap_size;
        for (i, col) in schema.iter_values().enumerate() {
            tuple.offsets.push(cursor);
            let is_null = tuple.is_null(i);

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

    fn is_null(&self, val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        let null_bitmap_size = (self.schema.values().len()).div_ceil(8);
        let bitmap = &self.data[self.key_len..self.key_len + null_bitmap_size];
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn value(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.values()[index].dtype;
        let is_null = self.is_null(index);
        let offset_idx = index + self.schema.num_keys as usize;
        if !is_null {
            let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[offset_idx]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    fn value_mut(&mut self, index: usize) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = self.schema.values()[index].dtype;
        let is_null = self.is_null(index);
        let offset_idx = index + self.schema.num_keys as usize;

        if !is_null {
            let (value, _) =
                reinterpret_cast_mut(dtype, &mut self.data[self.offsets[offset_idx]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRefMut::Null)
        }
    }

    fn key(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index]..])?;
        Ok(value)
    }

    fn key_mut(&mut self, index: usize) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) = reinterpret_cast_mut(dtype, &mut self.data[self.offsets[index]..])?;
        Ok(value)
    }

    fn bitmap(&self) -> &[u8] {
        let bitmap_len = self.schema.columns.len();
        &self.data[self.key_len..self.key_len + bitmap_len]
    }

    fn bitmap_mut(&mut self) -> &mut [u8] {
        let bitmap_len = self.schema.columns.len();
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
        let value = self.value_mut(index)?;
        updater(value);
        Ok(())
    }

    pub fn set_value(&mut self, index: usize, value: DataType) -> std::io::Result<()> {
        let dtype = self.schema.columns[index].dtype;
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

    pub fn set_null_unchecked(&mut self, index: usize) {
        // TODO: CHECK IF THE VALUE HAS A NON-NULL CONSTRAINT
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        let null_bitmap_size = self.schema.values().len().div_ceil(8);
        let bitmap = &mut self.data[self.key_len..self.key_len + null_bitmap_size];
        bitmap[byte_idx] |= 1 << bit_idx;
    }

    pub fn clear_null(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        let null_bitmap_size = self.schema.values().len().div_ceil(8);
        let bitmap = &mut self.data[self.key_len..self.key_len + null_bitmap_size];
        bitmap[byte_idx] &= !(1 << bit_idx);
    }
}

pub struct TupleRef<'a, 'b> {
    data: &'a [u8],
    schema: &'b Schema,
    offsets: Vec<usize>,
    key_len: usize,
}

impl<'a, 'b> TupleRef<'a, 'b> {
    pub fn read(buffer: &'a [u8], schema: &'b Schema) -> std::io::Result<TupleRef<'a, 'b>> {
        let mut tuple = TupleRef {
            data: buffer,
            schema,
            offsets: Vec::with_capacity(schema.columns.len()),
            key_len: 0,
        };

        let mut cursor = 0;
        for key in schema.iter_keys() {
            tuple.offsets.push(cursor);
            let (value, read_bytes) = reinterpret_cast(key.dtype, &tuple.data[cursor..])?;
            tuple.key_len += read_bytes;
            cursor += read_bytes;
        }

        let null_bitmap_size = schema.values().len().div_ceil(8);
        cursor += null_bitmap_size;
        for (i, col) in schema.iter_values().enumerate() {
            tuple.offsets.push(cursor);
            let is_null = tuple.is_null(i);

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

    fn is_null(&self, val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        let null_bitmap_size = (self.schema.values().len()).div_ceil(8);
        let bitmap = &self.data[self.key_len..self.key_len + null_bitmap_size];
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn value(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.values()[index].dtype;
        let is_null = self.is_null(index);
        let offset_idx = index + self.schema.num_keys as usize;
        if !is_null {
            let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[offset_idx]..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    pub fn key(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index]..])?;
        Ok(value)
    }

    fn bitmap(&self) -> &[u8] {
        let bitmap_len = self.schema.columns.len();
        &self.data[self.key_len..self.key_len + bitmap_len]
    }
}

impl<'a, 'b> std::fmt::Display for TupleRefMut<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TupleRefMut: ")?;
        write!(f, "Keys: [")?;
        for (i, col_def) in self.schema.iter_keys().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            };

            write!(f, "{}: ", col_def.name)?;

            match self.key(i) {
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
                    DataTypeRef::Text(b) => {
                        write!(f, "Text: \"{}\"", b.as_str(TextEncoding::Utf8))?
                    }
                },
                Err(e) => {
                    write!(f, "<error: {e}>")?;
                }
            }
        }

        writeln!(f, "]")?;
        write!(f, "Values: [")?;
        for (i, col_def) in self.schema.iter_values().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            };

            write!(f, "{}: ", col_def.name)?;

            match self.value(i) {
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
                    DataTypeRef::Text(b) => {
                        write!(f, "Text: \"{}\"", b.as_str(TextEncoding::Utf8))?
                    }
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

impl<'a, 'b> std::fmt::Display for TupleRef<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TupleRef: ")?;
        write!(f, "Keys: [")?;
        for (i, col_def) in self.schema.iter_keys().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            };

            write!(f, "{}: ", col_def.name)?;

            match self.key(i) {
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
                    DataTypeRef::Text(b) => {
                        write!(f, "Text: \"{}\"", b.as_str(TextEncoding::Utf8))?
                    }
                },
                Err(e) => {
                    write!(f, "<error: {e}>")?;
                }
            }
        }
        writeln!(f, "]")?;
        write!(f, "Values: [")?;
        for (i, col_def) in self.schema.iter_values().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            };

            write!(f, "{}: ", col_def.name)?;

            match self.value(i) {
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
                    DataTypeRef::Text(b) => {
                        write!(f, "Text: \"{}\"", b.as_str(TextEncoding::Utf8))?
                    }
                },
                Err(e) => {
                    write!(f, "<error: {e}>")?;
                }
            }
        }

        write!(f, "];\n")?;

        Ok(())
    }
}

pub struct Tuple<'schema> {
    data: Box<[u8]>,
    schema: &'schema Schema,
    offsets: Vec<usize>,
    key_len: usize,
}

impl<'schema> Tuple<'schema> {
    pub(crate) fn from_data(values: &[DataType], schema: &'schema Schema) -> std::io::Result<Self> {
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

        for (i, (value, col_def)) in values.iter().zip(schema.iter_columns()).enumerate() {
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

        let mut key_len = 0;
        let mut offsets: Vec<usize> = Vec::with_capacity(schema.columns().len());

        // Compute the required size.
        let null_bitmap_size = schema.values().len().div_ceil(8);
        let mut total_size = values.iter().map(|c| c.size()).sum();
        total_size += null_bitmap_size;

        // Single allocation with exact size
        let mut buffer: Box<[std::mem::MaybeUninit<u8>]> = Box::new_uninit_slice(total_size);

        // Safe initialization
        let data = unsafe {
            // Initialize null bitmap to 0
            let ptr = buffer.as_mut_ptr() as *mut u8;
            let mut cursor = 0;

            // Write the key first.
            for value in values.iter().take(schema.num_keys as usize) {
                let data = value.as_ref();
                offsets.push(cursor);
                key_len += cursor;
                std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                cursor += data.len();
            }

            std::ptr::write_bytes(ptr.add(cursor), 0, null_bitmap_size);
            let bitmap_start = cursor;
            cursor += null_bitmap_size;

            // Write values
            for (i, col_def) in schema
                .columns
                .iter()
                .enumerate()
                .skip(schema.num_keys as usize)
            {
                let value_opt = values.get(i);
                match value_opt {
                    Some(DataType::Null) | None => {
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

            Box::from_raw(Box::into_raw(buffer) as *mut [u8])
        };

        Ok(Self {
            data,
            schema,
            offsets,
            key_len,
        })
    }
}

impl<'a> From<Tuple<'a>> for Box<[u8]> {
    fn from(value: Tuple<'a>) -> Self {
        value.data
    }
}

impl<'schema> AsRef<[u8]> for Tuple<'schema> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl<'a, 'b> AsRef<[u8]> for TupleRef<'a, 'b> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl<'schema> AsMut<[u8]> for Tuple<'schema> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.data.as_mut()
    }
}

impl<'a, 'b> AsMut<[u8]> for TupleRefMut<'a, 'b> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.data
    }
}

impl<'a> From<&'a Tuple<'a>> for TupleRef<'a, 'a> {
    fn from(t: &'a Tuple<'a>) -> Self {
        TupleRef::read(t.as_ref(), t.schema).unwrap()
    }
}

impl<'a> From<&'a mut Tuple<'a>> for TupleRefMut<'a, 'a> {
    fn from(t: &'a mut Tuple<'a>) -> Self {
        let schema = t.schema;
        let data = t.data.as_mut();
        TupleRefMut::read(data, schema).unwrap()
    }
}
#[cfg(test)]
mod tests {


    // TODO! MUST CREATE A MACRO TO AUTOMATE TEST GENERATION HERE.
    use super::*;
    use crate::database::schema::Column;
    use crate::types::{DataType, Float64, Int32, UInt8};

    fn create_single_key_schema() -> Schema {
        Schema::from_columns(
            [
                Column::new_unindexed(DataTypeKind::Int, "id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
                Column::new_unindexed(DataTypeKind::Boolean, "active", None),
                Column::new_unindexed(DataTypeKind::Double, "balance", None),
                Column::new_unindexed(DataTypeKind::Double, "bonus", None),
                Column::new_unindexed(DataTypeKind::Text, "description", None),
            ]
            .as_ref(),
            1,
        )
    }

    fn create_multi_key_schema() -> Schema {
        Schema::from_columns(
            [
                Column::new_unindexed(DataTypeKind::Int, "id", None),
                Column::new_unindexed(DataTypeKind::Text, "product_name", None),
                Column::new_unindexed(DataTypeKind::Boolean, "active", None),
                Column::new_unindexed(DataTypeKind::Double, "balance", None),
                Column::new_unindexed(DataTypeKind::Double, "bonus", None),
                Column::new_unindexed(DataTypeKind::Text, "description", None),
            ]
            .as_ref(),
            2,
        )
    }

    #[test]
    fn test_tuple_1() -> std::io::Result<()> {
        let schema = create_single_key_schema();

        let values = vec![
            DataType::Int(Int32(42)),
            DataType::Text("Alice".into()),
            DataType::Boolean(UInt8(1)),
            DataType::Double(Float64(100.5)),
            DataType::Double(Float64(10.0)),
            DataType::Text("User description".into()),
        ];

        let tuple = Tuple::from_data(&values, &schema)?;
        assert!(!tuple.as_ref().is_empty());

        let tuple_ref = TupleRef::from(&tuple);
        assert_eq!(tuple_ref.num_fields(), schema.columns().len());
        println!("{tuple_ref}");

        if let DataTypeRef::Int(v) = tuple_ref.key(0)? {
            assert_eq!(v.to_owned(), 42);
        } else {
            panic!("Expected Int value");
        }

        if let DataTypeRef::Text(v) = tuple_ref.value(0)? {
            assert_eq!(v.as_str(TextEncoding::Utf8), "Alice");
        } else {
            panic!("Expected Text value");
        }


        if let DataTypeRef::Boolean(v) = tuple_ref.value(1)? {
            assert_eq!(v.to_owned(), 1u8);
        } else {
            panic!("Expected bool value");
        }

        if let DataTypeRef::Double(v) = tuple_ref.value(2)? {
            assert_eq!(v.to_owned(), 100.5);
        } else {
            panic!("Expected Float value");
        }

        if let DataTypeRef::Double(v) = tuple_ref.value(3)? {
            assert_eq!(v.to_owned(), 10.0);
        } else {
            panic!("Expected Float value");
        }

        if let DataTypeRef::Text(v) = tuple_ref.value(4)? {
            assert_eq!(v.as_str(TextEncoding::Utf8), "User description");
        } else {
            panic!("Expected Text value");
        }

        Ok(())
    }

    #[test]
    fn test_tuple_2() -> std::io::Result<()> {
        let schema = create_multi_key_schema();

        let values = vec![
            DataType::Int(Int32(42)),
            DataType::Text("Alice".into()),
            DataType::Boolean(UInt8(1)),
            DataType::Double(Float64(100.5)),
            DataType::Double(Float64(10.0)),
            DataType::Text("User description".into()),
        ];

        let tuple = Tuple::from_data(&values, &schema)?;
        assert!(!tuple.as_ref().is_empty());

        let tuple_ref = TupleRef::from(&tuple);
        assert_eq!(tuple_ref.num_fields(), schema.columns().len());
        println!("{tuple_ref}");

        if let DataTypeRef::Int(v) = tuple_ref.key(0)? {
            assert_eq!(v.to_owned(), 42);
        } else {
            panic!("Expected Int value");
        }

        if let DataTypeRef::Text(v) = tuple_ref.key(1)? {
            assert_eq!(v.as_str(TextEncoding::Utf8), "Alice");
        } else {
            panic!("Expected Text value");
        }

        if let DataTypeRef::Boolean(v) = tuple_ref.value(0)? {
            assert_eq!(v.to_owned(), 1u8);
        } else {
            panic!("Expected bool value");
        }

        if let DataTypeRef::Double(v) = tuple_ref.value(1)? {
            assert_eq!(v.to_owned(), 100.5);
        } else {
            panic!("Expected Float value");
        }

        if let DataTypeRef::Double(v) = tuple_ref.value(2)? {
            assert_eq!(v.to_owned(), 10.0);
        } else {
            panic!("Expected Float value");
        }

        if let DataTypeRef::Text(v) = tuple_ref.value(3)? {
            assert_eq!(v.as_str(TextEncoding::Utf8), "User description");
        } else {
            panic!("Expected Text value");
        }

        Ok(())
    }
}
