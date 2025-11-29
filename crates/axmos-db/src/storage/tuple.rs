use crate::{
    TRANSACTION_ZERO,
    database::schema::Schema,
    types::{
        DataType, DataTypeKind, DataTypeRef, DataTypeRefMut, Float32RefMut, Float64RefMut,
        Int8RefMut, Int16RefMut, Int32RefMut, Int64RefMut, TransactionId, UInt8RefMut,
        UInt16RefMut, UInt32RefMut, UInt64RefMut, VarInt, reinterpret_cast, reinterpret_cast_mut,
    },
    varint::MAX_VARINT_LEN,
};
use std::{
    mem,
    ptr::{
        write,
        write_bytes,
        copy_nonoverlapping
    },
    collections::HashMap

};

// Macro used to read a tuple view from a byte slice.
// Can be used with both mutable and immutable views of the tuple.
macro_rules! read_tuple {
    ($tuple_type:ident, $buffer:expr, $schema:expr) => {{ read_tuple!($tuple_type, $buffer, $schema, None) }};

    ($tuple_type:ident, $buffer:expr, $schema:expr, $target_version:expr) => {{
        let mut tuple = $tuple_type {
            data: $buffer,
            schema: $schema,
            offsets: Vec::with_capacity($schema.columns.len()),
            key_len: 0,
            last_version_end: 0,
            target_version: 0,
            null_bitmap_offset: 0,
            xmin: TRANSACTION_ZERO,
            xmax: TRANSACTION_ZERO,
        };

        let mut cursor = 0usize;

        // Read keys and store offsets
        for key in $schema.iter_keys() {
            tuple.offsets.push(cursor as u16);
            let (_, read_bytes) = reinterpret_cast(key.dtype, &tuple.data[cursor..])?;
            tuple.key_len += read_bytes as u16;
            cursor += read_bytes;
        }

        // Skip version byte:
        tuple.target_version = tuple.data[cursor];
        cursor += 1;

        // Skip xmin and xmax:
        let transaction_id_size = mem::size_of::<TransactionId>();
        tuple.xmin = TransactionId::try_from(&tuple.data[cursor..cursor + transaction_id_size])?;
        cursor += transaction_id_size;

        tuple.xmax = TransactionId::try_from(&tuple.data[cursor..cursor +transaction_id_size])?;
        cursor += transaction_id_size;

        // Skip null bitmap:
        tuple.null_bitmap_offset = cursor as u16;
        let null_bitmap_size = $schema.values().len().div_ceil(8);
        cursor += null_bitmap_size;

        // Read values and store offsets
        for (i, col) in $schema.iter_values().enumerate() {
            tuple.offsets.push(cursor as u16);
            let is_null = tuple.is_null(i);
            if !is_null {
                let (_value, read_bytes) = reinterpret_cast(col.dtype, &tuple.data[cursor..])?;
                cursor += read_bytes;
            }
        }

        tuple.last_version_end = cursor as u16;

        if let Some(target_ver) = $target_version {
            if target_ver > tuple.target_version {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Version {} doesn't exist yet (current: {})",
                        target_ver, tuple.target_version
                    ),
                ));
            }
            tuple.target_version = target_ver;
            let num_keys = tuple.schema.num_keys as usize;

            let mut current_version = 0;

            while cursor < tuple.data.len() {
                current_version = tuple.data[cursor];
                let version_start = cursor;
                cursor += 1;

                tuple.xmin = TransactionId::try_from(&tuple.data[cursor..cursor + transaction_id_size])?;
                cursor += transaction_id_size;

                let (size_varint, varint_bytes) =
                    VarInt::from_encoded_bytes(&tuple.data[cursor..])?;

                let delta_size: usize = size_varint.try_into()?;
                cursor += varint_bytes;

                let delta_end = cursor + delta_size;
                tuple.null_bitmap_offset = cursor as u16;
                cursor += null_bitmap_size;

                while cursor < delta_end {
                    let offset_idx = tuple.data[cursor];
                    cursor += 1;


                    let dtype = tuple.schema.values()[offset_idx as usize].dtype;
                    tuple.offsets[offset_idx as usize + tuple.schema.num_keys as usize] =
                        cursor as u16;
                    let (data, read_bytes) = reinterpret_cast(dtype, &tuple.data[cursor..])?;

                    cursor += read_bytes;
                }

                if current_version == tuple.target_version {
                    break;
                }
            }
        }

        Ok(tuple)
    }};
}

/// Mutable view of a tuple at a specific point in the version chain.
pub struct TupleRefMut<'a, 'b> {
    // Actual data as a byte slice
    data: &'a mut [u8],
    // schema of the tuple (inmutable)
    schema: &'b Schema,
    // offsets to fields in the data slice
    offsets: Vec<u16>,
    // length of the tuple's keys
    key_len: u16,
    // offset where the last version of the tuple ends in the slice
    last_version_end: u16, // End of current version values.
    // offset to the null bitmap
    null_bitmap_offset: u16,

    // Version of the data this view is targeting
    target_version: u8,

    // This is metadata about the tuple it self, not the view.
    xmin: TransactionId, // Min transaction id that can see the tuple
    xmax: TransactionId, // Max transaction id that can see the tuple.
}

/// Mutable reference to a tuple buffer.
/// Allows to make modifications to the values in place (zero copy).
impl<'a, 'b> TupleRefMut<'a, 'b> {
    // Reads the last version of the tuple.
    pub fn read(buffer: &'a mut [u8], schema: &'b Schema) -> std::io::Result<TupleRefMut<'a, 'b>> {
        read_tuple!(TupleRefMut, buffer, schema)
    }

    // Read a specific version of the tuple.
    pub fn read_version(
        buffer: &'a mut [u8],
        schema: &'b Schema,
        version: u8,
    ) -> std::io::Result<TupleRefMut<'a, 'b>> {
        read_tuple!(TupleRefMut, buffer, schema, Some(version))
    }

    // Returns the number of fields in the tuple.
    pub fn num_fields(&self) -> usize {
        self.offsets.len()
    }

    // Returns the last version of the tuple.
    pub fn version(&self) -> u8 {
        self.target_version
    }

    // Computes the position in the buffer where the bytes of the last version start.
    pub fn last_version_start(&self) -> u16 {
        let bitmap_offset = self.key_len + 1;
        let bitmap_len = self.schema.columns.len() as u16;
        bitmap_offset + bitmap_len
    }

    // Returns true if the value corresponding to val_idx is set to null in the bitmap
    fn is_null(&self, val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        let bitmap_offset = self.key_len + 1;
        let bitmap = self.bitmap();
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn value(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.values()[index].dtype;
        let is_null = self.is_null(index);
        let offset_idx = index + self.schema.num_keys as usize;
        if !is_null {
            let (value, _) =
                reinterpret_cast(dtype, &self.data[self.offsets[offset_idx] as usize..])?;
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
                reinterpret_cast_mut(dtype, &mut self.data[self.offsets[offset_idx] as usize..])?;
            Ok(value)
        } else {
            Ok(DataTypeRefMut::Null)
        }
    }

    pub fn key(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index] as usize..])?;
        Ok(value)
    }

    fn key_mut(&mut self, index: usize) -> std::io::Result<DataTypeRefMut<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) =
            reinterpret_cast_mut(dtype, &mut self.data[self.offsets[index] as usize..])?;
        Ok(value)
    }

    // Get an immutable reference to the bitmap in place
    fn bitmap(&self) -> &[u8] {
        let bitmap_offset = self.null_bitmap_offset as usize;
        let bitmap_len = self.schema.columns.len();
        &self.data[bitmap_offset..bitmap_offset + bitmap_len]
    }

    // Get a mutable reference to the bitmap
    fn bitmap_mut(&mut self) -> &mut [u8] {
        let bitmap_offset = self.null_bitmap_offset as usize;
        let bitmap_len = self.schema.columns.len();
        &mut self.data[bitmap_offset..bitmap_offset + bitmap_len]
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

    // Sets the item found at a specified index to the corresponding value.
    pub fn set_value(&mut self, index: usize, value: DataType) -> std::io::Result<()> {
        let dtype = self.schema.columns[index].dtype;
        let offset = self.offsets[index] as usize;

        match (dtype, value) {
            (_, DataType::Null) => {
                self.set_null_unchecked(index);
            }
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
                ));
            }
        }
        Ok(())
    }

    // Sets a value to null
    pub fn set_null_unchecked(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        let bitmap = self.bitmap_mut();
        bitmap[byte_idx] |= 1 << bit_idx;
    }

    // Sets the value to non null in the bitmap.
    pub fn clear_null(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        let bitmap = self.bitmap_mut();
        bitmap[byte_idx] &= !(1 << bit_idx);
    }

    // Returns a reference to the tuple's schema
    pub fn schema(&self) -> &'b Schema {
        self.schema
    }

    // Creates an owned tuple from the reference
    fn to_owned(&self) -> OwnedTuple {
        OwnedTuple(self.data.to_vec().into_boxed_slice())
    }

    /// Returns true if the tuple has been deleted.
    pub fn is_deleted(&self) -> bool {
        self.xmax != TRANSACTION_ZERO
    }

    /// Returns true if the tuple is visible to the given transaction.
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        self.xmin <= xid && (self.xmax == TRANSACTION_ZERO || self.xmax > xid)
    }

    /// Marks the tuple as deleted in the buffer.
    /// xmax offset: key_len + 1 (version) + 8 (xmin)
    pub fn delete(&mut self, xid: TransactionId) -> std::io::Result<()> {
        if self.xmax != TRANSACTION_ZERO {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Tuple is already deleted",
            ));
        }
        let xmax_offset = self.key_len as usize + 1 + mem::size_of::<TransactionId>();
        let xid_bytes = xid.as_ref();
        self.data[xmax_offset..xmax_offset + xid_bytes.len()].copy_from_slice(xid_bytes);
        self.xmax = xid;
        Ok(())
    }
}

// Inmutable view of a tuple at a specific point in time.
pub struct TupleRef<'a, 'b> {
    // actual data of the tuple as a slice of bytes
    data: &'a [u8],

    // schema of the tuple
    schema: &'b Schema,

    // offsets to  values in the data
    offsets: Vec<u16>,

    // length of the keys
    key_len: u16,

    // offset where the last version ends
    last_version_end: u16,

    // offset to the null bitmap
    null_bitmap_offset: u16,

    // version of the data this tuple is targeting
    target_version: u8,

    // Metadata about the underlying tuple.
    xmin: TransactionId,// Min transaction id that can see the tuple
    xmax: TransactionId,// Max transaction id that can see the tuple
}

impl<'a, 'b> TupleRef<'a, 'b> {
    // Reads the last version of the tuple.
    pub fn read(buffer: &'a [u8], schema: &'b Schema) -> std::io::Result<TupleRef<'a, 'b>> {
        read_tuple!(TupleRef, buffer, schema)
    }

    // Method to read a specific version of the tuple.
    pub fn read_version(
        buffer: &'a [u8],
        schema: &'b Schema,
        version: u8,
    ) -> std::io::Result<TupleRef<'a, 'b>> {
        read_tuple!(TupleRef, buffer, schema, Some(version))
    }

    // Returns a reference to the schema
    pub fn schema(&self) -> &'b Schema {
        self.schema
    }

    // returns the number of fields in the tuple (keys + values)
    pub fn num_fields(&self) -> usize {
        self.offsets.len()
    }

    // return the version this tuple targets
    pub fn version(&self) -> u8 {
        self.target_version
    }

    // Checks the nullability of a field
    fn is_null(&self, val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        let null_bitmap_size = (self.schema.values().len()).div_ceil(8);
        let bitmap = self.bitmap();
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    // Get a value identified by its index.
    pub fn value(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.values()[index].dtype;
        let is_null = self.is_null(index);
        let offset_idx = index + self.schema.num_keys as usize;
        if !is_null {
            let (value, _) =
                reinterpret_cast(dtype, &self.data[self.offsets[offset_idx] as usize..])?;
            Ok(value)
        } else {
            Ok(DataTypeRef::Null)
        }
    }

    // get a key identified by its index
    pub fn key(&self, index: usize) -> std::io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let (value, _) = reinterpret_cast(dtype, &self.data[self.offsets[index] as usize..])?;
        Ok(value)
    }

    // get a reference to the null bitmap
    fn bitmap(&self) -> &[u8] {
        let bitmap_offset = self.null_bitmap_offset as usize;
        let bitmap_len = self.schema.columns.len();
        &self.data[bitmap_offset..bitmap_offset + bitmap_len]
    }

    // Creates an owned tuple from the reference
    fn to_owned(&self) -> OwnedTuple {
        OwnedTuple(self.data.to_vec().into_boxed_slice())
    }

    /// Returns true if the tuple has been deleted.
    pub fn is_deleted(&self) -> bool {
        self.xmax != TRANSACTION_ZERO
    }

    /// Returns true if the tuple is visible to the given transaction.
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        self.xmin <= xid && (self.xmax == TRANSACTION_ZERO || self.xmax > xid)
    }
}

/// Owned tuple structure.
/// Tuples are generally a [COPY-ON-WRITE] data structure, that is, you copy when creating a new one or creating a new version of it.
/// I am using only one byte to store both the version and also the index to fields, and I am not concerned about that, given that one byte is enough to store up to 255 versions and 255 columns, which is pretty uncommon.
///
/// Also, the vaccum workers will take care of resetting the version to zero every time they visit a tuple, and clean it up, so for the version to overflow, we would need more than 255 active transactions modifying the same tuple which I think is unrealistic.
#[derive(Clone)]
pub struct Tuple<'schema> {
    // Last version data.
    data: Vec<DataType>,
    // Tuple schema that cannot be modified by the tuple itself
    schema: &'schema Schema,
    // Current version
    version: u8, // Current version of the tuple.
    /// Transaction that created the tuple:
    xmin: TransactionId,
    /// Transaction that deleted the tuple:
    xmax: TransactionId,
    // Directory of versions of the tuple.
    delta_dir: HashMap<u8, Delta>,
}

#[derive(Debug, Clone)]
struct Delta {
    xmin: TransactionId,
    changes: Vec<(u8, DataType)>,
}


impl Delta {
    fn new(xmin: TransactionId) -> Self {
        Self { xmin, changes: Vec::new() }
    }


    fn with_capacity(xmin: TransactionId, capacity: usize) -> Self {
        Self { xmin, changes: Vec::with_capacity(capacity) }
    }

    fn iter_changes(&self) -> impl Iterator<Item = &(u8, DataType)> {
        self.changes.iter()
    }

    fn push(&mut self, value: (u8, DataType)) {
        self.changes.push(value);
    }

    fn extend<I: IntoIterator<Item = (u8, DataType)>>(&mut self, iter: I) {
        self.changes.extend(iter);
    }

    fn len(&self) -> usize {
        self.changes.len()
    }

    fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

impl<'schema> Tuple<'schema> {
    pub(crate) fn new(
        values: &[DataType],
        schema: &'schema Schema,
        xmin: TransactionId,
    ) -> std::io::Result<Self> {
        // Validate the data first.
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

        Ok(Self {
            data: values.to_vec(),
            schema,
            version: 0,
            delta_dir: HashMap::new(),
            xmin,
            xmax: TRANSACTION_ZERO,
        })
    }

    pub fn num_fields(&self) -> usize {
        self.data.len()
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn set_version(&mut self, version: u8) {
        self.version = version;
    }

    pub fn keys(&self) -> &[DataType] {
        &self.data[..self.schema.num_keys as usize]
    }

    pub fn values(&self) -> &[DataType] {
        &self.data[self.schema.num_keys as usize..]
    }

    pub fn values_mut(&mut self) -> &mut [DataType] {
        &mut self.data[self.schema.num_keys as usize..]
    }

    pub fn set_value(&mut self, index: usize, new: DataType) -> Option<DataType> {
        self.values_mut()
            .get_mut(index)
            .map(|slot| mem::replace(slot, new))
    }

    pub fn add_version(
        &mut self,
        modified: &[(usize, DataType)],
        xmin: TransactionId,
    ) -> std::io::Result<()> {
        let old_version = self.version;
        self.version += 1;
        let mut diffs = Delta::with_capacity(xmin, modified.len());

        for (i, value) in modified {
            if let Some(val) = self.schema.values().get(*i)
                && value.matches(val.dtype)
            {
                if let Some(old) = self.set_value(*i, value.clone()) {
                    diffs.push((*i as u8, old));
                }
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid tuple data type for field {i}"),
                ));
            }
        }

        self.delta_dir.insert(old_version, diffs);

        Ok(())
    }

    /// Marks the tuple as deleted by the given transaction.
    /// After deletion, the tuple is invisible to transactions >= xid.
    pub fn delete(&mut self, xid: TransactionId) -> std::io::Result<()> {
        if self.xmax != TRANSACTION_ZERO {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Tuple is already deleted",
            ));
        }
        self.xmax = xid;
        Ok(())
    }

    /// Returns true if the tuple has been deleted.
    pub fn is_deleted(&self) -> bool {
        self.xmax != TRANSACTION_ZERO
    }

    /// Returns true if the tuple is visible to the given transaction.
    /// Visible if: xmin <= xid AND (xmax == 0 OR xmax > xid)
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        self.xmin <= xid && (self.xmax == TRANSACTION_ZERO || self.xmax > xid)
    }


}

#[derive(Clone, Debug)]
pub struct OwnedTuple(Box<[u8]>);

/// Serializes the tuple to a byte array, consuming it.
/// The layout in memory of a tuple is the following:
/// [Keys....][current version byte][null bitmap][values last version...][size of the deltas][version byte][delta diff][version byte][delta diff]...
///
/// Note that at each version level the bitmap is different: each version keeps its private bitmap data to be able to set values to null at a specific version level.
impl<'schema> From<Tuple<'schema>> for OwnedTuple {
    fn from(value: Tuple<'schema>) -> OwnedTuple {
        OwnedTuple::from(&value)
    }
}

/// Serializes the tuple to a byte array, without consuming
impl<'schema> From<&Tuple<'schema>> for OwnedTuple {
    fn from(value: &Tuple<'schema>) -> OwnedTuple {
        // Compute the exact required size for the tuple data
        let null_bitmap_size = value.values().len().div_ceil(8);
        // Take into account xmin and xmax sizes (8 bytes each)
        let header_size = 2 * mem::size_of::<TransactionId>();

        // Size of the current version
        let current_version_data_size: usize = value.data.iter().map(|dt| dt.size()).sum();

        // Total size of the deltas (must take into accout the encoded length of each diff)
        let mut total_delta_size = 0usize;
        for (_, diffs) in value.delta_dir.iter() {
            // First, take into account the xmin value:
            let xmin_size = mem::size_of::<TransactionId>();
            // Each delta contains: version(1) + varint_size + bitmap + data
            let delta_data_size = null_bitmap_size +
                                  diffs.len() + // size of the indexes
                                  diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

            // Compute the total size of a varint
            let varint_size = VarInt::encoded_size(delta_data_size as i64);
            total_delta_size += 1 + varint_size + delta_data_size + xmin_size;
        }

        // Total size
        let total_size =
            current_version_data_size + 1 + null_bitmap_size + total_delta_size + header_size;

        // Alocate once with the exact required size
        let mut buffer: Box<[mem::MaybeUninit<u8>]> = Box::new_uninit_slice(total_size);

        unsafe {
            let ptr = buffer.as_mut_ptr() as *mut u8;
            let mut cursor = 0;

            // Write keys
            for key in value.keys() {
                let data = key.as_ref();
                copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                cursor += data.len();
            }

            let keys_len = cursor;

            // Write the version
            write(ptr.add(cursor), value.version);
            cursor += 1;

            // Write xmin and xmax:
            let data = value.xmin.as_ref();
            copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
            cursor += data.len();

            let data = value.xmax.as_ref();
            copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
            cursor += data.len();

            // Initialize the bitmap with zeroes
            let bitmap_start = cursor;
            write_bytes(ptr.add(cursor), 0, null_bitmap_size);
            cursor += null_bitmap_size;

            // Write the values and update the bitmap if nulls are found
            for (i, val) in value.values().iter().enumerate() {
                if let DataType::Null = val {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    *ptr.add(bitmap_start + byte_idx) |= 1 << bit_idx;
                } else {
                    let data = val.as_ref();
                    copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                    cursor += data.len();
                }
            }

            // Write the delta directory
            for (version, diffs) in value.delta_dir.iter() {
                // Version:
                write(ptr.add(cursor), *version);
                cursor += 1;

                // Xmin:
                let data = diffs.xmin.as_ref();
                copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                cursor += data.len();

                // Precompute the size
                let delta_data_size = null_bitmap_size
                    + diffs.len()
                    + diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

                // Write the size as varint
                let mut varint_buffer = [0u8; MAX_VARINT_LEN];
                let encoded = VarInt::encode(delta_data_size as i64, &mut varint_buffer);
                copy_nonoverlapping(encoded.as_ptr(), ptr.add(cursor), encoded.len());
                cursor += encoded.len();

                // Copy the bitmap and modify in place
                let delta_bitmap_start = cursor;
                copy_nonoverlapping(
                    ptr.add(bitmap_start),
                    ptr.add(cursor),
                    null_bitmap_size,
                );
                cursor += null_bitmap_size;

                // Update the bitmap of the delta
                for (idx, old_value) in diffs.iter_changes() {
                    let byte_idx = *idx as usize / 8;
                    let bit_idx = *idx as usize % 8;

                    if let DataType::Null = old_value {
                        *ptr.add(delta_bitmap_start + byte_idx) |= 1 << bit_idx;
                    } else {
                        let current_is_null =
                            (*ptr.add(bitmap_start + byte_idx) & (1 << bit_idx)) != 0;
                        if current_is_null {
                            *ptr.add(delta_bitmap_start + byte_idx) &= !(1 << bit_idx);
                        }
                    }

                    // write the index
                    write(ptr.add(cursor), *idx);
                    cursor += 1;

                    // write the data
                    let data = old_value.as_ref();
                    copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                    cursor += data.len();
                }
            }

            // Verify we used exactly the allocated space
            debug_assert_eq!(cursor, total_size, "Size calculation mismatch");

            // Transmute from maybeuninit to u8
            OwnedTuple(Box::from_raw(Box::into_raw(buffer) as *mut [u8]))
        }
    }
}

impl AsRef<[u8]> for OwnedTuple {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for OwnedTuple {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl<'schema> TryFrom<(&[u8], &'schema Schema)> for Tuple<'schema> {
    type Error = std::io::Error;

    fn try_from((buffer, schema): (&[u8], &'schema Schema)) -> Result<Self, Self::Error> {
        let tuple_ref = TupleRef::read(buffer, schema)?;
        let mut data = Vec::with_capacity(schema.columns.len());
        for i in 0..schema.num_keys as usize {
            data.push(tuple_ref.key(i)?.to_owned());
        }

        for i in 0..schema.values().len() {
            data.push(tuple_ref.value(i)?.to_owned());
        }

        let current_version = buffer[tuple_ref.key_len as usize];
        let xmin = tuple_ref.xmin;
        let xmax = tuple_ref.xmax;
        let mut delta_dir = HashMap::new();
        let mut cursor = tuple_ref.last_version_end as usize;
        let null_bitmap_size = schema.values().len().div_ceil(8);

        while cursor < buffer.len() {
            let version = buffer[cursor];
            cursor += 1;
            let transaction_id_size = mem::size_of::<TransactionId>();
            let version_xmin = TransactionId::try_from(&buffer[cursor..cursor + transaction_id_size])?;
            cursor += transaction_id_size;

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes;

            let delta_end = cursor + delta_size;
            cursor += null_bitmap_size; // Skip bitmap

            let mut delta = Delta::new(version_xmin);

            while cursor < delta_end {
                let field_index = buffer[cursor];
                cursor += 1;

                let dtype = schema.values()[field_index as usize].dtype;
                let (value, read_bytes) = reinterpret_cast(dtype, &buffer[cursor..])?;
                delta.push((field_index, value.to_owned()));
                cursor += read_bytes;
            }


            delta_dir.insert(version, delta);
        }

        Ok(Self {
            data,
            schema,
            xmin,
            xmax,
            version: current_version,
            delta_dir,
        })
    }
}

#[cfg(test)]
mod tuple_tests {

    use super::*;
    use crate::database::schema::{Column, Schema};
    use crate::types::{DataType, DataTypeKind, UInt8};

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
    fn test_tuple_1() {
        let schema = create_single_key_schema();

        let result = Tuple::new(
            &[
                DataType::Int(123.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.5.into()),
                DataType::Double(5.75.into()),
                DataType::Text("Initial description".into()),
            ],
            &schema,
            TransactionId::from(1),
        );

        assert!(result.is_ok());
        let mut tup = result.unwrap();
        assert_eq!(tup.version(), 0);

        let id = &tup.keys()[0];
        assert_eq!(id, &DataType::Int(123.into()));

        let alice = &tup.values()[0];
        assert_eq!(alice, &DataType::Text("Alice".into()));

        let result = tup.add_version(
            vec![
                (0, DataType::Text("Alice Updated".into())),
                (2, DataType::Double(999.0.into())),
            ]
            .as_slice(),
            TransactionId::from(2),
        );

        assert!(result.is_ok());

        assert_eq!(tup.version(), 1);

        let buf: OwnedTuple = tup.into();

        let t = TupleRef::read(buf.as_ref(), &schema).unwrap();
        println!("{t}");
        assert_eq!(t.key(0).unwrap().to_owned(), DataType::Int(123.into()));

        assert_eq!(
            t.value(0).unwrap().to_owned(),
            DataType::Text("Alice Updated".into())
        );
        assert_eq!(
            t.value(1).unwrap().to_owned(),
            DataType::Boolean(true.into())
        );
        assert_eq!(
            t.value(2).unwrap().to_owned(),
            DataType::Double(999.0.into())
        );

        assert_eq!(
            t.value(3).unwrap().to_owned(),
            DataType::Double(5.75.into())
        );
        assert_eq!(
            t.value(4).unwrap().to_owned(),
            DataType::Text("Initial description".into())
        );

        let t0 = TupleRef::read_version(buf.as_ref(), &schema, 0).unwrap();

        // We should read the data from the previous version.
        assert_eq!(t0.version(), 0);
        assert_eq!(
            t0.value(0).unwrap().to_owned(),
            DataType::Text("Alice".into())
        );
        assert_eq!(
            t0.value(2).unwrap().to_owned(),
            DataType::Double(100.5.into())
        );
    }

    #[test]
    fn test_tuple_2() {
        let schema = create_multi_key_schema();

        let result = Tuple::new(
            &[
                DataType::Int(1.into()),                      // key 0
                DataType::Text("Widget A".into()),            // key 1
                DataType::Boolean(UInt8::TRUE),               // value 0
                DataType::Double(10.5.into()),                // value 1
                DataType::Double(2.25.into()),                // value 2
                DataType::Text("Initial description".into()), // value 3
            ],
            &schema,
            TransactionId::from(1),
        );

        assert!(result.is_ok());
        let mut tup = result.unwrap();
        assert_eq!(tup.version(), 0);

        let id = &tup.keys()[0];
        assert_eq!(id, &DataType::Int(1.into()));

        let pname = &tup.keys()[1];
        assert_eq!(pname, &DataType::Text("Widget A".into()));

        assert_eq!(tup.values()[0], DataType::Boolean(UInt8::TRUE));
        assert_eq!(tup.values()[1], DataType::Double(10.5.into()));
        assert_eq!(tup.values()[2], DataType::Double(2.25.into()));
        assert_eq!(
            tup.values()[3],
            DataType::Text("Initial description".into())
        );

        let result = tup.add_version(
            vec![
                (0, DataType::Boolean(UInt8::FALSE)),
                (3, DataType::Text("Updated description".into())),
            ]
            .as_slice(),
            TransactionId::from(2),
        );

        assert!(result.is_ok());
        assert_eq!(tup.version(), 1);

        let buf: OwnedTuple = tup.into();

        let t = TupleRef::read(buf.as_ref(), &schema).unwrap();

        println!("{t}");

        assert_eq!(t.key(0).unwrap().to_owned(), DataType::Int(1.into()));
        assert_eq!(
            t.key(1).unwrap().to_owned(),
            DataType::Text("Widget A".into())
        );
        assert_eq!(
            t.value(0).unwrap().to_owned(),
            DataType::Boolean(UInt8::FALSE)
        );
        assert_eq!(
            t.value(3).unwrap().to_owned(),
            DataType::Text("Updated description".into())
        );
        assert_eq!(
            t.value(1).unwrap().to_owned(),
            DataType::Double(10.5.into())
        );
        assert_eq!(
            t.value(2).unwrap().to_owned(),
            DataType::Double(2.25.into())
        );
        let t0 = TupleRef::read_version(buf.as_ref(), &schema, 0).unwrap();

        assert_eq!(t0.version(), 0);
        assert_eq!(
            t0.value(0).unwrap().to_owned(),
            DataType::Boolean(UInt8::TRUE)
        );
        assert_eq!(
            t0.value(3).unwrap().to_owned(),
            DataType::Text("Initial description".into())
        );

        assert_eq!(t0.key(0).unwrap().to_owned(), DataType::Int(1.into()));
        assert_eq!(
            t0.key(1).unwrap().to_owned(),
            DataType::Text("Widget A".into())
        );
    }

    #[test]
    fn test_tuple_3() {
        let schema = create_single_key_schema();

        let original = Tuple::new(
            &[
                DataType::Int(123.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.5.into()),
                DataType::Double(5.75.into()),
                DataType::Text("Initial description".into()),
            ],
            &schema,
            TransactionId::from(1),
        )
        .unwrap();

        let buffer: OwnedTuple = original.into();

        let reconstructed = Tuple::try_from((buffer.as_ref(), &schema)).unwrap();

        assert_eq!(reconstructed.version(), 0);
        assert_eq!(reconstructed.keys()[0], DataType::Int(123.into()));
        assert_eq!(reconstructed.values()[0], DataType::Text("Alice".into()));
        assert_eq!(reconstructed.values()[1], DataType::Boolean(UInt8::TRUE));
        assert_eq!(reconstructed.values()[2], DataType::Double(100.5.into()));
    }

    #[test]
    fn test_tuple_4() {
        let schema = create_single_key_schema();
        let xid_100 = TransactionId::from(100u64);
        let xid_150 = TransactionId::from(150u64);
        let xid_200 = TransactionId::from(200u64);

        let mut tup = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_100,
        )
        .unwrap();

        assert!(!tup.is_deleted());
        assert!(tup.is_visible(xid_100));
        assert!(tup.is_visible(xid_150));

        // Delete at xid 200
        tup.delete(xid_200).unwrap();

        assert!(tup.is_deleted());
        assert!(tup.is_visible(xid_150)); // Before delete: visible
        assert!(!tup.is_visible(xid_200)); // At delete: not visible
        assert!(!tup.is_visible(TransactionId::from(250u64))); // After delete: not visible

        // Cannot delete twice
        assert!(tup.delete(TransactionId::from(300u64)).is_err());
    }

    #[test]
    fn test_tuple_5() {
        let schema = create_single_key_schema();
        let xid_100 = TransactionId::from(100u64);
        let xid_200 = TransactionId::from(200u64);

        let mut tup = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_100,
        )
        .unwrap();

        tup.delete(xid_200).unwrap();

        let buf: OwnedTuple = tup.into();
        let t = TupleRef::read(buf.as_ref(), &schema).unwrap();

        assert!(t.is_deleted());
        assert!(t.is_visible(TransactionId::from(150u64)));
        assert!(!t.is_visible(xid_200));
    }

    #[test]
    fn test_tuple_6() {
        let schema = create_single_key_schema();
        let xid_100 = TransactionId::from(100u64);
        let xid_200 = TransactionId::from(200u64);

        let tup = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_100,
        )
        .unwrap();

        let mut buf: OwnedTuple = tup.into();

        {
            let mut t = TupleRefMut::read(buf.as_mut(), &schema).unwrap();
            assert!(!t.is_deleted());
            t.delete(xid_200).unwrap();
            assert!(t.is_deleted());
        }

        // Verify persisted
        let t = TupleRef::read(buf.as_ref(), &schema).unwrap();
        assert!(t.is_deleted());
        assert!(t.is_visible(TransactionId::from(150u64)));
        assert!(!t.is_visible(xid_200));
    }

    #[test]
    fn test_tuple_7() {
        let schema = create_single_key_schema();
        let xid_100 = TransactionId::from(100u64);
        let xid_50 = TransactionId::from(50u64);

        let tup = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_100,
        )
        .unwrap();

        // Transaction 50 started before tuple was created
        assert!(!tup.is_visible(xid_50));
        assert!(tup.is_visible(xid_100));
    }
}
