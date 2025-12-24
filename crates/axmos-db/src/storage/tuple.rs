use crate::{
    CELL_ALIGNMENT, SerializationError, SerializationResult, bytemuck_struct,
    multithreading::coordinator::Snapshot,
    schema::{Schema, base::SchemaError},
    storage::core::buffer::AlignedPayload,
    tree::cell_ops::{AsKeyBytes, Buildable, KeyBytes},
    types::{DataType, DataTypeKind, DataTypeRef, TransactionId},
};

use std::{
    alloc::AllocError,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    ops::{Index, IndexMut},
    slice::{Iter, IterMut},
};

#[derive(Debug)]
pub enum TupleError {
    Schema(SchemaError),
    Serialization(SerializationError),
    InvalidVersion(usize),
    KeyError(usize),
    ValueError(usize),
    DataTypeMismatch((usize, DataTypeKind)),
    InvalidValueCount(usize),
    AllocationError(AllocError),
}

impl Display for TupleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TupleError::KeyError(item) => write!(f, "key not found at index {item}"),
            TupleError::ValueError(item) => write!(f, "value not found at index {item}"),
            TupleError::Schema(err) => write!(f, "schema error {err}"),
            TupleError::Serialization(err) => write!(f, "serialization error {err}"),
            TupleError::InvalidVersion(item) => write!(f, "invalid version value {item}"),
            TupleError::DataTypeMismatch((index, datatype)) => write!(
                f,
                "datatype mismatch at index {index}. Expected: {datatype}"
            ),
            TupleError::InvalidValueCount(length) => {
                write!(f, "invalid number of values in the row {length}")
            }
            TupleError::AllocationError(err) => {
                write!(f, "tuple allocation error: {err}")
            }
        }
    }
}

impl From<SchemaError> for TupleError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
    }
}

impl From<SerializationError> for TupleError {
    fn from(value: SerializationError) -> Self {
        Self::Serialization(value)
    }
}

impl From<IoError> for TupleError {
    fn from(value: IoError) -> Self {
        Self::Serialization(SerializationError::Io(value))
    }
}

impl From<AllocError> for TupleError {
    fn from(value: AllocError) -> Self {
        Self::AllocationError(value)
    }
}

impl Error for TupleError {}
pub type TupleResult<T> = Result<T, TupleError>;

#[inline]
fn null_bitmap_size(num_values: usize) -> usize {
    num_values.div_ceil(8)
}

#[inline]
fn aligned_offset(offset: usize, align: usize) -> usize {
    (offset + align - 1) & !(align - 1)
}

/// A row is a higher level representation of a tuple
pub(crate) struct Row(Box<[DataType]>);

impl Row {
    pub(crate) fn new(data: Box<[DataType]>) -> Self {
        Self(data)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Validate a row by checking all value and keys against a [Schema]
    pub(crate) fn validate(&self, schema: &Schema) -> TupleResult<()> {
        if schema.num_columns() != self.len() {
            return Err(TupleError::InvalidValueCount(self.len()));
        }

        for (i, key_col) in schema.iter_keys().enumerate() {
            let row_key = self.key(i, schema).ok_or(TupleError::KeyError(i))?;
            if key_col.datatype() != row_key.kind() {
                return Err(TupleError::DataTypeMismatch((i, key_col.datatype())));
            }
        }

        for (i, val_col) in schema.iter_values().enumerate() {
            let value = self.value(i, schema).ok_or(TupleError::ValueError(i))?;
            if val_col.datatype() != value.kind() && !value.is_null() {
                return Err(TupleError::DataTypeMismatch((i, val_col.datatype())));
            }
        }

        Ok(())
    }

    /// Returns a reference to a key
    pub(crate) fn key(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        (index < schema.num_keys()).then(|| &self.0[index])
    }

    /// Returns a reference to a value in the row.
    pub(crate) fn value(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        self.0.get(schema.num_keys() + index)
    }

    pub(crate) fn key_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        (index < schema.num_keys()).then(|| &mut self.0[index])
    }

    pub(crate) fn value_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        self.0.get_mut(schema.num_keys() + index)
    }

    pub(crate) fn iter(&self) -> Iter<'_, DataType> {
        self.0.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> IterMut<'_, DataType> {
        self.0.iter_mut()
    }

    pub(crate) fn into_inner(self) -> Box<[DataType]> {
        self.0
    }
}

impl Index<usize> for Row {
    type Output = DataType;
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for Row {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a DataType;
    type IntoIter = Iter<'a, DataType>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a mut Row {
    type Item = &'a mut DataType;
    type IntoIter = IterMut<'a, DataType>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub(crate) struct TupleHeader {
    pub xmin: u64,
    pub xmax: i64,
    pub version: u8,
}

bytemuck_struct!(TupleHeader);

impl TupleHeader {
    pub fn new(version: u8, xmin: u64, xmax: Option<u64>) -> Self {
        Self {
            xmin,
            xmax: xmax.map(|v| v as i64).unwrap_or(-1),
            version,
        }
    }

    #[inline]
    pub fn xmax(&self) -> Option<u64> {
        if self.xmax < 0 {
            None
        } else {
            Some(self.xmax as u64)
        }
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn xmin(&self) -> u64 {
        self.xmin
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
struct DeltaHeader {
    pub xmin: u64,
    pub version: u8,
}

bytemuck_struct!(DeltaHeader);

impl DeltaHeader {
    pub fn new(version: u8, xmin: u64) -> Self {
        Self { xmin, version }
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn xmin(&self) -> u64 {
        self.xmin
    }
}

/// LAYOUT: [Header][Null Bitmap][Keys (aligned)][Values (aligned)][Deltas...]
///
/// Delta layout: [DeltaHeader][num_changes: u8][bitmap][changes...]
/// Each change: [field_idx: u8][value (aligned)]
#[derive(Debug, Clone)]
struct TupleLayout {
    /// Metadata about the tuple status in the database
    version_xmin: TransactionId,
    /// Set to None if the current version is not deleted
    version_xmax: Option<TransactionId>,

    /// Pointer to the start of the null bitmap for this version
    null_bitmap_start: usize,

    /// Offset to the keys for this version.
    ///
    /// Keys are inmutable so they should be the same for all versions.
    key_offsets: Vec<usize>,

    /// Offsets to the values of the tuple.
    value_offsets: Vec<usize>,

    /// POinter to where the data ends.
    data_end: usize,

    /// Current version the layout represents.
    version: u8,
}

impl TupleLayout {
    /// Parses the last version of the tuple.
    fn parse_last_version(data: &[u8], schema: &Schema) -> TupleResult<Self> {
        let num_values = schema.num_values();
        let num_keys = schema.num_keys();
        let bitmap_size = null_bitmap_size(num_values);

        let (header, header_end) = TupleHeader::read_from(data, 0);

        let null_bitmap_start = header_end;
        let null_bitmap = &data[null_bitmap_start..null_bitmap_start + bitmap_size];

        let mut key_offsets = Vec::with_capacity(num_keys);
        let mut cursor = null_bitmap_start + bitmap_size;

        // Parse keys
        for key_col in schema.iter_keys() {
            let dtype = key_col.datatype();
            key_offsets.push(cursor);
            let (_, new_cursor) = dtype.deserialize(data, cursor)?;
            cursor = new_cursor;
        }

        // Parse values
        let mut value_offsets = Vec::with_capacity(num_values);
        for (i, val_col) in schema.iter_values().enumerate() {
            if Self::check_null(null_bitmap, i) {
                value_offsets.push(cursor); // Placeholder for null
            } else {
                let dtype = val_col.datatype();
                value_offsets.push(cursor);
                let (_, new_cursor) = dtype.deserialize(data, cursor)?;
                cursor = new_cursor;
            }
        }

        Ok(TupleLayout {
            version_xmin: header.xmin(),
            version_xmax: header.xmax(),
            null_bitmap_start,
            key_offsets,
            value_offsets,
            data_end: cursor,
            version: header.version(),
        })
    }

    #[inline]
    fn current_version(&self) -> u8 {
        self.version
    }

    #[inline]
    fn delta_start(&self) -> usize {
        self.data_end
    }

    #[inline]
    fn version_xmax(&self) -> Option<TransactionId> {
        self.version_xmax
    }

    #[inline]
    fn version_xmin(&self) -> TransactionId {
        self.version_xmin
    }

    /// Parses a specific version by traversing deltas.
    fn parse_version(data: &[u8], schema: &Schema, target_version: u8) -> TupleResult<Self> {
        let mut layout = Self::parse_last_version(data, schema)?;

        if target_version > layout.current_version() {
            return Err(TupleError::InvalidVersion(target_version as usize));
        }

        if target_version == layout.current_version() {
            return Ok(layout);
        }

        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();

        // Delta layout: [DeltaHeader][num_changes: u8][bitmap][changes...]
        while cursor < data.len() {
            let (delta_header, header_end) = DeltaHeader::read_from(data, cursor);
            cursor = header_end;

            layout.version_xmax = Some(layout.version_xmin);
            layout.version = delta_header.version();
            layout.version_xmin = delta_header.xmin();

            // Read number of changes
            let num_changes = data[cursor] as usize;
            cursor += 1;

            // Delta's null bitmap
            layout.null_bitmap_start = cursor;
            cursor += bitmap_size;

            // Parse changes
            for _ in 0..num_changes {
                let field_idx = data[cursor] as usize;
                cursor += 1;

                let dtype = schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();

                layout.value_offsets[field_idx] = cursor;
                let (_, new_cursor) = dtype.deserialize(data, cursor)?;
                cursor = new_cursor;
            }

            if layout.current_version() == target_version {
                break;
            }
        }

        Ok(layout)
    }

    fn is_valid_for_snapshot(&self, snapshot: &Snapshot) -> bool {
        let created_before = snapshot.is_committed_before_snapshot(self.version_xmin);
        if let Some(xmax) = self.version_xmax {
            return created_before && !snapshot.is_committed_before_snapshot(xmax);
        }
        created_before
    }

    fn parse_for_snapshot(
        data: &[u8],
        schema: &Schema,
        snapshot: &Snapshot,
    ) -> TupleResult<Option<Self>> {
        let mut layout = Self::parse_last_version(data, schema)?;

        if let Some(xmax) = layout.version_xmax {
            if snapshot.is_committed_before_snapshot(xmax) {
                return Ok(None);
            }
        }

        if layout.is_valid_for_snapshot(snapshot) {
            return Ok(Some(layout));
        }

        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();

        while cursor < data.len() {
            let (delta_header, header_end) = DeltaHeader::read_from(data, cursor);
            cursor = header_end;

            layout.version_xmax = Some(layout.version_xmin);
            layout.version = delta_header.version();
            layout.version_xmin = delta_header.xmin();

            let num_changes = data[cursor] as usize;
            cursor += 1;

            layout.null_bitmap_start = cursor;
            cursor += bitmap_size;

            for _ in 0..num_changes {
                let field_idx = data[cursor] as usize;
                cursor += 1;

                let dtype = schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();


                layout.value_offsets[field_idx] = cursor;
                let (_, new_cursor) = dtype.deserialize(data, cursor)?;
                cursor = new_cursor;
            }

            if snapshot.is_committed_before_snapshot(layout.version_xmin) {
                return Ok(Some(layout));
            }
        }

        Ok(None)
    }

    #[inline]
    fn check_null(bitmap: &[u8], val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    #[inline]
    fn set_null_bit(bitmap: &mut [u8], val_idx: usize, is_null: bool) {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        if is_null {
            bitmap[byte_idx] |= 1 << bit_idx;
        } else {
            bitmap[byte_idx] &= !(1 << bit_idx);
        }
    }
}

pub(crate) struct TupleRef<'a, 'b> {
    data: &'a [u8],
    schema: &'b Schema,
    layout: TupleLayout,
}

impl<'a, 'b> TupleRef<'a, 'b> {
    pub(crate) fn read_last_version(buffer: &'a [u8], schema: &'b Schema) -> TupleResult<Self> {
        let layout = TupleLayout::parse_last_version(buffer, schema)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
        })
    }

    pub(crate) fn read_version(
        buffer: &'a [u8],
        schema: &'b Schema,
        version: u8,
    ) -> TupleResult<Self> {
        let layout = TupleLayout::parse_version(buffer, schema, version)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
        })
    }

    pub(crate) fn read_for_snapshot(
        buffer: &'a [u8],
        schema: &'b Schema,
        snapshot: &Snapshot,
    ) -> TupleResult<Option<Self>> {
        Ok(
            TupleLayout::parse_for_snapshot(buffer, schema, snapshot)?.map(|layout| Self {
                data: buffer,
                schema,
                layout,
            }),
        )
    }

    #[inline]
    pub(crate) fn schema(&self) -> &'b Schema {
        self.schema
    }

    #[inline]
    pub(crate) fn key_bytes(&self) -> KeyBytes<'_> {
        let keys_start = self.layout.key_offsets[0];
        let keys_end = self.layout.value_offsets[0];
        KeyBytes::from(&self.data[keys_start..keys_end])
    }

    #[inline]
    pub(crate) fn key_bytes_len(&self) -> usize {
        self.key_bytes().len()
    }

    #[inline]
    pub(crate) fn num_fields(&self) -> usize {
        self.schema.num_columns()
    }

    #[inline]
    pub(crate) fn current_version(&self) -> u8 {
        self.layout.current_version()
    }

    pub(crate) fn global_last_version(&self) -> u8 {
        let (header, _) = TupleHeader::read_from(self.data, 0);
        header.version()
    }

    pub(crate) fn global_xmin(&self) -> TransactionId {
        let (header, _) = TupleHeader::read_from(self.data, 0);
        header.xmin()
    }

    pub(crate) fn global_xmax(&self) -> Option<TransactionId> {
        let (header, _) = TupleHeader::read_from(self.data, 0);
        header.xmax()
    }

    #[inline]
    pub(crate) fn is_tuple_deleted(&self) -> bool {
        self.global_xmax().is_some()
    }

    #[inline]
    pub(crate) fn version_xmin(&self) -> TransactionId {
        self.layout.version_xmin()
    }

    #[inline]
    pub(crate) fn version_xmax(&self) -> Option<TransactionId> {
        self.layout.version_xmax()
    }

    #[inline]
    pub(crate) fn is_visible(&self, xid: TransactionId) -> bool {
        if let Some(xmax) = self.version_xmax() {
            return self.version_xmin() <= xid && xmax > xid;
        }
        self.version_xmin() <= xid
    }

    fn null_bitmap(&self) -> &[u8] {
        let start = self.layout.null_bitmap_start;
        let len = null_bitmap_size(self.schema.num_values());
        &self.data[start..start + len]
    }

    fn is_null(&self, val_idx: usize) -> bool {
        TupleLayout::check_null(self.null_bitmap(), val_idx)
    }

    pub(crate) fn key(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        if index >= self.schema.num_keys() {
            return Err(TupleError::KeyError(index));
        }

        let cursor = self
            .layout
            .key_offsets
            .get(index)
            .ok_or(TupleError::KeyError(index))?;
        let key_col = self.schema.key(index).ok_or(TupleError::KeyError(index))?;
        let dtype = key_col.datatype();
        let (value, _) = dtype.deserialize(self.data, *cursor)?;
        Ok(value)
    }

    pub(crate) fn value(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        if index >= self.schema.num_values() {
            return Err(TupleError::ValueError(index));
        }

        if self.is_null(index) {
            return Ok(DataTypeRef::Null);
        }

        let offset = self
            .layout
            .value_offsets
            .get(index)
            .ok_or(TupleError::ValueError(index))?;
        let dtype = self
            .schema
            .value(index)
            .ok_or(TupleError::ValueError(index))?
            .datatype();

        let (value, _) = dtype.deserialize(self.data, *offset)?;
        Ok(value)
    }

    pub(crate) fn to_row(&self) -> TupleResult<Row> {
        let mut data = Vec::with_capacity(self.num_fields());

        for i in 0..self.schema.num_keys() {
            let key_ref = self.key(i)?;
            data.push(key_ref.to_owned().unwrap_or(DataType::Null));
        }

        for i in 0..self.schema.num_values() {
            let val_ref = self.value(i)?;
            data.push(val_ref.to_owned().unwrap_or(DataType::Null));
        }

        Ok(Row(data.into_boxed_slice()))
    }
}

/// Layout: [Header][Null Bitmap][Keys][Values][Delta0][Delta1]...[DeltaN]
/// Delta: [DeltaHeader][num_changes: u8][bitmap][changes...]
/// Change: [field_idx: u8][value (aligned)]
#[derive(Clone)]
pub(crate) struct Tuple<'schema> {
    data: AlignedPayload,
    schema: &'schema Schema,
}

impl<'schema> Tuple<'schema> {
    pub(crate) fn new(row: Row, schema: &'schema Schema, xmin: TransactionId) -> TupleResult<Self> {
        row.validate(schema)?;

        let size = Self::calculate_initial_size(&row, schema);
        let mut data = AlignedPayload::alloc_aligned(size)?;

        Self::write_initial(&row, schema, xmin, data.data_mut())?;

        Ok(Self { data, schema })
    }

    /// Consumes the tuple and returns the aligned payload behind
    pub(crate) fn into_data(self) -> AlignedPayload {
        self.data
    }

    fn calculate_initial_size(row: &Row, schema: &Schema) -> usize {
        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);

        let mut cursor = TupleHeader::SIZE + bitmap_size;

        for (i, key_col) in schema.iter_keys().enumerate() {
            let dtype = key_col.datatype();
            cursor = aligned_offset(cursor, dtype.align());
            if let Some(key) = row.key(i, schema) {
                cursor += key.runtime_size();
            }
        }

        for (i, val_col) in schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, schema) {
                if !val.is_null() {
                    let dtype = val_col.datatype();
                    cursor = aligned_offset(cursor, dtype.align());
                    cursor += val.runtime_size();
                }
            }
        }

        cursor
    }

    fn write_initial(
        row: &Row,
        schema: &Schema,
        xmin: TransactionId,
        buffer: &mut [u8],
    ) -> SerializationResult<usize> {
        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);

        let header = TupleHeader::new(0, xmin, None);
        let mut cursor = header.write_to(buffer, 0);

        let bitmap_start = cursor;
        buffer[cursor..cursor + bitmap_size].fill(0);
        for (i, _) in schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, schema) {
                if val.is_null() {
                    TupleLayout::set_null_bit(
                        &mut buffer[bitmap_start..bitmap_start + bitmap_size],
                        i,
                        true,
                    );
                }
            }
        }
        cursor += bitmap_size;

        for (i, _key_col) in schema.iter_keys().enumerate() {
            if let Some(key) = row.key(i, schema) {
                cursor = key.write_to(buffer, cursor)?;
            }
        }

        for (i, _val_col) in schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, schema) {
                if !val.is_null() {
                    cursor = val.write_to(buffer, cursor)?;
                }
            }
        }

        Ok(cursor)
    }

    pub(crate) fn as_ref(&self) -> TupleRef<'_, 'schema> {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema).unwrap();
        TupleRef {
            data: self.data.data(),
            schema: self.schema,
            layout,
        }
    }

    #[inline]
    pub(crate) fn key_bytes(&self) -> KeyBytes<'_> {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema).unwrap();
        let keys_start = layout.key_offsets[0];
        let keys_end = layout.value_offsets[0];
        KeyBytes::from(&self.data.as_ref()[keys_start..keys_end])
    }

    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        self.data.data()
    }

    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        self.data.data_mut()
    }

    #[inline]
    pub(crate) fn key_bytes_len(&self) -> usize {
        self.as_ref().key_bytes_len()
    }

    pub(crate) fn key(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        if index >= self.schema.num_keys() {
            return Err(TupleError::KeyError(index));
        }

        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema)?;
        let cursor = layout
            .key_offsets
            .get(index)
            .ok_or(TupleError::KeyError(index))?;
        let key_col = self.schema.key(index).ok_or(TupleError::KeyError(index))?;
        let dtype = key_col.datatype();
        let (value, _) = dtype.deserialize(self.data.as_ref(), *cursor)?;
        Ok(value)
    }

    pub(crate) fn value(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        if index >= self.schema.num_values() {
            return Err(TupleError::ValueError(index));
        }

        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema)?;
        let bitmap_size = null_bitmap_size(self.schema.num_values());
        let null_bitmap =
            &self.data.data()[layout.null_bitmap_start..layout.null_bitmap_start + bitmap_size];

        if TupleLayout::check_null(null_bitmap, index) {
            return Ok(DataTypeRef::Null);
        }

        let offset = layout
            .value_offsets
            .get(index)
            .ok_or(TupleError::ValueError(index))?;
        let dtype = self
            .schema
            .value(index)
            .ok_or(TupleError::ValueError(index))?
            .datatype();

        let (value, _) = dtype.deserialize(self.data.as_ref(), *offset)?;
        Ok(value)
    }

    #[inline]
    pub(crate) fn num_keys(&self) -> usize {
        self.schema.num_keys()
    }

    #[inline]
    pub(crate) fn num_values(&self) -> usize {
        self.schema.num_values()
    }

    #[inline]
    pub(crate) fn num_fields(&self) -> usize {
        self.schema.num_columns()
    }

    pub(crate) fn version(&self) -> u8 {
        let (header, _) = TupleHeader::read_from(self.data.data(), 0);
        header.version()
    }

    pub(crate) fn xmin(&self) -> TransactionId {
        let (header, _) = TupleHeader::read_from(self.data.data(), 0);
        header.xmin()
    }

    pub(crate) fn xmax(&self) -> Option<TransactionId> {
        let (header, _) = TupleHeader::read_from(self.data.data(), 0);
        header.xmax()
    }

    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.xmax().is_some()
    }

    #[inline]
    pub(crate) fn is_visible(&self, xid: TransactionId) -> bool {
        if let Some(xmax) = self.xmax() {
            return self.xmin() <= xid && xmax > xid;
        }
        self.xmin() <= xid
    }

    /// Add a new version with modifications.
    ///
    /// Delta layout: [DeltaHeader][num_changes: u8][bitmap][changes...]
    /// Each change: [field_idx: u8][value (aligned)]
    pub(crate) fn add_version(
        &mut self,
        modified: &[(usize, DataType)],
        new_xmin: TransactionId,
    ) -> TupleResult<()> {
        // Validate modifications
        for (i, value) in modified {
            let col = self.schema.value(*i).ok_or(TupleError::ValueError(*i))?;
            if !value.is_null() && col.datatype() != value.kind() {
                return Err(TupleError::DataTypeMismatch((*i, col.datatype())));
            }
        }

        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema)?;
        let old_version = layout.version;
        let old_xmin = layout.version_xmin;
        let current_end = self.data.len();
        let num_values = self.schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);

        // Collect old values for delta
        let mut old_values: Vec<(u8, DataType)> = Vec::with_capacity(modified.len());
        for (i, _) in modified {
            let val_ref = self.value(*i)?;
            old_values.push((*i as u8, val_ref.to_owned().unwrap_or(DataType::Null)));
        }

        // Calculate delta size
        // Layout: [DeltaHeader (aligned)][num_changes: u8][bitmap][changes...]
        let delta_header_start = DeltaHeader::aligned_offset(current_end);
        let header_end = delta_header_start + DeltaHeader::SIZE;
        let num_changes_offset = header_end;
        let bitmap_start = num_changes_offset + 1;
        let changes_start = bitmap_start + bitmap_size;

        // Calculate changes size with alignment
        let mut cursor = changes_start;
        for (_, dt) in &old_values {
            cursor += 1; // field index byte
            cursor = aligned_offset(cursor, dt.align());
            cursor += dt.runtime_size();
        }
        let delta_end = cursor;
        let new_size = delta_end;

        // Reallocate buffer
        self.data.realloc(new_size)?;
        let buffer = self.data.data_mut();

        // Write delta header
        let delta_header = DeltaHeader::new(old_version, old_xmin);
        delta_header.write_to(buffer, delta_header_start);

        // Write number of changes
        buffer[num_changes_offset] = old_values.len() as u8;

        // Copy current null bitmap to delta
        let current_bitmap_start = layout.null_bitmap_start;
        buffer.copy_within(
            current_bitmap_start..current_bitmap_start + bitmap_size,
            bitmap_start,
        );

        // Write changes
        let mut cursor = changes_start;
        for (idx, old_value) in &old_values {
            // Update delta bitmap for old null state
            if old_value.is_null() {
                TupleLayout::set_null_bit(
                    &mut buffer[bitmap_start..bitmap_start + bitmap_size],
                    *idx as usize,
                    true,
                );
            }

            // Write field index
            buffer[cursor] = *idx;
            cursor += 1;

            // Write old value (write_to handles alignment)
            cursor = old_value.write_to(buffer, cursor)?;
        }

        // Update current values in-place
        for (i, new_value) in modified {
            let new_null = new_value.is_null();

            TupleLayout::set_null_bit(
                &mut buffer[current_bitmap_start..current_bitmap_start + bitmap_size],
                *i,
                new_null,
            );

            if new_null {
                continue;
            }

            let offset = layout.value_offsets[*i];
            new_value.write_to(buffer, offset)?;
        }

        // Update header
        let new_header = TupleHeader::new(old_version + 1, new_xmin, None);
        new_header.write_to(buffer, 0);

        Ok(())
    }

    pub(crate) fn delete(&mut self, xid: TransactionId) -> TupleResult<()> {
        if self.is_deleted() {
            return Ok(());
        }

        let buffer = self.data.data_mut();
        let (mut header, _) = TupleHeader::read_from(buffer, 0);
        header.xmax = xid as i64;
        header.write_to(buffer, 0);

        Ok(())
    }

    pub(crate) fn vacuum(&mut self, oldest_active_xid: TransactionId) -> TupleResult<usize> {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema)?;

        if layout.delta_start() >= self.data.len() {
            return Ok(0);
        }

        let num_values = self.schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();
        let mut last_needed_end = layout.delta_start();

        while cursor < self.data.len() {
            let (delta_header, header_end) = DeltaHeader::read_from(self.data.data(), cursor);

            if delta_header.xmin() >= oldest_active_xid {
                // Read num_changes and skip this delta
                let num_changes = self.data.data()[header_end] as usize;
                let mut skip_cursor = header_end + 1 + bitmap_size;

                for _ in 0..num_changes {
                    let field_idx = self.data.data()[skip_cursor] as usize;
                    skip_cursor += 1;

                    let dtype = self
                        .schema
                        .value(field_idx)
                        .ok_or(TupleError::ValueError(field_idx))?
                        .datatype();


                    let (_, new_cursor) = dtype.deserialize(self.data.data(), skip_cursor)?;
                    skip_cursor = new_cursor;
                }

                last_needed_end = skip_cursor;
                cursor = skip_cursor;
            } else {
                break;
            }
        }

        let freed = self.data.len() - last_needed_end;
        if freed > 0 {
            self.data.realloc(last_needed_end)?;
        }

        Ok(freed)
    }

    pub(crate) fn vacuum_for_snapshot(&mut self, snapshot: &Snapshot) -> TupleResult<usize> {
        self.vacuum(snapshot.xmin())
    }

    pub(crate) fn has_history(&self) -> bool {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema).unwrap();
        layout.delta_start() < self.data.len()
    }

    pub(crate) fn num_versions(&self) -> TupleResult<usize> {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema)?;

        if layout.delta_start() >= self.data.len() {
            return Ok(1);
        }

        let num_values = self.schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();
        let mut count = 1;

        while cursor < self.data.len() {
            let (_, header_end) = DeltaHeader::read_from(self.data.data(), cursor);
            let num_changes = self.data.data()[header_end] as usize;
            cursor = header_end + 1 + bitmap_size;

            for _ in 0..num_changes {
                let field_idx = self.data.data()[cursor] as usize;
                cursor += 1;

                let dtype = self
                    .schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();

                let (_, new_cursor) = dtype.deserialize(self.data.data(), cursor)?;
                cursor = new_cursor;
            }

            count += 1;
        }

        Ok(count)
    }

    pub(crate) fn into_row(self) -> TupleResult<Row> {
        self.as_ref().to_row()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn read_from(buffer: &[u8], schema: &'schema Schema) -> TupleResult<Self> {
        let _ = TupleLayout::parse_last_version(buffer, schema)?;

        let mut data = AlignedPayload::alloc_aligned(buffer.len())?;
        data.data_mut().copy_from_slice(buffer);

        Ok(Self { data, schema })
    }

    pub(crate) fn write_to(&self, buffer: &mut [u8]) -> SerializationResult<usize> {
        let len = self.data.len();
        buffer[..len].copy_from_slice(self.data.data());
        Ok(len)
    }

    pub(crate) fn serialized_size(&self) -> usize {
        self.data.len()
    }
}

impl<'schema> Buildable for Tuple<'schema> {
    fn built_size(&self) -> usize {
        self.data.len().next_multiple_of(CELL_ALIGNMENT as usize)
    }

    fn key_bounds(&self) -> (usize, usize) {
        let layout = TupleLayout::parse_last_version(self.data.data(), self.schema).unwrap();
        let keys_start = layout.key_offsets[0];
        let keys_end = layout.value_offsets[0];
        (keys_start, keys_end)
    }
}

impl<'schema> AsKeyBytes<'_> for Tuple<'schema> {
    fn as_key_bytes(&self) -> KeyBytes<'_> {
        self.key_bytes()
    }
}

impl<'a, 'schema> AsKeyBytes<'a> for TupleRef<'a, 'schema> {
    fn as_key_bytes(&self) -> KeyBytes<'_> {
        self.key_bytes()
    }
}

impl<'schema> From<Tuple<'schema>> for AlignedPayload {
    fn from(value: Tuple<'schema>) -> Self {
        value.into_data()
    }
}

#[cfg(test)]
mod tuple_tests {
    use super::*;
    use crate::Int64;
    use crate::schema::{Column, Schema};
    use crate::types::{Blob, DataType, DataTypeKind};

    fn test_schema() -> Schema {
        Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::BigInt, "age"),
            Column::new_with_defaults(DataTypeKind::Blob, "email"),
        ])
    }

    fn test_row() -> Row {
        Row(Box::new([
            DataType::BigInt(Int64(1)),
            DataType::Blob(Blob::from("Alice")),
            DataType::BigInt(Int64(30)),
            DataType::Blob(Blob::from("alice@example.com")),
        ]))
    }

    fn test_row_with_nulls() -> Row {
        Row(Box::new([
            DataType::BigInt(Int64(1)),
            DataType::Null,
            DataType::BigInt(Int64(30)),
            DataType::Null,
        ]))
    }

    #[test]
    fn test_tuple_new() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        assert_eq!(tuple.version(), 0);
        assert_eq!(tuple.xmin(), 1);
        assert_eq!(tuple.xmax(), None);
        assert!(!tuple.is_deleted());
        assert_eq!(tuple.num_keys(), 1);
        assert_eq!(tuple.num_values(), 3);
    }

    #[test]
    fn test_tuple_key_access() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        match tuple.key(0).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 1),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_value_access() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        match tuple.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_with_nulls() {
        let schema = test_schema();
        let row = test_row_with_nulls();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        assert!(matches!(tuple.value(0).unwrap(), DataTypeRef::Null));
        match tuple.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
        assert!(matches!(tuple.value(2).unwrap(), DataTypeRef::Null));
    }

    #[test]
    fn test_tuple_delete() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        assert!(!tuple.is_deleted());
        tuple.delete(10).unwrap();
        assert!(tuple.is_deleted());
        assert_eq!(tuple.xmax(), Some(10));
    }

    #[test]
    fn test_tuple_visibility() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 5).unwrap();

        assert!(!tuple.is_visible(4));
        assert!(tuple.is_visible(5));
        assert!(tuple.is_visible(100));
    }

    #[test]
    fn test_tuple_deleted_visibility() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 5).unwrap();
        tuple.delete(15).unwrap();

        assert!(!tuple.is_visible(4));
        assert!(tuple.is_visible(5));
        assert!(tuple.is_visible(10));
        assert!(!tuple.is_visible(15));
        assert!(!tuple.is_visible(20));
    }

    #[test]
    #[cfg(not(miri))] // Miri might force certain alignments that are not compatible with bytemuck
    fn test_tuple_serialization_roundtrip() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        let written = tuple.write_to(&mut buffer).unwrap();
        assert_eq!(written, size);

        let loaded = Tuple::read_from(&buffer, &schema).unwrap();
        assert_eq!(loaded.version(), tuple.version());
        assert_eq!(loaded.xmin(), tuple.xmin());
    }

    #[test]
    fn test_tuple_into_row() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let recovered = tuple.into_row().unwrap();
        assert_eq!(recovered.len(), 4);
    }

    #[test]
    fn test_tuple_ref_key_bytes() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let key_bytes = tuple.key_bytes();
        assert!(!key_bytes.is_empty());
    }

    #[test]
    fn test_tuple_add_version() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        assert_eq!(tuple.version(), 0);
        assert!(!tuple.has_history());

        let modifications = vec![(1, DataType::BigInt(Int64(31)))];
        tuple.add_version(&modifications, 10).unwrap();

        assert_eq!(tuple.version(), 1);
        assert!(tuple.has_history());
        assert_eq!(tuple.num_versions().unwrap(), 2);
        assert_eq!(tuple.xmin(), 10);

        match tuple.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 31),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_version_history_read() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        tuple
            .add_version(&[(1, DataType::BigInt(Int64(31)))], 10)
            .unwrap();

        let buffer = tuple.data();

        let ref_v1 = TupleRef::read_version(buffer, &schema, 1).unwrap();
        match ref_v1.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 31),
            _ => panic!("Expected BigInt"),
        }

        let ref_v0 = TupleRef::read_version(buffer, &schema, 0).unwrap();
        match ref_v0.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
    }
}
