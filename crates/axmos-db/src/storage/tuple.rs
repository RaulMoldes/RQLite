use crate::{
    SerializationError, SerializationResult, bytemuck_struct,
    multithreading::coordinator::Snapshot,
    schema::{Schema, base::SchemaError},
    storage::core::buffer::{Payload, PayloadRef},
    types::{DataType, DataTypeKind, DataTypeRef, TransactionId},
};

use std::{
    alloc::AllocError,
    collections::HashMap,
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
#[derive(Clone, Debug)]
pub struct Row(Box<[DataType]>);

impl Default for Row {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Row {
    pub fn new(data: Box<[DataType]>) -> Self {
        Self(data)
    }

    pub fn new_empty() -> Self {
        Self(Box::new([]))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Validate a row by checking all value and keys against a [Schema]
    fn validate(&self, schema: &Schema) -> TupleResult<()> {
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
    pub fn key(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        (index < schema.num_keys()).then(|| &self.0[index])
    }

    /// Returns a reference to a value in the row.
    pub fn value(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        self.0.get(schema.num_keys() + index)
    }

    pub fn key_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        (index < schema.num_keys()).then(|| &mut self.0[index])
    }

    pub fn value_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        self.0.get_mut(schema.num_keys() + index)
    }

    pub fn iter(&self) -> Iter<'_, DataType> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, DataType> {
        self.0.iter_mut()
    }

    pub fn into_inner(self) -> Box<[DataType]> {
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
pub(crate) struct TupleLayout {
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

impl Display for TupleLayout {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f, "TupleLayout {{")?;
        writeln!(f, "  version: {}", self.version)?;
        writeln!(f, "  version_xmin: {:?}", self.version_xmin)?;
        writeln!(f, "  version_xmax: {:?}", self.version_xmax)?;
        writeln!(f, "  null_bitmap_start: {}", self.null_bitmap_start)?;
        writeln!(f, "  key_offsets: {:?}", self.key_offsets)?;
        writeln!(f, "  value_offsets: {:?}", self.value_offsets)?;
        writeln!(f, "  data_end: {}", self.data_end)?;
        write!(f, "}}")
    }
}

// Tuple builder and tuple reader structs allow to avoid storing the schema reference inside the tuple which is annoying
pub struct TupleReader<'a> {
    schema: &'a Schema,
}

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
}

/// The tuple reader parses byte arraays and builds tuple layouts.
impl<'a> TupleBuilder<'a> {
    pub fn from_schema(schema: &'a Schema) -> Self {
        Self { schema }
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

    /// Write the initial content of the tuple to a buffer.
    fn write_initial(
        &self,
        row: &Row,
        xmin: TransactionId,
        buffer: &mut [u8],
    ) -> SerializationResult<usize> {
        let num_values = self.schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);

        let header = TupleHeader::new(0, xmin, None);
        let mut cursor = header.write_to(buffer, 0);

        let bitmap_start = cursor;
        buffer[cursor..cursor + bitmap_size].fill(0);
        for (i, _) in self.schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, self.schema) {
                if val.is_null() {
                    Self::set_null_bit(
                        &mut buffer[bitmap_start..bitmap_start + bitmap_size],
                        i,
                        true,
                    );
                }
            }
        }
        cursor += bitmap_size;

        for (i, _key_col) in self.schema.iter_keys().enumerate() {
            if let Some(key) = row.key(i, self.schema) {
                cursor = key.write_to(buffer, cursor)?;
            }
        }

        for (i, _val_col) in self.schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, self.schema) {
                if !val.is_null() {
                    cursor = val.write_to(buffer, cursor)?;
                }
            }
        }

        Ok(cursor)
    }

    /// Computes the required initial bytes for a tuple to b stored.
    fn compute_initial_size(&self, row: &Row) -> usize {
        let num_values = self.schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);

        let mut cursor = TupleHeader::SIZE + bitmap_size;

        for (i, key_col) in self.schema.iter_keys().enumerate() {
            let dtype = key_col.datatype();
            cursor = aligned_offset(cursor, dtype.align());
            if let Some(key) = row.key(i, self.schema) {
                cursor += key.runtime_size();
            }
        }

        for (i, val_col) in self.schema.iter_values().enumerate() {
            if let Some(val) = row.value(i, self.schema) {
                if !val.is_null() {
                    let dtype = val_col.datatype();
                    cursor = aligned_offset(cursor, dtype.align());
                    cursor += val.runtime_size();
                }
            }
        }

        cursor
    }

    /// Build a tuple from a given row.
    pub(crate) fn build(&self, row: Row, xmin: TransactionId) -> TupleResult<Tuple> {
        row.validate(self.schema)?;

        let size = self.compute_initial_size(&row);
        let mut data = Payload::alloc_aligned(size)?;

        self.write_initial(&row, xmin, data.effective_data_mut())?;

        Ok(Tuple { data })
    }
}

/// Inmutable tuple accessor to get access to a single tuple ref.
pub struct RefTupleAccessor<'a, 'b> {
    schema: &'a Schema,
    tuple: TupleRef<'b>,
}

impl<'a, 'b> RefTupleAccessor<'a, 'b> {
    pub(crate) fn new(tuple: TupleRef<'b>, schema: &'a Schema) -> Self {
        Self { schema, tuple }
    }

    pub(crate) fn key(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        self.tuple.key_with_schema(index, &self.schema)
    }

    pub(crate) fn value(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        self.tuple.value_with_schema(index, &self.schema)
    }

    pub(crate) fn is_null(&self, index: usize) -> bool {
        self.tuple.is_null_with_schema(index, self.schema)
    }

    pub(crate) fn null_bitmap(&self) -> &[u8] {
        self.tuple.null_bitmap_with_schema(&self.schema)
    }

    pub(crate) fn to_row(&self) -> TupleResult<Row> {
        self.tuple.to_row_with_schema(&self.schema)
    }

    pub(crate) fn reader(&self) -> TupleReader<'a> {
        TupleReader {
            schema: self.schema,
        }
    }
}

/// Tuple modifier with an owned version of the tuple
pub struct OwnedTupleAccessor<'a> {
    tuple: Tuple,
    schema: &'a Schema,
}

impl<'a> OwnedTupleAccessor<'a> {
    fn new(tuple: Tuple, schema: &'a Schema) -> Self {
        Self { tuple, schema }
    }

    fn add_version(
        &mut self,
        modified: &HashMap<usize, DataType>,
        new_xmin: TransactionId,
    ) -> TupleResult<()> {
        self.tuple
            .add_version_with_schema(modified, new_xmin, self.schema)
    }

    fn vaccum_for_snapshot(&mut self, snapshot: &Snapshot) -> TupleResult<usize> {
        self.tuple.vacuum_with_schema(snapshot.xmin(), self.schema)
    }

    fn has_history(&self) -> bool {
        self.tuple.has_history_with_schema(&self.schema)
    }

    fn num_versions(&self) -> TupleResult<usize> {
        self.tuple.num_versions_with_schema(self.schema)
    }

    pub fn reader(&self) -> TupleReader<'a> {
        TupleReader {
            schema: self.schema,
        }
    }

    pub(crate) fn into_data(self) -> Payload {
        self.tuple.into_data()
    }

    pub(crate) fn version(&self) -> u8 {
        self.tuple.version()
    }

    pub(crate) fn payload_ref(&self) -> PayloadRef<'_> {
        self.tuple.payload_ref()
    }

    pub(crate) fn delete(&mut self, transaction_id: TransactionId) -> TupleResult<()> {
        self.tuple.delete(transaction_id)
    }

    pub(crate) fn xmin(&self) -> TransactionId {
        self.tuple.xmin()
    }

    pub(crate) fn xmax(&self) -> Option<TransactionId> {
        self.tuple.xmax()
    }

    pub(crate) fn is_visible(&self, xid: TransactionId) -> bool {
        self.tuple.is_visible(xid)
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.tuple.is_deleted()
    }
}

impl<'schema> From<OwnedTupleAccessor<'schema>> for Payload {
    fn from(value: OwnedTupleAccessor<'schema>) -> Self {
        value.tuple.into_data()
    }
}

/// The tuple reader parses byte arraays and builds tuple layouts.
impl<'a> TupleReader<'a> {
    pub fn from_schema(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Parses the last version of the tuple.
    pub(crate) fn parse_last_version(&self, data: &[u8]) -> TupleResult<TupleLayout> {
        let num_values = self.schema.num_values();
        let num_keys = self.schema.num_keys();
        let bitmap_size = null_bitmap_size(num_values);

        let (header, header_end) = TupleHeader::read_from(data, 0);

        let null_bitmap_start = header_end;
        let null_bitmap = &data[null_bitmap_start..null_bitmap_start + bitmap_size];

        let mut key_offsets = Vec::with_capacity(num_keys);
        let mut cursor = null_bitmap_start + bitmap_size;

        // Parse keys
        for key_col in self.schema.iter_keys() {
            let dtype = key_col.datatype();
            key_offsets.push(cursor);
            let (_, new_cursor) = dtype.deserialize(data, cursor)?;
            cursor = new_cursor;
        }

        // Parse values
        let mut value_offsets = Vec::with_capacity(num_values);
        for (i, val_col) in self.schema.iter_values().enumerate() {
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

    /// Parses a specific version by traversing deltas.
    fn parse_version(&self, data: &[u8], target_version: u8) -> TupleResult<TupleLayout> {
        let mut layout = self.parse_last_version(data)?;

        if target_version > layout.current_version() {
            return Err(TupleError::InvalidVersion(target_version as usize));
        }

        if target_version == layout.current_version() {
            return Ok(layout);
        }

        let num_values = self.schema.num_values();
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

                let dtype = self
                    .schema
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

    /// Parses a byte array for a specific snapshot.
    ///
    /// See [crate::multithreading::Snapshot]
    pub fn parse_for_snapshot(
        &self,
        data: &[u8],
        snapshot: &Snapshot,
    ) -> TupleResult<Option<TupleLayout>> {
        let mut layout = self.parse_last_version(data)?;

        if let Some(xmax) = layout.version_xmax {
            if snapshot.is_committed_before_snapshot(xmax) {
                return Ok(None);
            }
        }

        if layout.is_valid_for_snapshot(snapshot) {
            return Ok(Some(layout));
        }

        let num_values = self.schema.num_values();
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

                let dtype = self
                    .schema
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
}

impl TupleLayout {
    #[inline]
    fn current_version(&self) -> u8 {
        self.version
    }

    #[inline]
    fn delta_start(&self) -> usize {
        DeltaHeader::aligned_offset(self.data_end)
    }

    #[inline]
    fn version_xmax(&self) -> Option<TransactionId> {
        self.version_xmax
    }

    #[inline]
    fn version_xmin(&self) -> TransactionId {
        self.version_xmin
    }

    fn is_valid_for_snapshot(&self, snapshot: &Snapshot) -> bool {

        let created_by_snapshot = snapshot.xid() == self.version_xmin;
        let created_before = snapshot.is_committed_before_snapshot(self.version_xmin) || created_by_snapshot;

        if let Some(xmax) = self.version_xmax {
            let deleted_by_snapshot = snapshot.xid() == xmax;
            return created_before && !(snapshot.is_committed_before_snapshot(xmax) || deleted_by_snapshot);
        }
        created_before
    }

    pub fn key_start(&self) -> usize {
        self.key_offsets[0]
    }

    pub fn key_size(&self) -> usize {
        self.value_offsets[0].saturating_sub(self.key_start())
    }
}

#[derive(Clone)]
pub(crate) struct TupleRef<'a> {
    data: &'a [u8],
    layout: TupleLayout,
}

impl<'a> TupleRef<'a> {
    pub fn new(data: &'a [u8], layout: TupleLayout) -> Self {
        Self { data, layout }
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

    fn null_bitmap_with_schema(&self, schema: &Schema) -> &[u8] {
        let start = self.layout.null_bitmap_start;
        let len = null_bitmap_size(schema.num_values());
        &self.data[start..start + len]
    }

    fn is_null_with_schema(&self, val_idx: usize, schema: &Schema) -> bool {
        TupleReader::check_null(self.null_bitmap_with_schema(schema), val_idx)
    }

    /// Returns the key found at a specific index
    pub(crate) fn key_with_schema(
        &self,
        index: usize,
        schema: &Schema,
    ) -> TupleResult<DataTypeRef<'_>> {
        if index >= schema.num_keys() {
            return Err(TupleError::KeyError(index));
        }

        let cursor = self
            .layout
            .key_offsets
            .get(index)
            .ok_or(TupleError::KeyError(index))?;
        let key_col = schema.key(index).ok_or(TupleError::KeyError(index))?;
        let dtype = key_col.datatype();
        let (value, _) = dtype.deserialize(self.data, *cursor)?;
        Ok(value)
    }

    pub(crate) fn value_with_schema(
        &self,
        index: usize,
        schema: &Schema,
    ) -> TupleResult<DataTypeRef<'_>> {
        if index >= schema.num_values() {
            return Err(TupleError::ValueError(index));
        }

        if self.is_null_with_schema(index, schema) {
            return Ok(DataTypeRef::Null);
        }

        let offset = self
            .layout
            .value_offsets
            .get(index)
            .ok_or(TupleError::ValueError(index))?;
        let dtype = schema
            .value(index)
            .ok_or(TupleError::ValueError(index))?
            .datatype();

        let (value, _) = dtype.deserialize(self.data, *offset)?;
        Ok(value)
    }

    pub(crate) fn to_row_with_schema(&self, schema: &Schema) -> TupleResult<Row> {
        let mut data = Vec::with_capacity(schema.num_columns());

        for i in 0..schema.num_keys() {
            let key_ref = self.key_with_schema(i, schema)?;
            data.push(key_ref.to_owned().unwrap_or(DataType::Null));
        }

        for i in 0..schema.num_values() {
            let val_ref = self.value_with_schema(i, schema)?;
            data.push(val_ref.to_owned().unwrap_or(DataType::Null));
        }

        Ok(Row(data.into_boxed_slice()))
    }

    pub fn format_with(&self, schema: &'a Schema, snapshot: &'a Snapshot) -> TupleFormatter<'a> {
        let new_layout = TupleReader::from_schema(schema)
            .parse_for_snapshot(self.data, snapshot)
            .expect("Failed to parse")
            .expect("Tuple is not visible");
        let new_tuple = TupleRef::new(self.data, new_layout);
        TupleFormatter::new(new_tuple, schema)
    }
}

impl Display for DeltaHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "DeltaHeader {{ xmin: {}, version: {} }}",
            self.xmin, self.version
        )
    }
}

/// Layout: [Header][Null Bitmap][Keys][Values][Delta0][Delta1]...[DeltaN]
/// Delta: [DeltaHeader][num_changes: u8][bitmap][changes...]
/// Change: [field_idx: u8][value (aligned)]
#[derive(Clone)]
pub(crate) struct Tuple {
    data: Payload,
}

impl Tuple {
    /// Consumes the tuple and returns the aligned payload behind
    pub(crate) fn into_data(self) -> Payload {
        self.data
    }

    #[inline]
    pub(crate) fn payload_ref(&self) -> PayloadRef<'_> {
        self.data.as_payload_ref()
    }

    #[inline]
    pub(crate) fn effective_data(&self) -> &[u8] {
        self.data.effective_data()
    }

    #[inline]
    pub(crate) fn effective_data_mut(&mut self) -> &mut [u8] {
        self.data.effective_data_mut()
    }

    #[inline]
    pub(crate) fn full_data(&self) -> &[u8] {
        self.data.full_data()
    }

    #[inline]
    pub(crate) fn keys_offset(num_values: usize) -> usize {
        TupleHeader::SIZE + null_bitmap_size(num_values)
    }

    pub fn as_tuple_ref_with_schema(&self, schema: &Schema) -> TupleRef<'_> {
        let reader = TupleReader::from_schema(schema);
        let layout = reader
            .parse_last_version(self.effective_data())
            .expect("Parsing failed");
        TupleRef::new(self.data.effective_data(), layout)
    }

    pub fn as_tuple_ref(&self, schema: &Schema, snapshot: &Snapshot) -> Option<TupleRef<'_>> {
        let reader = TupleReader::from_schema(schema);
        let layout = reader
            .parse_for_snapshot(self.effective_data(), snapshot)
            .expect("Parsing failed")?;
        Some(TupleRef::new(self.data.effective_data(), layout))
    }

    #[inline]
    pub(crate) fn full_data_mut(&mut self) -> &mut [u8] {
        self.data.full_data_mut()
    }

    pub(crate) fn version(&self) -> u8 {
        let (header, _) = TupleHeader::read_from(self.data.effective_data(), 0);
        header.version()
    }

    // Utility to vacuum the tuple after each operation
    fn vaccum_for_snapshot(&mut self, schema: &Schema, snapshot: &Snapshot) -> TupleResult<usize> {
        self.vacuum_with_schema(snapshot.xmin(), schema)
    }

    pub(crate) fn xmin(&self) -> TransactionId {
        let (header, _) = TupleHeader::read_from(self.data.effective_data(), 0);
        header.xmin()
    }

    pub(crate) fn xmax(&self) -> Option<TransactionId> {
        let (header, _) = TupleHeader::read_from(self.data.effective_data(), 0);
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
    pub(crate) fn add_version_with_schema(
        &mut self,
        modified: &HashMap<usize, DataType>,
        new_xmin: TransactionId,
        schema: &Schema,
    ) -> TupleResult<()> {
        // Validate modifications
        for (i, value) in modified {
            let col = schema.value(*i).ok_or(TupleError::ValueError(*i))?;
            if !value.is_null() && col.datatype() != value.kind() {
                return Err(TupleError::DataTypeMismatch((*i, col.datatype())));
            }
        }

        let reader = TupleReader::from_schema(schema);
        let layout = reader.parse_last_version(self.data.effective_data())?;
        let tuple_reference = TupleRef::new(self.data.effective_data(), layout.clone());

        let old_version = tuple_reference.current_version();
        let old_xmin = tuple_reference.version_xmin();
        let num_values = schema.num_values();
        let num_keys = schema.num_keys();
        let bitmap_size = null_bitmap_size(num_values);

        // Collect old values for delta
        let mut old_values: Vec<(u8, DataType)> = Vec::with_capacity(modified.len());
        for (i, _) in modified {
            let val_ref = tuple_reference.value_with_schema(*i, schema)?;
            old_values.push((*i as u8, val_ref.to_owned().unwrap_or(DataType::Null)));
        }

        // Read all keys (immutable, just copy references for size calculation)
        let mut keys: Vec<DataType> = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let key_ref = tuple_reference.key_with_schema(i, schema)?;
            keys.push(key_ref.to_owned().unwrap_or(DataType::Null));
        }

        // Read all values, applying modifications for new values
        let mut new_values: Vec<DataType> = Vec::with_capacity(num_values);
        for i in 0..num_values {
            if let Some(new_val) = modified.get(&i) {
                new_values.push(new_val.clone());
            } else {
                let val_ref = tuple_reference.value_with_schema(i, schema)?;
                new_values.push(val_ref.to_owned().unwrap_or(DataType::Null));
            }
        }

        // Calculate total size needed
        // 1. Header + bitmap + keys + new values
        let mut data_size = TupleHeader::SIZE + bitmap_size;
        for key in &keys {
            data_size = aligned_offset(data_size, key.align());
            data_size += key.runtime_size();
        }
        for val in &new_values {
            if !val.is_null() {
                data_size = aligned_offset(data_size, val.align());
                data_size += val.runtime_size();
            }
        }

        // 2. Delta: [DeltaHeader][num_changes][bitmap][changes...]
        let delta_header_start = DeltaHeader::aligned_offset(data_size);
        let header_end = delta_header_start + DeltaHeader::SIZE;
        let num_changes_offset = header_end;
        let delta_bitmap_start = num_changes_offset + 1;
        let changes_start = delta_bitmap_start + bitmap_size;

        let mut delta_cursor = changes_start;
        for (_, old_val) in &old_values {
            delta_cursor += 1; // field index
            delta_cursor = aligned_offset(delta_cursor, old_val.align());
            delta_cursor += old_val.runtime_size();
        }
        let total_size = delta_cursor;

        // Allocate new buffer
        let mut new_data = Payload::alloc_aligned(total_size)?;
        let buffer = new_data.effective_data_mut();

        // Write new header
        let new_header = TupleHeader::new(old_version + 1, new_xmin, None);
        let mut cursor = new_header.write_to(buffer, 0);

        // Write null bitmap for new values
        let new_bitmap_start = cursor;
        buffer[cursor..cursor + bitmap_size].fill(0);
        for (i, val) in new_values.iter().enumerate() {
            if val.is_null() {
                TupleBuilder::set_null_bit(
                    &mut buffer[new_bitmap_start..new_bitmap_start + bitmap_size],
                    i,
                    true,
                );
            }
        }
        cursor += bitmap_size;

        // Write keys
        for key in &keys {
            cursor = key.write_to(buffer, cursor)?;
        }

        // Write new values
        for val in &new_values {
            if !val.is_null() {
                cursor = val.write_to(buffer, cursor)?;
            }
        }

        // Write delta header
        let delta_header = DeltaHeader::new(old_version, old_xmin);
        delta_header.write_to(buffer, delta_header_start);

        // Write number of changes
        buffer[num_changes_offset] = old_values.len() as u8;

        // Write delta bitmap (old null states)
        buffer[delta_bitmap_start..delta_bitmap_start + bitmap_size].fill(0);
        for (idx, old_val) in &old_values {
            if old_val.is_null() {
                TupleBuilder::set_null_bit(
                    &mut buffer[delta_bitmap_start..delta_bitmap_start + bitmap_size],
                    *idx as usize,
                    true,
                );
            }
        }

        // Write delta changes (old values)
        let mut delta_cursor = changes_start;
        for (idx, old_val) in &old_values {
            buffer[delta_cursor] = *idx;
            delta_cursor += 1;
            delta_cursor = old_val.write_to(buffer, delta_cursor)?;
        }

        // Replace old data with new
        self.data = new_data;

        Ok(())
    }

    pub(crate) fn delete(&mut self, xid: TransactionId) -> TupleResult<()> {
        if self.is_deleted() {
            return Ok(());
        }

        let buffer = self.data.effective_data_mut();
        let (mut header, _) = TupleHeader::read_from(buffer, 0);
        header.xmax = xid as i64;
        header.write_to(buffer, 0);

        Ok(())
    }

    /// Vaccums the tuple by deleting all versions that are not visible by the oldest [TransactionId].
    ///
    /// Truncates the delta directory removing older versions.
    pub(crate) fn vacuum_with_schema(
        &mut self,
        oldest_active_xid: TransactionId,
        schema: &Schema,
    ) -> TupleResult<usize> {
        let reader = TupleReader::from_schema(schema);
        let layout = reader.parse_last_version(self.data.effective_data())?;

        if layout.delta_start() >= self.data.len() {
            return Ok(0);
        }

        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();
        let mut last_needed_end = layout.delta_start();

        while cursor < self.data.len() {
            let (delta_header, header_end) =
                DeltaHeader::read_from(self.data.effective_data(), cursor);

            if delta_header.xmin() >= oldest_active_xid {
                // Read num_changes and skip this delta
                let num_changes = self.data.effective_data()[header_end] as usize;
                let mut skip_cursor = header_end + 1 + bitmap_size;

                for _ in 0..num_changes {
                    let field_idx = self.data.effective_data()[skip_cursor] as usize;
                    skip_cursor += 1;

                    let dtype = schema
                        .value(field_idx)
                        .ok_or(TupleError::ValueError(field_idx))?
                        .datatype();

                    let (_, new_cursor) =
                        dtype.deserialize(self.data.effective_data(), skip_cursor)?;
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

    pub(crate) fn has_history_with_schema(&self, schema: &Schema) -> bool {
        let reader = TupleReader::from_schema(schema);
        let layout = reader
            .parse_last_version(self.data.effective_data())
            .unwrap();
        layout.delta_start() < self.data.len()
    }

    pub(crate) fn total_size(&self) -> usize {
        self.full_data().len()
    }

    /// Parses the delta directory and counts the number of active versions in the tuple at this point in time.
    pub(crate) fn num_versions_with_schema(&self, schema: &Schema) -> TupleResult<usize> {
        let reader = TupleReader::from_schema(schema);
        let layout = reader
            .parse_last_version(self.data.effective_data())
            .unwrap();

        if layout.delta_start() >= self.data.len() {
            return Ok(1);
        }

        let num_values = schema.num_values();
        let bitmap_size = null_bitmap_size(num_values);
        let mut cursor = layout.delta_start();
        let mut count = 1;

        while cursor < self.data.len() {
            let (_, header_end) = DeltaHeader::read_from(self.data.effective_data(), cursor);
            let num_changes = self.data.effective_data()[header_end] as usize;
            cursor = header_end + 1 + bitmap_size;

            for _ in 0..num_changes {
                let field_idx = self.data.effective_data()[cursor] as usize;
                cursor += 1;

                let dtype = schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();

                let (_, new_cursor) = dtype.deserialize(self.data.effective_data(), cursor)?;
                cursor = new_cursor;
            }

            count += 1;
        }

        Ok(count)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Read the tuple from an unaligned buffer (allocates memory)
    pub(crate) fn from_slice_unchecked(buffer: &[u8]) -> TupleResult<Self> {
        let mut data = Payload::alloc_aligned(buffer.len())?;
        data.effective_data_mut().copy_from_slice(buffer);
        Ok(Self { data })
    }

    /// Writes the contents of the tuple to a buffer.
    pub(crate) fn write_to(&self, buffer: &mut [u8]) -> SerializationResult<usize> {
        let len = self.data.full_data().len();
        buffer[..len].copy_from_slice(self.data.full_data());
        Ok(len)
    }

    /// Returns the total serialized size of the tuple.
    pub(crate) fn serialized_size(&self) -> usize {
        self.data.full_data().len()
    }
}

impl From<&Tuple> for Box<[u8]> {
    fn from(value: &Tuple) -> Self {
        Box::from(value.full_data())
    }
}

impl<'a, 'b> Display for RefTupleAccessor<'a, 'b> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let row = self.to_row().map_err(|_| std::fmt::Error)?;

        write!(f, "(")?;
        for (i, value) in row.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{value}")?;
        }
        write!(f, ")")
    }
}

impl<'a> Display for OwnedTupleAccessor<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let reader = self.reader();
        let layout = reader
            .parse_last_version(self.tuple.effective_data())
            .map_err(|_| std::fmt::Error)?;

        let tuple_ref = TupleRef::new(self.tuple.effective_data(), layout);
        let accessor = RefTupleAccessor::new(tuple_ref, self.schema);

        write!(f, "{accessor}")
    }
}

pub struct DisplayWith<'a, T> {
    tuple: &'a T,
    schema: &'a Schema,
    snapshot: &'a Snapshot,
}

impl<'a> Display for DisplayWith<'a, TupleRef<'a>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let s = self
            .tuple
            .format_with(self.schema, self.snapshot)
            .to_string();
        write!(f, "{s}")
    }
}

impl<'a> Display for DisplayWith<'a, Tuple> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let s = self
            .tuple
            .as_tuple_ref(&self.schema, &self.snapshot)
            .expect("Tuple is not visible")
            .format_with(self.schema, self.snapshot)
            .to_string();
        write!(f, "{s}")
    }
}

impl<'a> TupleRef<'a> {
    pub fn display_with(
        &'a self,
        schema: &'a Schema,
        snapshot: &'a Snapshot,
    ) -> DisplayWith<'a, TupleRef<'a>> {
        DisplayWith {
            tuple: self,
            schema,
            snapshot,
        }
    }
}

pub struct TupleFormatter<'a> {
    tuple: TupleRef<'a>,
    schema: &'a Schema,
    only_keys: bool,
    pretty: bool,
}

impl<'a> TupleFormatter<'a> {
    fn new(tuple: TupleRef<'a>, schema: &'a Schema) -> Self {
        Self {
            tuple,
            schema,
            only_keys: false,

            pretty: false,
        }
    }

    pub fn keys_only(mut self) -> Self {
        self.only_keys = true;
        self
    }

    pub fn pretty(mut self) -> Self {
        self.pretty = true;
        self
    }

    pub fn to_string(self) -> String {
        let accessor = RefTupleAccessor::new(self.tuple, self.schema);

        let row = match accessor.to_row() {
            Ok(r) => r,
            Err(_) => return "<invalid tuple>".to_string(),
        };

        let columns = self.schema.columns();

        let mut values = Vec::new();

        for (idx, value) in row.iter().enumerate() {
            if self.only_keys && idx >= self.schema.num_keys() {
                break;
            }

            let rendered = value.to_string();
            if self.pretty {
                values.push(format!("{} = {}", columns[idx].name, rendered));
            } else {
                values.push(rendered);
            }
        }

        if self.pretty {
            format!("{{ {} }}", values.join(", "))
        } else {
            format!("({})", values.join(", "))
        }
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

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        assert_eq!(tuple.version(), 0);
        assert_eq!(tuple.xmin(), 1);
        assert_eq!(tuple.xmax(), None);
        assert!(!tuple.is_deleted());
    }

    #[test]
    fn test_tuple_key_access() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_last_version(tuple.data.effective_data())
            .unwrap();
        let reference = TupleRef::new(tuple.data.effective_data(), layout);

        let accessor = RefTupleAccessor::new(reference, &schema);

        match accessor.key(0).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 1),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_value_access() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_last_version(tuple.data.effective_data())
            .unwrap();
        let reference = TupleRef::new(tuple.data.effective_data(), layout);

        let accessor = RefTupleAccessor::new(reference, &schema);

        match accessor.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_with_nulls() {
        let schema = test_schema();
        let row = test_row_with_nulls();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_last_version(tuple.data.effective_data())
            .unwrap();
        let reference = TupleRef::new(tuple.data.effective_data(), layout);

        let accessor = RefTupleAccessor::new(reference, &schema);

        assert!(matches!(accessor.value(0).unwrap(), DataTypeRef::Null));
        match accessor.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
        assert!(matches!(accessor.value(2).unwrap(), DataTypeRef::Null));
    }

    #[test]
    fn test_tuple_delete() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let mut tuple = builder.build(row, 1).unwrap();

        assert!(!tuple.is_deleted());
        tuple.delete(10).unwrap();
        assert!(tuple.is_deleted());
        assert_eq!(tuple.xmax(), Some(10));
    }

    #[test]
    fn test_tuple_visibility() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        assert!(tuple.is_visible(4));
        assert!(tuple.is_visible(5));
        assert!(tuple.is_visible(100));
    }

    #[test]
    fn test_tuple_deleted_visibility() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let mut tuple = builder.build(row, 1).unwrap();
        tuple.delete(15).unwrap();

        assert!(tuple.is_visible(4));
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
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        let written = tuple.write_to(&mut buffer).unwrap();
        assert_eq!(written, size);

        let loaded = Tuple::from_slice_unchecked(&buffer).unwrap();
        assert_eq!(loaded.version(), tuple.version());
        assert_eq!(loaded.xmin(), tuple.xmin());
    }

    #[test]
    fn test_tuple_add_version() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let mut accessor = OwnedTupleAccessor::new(tuple, &schema);

        assert_eq!(accessor.version(), 0);
        assert!(!accessor.has_history());
        let mut modifications = HashMap::new();
        modifications.insert(1, DataType::BigInt(Int64(31)));
        accessor.add_version(&modifications, 10).unwrap();

        assert_eq!(accessor.version(), 1);
        assert!(accessor.has_history());
        assert_eq!(accessor.num_versions().unwrap(), 2);
        assert_eq!(accessor.xmin(), 10);

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_last_version(accessor.tuple.data.effective_data())
            .unwrap();
        let reference = TupleRef::new(accessor.tuple.data.effective_data(), layout);
        let accessor = RefTupleAccessor::new(reference, &schema);

        match accessor.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 31),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_version_history_read() {
        let schema = test_schema();
        let row = test_row();
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();
        let mut accessor = OwnedTupleAccessor::new(tuple, &schema);
        let mut modifications = HashMap::new();
        modifications.insert(1, DataType::BigInt(Int64(31)));
        accessor.add_version(&modifications, 10).unwrap();

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_last_version(accessor.tuple.data.effective_data())
            .unwrap();
        let ref_v1 = TupleRef::new(accessor.tuple.data.effective_data(), layout);
        let ref_accessor = RefTupleAccessor::new(ref_v1, &schema);
        match ref_accessor.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 31),
            _ => panic!("Expected BigInt"),
        }

        let reader = TupleReader::from_schema(&schema);
        let layout = reader
            .parse_version(accessor.tuple.data.effective_data(), 0)
            .unwrap();
        let ref_v0 = TupleRef::new(accessor.tuple.data.effective_data(), layout);
        let ref_accessor = RefTupleAccessor::new(ref_v0, &schema);
        match ref_accessor.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_keys_offset_calculation() {
        assert_eq!(Tuple::keys_offset(0), 24);
        assert_eq!(Tuple::keys_offset(1), 25);
        assert_eq!(Tuple::keys_offset(7), 25);
        assert_eq!(Tuple::keys_offset(8), 25);
        assert_eq!(Tuple::keys_offset(9), 26);
        assert_eq!(Tuple::keys_offset(16), 26);
        assert_eq!(Tuple::keys_offset(17), 27);
    }

    #[test]
    fn test_tuple_serialization_biguint_key() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let row = Row::new(Box::new([
            DataType::BigUInt(crate::UInt64(42)),
            DataType::Blob(Blob::from("test")),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        assert_eq!(tuple.version(), 0);
        assert_eq!(tuple.xmin(), 1);
        assert_eq!(tuple.xmax(), None);

        let reader = TupleReader::from_schema(&schema);
        let layout = reader.parse_last_version(tuple.effective_data()).unwrap();
        let tuple_ref = TupleRef::new(tuple.effective_data(), layout);

        let key = tuple_ref.key_with_schema(0, &schema).unwrap();
        match key {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 42),
            _ => panic!("Expected BigUInt, got {:?}", key),
        }
    }

    #[test]
    fn test_tuple_key_offset_in_serialized_data() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::BigUInt, "value"),
        ]);

        let row = Row::new(Box::new([
            DataType::BigUInt(crate::UInt64(0x123456789ABCDEF0)),
            DataType::BigUInt(crate::UInt64(999)),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let data = tuple.effective_data();
        let offset = Tuple::keys_offset(schema.num_values());

        let (key_value, _) = DataTypeKind::BigUInt
            .deserialize(data, offset)
            .expect("Deserialization failed");

        assert_eq!(
            key_value.to_owned().unwrap(),
            DataType::BigUInt(crate::UInt64(0x123456789ABCDEF0)),
            "Key value at offset {} should match",
            offset
        );
    }

    #[test]
    fn test_tuple_serialization_composite_key() {
        let schema = Schema::new_index(
            vec![
                Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
                Column::new_with_defaults(DataTypeKind::Blob, "name"),
                Column::new_with_defaults(DataTypeKind::BigUInt, "extra"),
            ],
            2,
        );

        let row = Row::new(Box::new([
            DataType::BigUInt(crate::UInt64(100)),
            DataType::Blob(Blob::from("hello")),
            DataType::BigUInt(crate::UInt64(999)),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 1).unwrap();

        let reader = TupleReader::from_schema(&schema);
        let layout = reader.parse_last_version(tuple.effective_data()).unwrap();
        let tuple_ref = TupleRef::new(tuple.effective_data(), layout);

        let key0 = tuple_ref.key_with_schema(0, &schema).unwrap();
        match key0 {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 100),
            _ => panic!("Expected BigUInt for key 0"),
        }

        let key1 = tuple_ref.key_with_schema(1, &schema).unwrap();
        match key1 {
            DataTypeRef::Blob(v) => assert_eq!(v.data().unwrap(), b"hello"),
            _ => panic!("Expected Blob for key 1"),
        }

        let val = tuple_ref.value_with_schema(0, &schema).unwrap();
        match val {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 999),
            _ => panic!("Expected BigUInt for value"),
        }
    }
}
