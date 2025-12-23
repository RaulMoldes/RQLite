use crate::{
    SerializationError, SerializationResult, bytemuck_struct,
    multithreading::coordinator::Snapshot,
    schema::{Schema, base::SchemaError},
    tree::cell_ops::KeyBytes,
    types::{DataType, DataTypeKind, DataTypeRef, TransactionId, VarInt},
    varint::MAX_VARINT_LEN,
};

use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    mem,
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

impl std::error::Error for TupleError {}
pub type TupleResult<T> = Result<T, TupleError>;

#[inline]
fn null_bitmap_size(num_value_columns: usize) -> usize {
    num_value_columns.div_ceil(8)
}

/// high level representaton of a row (a list of values)
pub(crate) struct Row(Box<[DataType]>);

impl Row {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Splits the row in two halves at the specified index.
    pub(crate) fn into_parts(self, index: usize) -> (Row, Row) {
        let mut iter = self.0.into_iter();
        let left: Box<[DataType]> = iter.by_ref().take(index).collect();
        let right: Box<[DataType]> = iter.collect();
        (Self(left), Self(right))
    }

    pub(crate) fn validate(&self, schema: &Schema) -> TupleResult<()> {
        if schema.num_columns() != self.len() {
            return Err(TupleError::InvalidValueCount(self.len()));
        };
        // Validate the keys.
        for (i, key) in schema.iter_keys().enumerate() {
            if key.datatype() != self.key(i, schema).ok_or(TupleError::KeyError(i))?.kind() {
                return Err(TupleError::DataTypeMismatch((i, key.datatype())));
            }
        }

        // Validate the values.
        for (i, key) in schema.iter_values().enumerate() {
            let value = self.value(i, schema).ok_or(TupleError::ValueError(i))?;
            if key.datatype() != value.kind() && !value.is_null() {
                return Err(TupleError::DataTypeMismatch((i, key.datatype())));
            }
        }

        Ok(())
    }

    /// Computes the total size of the row
    pub(crate) fn total_size(&self) -> usize {
        self.0.iter().map(|d| d.runtime_size()).sum()
    }

    /// Gets the key at a specified index.
    pub(crate) fn key(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        (index < schema.num_keys())
            .then(|| index)
            .and_then(|i| self.0.get(i))
    }

    /// Gets the value at a specified index.
    pub(crate) fn value(&self, index: usize, schema: &Schema) -> Option<&DataType> {
        self.0.get(schema.num_keys() + index)
    }

    /// Gets the key at a specified index.
    pub(crate) fn key_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        (index < schema.num_keys())
            .then(|| index)
            .and_then(|i| self.0.get_mut(i))
    }

    /// Gets the value at a specified index.
    pub(crate) fn value_mut(&mut self, index: usize, schema: &Schema) -> Option<&mut DataType> {
        self.0.get_mut(schema.num_keys() + index)
    }

    /// Iteration utilities for row.
    pub(crate) fn iter(&self) -> Iter<'_, DataType> {
        self.0.iter()
    }

    /// Mutable iteration utilities
    pub(crate) fn iter_mut(&mut self) -> IterMut<'_, DataType> {
        self.0.iter_mut()
    }

    /// Gets the inner Boxed slice
    pub(crate) fn into_inner(self) -> Box<[DataType]> {
        self.0
    }

    /// Serializes the [Row] to a boxed slice.
    pub(crate) fn to_bytes(&self) -> TupleResult<Box<[u8]>> {
        let size: usize = self.total_aligned_size();

        // Allocate a row for the keys data.
        // The keys are stored contiguously always at the beginning of the tuple.
        let mut bytes = vec![0u8; size];
        let mut cursor = 0;
        for data in self.iter() {
            cursor = data.write_to(&mut bytes, cursor)?;
        }

        Ok(bytes.into_boxed_slice())
    }

    pub(crate) fn row_alignment(&self) -> usize {
        self.0.iter().map(|datatype| datatype.align()).max().expect("Unable to compute row alignment")
    }

    pub(crate) fn total_aligned_size(&self) -> usize {
        let total_size = self.total_size();
        let alignment = self.row_alignment();
        total_size.next_multiple_of(alignment)
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

/// Tuple header data contains metadata about the tuple itself.
/// As it happens with normal headers, it is fixed size (8 + 8 + 1 byte)
/// We only use one byte to keep track of versions because due to incremental vaccum being applied it is very weird that a tuple accumulates a lot of versions.

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))] // Aligns to 8 bytes for u64
pub(crate) struct TupleHeader {
    pub xmin: u64,
    pub xmax: i64, // Set to [-1] when it is None
    pub version: u8,
}
bytemuck_struct!(TupleHeader);

impl TupleHeader {
    pub fn new(version: u8, xmin: u64, xmax: Option<u64>) -> Self {
        Self {
            xmin,
            xmax: xmax.map(|item| item as i64).unwrap_or(-1),
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
#[repr(C, align(8))] // Aligns to 8 bytes for u64
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

/// Representation of a delta.
/// The xmin is the transaction id that created this version of a tuple, while the changes vector represents the array of chanes, identified by column index in the table schema.
#[derive(Debug, Clone)]
struct Delta {
    header: DeltaHeader,
    changes: Vec<(u8, DataType)>,
}

impl Delta {
    fn new(version: u8, xmin: TransactionId) -> Self {
        Self {
            header: DeltaHeader::new(version, xmin),
            changes: Vec::new(),
        }
    }

    fn with_capacity(version: u8, xmin: TransactionId, capacity: usize) -> Self {
        Self {
            header: DeltaHeader::new(version, xmin),
            changes: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn xmin(&self) -> TransactionId {
        self.header.xmin()
    }

    #[inline]
    fn version(&self) -> u8 {
        self.header.version()
    }

    #[inline]
    fn iter_changes(&self) -> impl Iterator<Item = &(u8, DataType)> {
        self.changes.iter()
    }

    #[inline]
    fn push(&mut self, value: (u8, DataType)) {
        self.changes.push(value);
    }

    #[inline]
    fn len(&self) -> usize {
        self.changes.len()
    }
}

/// Structure including tuple layout information.
/// The layout of a tuple in memory is quite special.
///
/// Since comparators work on byte slices from the left to the right, it is much easier to work with cell data if the key goes always first (even before the header). Keys are inmutable, and therefore we can get away with a single key storage for all versions of the tuple.
///
/// At the same time, due to MVCC concerns, we need to store the versions (or rather changes/deltas) of the tuple somewhere.
///
/// There are several approaches here.
///
/// Postgres does it by storing the full new version somewhere else in the database. But a more intelligent approach is to only store deltas (changes to specific fields of the tuple).
///
/// Once we know that we are going to store deltas (changes only), the next question is where to store them.
///
/// One option would be to put them at the end of the tuple, appending bytes as we add modifications to it.
///
/// However this is not ideal, since the last version is what is going to be read the most, and that would require to traverse till the end of the tuple when reading in order to reconstruct the most recent view.
///
/// Instead of that, I do the opposite. The last (most recent version) of the tuple is stored at the beginning in an contiguous chunk of memory. After that it comes the header with the null bitmap indicating which values in the tuple are set null. After the bitmap, the last version's values are stored contiguously followed by each version delta in reverse order.
///
/// This layout also makes it handy to vaccum the tuple whenever we want, since vaccuming is just done by truncating the tuple bytes to the oldest version that can be seen by any transaction.
#[derive(Debug, Clone)]
struct TupleLayout {
    /// Transaction which created the tuple:
    version_xmin: TransactionId,
    /// Transaction which deleted the tuple (if any)
    version_xmax: Option<TransactionId>,
    // Offsets to the data of each tuple's column.
    data_offsets: Vec<usize>,
    // Offset where the header of the tuple starts
    header_start: usize,
    // Offset where the null bitmap starts ( after the header)
    null_bitmap_start: usize,
    // Offset where the last version ends and start of the delta storage.
    last_version_end: usize,
    /// Layout version information
    version: u8,
}

/// Parses byte slice containing a tuple into a tuple layout in order to reconstruct tuples from it.
///
/// The [`parse`] function parses the last version of the tuple, while the parse version is used to parse a specific version by reapplying the deltas incrementally.
impl TupleLayout {
    /// Parses the last version of the tuple structure into a tuple layout.
    fn parse_last_version(data: &[u8], schema: &Schema) -> TupleResult<Self> {
        let num_keys = schema.num_keys();
        let num_values = schema.num_values();
        let null_bitmap_size = null_bitmap_size(num_values);

        let mut offsets = Vec::with_capacity(schema.num_columns());
        let mut cursor = 0usize;

        // First we parse the keys of the schema without actually copying the data.
        for key in schema.iter_keys() {
            offsets.push(cursor);
            let (_, read_bytes) = key.datatype().reinterpret_cast(&data[cursor..])?;
            cursor += read_bytes;
        }

        let header_start = cursor;

        // Read the header in order to get the offset where its data ends.
        let (header, next_offset) = TupleHeader::read_from(data, cursor);
        cursor = next_offset;

        let layout_version = header.version();
        let version_xmax = header.xmax();
        let version_xmin = header.xmin();

        // Store the null bitmap offset
        let null_bitmap_start = cursor;

        // Parse the null bitmap and use it to get the offset to each of the values.
        let null_bitmap = &data[cursor..cursor + null_bitmap_size];
        cursor += null_bitmap_size;

        // Iterates over the schema ad checks for null values.
        // This is not done with keys since keys cannot be null
        for (i, col) in schema.iter_values().enumerate() {
            offsets.push(cursor);
            let is_null = Self::check_null(null_bitmap, i);
            if !is_null {

                let (_, read_bytes) = col.datatype().reinterpret_cast(&data[cursor..])?;
                cursor += read_bytes;
            }
        }

        // Build and return the layout
        Ok(TupleLayout {
            data_offsets: offsets,
            header_start,
            null_bitmap_start,
            last_version_end: cursor,
            version: layout_version,
            version_xmax,
            version_xmin,
        })
    }

    #[inline]
    fn current_version(&self) -> u8 {
        self.version
    }

    #[inline]
    fn delta_dir_start(&self) -> usize {
        self.last_version_end as usize
    }

    #[inline]
    fn null_bitmap_start(&self) -> usize {
        self.null_bitmap_start as usize
    }

    #[inline]
    fn version_xmax(&self) -> Option<TransactionId> {
        self.version_xmax
    }

    #[inline]
    fn version_xmin(&self) -> TransactionId {
        self.version_xmin
    }

    /// Parses a specific version of the tuple
    fn parse_version(data: &[u8], schema: &Schema, target_version: u8) -> TupleResult<TupleLayout> {
        let mut layout = Self::parse_last_version(data, schema)?;

        // Propagate an error if the version is invalid.
        if target_version > layout.current_version() {
            return Err(TupleError::InvalidVersion(target_version as usize));
        }

        // Return the current layout if the target version matches the current version
        if target_version == layout.current_version() {
            return Ok(layout);
        }

        let num_keys = schema.num_keys();
        let num_values = schema.num_values();
        let null_bitmap_size = null_bitmap_size(num_values);

        // Position the cursor where the last version ended
        let mut cursor = layout.delta_dir_start();

        // For each delta, we parse the version, the xmin of the delta (transaction which created it) and the actual modified data.
        // The layout of the delta data is the following
        //
        // The [VarInt] prefix holds the actual data length in bytes.
        // The version xmax is simply obtained as the xmin of the next version.
        //
        // The variable length data holds both the null bitmap for the version, whose size is of known size given the schema, and the values data one after the other.
        //
        // [Version (1 byte)][Xmin (8 bytes)][Varint prefix][actual data....]
        while cursor < data.len() {
            // Parse the delta header.
            let (delta_header, read_end) = DeltaHeader::read_from(&data, cursor);

            layout.version_xmax = Some(layout.version_xmin());
            layout.version = delta_header.version();
            layout.version_xmin = delta_header.xmin();
            cursor = read_end;

            // Parse the Varint prefix.
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&data[cursor..])?;
            let delta_size: usize = size_varint.into();
            cursor += varint_bytes;

            // Now parse the actual data.
            let delta_end = cursor + delta_size;

            layout.null_bitmap_start = cursor;
            cursor += null_bitmap_size;

            // Each delta holds the index of the value in the schema and the actual data.
            // Note that this is the index of the [VALUE] not the index of the [COLUMN]
            // See [Schema] for details on that.
            while cursor < delta_end {
                //
                let field_idx = data[cursor] as usize;
                cursor += 1;
                let dtype = schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();
                // Set the data offset of this value (skip num keys)
                layout.data_offsets[field_idx + num_keys] = cursor;
                let (_, read_bytes) = dtype.reinterpret_cast(&data[cursor..])?;
                cursor += read_bytes;
            }

            if layout.current_version() == target_version {
                break;
            }
        }

        Ok(layout)
    }

    /// Returns true if a specific tuple layout is valid for a specific snapshot
    fn is_valid_for_snapshot(&self, snapshot: &Snapshot) -> bool {
        // Check if the version creation happened before the snapshot was taken.
        let version_created_before_snapshot =
            snapshot.is_committed_before_snapshot(self.version_xmin());

        // Check whether the version was deleted before the snapshot was taken.
        if let Some(xmax) = self.version_xmax() {
            return version_created_before_snapshot && !snapshot.is_committed_before_snapshot(xmax);
        }
        version_created_before_snapshot
    }

    /// Parse a specific tuple version given the provided snapshot
    fn parse_for_snapshot(
        data: &[u8],
        schema: &Schema,
        snapshot: &Snapshot,
    ) -> TupleResult<Option<Self>> {
        let mut layout = Self::parse_last_version(data, schema)?;

        // Check if the tuple is already deleted.
        if let Some(xmax) = layout.version_xmax()
            && !snapshot.is_committed_before_snapshot(xmax)
        {
            return Ok(None); // The tuple does no longer exist
        }

        if layout.is_valid_for_snapshot(snapshot) {
            return Ok(Some(layout));
        }

        let mut cursor = layout.delta_dir_start();
        let num_keys = schema.num_keys();
        let num_values = schema.num_values();
        let null_bitmap_size = null_bitmap_size(num_values);

        while cursor < data.len() {
            // Parse the delta header.
            let (delta_header, read_end) = DeltaHeader::read_from(&data, cursor);

            layout.version_xmax = Some(layout.version_xmin());
            layout.version = delta_header.version();
            layout.version_xmin = delta_header.xmin();
            cursor = read_end;

            // Parse the Varint prefix.
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&data[cursor..])?;
            let delta_size: usize = size_varint.into();
            cursor += varint_bytes;

            // Now parse the actual data.
            let delta_end = cursor + delta_size;

            layout.null_bitmap_start = cursor;
            cursor += null_bitmap_size;

            // Each delta holds the index of the value in the schema and the actual data.
            // Note that this is the index of the [VALUE] not the index of the [COLUMN]
            // See [Schema] for details on that.
            while cursor < delta_end {
                //
                let field_idx = data[cursor] as usize;
                cursor += 1;
                let dtype = schema
                    .value(field_idx)
                    .ok_or(TupleError::ValueError(field_idx))?
                    .datatype();
                // Set the data offset of this value (skip num keys)
                layout.data_offsets[field_idx + num_keys] = cursor;
                let (_, read_bytes) = dtype.reinterpret_cast(&data[cursor..])?;
                cursor += read_bytes;
            }

            if snapshot.is_committed_before_snapshot(layout.version_xmin()) {
                return Ok(Some(layout));
            }
        }

        // If we reach this point it means the tuple was created after our snapshot was taken.
        Ok(None)
    }

    /// Check if a specific value in the tuple is null by masking with the null bitmap.
    #[inline]
    fn check_null(bitmap: &[u8], val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }
}

/// Inmutable view of a specific version of a tuple.
/// Tuples are always parsed with their schema, so there is no need to copy the data as long as we know in advance the size of each element.
/// Due to our type system, the bytes occupied by even variable length datatypes like blob or text is know in advance.
pub(crate) struct TupleRef<'a, 'b> {
    data: &'a [u8],
    schema: &'b Schema,
    layout: TupleLayout,
    header: TupleHeader,
}

impl<'a, 'b> TupleRef<'a, 'b> {
    /// Read the last version of the tuple
    pub(crate) fn read_last_version(buffer: &'a [u8], schema: &'b Schema) -> TupleResult<Self> {
        let layout = TupleLayout::parse_last_version(buffer, schema)?;
        let (header, _) = TupleHeader::read_from(buffer, layout.header_start as usize);
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    /// Read a specific version of the tuple.
    pub(crate) fn read_version(
        buffer: &'a [u8],
        schema: &'b Schema,
        version: u8,
    ) -> TupleResult<Self> {
        let layout = TupleLayout::parse_version(buffer, schema, version)?;
        let (header, _) = TupleHeader::read_from(buffer, layout.header_start as usize);
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    /// Read a specific version of the tuple.
    pub(crate) fn read_for_snapshot(
        buffer: &'a [u8],
        schema: &'b Schema,
        snapshot: &Snapshot,
    ) -> TupleResult<Option<Self>> {
        Ok(
            TupleLayout::parse_for_snapshot(buffer, schema, snapshot)?.map(|layout| {
                let (header, _) = TupleHeader::read_from(buffer, layout.header_start as usize);
                Self {
                    data: buffer,
                    schema,
                    layout,
                    header,
                }
            }),
        )
    }

    /// Get a reference to the schema of the tuple
    #[inline]
    pub(crate) fn schema(&self) -> &'b Schema {
        self.schema
    }

    /// Get a reference to the key as a [KeyBytes]
    #[inline]
    pub(crate) fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.data[..self.layout.header_start])
    }

    /// Field count in the tuple.
    #[inline]
    pub(crate) fn num_fields(&self) -> usize {
        self.layout.data_offsets.len()
    }

    /// Get the current version of the tuple.
    #[inline]
    pub(crate) fn current_version(&self) -> u8 {
        self.layout.current_version()
    }

    /// Get the last version of the tuple (globally)
    #[inline]
    pub(crate) fn global_last_version(&self) -> u8 {
        self.header.version()
    }

    /// Get the global xmin of the tuple (from the parsed header)
    #[inline]
    pub(crate) fn global_xmin(&self) -> TransactionId {
        self.header.xmin
    }

    /// Get the global xmax of the tuple (from the parsed header)
    #[inline]
    pub(crate) fn global_xmax(&self) -> Option<TransactionId> {
        self.header.xmax()
    }

    /// Check if the tuple has been deleted (globally)
    #[inline]
    pub(crate) fn is_tuple_deleted(&self) -> bool {
        self.global_xmax().is_some()
    }

    /// Metadata about this specific version.
    ///
    /// Get the global xmin of the version (from the parsed header)
    #[inline]
    pub(crate) fn version_xmin(&self) -> TransactionId {
        self.layout.version_xmin()
    }

    /// Get the version xmax (from the parsed header)
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

    #[inline]
    fn null_bitmap(&self) -> &[u8] {
        let offset = self.layout.null_bitmap_start() as usize;
        let num_values = self.schema.num_values();
        let len = null_bitmap_size(num_values);
        &self.data[offset..offset + len]
    }

    #[inline]
    fn is_null(&self, val_idx: usize) -> bool {
        TupleLayout::check_null(self.null_bitmap(), val_idx)
    }

    /// Returns a reference to the key at the specified index.
    pub(crate) fn key(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        let offset = self.layout.data_offsets[index] as usize;
        let (value, _) = self
            .schema
            .key(index)
            .ok_or(TupleError::KeyError(index))?
            .datatype()
            .reinterpret_cast(&self.data[offset..])?;
        Ok(value)
    }

    /// Returns a reference to the value at the specified index.
    pub(crate) fn value(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        if self.is_null(index) {
            return Ok(DataTypeRef::Null);
        }

        let offset_idx = index + self.schema.num_keys();
        let offset = self.layout.data_offsets[offset_idx] as usize;

        let (value, _) = self
            .schema
            .value(index)
            .ok_or(TupleError::KeyError(index))?
            .datatype()
            .reinterpret_cast(&self.data[offset..])?;
        Ok(value)
    }
}

/// Owned tuple representation.
#[derive(Clone)]
pub(crate) struct Tuple<'schema> {
    key_bytes: Box<[u8]>, // The key bytes are kept contiguous in memory for convenience.
    values: Box<[DataType]>, // Values struct.
    schema: &'schema Schema,
    header: TupleHeader, // Global xmax of the tuple. If set to zero, it means the tuple is not deleted.
    delta_dir: BTreeMap<u8, Delta>, // Delta directory of the tuple encoded as a btreemap.
}

impl<'schema> Tuple<'schema> {
    /// Build a tuple from a Row and a reference to the schema.
    pub(crate) fn new(
        data: Row,
        schema: &'schema Schema,
        xmin: TransactionId,
    ) -> TupleResult<Self> {
        // Validate the data.
        data.validate(schema)?;
        let (keys, values) = data.into_parts(schema.num_keys());
        let key_bytes = keys.to_bytes()?;
        let values = values.into_inner();
        let header = TupleHeader::new(0, xmin, None);
        Ok(Self {
            key_bytes,
            values,
            schema,
            header,
            delta_dir: BTreeMap::new(),
        })
    }

    #[inline]
    pub(crate) fn key_bytes_len(&self) -> usize {
        self.key_bytes.len()
    }

    /// Gets the key from the contiguous byte slice.
    pub(crate) fn key(&self, index: usize) -> TupleResult<DataTypeRef<'_>> {
        let mut cursor = 0;
        for (i, key_col) in self.schema.iter_keys().enumerate() {
            if i == index {
                let (value, _) = key_col
                    .datatype()
                    .reinterpret_cast(&self.key_bytes[cursor..])?;
                return Ok(value);
            }
            let (_, read_bytes) = key_col
                .datatype()
                .reinterpret_cast(&self.key_bytes[cursor..])?;
            cursor += read_bytes;
        }
        Err(TupleError::KeyError(index))
    }

    #[inline]
    pub(crate) fn num_keys(&self) -> usize {
        self.schema.num_keys()
    }

    #[inline]
    pub(crate) fn num_values(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub(crate) fn num_fields(&self) -> usize {
        self.num_keys() + self.num_values()
    }

    #[inline]
    pub(crate) fn version(&self) -> u8 {
        self.header.version()
    }

    #[inline]
    pub(crate) fn set_version(&mut self, version: u8) {
        self.header.version = version;
    }

    #[inline]
    pub(crate) fn values(&self) -> &[DataType] {
        &self.values
    }

    #[inline]
    pub(crate) fn values_mut(&mut self) -> &mut [DataType] {
        &mut self.values
    }

    #[inline]
    pub(crate) fn set_value(&mut self, index: usize, new: DataType) -> Option<DataType> {
        self.values
            .get_mut(index)
            .map(|slot| mem::replace(slot, new))
    }

    #[inline]
    pub(crate) fn xmin(&self) -> TransactionId {
        self.header.xmin()
    }

    #[inline]
    pub(crate) fn xmax(&self) -> Option<TransactionId> {
        self.header.xmax()
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

    pub(crate) fn add_version(
        &mut self,
        modified: &[(usize, DataType)],
        xmin: TransactionId,
    ) -> TupleResult<()> {
        let old_version = self.version();
        let old_xmin = self.xmin();

        self.header.version += 1;
        self.header.xmin = xmin;

        // Create a new delta in the dir.
        let mut diffs = Delta::with_capacity(old_version, old_xmin, modified.len());

        for (i, value) in modified {
            let col = self.schema.value(*i).ok_or(TupleError::ValueError(*i))?;

            // Check for datatype mismatches
            if !value.is_null() && col.datatype() != value.kind() {
                return Err(TupleError::DataTypeMismatch((*i, col.datatype())));
            }

            if let Some(old) = self.set_value(*i, value.clone()) {
                diffs.push((*i as u8, old));
            }
        }

        self.delta_dir.insert(old_version, diffs);
        Ok(())
    }

    /// Deletes the tuple by marking the xmax flag to the given transaction id.
    ///
    /// If the tuple is already deleted previously behaves like a No-Op.
    pub(crate) fn delete(&mut self, xid: TransactionId) -> TupleResult<()> {
        if self.xmax().is_some() {
            return Ok(());
        }
        self.header.xmax = xid as i64;
        Ok(())
    }

    /// Vacuums the tuple by removing all versions older than the oldest visible version.
    ///
    /// The [`oldest_active_xid`] parameter represents the oldest transaction ID that might
    /// still need to see old versions. All versions created before this can be safely removed.
    ///
    /// Returns the number of versions removed.
    fn vacuum(&mut self, oldest_active_xid: TransactionId) -> usize {
        if self.delta_dir.is_empty() {
            return 0;
        }

        // Find versions that can be removed (versions whose xmin < oldest_active_xid)
        // We need to keep at least the most recent version that is visible to oldest_active_xid
        let versions_to_remove: Vec<u8> = self
            .delta_dir
            .iter()
            .filter(|(_, delta)| delta.xmin() < oldest_active_xid)
            .map(|(version, _)| *version)
            .collect();

        let removed_count = versions_to_remove.len();

        for version in versions_to_remove {
            self.delta_dir.remove(&version);
        }

        removed_count
    }

    /// Vacuums the tuple using a snapshot, removing all versions that are no longer
    /// visible to any transaction in the snapshot's active set.
    ///
    ///
    /// Returns the number of versions removed.
    pub(crate) fn vacuum_for_snapshot(&mut self, snapshot: &Snapshot) -> usize {
        if self.delta_dir.is_empty() {
            return 0;
        }

        // The oldest xmin we need to keep is the snapshot [xmin]
        // (represents the oldest transaction id at the time the transaction snapshot was taken)
        let oldest_needed = snapshot.xmin();

        self.vacuum(oldest_needed)
    }

    /// Returns the oldest version number stored in this tuple.
    /// Returns 0 if there are no deltas (only the current version exists).
    #[inline]
    pub fn oldest_version(&self) -> u8 {
        self.delta_dir.keys().next().copied().unwrap_or(0)
    }

    /// Returns the number of versions stored (including current).
    #[inline]
    pub fn num_versions(&self) -> usize {
        self.delta_dir.len() + 1 // +1 for current version
    }

    /// Returns true if this tuple has any historical versions.
    #[inline]
    pub fn has_history(&self) -> bool {
        !self.delta_dir.is_empty()
    }

    /// Consumes the tuple to create a new [Row] with the last version.
    pub fn into_row(self) -> TupleResult<Row> {
        let mut data = Vec::with_capacity(self.num_fields());
        let mut cursor = 0;
        for key in self.schema.iter_keys() {
            let (datatype, bytes_read) =
                key.datatype().reinterpret_cast(&self.key_bytes[cursor..])?;
            let owned: DataType = datatype.to_owned().unwrap_or(DataType::Null);
            data.push(owned);
            cursor += bytes_read;
        }

        data.extend_from_slice(self.values());
        Ok(Row(data.into_boxed_slice()))
    }
}

impl Delta {
    /// Estimates the total serialized size of a delta.
    fn serialized_size(&self, num_values: usize, prev_cursor: usize) -> usize {
        let aligned_start = DeltaHeader::aligned_offset(prev_cursor);
        let padding = aligned_start - prev_cursor;

        let header_size = DeltaHeader::SIZE;
        let null_bitmap_size = null_bitmap_size(num_values);

        let changes_size: usize = self
            .iter_changes()
            .map(|(_, dt)| 1 + dt.runtime_size())
            .sum();

        let delta_data_size = null_bitmap_size + changes_size;
        let varint_size = VarInt::encoded_size(delta_data_size as i64);

        padding + header_size + varint_size + delta_data_size
    }

    /// Write the delta to a byte slice.
    fn write_to(
        &self,
        buffer: &mut [u8],
        cursor: usize,
        num_values: usize,
        original_bitmap_start: usize,
    ) -> SerializationResult<usize> {
        let mut cursor = self.header.write_to(buffer, cursor);
        let null_bitmap_size = null_bitmap_size(num_values);

        // Calculate and write delta data size as [VarInt]
        let changes_size: usize = self
            .iter_changes()
            .map(|(_, dt)| 1 + dt.runtime_size()) // one extra byte for the column indx.
            .sum();

        let delta_data_size = null_bitmap_size + changes_size;
        let mut varint_buffer = [0u8; MAX_VARINT_LEN];
        let encoded = VarInt::encode(delta_data_size as i64, &mut varint_buffer);
        buffer[cursor..cursor + encoded.len()].copy_from_slice(encoded);
        cursor += encoded.len();

        //  Write the delta's null bitmap.
        let delta_bitmap_start = cursor;
        buffer.copy_within(
            original_bitmap_start..original_bitmap_start + null_bitmap_size,
            cursor,
        );
        cursor += null_bitmap_size;

        // Write changes
        for (idx, old_value) in self.iter_changes() {
            let byte_idx = *idx as usize / 8;
            let bit_idx = *idx as usize % 8;

            // Update delta bitmap based on old value
            if old_value.is_null() {
                // Set the old
                buffer[delta_bitmap_start + byte_idx] |= 1 << bit_idx;
            } else {
                // If current is null but old wasn't, clear the bit
                let current_is_null = TupleLayout::check_null(
                    &buffer[original_bitmap_start..original_bitmap_start + null_bitmap_size],
                    byte_idx,
                );

                if current_is_null {
                    buffer[delta_bitmap_start + byte_idx] &= !(1 << bit_idx);
                }
            }

            // Write field index
            buffer[cursor] = *idx;
            cursor += 1;

            // Write old value
            cursor = old_value.write_to(buffer, cursor)?;
        }

        Ok(cursor)
    }
}

impl<'schema> Tuple<'schema> {
    /// Compute the serialized total size of a tuple.
    fn serialized_size(&self) -> usize {
        let num_values = self.values.len();
        let null_bitmap_size = null_bitmap_size(num_values);

        // Compute the header size taking padding into account
        let header_size = TupleHeader::SIZE;
        let key_size = self.key_bytes.len();
        let header_aligned_start = TupleHeader::aligned_offset(key_size);
        let header_padding = header_aligned_start - key_size;

        let values_size: usize = self
            .values()
            .iter()
            .filter(|dt| !dt.is_null())
            .map(|dt| dt.runtime_size())
            .sum();
        let mut cursor =
            key_size + header_padding + TupleHeader::SIZE + null_bitmap_size + values_size;
        for (_, diffs) in self.delta_dir.iter() {
            cursor += diffs.serialized_size(num_values, cursor);
        }

        cursor
    }

    /// Write the tuple data to a mutable byte buffer.
    fn write_to(&self, buffer: &mut [u8]) -> SerializationResult<usize> {
        let mut cursor = 0usize;
        let num_values = self.num_values();
        let null_bitmap_size = null_bitmap_size(num_values);

        // Copy the keys.
        buffer[..self.key_bytes_len()].copy_from_slice(&self.key_bytes);
        cursor += self.key_bytes.len();
        cursor = self.header.write_to(buffer, cursor);

        // Write the null bitmap after the header
        let bitmap_start = cursor;

        // Fill the bitmap with zeros before writing actual data.
        buffer[cursor..cursor + null_bitmap_size].fill(0);
        cursor += null_bitmap_size;

        for (i, val) in self.values.iter().enumerate() {
            if val.is_null() {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                unsafe {
                    let ptr = buffer.as_mut_ptr();
                    *ptr.add(bitmap_start + byte_idx) |= 1 << bit_idx
                };
            } else {
                cursor = val.write_to(buffer, cursor)?;
            }
        }

        for (_, delta) in self.delta_dir.iter().rev() {
            cursor = delta.write_to(buffer, cursor, num_values, bitmap_start)?;
        }

        Ok(cursor)
    }

    /// Reads a tuple from a byte buffer.
    fn read_from(buffer: &[u8], schema: &'schema Schema) -> TupleResult<Tuple<'schema>> {
        let tuple_ref = TupleRef::read_last_version(buffer, schema)?;
        let key_len = tuple_ref.layout.header_start;

        // Get the key bytes from the last version.
        let key_bytes = buffer[..key_len].to_vec().into_boxed_slice();

        // Copy the last version values into the values buffer
        let mut values = Vec::with_capacity(schema.num_values());
        for i in 0..schema.num_values() {
            let value_ref = tuple_ref.value(i)?;
            values.push(value_ref.to_owned().unwrap_or(DataType::Null));
        }

        let header = tuple_ref.header;
        let delta_dir = Self::parse_deltas(buffer, schema, &tuple_ref)?;

        Ok(Tuple {
            key_bytes,
            values: values.into_boxed_slice(),
            schema,
            header,
            delta_dir,
        })
    }

    fn parse_deltas(
        buffer: &[u8],
        schema: &Schema,
        tuple_ref: &TupleRef,
    ) -> TupleResult<BTreeMap<u8, Delta>> {
        let mut delta_dir = BTreeMap::new();
        let null_bitmap_size = null_bitmap_size(schema.num_values());
        let mut cursor = tuple_ref.layout.last_version_end;

        while cursor < buffer.len() {
            // Read delta header
            let (delta_header, read_end) = DeltaHeader::read_from(buffer, cursor);
            cursor = read_end;

            // Read delta data size
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let delta_size: usize = size_varint.into();
            cursor += varint_bytes;

            let delta_end = cursor + delta_size;

            // Skip null bitmap  as we do not need it for owned tuples
            cursor += null_bitmap_size;

            // Read changes
            let mut delta = Delta::new(delta_header.version(), delta_header.xmin());

            while cursor < delta_end {
                let field_index = buffer[cursor];
                cursor += 1;

                let dtype = schema
                    .value(field_index as usize)
                    .ok_or(TupleError::ValueError(field_index as usize))?
                    .datatype();

                let (value_ref, read_bytes) = dtype.reinterpret_cast(&buffer[cursor..])?;
                delta.push((field_index, value_ref.to_owned().unwrap_or(DataType::Null)));
                cursor += read_bytes;
            }

            delta_dir.insert(delta_header.version(), delta);
        }

        Ok(delta_dir)
    }
}

#[cfg(test)]
mod tuple_tests {
    use super::*;
    use crate::schema::{Column, Schema};
    use crate::types::{Blob, DataType, DataTypeKind, core::RuntimeSized};
    use crate::{matrix_tests, param_tests, param2_tests};

    fn test_schema() -> Schema {
        Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::BigInt, "age"),
            Column::new_with_defaults(DataTypeKind::Blob, "email"),
        ])
    }

    fn test_schema_multi_key() -> Schema {
        Schema::new_index(
            vec![
                Column::new_with_defaults(DataTypeKind::BigInt, "id1"),
                Column::new_with_defaults(DataTypeKind::BigInt, "id2"),
                Column::new_with_defaults(DataTypeKind::Blob, "value"),
            ],
            2, // 2 keys
        )
    }

    fn test_row() -> Row {
        Row(Box::new([
            DataType::BigInt(crate::Int64(1)),
            DataType::Blob(Blob::from("Alice")),
            DataType::BigInt(crate::Int64(30)),
            DataType::Blob(Blob::from("alice@example.com")),
        ]))
    }

    fn test_row_with_nulls() -> Row {
        Row(Box::new([
            DataType::BigInt(crate::Int64(1)),
            DataType::Null,
            DataType::BigInt(crate::Int64(30)),
            DataType::Null,
        ]))
    }

    #[test]
    fn test_row_len() {
        let row = test_row();
        assert_eq!(row.len(), 4);
    }

    #[test]
    fn test_row_is_empty() {
        let row = test_row();
        assert!(!row.is_empty());

        let empty_row = Row(Box::new([]));
        assert!(empty_row.is_empty());
    }

    fn row_into_parts_at_index(index: usize) {
        let row = Row(Box::new([
            DataType::BigInt(crate::Int64(1)),
            DataType::BigInt(crate::Int64(2)),
            DataType::BigInt(crate::Int64(3)),
            DataType::BigInt(crate::Int64(4)),
        ]));

        let (left, right) = row.into_parts(index);
        assert_eq!(left.len(), index);
        assert_eq!(right.len(), 4 - index);
    }
    param_tests!(row_into_parts_at_index, index => [0, 1, 2, 3, 4], miri_safe);

    #[test]
    fn test_row_validate_success() {
        let schema = test_schema();
        let row = test_row();
        assert!(row.validate(&schema).is_ok());
    }

    #[test]
    fn test_row_validate_wrong_count() {
        let schema = test_schema();
        let row = Row(Box::new([DataType::BigInt(crate::Int64(1))]));

        let result = row.validate(&schema);
        assert!(matches!(result, Err(TupleError::InvalidValueCount(_))));
    }

    #[test]
    fn test_row_validate_type_mismatch() {
        let schema = test_schema();
        let row = Row(Box::new([
            DataType::Blob(Blob::from("wrong")), // Should be BigInt
            DataType::Blob(Blob::from("Alice")),
            DataType::BigInt(crate::Int64(30)),
            DataType::Blob(Blob::from("alice@example.com")),
        ]));

        let result = row.validate(&schema);
        assert!(matches!(result, Err(TupleError::DataTypeMismatch(_))));
    }

    #[test]
    fn test_row_key_value_access() {
        let schema = test_schema();
        let row = test_row();

        assert!(row.key(0, &schema).is_some());
        assert!(row.key(1, &schema).is_none()); // Out of key bounds

        assert!(row.value(0, &schema).is_some());
        assert!(row.value(1, &schema).is_some());
        assert!(row.value(2, &schema).is_some());
    }

    #[test]
    fn test_row_to_bytes_roundtrip() {
        let row = test_row();
        let bytes = row.to_bytes();
        assert!(bytes.is_ok());
        assert!(!bytes.unwrap().is_empty());
    }
    #[test]
    fn test_tuple_header_new() {
        let header = TupleHeader::new(5, 100, Some(200));
        assert_eq!(header.version(), 5);
        assert_eq!(header.xmin(), 100);
        assert_eq!(header.xmax(), Some(200));
    }

    #[test]
    fn test_tuple_header_xmax_none() {
        let header = TupleHeader::new(0, 100, None);
        assert_eq!(header.xmax(), None);
    }

    fn test_tuple_header_write_read_at_offset(offset: usize) {
        let header = TupleHeader::new(3, 12345, Some(67890));
        let mut buffer = [0u8; 128];

        let end = header.write_to(&mut buffer, offset);
        let (read_header, read_end) = TupleHeader::read_from(&buffer, offset);

        assert_eq!(end, read_end);
        assert_eq!(read_header.version(), 3);
        assert_eq!(read_header.xmin(), 12345);
        assert_eq!(read_header.xmax(), Some(67890));
    }
    param_tests!(test_tuple_header_write_read_at_offset, offset => [0, 1, 5, 7, 8, 15], miri_safe);

    #[test]
    fn test_delta_header_new() {
        let header = DeltaHeader::new(2, 500);
        assert_eq!(header.version(), 2);
        assert_eq!(header.xmin(), 500);
    }

    fn test_delta_header_write_read_at_offset(offset: usize) {
        let header = DeltaHeader::new(7, 99999);
        let mut buffer = [0u8; 64];

        let end = header.write_to(&mut buffer, offset);
        let (read_header, read_end) = DeltaHeader::read_from(&buffer, offset);

        assert_eq!(end, read_end);
        assert_eq!(read_header.version(), 7);
        assert_eq!(read_header.xmin(), 99999);
    }
    param_tests!(test_delta_header_write_read_at_offset, offset => [0, 1, 4, 7], miri_safe);

    #[test]
    fn test_tuple_new() {
        let schema = test_schema();
        let row = test_row();

        let tuple = Tuple::new(row, &schema, 1);
        assert!(tuple.is_ok());

        let tuple = tuple.unwrap();
        assert_eq!(tuple.version(), 0);
        assert_eq!(tuple.xmin(), 1);
        assert_eq!(tuple.xmax(), None);
        assert!(!tuple.is_deleted());
        assert_eq!(tuple.num_keys(), 1);
        assert_eq!(tuple.num_values(), 3);
        assert_eq!(tuple.num_fields(), 4);
        assert!(!tuple.has_history());
    }

    #[test]
    fn test_tuple_new_with_nulls() {
        let schema = test_schema();
        let row = test_row_with_nulls();

        let tuple = Tuple::new(row, &schema, 1);
        assert!(tuple.is_ok());
    }

    #[test]
    fn test_tuple_key_access() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let key = tuple.key(0);
        assert!(key.is_ok());
        match key.unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 1),
            _ => panic!("Expected BigInt"),
        }
    }

    #[test]
    fn test_tuple_key_out_of_bounds() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let result = tuple.key(5);
        assert!(matches!(result, Err(TupleError::KeyError(_))));
    }

    #[test]
    fn test_tuple_set_value() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        let old = tuple.set_value(0, DataType::Blob(Blob::from("Bob")));
        assert!(old.is_some());
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
    fn test_tuple_delete_idempotent() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        tuple.delete(10).unwrap();
        tuple.delete(20).unwrap(); // Should be no-op

        assert_eq!(tuple.xmax(), Some(10)); // Still 10, not 20
    }

    fn test_tuple_visibility_at_xid(xid: u64) {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 5).unwrap();

        let expected = xid >= 5;
        assert_eq!(tuple.is_visible(xid), expected);
    }
    param_tests!(test_tuple_visibility_at_xid, xid => [0, 4, 5, 10, 100], miri_safe);

    fn test_tuple_deleted_visibility(xid: u64, delete_at: u64) {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 5).unwrap();
        tuple.delete(delete_at).unwrap();

        let expected = xid >= 5 && xid < delete_at;
        assert_eq!(tuple.is_visible(xid), expected);
    }
    param2_tests!(test_tuple_deleted_visibility, xid, delete_at => [
        (4, 15),
        (5, 15),
        (10, 15),
        (15, 15),
        (20, 15)
    ], miri_safe);

    #[test]
    fn test_tuple_add_version() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        assert_eq!(tuple.version(), 0);
        assert!(!tuple.has_history());

        let modifications = vec![(0, DataType::Blob(Blob::from("Bob")))];
        tuple.add_version(&modifications, 10).unwrap();

        assert_eq!(tuple.version(), 1);
        assert!(tuple.has_history());
        assert_eq!(tuple.num_versions(), 2);
        assert_eq!(tuple.xmin(), 10);
    }

    fn test_tuple_add_n_versions(n: usize) {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        for i in 0..n {
            let xmin = (i + 2) as u64 * 10;
            tuple
                .add_version(&[(0, DataType::Blob(Blob::from(format!("v{}", i))))], xmin)
                .unwrap();
        }

        assert_eq!(tuple.version(), n as u8);
        assert_eq!(tuple.num_versions(), n + 1);
        if n > 0 {
            assert!(tuple.has_history());
            assert_eq!(tuple.oldest_version(), 0);
        }
    }
    param_tests!(test_tuple_add_n_versions, n => [1, 2, 5, 10], miri_safe);

    #[test]
    fn test_tuple_add_version_type_mismatch() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        // Try to set a BigInt where Blob is expected
        let modifications = vec![(0, DataType::BigInt(crate::Int64(999)))];
        let result = tuple.add_version(&modifications, 10);

        assert!(matches!(result, Err(TupleError::DataTypeMismatch(_))));
    }

    #[test]
    fn test_tuple_add_version_invalid_index() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        let modifications = vec![(99, DataType::BigInt(crate::Int64(1)))];
        let result = tuple.add_version(&modifications, 10);

        assert!(matches!(result, Err(TupleError::ValueError(_))));
    }

    #[test]
    fn test_tuple_serialization_roundtrip() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];

        let written = tuple.write_to(&mut buffer).unwrap();
        assert_eq!(written, size);

        let deserialized = Tuple::read_from(&buffer, &schema).unwrap();

        assert_eq!(deserialized.version(), tuple.version());
        assert_eq!(deserialized.xmin(), tuple.xmin());
        assert_eq!(deserialized.xmax(), tuple.xmax());
        assert_eq!(deserialized.num_values(), tuple.num_values());
    }

    fn tuple_serialization_with_n_versions(n: usize) {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        for i in 0..n {
            let xmin = (i + 2) as u64 * 10;
            tuple
                .add_version(&[(0, DataType::Blob(Blob::from(format!("v{}", i))))], xmin)
                .unwrap();
        }

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let deserialized = Tuple::read_from(&buffer, &schema).unwrap();

        assert_eq!(deserialized.version(), n as u8);
        assert_eq!(deserialized.num_versions(), n + 1);
    }
    param_tests!(tuple_serialization_with_n_versions, n => [0, 1, 2, 5], miri_safe);

    #[test]
    fn test_tuple_serialization_deleted() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();
        tuple.delete(50).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let deserialized = Tuple::read_from(&buffer, &schema).unwrap();

        assert!(deserialized.is_deleted());
        assert_eq!(deserialized.xmax(), Some(50));
    }

    #[test]
    fn test_tuple_serialization_with_nulls() {
        let schema = test_schema();
        let row = test_row_with_nulls();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let deserialized = Tuple::read_from(&buffer, &schema).unwrap();
        assert_eq!(deserialized.num_values(), 3);
    }

    // ==================== TupleRef Tests ====================

    #[test]
    fn test_tuple_ref_read_last_version() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let tuple_ref = TupleRef::read_last_version(&buffer, &schema).unwrap();

        assert_eq!(tuple_ref.current_version(), 0);
        assert_eq!(tuple_ref.global_xmin(), 1);
        assert_eq!(tuple_ref.num_fields(), 4);
    }

    #[test]
    fn test_tuple_ref_key_value_access() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let tuple_ref = TupleRef::read_last_version(&buffer, &schema).unwrap();

        // Key access
        match tuple_ref.key(0).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 1),
            _ => panic!("Expected BigInt"),
        }

        // Value access
        match tuple_ref.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
    }

    fn tuple_ref_read_version(target_version: u8) {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        // Add versions up to 3
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v1")))], 10)
            .unwrap();
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v2")))], 20)
            .unwrap();
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v3")))], 30)
            .unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let tuple_ref = TupleRef::read_version(&buffer, &schema, target_version).unwrap();
        assert_eq!(tuple_ref.current_version(), target_version);
    }
    param_tests!(tuple_ref_read_version, target_version => [0, 1, 2, 3], miri_safe);

    #[test]
    fn test_tuple_ref_invalid_version() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let result = TupleRef::read_version(&buffer, &schema, 5);
        assert!(matches!(result, Err(TupleError::InvalidVersion(_))));
    }

    #[test]
    fn test_tuple_ref_null_values() {
        let schema = test_schema();
        let row = test_row_with_nulls();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let tuple_ref = TupleRef::read_last_version(&buffer, &schema).unwrap();

        assert!(matches!(tuple_ref.value(0).unwrap(), DataTypeRef::Null));
        match tuple_ref.value(1).unwrap() {
            DataTypeRef::BigInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigInt"),
        }
        assert!(matches!(tuple_ref.value(2).unwrap(), DataTypeRef::Null));
    }

    fn tuple_vacuum_with_threshold(oldest_active_xid: u64) {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        // Add versions: v0@1, v1@10, v2@20, v3@30
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v1")))], 10)
            .unwrap();
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v2")))], 20)
            .unwrap();
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("v3")))], 30)
            .unwrap();

        let initial_versions = tuple.num_versions();
        let removed = tuple.vacuum(oldest_active_xid);

        assert!(tuple.num_versions() <= initial_versions);
        assert_eq!(removed, initial_versions - tuple.num_versions());
    }
    param_tests!(tuple_vacuum_with_threshold, oldest_active_xid => [0, 5, 15, 25, 100], miri_safe);

    #[test]
    fn test_tuple_vacuum_empty_history() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        let removed = tuple.vacuum(100);
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_tuple_into_row() {
        let schema = test_schema();
        let row = test_row();
        let tuple = Tuple::new(row, &schema, 1).unwrap();

        let recovered_row = tuple.into_row().unwrap();

        assert_eq!(recovered_row.len(), 4);
        assert_eq!(recovered_row[0], DataType::BigInt(crate::Int64(1)));
    }

    #[test]
    fn test_tuple_into_row_after_modifications() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("Modified")))], 10)
            .unwrap();

        let recovered_row = tuple.into_row().unwrap();
        assert_eq!(recovered_row.len(), 4);
    }

    #[test]
    fn test_tuple_null_to_value_transition() {
        let schema = test_schema();
        let row = test_row_with_nulls();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        // Change null to value
        tuple
            .add_version(&[(0, DataType::Blob(Blob::from("Now has name")))], 10)
            .unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        // Read latest version
        let ref_latest = TupleRef::read_last_version(&buffer, &schema).unwrap();
        match ref_latest.value(0).unwrap() {
            DataTypeRef::Blob(b) => assert_eq!(b.as_str().unwrap(), "Now has name"),
            _ => panic!("Expected Blob"),
        }

        // Read version 0 (should have null)
        let ref_v0 = TupleRef::read_version(&buffer, &schema, 0).unwrap();
        assert!(matches!(ref_v0.value(0).unwrap(), DataTypeRef::Null));
    }

    #[test]
    fn test_tuple_value_to_null_transition() {
        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        // Change value to null
        tuple.add_version(&[(0, DataType::Null)], 10).unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        // Read latest version (should be null)
        let ref_latest = TupleRef::read_last_version(&buffer, &schema).unwrap();
        assert!(matches!(ref_latest.value(0).unwrap(), DataTypeRef::Null));

        // Read version 0 (should have original value)
        let ref_v0 = TupleRef::read_version(&buffer, &schema, 0).unwrap();
        match ref_v0.value(0).unwrap() {
            DataTypeRef::Blob(b) => assert_eq!(b.as_str().unwrap(), "Alice"),
            _ => panic!("Expected Blob"),
        }
    }

    // ==================== Matrix Tests ====================

    fn tuple_version_xmin_check(num_versions: usize, check_version: usize) {
        if check_version > num_versions {
            return; // Skip invalid combinations
        }

        let schema = test_schema();
        let row = test_row();
        let mut tuple = Tuple::new(row, &schema, 1).unwrap();

        let mut expected_xmins = vec![1u64];
        for i in 0..num_versions {
            let xmin = (i + 2) as u64 * 10;
            expected_xmins.push(xmin);
            tuple
                .add_version(&[(0, DataType::Blob(Blob::from(format!("v{}", i))))], xmin)
                .unwrap();
        }

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];
        tuple.write_to(&mut buffer).unwrap();

        let tuple_ref = TupleRef::read_version(&buffer, &schema, check_version as u8).unwrap();
        assert_eq!(tuple_ref.version_xmin(), expected_xmins[check_version]);
    }
    matrix_tests!(
        tuple_version_xmin_check,
        num_versions => [0, 1, 2, 3],
        check_version => [0, 1, 2, 3],
        miri_safe
    );
}
