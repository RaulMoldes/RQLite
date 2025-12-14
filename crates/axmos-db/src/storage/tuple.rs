use crate::{
    TRANSACTION_ZERO,
    database::schema::Schema,
    storage::cell::OwnedCell,
    structures::builder::{IntoCell, KeyBytes},
    transactions::Snapshot,
    types::{DataType, DataTypeRef, TransactionId, VarInt, reinterpret_cast},
    varint::MAX_VARINT_LEN,
};
use std::{
    collections::BTreeMap,
    io::{self, Error as IoError, ErrorKind},
    mem,
    ptr::{copy_nonoverlapping, write, write_bytes},
};

/// Macro for creating a Tuple with less boilerplate
#[macro_export]
macro_rules! tuple {
    ($schema:expr, $tx_id:expr, [$($value:expr),+ $(,)?]) => {{
        $crate::storage::tuple::Tuple::new(
            &[$($value),+],
            $schema,
            $crate::types::TransactionId::from($tx_id as u64),
        )
    }};
}

/// Macro for version update operations
#[macro_export]
macro_rules! update_tuple {
    ($tuple:expr, tx: $tx_id:expr, [$( $idx:expr => $value:expr ),+ $(,)?]) => {{
        $tuple.add_version(
            &[$( ($idx, $value) ),+],
            $crate::types::TransactionId::from($tx_id as u64),
        )
    }};
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
///
/// Also, this allows us to perform incremental vacuum (aka truncating tuple data at each update, opposed to a big fucking separate process as postgres does).
///
/// # Tuple layout:
///
/// [Keys ..........] [TUPLE HEADER DATA] [NULL BITMAP][LAST (n) VERSION VALUES ....] [DELTAS (n - 1)...][DELTAS (n - 2)...]
///
/// My tuple MVCC implementation is mostly based in what InnoDB (MySQL) does, since I am implementing a kind of multithreaded - Index-Organized DB like them.
#[derive(Debug, Clone)]
struct TupleLayout {
    offsets: Vec<u16>,
    key_len: u16,
    null_bitmap_offset: u16,
    last_version_end: u16,
}

/// Tuple header data contains metadata about the tuple itself.
/// As it happens with normal headers, it is fixed size (8 + 8 + 1 byte)
/// We only use one byte to keep track of versions because due to incremental vaccum being applied it is very weird that a tuple accumulates a lot of versions.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TupleHeader {
    xmin: TransactionId,
    xmax: TransactionId,
    version: u8,
}

impl TupleHeader {
    const SIZE: usize = mem::size_of::<u8>() + 2 * mem::size_of::<TransactionId>();

    #[inline]
    fn read(data: &[u8], offset: usize) -> io::Result<Self> {
        let mut cursor = offset;

        let version = data[cursor];
        cursor += 1;

        let xmin =
            TransactionId::try_from(&data[cursor..cursor + mem::size_of::<TransactionId>()])?;
        cursor += mem::size_of::<TransactionId>();

        let xmax =
            TransactionId::try_from(&data[cursor..cursor + mem::size_of::<TransactionId>()])?;

        Ok(Self {
            version,
            xmin,
            xmax,
        })
    }

    /// Allows to write the xmax directly to a byte buffer.
    #[inline]
    fn write_xmax(data: &mut [u8], key_len: usize, xid: TransactionId) {
        let xmax_offset = key_len + mem::size_of::<u8>() + mem::size_of::<TransactionId>();
        let xid_bytes = xid.as_ref();
        data[xmax_offset..xmax_offset + xid_bytes.len()].copy_from_slice(xid_bytes);
    }
}

/// Parses byte slice containing a tuple into a tuple layout in order to reconstruct tuples from it.
///
/// The [`parse`] function parses the last version of the tuple, while the parse version is used to parse a specific version by reapplying the deltas incrementally.
struct TupleParser;

impl TupleParser {
    fn parse(data: &[u8], schema: &Schema) -> io::Result<(TupleLayout, TupleHeader)> {
        let num_keys = schema.num_keys as usize;
        let num_values = schema.values().len();
        let null_bitmap_size = num_values.div_ceil(8);

        let mut offsets = Vec::with_capacity(schema.columns.len());
        let mut cursor = 0usize;

        for key in schema.iter_keys() {
            offsets.push(cursor as u16);
            let (_, read_bytes) = reinterpret_cast(key.dtype, &data[cursor..])?;
            cursor += read_bytes;
        }

        let key_len = cursor as u16;

        let header = TupleHeader::read(data, cursor)?;
        cursor += TupleHeader::SIZE;

        let null_bitmap_offset = cursor as u16;
        let null_bitmap = &data[cursor..cursor + null_bitmap_size];
        cursor += null_bitmap_size;

        for (i, col) in schema.iter_values().enumerate() {
            offsets.push(cursor as u16);
            let is_null = Self::check_null(null_bitmap, i);
            if !is_null {
                let (_, read_bytes) = reinterpret_cast(col.dtype, &data[cursor..])?;
                cursor += read_bytes;
            }
        }

        let layout = TupleLayout {
            offsets,
            key_len,
            null_bitmap_offset,
            last_version_end: cursor as u16,
        };

        Ok((layout, header))
    }

    fn parse_version(
        data: &[u8],
        schema: &Schema,
        target_version: u8,
    ) -> io::Result<(TupleLayout, TupleHeader)> {
        let (mut layout, mut header) = Self::parse(data, schema)?;

        if target_version > header.version {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Version {} doesn't exist yet (current: {})",
                    target_version, header.version
                ),
            ));
        }

        if target_version == header.version {
            return Ok((layout, header));
        }

        let num_keys = schema.num_keys as usize;
        let null_bitmap_size = schema.values().len().div_ceil(8);
        let mut cursor = layout.last_version_end as usize;

        while cursor < data.len() {
            let delta_version = data[cursor];
            cursor += 1;

            let delta_xmin =
                TransactionId::try_from(&data[cursor..cursor + mem::size_of::<TransactionId>()])?;
            cursor += mem::size_of::<TransactionId>();

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&data[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes;

            let delta_end = cursor + delta_size;

            layout.null_bitmap_offset = cursor as u16;
            cursor += null_bitmap_size;

            while cursor < delta_end {
                let field_idx = data[cursor] as usize;
                cursor += 1;

                let dtype = schema.values()[field_idx].dtype;
                layout.offsets[field_idx + num_keys] = cursor as u16;

                let (_, read_bytes) = reinterpret_cast(dtype, &data[cursor..])?;
                cursor += read_bytes;
            }

            header.xmin = delta_xmin;
            header.version = delta_version;

            if delta_version == target_version {
                break;
            }
        }

        Ok((layout, header))
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
    pub(crate) fn read(buffer: &'a [u8], schema: &'b Schema) -> io::Result<Self> {
        let (layout, header) = TupleParser::parse(buffer, schema)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    pub(crate) fn read_version(
        buffer: &'a [u8],
        schema: &'b Schema,
        version: u8,
    ) -> io::Result<Self> {
        let (layout, header) = TupleParser::parse_version(buffer, schema, version)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    #[inline]
    pub(crate) fn schema(&self) -> &'b Schema {
        self.schema
    }

    #[inline]
    pub(crate) fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.data[..self.layout.key_len as usize])
    }

    #[inline]
    pub(crate) fn num_fields(&self) -> usize {
        self.layout.offsets.len()
    }

    #[inline]
    pub(crate) fn version(&self) -> u8 {
        self.header.version
    }

    #[inline]
    pub(crate) fn xmin(&self) -> TransactionId {
        self.header.xmin
    }

    #[inline]
    pub(crate) fn xmax(&self) -> TransactionId {
        self.header.xmax
    }

    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.header.xmax != TRANSACTION_ZERO
    }

    #[inline]
    pub(crate) fn is_visible(&self, xid: TransactionId) -> bool {
        self.header.xmin <= xid && (self.header.xmax == TRANSACTION_ZERO || self.header.xmax > xid)
    }

    #[inline]
    fn null_bitmap(&self) -> &[u8] {
        let offset = self.layout.null_bitmap_offset as usize;
        let len = self.schema.values().len().div_ceil(8);
        &self.data[offset..offset + len]
    }

    #[inline]
    fn is_null(&self, val_idx: usize) -> bool {
        TupleParser::check_null(self.null_bitmap(), val_idx)
    }

    pub(crate) fn key(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let offset = self.layout.offsets[index] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    pub(crate) fn value(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        if self.is_null(index) {
            return Ok(DataTypeRef::Null);
        }

        let dtype = self.schema.values()[index].dtype;
        let offset_idx = index + self.schema.num_keys as usize;
        let offset = self.layout.offsets[offset_idx] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    pub(crate) fn version_xmin(&self, version: u8) -> io::Result<TransactionId> {
        if version == self.header.version {
            return Ok(self.header.xmin);
        }

        let mut cursor = self.layout.last_version_end as usize;

        while cursor < self.data.len() {
            let delta_version = self.data[cursor];
            cursor += 1;

            let delta_xmin = TransactionId::try_from(
                &self.data[cursor..cursor + mem::size_of::<TransactionId>()],
            )?;
            cursor += mem::size_of::<TransactionId>();

            if delta_version == version {
                return Ok(delta_xmin);
            }

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&self.data[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes + delta_size;
        }

        Err(IoError::new(
            ErrorKind::NotFound,
            format!("Version {} not found in delta chain", version),
        ))
    }

    /// Reads the most recent allowed version of a tuple for a specific transaction spanshot.
    pub(crate) fn read_for_snapshot(
        buffer: &'a [u8],
        schema: &'b Schema,
        snapshot: &crate::transactions::Snapshot,
    ) -> io::Result<Option<Self>> {
        let visible_version = Self::find_visible_version(buffer, schema, snapshot)?;
        let tuple = Self::read_version(buffer, schema, visible_version)?;

        if !snapshot.is_tuple_visible(tuple.xmin(), tuple.xmax()) {
            return Ok(None);
        }

        Ok(Some(tuple))
    }

    fn find_visible_version(
        buffer: &[u8],
        schema: &Schema,
        snapshot: &crate::transactions::Snapshot,
    ) -> io::Result<u8> {
        let (layout, header) = TupleParser::parse(buffer, schema)?;

        if header.version == 0 {
            return Ok(0);
        }

        if header.xmin == snapshot.xid() {
            return Ok(header.version);
        }

        if snapshot.is_committed_before_snapshot(header.xmin) {
            return Ok(header.version);
        }

        let mut cursor = layout.last_version_end as usize;

        while cursor < buffer.len() {
            let version = buffer[cursor];
            cursor += 1;

            let delta_xmin =
                TransactionId::try_from(&buffer[cursor..cursor + mem::size_of::<TransactionId>()])?;
            cursor += mem::size_of::<TransactionId>();

            if snapshot.is_committed_before_snapshot(delta_xmin) || delta_xmin == snapshot.xid() {
                return Ok(version);
            }

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes + delta_size;
        }

        Ok(0)
    }
}

/// Representation of a delta.
/// The xmin is the transaction id that created this version of a tuple, while the changes vector represents the array of chanes, identified by column index in the table schema.
#[derive(Debug, Clone)]
struct Delta {
    xmin: TransactionId,
    changes: Vec<(u8, DataType)>,
}

impl Delta {
    fn new(xmin: TransactionId) -> Self {
        Self {
            xmin,
            changes: Vec::new(),
        }
    }

    fn with_capacity(xmin: TransactionId, capacity: usize) -> Self {
        Self {
            xmin,
            changes: Vec::with_capacity(capacity),
        }
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

#[derive(Clone)]
pub(crate) struct Tuple<'schema> {
    key_bytes: Box<[u8]>, // The key bytes are kept contiguous in memory for convenience.
    values: Vec<DataType>, // Values struct.
    schema: &'schema Schema,
    version: u8,
    xmin: TransactionId,
    xmax: TransactionId, // Global xmax of the tuple. If set to zero, it means the tuple is not deleted.
    delta_dir: BTreeMap<u8, Delta>, // Delta directory of the tuple encoded as a btreemap.
}

impl<'schema> Tuple<'schema> {
    pub(crate) fn new(
        data: &[DataType],
        schema: &'schema Schema,
        xmin: TransactionId,
    ) -> io::Result<Self> {
        Self::validate(data, schema)?;

        let num_keys = schema.num_keys as usize;

        let key_size: usize = data[..num_keys].iter().map(|k| k.size()).sum();
        let mut key_bytes = vec![0u8; key_size].into_boxed_slice();
        let mut cursor = 0;
        for key in &data[..num_keys] {
            let bytes = key.as_ref();
            key_bytes[cursor..cursor + bytes.len()].copy_from_slice(bytes);
            cursor += bytes.len();
        }

        let values = data[num_keys..].to_vec();

        Ok(Self {
            key_bytes,
            values,
            schema,
            version: 0,
            delta_dir: BTreeMap::new(),
            xmin,
            xmax: TRANSACTION_ZERO,
        })
    }

    fn validate(data: &[DataType], schema: &Schema) -> io::Result<()> {
        if data.len() > schema.columns.len() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Too many values: {} values for {} columns",
                    data.len(),
                    schema.columns.len()
                ),
            ));
        }

        for (i, (value, col_def)) in data.iter().zip(schema.iter_columns()).enumerate() {
            if !value.matches(col_def.dtype) {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "Type mismatch at column {}: expected {:?}, got {:?}",
                        i, col_def.dtype, value
                    ),
                ));
            }
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn key_bytes_len(&self) -> usize {
        self.key_bytes.len()
    }

    pub(crate) fn key(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        if index >= self.schema.num_keys as usize {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("Key index {} out of bounds", index),
            ));
        }

        let mut cursor = 0;
        for (i, key_col) in self.schema.iter_keys().enumerate() {
            if i == index {
                let (value, _) = reinterpret_cast(key_col.dtype, &self.key_bytes[cursor..])?;
                return Ok(value);
            }
            let (_, read_bytes) = reinterpret_cast(key_col.dtype, &self.key_bytes[cursor..])?;
            cursor += read_bytes;
        }

        unreachable!()
    }

    #[inline]
    pub(crate) fn num_keys(&self) -> usize {
        self.schema.num_keys as usize
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
        self.version
    }

    #[inline]
    pub(crate) fn set_version(&mut self, version: u8) {
        self.version = version;
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
        self.xmin
    }

    #[inline]
    pub(crate) fn xmax(&self) -> TransactionId {
        self.xmax
    }

    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.xmax != TRANSACTION_ZERO
    }

    #[inline]
    pub(crate) fn is_visible(&self, xid: TransactionId) -> bool {
        self.xmin <= xid && (self.xmax == TRANSACTION_ZERO || self.xmax > xid)
    }

    pub(crate) fn add_version(
        &mut self,
        modified: &[(usize, DataType)],
        xmin: TransactionId,
    ) -> io::Result<()> {
        let old_version = self.version;
        let old_xmin = self.xmin;

        self.version += 1;
        self.xmin = xmin;

        let mut diffs = Delta::with_capacity(old_xmin, modified.len());

        for (i, value) in modified {
            let col = self.schema.values().get(*i).ok_or_else(|| {
                IoError::new(ErrorKind::InvalidData, format!("Invalid field index {}", i))
            })?;

            if !value.matches(col.dtype) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("Invalid tuple data type for field {}", i),
                ));
            }

            if let Some(old) = self.set_value(*i, value.clone()) {
                diffs.push((*i as u8, old));
            }
        }

        self.delta_dir.insert(old_version, diffs);
        Ok(())
    }

    pub(crate) fn delete(&mut self, xid: TransactionId) -> io::Result<()> {
        if self.xmax != TRANSACTION_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Tuple is already deleted",
            ));
        }
        self.xmax = xid;
        Ok(())
    }

    pub fn serialize_into(&self, dest: &mut [u8]) -> io::Result<usize> {
        let serializer = TupleSerializer::new(self);
        let size = serializer.compute_size();
        if dest.len() < size {
            return Err(IoError::new(ErrorKind::InvalidInput, "Buffer too small"));
        }
        unsafe { Ok(serializer.write_to(dest.as_mut_ptr())) }
    }

    pub fn as_bytes(&self) -> Box<[u8]> {
        Box::from(self)
    }

    pub fn into_bytes(self) -> Box<[u8]> {
        Box::from(self)
    }

    /// Vacuums the tuple by removing all versions older than the oldest visible version.
    ///
    /// The `oldest_active_xid` parameter represents the oldest transaction ID that might
    /// still need to see old versions. All versions created before this can be safely removed.
    ///
    /// Returns the number of versions removed.
    pub fn vacuum(&mut self, oldest_active_xid: TransactionId) -> usize {
        if self.delta_dir.is_empty() {
            return 0;
        }

        // Find versions that can be removed (versions whose xmin < oldest_active_xid)
        // We need to keep at least the most recent version that is visible to oldest_active_xid
        let versions_to_remove: Vec<u8> = self
            .delta_dir
            .iter()
            .filter(|(_, delta)| delta.xmin < oldest_active_xid)
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
    /// This is more precise than `vacuum` as it considers the full snapshot state.
    ///
    /// Returns the number of versions removed.
    pub fn vacuum_for_snapshot(&mut self, snapshot: &Snapshot) -> usize {
        if self.delta_dir.is_empty() {
            return 0;
        }

        // The oldest xmin we need to keep is the snapshot [xmin]
        // (represents the oldest transaction id at the time the transaction snapshot was taken)
        let oldest_needed = snapshot.xmin();

        self.vacuum(oldest_needed)
    }

    /// Aggressively vacuums the tuple, collapsing all versions into the current version.
    ///
    /// This should only be called when we're certain no transaction needs old versions.
    /// After this operation, the tuple will have version 0 with no delta chain.
    ///
    /// Returns the number of versions removed.
    pub fn vacuum_full(&mut self) -> usize {
        let removed_count = self.delta_dir.len();
        self.delta_dir.clear();

        // Reset to version 0 since we no longer have any history
        // Note: We keep xmin as-is since it represents when this data became visible
        self.version = 0;

        removed_count
    }

    /// Truncates the tuple to a specific version, removing all older versions.
    ///
    /// The tuple's values will remain at the current (latest) version,
    /// but the delta chain will be truncated to only include versions >= min_version.
    ///
    /// Returns the number of versions removed.
    pub fn truncate_to_version(&mut self, min_version: u8) -> usize {
        if self.delta_dir.is_empty() || min_version == 0 {
            let count = self.delta_dir.len();
            self.delta_dir.clear();
            return count;
        }

        // Remove all versions < min_version
        let versions_to_remove: Vec<u8> = self
            .delta_dir
            .keys()
            .filter(|&&v| v < min_version)
            .copied()
            .collect();

        let removed_count = versions_to_remove.len();

        for version in versions_to_remove {
            self.delta_dir.remove(&version);
        }

        removed_count
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
}

impl<'schema> TryFrom<(&[u8], &'schema Schema)> for Tuple<'schema> {
    type Error = IoError;

    fn try_from((buffer, schema): (&[u8], &'schema Schema)) -> Result<Self, Self::Error> {
        TupleDeserializer::deserialize(buffer, schema)
    }
}

impl<'schema> IntoCell for Tuple<'schema> {
    fn serialized_size(&self) -> usize {
        TupleSerializer::new(self).compute_size()
    }

    #[inline]
    fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(self.key_bytes.as_ref())
    }
}

impl<'schema> From<Tuple<'schema>> for OwnedCell {
    fn from(tuple: Tuple<'schema>) -> OwnedCell {
        OwnedCell::from(&tuple)
    }
}

impl<'schema> From<&Tuple<'schema>> for OwnedCell {
    fn from(tuple: &Tuple<'schema>) -> OwnedCell {
        let size = tuple.serialized_size();
        OwnedCell::new_with_writer(size, |buf| {
            tuple
                .serialize_into(buf)
                .expect("buffer size was computed correctly")
        })
    }
}

impl<'schema> From<Tuple<'schema>> for Box<[u8]> {
    fn from(tuple: Tuple<'schema>) -> Box<[u8]> {
        Box::from(&tuple)
    }
}

impl<'schema> From<&Tuple<'schema>> for Box<[u8]> {
    fn from(tuple: &Tuple<'schema>) -> Box<[u8]> {
        let size = tuple.serialized_size();
        let mut buf = vec![0u8; size];
        tuple
            .serialize_into(&mut buf)
            .expect("Size was computed properly!");
        buf.into_boxed_slice()
    }
}

struct TupleSerializer<'a, 'schema> {
    tuple: &'a Tuple<'schema>,
}

impl<'a, 'schema> TupleSerializer<'a, 'schema> {
    fn new(tuple: &'a Tuple<'schema>) -> Self {
        Self { tuple }
    }

    fn compute_size(&self) -> usize {
        let null_bitmap_size = self.tuple.values.len().div_ceil(8);
        let header_size = TupleHeader::SIZE;
        let key_size = self.tuple.key_bytes.len();
        let values_size: usize = self.tuple.values.iter().map(|dt| dt.size()).sum();
        let delta_size = self.compute_delta_size(null_bitmap_size);

        key_size + header_size + null_bitmap_size + values_size + delta_size
    }

    fn compute_delta_size(&self, null_bitmap_size: usize) -> usize {
        let mut total = 0usize;

        for (_, diffs) in self.tuple.delta_dir.iter() {
            let delta_data_size = null_bitmap_size
                + diffs.len()
                + diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

            let varint_size = VarInt::encoded_size(delta_data_size as i64);
            total += mem::size_of::<u8>()
                + mem::size_of::<TransactionId>()
                + varint_size
                + delta_data_size;
        }

        total
    }

    unsafe fn write_to(&self, ptr: *mut u8) -> usize {
        let mut cursor = 0usize;
        let null_bitmap_size = self.tuple.values.len().div_ceil(8);

        unsafe {
            copy_nonoverlapping(
                self.tuple.key_bytes.as_ptr(),
                ptr.add(cursor),
                self.tuple.key_bytes.len(),
            );
        }
        cursor += self.tuple.key_bytes.len();

        unsafe { write(ptr.add(cursor), self.tuple.version) };
        cursor += 1;

        let xmin_bytes = self.tuple.xmin.as_ref();
        unsafe { copy_nonoverlapping(xmin_bytes.as_ptr(), ptr.add(cursor), xmin_bytes.len()) };
        cursor += xmin_bytes.len();

        let xmax_bytes = self.tuple.xmax.as_ref();
        unsafe { copy_nonoverlapping(xmax_bytes.as_ptr(), ptr.add(cursor), xmax_bytes.len()) };
        cursor += xmax_bytes.len();

        let bitmap_start = cursor;
        unsafe { write_bytes(ptr.add(cursor), 0, null_bitmap_size) };
        cursor += null_bitmap_size;

        for (i, val) in self.tuple.values.iter().enumerate() {
            if let DataType::Null = val {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                unsafe { *ptr.add(bitmap_start + byte_idx) |= 1 << bit_idx };
            } else {
                let data = val.as_ref();
                unsafe { copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len()) };
                cursor += data.len();
            }
        }

        for (version, diffs) in self.tuple.delta_dir.iter().rev() {
            unsafe {
                cursor +=
                    self.write_delta(ptr, cursor, *version, diffs, bitmap_start, null_bitmap_size);
            }
        }

        cursor
    }

    unsafe fn write_delta(
        &self,
        ptr: *mut u8,
        cursor: usize,
        version: u8,
        diffs: &Delta,
        current_bitmap_start: usize,
        null_bitmap_size: usize,
    ) -> usize {
        let start = cursor;
        let mut cursor = cursor;

        unsafe { write(ptr.add(cursor), version) };
        cursor += 1;

        let xmin_bytes = diffs.xmin.as_ref();
        unsafe { copy_nonoverlapping(xmin_bytes.as_ptr(), ptr.add(cursor), xmin_bytes.len()) };
        cursor += xmin_bytes.len();

        let delta_data_size = null_bitmap_size
            + diffs.len()
            + diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

        let mut varint_buffer = [0u8; MAX_VARINT_LEN];
        let encoded = VarInt::encode(delta_data_size as i64, &mut varint_buffer);
        unsafe { copy_nonoverlapping(encoded.as_ptr(), ptr.add(cursor), encoded.len()) };
        cursor += encoded.len();

        let delta_bitmap_start = cursor;
        unsafe {
            copy_nonoverlapping(
                ptr.add(current_bitmap_start),
                ptr.add(cursor),
                null_bitmap_size,
            );
        }
        cursor += null_bitmap_size;

        for (idx, old_value) in diffs.iter_changes() {
            let byte_idx = *idx as usize / 8;
            let bit_idx = *idx as usize % 8;

            if let DataType::Null = old_value {
                unsafe { *ptr.add(delta_bitmap_start + byte_idx) |= 1 << bit_idx };
            } else {
                unsafe {
                    let current_is_null =
                        (*ptr.add(current_bitmap_start + byte_idx) & (1 << bit_idx)) != 0;
                    if current_is_null {
                        *ptr.add(delta_bitmap_start + byte_idx) &= !(1 << bit_idx);
                    }
                }
            }

            unsafe { write(ptr.add(cursor), *idx) };
            cursor += 1;

            let data = old_value.as_ref();
            unsafe { copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len()) };
            cursor += data.len();
        }

        cursor - start
    }
}

struct TupleDeserializer;

impl TupleDeserializer {
    fn deserialize<'schema>(buffer: &[u8], schema: &'schema Schema) -> io::Result<Tuple<'schema>> {
        let tuple_ref = TupleRef::read(buffer, schema)?;

        let key_len = tuple_ref.layout.key_len as usize;
        let key_bytes = buffer[..key_len].to_vec().into_boxed_slice();

        let mut values = Vec::with_capacity(schema.values().len());
        for i in 0..schema.values().len() {
            values.push(tuple_ref.value(i)?.to_owned());
        }

        let delta_dir = Self::parse_deltas(buffer, schema, &tuple_ref)?;

        Ok(Tuple {
            key_bytes,
            values,
            schema,
            version: tuple_ref.version(),
            xmin: tuple_ref.xmin(),
            xmax: tuple_ref.xmax(),
            delta_dir,
        })
    }

    fn parse_deltas(
        buffer: &[u8],
        schema: &Schema,
        tuple_ref: &TupleRef,
    ) -> io::Result<BTreeMap<u8, Delta>> {
        let mut delta_dir = BTreeMap::new();
        let null_bitmap_size = schema.values().len().div_ceil(8);
        let mut cursor = tuple_ref.layout.last_version_end as usize;

        while cursor < buffer.len() {
            let version = buffer[cursor];
            cursor += 1;

            let version_xmin =
                TransactionId::try_from(&buffer[cursor..cursor + mem::size_of::<TransactionId>()])?;
            cursor += mem::size_of::<TransactionId>();

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes;

            let delta_end = cursor + delta_size;
            cursor += null_bitmap_size;

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

        Ok(delta_dir)
    }
}

#[cfg(test)]
mod tuple_tests {
    use crate::{
        assert_tuple, assert_tuple_err, assert_tuple_ref, assert_visibility,
        database::schema::Schema,
        dt, schema, snapshot,
        storage::{cell::OwnedCell, tuple::TupleRef},
        structures::builder::IntoCell,
        types::TransactionId,
    };

    fn single_key_schema() -> Schema {
        schema!(keys: 1,
            id: Int,
            name: Text,
            active: Boolean,
            balance: Double,
            bonus: Double,
            description: Text
        )
    }

    fn multi_key_schema() -> Schema {
        schema!(keys: 2,
            id: Int,
            product_name: Text,
            active: Boolean,
            balance: Double,
            bonus: Double,
            description: Text
        )
    }

    #[test]
    fn test_tuple_creation() {
        // Single-key schema tuple
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 123),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Description")
            ]
        )
        .unwrap();

        assert_tuple!(tuple, version == 0);
        assert_tuple!(tuple, num_keys == 1);
        assert_tuple!(tuple, num_values == 5);
        assert_tuple!(tuple, key_bytes_len == 4);
        assert_tuple!(tuple, key[0] == dt!(int 123));
        assert_tuple!(tuple, value[0] == dt!(text "Alice"));

        // Multi-key schema tuple.
        let schema = multi_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(text "Widget"),
                dt!(bool true),
                dt!(double 10.5),
                dt!(double 2.25),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        assert_tuple!(tuple, num_keys == 2);
        assert_tuple!(tuple, key[0] == dt!(int 1));
        assert_tuple!(tuple, key[1] == dt!(text "Widget"));
    }

    #[test]
    fn test_tuple_serialization() {
        let schema = single_key_schema();

        let original = tuple!(
            &schema,
            1,
            [
                dt!(int 123),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Description")
            ]
        )
        .unwrap();

        let cell: OwnedCell = (&original).into();
        let reconstructed =
            TupleRef::read(cell.payload(), &schema).expect("Failed to deserialize tuple");

        assert_eq!(
            reconstructed.version(),
            original.version(),
            "Roundtrip version mismatch"
        );
        assert_eq!(
            reconstructed.key_bytes(),
            original.key_bytes(),
            "Roundtrip key_bytes mismatch"
        );

        assert_tuple_ref!(reconstructed, value[0] == dt!(text "Alice"));
        assert_tuple_ref!(reconstructed, value[1] == dt!(bool true));
        assert_tuple_ref!(reconstructed, value[2] == dt!(double 100.5));
    }

    #[test]
    fn test_serialize_into() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 123),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        let size = tuple.serialized_size();
        let mut buffer = vec![0u8; size];

        let written = tuple.serialize_into(&mut buffer).unwrap();
        assert_eq!(written, size);

        let tuple_ref = TupleRef::read(&buffer, &schema).unwrap();
        assert_tuple_ref!(tuple_ref, key[0] == dt!(int 123));
        assert_tuple_ref!(tuple_ref, value[0] == dt!(text "Alice"));
    }

    #[test]
    fn test_serialization_small_buffer() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(text "Test"),
                dt!(bool true),
                dt!(double 1.0),
                dt!(double 2.0),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        let mut small_buffer = vec![0u8; 10];
        assert_tuple_err!(tuple.serialize_into(&mut small_buffer));
    }

    #[test]
    fn test_tuple_into_cell_() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(text "Test"),
                dt!(bool true),
                dt!(double 1.0),
                dt!(double 2.0),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        let size = tuple.serialized_size();
        assert!(size > 0);

        let cell: OwnedCell = tuple.into();
        assert_eq!(cell.len(), size);
    }

    #[test]
    fn test_tuple_update() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 123),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Initial")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 2, [
            0 => dt!(text "Alice Updated"),
            2 => dt!(double 999.0)
        ])
        .unwrap();

        assert_tuple!(tuple, version == 1);
        assert_tuple!(tuple, value[0] == dt!(text "Alice Updated"));
        assert_tuple!(tuple, value[2] == dt!(double 999.0));
    }

    #[test]
    fn test_version_chain_serialization() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 123),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Initial")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 2, [0 => dt!(text "Alice Updated")]).unwrap();

        let cell: OwnedCell = (&tuple).into();

        // Latest version
        let t_latest = TupleRef::read(cell.payload(), &schema).unwrap();
        assert_tuple_ref!(t_latest, version == 1);
        assert_tuple_ref!(t_latest, value[0] == dt!(text "Alice Updated"));

        // Version 0
        let t_v0 = TupleRef::read_version(cell.payload(), &schema, 0).unwrap();
        assert_tuple_ref!(t_v0, version == 0);
        assert_tuple_ref!(t_v0, value[0] == dt!(text "Alice"));
    }

    #[test]
    fn test_version_xmin() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "V0"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 20, [0 => dt!(text "V1")]).unwrap();
        update_tuple!(tuple, tx: 30, [0 => dt!(text "V2")]).unwrap();

        let cell: OwnedCell = (&tuple).into();
        let tuple_ref = TupleRef::read(cell.payload(), &schema).unwrap();

        assert_tuple_ref!(tuple_ref, version == 2);
        assert_tuple_ref!(tuple_ref, version_xmin(2) == 30);
        assert_tuple_ref!(tuple_ref, version_xmin(1) == 20);
        assert_tuple_ref!(tuple_ref, version_xmin(0) == 10);
    }

    #[test]
    fn test_delete_tuple() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            100,
            [
                dt!(int 1),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        assert_tuple!(tuple, is_deleted == false);
        tuple.delete(TransactionId::from(200u64)).unwrap();
        assert_tuple!(tuple, is_deleted == true);

        // Cannot delete twice
        assert_tuple_err!(tuple.delete(TransactionId::from(300u64)));
    }

    #[test]
    fn test_tuple_visibility() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            100,
            [
                dt!(int 1),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        assert_tuple!(tuple, is_visible(100) == true);
        assert_tuple!(tuple, is_visible(150) == true);

        tuple.delete(TransactionId::from(200u64)).unwrap();

        assert_tuple!(tuple, is_visible(150) == true);
        assert_tuple!(tuple, is_visible(200) == false);
        assert_tuple!(tuple, is_visible(250) == false);
    }

    #[test]
    fn test_snapshot_visibility() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Original"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 20, [0 => dt!(text "Updated")]).unwrap();

        let snapshot = snapshot!(xid: 15, xmin: 10, xmax: 21, active: [20]);

        assert_visibility!(tuple, &schema, snapshot, visible);
    }

    #[test]
    fn test_snapshot_sees_own_changes() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Original"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 20, [0 => dt!(text "Modified")]).unwrap();

        let snapshot = snapshot!(xid: 20, xmin: 10, xmax: 21);

        assert_visibility!(tuple, &schema, snapshot, version == 1);
    }

    #[test]
    fn test_tuple_not_visible_before_creation() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Data"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        let snapshot = snapshot!(xid: 5, xmin: 5, xmax: 6);

        assert_visibility!(tuple, &schema, snapshot, not_visible);
    }

    #[test]
    fn test_deleted_tuple_visibility() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Data"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        tuple.delete(TransactionId::from(20u64)).unwrap();

        // Visible before delete
        let snapshot_15 = snapshot!(xid: 15, xmin: 10, xmax: 16);
        assert_visibility!(tuple, &schema, snapshot_15, visible);

        // Not visible after delete
        let snapshot_30 = snapshot!(xid: 30, xmin: 30, xmax: 31);
        assert_visibility!(tuple, &schema, snapshot_30, not_visible);
    }

    #[test]
    fn test_multi_version_snapshot_visibility() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "V0"),
                dt!(bool true),
                dt!(double 100.0),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 20, [0 => dt!(text "V1")]).unwrap();
        update_tuple!(tuple, tx: 30, [0 => dt!(text "V2")]).unwrap();

        let snapshot = snapshot!(xid: 25, xmin: 10, xmax: 31, active: [30]);

        assert_visibility!(tuple, &schema, snapshot, visible);
    }

    #[test]
    fn test_null_values() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(null),
                dt!(bool true),
                dt!(null),
                dt!(double 5.0),
                dt!(text "Test")
            ]
        )
        .unwrap();

        let cell: OwnedCell = (&tuple).into();
        let tuple_ref = TupleRef::read(cell.payload(), &schema).unwrap();

        assert_tuple_ref!(tuple_ref, value[0] is null);
        assert_tuple_ref!(tuple_ref, value[1] == dt!(bool true));
        assert_tuple_ref!(tuple_ref, value[2] is null);
        assert_tuple_ref!(tuple_ref, value[3] == dt!(double 5.0));
    }

    #[test]
    fn test_type_validation() {
        let schema = single_key_schema();

        let result = tuple!(
            &schema,
            1,
            [
                dt!(text "not an int"), // Should be Int
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Desc")
            ]
        );

        assert_tuple_err!(result, "Type mismatch should fail");
    }

    #[test]
    fn test_too_many_values() {
        let schema = single_key_schema();

        let result = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(text "Alice"),
                dt!(bool true),
                dt!(double 100.5),
                dt!(double 5.75),
                dt!(text "Desc"),
                dt!(int 999) // Extra value
            ]
        );

        assert_tuple_err!(result, "Too many values should fail");
    }

    #[test]
    fn test_key_index_oob() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            1,
            [
                dt!(int 1),
                dt!(text "Test"),
                dt!(bool true),
                dt!(double 1.0),
                dt!(double 2.0),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        assert_tuple_err!(tuple.key(5), "Key index out of bounds");
    }

    #[test]
    fn test_version_not_found() {
        let schema = single_key_schema();

        let tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Test"),
                dt!(bool true),
                dt!(double 1.0),
                dt!(double 2.0),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        let cell: OwnedCell = (&tuple).into();
        let tuple_ref = TupleRef::read(cell.payload(), &schema).unwrap();

        assert_tuple_err!(tuple_ref.version_xmin(5), "Version not found");
    }

    #[test]
    fn test_vacuum() {
        let schema = single_key_schema();

        let mut tuple = tuple!(
            &schema,
            10,
            [
                dt!(int 1),
                dt!(text "Test"),
                dt!(bool true),
                dt!(double 1.0),
                dt!(double 2.0),
                dt!(text "Desc")
            ]
        )
        .unwrap();

        update_tuple!(tuple, tx: 20, [0 => dt!(text "V1")]).unwrap();
        update_tuple!(tuple, tx: 30, [0 => dt!(text "V2")]).unwrap();
        update_tuple!(tuple, tx: 40, [0 => dt!(text "V3")]).unwrap();

        assert_eq!(tuple.num_versions(), 4);
        assert_eq!(tuple.version(), 3);

        // Vacuum with oldest_active_xid = 25
        // Deltas have xmin values: 0->10, 1->20, 2->30
        // Should remove versions where xmin < 25: versions 0 (xmin=10) and 1 (xmin=20)
        let removed = tuple.vacuum(TransactionId::from(25u64));

        assert_eq!(removed, 2); // Both version 0 and 1 are removed
        assert_eq!(tuple.num_versions(), 2); // Current (3) + delta for version 2
        assert_eq!(tuple.oldest_version(), 2); // Oldest remaining is version 2
    }
}
