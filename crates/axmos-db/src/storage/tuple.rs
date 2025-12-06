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
    collections::BTreeMap,
    io::{self, Error as IoError, ErrorKind},
    mem,
    ptr::{copy_nonoverlapping, write, write_bytes},
};

/// Parsed tuple layout metadata - computed once, reused for all field accesses.
/// This is the core optimization: we parse the structure once and store offsets.
#[derive(Debug, Clone)]
struct TupleLayout {
    /// Byte offset where each field (keys + values) starts
    offsets: Vec<u16>,
    /// Total byte length of all keys
    key_len: u16,
    /// Byte offset where the null bitmap starts
    null_bitmap_offset: u16,
    /// Byte offset where the last version's data ends (start of delta chain)
    last_version_end: u16,
}

/// Parsed tuple header - the fixed portion after keys
#[derive(Debug, Clone, Copy)]
struct TupleHeaderData {
    version: u8,
    xmin: TransactionId,
    xmax: TransactionId,
}

impl TupleHeaderData {
    const TRANSACTION_ID_SIZE: usize = mem::size_of::<TransactionId>();
    /// Size of header: version(1) + xmin(8) + xmax(8) = 17 bytes
    const SIZE: usize = 1 + 2 * Self::TRANSACTION_ID_SIZE;

    #[inline]
    fn read(data: &[u8], offset: usize) -> io::Result<Self> {
        let mut cursor = offset;

        let version = data[cursor];
        cursor += 1;

        let xmin = TransactionId::try_from(&data[cursor..cursor + Self::TRANSACTION_ID_SIZE])?;
        cursor += Self::TRANSACTION_ID_SIZE;

        let xmax = TransactionId::try_from(&data[cursor..cursor + Self::TRANSACTION_ID_SIZE])?;

        Ok(Self {
            version,
            xmin,
            xmax,
        })
    }

    #[inline]
    fn write_xmax(data: &mut [u8], key_len: usize, xid: TransactionId) {
        let xmax_offset = key_len + 1 + Self::TRANSACTION_ID_SIZE;
        let xid_bytes = xid.as_ref();
        data[xmax_offset..xmax_offset + xid_bytes.len()].copy_from_slice(xid_bytes);
    }
}

/// Core parsing logic shared between TupleRef and TupleRefMut
struct TupleParser;

impl TupleParser {
    /// Parse the tuple structure and return layout + header.
    /// This is the single source of truth for tuple parsing.
    fn parse(data: &[u8], schema: &Schema) -> io::Result<(TupleLayout, TupleHeaderData)> {
        let num_keys = schema.num_keys as usize;
        let num_values = schema.values().len();
        let null_bitmap_size = num_values.div_ceil(8);

        let mut offsets = Vec::with_capacity(schema.columns.len());
        let mut cursor = 0usize;

        // Parse keys
        for key in schema.iter_keys() {
            offsets.push(cursor as u16);
            let (_, read_bytes) = reinterpret_cast(key.dtype, &data[cursor..])?;
            cursor += read_bytes;
        }

        let key_len = cursor as u16;

        // Parse header
        let header = TupleHeaderData::read(data, cursor)?;
        cursor += TupleHeaderData::SIZE;

        // Record null bitmap offset
        let null_bitmap_offset = cursor as u16;
        let null_bitmap = &data[cursor..cursor + null_bitmap_size];
        cursor += null_bitmap_size;

        // Parse values (skip null values)
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

    /// Parse a specific version by traversing the delta chain.
    fn parse_version(
        data: &[u8],
        schema: &Schema,
        target_version: u8,
    ) -> io::Result<(TupleLayout, TupleHeaderData)> {
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

        // If requesting current version, we're done
        if target_version == header.version {
            return Ok((layout, header));
        }

        // Traverse delta chain
        let num_keys = schema.num_keys as usize;
        let null_bitmap_size = schema.values().len().div_ceil(8);
        let mut cursor = layout.last_version_end as usize;

        while cursor < data.len() {
            let delta_version = data[cursor];
            cursor += 1;

            let delta_xmin = TransactionId::try_from(
                &data[cursor..cursor + TupleHeaderData::TRANSACTION_ID_SIZE],
            )?;
            cursor += TupleHeaderData::TRANSACTION_ID_SIZE;

            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&data[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes;

            let delta_end = cursor + delta_size;

            // Update null bitmap offset for this version
            layout.null_bitmap_offset = cursor as u16;
            cursor += null_bitmap_size;

            // Update field offsets from delta
            while cursor < delta_end {
                let field_idx = data[cursor] as usize;
                cursor += 1;

                let dtype = schema.values()[field_idx].dtype;
                layout.offsets[field_idx + num_keys] = cursor as u16;

                let (_, read_bytes) = reinterpret_cast(dtype, &data[cursor..])?;
                cursor += read_bytes;
            }

            // Update header for this version
            header.xmin = delta_xmin;
            header.version = delta_version;

            if delta_version == target_version {
                break;
            }
        }

        Ok((layout, header))
    }

    #[inline]
    fn check_null(bitmap: &[u8], val_idx: usize) -> bool {
        let byte_idx = val_idx / 8;
        let bit_idx = val_idx % 8;
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }
}

/// Immutable view of a tuple at a specific point in time.
pub struct TupleRef<'a, 'b> {
    data: &'a [u8],
    schema: &'b Schema,
    layout: TupleLayout,
    header: TupleHeaderData,
}

impl<'a, 'b> TupleRef<'a, 'b> {
    /// Reads the latest version of the tuple.
    pub fn read(buffer: &'a [u8], schema: &'b Schema) -> io::Result<Self> {
        let (layout, header) = TupleParser::parse(buffer, schema)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    /// Reads a specific version of the tuple.
    pub fn read_version(buffer: &'a [u8], schema: &'b Schema, version: u8) -> io::Result<Self> {
        let (layout, header) = TupleParser::parse_version(buffer, schema, version)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    #[inline]
    pub fn schema(&self) -> &'b Schema {
        self.schema
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.layout.offsets.len()
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    #[inline]
    pub fn xmin(&self) -> TransactionId {
        self.header.xmin
    }

    #[inline]
    pub fn xmax(&self) -> TransactionId {
        self.header.xmax
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.header.xmax != TRANSACTION_ZERO
    }

    #[inline]
    pub fn is_visible(&self, xid: TransactionId) -> bool {
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

    pub fn key(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let offset = self.layout.offsets[index] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    pub fn value(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        if self.is_null(index) {
            return Ok(DataTypeRef::Null);
        }

        let dtype = self.schema.values()[index].dtype;
        let offset_idx = index + self.schema.num_keys as usize;
        let offset = self.layout.offsets[offset_idx] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    /// Gets the xmin for a specific version
    pub fn version_xmin(&self, version: u8) -> io::Result<TransactionId> {
        if version == self.header.version {
            return Ok(self.header.xmin);
        }

        // Search delta chain
        let mut cursor = self.layout.last_version_end as usize;
        let null_bitmap_size = self.schema.values().len().div_ceil(8);

        while cursor < self.data.len() {
            let delta_version = self.data[cursor];
            cursor += 1;

            let delta_xmin = TransactionId::try_from(
                &self.data[cursor..cursor + TupleHeaderData::TRANSACTION_ID_SIZE],
            )?;
            cursor += TupleHeaderData::TRANSACTION_ID_SIZE;

            if delta_version == version {
                return Ok(delta_xmin);
            }

            // Skip delta content
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&self.data[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes + delta_size;
        }

        Err(IoError::new(
            ErrorKind::NotFound,
            format!("Version {} not found in delta chain", version),
        ))
    }

    /// Reads the visible version of the tuple for the provided snapshot.
    pub fn read_for_snapshot(
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

    /// Finds the visible version number for a given snapshot.
    fn find_visible_version(
        buffer: &[u8],
        schema: &Schema,
        snapshot: &crate::transactions::Snapshot,
    ) -> io::Result<u8> {
        let (layout, header) = TupleParser::parse(buffer, schema)?;

        // Single version - no deltas to check
        if header.version == 0 {
            return Ok(0);
        }

        // If our transaction created this version, it's visible
        if header.xmin == snapshot.xid() {
            return Ok(header.version);
        }

        // Check if latest version is visible
        if snapshot.is_committed_before_snapshot(header.xmin) {
            return Ok(header.version);
        }

        // Parse deltas to find visible version
        let null_bitmap_size = schema.values().len().div_ceil(8);
        let mut cursor = layout.last_version_end as usize;

        while cursor < buffer.len() {
            let version = buffer[cursor];
            cursor += 1;

            let delta_xmin = TransactionId::try_from(
                &buffer[cursor..cursor + TupleHeaderData::TRANSACTION_ID_SIZE],
            )?;
            cursor += TupleHeaderData::TRANSACTION_ID_SIZE;

            // Check visibility
            if snapshot.is_committed_before_snapshot(delta_xmin) || delta_xmin == snapshot.xid() {
                return Ok(version);
            }

            // Skip delta content
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&buffer[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes + delta_size;
        }

        // No visible version found
        Ok(0)
    }

    fn to_owned(&self) -> OwnedTuple {
        OwnedTuple(self.data.to_vec().into_boxed_slice())
    }
}

/// Mutable view of a tuple at a specific point in the version chain.
pub struct TupleRefMut<'a, 'b> {
    data: &'a mut [u8],
    schema: &'b Schema,
    layout: TupleLayout,
    header: TupleHeaderData,
}

impl<'a, 'b> TupleRefMut<'a, 'b> {
    /// Reads the latest version of the tuple.
    pub fn read(buffer: &'a mut [u8], schema: &'b Schema) -> io::Result<Self> {
        // Parse immutably first, then take mutable reference
        let (layout, header) = TupleParser::parse(buffer, schema)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    /// Reads a specific version of the tuple.
    pub fn read_version(buffer: &'a mut [u8], schema: &'b Schema, version: u8) -> io::Result<Self> {
        let (layout, header) = TupleParser::parse_version(buffer, schema, version)?;
        Ok(Self {
            data: buffer,
            schema,
            layout,
            header,
        })
    }

    #[inline]
    pub fn schema(&self) -> &'b Schema {
        self.schema
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.layout.offsets.len()
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    #[inline]
    pub fn xmin(&self) -> TransactionId {
        self.header.xmin
    }

    #[inline]
    pub fn xmax(&self) -> TransactionId {
        self.header.xmax
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.header.xmax != TRANSACTION_ZERO
    }

    #[inline]
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        self.header.xmin <= xid && (self.header.xmax == TRANSACTION_ZERO || self.header.xmax > xid)
    }

    #[inline]
    fn null_bitmap(&self) -> &[u8] {
        let offset = self.layout.null_bitmap_offset as usize;
        let len = self.schema.values().len().div_ceil(8);
        &self.data[offset..offset + len]
    }

    #[inline]
    fn null_bitmap_mut(&mut self) -> &mut [u8] {
        let offset = self.layout.null_bitmap_offset as usize;
        let len = self.schema.values().len().div_ceil(8);
        &mut self.data[offset..offset + len]
    }

    #[inline]
    fn is_null(&self, val_idx: usize) -> bool {
        TupleParser::check_null(self.null_bitmap(), val_idx)
    }

    pub fn key(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let offset = self.layout.offsets[index] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    fn key_mut(&mut self, index: usize) -> io::Result<DataTypeRefMut<'_>> {
        let dtype = self.schema.keys()[index].dtype;
        let offset = self.layout.offsets[index] as usize;
        let (value, _) = reinterpret_cast_mut(dtype, &mut self.data[offset..])?;
        Ok(value)
    }

    pub fn value(&self, index: usize) -> io::Result<DataTypeRef<'_>> {
        if self.is_null(index) {
            return Ok(DataTypeRef::Null);
        }

        let dtype = self.schema.values()[index].dtype;
        let offset_idx = index + self.schema.num_keys as usize;
        let offset = self.layout.offsets[offset_idx] as usize;
        let (value, _) = reinterpret_cast(dtype, &self.data[offset..])?;
        Ok(value)
    }

    fn value_mut(&mut self, index: usize) -> io::Result<DataTypeRefMut<'_>> {
        if self.is_null(index) {
            return Ok(DataTypeRefMut::Null);
        }

        let dtype = self.schema.values()[index].dtype;
        let offset_idx = index + self.schema.num_keys as usize;
        let offset = self.layout.offsets[offset_idx] as usize;
        let (value, _) = reinterpret_cast_mut(dtype, &mut self.data[offset..])?;
        Ok(value)
    }

    /// Sets a value to null in the bitmap
    pub fn set_null_unchecked(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.null_bitmap_mut()[byte_idx] |= 1 << bit_idx;
    }

    /// Clears the null flag for a value
    pub fn clear_null(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.null_bitmap_mut()[byte_idx] &= !(1 << bit_idx);
    }

    /// Update a value in place
    pub fn update_value<F>(&mut self, index: usize, updater: F) -> io::Result<()>
    where
        F: FnOnce(DataTypeRefMut<'_>),
    {
        let value = self.value_mut(index)?;
        updater(value);
        Ok(())
    }

    /// Sets the item at a specified index to the corresponding value.
    pub fn set_value(&mut self, index: usize, value: DataType) -> io::Result<()> {
        let dtype = self.schema.columns[index].dtype;
        let offset = self.layout.offsets[index] as usize;

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
                Self::set_varlen_value(&mut self.data[offset..], &v)?;
            }
            (DataTypeKind::Blob, DataType::Blob(v)) => {
                Self::set_varlen_value(&mut self.data[offset..], &v)?;
            }
            _ => {
                return Err(IoError::new(ErrorKind::InvalidInput, "Type mismatch"));
            }
        }
        Ok(())
    }

    /// Helper for setting variable-length values (Text/Blob)
    fn set_varlen_value(data: &mut [u8], new_value: &impl AsRef<[u8]>) -> io::Result<()> {
        let (len_varint, varint_size) = VarInt::from_encoded_bytes(data)?;
        let existing_len: usize = len_varint.try_into()?;
        let existing_total = existing_len + varint_size;

        let new_bytes = new_value.as_ref();
        let (new_len_varint, new_varint_size) = VarInt::from_encoded_bytes(new_bytes)?;
        let new_len: usize = new_len_varint.try_into()?;
        let new_total = new_len + new_varint_size;

        if new_total <= existing_total {
            data[..new_bytes.len()].copy_from_slice(new_bytes);
            // Pad remaining space with zeros
            if new_bytes.len() < existing_total {
                data[new_bytes.len()..existing_total].fill(0);
            }
            Ok(())
        } else {
            Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "New value ({} bytes) exceeds allocated space ({} bytes)",
                    new_total, existing_total
                ),
            ))
        }
    }

    /// Marks the tuple as deleted.
    pub fn delete(&mut self, xid: TransactionId) -> io::Result<()> {
        if self.header.xmax != TRANSACTION_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Tuple is already deleted",
            ));
        }

        TupleHeaderData::write_xmax(self.data, self.layout.key_len as usize, xid);
        self.header.xmax = xid;
        Ok(())
    }

    /// Gets the xmin for a specific version
    pub fn version_xmin(&self, version: u8) -> io::Result<TransactionId> {
        if version == self.header.version {
            return Ok(self.header.xmin);
        }

        // Search delta chain
        let mut cursor = self.layout.last_version_end as usize;

        while cursor < self.data.len() {
            let delta_version = self.data[cursor];
            cursor += 1;

            let delta_xmin = TransactionId::try_from(
                &self.data[cursor..cursor + TupleHeaderData::TRANSACTION_ID_SIZE],
            )?;
            cursor += TupleHeaderData::TRANSACTION_ID_SIZE;

            if delta_version == version {
                return Ok(delta_xmin);
            }

            // Skip delta content
            let (size_varint, varint_bytes) = VarInt::from_encoded_bytes(&self.data[cursor..])?;
            let delta_size: usize = size_varint.try_into()?;
            cursor += varint_bytes + delta_size;
        }

        Err(IoError::new(
            ErrorKind::NotFound,
            format!("Version {} not found in delta chain", version),
        ))
    }

    /// Reads the visible version of the tuple for the provided snapshot.
    pub fn read_for_snapshot(
        buffer: &'a mut [u8],
        schema: &'b Schema,
        snapshot: &crate::transactions::Snapshot,
    ) -> io::Result<Option<Self>> {
        // Use immutable find first
        let visible_version = TupleRef::find_visible_version(buffer, schema, snapshot)?;
        let tuple = Self::read_version(buffer, schema, visible_version)?;

        if !snapshot.is_tuple_visible(tuple.xmin(), tuple.xmax()) {
            return Ok(None);
        }

        Ok(Some(tuple))
    }

    fn to_owned(&self) -> OwnedTuple {
        OwnedTuple(self.data.to_vec().into_boxed_slice())
    }
}

/// Delta stores changes for a specific version
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

    #[inline]
    fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Owned tuple structure.
/// Tuples are generally a [COPY-ON-WRITE] data structure.
#[derive(Clone)]
pub struct Tuple<'schema> {
    data: Vec<DataType>,
    schema: &'schema Schema,
    version: u8,
    xmin: TransactionId,
    xmax: TransactionId,
    delta_dir: BTreeMap<u8, Delta>,
}

impl<'schema> Tuple<'schema> {
    pub fn new(
        values: &[DataType],
        schema: &'schema Schema,
        xmin: TransactionId,
    ) -> io::Result<Self> {
        Self::validate(values, schema)?;

        Ok(Self {
            data: values.to_vec(),
            schema,
            version: 0,
            delta_dir: BTreeMap::new(),
            xmin,
            xmax: TRANSACTION_ZERO,
        })
    }

    fn validate(values: &[DataType], schema: &Schema) -> io::Result<()> {
        if values.len() > schema.columns.len() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Too many values: {} values for {} columns",
                    values.len(),
                    schema.columns.len()
                ),
            ));
        }

        for (i, (value, col_def)) in values.iter().zip(schema.iter_columns()).enumerate() {
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
    pub fn num_fields(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn set_version(&mut self, version: u8) {
        self.version = version;
    }

    #[inline]
    pub fn keys(&self) -> &[DataType] {
        &self.data[..self.schema.num_keys as usize]
    }

    #[inline]
    pub fn values(&self) -> &[DataType] {
        &self.data[self.schema.num_keys as usize..]
    }

    #[inline]
    pub fn values_mut(&mut self) -> &mut [DataType] {
        &mut self.data[self.schema.num_keys as usize..]
    }

    #[inline]
    pub fn set_value(&mut self, index: usize, new: DataType) -> Option<DataType> {
        self.values_mut()
            .get_mut(index)
            .map(|slot| mem::replace(slot, new))
    }

    #[inline]
    pub fn xmin(&self) -> TransactionId {
        self.xmin
    }

    #[inline]
    pub fn xmax(&self) -> TransactionId {
        self.xmax
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.xmax != TRANSACTION_ZERO
    }

    #[inline]
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        self.xmin <= xid && (self.xmax == TRANSACTION_ZERO || self.xmax > xid)
    }

    pub fn add_version(
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

    pub fn delete(&mut self, xid: TransactionId) -> io::Result<()> {
        if self.xmax != TRANSACTION_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Tuple is already deleted",
            ));
        }
        self.xmax = xid;
        Ok(())
    }
}

/// Serialized tuple as raw bytes
#[derive(Clone, Debug)]
pub struct OwnedTuple(Box<[u8]>);

impl OwnedTuple {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes.into_boxed_slice())
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for OwnedTuple {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for OwnedTuple {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

/// Serialization: Tuple -> OwnedTuple
impl<'schema> From<Tuple<'schema>> for OwnedTuple {
    fn from(value: Tuple<'schema>) -> OwnedTuple {
        OwnedTuple::from(&value)
    }
}

impl<'schema> From<&Tuple<'schema>> for OwnedTuple {
    fn from(tuple: &Tuple<'schema>) -> OwnedTuple {
        let serializer = TupleSerializer::new(tuple);
        serializer.serialize()
    }
}

/// Deserialization: (&[u8], &Schema) -> Tuple
impl<'schema> TryFrom<(&[u8], &'schema Schema)> for Tuple<'schema> {
    type Error = IoError;

    fn try_from((buffer, schema): (&[u8], &'schema Schema)) -> Result<Self, Self::Error> {
        TupleDeserializer::deserialize(buffer, schema)
    }
}

/// Handles tuple serialization to bytes
struct TupleSerializer<'a, 'schema> {
    tuple: &'a Tuple<'schema>,
}

impl<'a, 'schema> TupleSerializer<'a, 'schema> {
    fn new(tuple: &'a Tuple<'schema>) -> Self {
        Self { tuple }
    }

    fn serialize(self) -> OwnedTuple {
        let total_size = self.compute_size();
        let mut buffer: Box<[mem::MaybeUninit<u8>]> = Box::new_uninit_slice(total_size);

        // SAFETY: We have exclusive access to the buffer, and `write_to` will initialize
        // exactly `total_size` bytes. The buffer was allocated with the exact size needed.
        unsafe {
            let ptr = buffer.as_mut_ptr() as *mut u8;
            let written = self.write_to(ptr);
            debug_assert_eq!(written, total_size, "Size calculation mismatch");
            OwnedTuple(Box::from_raw(Box::into_raw(buffer) as *mut [u8]))
        }
    }

    fn compute_size(&self) -> usize {
        let null_bitmap_size = self.tuple.values().len().div_ceil(8);
        let header_size = TupleHeaderData::SIZE;
        let data_size: usize = self.tuple.data.iter().map(|dt| dt.size()).sum();
        let delta_size = self.compute_delta_size(null_bitmap_size);

        data_size + header_size + null_bitmap_size + delta_size
    }

    fn compute_delta_size(&self, null_bitmap_size: usize) -> usize {
        let mut total = 0usize;

        for (_, diffs) in self.tuple.delta_dir.iter() {
            // Each delta: version(1) + xmin(8) + varint + bitmap + (index + data) per change
            let delta_data_size = null_bitmap_size
                + diffs.len()
                + diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

            let varint_size = VarInt::encoded_size(delta_data_size as i64);
            total += 1 + TupleHeaderData::TRANSACTION_ID_SIZE + varint_size + delta_data_size;
        }

        total
    }

    /// Writes the complete tuple to the given memory location.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    ///
    /// - `ptr` is valid for writes of `self.compute_size()` bytes.
    /// - `ptr` is properly aligned for `u8` writes (always satisfied for `*mut u8`).
    /// - The memory region `[ptr, ptr + compute_size())` does not overlap with any
    ///   memory referenced by `self.tuple`.
    /// - No other references to the memory region exist for the duration of this call.
    ///
    /// # Returns
    ///
    /// The number of bytes written. This must equal `self.compute_size()`.
    unsafe fn write_to(&self, ptr: *mut u8) -> usize {
        let mut cursor = 0usize;
        let null_bitmap_size = self.tuple.values().len().div_ceil(8);

        // Write keys
        for key in self.tuple.keys() {
            let data = key.as_ref();
            unsafe {
                copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
            };
            cursor += data.len();
        }

        // Write Version
        unsafe {
            write(ptr.add(cursor), self.tuple.version);
        };
        cursor += 1;

        // Write xmin
        let xmin_bytes = self.tuple.xmin.as_ref();
        unsafe {
            copy_nonoverlapping(xmin_bytes.as_ptr(), ptr.add(cursor), xmin_bytes.len());
        };
        cursor += xmin_bytes.len();

        // Write xmax
        let xmax_bytes = self.tuple.xmax.as_ref();
        unsafe {
            copy_nonoverlapping(xmax_bytes.as_ptr(), ptr.add(cursor), xmax_bytes.len());
        };
        cursor += xmax_bytes.len();

        // Write null bitmap (zeroed initially)
        let bitmap_start = cursor;
        unsafe {
            write_bytes(ptr.add(cursor), 0, null_bitmap_size);
        };
        cursor += null_bitmap_size;

        // Write values and update bitmap for nulls
        for (i, val) in self.tuple.values().iter().enumerate() {
            if let DataType::Null = val {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                unsafe {
                    *ptr.add(bitmap_start + byte_idx) |= 1 << bit_idx;
                };
            } else {
                let data = val.as_ref();
                unsafe {
                    copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
                };
                cursor += data.len();
            }
        }

        // Write deltas in reverse order (newest first)
        for (version, diffs) in self.tuple.delta_dir.iter().rev() {
            unsafe {
                cursor +=
                    self.write_delta(ptr, cursor, *version, diffs, bitmap_start, null_bitmap_size);
            };
        }

        cursor
    }

    /// Writes a single delta record to the given memory location.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    ///
    /// - `ptr` points to a valid memory region large enough to hold the entire tuple
    ///   (as computed by `compute_size()`).
    /// - `cursor` is a valid offset within the allocated region, and the remaining
    ///   space `[ptr + cursor, ptr + compute_size())` is sufficient for this delta.
    /// - `current_bitmap_start` is a valid offset pointing to the current version's
    ///   null bitmap within the same memory region.
    /// - `null_bitmap_size` correctly represents the size of the null bitmap in bytes.
    /// - The memory regions for the delta and the source bitmap do not overlap in a
    ///   way that would cause undefined behavior during the copy operation.
    /// - No other mutable references to the affected memory regions exist.
    ///
    /// # Arguments
    ///
    /// - `ptr`: Base pointer to the tuple buffer.
    /// - `cursor`: Current write offset from `ptr`.
    /// - `version`: Version number for this delta.
    /// - `diffs`: The delta changes to write.
    /// - `current_bitmap_start`: Offset to the current version's null bitmap.
    /// - `null_bitmap_size`: Size of the null bitmap in bytes.
    ///
    /// # Returns
    ///
    /// The number of bytes written for this delta.
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

        // Version byte
        unsafe {
            write(ptr.add(cursor), version);
        };
        cursor += 1;

        // Xmin
        let xmin_bytes = diffs.xmin.as_ref();
        unsafe {
            copy_nonoverlapping(xmin_bytes.as_ptr(), ptr.add(cursor), xmin_bytes.len());
        };
        cursor += xmin_bytes.len();

        // Compute and write delta size as varint
        let delta_data_size = null_bitmap_size
            + diffs.len()
            + diffs.iter_changes().map(|(_, dt)| dt.size()).sum::<usize>();

        let mut varint_buffer = [0u8; MAX_VARINT_LEN];
        let encoded = VarInt::encode(delta_data_size as i64, &mut varint_buffer);
        unsafe {
            copy_nonoverlapping(encoded.as_ptr(), ptr.add(cursor), encoded.len());
        };
        cursor += encoded.len();

        // Copy current bitmap as base for this delta's bitmap
        let delta_bitmap_start = cursor;
        unsafe {
            copy_nonoverlapping(
                ptr.add(current_bitmap_start),
                ptr.add(cursor),
                null_bitmap_size,
            );
        };
        cursor += null_bitmap_size;

        // Write changes and update delta bitmap
        for (idx, old_value) in diffs.iter_changes() {
            let byte_idx = *idx as usize / 8;
            let bit_idx = *idx as usize % 8;

            // Update bitmap based on whether old value was null
            if let DataType::Null = old_value {
                // Old value was null, set the bit
                unsafe {
                    *ptr.add(delta_bitmap_start + byte_idx) |= 1 << bit_idx;
                };
            } else {
                // Old value was not null; if current is null, clear the bit
                unsafe {
                    let current_is_null =
                        (*ptr.add(current_bitmap_start + byte_idx) & (1 << bit_idx)) != 0;
                    if current_is_null {
                        *ptr.add(delta_bitmap_start + byte_idx) &= !(1 << bit_idx);
                    }
                };
            }

            // Write field index
            unsafe {
                write(ptr.add(cursor), *idx);
            };
            cursor += 1;

            // Write field data
            let data = old_value.as_ref();
            unsafe {
                copy_nonoverlapping(data.as_ptr(), ptr.add(cursor), data.len());
            };
            cursor += data.len();
        }

        cursor - start
    }
}
/// Handles tuple deserialization from bytes
struct TupleDeserializer;

impl TupleDeserializer {
    fn deserialize<'schema>(buffer: &[u8], schema: &'schema Schema) -> io::Result<Tuple<'schema>> {
        let tuple_ref = TupleRef::read(buffer, schema)?;

        // Extract data
        let mut data = Vec::with_capacity(schema.columns.len());

        for i in 0..schema.num_keys as usize {
            data.push(tuple_ref.key(i)?.to_owned());
        }

        for i in 0..schema.values().len() {
            data.push(tuple_ref.value(i)?.to_owned());
        }

        // Parse delta directory
        let delta_dir = Self::parse_deltas(buffer, schema, &tuple_ref)?;

        Ok(Tuple {
            data,
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

            let version_xmin = TransactionId::try_from(
                &buffer[cursor..cursor + TupleHeaderData::TRANSACTION_ID_SIZE],
            )?;
            cursor += TupleHeaderData::TRANSACTION_ID_SIZE;

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

        Ok(delta_dir)
    }
}
#[cfg(test)]
mod tuple_tests {

    use super::*;
    use crate::database::schema::{Column, Schema};
    use crate::transactions::Snapshot;
    use crate::types::{DataType, DataTypeKind, UInt8};
    use std::collections::HashSet;

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

    #[test]
    fn test_tuple_8() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);

        let tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        let buf: OwnedTuple = tuple.into();
        let tuple_ref = TupleRef::read(buf.as_ref(), &schema).unwrap();

        assert_eq!(tuple_ref.version(), 0);
        assert_eq!(tuple_ref.version_xmin(0).unwrap(), xid_10);
    }

    #[test]
    fn test_tuple_9() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_30 = TransactionId::from(30u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("V0".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("V1".into()))], xid_20)
            .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("V2".into()))], xid_30)
            .unwrap();

        let buf: OwnedTuple = tuple.into();
        let tuple_ref = TupleRef::read(buf.as_ref(), &schema).unwrap();

        assert_eq!(tuple_ref.version(), 2);
        assert_eq!(tuple_ref.version_xmin(2).unwrap(), xid_30);
        assert_eq!(tuple_ref.version_xmin(1).unwrap(), xid_20);
        assert_eq!(tuple_ref.version_xmin(0).unwrap(), xid_10);
    }

    #[test]
    fn test_tuple_10() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);

        let tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Alice".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        let buf: OwnedTuple = tuple.into();
        let tuple_ref = TupleRef::read(buf.as_ref(), &schema).unwrap();

        // Version 5 does not exist
        assert!(tuple_ref.version_xmin(5).is_err());
    }

    #[test]
    fn test_tuple_11() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Original".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("Modified".into()))], xid_20)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot de tx 20 debería ver sus propios cambios (version 1)
        let snapshot_20 = Snapshot::new(xid_20, xid_10, TransactionId::from(21u64), HashSet::new());

        let version = TupleRef::find_visible_version(buf.as_ref(), &schema, &snapshot_20).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn test_tuple_12() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_30 = TransactionId::from(30u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("V0".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("V1".into()))], xid_20)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Tx 30 started before tx 20 committed
        let snapshot_30 = Snapshot::new(xid_30, xid_30, TransactionId::from(31u64), HashSet::new());

        let version = TupleRef::find_visible_version(buf.as_ref(), &schema, &snapshot_30).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn test_tuple_14() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_15 = TransactionId::from(15u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("V0".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        // Tx 20 modifies but has not committed yet
        tuple
            .add_version(&[(0, DataType::Text("V1".into()))], xid_20)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot of tx 15 with tx 20 active.
        let mut active = HashSet::new();
        active.insert(xid_20);

        let snapshot_15 = Snapshot::new(xid_15, xid_10, TransactionId::from(21u64), active);

        let version = TupleRef::find_visible_version(buf.as_ref(), &schema, &snapshot_15).unwrap();
        assert_eq!(version, 0);
    }

    #[test]
    fn test_tuple_15() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_30 = TransactionId::from(30u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Original".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("Updated".into()))], xid_20)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot after tx 20 reads 'Updated'
        let snapshot_30 = Snapshot::new(xid_30, xid_30, TransactionId::from(31u64), HashSet::new());

        let tuple_ref = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_30)
            .unwrap()
            .expect("Should find a visible tuple");

        assert_eq!(
            tuple_ref.value(0).unwrap().to_owned(),
            DataType::Text("Updated".into())
        );
    }

    #[test]
    fn test_tuple_16() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_15 = TransactionId::from(15u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Original".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("Updated".into()))], xid_20)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot with tx being active sees 'Original'
        let mut active = HashSet::new();
        active.insert(xid_20);

        let snapshot_15 = Snapshot::new(xid_15, xid_10, TransactionId::from(21u64), active);

        let tuple_ref = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_15)
            .unwrap()
            .expect("Should find a visible tuple");

        assert_eq!(
            tuple_ref.value(0).unwrap().to_owned(),
            DataType::Text("Original".into())
        );
    }

    #[test]
    fn test_tuple_17() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_5 = TransactionId::from(5u64);

        let tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Data".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot before the tuple existed
        let snapshot_5 = Snapshot::new(xid_5, xid_5, TransactionId::from(6u64), HashSet::new());

        let result = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_5).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_tuple_18() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_15 = TransactionId::from(15u64);
        let xid_30 = TransactionId::from(30u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("Data".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple.delete(xid_20).unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot was taken before delete so tuple should be visible
        let snapshot_15 = Snapshot::new(xid_15, xid_10, TransactionId::from(16u64), HashSet::new());

        let result = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_15).unwrap();
        assert!(result.is_some());

        // Snapshot taken after delete should not be visible
        let snapshot_30 = Snapshot::new(xid_30, xid_30, TransactionId::from(31u64), HashSet::new());

        let result = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_30).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_tuple_19() {
        let schema = create_single_key_schema();
        let xid_10 = TransactionId::from(10u64);
        let xid_20 = TransactionId::from(20u64);
        let xid_30 = TransactionId::from(30u64);
        let xid_25 = TransactionId::from(25u64);

        let mut tuple = Tuple::new(
            &[
                DataType::Int(1.into()),
                DataType::Text("V0".into()),
                DataType::Boolean(UInt8::TRUE),
                DataType::Double(100.0.into()),
                DataType::Double(5.0.into()),
                DataType::Text("Test".into()),
            ],
            &schema,
            xid_10,
        )
        .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("V1".into()))], xid_20)
            .unwrap();

        tuple
            .add_version(&[(0, DataType::Text("V2".into()))], xid_30)
            .unwrap();

        let buf: OwnedTuple = tuple.into();

        // Snapshot with tx active should not see data modified by it
        let mut active = HashSet::new();
        active.insert(xid_30);

        let snapshot_25 = Snapshot::new(xid_25, xid_10, TransactionId::from(31u64), active);

        let tuple_ref = TupleRef::read_for_snapshot(buf.as_ref(), &schema, &snapshot_25)
            .unwrap()
            .expect("Should find a visible tuple");
        println!("{tuple_ref}");
        assert_eq!(tuple_ref.version(), 1);
        assert_eq!(
            tuple_ref.value(0).unwrap().to_owned(),
            DataType::Text("V1".into())
        );
    }
}
