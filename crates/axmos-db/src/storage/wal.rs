/// This module includes the main data structures that represent Write Ahead Log blocks and records.
use crate::{
    CELL_ALIGNMENT, MAX_PAGE_SIZE, MIN_PAGE_SIZE, bytemuck_slice,
    storage::{
        Allocatable, WalMetadata,
        core::{
            buffer::MemBlock,
            traits::{AvailableSpace, ComposedMetadata, Identifiable},
        },
    },
    types::{BlockId, Lsn, ObjectId, TransactionId},
    writable_layout,
};

const MIN_WAL_BLOCK_SIZE: usize = MIN_PAGE_SIZE;
const MAX_WAL_BLOCK_SIZE: usize = MAX_PAGE_SIZE * 10;

use std::{mem, ptr::NonNull, slice};

/// Module constants
pub(crate) const WAL_RECORD_ALIGNMENT: usize = CELL_ALIGNMENT as usize;
pub(crate) const WAL_BLOCK_SIZE: usize = 4 * 1024 * 10; // 40KB blocks
pub(crate) const WAL_HEADER_SIZE: usize = mem::size_of::<WalHeader>();
pub(crate) const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockHeader>();
pub(crate) const RECORD_HEADER_SIZE: usize = mem::size_of::<RecordHeader>();

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum RecordType {
    Begin = 0x00,
    Commit = 0x01,
    Abort = 0x02,
    End = 0x03,
    Alloc = 0x04,
    Dealloc = 0x05,
    Update = 0x06,
    Delete = 0x07,
    Insert = 0x08,
    CreateTable = 0x09,
}

/// The write ahead log header stores metadata about the file.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub(crate) struct WalHeader {
    pub(crate) global_start_lsn: Option<Lsn>,
    pub(crate) global_last_lsn: Option<Lsn>,
    pub(crate) last_checkpoint_offset: u64,
    pub(crate) total_blocks: u64,
    pub(crate) block_size: u32,
    pub(crate) total_entries: u32,
    pub(crate) last_block_used: u32,
}

/// The block header stores metadata about the blocks themselves.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockHeader {
    pub(crate) block_number: BlockId,
    pub(crate) block_first_lsn: Option<Lsn>,
    pub(crate) block_last_lsn: Option<Lsn>,
    pub(crate) used_bytes: u64,
}

/// Block zero is a special type of block used to store block data and also the file header, therefore its header is special. It is similar in concept as the PageZeroHeader but here I followed a different approach since wal blocks are much more simpler than pages.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockZeroHeader {
    pub(crate) block_header: BlockHeader,
    pub(crate) wal_header: WalHeader,
}

bytemuck_slice!(BlockZeroHeader);

impl BlockHeader {
    // As wal blocks are never deallocated (there is no concept of free block in memory as happens with pages, when we need a new block we simply allocate it), there is no issue in always creating block ids in the header initialization. Blocks are also never modified once they are flushed to disk, which makes sequential access easiar. As we do with pages, we can access the offset of a specific block in the file by computing: [BlockId] *  [block_size].
    fn new(id: BlockId) -> Self {
        Self {
            block_number: id,
            block_first_lsn: None,
            block_last_lsn: None,
            used_bytes: 0,
        }
    }
}

impl Identifiable for BlockHeader {
    type IdType = BlockId;

    fn id(&self) -> Self::IdType {
        self.block_number
    }
}

impl Allocatable for BlockHeader {
    const MAX_SIZE: usize = MAX_WAL_BLOCK_SIZE;
    const MIN_SIZE: usize = MIN_WAL_BLOCK_SIZE;
    fn alloc(id: Self::IdType, _size: usize) -> Self {
        Self::new(id)
    }
}

impl Identifiable for BlockZeroHeader {
    type IdType = BlockId;

    fn id(&self) -> Self::IdType {
        self.block_header.block_number
    }
}

impl Allocatable for BlockZeroHeader {
    const MAX_SIZE: usize = MAX_WAL_BLOCK_SIZE;
    const MIN_SIZE: usize = MIN_WAL_BLOCK_SIZE;

    fn alloc(id: Self::IdType, size: usize) -> Self {
        Self {
            wal_header: WalHeader::new(size as u32),
            block_header: BlockHeader::alloc(id, size),
        }
    }
}

impl WalHeader {
    fn new(block_size: u32) -> Self {
        Self {
            global_start_lsn: None,
            global_last_lsn: None,
            last_checkpoint_offset: 0,
            block_size,
            total_blocks: 1, // Block zero always exists.
            total_entries: 0,
            last_block_used: 0,
        }
    }
}

bytemuck_slice!(WalHeader);
bytemuck_slice!(BlockHeader);

/// Records are similar to cells in the sense that they are self-contained and have a reference version (RecordRef) here.
/// I am doing physiological logging: https://medium.com/@moali314/database-logging-wal-redo-and-undo-mechanisms-58c076fbe36e.
///
/// So I store table ids and tuple ids. The prev_lsn resembles the lsn of the previous log record in the same transaction.
///
/// The header is mainly architected following the ARIES protocol for database recovery: https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics
#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct RecordHeader {
    pub(crate) lsn: Lsn,
    pub(crate) tid: TransactionId,
    pub(crate) prev_lsn: Option<Lsn>,
    pub(crate) object_id: ObjectId,
    pub(crate) total_size: u32,
    pub(crate) undo_len: u16,
    pub(crate) redo_len: u16,
    pub(crate) log_type: RecordType,
    pub(crate) padding: [u8; 7],
}

bytemuck_slice!(RecordHeader);

impl RecordHeader {
    fn new(
        lsn: Lsn,
        tid: TransactionId,
        object_id: ObjectId,
        prev_lsn: Option<Lsn>,
        total_size: u32,
        undo_len: u16,
        redo_len: u16,
        log_type: RecordType,
    ) -> Self {
        Self {
            lsn,
            tid,
            prev_lsn,
            object_id,
            total_size,
            undo_len,
            redo_len,
            log_type,
            padding: [0; 7],
        }
    }
}

/// Resembles an owned record in memory.
/// The payload includes both the undo data and the redo data.
/// As with cells, when creating records from a payload we need to ensure that the data will be aligned after adding the header.
#[derive(Debug)]
pub struct OwnedRecord {
    header: RecordHeader,
    payload: Box<[u8]>,
}

impl OwnedRecord {
    #[inline]
    /// See [OwnedCell::compute_padding]
    pub(crate) fn compute_padded_size(payload_size: usize) -> usize {
        (payload_size + RECORD_HEADER_SIZE).next_multiple_of(WAL_RECORD_ALIGNMENT)
            - RECORD_HEADER_SIZE
    }

    pub(crate) fn new(
        lsn: Lsn,
        tid: TransactionId,
        prev_lsn: Option<Lsn>,
        object_id: ObjectId,
        log_type: RecordType,
        undo_payload: &[u8],
        redo_payload: &[u8],
    ) -> Self {
        let payload_size = Self::compute_padded_size(undo_payload.len() + redo_payload.len());
        let mut payload = vec![0u8; payload_size].into_boxed_slice();
        payload[..undo_payload.len()].copy_from_slice(undo_payload);
        payload[undo_payload.len()..undo_payload.len() + redo_payload.len()]
            .copy_from_slice(redo_payload);

        let header = RecordHeader::new(
            lsn,
            tid,
            object_id,
            prev_lsn,
            (RECORD_HEADER_SIZE + payload_size) as u32,
            undo_payload.len() as u16,
            redo_payload.len() as u16,
            log_type,
        );

        Self { header, payload }
    }

    #[inline]
    /// Return the record id.
    pub fn lsn(&self) -> Lsn {
        self.header.lsn
    }

    #[inline]
    /// Return the header as a reference
    pub fn metadata(&self) -> &RecordHeader {
        &self.header
    }

    #[inline]
    /// Returns the transaction id that created the record
    pub fn tid(&self) -> TransactionId {
        self.header.tid
    }

    #[inline]
    /// Return the record type
    pub fn log_type(&self) -> RecordType {
        self.header.log_type
    }

    #[inline]
    /// Return the full payload (including padding)
    pub fn data(&self) -> &[u8] {
        &self.payload
    }
    #[inline]
    /// Return the undo part of the payload
    pub fn undo_payload(&self) -> &[u8] {
        &self.payload[..self.header.undo_len as usize]
    }

    #[inline]
    /// Return the redo part of the payload
    pub fn redo_payload(&self) -> &[u8] {
        let start = self.header.undo_len as usize;
        let end = start + self.header.redo_len as usize;
        &self.payload[start..end]
    }

    /// Return a reference to this record
    pub fn as_record_ref(&self) -> RecordRef<'_> {
        RecordRef {
            header: &self.header,
            data: self.data(),
        }
    }

    /// Create an owned record from a reference
    pub fn from_ref(reference: RecordRef<'_>) -> Self {
        Self {
            header: *reference.header,
            payload: reference.data.into(),
        }
    }
}

#[derive(Debug)]
pub struct RecordRef<'a> {
    header: &'a RecordHeader,
    data: &'a [u8],
}

impl<'a> RecordRef<'a> {
    /// Build a [RecordRef] from a NonNull pointer to the header.
    pub fn from_raw(ptr: NonNull<RecordHeader>) -> Self {
        unsafe {
            let header = ptr.as_ref();
            let data_ptr = ptr.byte_add(RECORD_HEADER_SIZE).cast::<u8>().as_ptr();
            let payload_size = header.total_size as usize - RECORD_HEADER_SIZE;
            let data = slice::from_raw_parts(data_ptr, payload_size);
            Self { header, data }
        }
    }

    #[inline]
    /// See [OwnedRecord::metadata]
    pub fn metadata(&self) -> &RecordHeader {
        self.header
    }

    #[inline]
    /// See [OwnedRecord::data]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    /// See [OwnedRecord::lsn]
    pub fn lsn(&self) -> Lsn {
        self.header.lsn
    }

    #[inline]
    /// See [OwnedRecord::tid]
    pub fn tid(&self) -> TransactionId {
        self.header.tid
    }

    #[inline]
    /// See [OwnedRecord::log_type]
    pub fn log_type(&self) -> RecordType {
        self.header.log_type
    }

    #[inline]
    /// See [OwnedRecord::undo_payload]
    pub fn undo_payload(&self) -> &[u8] {
        &self.data[..self.header.undo_len as usize]
    }

    #[inline]
    /// See [OwnedRecord::redo_payload]
    pub fn redo_payload(&self) -> &[u8] {
        let start = self.header.undo_len as usize;
        let end = start + self.header.redo_len as usize;
        &self.data[start..end]
    }

    #[inline]
    /// See [OwnedRecord::total_size]
    pub fn total_size(&self) -> usize {
        self.header.total_size as usize
    }

    /// Copies the record ref into an owned buffer.
    pub fn to_owned(&self) -> OwnedRecord {
        OwnedRecord::new(
            self.header.lsn,
            self.header.tid,
            self.header.prev_lsn,
            self.header.object_id,
            self.header.log_type,
            self.undo_payload(),
            self.redo_payload(),
        )
    }
}

writable_layout!(RecordRef<'_>, RecordHeader, header, data, lifetime);
writable_layout!(OwnedRecord, RecordHeader, header, payload);

pub(crate) type BlockZero = MemBlock<BlockZeroHeader>;
pub(crate) type WalBlock = MemBlock<BlockHeader>;

impl AvailableSpace for BlockZero {
    fn available_space(&self) -> usize {
        Self::usable_space(self.capacity())
            .saturating_sub(self.metadata().block_header.used_bytes as usize)
    }
}

impl AvailableSpace for WalBlock {
    fn available_space(&self) -> usize {
        Self::usable_space(self.capacity()).saturating_sub(self.metadata().used_bytes as usize)
    }
}

impl ComposedMetadata for BlockHeader {
    type ItemHeader = RecordHeader;
}

impl ComposedMetadata for BlockZeroHeader {
    type ItemHeader = RecordHeader;
}

impl WalMetadata for BlockHeader {
    fn last_lsn(&self) -> Option<Lsn> {
        self.block_last_lsn
    }

    fn start_lsn(&self) -> Option<Lsn> {
        self.block_first_lsn
    }

    fn set_last_lsn(&mut self, lsn: Lsn) {
        self.block_last_lsn = Some(lsn);
    }

    fn set_start_lsn(&mut self, lsn: Lsn) {
        self.block_first_lsn = Some(lsn);
    }

    fn increment_write_offset(&mut self, added_bytes: usize) {
        self.used_bytes += added_bytes as u64
    }

    fn used_bytes(&self) -> usize {
        self.used_bytes as usize
    }
}

impl WalMetadata for BlockZeroHeader {
    fn last_lsn(&self) -> Option<Lsn> {
        self.block_header.block_last_lsn
    }

    fn start_lsn(&self) -> Option<Lsn> {
        self.block_header.block_first_lsn
    }

    fn set_last_lsn(&mut self, lsn: Lsn) {
        self.block_header.block_last_lsn = Some(lsn);
    }

    fn set_start_lsn(&mut self, lsn: Lsn) {
        self.block_header.block_first_lsn = Some(lsn);
    }

    fn increment_write_offset(&mut self, added_bytes: usize) {
        self.block_header.used_bytes += added_bytes as u64
    }

    fn used_bytes(&self) -> usize {
        self.block_header.used_bytes as usize
    }
}

/// Macro to create WAL (Write-Ahead Logging) records inline.
///
/// # Syntax
///
/// The macro supports different types of records with optional parameters:
///
/// ## Begin Record
/// ```rust no-run
/// record!(begin 123);
/// record!(begin_tid transaction_id);
/// ```
///
/// ## Commit Record
/// ```rust  no-run
/// record!(commit 456);
/// record!(commit 456, prev: Some(lsn));
/// ```
///
/// ## Update Record
/// ```rust no-run
/// record!(update 789, 1, b"old_data", b"new_data");
/// record!(update 789, 1, prev: Some(lsn), b"old", b"new");
/// ```
///
/// ## Insert Record
/// ```rust no-run
/// record!(insert 999, 2, b"data_to_insert");
/// record!(insert 999, 2, prev: Some(lsn), b"data");
/// ```
///
/// ## Delete Record
/// ```rust  no-run
/// record!(delete 111, 3, b"data_to_delete");
/// record!(delete 111, 3, prev: Some(lsn), b"data");
/// ```
///
/// ## Dynamic Record Type (accepts RecordType expression)
/// ```rust no-run
/// let rec_type = RecordType::Update;
/// record!(type: rec_type, tid: 1, oid: 2, undo: b"old", redo: b"new");
/// record!(type: rec_type, tid: 1, oid: 2, prev: Some(lsn), undo: b"old", redo: b"new");
///
/// // With sized payloads (generates vec![0; size])
/// record!(type: rec_type, tid: 1, oid: 2, undo_size: 64, redo_size: 128);
/// ```
#[macro_export]
macro_rules! record {
    // Begin records.
    (begin $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Begin,
            &[],
            &[],
        )
    }};

    (begin_tid $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Begin,
            &[],
            &[],
        )
    }};

    // Commit records
    (commit $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Commit,
            &[],
            &[],
        )
    }};

    (commit $lsn:expr, $tid:expr, prev: $prev_lsn:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Commit,
            &[],
            &[],
        )
    }};

    (commit_tid $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Commit,
            &[],
            &[],
        )
    }};

    (commit_tid $lsn:expr, $tid:expr, prev: $prev_lsn:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            $prev_lsn,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Commit,
            &[],
            &[],
        )
    }};

    // Abort records
    (abort $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Abort,
            &[],
            &[],
        )
    }};

    (abort $lsn:expr, $tid:expr, prev: $prev_lsn:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::Abort,
            &[],
            &[],
        )
    }};

    // End records
    (end $lsn:expr, $tid:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::End,
            &[],
            &[],
        )
    }};

    (end $lsn:expr, $tid:expr, prev: $prev_lsn:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::default(),
            $crate::storage::wal::RecordType::End,
            &[],
            &[],
        )
    }};

    // Update records
    (update $lsn:expr, $tid:expr, $oid:expr, $old:expr, $new:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Update,
            $old,
            $new,
        )
    }};

    (update $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $old:expr, $new:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Update,
            $old,
            $new,
        )
    }};

    (update_tid $lsn:expr, $tid:expr, $oid:expr, $old:expr, $new:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $oid,
            $crate::storage::wal::RecordType::Update,
            $old,
            $new,
        )
    }};

    (update_tid $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $old:expr, $new:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            $prev_lsn,
            $oid,
            $crate::storage::wal::RecordType::Update,
            $old,
            $new,
        )
    }};

    // Insert records
    (insert $lsn:expr, $tid:expr, $oid:expr, $data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Insert,
            &[],
            $data,
        )
    }};

    (insert $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Insert,
            &[],
            $data,
        )
    }};

    (insert_tid $lsn:expr, $tid:expr, $oid:expr, $data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $oid,
            $crate::storage::wal::RecordType::Insert,
            &[],
            $data,
        )
    }};

    (insert_tid $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            $prev_lsn,
            $oid,
            $crate::storage::wal::RecordType::Insert,
            &[],
            $data,
        )
    }};

    // Delete records
    (delete $lsn:expr, $tid:expr, $oid:expr, $old_data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Delete,
            $old_data,
            &[],
        )
    }};

    (delete $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $old_data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::Delete,
            $old_data,
            &[],
        )
    }};

    (delete_tid $lsn:expr, $tid:expr, $oid:expr, $old_data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $oid,
            $crate::storage::wal::RecordType::Delete,
            $old_data,
            &[],
        )
    }};

    (delete_tid $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $old_data:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            $prev_lsn,
            $oid,
            $crate::storage::wal::RecordType::Delete,
            $old_data,
            &[],
        )
    }};

    // Basic dynamic type with data slices
    (rec_type: $rec_type:expr, tid: $tid:expr, lsn: $lsn:expr, oid: $oid:expr, undo: $undo:expr, redo: $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $rec_type,
            $undo,
            $redo,
        )
    }};

    // Dynamic type with prev_lsn and data slices
    (rec_type: $rec_type:expr, lsn: $lsn:expr, tid: $tid:expr, oid: $oid:expr, prev: $prev_lsn:expr, undo: $undo:expr, redo: $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $rec_type,
            $undo,
            $redo,
        )
    }};

    // Dynamic type with sized payloads (generates zero-filled vecs)
    (rec_type: $rec_type:expr,  lsn: $lsn:expr, tid: $tid:expr, oid: $oid:expr, undo_size: $undo_size:expr, redo_size: $redo_size:expr) => {{
        let undo_data = vec![0u8; $undo_size];
        let redo_data = vec![0u8; $redo_size];
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $rec_type,
            &undo_data,
            &redo_data,
        )
    }};

    // Dynamic type with prev_lsn and sized payloads
    (rec_type: $rec_type:expr,  lsn: $lsn:expr, tid: $tid:expr, oid: $oid:expr, prev: $prev_lsn:expr, undo_size: $undo_size:expr, redo_size: $redo_size:expr) => {{
        let undo_data = vec![0u8; $undo_size];
        let redo_data = vec![0u8; $redo_size];
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $rec_type,
            &undo_data,
            &redo_data,
        )
    }};

    // Dynamic type with TransactionId and ObjectId directly
    (rec_type: $rec_type:expr,  lsn: $lsn:expr, tid_raw: $tid:expr, oid_raw: $oid:expr, undo: $undo:expr, redo: $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            None,
            $oid,
            $rec_type,
            $undo,
            $redo,
        )
    }};

    (rec_type: $rec_type:expr,  lsn: $lsn:expr, tid_raw: $tid:expr, oid_raw: $oid:expr, prev: $prev_lsn:expr, undo: $undo:expr, redo: $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $tid,
            $prev_lsn,
            $oid,
            $rec_type,
            $undo,
            $redo,
        )
    }};

    // Static type identifier pattern (e.g., record!(Update 1, 2, b"old", b"new"))
    ($type:ident,  lsn: $lsn:expr, $tid:expr, $oid:expr, $undo:expr, $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            None,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::$type,
            $undo,
            $redo,
        )
    }};

    ($type:ident,  lsn: $lsn:expr, $tid:expr, $oid:expr, prev: $prev_lsn:expr, $undo:expr, $redo:expr) => {{
        $crate::storage::wal::OwnedRecord::new(
            $lsn as $crate::types::Lsn,
            $crate::types::TransactionId::from($tid as u64),
            $prev_lsn,
            $crate::types::ObjectId::from($oid as u64),
            $crate::storage::wal::RecordType::$type,
            $undo,
            $redo,
        )
    }};
}
