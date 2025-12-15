/// This module includes the main data structures that represent Write Ahead Log blocks and records.
use crate::{
    CELL_ALIGNMENT, as_slice, repr_enum,
    storage::buffer::MemBlock,
    types::{BlockId, LogId, ObjectId, TransactionId},
};

use std::{
    io::{self, Error as IoError, ErrorKind},
    mem,
    ptr::NonNull,
    slice,
};

/// Module constants
pub(crate) const WAL_RECORD_ALIGNMENT: usize = CELL_ALIGNMENT as usize;
pub(crate) const WAL_BLOCK_SIZE: usize = 4 * 1024 * 10; // 40KB blocks
pub(crate) const WAL_HEADER_SIZE: usize = mem::size_of::<WalHeader>();
pub(crate) const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockHeader>();
pub(crate) const RECORD_HEADER_SIZE: usize = mem::size_of::<RecordHeader>();

repr_enum!(pub enum RecordType: u8 {
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
});

/// The write ahead log header stores metadata about the file.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub(crate) struct WalHeader {
    pub(crate) global_start_lsn: Option<LogId>,
    pub(crate) global_last_lsn: Option<LogId>,
    pub(crate) last_checkpoint_offset: u64,
    pub(crate) block_size: u32,
    pub(crate) total_blocks: u32,
    pub(crate) total_entries: u32,
    pub(crate) last_block_used: u32,
}

/// The block header stores metadata about the blocks themselves.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockHeader {
    pub(crate) block_number: BlockId,
    pub(crate) block_first_lsn: Option<LogId>,
    pub(crate) block_last_lsn: Option<LogId>,
    pub(crate) used_bytes: u64,
}

/// Block zero is a special type of block used to store block data and also the file header, therefore its header is special. It is similar in concept as the PageZeroHeader but here I followed a different approach since wal blocks are much more simpler than pages.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockZeroHeader {
    pub(crate) block_header: BlockHeader,
    pub(crate) wal_header: WalHeader,
}

impl BlockHeader {
    // As wal blocks are never deallocated (there is no concept of free block in memory as happens with pages, when we need a new block we simply allocate it), there is no issue in always creating block ids in the header initialization. Blocks are also never modified once they are flushed to disk, which makes sequential access easiar. As we do with pages, we can access the offset of a specific block in the file by computing: [BlockId] *  [block_size].
    fn new() -> Self {
        Self {
            block_number: BlockId::new(),
            block_first_lsn: None,
            block_last_lsn: None,
            used_bytes: 0,
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

as_slice!(WalHeader);
as_slice!(BlockHeader);

/// Records are similar to cells in the sense that they are self-contained and have a reference version (RecordRef) here.
/// I am doing physiological logging: https://medium.com/@moali314/database-logging-wal-redo-and-undo-mechanisms-58c076fbe36e.
///
/// So I store table ids and tuple ids. The prev_lsn resembles the lsn of the previous log record in the same transaction.
///
/// The header is mainly architected following the ARIES protocol for database recovery: https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics
#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct RecordHeader {
    pub(crate) lsn: LogId,
    pub(crate) tid: TransactionId,
    pub(crate) prev_lsn: Option<LogId>,
    pub(crate) object_id: ObjectId,
    pub(crate) total_size: u32,
    pub(crate) undo_len: u16,
    pub(crate) redo_len: u16,
    pub(crate) log_type: RecordType,
    pub(crate) padding: [u8; 7],
}

as_slice!(RecordHeader);

impl RecordHeader {
    fn new(
        tid: TransactionId,
        object_id: ObjectId,
        prev_lsn: Option<LogId>,
        total_size: u32,
        undo_len: u16,
        redo_len: u16,
        log_type: RecordType,
    ) -> Self {
        Self {
            lsn: LogId::new(),
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
        tid: TransactionId,
        prev_lsn: Option<LogId>,
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
    pub fn lsn(&self) -> LogId {
        self.header.lsn
    }

    #[inline]
    /// Return the total size of the record (including header)
    pub fn total_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.payload.len()
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

    /// Write the record to a mutable slice
    pub fn write_to(&self, buffer: &mut [u8]) -> usize {
        buffer[..RECORD_HEADER_SIZE].copy_from_slice(self.header.as_ref());
        buffer[RECORD_HEADER_SIZE..self.total_size()].copy_from_slice(&self.payload);
        self.total_size()
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
    pub fn lsn(&self) -> LogId {
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
            self.header.tid,
            self.header.prev_lsn,
            self.header.object_id,
            self.header.log_type,
            self.undo_payload(),
            self.redo_payload(),
        )
    }
}

pub(crate) type BlockZero = MemBlock<BlockZeroHeader>;
pub(crate) type WalBlock = MemBlock<BlockHeader>;

/// Standard wal block (all of them but block zero)
impl WalBlock {
    /// Allocates a new Wal Block of the given size
    pub(crate) fn alloc(size: usize) -> Self {
        Self::new(size)
    }

    #[inline]
    /// Return the block number of this block
    fn block_number(&self) -> BlockId {
        self.metadata().block_number
    }

    /// Get a pointer to a record at a given offset
    ///
    /// # SAFETY
    /// The caller must ensure that the offset stays valid (there is an actual record at that poistion in memory).
    unsafe fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    /// Reconstruct a record view at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    pub(crate) fn record(&self, offset: usize) -> RecordRef<'_> {
        unsafe { RecordRef::from_raw(self.record_at_offset(offset)) }
    }

    /// Reconstruct an owned record at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    pub(crate) fn record_owned(&self, offset: usize) -> OwnedRecord {
        unsafe { OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset))) }
    }

    /// Attempts to push a record at the end of the block.
    /// If the record does not fit, returns an error.
    pub(crate) fn try_push(&mut self, record: OwnedRecord) -> io::Result<LogId> {
        let record_size = record.total_size();

        if self.available_space() < record_size {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Data too large for this block.",
            ));
        };

        let lsn = record.lsn();
        let offset = self.metadata().used_bytes as usize;
        let dest = &mut self.data_mut()[offset..];

        record.write_to(dest);
        self.metadata_mut().used_bytes += record_size as u64;

        if self.metadata().block_first_lsn.is_none() {
            self.metadata_mut().block_first_lsn = Some(lsn);
        };
        self.metadata_mut().block_last_lsn = Some(lsn);

        Ok(lsn)
    }

    /// Utility to compute the available space in the block.
    pub(crate) fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().used_bytes as usize)
    }
}

impl BlockZero {
    // Allocates a new Wal Block of the given size
    pub(crate) fn alloc(size: usize) -> Self {
        Self::new(size)
    }

    #[inline]
    /// Return the block number of this block
    fn block_number(&self) -> BlockId {
        self.metadata().block_header.block_number
    }

    /// Get a pointer to a record at a given offset
    ///
    /// # SAFETY
    /// The caller must ensure that the offset stays valid (there is an actual record at that poistion in memory).
    unsafe fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    /// Reconstruct a record view at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    pub(crate) fn record(&self, offset: usize) -> RecordRef<'_> {
        unsafe { RecordRef::from_raw(self.record_at_offset(offset)) }
    }

    /// Reconstruct an owned record at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    pub(crate) fn record_owned(&self, offset: usize) -> OwnedRecord {
        unsafe { OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset))) }
    }

    /// Attempts to push a record at the end of the block.
    /// If the record does not fit, returns an error.
    pub fn try_push(&mut self, record: OwnedRecord) -> io::Result<LogId> {
        let record_size = record.total_size();

        if self.available_space() < record_size {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Data too large for this block.",
            ));
        };

        let lsn = record.lsn();
        let offset = self.metadata().block_header.used_bytes as usize;
        let dest = &mut self.data_mut()[offset..];

        record.write_to(dest);
        self.metadata_mut().block_header.used_bytes += record_size as u64;

        if self.metadata().block_header.block_first_lsn.is_none() {
            self.metadata_mut().block_header.block_first_lsn = Some(lsn);
        };
        self.metadata_mut().block_header.block_last_lsn = Some(lsn);

        // Wal header gets updated in [`WriteAheadLog::push`]

        Ok(lsn)
    }

    /// Utility to compute the available space in the block.
    pub(crate) fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().block_header.used_bytes as usize)
    }
}
