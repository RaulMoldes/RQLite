use crate::{
    as_slice, repr_enum,
    storage::buffer::MemBlock,
    types::{BlockId, LogId, ObjectId, TransactionId},
};

use std::{
    io::{self, Error as IoError, ErrorKind},
    mem,
    ptr::NonNull,
    slice,
};

pub const WAL_RECORD_ALIGNMENT: usize = 64;
pub const WAL_BLOCK_SIZE: usize = 4 * 1024 * 10; // 40KB blocks

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

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub(crate) struct WalHeader {
    pub(crate) global_start_lsn: Option<LogId>,
    pub(crate) global_last_lsn: Option<LogId>,
    pub(crate) last_checkpoint_offset: u64,
    pub(crate) block_size: u32,
    pub(crate) total_blocks: u32,
    pub(crate) total_entries: u32,
    pub(crate) last_block_used: u32,
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct BlockHeader {
    pub(crate) block_number: BlockId,
    pub(crate) block_first_lsn: Option<LogId>,
    pub(crate) block_last_lsn: Option<LogId>,
    pub(crate) used_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockZeroHeader {
    pub(crate) block_header: BlockHeader,
    pub(crate) wal_header: WalHeader,
}

impl BlockHeader {
    fn new() -> Self {
        Self {
            block_number: BlockId::new(),
            block_first_lsn: None,
            block_last_lsn: None,
            used_bytes: 0,
        }
    }
}

pub const WAL_HEADER_SIZE: usize = mem::size_of::<WalHeader>();
pub const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockHeader>();

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

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct RecordHeader {
    pub(crate) lsn: LogId,
    pub(crate) tid: TransactionId,
    pub(crate) prev_lsn: LogId,
    pub(crate) object_id: ObjectId,
    pub(crate) total_size: u32,
    pub(crate) undo_len: u16,
    pub(crate) redo_len: u16,
    pub(crate) log_type: RecordType,
    pub(crate) padding: [u8; 7],
}

as_slice!(RecordHeader);

pub const RECORD_HEADER_SIZE: usize = mem::size_of::<RecordHeader>();

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
            prev_lsn: prev_lsn.unwrap_or(LogId::from(0)),
            object_id,
            total_size,
            undo_len,
            redo_len,
            log_type,
            padding: [0; 7],
        }
    }
}

#[derive(Debug)]
pub struct OwnedRecord {
    header: RecordHeader,
    payload: Box<[u8]>,
}

impl OwnedRecord {
    pub fn new(
        tid: TransactionId,
        prev_lsn: Option<LogId>,
        object_id: ObjectId,
        log_type: RecordType,
        undo_payload: &[u8],
        redo_payload: &[u8],
    ) -> Self {
        let payload_size = (undo_payload.len() + redo_payload.len() + RECORD_HEADER_SIZE)
            .next_multiple_of(WAL_RECORD_ALIGNMENT)
            - RECORD_HEADER_SIZE;

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

    pub fn lsn(&self) -> LogId {
        self.header.lsn
    }

    pub fn total_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.payload.len()
    }

    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn undo_data(&self) -> &[u8] {
        &self.payload[..self.header.undo_len as usize]
    }

    pub fn redo_data(&self) -> &[u8] {
        let start = self.header.undo_len as usize;
        let end = start + self.header.redo_len as usize;
        &self.payload[start..end]
    }

    pub fn write_to(&self, buffer: &mut [u8]) -> usize {
        buffer[..RECORD_HEADER_SIZE].copy_from_slice(self.header.as_ref());
        buffer[RECORD_HEADER_SIZE..self.total_size()].copy_from_slice(&self.payload);
        self.total_size()
    }

    pub fn as_record_ref(&self) -> RecordRef<'_> {
        RecordRef {
            header: &self.header,
            data: self.payload(),
        }
    }

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
    pub fn from_raw(ptr: NonNull<RecordHeader>) -> Self {
        unsafe {
            let header = ptr.as_ref();
            let data_ptr = ptr.byte_add(RECORD_HEADER_SIZE).cast::<u8>().as_ptr();
            let payload_size = header.total_size as usize - RECORD_HEADER_SIZE;
            let data = slice::from_raw_parts(data_ptr, payload_size);
            Self { header, data }
        }
    }

    pub fn header(&self) -> &RecordHeader {
        self.header
    }

    pub fn lsn(&self) -> LogId {
        self.header.lsn
    }

    pub fn tid(&self) -> TransactionId {
        self.header.tid
    }

    pub fn log_type(&self) -> RecordType {
        self.header.log_type
    }

    pub fn undo_data(&self) -> &[u8] {
        &self.data[..self.header.undo_len as usize]
    }

    pub fn redo_data(&self) -> &[u8] {
        let start = self.header.undo_len as usize;
        let end = start + self.header.redo_len as usize;
        &self.data[start..end]
    }

    pub fn total_size(&self) -> usize {
        self.header.total_size as usize
    }

    pub fn to_owned(&self) -> OwnedRecord {
        OwnedRecord::new(
            self.header.tid,
            if self.header.prev_lsn == LogId::from(0) {
                None
            } else {
                Some(self.header.prev_lsn)
            },
            self.header.object_id,
            self.header.log_type,
            self.undo_data(),
            self.redo_data(),
        )
    }
}

pub type BlockZero = MemBlock<BlockZeroHeader>;
pub type WalBlock = MemBlock<BlockHeader>;

impl WalBlock {
    pub fn allocate(size: usize) -> Self {
        Self::new(size)
    }

    fn block_number(&self) -> BlockId {
        self.metadata().block_number
    }

    fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    pub fn record(&self, offset: usize) -> RecordRef<'_> {
        RecordRef::from_raw(self.record_at_offset(offset))
    }

    fn record_owned(&self, offset: usize) -> OwnedRecord {
        OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset)))
    }

    pub fn try_push(&mut self, record: OwnedRecord) -> io::Result<LogId> {
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

    pub fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().used_bytes as usize)
    }
}

impl BlockZero {
    pub fn allocate(size: usize) -> Self {
        let mut blk = Self::new(size);
        blk.metadata_mut().wal_header = WalHeader::new(size as u32);
        blk
    }

    fn block_number(&self) -> BlockId {
        self.metadata().block_header.block_number
    }

    fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    pub fn record(&self, offset: usize) -> RecordRef<'_> {
        RecordRef::from_raw(self.record_at_offset(offset))
    }

    fn record_owned(&self, offset: usize) -> OwnedRecord {
        OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset)))
    }

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

    pub fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().block_header.used_bytes as usize)
    }
}
