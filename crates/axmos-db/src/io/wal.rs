use crate::{
    as_slice,
    io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
    repr_enum,
    storage::{buffer::MemBlock, tuple::OwnedTuple},
    types::{BLOCK_ZERO, BlockId, LogId, OBJECT_ZERO, ObjectId, TransactionId, initialize_block_count},
};

use std::{
    collections::VecDeque,
    fs,
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    mem,
    path::Path,
    ptr::{self, NonNull},
    slice,
};

pub const WAL_RECORD_ALIGNMENT: usize = 64;
pub const WAL_BLOCK_SIZE: usize = 4 * 1024 * 10; // 40KB blocks

pub trait Operation {
    fn op_type(&self) -> RecordType;
    fn object_id(&self) -> ObjectId;
    fn undo(&self) -> &[u8];
    fn redo(&self) -> &[u8];
}

pub struct Begin;

impl Operation for Begin {
    fn op_type(&self) -> RecordType {
        RecordType::Begin
    }
    fn object_id(&self) -> ObjectId {
        OBJECT_ZERO
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
}

pub struct End;

impl Operation for End {
    fn op_type(&self) -> RecordType {
        RecordType::End
    }
    fn object_id(&self) -> ObjectId {
        OBJECT_ZERO
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
}

pub struct Commit;

impl Operation for Commit {
    fn op_type(&self) -> RecordType {
        RecordType::Commit
    }
    fn object_id(&self) -> ObjectId {
        OBJECT_ZERO
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
}

pub struct Abort;

impl Operation for Abort {
    fn op_type(&self) -> RecordType {
        RecordType::Abort
    }
    fn object_id(&self) -> ObjectId {
        OBJECT_ZERO
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
}

#[repr(C)]
pub struct Update {
    oid: ObjectId,
    old: OwnedTuple,
    new: OwnedTuple,
}

impl Update {
    pub(crate) fn new(oid: ObjectId, old: OwnedTuple, new: OwnedTuple) -> Self {
        Self { oid, old, new }
    }
}

impl Operation for Update {
    fn op_type(&self) -> RecordType {
        RecordType::Update
    }
    fn object_id(&self) -> ObjectId {
        self.oid
    }
    fn redo(&self) -> &[u8] {
        self.new.as_ref()
    }
    fn undo(&self) -> &[u8] {
        self.old.as_ref()
    }
}

#[repr(C)]
pub struct Insert {
    oid: ObjectId,
    new: OwnedTuple,
}

impl Insert {
    pub(crate) fn new(oid: ObjectId, new: OwnedTuple) -> Self {
        Self { oid, new }
    }
}

impl Operation for Insert {
    fn op_type(&self) -> RecordType {
        RecordType::Insert
    }
    fn object_id(&self) -> ObjectId {
        self.oid
    }
    fn redo(&self) -> &[u8] {
        self.new.as_ref()
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
}

#[repr(C)]
pub struct Delete {
    oid: ObjectId,
    old: OwnedTuple,
}

impl Delete {
    pub(crate) fn new(oid: ObjectId, old: OwnedTuple) -> Self {
        Self { oid, old }
    }
}

impl Operation for Delete {
    fn op_type(&self) -> RecordType {
        RecordType::Delete
    }
    fn object_id(&self) -> ObjectId {
        self.oid
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
    fn undo(&self) -> &[u8] {
        self.old.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RecordBuilder {
    tx_id: TransactionId,
    last_lsn: Option<LogId>,
}

impl RecordBuilder {
    pub(crate) fn for_transaction(tx_id: TransactionId) -> Self {
        Self {
            tx_id,
            last_lsn: None,
        }
    }

    pub(crate) fn build_rec<O>(&mut self, operation: O) -> OwnedRecord
    where
        O: Operation,
    {
        let log = OwnedRecord::new(
            self.tx_id,
            self.last_lsn,
            operation.object_id(),
            operation.op_type(),
            operation.undo(),
            operation.redo(),
        );
        self.last_lsn = Some(log.lsn());
        log
    }
}

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
#[repr(C, align(64))]
pub struct WalHeader {
    pub(crate) block_number: BlockId,

    pub(crate) first_lsn: LogId,
    pub(crate) last_lsn: LogId,

    pub(crate) global_start_lsn: LogId,
    pub(crate) global_last_lsn: LogId,

    pub(crate) used_bytes: u32,
    pub(crate) num_records: u32,

    pub(crate) block_size: u32,
    pub(crate) total_blocks: u32,
    pub(crate) total_entries: u32,

    pub(crate) last_block_used: u32,
}

pub const WAL_HEADER_SIZE: usize = mem::size_of::<WalHeader>();

impl WalHeader {
    fn new(block_size: u32) -> Self {
        Self {
            block_number: BLOCK_ZERO,
            block_size,
            total_blocks: 1,
            total_entries: 0,
            first_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            global_start_lsn: LogId::from(0),
            global_last_lsn: LogId::from(0),
            last_block_used: 0,
            num_records: 0,
            used_bytes: 0,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const WalHeader as *const u8, WAL_HEADER_SIZE) }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= WAL_HEADER_SIZE);
        unsafe { ptr::read(bytes.as_ptr() as *const WalHeader) }
    }
}

as_slice!(WalHeader);

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct BlockHeader {
    pub(crate) first_lsn: LogId,
    pub(crate) last_lsn: LogId,
    pub(crate) block_number: BlockId,
    pub(crate) used_bytes: u32,
    pub(crate) num_records: u32,

    pub(crate) padding: [u8; 24],
}

pub const WAL_BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockHeader>();

impl BlockHeader {
    fn new() -> Self {
        Self {
            block_number: BlockId::new(),
            used_bytes: 0,
            num_records: 0,
            first_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            padding: [0; 24],
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                self as *const BlockHeader as *const u8,
                WAL_BLOCK_HEADER_SIZE,
            )
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= WAL_BLOCK_HEADER_SIZE);
        unsafe { ptr::read(bytes.as_ptr() as *const BlockHeader) }
    }
}

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
            let data = slice::from_raw_parts(data_ptr, header.total_size as usize);
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
        let start = RECORD_HEADER_SIZE;
        let end = start + self.header.undo_len as usize;
        &self.data[start..end]
    }

    pub fn redo_data(&self) -> &[u8] {
        let start = RECORD_HEADER_SIZE + self.header.undo_len as usize;
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

type WalBlock = MemBlock<BlockHeader>;
type BlockZero = MemBlock<WalHeader>;

impl WalBlock {
    pub(crate) fn id(&self) -> BlockId {
        self.metadata().block_number
    }

    fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    fn record(&self, offset: usize) -> RecordRef<'_> {
        RecordRef::from_raw(self.record_at_offset(offset))
    }

    fn record_owned(&self, offset: usize) -> OwnedRecord {
        OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset)))
    }

    fn try_push(&mut self, record: OwnedRecord) -> io::Result<LogId> {
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
        self.metadata_mut().used_bytes += record_size as u32;

        if self.metadata().first_lsn == LogId::from(0) {
            self.metadata_mut().first_lsn = lsn;
        };
        self.metadata_mut().last_lsn = lsn;
        self.metadata_mut().num_records += 1;

        Ok(lsn)
    }

    fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().used_bytes as usize)
    }
}

impl BlockZero {
    pub(crate) fn id(&self) -> BlockId {
        BLOCK_ZERO
    }

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
        self.metadata_mut().used_bytes += record_size as u32;

        if self.metadata().global_start_lsn == LogId::from(0) {
            self.metadata_mut().global_start_lsn = lsn;
            self.metadata_mut().first_lsn = lsn;
        };
        self.metadata_mut().last_lsn = lsn;
        self.metadata_mut().global_last_lsn = lsn;
        self.metadata_mut().num_records += 1;
        self.metadata_mut().total_entries += 1;

        Ok(lsn)
    }

    fn available_space(&self) -> usize {
        self.capacity()
            .saturating_sub(self.metadata().used_bytes as usize)
    }


    fn record_at_offset(&self, offset: usize) -> NonNull<RecordHeader> {
        unsafe { self.data.byte_add(offset).cast() }
    }

    fn record(&self, offset: usize) -> RecordRef<'_> {
        RecordRef::from_raw(self.record_at_offset(offset))
    }

    fn record_owned(&self, offset: usize) -> OwnedRecord {
        OwnedRecord::from_ref(RecordRef::from_raw(self.record_at_offset(offset)))
    }
}

#[derive(Debug)]
enum DynBlock {
    Zero(BlockZero),
    Other(WalBlock),
}

impl DynBlock {

    pub fn used_bytes(&self) -> usize {
        match self {
            DynBlock::Zero(b) => b.metadata().used_bytes as usize,
            DynBlock::Other(b) => b.metadata().used_bytes as usize,
        }
    }

    pub fn id(&self) -> BlockId {
        match self {
            DynBlock::Zero(b) => b.id(),
            DynBlock::Other(b) => b.id(),
        }
    }

    pub fn available_space(&self) -> usize {
        match self {
            DynBlock::Zero(b) => b.available_space(),
            DynBlock::Other(b) => b.available_space(),
        }
    }

    pub fn try_push(&mut self, record: OwnedRecord) -> io::Result<LogId> {
        match self {
            DynBlock::Zero(b) => b.try_push(record),
            DynBlock::Other(b) => b.try_push(record),
        }
    }


    pub fn record_owned(&self, offset: usize) -> OwnedRecord {
        match self {
            DynBlock::Zero(b) => b.record_owned(offset),
            DynBlock::Other(b) => b.record_owned(offset)
        }
    }


    pub fn record(&self, offset: usize) -> RecordRef<'_> {
        match self {
            DynBlock::Zero(b) => b.record(offset),
            DynBlock::Other(b) => b.record(offset)
        }
    }
}

#[derive(Debug)]
pub struct WriteAheadLog {
    header: WalHeader,
    current_block: DynBlock,
    flush_queue: VecDeque<DynBlock>,
    file: DBFile,
    block_size: usize,
}

impl FileOperations for WriteAheadLog {
    fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = DBFile::create(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;
        let block_size = WAL_BLOCK_SIZE.next_multiple_of(fs_block_size);
        let zero = BlockZero::new(block_size);
        let header = WalHeader::from_bytes(zero.metadata().as_ref());
        let current_block = DynBlock::Zero(zero);
        initialize_block_count(1); // Initialize the in-memory block counter.

        Ok(Self {
            header,
            current_block,
            flush_queue: VecDeque::new(),
            file,
            block_size,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = DBFile::open(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;


        // Read block 0 (global header)
        let mut tmp_header_buf: MemBlock<()> = MemBlock::new(fs_block_size);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(tmp_header_buf.as_mut())?;

        let header = WalHeader::from_bytes(tmp_header_buf.as_ref());
        let block_size = header.block_size as usize;
        let block_count = header.total_blocks as u64;
        initialize_block_count(block_count);

        // Load the lnext block
        let current_block = DynBlock::Other(WalBlock::new(block_size));



        Ok(Self {
            header,
            current_block,
            flush_queue: VecDeque::new(),
            file,
            block_size,
        })
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        fs::remove_file(&path)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.truncate()?;
        self.header = WalHeader::new(self.block_size as u32);
        self.current_block = DynBlock::Zero(BlockZero::new(self.block_size));
        self.flush_queue.clear();
        Ok(())
    }
}

impl Write for WriteAheadLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.perform_flush()
    }
}

impl Read for WriteAheadLog {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for WriteAheadLog {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl WriteAheadLog {
    /// Maximum record size that can fit in a single block
    pub fn max_record_size(&self) -> usize {
        self.block_size - WAL_BLOCK_HEADER_SIZE
    }

    pub fn push(&mut self, record: OwnedRecord) -> io::Result<()> {
        let record_size = record.total_size();

        // Check if record can ever fit in a block
        if record_size > self.max_record_size() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Record size {} exceeds maximum block capacity {}",
                    record_size,
                    self.max_record_size()
                ),
            ));
        }

        // If record doesn't fit in current block, rotate
        if !self.current_block.available_space() <= record_size {
            self.rotate_block()?;
        }

        // Update global stats
        let lsn = record.lsn();
        if self.header.global_start_lsn == LogId::from(0) {
            self.header.global_start_lsn = lsn;
            self.header.first_lsn = lsn;
        }
        self.header.last_lsn = lsn;
        self.header.global_last_lsn = lsn;
        self.header.total_entries += 1;

        // Write record to current block
        self.current_block.try_push(record)?;

        Ok(())
    }

    fn write_header(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0u64))?;
        self.file.write_all(self.header.as_ref())?;
        Ok(())
    }

    fn rotate_block(&mut self) -> io::Result<()> {
        let new_block = DynBlock::Other(WalBlock::new(self.block_size));
        let full_block = mem::replace(&mut self.current_block, new_block);
        self.flush_queue.push_back(full_block);

        Ok(())
    }

    pub fn perform_flush(&mut self) -> io::Result<()> {

        let blocks_already_flushed = self.header.total_blocks;
        let mut write_offset = self.block_size as u64 * (blocks_already_flushed as u64 + 1);



        // Flush queued blocks
        self.file.seek(SeekFrom::Start(write_offset))?;
        while let Some(block) = self.flush_queue.pop_front() {

            match block {
                DynBlock::Zero(b) => {
                    assert_eq!(write_offset, 0, "Invalid write offset for block zero");
                    self.file.write_all(b.as_ref())?;
                },
                DynBlock::Other(b) => {
                    self.file.write_all(b.as_ref())?;
                }
            }
            self.header.total_blocks += 1;
            write_offset += self.block_size as u64;
        }

        // Flush current block if it has data
        if self.current_block.used_bytes() > 0 {

            match &self.current_block {
                DynBlock::Zero(b) => {
                    assert_eq!(write_offset, 0, "Invalid write offset for block zero");
                    self.file.write_all(b.as_ref())?;
                },
                DynBlock::Other(b) => {
                    self.file.write_all(b.as_ref())?;
                }
            }

            self.header.total_blocks += 1;
            self.header.last_block_used = self.current_block.used_bytes() as u32;
        }

        // Write global header to block 0
        self.write_header()?;

        self.file.sync_all()
    }


    pub fn stats(&self) -> WalStats {
        WalStats {
            start_lsn: self.header.global_start_lsn,
            last_lsn: self.header.last_lsn,
            total_entries: self.header.total_entries,
            total_blocks: self.header.total_blocks,
            pending_blocks: self.flush_queue.len(),
            block_size: self.block_size,
        }
    }
}


#[derive(Debug, Clone)]
pub struct WalStats {
    pub start_lsn: LogId,
    pub last_lsn: LogId,
    pub total_entries: u32,
    pub total_blocks: u32,
    pub pending_blocks: usize,
    pub block_size: usize,
}



pub struct  WalReader<'a> {
    file: &'a mut DBFile,
    block_queue: Vec<DynBlock>,
    read_ahead_size: usize,
    current_block_offset: usize,
    current_block_index: usize,
    file_offset: u64,
    last_flushed_offset: u64,
    block_size: u32
}





impl<'a> WalReader<'a> {

    fn new(file: &'a mut DBFile, read_ahead_size: usize, block_size: usize, last_flushed_offset: usize) -> io::Result<Self> {
        let read_ahead_size = read_ahead_size.next_multiple_of(block_size).max(last_flushed_offset);
        let num_blocks = read_ahead_size /  block_size;

        let mut buffer = Vec::with_capacity(num_blocks);
        let mut block_zero = BlockZero::new(block_size);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(block_zero.as_mut())?;
        buffer.push(DynBlock::Zero(block_zero));
        let mut file_offset: u64 = block_size as u64;

        while buffer.len() < num_blocks {
            let mut block = WalBlock::new(block_size);
            file.seek(SeekFrom::Start(file_offset))?;
            file.read_exact(block.as_mut())?;
            buffer.push(DynBlock::Other(block));
            file_offset += block_size as u64;
        };


        Ok(Self { file, block_queue: buffer, read_ahead_size, current_block_offset: 0, file_offset, current_block_index: 0, block_size: block_size as u32, last_flushed_offset: last_flushed_offset as u64 })


    }

    fn reload_from(&mut self, offset: u64, num_blocks: usize) -> io::Result<bool>{
        let last_load = self.last_flushed_offset.saturating_sub(self.block_size as u64);
        if self.file_offset >= last_load {
            return Ok(false);
        }

        while self.block_queue.len() < num_blocks && self.file_offset <= last_load {
            let mut block = WalBlock::new(self.block_size as usize);
            self.file.seek(SeekFrom::Start(self.file_offset))?;
            self.file.read_exact(block.as_mut())?;
            self.block_queue.push(DynBlock::Other(block));
            self.file_offset += self.block_size as u64;
        };
        self.current_block_index = 0;
        Ok(true)
    }

    fn next_ref(&mut self) -> io::Result<Option<RecordRef<'_>>> {

        if self.current_block_offset >= self.block_queue[self.current_block_index].used_bytes() {
            self.current_block_index +=1;
            self.current_block_offset = 0;
        };

        if self.current_block_index >= self.block_queue.len()  {
            let num_blocks = self.read_ahead_size /  self.block_size as usize;
            if !self.reload_from(self.file_offset, num_blocks)?{
                return Ok(None);
            };
        };

        let record = self.block_queue[self.current_block_index].record(self.current_block_offset);
        self.current_block_offset += record.total_size() as usize;
        Ok(Some(record))
    }
}
