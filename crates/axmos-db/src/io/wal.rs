use crate::{
    io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
    repr_enum,as_slice,
    storage::{buffer::MemBlock, tuple::OwnedTuple},
    types::{LogId, OBJECT_ZERO, ObjectId, TransactionId},
};

use std::{
    collections::VecDeque,
    fs,
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    mem,
    path::Path,
};

pub const WAL_RECORD_ALIGNMENT: usize = 64;
pub const WAL_BLOCK_SIZE: usize = 4 * 1024 * 10; // 40KB blocks
pub const WAL_READ_AHEAD_BLOCKS: usize = 4;

pub trait Operation {
    fn op_type(&self) -> LogRecordType;
    fn object_id(&self) -> ObjectId;
    fn undo(&self) -> &[u8];
    fn redo(&self) -> &[u8];
}

pub struct Begin;

impl Operation for Begin {
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Begin
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::End
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Commit
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Abort
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Update
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Insert
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
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Delete
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
pub struct LogRecordBuilder {
    tx_id: TransactionId,
    last_lsn: Option<LogId>,
}

impl LogRecordBuilder {
    pub(crate) fn for_transaction(tx_id: TransactionId) -> Self {
        Self {
            tx_id,
            last_lsn: None,
        }
    }

    pub(crate) fn build_rec<O>(&mut self, operation: O) -> OwnedLogRecord
    where
        O: Operation,
    {
        let log = OwnedLogRecord::new(
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

repr_enum!(pub enum LogRecordType: u8 {
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

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub(crate) struct WalHeader {
    pub(crate) start_lsn: LogId,
    pub(crate) last_lsn: LogId,
    pub(crate) last_flushed_offset: u64,
    pub(crate) num_entries: u32,
    pub(crate) block_size: u32,
    pub(crate) current_block_offset: u32, // Offset within current block (for recovery)
    padding: [u8; 28],                    // Pad to 64 bytes
}

pub const WAL_HEADER_SIZE: usize = mem::size_of::<WalHeader>();

impl WalHeader {
    fn new(block_size: u32) -> Self {
        Self {
            start_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            last_flushed_offset: block_size as u64, // First block is header
            num_entries: 0,
            block_size,
            current_block_offset: 0,
            padding: [0; 28],
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= WAL_HEADER_SIZE);
        unsafe { std::ptr::read(bytes.as_ptr() as *const WalHeader) }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const WalHeader as *const u8, WAL_HEADER_SIZE)
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct LogHeader {
    pub(crate) lsn: LogId,
    pub(crate) tid: TransactionId,
    pub(crate) prev_lsn: LogId,
    pub(crate) object_id: ObjectId,
    pub(crate) total_size: u32,
    pub(crate) undo_len: u16,
    pub(crate) redo_len: u16,
    pub(crate) log_type: LogRecordType,
    pub(crate) padding: [u8; 7],
}

as_slice!(LogHeader);

pub const LOG_HEADER_SIZE: usize = mem::size_of::<LogHeader>();

impl LogHeader {
    fn new(
        tid: TransactionId,
        object_id: ObjectId,
        prev_lsn: Option<LogId>,
        total_size: u32,
        undo_len: u16,
        redo_len: u16,
        log_type: LogRecordType,
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
pub struct OwnedLogRecord {
    header: LogHeader,
    payload: Box<[u8]>, // undo bytes followed by redo bytes
}

impl OwnedLogRecord {
    pub fn new(
        tid: TransactionId,
        prev_lsn: Option<LogId>,
        object_id: ObjectId,
        log_type: LogRecordType,
        undo_payload: &[u8],
        redo_payload: &[u8],
    ) -> Self {
        let payload_size = (undo_payload.len() + redo_payload.len() + LOG_HEADER_SIZE)
            .next_multiple_of(WAL_RECORD_ALIGNMENT)
            - LOG_HEADER_SIZE;

        let mut payload = vec![0u8; payload_size].into_boxed_slice();
        payload[..undo_payload.len()].copy_from_slice(undo_payload);
        payload[undo_payload.len()..undo_payload.len() + redo_payload.len()]
            .copy_from_slice(redo_payload);

        let header = LogHeader::new(
            tid,
            object_id,
            prev_lsn,
            (LOG_HEADER_SIZE + payload_size) as u32,
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
        LOG_HEADER_SIZE + self.payload.len()
    }

    pub fn header(&self) -> &LogHeader {
        &self.header
    }

    pub fn payload(&self) -> &[u8]{
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

    /// Write header + payload into a buffer. Returns bytes written.
    pub fn write_to(&self, buffer: &mut [u8]) -> usize {
        let header_bytes= self.header.as_ref();
        buffer[..LOG_HEADER_SIZE].copy_from_slice(header_bytes);
        buffer[LOG_HEADER_SIZE..self.total_size()].copy_from_slice(&self.payload);
        self.total_size()
    }


}

#[derive(Debug)]
pub struct LogRecordRef<'a> {
    header: &'a LogHeader,
    data: &'a [u8],
}

impl<'a> LogRecordRef<'a> {
    pub fn header(&self) -> &LogHeader {
        self.header
    }

    pub fn lsn(&self) -> LogId {
        self.header.lsn
    }

    pub fn tid(&self) -> TransactionId {
        self.header.tid
    }

    pub fn log_type(&self) -> LogRecordType {
        self.header.log_type
    }

    pub fn undo_data(&self) -> &[u8] {
        let start = LOG_HEADER_SIZE;
        let end = start + self.header.undo_len as usize;
        &self.data[start..end]
    }

    pub fn redo_data(&self) -> &[u8] {
        let start = LOG_HEADER_SIZE + self.header.undo_len as usize;
        let end = start + self.header.redo_len as usize;
        &self.data[start..end]
    }

    pub fn total_size(&self) -> usize {
        self.header.total_size as usize
    }
}

#[derive(Debug)]
pub struct WriteAheadLog {
    header: WalHeader,
    current_block: MemBlock<()>,
    current_offset: usize,
    flush_queue: VecDeque<MemBlock<()>>,
    file: DBFile,
    block_size: usize,
}

impl Write for WriteAheadLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.perform_flush()
        //self.file.flush()
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
impl FileOperations for WriteAheadLog {
    fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = DBFile::create(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;

        // WAL block size must be multiple of filesystem block size
        let block_size = WAL_BLOCK_SIZE.next_multiple_of(fs_block_size);

        let header = WalHeader::new(block_size as u32);
        let current_block = MemBlock::new(block_size);

        // Write initial header block
        let mut header_block = MemBlock::<()>::new(block_size);
        header_block.data_mut()[..WAL_HEADER_SIZE].copy_from_slice(header.as_bytes());
        file.write_all(header_block.as_ref())?;
        file.sync_all()?;

        Ok(Self {
            header,
            current_block,
            current_offset: 0,
            flush_queue: VecDeque::new(),
            file,
            block_size,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = DBFile::open(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;
        let block_size = WAL_BLOCK_SIZE.next_multiple_of(fs_block_size);

        // Read header from first block
        let mut header_block = MemBlock::<()>::new(block_size);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(header_block.data_mut())?;
        let header = WalHeader::from_bytes(header_block.data());

        // Use block size from header if available, otherwise use computed
        let block_size = if header.block_size > 0 {
            header.block_size as usize
        } else {
            block_size
        };

        let current_block = MemBlock::new(block_size);

        Ok(Self {
            header,
            current_block,
            current_offset: 0,
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
        self.current_block = MemBlock::new(self.block_size);
        self.current_offset = 0;
        self.flush_queue.clear();

        // Rewrite header
        let mut header_block = MemBlock::<()>::new(self.block_size);
        header_block.data_mut()[..WAL_HEADER_SIZE].copy_from_slice(self.header.as_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(header_block.as_ref())?;
        self.file.sync_all()
    }
}

impl WriteAheadLog {

    fn available_space(&self) -> usize {
        self.block_size.saturating_sub(self.current_offset)
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        let dest = &mut self.current_block.data_mut()[self.current_offset..];
        dest.copy_from_slice(bytes);
        self.current_offset += bytes.len();
    }

    /// Push a log record to the WAL
    pub fn push(&mut self, record: OwnedLogRecord) -> io::Result<()> {
        let record_size = record.total_size();
        let available_space = self.available_space();

        // Not enough available space. Needs rotation, but first we need to copy as much as we can.
        if record_size >= available_space && available_space > 0 {
            let bytes_to_write = std::cmp::min(mem::size_of::<LogHeader>(), available_space);

            // Write the header.
            self.push_bytes(&record.header().as_ref()[..bytes_to_write]);
            // If there is more space, we copy also as much as we can from the data.
            let available_space = self.available_space();
            if available_space > 0 {
                let bytes_to_write = std::cmp::min(record.payload().len(), available_space);
                self.push_bytes(&record.payload()[..bytes_to_write]);
            };

            // Rotate the block once we have written all the data we could in the previous one.
            self.rotate_block()?;

            // Now we copy the remaining in the rotated block.
            self.push_bytes(&record.payload()[bytes_to_write..]);
            self.update_tracking(record.lsn());
            return  Ok(());

        } else if available_space == 0 {
            self.rotate_block()?;
        };

        // Write record to current block
        let dest = &mut self.current_block.data_mut()[self.current_offset..];
        record.write_to(dest);
        self.current_offset += record_size;

        self.update_tracking(record.lsn());
        Ok(())
    }


    fn update_tracking(&mut self, lsn: LogId) {
        // Update header stats
        if self.header.start_lsn == LogId::from(0) {
            self.header.start_lsn = lsn
        }
        self.header.last_lsn = lsn;
        self.header.num_entries += 1;
        self.header.current_block_offset = self.current_offset as u32;

    }



    /// Rotate current block to flush queue and allocate new block
    fn rotate_block(&mut self) -> io::Result<()> {
        if self.current_offset == 0 {
            return Ok(()); // Nothing to rotate
        }

        let full_block = std::mem::replace(&mut self.current_block, MemBlock::new(self.block_size));
        self.flush_queue.push_back(full_block);
        self.current_offset = 0;
        self.header.current_block_offset = 0;

        Ok(())
    }

    /// Flush all pending blocks to disk
    pub fn perform_flush(&mut self) -> io::Result<()> {

        // Flush all queued blocks
        self.file
            .seek(SeekFrom::Start(self.header.last_flushed_offset))?;
        while let Some(block) = self.flush_queue.pop_front() {
            self.file.write_all(block.as_ref())?;
            self.header.last_flushed_offset += self.block_size as u64;
        }

        // Write current block in case it has data:
        if self.current_offset > 0 {
            self.file.write_all(self.current_block.as_ref())?;
        }

        // Sync header AFTER updating last_flushed_offset
        self.sync_header()?;
        self.file.sync_all()
    }

    /// Write header to first block
    fn sync_header(&mut self) -> io::Result<()> {
        // Read current header block
        let mut header_block = MemBlock::<()>::new(self.block_size);
        self.file.seek(SeekFrom::Start(0))?;

        // Try to read existing, but it's okay if file is smaller
        let _ = self.file.read(header_block.data_mut());

        // Overwrite header portion
        header_block.data_mut()[..WAL_HEADER_SIZE].copy_from_slice(self.header.as_bytes());

        // Write back
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(header_block.as_ref())
    }

    /// Create an iterator over all log records
    pub fn iter(&mut self) -> io::Result<WalIterator<'_>> {
        WalIterator::new(
            &mut self.file,
            self.block_size,
            self.header.start_lsn,
            self.header.last_lsn,
            self.header.last_flushed_offset,
            self.header.num_entries,
        )
    }

    /// Get WAL statistics
    pub fn stats(&self) -> WalStats {
        WalStats {
            start_lsn: self.header.start_lsn,
            last_lsn: self.header.last_lsn,
            num_entries: self.header.num_entries,
            flushed_bytes: self.header.last_flushed_offset,
            pending_blocks: self.flush_queue.len(),
            current_block_usage: self.current_offset,
        }
    }
}
#[derive(Debug, Clone)]
pub struct WalStats {
    pub start_lsn: LogId,
    pub last_lsn: LogId,
    pub num_entries: u32,
    pub flushed_bytes: u64,
    pub pending_blocks: usize,
    pub current_block_usage: usize,
}

pub struct WalIterator<'a> {
    file: &'a mut DBFile,
    block_size: usize,
    read_buffer: MemBlock<()>,
    buffer_offset: usize,
    file_offset: u64,
    end_offset: u64,
    start_lsn: LogId,
    last_lsn: LogId,
    entries_read: u32,
    num_entries: u32,
}

impl<'a> WalIterator<'a> {
    fn new(
        file: &'a mut DBFile,
        block_size: usize,
        start_lsn: LogId,
        last_lsn: LogId,
        end_offset: u64,
        num_entries: u32,
    ) -> io::Result<Self> {
        let read_ahead_size = block_size * WAL_READ_AHEAD_BLOCKS;
        let mut read_buffer = MemBlock::<()>::new(read_ahead_size);

        // Start reading after header block
        let file_offset = block_size as u64;

        // Calculate how much data is available
        let available = end_offset.saturating_sub(file_offset);

        if available == 0 {
            // No data to read
            return Ok(Self {
                file,
                block_size,
                read_buffer,
                buffer_offset: 0,
                file_offset,
                end_offset,
                start_lsn,
                last_lsn,
                entries_read: 0,
                num_entries,
            });
        }

        // Read up to read_ahead_size or available, whichever is smaller
        let read_size = std::cmp::min(read_ahead_size, available as usize);

        // Align read_size to block boundary for direct I/O
        let aligned_read_size = read_size.next_multiple_of(block_size);
        let actual_read = std::cmp::min(aligned_read_size, available as usize);

        if actual_read > 0 {
            file.seek(SeekFrom::Start(file_offset))?;
            // Only read what's actually available (aligned)
            let to_read = actual_read.min(read_buffer.size());
            file.read_exact(&mut read_buffer.data_mut()[..to_read])?;
        }

        Ok(Self {
            file,
            block_size,
            read_buffer,
            buffer_offset: 0,
            file_offset,
            end_offset,
            start_lsn,
            last_lsn,
            entries_read: 0,
            num_entries,
        })
    }

    pub fn next_ref(&mut self) -> io::Result<Option<LogRecordRef<'_>>> {
        // Check termination conditions
        if self.num_entries > 0 && self.entries_read >= self.num_entries {
            return Ok(None);
        }

        // Need to reload buffer?
        let buffer_remaining = self.read_buffer.size() - self.buffer_offset;
        if buffer_remaining < LOG_HEADER_SIZE {
            if !self.reload_buffer()? {
                return Ok(None);
            }
        }

        // First pass: check if we have a complete record, reload if needed
        // We do this without holding any borrows
        loop {
            let data = self.read_buffer.data();

            // Check we have enough data for header
            if self.buffer_offset + LOG_HEADER_SIZE > data.len() {
                return Ok(None);
            }

            let header_ptr = data[self.buffer_offset..].as_ptr() as *const LogHeader;
            let total_size = unsafe { (*header_ptr).total_size as usize };
            let lsn = unsafe { (*header_ptr).lsn };

            // Zero LSN indicates end of valid records
            if lsn == LogId::from(0) {
                return Ok(None);
            }

            // Check if full record is in buffer
            if self.buffer_offset + total_size > self.read_buffer.size() {
                // Record spans buffer boundary - need to reload with record at start
                if !self.reload_buffer_at_record()? {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Incomplete log record at end of file",
                    ));
                }
                // Loop again to re-check with new buffer
                continue;
            }

            // Record is complete in buffer, break to return it
            break;
        }

        // Now we know the record is complete, safe to borrow and return
        let data = self.read_buffer.data();
        let header_ptr = data[self.buffer_offset..].as_ptr() as *const LogHeader;
        let header = unsafe { &*header_ptr };

        // Validate LSN is in expected range
        if self.start_lsn != LogId::from(0) && header.lsn < self.start_lsn {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("LSN {} before start_lsn {}", header.lsn, self.start_lsn),
            ));
        }

        let total_size = header.total_size as usize;
        let record_data = &data[self.buffer_offset..self.buffer_offset + total_size];

        // Advance position
        self.buffer_offset += total_size;
        self.entries_read += 1;

        Ok(Some(LogRecordRef {
            header,
            data: record_data,
        }))
    }

    /// Reload buffer with next chunk of data
    fn reload_buffer(&mut self) -> io::Result<bool> {
        let new_file_offset = self.file_offset + self.read_buffer.size() as u64;

        if new_file_offset >= self.end_offset {
            return Ok(false);
        }

        let available = self.end_offset.saturating_sub(new_file_offset);
        if available == 0 {
            return Ok(false);
        }

        let read_size = std::cmp::min(self.read_buffer.size(), available as usize);

        // Align to block size
        let aligned_read = read_size.next_multiple_of(self.block_size);
        let actual_read = std::cmp::min(aligned_read, available as usize);

        if actual_read == 0 {
            return Ok(false);
        }

        // Clear buffer
        self.read_buffer.data_mut().fill(0);

        self.file.seek(SeekFrom::Start(new_file_offset))?;
        self.file
            .read_exact(&mut self.read_buffer.data_mut()[..actual_read])?;

        self.file_offset = new_file_offset;
        self.buffer_offset = 0;

        Ok(true)
    }

    /// Reload buffer so current record starts at buffer beginning
    fn reload_buffer_at_record(&mut self) -> io::Result<bool> {
        // Calculate where current record starts in file
        let record_file_offset = self.file_offset + self.buffer_offset as u64;

        if record_file_offset >= self.end_offset {
            return Ok(false);
        }

        // Align down to block boundary for direct I/O
        let aligned_offset = (record_file_offset / self.block_size as u64) * self.block_size as u64;
        let offset_within_block = (record_file_offset - aligned_offset) as usize;

        let available = self.end_offset.saturating_sub(aligned_offset);
        if available == 0 {
            return Ok(false);
        }

        let read_size = std::cmp::min(self.read_buffer.size(), available as usize);

        if read_size == 0 {
            return Ok(false);
        }

        // Clear and reload
        self.read_buffer.data_mut().fill(0);

        self.file.seek(SeekFrom::Start(aligned_offset))?;
        self.file
            .read_exact(&mut self.read_buffer.data_mut()[..read_size])?;

        self.file_offset = aligned_offset;
        self.buffer_offset = offset_within_block;

        Ok(true)
    }

    /// Process all records with a closure
    pub fn for_each<F>(&mut self, mut f: F) -> io::Result<()>
    where
        F: FnMut(LogRecordRef<'_>) -> io::Result<()>,
    {
        while let Some(record) = self.next_ref()? {
            f(record)?;
        }
        Ok(())
    }

    /// Collect all records (makes copies)
    pub fn collect_all(&mut self) -> io::Result<Vec<OwnedLogRecord>> {
        let mut records = Vec::new();
        while let Some(record) = self.next_ref()? {
            let owned = OwnedLogRecord::new(
                record.tid(),
                Some(record.header().prev_lsn),
                record.header().object_id,
                record.log_type(),
                record.undo_data(),
                record.redo_data(),
            );
            records.push(owned);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod write_ahead_logging_tests {
    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use tempfile::tempdir;

    fn make_test_record(
        lsn_seed: u64,
        tid: u64,
        oid: u64,
        undo_len: usize,
        redo_len: usize,
    ) -> OwnedLogRecord {
        let mut rng = StdRng::seed_from_u64(lsn_seed);
        let undo: Vec<u8> = (0..undo_len).map(|_| rng.random()).collect();
        let redo: Vec<u8> = (0..redo_len).map(|_| rng.random()).collect();

        OwnedLogRecord::new(
            TransactionId::from(tid),
            if lsn_seed > 1 {
                Some(LogId::from(lsn_seed - 1))
            } else {
                None
            },
            ObjectId::from(oid),
            LogRecordType::Update,
            &undo,
            &redo,
        )
    }

    fn verify_record(
        record: &LogRecordRef<'_>,
        expected_seed: u64,
        tid: u64,
        undo_len: usize,
        redo_len: usize,
    ) {
        assert_eq!(record.tid(), TransactionId::from(tid));

        let mut rng = StdRng::seed_from_u64(expected_seed);

        let undo = record.undo_data();
        assert_eq!(undo.len(), undo_len);
        for &byte in undo {
            assert_eq!(byte, rng.random::<u8>());
        }

        let redo = record.redo_data();
        assert_eq!(redo.len(), redo_len);
        for &byte in redo {
            assert_eq!(byte, rng.random::<u8>());
        }
    }

    #[test]
    fn test_wal_basic_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let num_records = 100;
        let undo_len = 50;
        let redo_len = 75;

        // Write phase
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            for i in 1..=num_records {
                let record = make_test_record(i, 1, 1, undo_len, redo_len);
                wal.push(record).unwrap();
            }

            assert_eq!(wal.header.num_entries as u64, num_records);
            wal.flush().unwrap();
        }

        // Read phase
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            assert_eq!(wal.header.num_entries as u64, num_records);

            let mut count = 0u64;
            wal.iter()
                .unwrap()
                .for_each(|record| {
                    count += 1;
                    verify_record(&record, count, 1, undo_len, redo_len);
                    Ok(())
                })
                .unwrap();

            assert_eq!(count, num_records as u64);
        }
    }

    #[test]
    fn test_wal_multiple_flushes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_multi.wal");

        let records_per_batch = 10;
        let num_batches = 5;
        let total_records = records_per_batch * num_batches;

        // Write multiple batches with flushes
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            for batch in 0..num_batches {
                for i in 0..records_per_batch {
                    let record_num = (batch * records_per_batch + i + 1) as u64;
                    let record = make_test_record(record_num, 1, 1, 100, 100);
                    wal.push(record).unwrap();
                }
                wal.flush().unwrap();
            }

            assert_eq!(wal.header.num_entries, total_records as u32);
        }

        // Verify all records
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            assert_eq!(wal.header.num_entries, total_records as u32);

            let mut count = 0u64;
            wal.iter()
                .unwrap()
                .for_each(|record| {
                    count += 1;
                    verify_record(&record, count, 1, 100, 100);
                    Ok(())
                })
                .unwrap();

            assert_eq!(count, total_records as u64);
        }
    }

    #[test]
    fn test_wal_large_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_large.wal");

        // Records that are ~1/4 of block size each
        let record_size = WAL_BLOCK_SIZE / 4 - LOG_HEADER_SIZE - 100;
        let num_records = 20;

        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            for i in 1..=num_records {
                let record = make_test_record(i as u64, 1, 1, record_size / 2, record_size / 2);
                wal.push(record).unwrap();
            }

            wal.flush().unwrap();
        }

        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            let mut count = 0u64;
            wal.iter()
                .unwrap()
                .for_each(|record| {
                    count += 1;
                    verify_record(&record, count, 1, record_size / 2, record_size / 2);
                    Ok(())
                })
                .unwrap();

            assert_eq!(count, num_records as u64);
        }
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_truncate.wal");

        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            for i in 1..=100 {
                let record = make_test_record(i, 1, 1, 50, 50);
                wal.push(record).unwrap();
            }
            wal.flush().unwrap();

            assert_eq!(wal.header.num_entries, 100);

            // Truncate
            wal.truncate().unwrap();

            assert_eq!(wal.header.num_entries, 0);
            assert_eq!(wal.header.start_lsn, LogId::from(0));
        }

        // Verify empty after reopen
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            assert_eq!(wal.header.num_entries, 0);

            let mut count = 0;
            wal.iter()
                .unwrap()
                .for_each(|_| {
                    count += 1;
                    Ok(())
                })
                .unwrap();

            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_wal_record_builder() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_builder.wal");

        {
            let mut wal = WriteAheadLog::create(&path).unwrap();
            let mut builder = LogRecordBuilder::for_transaction(TransactionId::from(42));

            // Build records using operations
            let begin = builder.build_rec(Begin);
            wal.push(begin).unwrap();

            // Note: For real usage you'd have OwnedTuple, using empty for test
            let commit = builder.build_rec(Commit);
            wal.push(commit).unwrap();

            let end = builder.build_rec(End);
            wal.push(end).unwrap();

            wal.flush().unwrap();
        }

        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            let records = wal.iter().unwrap().collect_all().unwrap();

            assert_eq!(records.len(), 3);
            assert_eq!(records[0].header().log_type, LogRecordType::Begin);
            assert_eq!(records[1].header().log_type, LogRecordType::Commit);
            assert_eq!(records[2].header().log_type, LogRecordType::End);

            // Verify transaction ID
            assert_eq!(records[0].header().tid, TransactionId::from(42));
        }
    }

    #[test]
    fn test_wal_cross_block_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_cross.wal");

        // Create records of varying sizes to force cross-block scenarios
        let sizes = [100, 500, 1000, 2000, 5000, 100, 3000];

        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            for (i, &size) in sizes.iter().enumerate() {
                let record = make_test_record((i + 1) as u64, 1, 1, size / 2, size / 2);
                wal.push(record).unwrap();
            }

            wal.flush().unwrap();
        }

        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            let mut count = 0;
            wal.iter()
                .unwrap()
                .for_each(|record| {
                    let expected_size = sizes[count];
                    verify_record(
                        &record,
                        (count + 1) as u64,
                        1,
                        expected_size / 2,
                        expected_size / 2,
                    );
                    count += 1;
                    Ok(())
                })
                .unwrap();

            assert_eq!(count, sizes.len());
        }
    }
}
