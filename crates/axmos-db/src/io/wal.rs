use crate::PAGE_ZERO;
use crate::io::MemBuffer;
use crate::io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize};
use crate::storage::buffer::BufferWithMetadata;
use crate::storage::page::MemPage;
use crate::types::{LogId, TxId};
use std::fs::{self};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr::NonNull;

use crate::types::PageId;

pub const WAL_RECORD_ALIGNMENT: usize = 64;

pub trait Operation {
    fn op_type(&self) -> LogRecordType;
    fn page_nb(&self) -> PageId;
    fn undo(&self) -> &[u8];
    fn redo(&self) -> &[u8];
}

#[repr(transparent)]
pub struct PageAllocation(MemPage);

impl PageAllocation {
    pub fn new(page: MemPage) -> Self {
        Self(page)
    }
}

impl Operation for PageAllocation {
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Alloc
    }

    fn page_nb(&self) -> PageId {
        self.0.page_number()
    }

    fn redo(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn undo(&self) -> &[u8] {
        &[]
    }
}

#[repr(transparent)]
pub struct PageDeallocation(MemPage);

impl PageDeallocation {
    pub fn new(page: MemPage) -> Self {
        Self(page)
    }
}

impl Operation for PageDeallocation {
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Dealloc
    }

    fn page_nb(&self) -> PageId {
        self.0.page_number()
    }

    fn undo(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn redo(&self) -> &[u8] {
        &[]
    }
}

pub struct Begin;

impl Operation for Begin {
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Begin
    }

    fn page_nb(&self) -> PageId {
        PAGE_ZERO
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

    fn page_nb(&self) -> PageId {
        PAGE_ZERO
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

    fn page_nb(&self) -> PageId {
        PAGE_ZERO
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

    fn page_nb(&self) -> PageId {
        PAGE_ZERO
    }

    fn redo(&self) -> &[u8] {
        &[]
    }

    fn undo(&self) -> &[u8] {
        &[]
    }
}

#[repr(C)]
pub struct PageOverwrite {
    old_content: MemPage,
    new_content: MemPage,
}

impl PageOverwrite {
    pub fn new(old_content: MemPage, new_content: MemPage) -> Self {
        Self {
            old_content,
            new_content,
        }
    }
}

impl Operation for PageOverwrite {
    fn op_type(&self) -> LogRecordType {
        LogRecordType::Overwrite
    }

    fn page_nb(&self) -> PageId {
        self.old_content.page_number()
    }

    fn redo(&self) -> &[u8] {
        self.new_content.as_ref()
    }

    fn undo(&self) -> &[u8] {
        self.old_content.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct LogRecordBuilder {
    tx_id: TxId,
    last_lsn: Option<LogId>,
}

impl LogRecordBuilder {
    pub fn for_transaction(tx_id: TxId) -> Self {
        Self {
            tx_id,
            last_lsn: None,
        }
    }

    pub fn build_rec<O>(&mut self, log_record_type: LogRecordType, operation: O) -> LogRecord
    where
        O: Operation,
    {
        let log = LogRecord::new(
            self.tx_id,
            self.last_lsn,
            operation.page_nb(),
            operation.op_type(),
            operation.redo(),
            operation.undo(),
        );

        self.last_lsn = Some(log.lsn());
        log
    }
}

// Log type representation.
// Redo logs store the database state after the transaction.
// Used for recovery after failures and eventual flushing to disk.
// Undo logs store the previous database state before the transaction started.
// Used for rollback of changes.
crate::repr_enum!(pub enum LogRecordType: u8 {
    Begin = 0x00,
    Commit = 0x01,
    Abort = 0x02,
    End = 0x03, // Signifies end of commit or abort
    Alloc = 0x04,
    Dealloc = 0x05,
    // For now the only supported op types are updating the full page and updating the metadata. For efficiency we might want to add more granularity here but it is not a priority right now.
    Overwrite = 0x06,
    OverwriteMetadata = 0x07

});

#[derive(Debug)]
#[repr(C, align(64))]
pub(crate) struct WalHeader {
    start_lsn: LogId,
    last_lsn: LogId,
    last_flushed_offset: u64,
    padding: u32,        // Padding at the end of the journal.
    journal_cursor: u32, // Pointer to where the journal starts in the file.
    num_entries: u32,
    blk_size: u32,
}

impl WalHeader {
    fn new(blk_size: u32) -> Self {
        Self {
            start_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            last_flushed_offset: 0,
            journal_cursor: 0,
            padding: 0,
            num_entries: 0,
            blk_size,
        }
    }

    // SAFETY:
    // As long as the bytes are a valid representation of th header this is safe.
    fn from_buf(bytes: &[u8]) -> Self {
        unsafe { std::ptr::read(bytes[..std::mem::size_of::<Self>()].as_ptr() as *const WalHeader) }
    }
}

impl AsRef<[u8]> for WalHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const WalHeader) as *const u8,
                std::mem::size_of::<WalHeader>(),
            )
        }
    }
}

#[repr(C, align(64))]
pub struct LogHeader {
    /// Auto-incrementing log sequence number
    lsn: LogId,
    txid: TxId, // Id of the current transaction
    /// Log id of the previous LSN of the current transaction.
    prev_lsn: LogId,
    page_id: PageId,
    total_size: u32,
    undo_offset: u16,
    redo_offset: u16,
    log_type: LogRecordType,
    padding: u8,
}

impl LogHeader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        txid: TxId,
        page_id: PageId,
        prev_lsn: Option<LogId>,
        total_size: u32,
        redo_offset: u16,
        undo_offset: u16,
        log_type: LogRecordType,
        padding: u8,
    ) -> Self {
        Self {
            lsn: LogId::new(),
            txid,
            prev_lsn: prev_lsn.unwrap_or(LogId::from(0)),
            page_id,
            total_size,
            undo_offset,
            redo_offset,
            log_type,
            padding,
        }
    }

    fn from_buf(bytes: &[u8]) -> Self {
        unsafe {
            std::ptr::read(bytes[..std::mem::size_of::<LogHeader>()].as_ptr() as *const LogHeader)
        }
    }
}

pub type LogRecord = BufferWithMetadata<LogHeader>;

impl LogRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        txid: TxId,
        prev_lsn: Option<LogId>,
        page_id: PageId,
        log_type: LogRecordType,
        redo_payload: &[u8],
        undo_payload: &[u8],
    ) -> Self {
        let size = (redo_payload.len() + undo_payload.len() + std::mem::size_of::<LogHeader>())
            .next_multiple_of(WAL_RECORD_ALIGNMENT);

        // Align payload size to WAL_RECORD_ALIGNMENT
        let payload_size = size - std::mem::size_of::<LogHeader>();
        let padding = payload_size - redo_payload.len() - undo_payload.len();
        let undo_offset = undo_payload.len() as u16;
        let redo_offset = undo_offset + redo_payload.len() as u16;

        // Create buffer with total size = header + aligned payload
        let mut buffer = BufferWithMetadata::<LogHeader>::with_capacity(
            size, // usable space for data
            WAL_RECORD_ALIGNMENT,
        );

        // Initialize header
        let header = LogHeader::new(
            txid,
            page_id,
            prev_lsn,
            size as u32,
            redo_offset,
            undo_offset,
            log_type,
            padding as u8,
        );

        *buffer.metadata_mut() = header;
        // Copy payload and update length
        buffer.push_bytes(undo_payload);
        buffer.push_bytes(redo_payload);

        buffer
    }

    fn lsn(&self) -> LogId {
        self.metadata().lsn
    }

    fn content_at(&self, offset: u16) -> NonNull<[u8]> {
        unsafe { self.data.byte_add(offset as usize) }
    }

    fn redo_content(&self) -> NonNull<[u8]> {
        self.content_at(self.metadata().redo_offset)
    }

    fn undo_content(&self) -> NonNull<[u8]> {
        self.content_at(self.metadata().undo_offset)
    }

    /// Get the redo payload as a slice
    pub(crate) fn redo_data(&self) -> &[u8] {
        let start = self.metadata().undo_offset as usize;
        let end = self.metadata().redo_offset as usize;
        unsafe {
            std::slice::from_raw_parts(self.data.byte_add(start).as_ptr() as *const u8, end - start)
        }
    }

    /// Get the undo payload as a slice
    pub(crate) fn undo_data(&self) -> &[u8] {
        let len = self.metadata().undo_offset as usize;
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const u8, len) }
    }
}

#[derive(Debug)]
pub struct WriteAheadLog {
    header: WalHeader,
    journal: MemBuffer, // in memory write buffer
    file: DBFile,
}

impl FileOperations for WriteAheadLog {
    // Create the file. Allocate an initial buffer of the size of one block, write the header to the buffer and flush to the file.
    fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DBFile::create(&path)?;
        let block_size = FileSystem::block_size(&path)? as u32;
        let mut header = WalHeader::new(block_size);
        let mut journal = MemBuffer::alloc(block_size as usize, block_size as usize)?;
        header.padding = 0;
        header.journal_cursor = 0; // The journal starts at zero.
        header.last_flushed_offset = 0;
        journal.write_all(header.as_ref())?;
        file.write_all(journal.as_ref())?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            header,
            journal,
            file,
        })
    }

    // Open the file.
    // Read the header.
    // Read the membuffer from the file.
    fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let block_size = FileSystem::block_size(&path)? as u32;

        let mut file = DBFile::open(&path)?;
        let header = Self::load_header(&mut file, block_size as usize)?;
        let old_padding = header.padding;

        let mut log = Self {
            header,
            journal: MemBuffer::empty(),
            file,
        };
        log.reload_journal_from(old_padding as usize)?;
        Ok(log)
    }

    fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
        fs::remove_file(&path)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.file.truncate()
    }
}

impl Write for WriteAheadLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.do_flush()
    }
}

impl Read for WriteAheadLog {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for WriteAheadLog {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl WriteAheadLog {
    fn recompute_padding(&mut self) -> usize {
        let total_size = self.journal.len();
        let current_pos = self.journal.position();
        self.header.padding = (total_size - current_pos) as u32;
        self.header.padding as usize
    }

    pub fn push(&mut self, record: LogRecord) -> std::io::Result<()> {
        let size = record.size();
        let lsn = record.metadata().lsn;
        let journal_length = self.journal.len();
        let journal_pos = self.journal.position();

        if journal_length < (size + journal_pos) {
            //  let alignment = self.header.blk_size;
            self.journal.extend_from_slice(record.as_ref())?;
        } else {
            self.journal.write_all(record.as_ref())?;
        };

        self.recompute_padding();

        if self.header.start_lsn == LogId::from(0) {
            self.header.start_lsn = lsn;
        };

        self.header.last_lsn = lsn;
        self.header.num_entries += 1;

        Ok(())
    }

    fn load_header(file: &mut DBFile, block_size: usize) -> std::io::Result<WalHeader> {
        let mut block = MemBuffer::alloc(block_size, block_size)?;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut block)?;
        Ok(WalHeader::from_buf(&block))
    }

    fn load_blks_at(&mut self, offset: usize) -> std::io::Result<()> {
        debug_assert!(
            (self.header.last_flushed_offset as usize - offset) == self.journal.len(),
            "Invalid input . Failed to fill whole buffer"
        );

        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(self.journal.as_mut())?;
        self.header.journal_cursor = offset as u32;
        Ok(())
    }

    fn flush_journal(&mut self) -> std::io::Result<()> {
        if self.journal.is_empty() {
            return Ok(());
        };

        self.journal.seek(SeekFrom::Start(0))?;
        let blk_offset = self.header.journal_cursor as u64;
        self.file.seek(SeekFrom::Start(blk_offset))?;
        let total_size: usize = self.journal.len();

        let alignment: usize = self.journal.mem_alignment();
        debug_assert!(
            total_size.is_multiple_of(self.header.blk_size as usize),
            "Invalid journal lengh, must be aligned to filesystem blk size"
        );
        debug_assert!(
            alignment.is_multiple_of(self.header.blk_size as usize),
            "Invalid journal alignement, must be aligned to filesystem blk size"
        );
        self.file.write_all(self.journal.as_ref())?;

        // Recompute statistics after flush
        self.header.last_flushed_offset = self.header.journal_cursor as u64 + total_size as u64;

        Ok(())
    }

    fn reload_journal_from(&mut self, old_padding: usize) -> std::io::Result<()> {
        self.journal =
            MemBuffer::alloc(self.header.blk_size as usize, self.header.blk_size as usize)?;

        // Si hay padding, el último bloque está a block_size bytes antes de last_flushed_offset
        let read_offset = if old_padding > 0 {
            self.header
                .last_flushed_offset
                .saturating_sub(self.header.blk_size as u64)
        } else {
            // Si no hay padding, empezamos desde last_flushed_offset
            self.header.last_flushed_offset
        };

        if old_padding > 0 {
            self.load_blks_at(read_offset as usize)?;
            let write_offset = (self.header.blk_size - old_padding as u32) as u64;
            self.journal.seek(SeekFrom::Start(write_offset))?;
            self.header.padding = (self.journal.len() - self.journal.position()) as u32;
        } else {
            // Journal vacío, empezar desde 0
            self.header.journal_cursor = read_offset as u32;
            self.journal.seek(SeekFrom::Start(0))?;
        }
        Ok(())
    }

    // When flushing the journal to disk we generally want to increment the pointer to the last flushed offset. There are two exceptions, though:
    // Exception 1 is the header or block zero, which does not modify the last flushed offset but needs to be synced to disk to ensure statistics are persisted.
    // Exception 2 is self.current_block, which might have some free space, so for space optimization we prefer not to increase the offset to be able to reuse it.
    fn do_flush(&mut self) -> std::io::Result<()> {
        let current_padding = self.recompute_padding();
        self.flush_journal()?;
        self.sync_header()?;
        self.sync_all()?;

        // RELOAD THE JOURNAL TO BE ABLE TO KEEP DOING STUFF
        // NEED TO REVIEW THIS.
        self.reload_journal_from(current_padding)?;

        Ok(())
    }

    fn sync_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut tmp =
            MemBuffer::alloc(self.header.blk_size as usize, self.header.blk_size as usize)?;
        self.file.read_exact(&mut tmp)?;
        tmp.as_mut()[..std::mem::size_of::<WalHeader>()].copy_from_slice(self.header.as_ref());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&tmp)?;
        Ok(())
    }

    /// Recover and load all blocks from disk after opening
    fn replay(&mut self) -> std::io::Result<WalIterator<'_>> {
        WalIterator::new(
            &mut self.file,
            self.header.blk_size as usize,
            self.header.start_lsn,
            self.header.last_lsn,
            self.header.last_flushed_offset as usize,
            self.header.num_entries,
        )
    }
}

/// Iterator over log records in a WAL.
pub(crate) struct WalIterator<'a> {
    file: &'a mut DBFile,
    block_size: u32,
    start_lsn: LogId,
    last_lsn: LogId,
    last_flushed_offset: u64,
    read_ahead_size: usize,
    read_ahead_buffer: MemBuffer, // We cannot use the mem buffer here because it would require to take mutable references in order to seek it, which would fuck up our zero copy iteration approach.
    file_cursor: u64,
    mem_cursor: u64,
    current_lsn: LogId,
    num_entries: u32,
    entries_read: u32,
}

/// A reference to a log record in the buffer without copying
pub struct LogRecordRef<'a> {
    header: &'a LogHeader,
    data: &'a [u8], // The full record data including header
}

impl<'a> LogRecordRef<'a> {
    /// Get the header
    pub fn metadata(&self) -> &LogHeader {
        self.header
    }

    /// Get the redo payload as a slice
    pub fn redo_data(&self) -> &[u8] {
        let start = self.header.undo_offset as usize;
        let end = self.header.redo_offset as usize;
        &self.data[std::mem::size_of::<LogHeader>() + start..std::mem::size_of::<LogHeader>() + end]
    }

    /// Get the undo payload as a slice
    pub fn undo_data(&self) -> &[u8] {
        let end = self.header.undo_offset as usize;
        &self.data[std::mem::size_of::<LogHeader>()..std::mem::size_of::<LogHeader>() + end]
    }

    /// Get total size of the record
    pub fn size(&self) -> usize {
        self.header.total_size as usize
    }
}

impl<'a> WalIterator<'a> {
    fn new(
        file: &'a mut DBFile,
        block_size: usize,
        start_lsn: LogId,
        last_lsn: LogId,
        last_flushed_offset: usize,
        num_entries: u32,
    ) -> std::io::Result<Self> {
        let read_ahead_size = std::cmp::min(block_size * 4, last_flushed_offset); // Read 4 blocks at a time

        // Create aligned MemBuffer for read-ahead
        let mut read_ahead_buffer = MemBuffer::alloc(read_ahead_size, block_size)?;
        let cursor_start = read_ahead_buffer.len() as usize;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut read_ahead_buffer)?;

        Ok(Self {
            file,
            block_size: block_size as u32,
            start_lsn,
            last_lsn,
            last_flushed_offset: last_flushed_offset as u64,
            read_ahead_size,
            read_ahead_buffer,
            file_cursor: cursor_start as u64, // Start after header
            current_lsn: start_lsn,
            num_entries,
            entries_read: 0,
            mem_cursor: std::mem::size_of::<WalHeader>() as u64,
        })
    }

    /// Reload the read-ahead buffer from disk
    fn reload_buffer(&mut self) -> std::io::Result<bool> {
        // Check if we've read everything
        if self.file_cursor >= self.last_flushed_offset {
            return Ok(false);
        }

        // Early exit if we've read all entries based on metadata
        if self.entries_read >= self.num_entries && self.num_entries > 0 {
            return Ok(false);
        }

        // Calculate how much to read
        let remaining = (self.last_flushed_offset - self.file_cursor) as usize;
        let aligned_remaining = remaining & !(4096 - 1);
        let read_size = self.read_ahead_size.min(aligned_remaining);

        // Clear and resize buffer
        self.mem_cursor = 0;

        // This fails when aligned buffer is larger that the total file size.
        self.read_ahead_buffer = MemBuffer::alloc(read_size, self.block_size as usize)?;
        let metadata = self.file.metadata()?;

        if !self.file_cursor.is_multiple_of(self.block_size as u64) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unaligned pointer to file content",
            ));
        }
        if self.file_cursor as usize + self.read_ahead_buffer.len() > metadata.len() as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to fill whole buffer",
            ));
        };

        if self.read_ahead_buffer.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Attempted o read an empty amount",
            ));
        }

        // Read from file
        self.file.seek(SeekFrom::Start(self.file_cursor))?;
        self.file.read_exact(self.read_ahead_buffer.as_mut())?;

        // Update file cursor
        self.file_cursor += self.read_ahead_buffer.len() as u64;

        Ok(true)
    }

    /// Get the next record as a reference
    pub fn next_ref(&mut self) -> std::io::Result<Option<LogRecordRef<'_>>> {
        // Check if we've already read all entries
        if self.last_lsn != LogId::from(0) && self.current_lsn > self.last_lsn {
            return Ok(None);
        }

        // Check if buffer needs reloading
        if self.mem_cursor >= self.read_ahead_buffer.len() as u64 && !self.reload_buffer()? {
            return Ok(None);
        }

        let buffer_len = self.read_ahead_buffer.len();

        // Check if we have enough bytes for a header
        if self.mem_cursor as usize + std::mem::size_of::<LogHeader>() > buffer_len {
            // Check if we're at the end of valid data
            if self.file_cursor >= self.last_flushed_offset {
                return Ok(None);
            }
            // Otherwise, reload buffer
            if !self.reload_buffer()? {
                return Ok(None);
            }
            // Retry after reload
            return self.next_ref();
        }

        // Get header reference.
        // SAFETY: This should be safe as long as the header is valid
        let header = {
            let buf_ref: &[u8] = self.read_ahead_buffer.as_ref();
            let header_ptr = &buf_ref[self.mem_cursor as usize] as *const u8 as *const LogHeader;
            unsafe { &*header_ptr }
        };

        // Check if this is padding at the end (lsn == 0)
        if header.lsn == LogId::from(0) {
            return Ok(None);
        }

        // Validate LSN is within expected range
        if self.start_lsn != LogId::from(0) && header.lsn < self.start_lsn {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("LSN {} is before start_lsn {}", header.lsn, self.start_lsn),
            ));
        }

        let total_size = header.total_size as usize;

        // Check if we have the full record in buffer
        // This can happen if a record happens to be at a mid point in the buffer.
        if self.mem_cursor as usize + total_size > buffer_len {
            // Calculate where this record starts
            let record_start = self.file_cursor - (buffer_len - self.mem_cursor as usize) as u64;

            // Align down to block boundary
            let aligned_start = record_start & !(self.block_size as u64 - 1);
            self.file_cursor = aligned_start;

            if !self.reload_buffer()? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Incomplete log record at end of file",
                ));
            }
            self.mem_cursor = record_start - aligned_start;
            // Retry after reload
            return self.next_ref();
        }

        let buf_ref: &[u8] = self.read_ahead_buffer.as_ref();
        // Get a reference to the full record data
        let record_data = &buf_ref[self.mem_cursor as usize..self.mem_cursor as usize + total_size];

        let header_ref = unsafe { &*(record_data.as_ptr() as *const LogHeader) };

        // Create LogRecordRef
        let record_ref = LogRecordRef {
            header: header_ref,
            data: record_data,
        };

        // Advance buffer position
        self.mem_cursor += total_size as u64;

        // Update tracking
        self.current_lsn = header.lsn;
        self.entries_read += 1;

        Ok(Some(record_ref))
    }

    /// Check if there's more data to read
    fn has_more_data(&self) -> bool {
        // Check if we've read all expected entries
        if self.num_entries > 0 && self.entries_read >= self.num_entries {
            return false;
        }

        // Check if we've reached the last LSN
        if self.last_lsn != LogId::from(0) && self.current_lsn >= self.last_lsn {
            return false;
        }

        let max_mem = self.read_ahead_buffer.len() as u64;
        // Check physical boundaries
        self.mem_cursor < max_mem || self.file_cursor < self.last_flushed_offset
    }
}

impl<'a> WalIterator<'a> {
    /// Process each record with a closure (zero-copy)
    pub fn for_each<F>(&mut self, mut f: F) -> std::io::Result<()>
    where
        F: FnMut(LogRecordRef) -> std::io::Result<()>,
    {
        while self.has_more_data() {
            if let Some(record) = self.next_ref()? {
                f(record)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod wal_tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::tempdir;

    pub fn push_items(wal: &mut WriteAheadLog, start: u64, end: u64) {
        let start_entries = wal.header.num_entries;
        let num_rec = ((end - start) + 1) as u32;
        for i in start..=end {
            let rec = make_record(i, 1, 1, i as u16, 100, 100);
            wal.push(rec).unwrap();
        }

        assert_eq!(wal.header.last_lsn, LogId::from(end));
        assert_eq!(wal.header.num_entries, start_entries + num_rec);
    }

    pub fn validate_replay(wal: &mut WriteAheadLog, expected_count: usize) {
        let mut iterator = wal.replay().unwrap();
        let mut count = 0;

        iterator
            .for_each(|record| {
                count += 1;
                verify_record(&record, count as u64, 1);
                Ok(())
            })
            .unwrap();

        assert_eq!(count, expected_count);
    }

    pub fn make_record(
        lsn: u64,
        txid: u64,
        page: u64,
        row: u16,
        redo_len: usize,
        undo_len: usize,
    ) -> LogRecord {
        let mut rng = StdRng::seed_from_u64(lsn);
        let redo: Vec<u8> = (0..redo_len).map(|_| rng.random_range(0..=255)).collect();
        let undo: Vec<u8> = (0..undo_len).map(|_| rng.random_range(0..=255)).collect();
        LogRecord::new(
            TxId::from(txid),
            Some(LogId::from(lsn.saturating_sub(1))),
            PageId::from(page),
            LogRecordType::Overwrite,
            &redo,
            &undo,
        )
    }

    fn verify_record(record_ref: &LogRecordRef, expected_lsn: u64, expected_txid: u64) {
        assert_eq!(record_ref.metadata().lsn, LogId::from(expected_lsn));
        assert_eq!(record_ref.metadata().txid, TxId::from(expected_txid));

        // Verify data integrity
        let mut rng = StdRng::seed_from_u64(expected_lsn);
        let redo_data = record_ref.redo_data();
        let undo_data = record_ref.undo_data();

        for byte in redo_data {
            assert_eq!(*byte, rng.random_range(0..=255));
        }

        for byte in undo_data {
            assert_eq!(*byte, rng.random_range(0..=255));
        }
    }

    #[test]
    fn test_wal_1() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let mut wal = WriteAheadLog::create(&path).unwrap();
        let rec = make_record(1, 2, 3, 3, 11, 11);
        wal.push(rec).unwrap();

        // After pushing one record, journal should have the record + padding
        assert!(!wal.journal.is_empty());
        assert_eq!(wal.header.last_lsn, LogId::from(1));
        assert_eq!(wal.header.start_lsn, LogId::from(1));
    }

    #[test]
    fn test_wal_2() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");

        let mut wal = WriteAheadLog::create(&path).unwrap();
        let block_size = wal.header.blk_size as usize;

        // Create records that are large enough to trigger extension
        let big_rec1 = make_record(1, 2, 3, 3, block_size / 2, block_size / 2);
        let big_rec2 = make_record(2, 2, 3, 4, block_size / 2, block_size / 2);

        wal.push(big_rec1).unwrap();
        wal.push(big_rec2).unwrap();

        // Both records should be in journal now
        assert!(!wal.journal.is_empty());
        assert_eq!(wal.header.last_lsn, LogId::from(2));
        assert_eq!(wal.header.start_lsn, LogId::from(1));
    }

    #[test]
    fn test_wal_3() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_basic.db");
        let num_rec = 4555;
        // Write phase
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();
            push_items(&mut wal, 1, num_rec);
            // Flush to disk
            wal.flush().unwrap();
        }

        // Read phase
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            // Replay and verify records
            validate_replay(&mut wal, num_rec as usize);
        }
    }

    #[test]
    fn test_wal_4() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_basic.db");
        let num_rec = 100;
        // Write phase
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            push_items(&mut wal, 1, num_rec);
            // Flush to disk
            wal.flush().unwrap();

            push_items(&mut wal, num_rec + 1, 2 * num_rec);

            wal.flush().unwrap();

            push_items(&mut wal, 2 * num_rec + 1, 3 * num_rec);

            wal.flush().unwrap();
        }

        // Read phase
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            // Verify header was persisted correctly
            push_items(&mut wal, 3 * num_rec + 1, 4 * num_rec);

            wal.flush().unwrap();

            validate_replay(&mut wal, 4 * num_rec as usize);
        }
    }

    #[test]
    fn test_wal_5() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_basic.db");
        let num_rec = 100;
        // Write phase
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();

            push_items(&mut wal, 1, num_rec);
            // Flush to disk
            wal.flush().unwrap();

            // These extra items should not be read.
            push_items(&mut wal, num_rec + 1, 2 * num_rec);
        }

        // Read phase
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();

            validate_replay(&mut wal, num_rec as usize);
        }
    }
}
