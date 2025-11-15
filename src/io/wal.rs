use crate::io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize};
use crate::storage::buffer::BufferWithMetadata;
use crate::types::{BlockId, Key, LogId, OId, TxId, UInt64};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr::NonNull;

pub const WAL_RECORD_ALIGNMENT: usize = 64;

// Log type representation.
// Redo logs store the database state after the transaction.
// Used for recovery after failures and eventual flushing to disk.
// Undo logs store the previous database state before the transaction started.
// Used for rollback of changes.
crate::repr_enum!(pub enum LogRecordType: u8 {
    Update = 0x00,
    Commit = 0x01,
    Abort = 0x02,
    End = 0x03 // Signifies end of commit or abort
});

#[derive(Debug)]
#[repr(C, align(64))]
pub(crate) struct WalBlockZeroHeader {
    start_lsn: LogId,
    last_lsn: LogId,
    blk_zero_start_lsn: LogId,
    blk_zero_last_lsn: LogId,
    last_flushed_offset: u64,
    current_blk_offset: u64, // To recover from disk.
    current_blk_size: u32,
    num_blocks: u32,
    blk_size: u32,
    blk_zero_last_used_offset: u32,
    blk_zero_num_entries: u32,
    blk_zero_is_closed: bool,
}

impl AsRef<[u8]> for WalBlockZeroHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const WalBlockZeroHeader) as *const u8,
                std::mem::size_of::<WalBlockZeroHeader>(),
            )
        }
    }
}

impl WalBlockZeroHeader {
    fn new(blk_size: u32) -> Self {
        Self {
            start_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            blk_zero_start_lsn: LogId::from(0),
            blk_zero_last_lsn: LogId::from(0),
            blk_zero_last_used_offset: 0,
            last_flushed_offset: 0,
            current_blk_offset: 0,
            current_blk_size: blk_size,
            num_blocks: 0,
            blk_size,
            blk_zero_num_entries: 0,
            blk_zero_is_closed: false,
        }
    }
}

type BlockZero = BufferWithMetadata<WalBlockZeroHeader>;

impl BlockZero {
    pub fn alloc(block_size: u32) -> Self {
        // Create buffer with total size = header + aligned payload
        let mut buffer = BufferWithMetadata::<WalBlockZeroHeader>::with_capacity(
            block_size as usize, // usable space for data
            block_size as usize,
        );

        // Initialize header
        let header = WalBlockZeroHeader::new(block_size);

        *buffer.metadata_mut() = header;

        buffer
    }

    fn record_at(&self, offset: u16) -> NonNull<LogRecord> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }

    fn push(&mut self, record: LogRecord) {
        let size = record.size();
        let lsn = record.metadata().lsn;
        unsafe {
            let ptr = self.record_at(
                self.metadata()
                    .blk_zero_last_used_offset
                    .try_into()
                    .unwrap(),
            );
            ptr.write(record);
        };

        if self.metadata().start_lsn == LogId::from(0) {
            self.metadata_mut().start_lsn = lsn;
            self.metadata_mut().blk_zero_start_lsn = lsn;
        };

        self.metadata_mut().blk_zero_last_lsn = lsn;
        self.metadata_mut().blk_zero_last_used_offset += size as u32;
        self.metadata_mut().blk_zero_num_entries += 1;
    }

    pub fn close(&mut self) {
        self.metadata_mut().blk_zero_is_closed = true;
    }

    /// Returns an iterator over the log records in block zero
    pub fn iter(&self) -> BlockZeroIterator<'_> {
        BlockZeroIterator {
            block: self,
            current_offset: 0,
            remaining_entries: self.metadata().blk_zero_num_entries,
        }
    }

    fn available_space(&self) -> usize {
        self.size() - self.metadata().blk_zero_last_used_offset as usize
    }
}

/// Iterator over log records in block zero
pub struct BlockZeroIterator<'a> {
    block: &'a BlockZero,
    current_offset: u32,
    remaining_entries: u32,
}

impl<'a> Iterator for BlockZeroIterator<'a> {
    type Item = &'a LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_entries == 0 {
            return None;
        }

        unsafe {
            let record_ptr = self.block.record_at(self.current_offset as u16);
            let record = &*record_ptr.as_ptr();

            self.current_offset += record.metadata().total_size;
            self.remaining_entries -= 1;

            Some(record)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining_entries as usize;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for BlockZeroIterator<'a> {
    fn len(&self) -> usize {
        self.remaining_entries as usize
    }
}

#[derive(Debug)]
#[repr(C, align(64))]
pub(crate) struct WalBlockHeader {
    block_id: BlockId,
    start_lsn: LogId,
    last_lsn: LogId,
    num_entries: u32,
    total_size: u32,
    last_used_offset: u32,
    padding: u32,
    is_closed: bool,
}

impl AsRef<[u8]> for WalBlockHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const WalBlockHeader) as *const u8,
                std::mem::size_of::<WalBlockHeader>(),
            )
        }
    }
}

impl WalBlockHeader {
    fn new(total_size: u32) -> Self {
        Self {
            block_id: BlockId::new_key(),
            start_lsn: LogId::from(0),
            last_lsn: LogId::from(0),
            num_entries: 0,
            total_size,
            last_used_offset: 0,
            padding: total_size,
            is_closed: false,
        }
    }
}

type WalBlock = BufferWithMetadata<WalBlockHeader>;

impl WalBlock {
    pub fn id(&self) -> BlockId {
        self.metadata().block_id
    }

    pub fn alloc(total_size: u32) -> Self {
        // Create buffer with total size = header + aligned payload
        let mut buffer = BufferWithMetadata::<WalBlockHeader>::with_capacity(
            total_size as usize, // usable space for data
            total_size as usize,
        );

        // Initialize header
        let header = WalBlockHeader::new(total_size);

        *buffer.metadata_mut() = header;
        buffer.set_len(total_size as usize);

        buffer
    }

    fn record_at(&self, offset: u16) -> NonNull<LogRecord> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }

    fn available_space(&self) -> usize {
        self.size() - self.metadata().last_used_offset as usize
    }

    pub fn close(&mut self) {
        self.metadata_mut().is_closed = true;
    }

    fn push(&mut self, record: LogRecord) {
        let size = record.size();
        let lsn = record.metadata().lsn;
        unsafe {
            let ptr = self.record_at(self.metadata().last_used_offset.try_into().unwrap());
            ptr.write(record);
        };

        if self.metadata().start_lsn == LogId::from(0) {
            self.metadata_mut().start_lsn = lsn;
        }

        self.metadata_mut().last_lsn = lsn;
        self.metadata_mut().last_used_offset += size as u32;
        self.metadata_mut().num_entries += 1;
    }

    pub fn iter(&self) -> WalBlockIterator<'_> {
        WalBlockIterator {
            block: self,
            current_offset: 0,
            remaining_entries: self.metadata().num_entries,
        }
    }
}

/// Iterator over log records in a WAL block
pub(crate) struct WalBlockIterator<'a> {
    block: &'a WalBlock,
    current_offset: u32,
    remaining_entries: u32,
}

impl<'a> Iterator for WalBlockIterator<'a> {
    type Item = &'a LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_entries == 0 {
            return None;
        }

        // Safety: We know the record exists at this offset because:
        // 1. We track remaining_entries from the block header
        // 2. The offset is within the bounds of used data
        unsafe {
            let record_ptr = self.block.record_at(self.current_offset as u16);
            let record = &*record_ptr.as_ptr();

            // Move to next record using the total_size from header
            self.current_offset += record.metadata().total_size;
            self.remaining_entries -= 1;

            Some(record)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining_entries as usize;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for WalBlockIterator<'a> {
    fn len(&self) -> usize {
        self.remaining_entries as usize
    }
}

#[repr(C, align(64))]
pub struct LogHeader {
    /// Auto-incrementing log sequence number
    lsn: LogId,
    txid: TxId, // Id of the current transaction
    /// Log id of the previous LSN of the current transaction.
    prev_lsn: LogId,
    table_id: OId,
    row_id: UInt64,
    total_size: u32,
    undo_offset: u16,
    redo_offset: u16,
    log_type: LogRecordType,
    padding: u8,
}

impl LogHeader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        lsn: LogId,
        txid: TxId,
        table_id: OId,
        row_id: UInt64,
        prev_lsn: Option<LogId>,
        total_size: u32,
        redo_offset: u16,
        undo_offset: u16,
        log_type: LogRecordType,
        padding: u8,
    ) -> Self {
        Self {
            lsn,
            txid,
            prev_lsn: prev_lsn.unwrap_or(LogId::from(0)),
            table_id,
            row_id,
            total_size,
            undo_offset,
            redo_offset,
            log_type,
            padding,
        }
    }
}

pub type LogRecord = BufferWithMetadata<LogHeader>;

impl LogRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: LogId,
        txid: TxId,
        prev_lsn: Option<LogId>,
        table_id: OId,
        row_id: UInt64,
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
            id,
            txid,
            table_id,
            row_id,
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
    header: BlockZero,
    journal: Vec<WalBlock>, // in memory write queue
    current_block: WalBlock,
    file: DBFile,
}

impl FileOperations for WriteAheadLog {
    fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DBFile::create(&path)?;
        let block_size = FileSystem::block_size(&path)? as u32;
        let mut header = Self::alloc_block_zero(&mut file, block_size)?;
        header.metadata_mut().num_blocks += 1;
        header.metadata_mut().current_blk_offset = 0;
        header.metadata_mut().current_blk_size = block_size;

        // Preallocate a single block.
        let block = WalBlock::alloc(block_size);

        Ok(Self {
            header,
            journal: Vec::new(),
            current_block: block,
            file,
        })
    }

    fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let block_size = FileSystem::block_size(&path)? as u32;

        let mut file = DBFile::open(&path)?;
        let mut header = Self::load_block_zero(&mut file, block_size)?;

        // TODO! Need to review this section.
        let block = if header.metadata().current_blk_offset == 0 {
            WalBlock::alloc(block_size)
        } else {
            let cblock_size = header.metadata().current_blk_size;
            let cblock_offset = header.metadata().current_blk_offset;
            let mut buf = FileSystem::alloc_buffer(&path, cblock_size as usize)?;

            file.seek(SeekFrom::Start(cblock_offset))?;
            file.read_exact(&mut buf)?;
            header.metadata_mut().last_flushed_offset = cblock_offset;

            WalBlock::try_from((buf.as_ref(), block_size as usize)).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Cannot load a blk of size too large from disk!",
                )
            })?
        };

        Ok(Self {
            header,
            journal: Vec::new(),
            current_block: block,
            file,
        })
    }

    fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
        fs::remove_file(&path)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.journal.clear();
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
    // There are three cases here.
    // [Case A]: The header still has space for the record: we push the record into the header.
    // [Case B]: The header does not have space for the record, but our current block does. We push the record into our current block and mark the header as closed.
    // [Case C]: The current block cannot fit the block. We allocate a new block, journal this block marking it as closed and push it to the journal. We need to ensure that this new block fits the record. We do this by allocating it with at least the required size for the record. This way we can make sure that records are always going to fit.
    fn push(&mut self, record: LogRecord) {
        let size = record.size();
        let lsn = record.metadata().lsn;

        // Record to header
        if size <= self.header.available_space() && !self.header.metadata().blk_zero_is_closed {
            self.header.push(record);

        // Record to cur block and we need to close the header
        } else if !self.header.metadata().blk_zero_is_closed {
            self.header.close();
            self.header.metadata_mut().current_blk_offset += self.header.size() as u64;
            self.header.metadata_mut().num_blocks += 1;
            self.current_block.push(record);

        // Standard block to current block with header already closed.
        } else if size <= self.current_block.available_space() {
            self.current_block.push(record);

        // No space available. Allocate a new block.
        } else {
            let required_size = record
                .size()
                .next_multiple_of(self.header.metadata().blk_size as usize);
            let mut new_blk = WalBlock::alloc(required_size as u32);
            new_blk.push(record);

            self.journal_block(new_blk);
        };
        self.header.metadata_mut().last_lsn = lsn;
    }

    fn alloc_block_zero(file: &mut DBFile, block_size: u32) -> std::io::Result<BlockZero> {
        let mut block = BlockZero::alloc(block_size);
        block.metadata_mut().last_flushed_offset = block_size as u64;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(block.as_ref())?;
        file.flush()?;
        Ok(block)
    }

    fn load_block_zero(file: &mut DBFile, block_size: u32) -> std::io::Result<BlockZero> {
        let mut block = vec![0u8; block_size as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut block)?;
        let zero = BlockZero::try_from((block.as_ref(), block_size as usize)).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unable to allocate block zero".to_string(),
            )
        })?;
        Ok(zero)
    }

    fn flush_journal(&mut self) -> std::io::Result<()> {
        if self.journal.is_empty() {
            return Ok(());
        };
        let blk_offset = self.header.metadata().last_flushed_offset;
        self.file.seek(SeekFrom::Start(blk_offset))?;
        let total_size: usize = self.journal.iter().map(|c| c.size()).sum();
        for block in self.journal.drain(..) {
            self.file.write_all(block.as_ref())?;
        }
        Ok(())
    }

    fn journal_block(&mut self, new_block: WalBlock) {
        let mut old_blk = std::mem::replace(&mut self.current_block, new_block);
        old_blk.close();
        self.header.metadata_mut().current_blk_offset += old_blk.size() as u64;
        self.header.metadata_mut().current_blk_size = self.current_block.size() as u32;
        self.header.metadata_mut().num_blocks += 1;
        self.journal.push(old_blk);
    }

    // When flushing the journal to disk we generally want to increment the pointer to the last flushed offset. There are two exceptions, though:
    // Exception 1 is the header or block zero, which does not modify the last flushed offset but needs to be synced to disk to ensure statistics are persisted.
    // Exception 2 is self.current_block, which might have some free space, so for space optimization we prefer not to increase the offset to be able to reuse it.
    fn do_flush(&mut self) -> std::io::Result<()> {
        self.flush_journal()?;
        self.file.write_all(self.current_block.as_ref())?;
        self.header.metadata_mut().last_flushed_offset = self.header.metadata().current_blk_offset
            + self.header.metadata().current_blk_size as u64;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(self.header.as_ref())?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Recover and load all blocks from disk after opening
    fn replay(&mut self) -> WalIterator {
        WalIterator {
            wal: self as *mut WriteAheadLog,
            block_size: self.header.metadata().blk_size,
            last_flushed_offset: self.header.metadata().last_flushed_offset,
            read_ahead_size: self.header.metadata().blk_size as u64 * 4,
            read_ahead_buf: Vec::new(),
            read_ahead_cursor: 0,
            mem_cursor: 0,
            current_iter: None,
        }
    }
}

enum BlockIterator<'a> {
    Zero(BlockZeroIterator<'a>),
    Wal(WalBlockIterator<'a>),
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = &'a LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Zero(b) => b.next(),
            Self::Wal(b) => b.next(),
        }
    }
}

impl<'a> ExactSizeIterator for BlockIterator<'a> {
    fn len(&self) -> usize {
        match self {
            Self::Zero(b) => b.len(),
            Self::Wal(b) => b.len(),
        }
    }
}

enum Block {
    Zero(BlockZero),
    Wal(WalBlock),
}

impl Block {
    fn iter(&self) -> BlockIterator<'_> {
        match self {
            Self::Wal(b) => BlockIterator::Wal(b.iter()),
            Self::Zero(b) => BlockIterator::Zero(b.iter()),
        }
    }
}

/// Iterator over log records in a WAL.
/// Loads blocks on a read-ahead manner from disk, maintaining a read-ahead cache.
/// Building iterators over disk data structures is difficult in rust without copying.
/// I have found this lifetime extension pattern useful.
/// This forces us to decouple the lifetime of the iterator from the lifetime of the WAL.
/// It can be done with a raw pointer as it is demonstrated down here.
/// TODO! Must try to implement this approach on the btree.
pub(crate) struct WalIterator {
    wal: *mut WriteAheadLog,
    block_size: u32,
    last_flushed_offset: u64,
    read_ahead_size: u64,
    read_ahead_buf: Vec<u8>,
    read_ahead_cursor: usize,
    mem_cursor: usize,
    current_iter: Option<BlockIterator<'static>>,
}

impl WalIterator {
    /// Returns next record from current block, if any.
    fn next_record_from_current(&mut self) -> Option<&'static LogRecord> {
        if let Some(iter) = &mut self.current_iter
            && let Some(rec) = iter.next()
        {
            return Some(rec);
        }
        None
    }

    fn reload_buf(&mut self) -> std::io::Result<bool> {
        if self.read_ahead_cursor >= self.last_flushed_offset as usize {
            return Ok(false);
        };

        // Get a mutable reference to the file.
        let wal = unsafe { &mut *self.wal };
        let file = &mut wal.file;

        self.read_ahead_buf.clear();
        let remaining_size = self
            .last_flushed_offset
            .saturating_sub(self.read_ahead_cursor as u64);
        let read_size = self.read_ahead_size.min(remaining_size);

        // Load block.
        // WAL blocks are variable size. We therefore need to interpret their metadata before we load the full block into memory to address what is the actual total size of this block.
        // This can be seen as a disadvantage in terms of efficiency, but it is not as long as you read ahead on the file. We know in advance the filesystem block size so we can load a big chunk of bytes that is a multiple of that into memory, operate on it until there are no more blocks that fit, and finally load the next chunk.
        let mut chunk_buffer = FileSystem::alloc_buffer(file.path(), read_size as usize)?;
        // As blocks are aligned to the filesystem blk size, and chunk size is a multiple of that, the odds that we have landed in the middle of a block are low, but IT CAN HAPPEN and we need to handle it.

        // Reposition the buffer at the cursor and reload the chunk in memory
        file.seek(SeekFrom::Start(self.read_ahead_cursor as u64))?;
        file.read_exact(&mut chunk_buffer)?;
        self.read_ahead_buf = chunk_buffer;
        self.mem_cursor = 0;
        Ok(true)
    }

    fn load_block_unchecked(&mut self, block_size: usize) -> std::io::Result<bool> {
        let block = if self.mem_cursor == 0 && self.read_ahead_cursor == 0 {
            Block::Zero(self.blk_zero(block_size)?)
        } else {
            let block: BufferWithMetadata<WalBlockHeader> =
                WalBlock::try_from((&self.read_ahead_buf[self.mem_cursor..], block_size)).map_err(
                    |_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Invalid WAL block at mem cursor: {}, file cursor: {}",
                                self.mem_cursor, self.read_ahead_cursor
                            ),
                        )
                    },
                )?;
            Block::Wal(block)
        };

        let iter = block.iter();
        // Turn it into a `'static` iterator
        let iter_static: BlockIterator<'static> = unsafe { std::mem::transmute(iter) };
        self.current_iter = Some(iter_static);
        self.mem_cursor += block_size;
        self.read_ahead_cursor += block_size;
        Ok(true)
    }

    fn load_header(&mut self) -> std::io::Result<&WalBlockZeroHeader> {
        debug_assert_eq!(
            self.read_ahead_cursor, 0,
            "Invalid read ahead cursor value {} for reading header!",
            self.read_ahead_cursor
        );
        let header_ptr = self.read_ahead_buf[0..std::mem::size_of::<WalBlockZeroHeader>()].as_ptr()
            as *const WalBlockZeroHeader;
        let header = unsafe { &*header_ptr };
        Ok(header)
    }

    fn blk_zero(&mut self, block_size: usize) -> std::io::Result<BlockZero> {
        debug_assert_eq!(
            self.read_ahead_cursor, 0,
            "Invalid read ahead cursor value {} for reading header!",
            self.read_ahead_cursor
        );
        BlockZero::try_from((&self.read_ahead_buf[0..], block_size)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid WAL block at mem cursor: {}, file cursor: {}",
                    self.mem_cursor, self.read_ahead_cursor
                ),
            )
        })
    }

    fn load_blk_header(&mut self) -> std::io::Result<&WalBlockHeader> {
        let header_ptr = self.read_ahead_buf
            [self.mem_cursor..self.mem_cursor + std::mem::size_of::<WalBlockHeader>()]
            .as_ptr() as *const WalBlockHeader;
        let header = unsafe { &*header_ptr };
        Ok(header)
    }

    fn load_block(&mut self) -> std::io::Result<bool> {
        if self.mem_cursor >= self.read_ahead_buf.len() {
            let reload = self.reload_buf()?;
            if !reload {
                return Ok(false);
            }
        };

        let block_size = if self.mem_cursor == 0 && self.read_ahead_cursor == 0 {
            let header = self.load_header()?;
            header.blk_size as usize
        } else {
            let header = self.load_blk_header()?;
            header.total_size as usize
        };

        if block_size + self.mem_cursor > self.read_ahead_buf.len() {
            // At this point the read ahead buf requires space.
            if !self.reload_buf()? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "File corruption. Unable to load a partially sized block from disk.",
                ));
            };
        };
        self.load_block_unchecked(block_size)
    }
}

impl Iterator for WalIterator {
    type Item = &'static LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(rec) = self.next_record_from_current() {
                return Some(rec);
            }

            match self.load_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(_) => return None,
            }
        }
    }
}

#[cfg(test)]
mod wal_tests {

    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::tempdir;

    pub fn make_record(
        lsn: u64,
        txid: u64,
        table: u64,
        row: u64,
        redo_len: usize,
        undo_len: usize,
    ) -> LogRecord {
        let mut rng = StdRng::seed_from_u64(lsn);
        let redo: Vec<u8> = (0..redo_len)
            .map(|_| rng.gen_range(0..redo_len) as u8)
            .collect();
        let undo: Vec<u8> = (0..undo_len)
            .map(|_| rng.gen_range(0..redo_len) as u8)
            .collect();

        LogRecord::new(
            LogId::from(lsn),
            TxId::from(txid),
            Some(LogId::from(lsn.saturating_sub(1))),
            OId::from(table),
            UInt64::from(row),
            LogRecordType::Update,
            &redo,
            &undo,
        )
    }

    #[test]
    fn test_wal_1() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let mut wal = WriteAheadLog::create(&path).unwrap();
        let rec = make_record(1, 2, 3, 3, 11, 11);
        wal.push(rec);
        assert_eq!(wal.journal.len(), 0);
        assert_eq!(wal.current_block.metadata().num_entries, 1);
    }

    #[test]
    fn test_wal_2() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");

        let mut wal = WriteAheadLog::create(&path).unwrap();
        let block_size = wal.header.metadata().blk_size as usize;

        let big_rec = make_record(1, 2, 3, 3, block_size / 2, block_size / 2);

        wal.push(big_rec.clone());
        wal.push(big_rec);

        assert_eq!(wal.journal.len(), 1);
    }

    #[test] // TODO. Review how to do this with a macro
    fn test_wal_3() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_iter.db");
        let mut expected = Vec::new();

        // Allocate a new wal and flush it.
        {
            let mut wal = WriteAheadLog::create(&path).unwrap();
            let blk_size = FileSystem::block_size(&path).unwrap();
            let record_payload = blk_size / 4;

            for i in 1..=1000 {
                let rec = make_record(i, 10, 5, i, record_payload, record_payload);
                wal.push(rec.clone());
                expected.push(rec);
            }

            wal.flush().unwrap();
        }

        // Reopen the wal and replay.
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            let blk_size = FileSystem::block_size(&path).unwrap();
            let record_payload = blk_size / 4;

            for (i, entry) in wal.replay().enumerate() {
                assert_eq!(entry.metadata().lsn, expected[i].metadata().lsn);
                assert_eq!(entry.metadata().log_type, expected[i].metadata().log_type);
                assert_eq!(
                    entry.metadata().redo_offset,
                    expected[i].metadata().redo_offset
                );
                assert_eq!(
                    entry.metadata().undo_offset,
                    expected[i].metadata().undo_offset
                );
                assert_eq!(entry.metadata().prev_lsn, expected[i].metadata().prev_lsn);
                assert_eq!(entry.metadata().table_id, expected[i].metadata().table_id);
                assert_eq!(entry.metadata().row_id, expected[i].metadata().row_id);
                assert_eq!(entry.metadata().txid, expected[i].metadata().txid);
                assert_eq!(entry.undo_data(), expected[i].undo_data());
                assert_eq!(entry.redo_data(), expected[i].redo_data());
            }

            for i in 1001..=2000 {
                let rec = make_record(1001, 10, 5, 1001, record_payload, record_payload);

                wal.push(rec.clone());
                expected.push(rec);
            }

            wal.flush().unwrap();
        }

        // Reopen the wal and replay.
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            for (i, entry) in wal.replay().enumerate() {
                dbg!(entry.metadata().lsn);
                assert_eq!(entry.metadata().lsn, expected[i].metadata().lsn);
                assert_eq!(entry.metadata().log_type, expected[i].metadata().log_type);
                assert_eq!(
                    entry.metadata().redo_offset,
                    expected[i].metadata().redo_offset
                );
                assert_eq!(
                    entry.metadata().undo_offset,
                    expected[i].metadata().undo_offset
                );
                assert_eq!(entry.metadata().prev_lsn, expected[i].metadata().prev_lsn);
                assert_eq!(entry.metadata().table_id, expected[i].metadata().table_id);
                assert_eq!(entry.metadata().row_id, expected[i].metadata().row_id);
                assert_eq!(entry.metadata().txid, expected[i].metadata().txid);
                assert_eq!(entry.undo_data(), expected[i].undo_data());
                assert_eq!(entry.redo_data(), expected[i].redo_data());
            }
        }
    }
}
