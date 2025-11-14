use crate::io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize};
use crate::storage::buffer::BufferWithMetadata;
use crate::types::{ LogId, OId, TxId, UInt64};
use std::io::{ Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr::NonNull;

pub const WAL_RECORD_ALIGNMENT: usize = 64;

// Log type representation.
// Redo logs store the database state after the transaction.
// Used for recovery after failures and eventual flushing to disk.
// Undo logs store the previous database state before the transaction started.
// Used for rollback of changes.
crate::repr_enum!(pub(crate) enum LogRecordType: u8 {
    Update = 0x00,
    Commit = 0x01,
    Abort = 0x02,
    End = 0x03 // Signifies end of commit or abort
});

#[repr(C, align(8))]
pub(crate) struct WalBlockZeroHeader {
    start_lsn: LogId,          // 8b
    last_lsn: LogId,           // 16b
    blk_zero_start_lsn: LogId, // 8b
    blk_zero_last_lsn: LogId,  // 16b
    blk_zero_num_entries: u32, // 20b
    last_flushed_offset: u64,
    num_blocks: u32,
    blk_size: u32,
    blk_zero_last_used_offset: u32,
    blk_zero_padding: u32,
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
            num_blocks: 0,
            blk_size,
            blk_zero_num_entries: 0,
            blk_zero_padding: blk_size,
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
        unsafe {
            let ptr = self.record_at(
                self.metadata()
                    .blk_zero_last_used_offset
                    .try_into()
                    .unwrap(),
            );
            ptr.write(record);
        };

        self.metadata_mut().blk_zero_last_used_offset += size as u32;
    }
}

#[repr(C, align(8))]
pub(crate) struct WalBlockHeader {
    start_lsn: LogId, // 8b
    last_lsn: LogId,  // 16b
    num_entries: u32, // 20b
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

    fn push(&mut self, record: LogRecord) {
        let size = record.size();

        unsafe {
            let ptr = self.record_at(self.metadata().last_used_offset.try_into().unwrap());
            ptr.write(record);
        };

        self.metadata_mut().last_used_offset += size as u32;
        self.metadata_mut().num_entries += 1;
    }
}

#[repr(C, align(8))]
pub(crate) struct LogHeader {
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

type LogRecord = BufferWithMetadata<LogHeader>;

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
}


struct WriteAheadLog {
    header: BlockZero,
    journal: Vec<WalBlock>, // in memory buffer
    current_block: WalBlock,
    file: DBFile,
}



// TODO! REVISAR COMO HACER PARA USAR PRIMERO BLOCK ZERO PARA ESCRIBIR LOS RECORDS.
impl WriteAheadLog {
    fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DBFile::create(&path)?;
        let block_size = FileSystem::block_size(&path)? as u32;
        let header = Self::alloc_block_zero(&mut file, block_size)?;
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

        let mut file = DBFile::open(path)?;
        let header = Self::load_block_zero(&mut file, block_size)?;
        let block = WalBlock::alloc(block_size);

        Ok(Self {
            header,
            journal: Vec::new(),
            current_block: block,
            file,
        })
    }

    fn push(&mut self, record: LogRecord) {
        let size = record.size();
        if size <= self.current_block.available_space() {
            self.current_block.push(record);
        } else {
            let required_size = record
                .size()
                .next_multiple_of(self.header.metadata().blk_size as usize);

            let mut old_blk = std::mem::replace(
                &mut self.current_block,
                WalBlock::alloc(required_size as u32),
            );

            old_blk.metadata_mut().is_closed = true;
            self.journal.push(old_blk);
        };
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

    fn flush(&mut self) -> std::io::Result<()> {
        let start = self.header.metadata().last_flushed_offset;
        self.file.seek(SeekFrom::Start(start))?;
        for bl in self.journal.iter() {
            let size = bl.size() as u64;
            self.header.metadata_mut().last_flushed_offset += size;
            self.file.write_all(bl.as_ref())?;
        }

        self.header.metadata_mut().last_flushed_offset += self.current_block.size() as u64;
        self.current_block.metadata_mut().is_closed = true;
        let last_written = self.current_block.metadata().last_used_offset;
        let size = self.current_block.size();
        self.current_block.metadata_mut().padding = size as u32 - last_written;
        self.file.write_all(self.current_block.as_ref())?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(self.header.as_ref())?;
        Ok(())
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
    fn test_wal_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let wal = WriteAheadLog::create(&path).unwrap();
        assert_eq!(wal.journal.len(), 0);
        assert_eq!(wal.header.metadata().num_blocks, 0);
    }

    #[test]
    fn test_push_rec() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let mut wal = WriteAheadLog::create(&path).unwrap();
        let rec = make_record(1, 2, 3, 3, 11, 11);
        wal.push(rec);
        assert_eq!(wal.journal.len(), 0);
        assert_eq!(wal.current_block.metadata().num_entries, 1);
    }

    #[test]
    fn test_blocks_buffering() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");

        let mut wal = WriteAheadLog::create(&path).unwrap();
        let block_size = wal.header.metadata().blk_size as usize;

        let big_rec = make_record(1, 2, 3, 3, block_size / 2, block_size / 2);

        wal.push(big_rec.clone());
        wal.push(big_rec);

        assert_eq!(wal.journal.len(), 1);
    }

    #[test]
    fn test_wal_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");

        let mut wal = WriteAheadLog::create(&path).unwrap();
        wal.push(make_record(1, 2, 3, 3, 11, 11));
        wal.push(make_record(2, 2, 3, 3, 11, 11));
        wal.flush().unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > wal.header.metadata().blk_size as u64);
    }
}
