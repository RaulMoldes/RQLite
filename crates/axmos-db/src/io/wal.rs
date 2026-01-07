use crate::{
    TransactionId,
    io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
    io::logger::{Delete, Insert, Update},
    storage::{
        Allocatable, AvailableSpace, WalMetadata, WalOps, Writable,
        wal::{BlockZero, OwnedRecord, RecordRef, RecordType, WAL_BLOCK_SIZE, WalBlock},
    },
    types::{BlockId, Lsn},
};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug)]
pub struct WriteAheadLog {
    header: BlockZero,
    current_block: Option<WalBlock>,
    flush_queue: VecDeque<WalBlock>,
    file: DBFile,
    block_size: usize,
}

impl FileOperations for WriteAheadLog {
    fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = DBFile::create(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;
        let block_size = WAL_BLOCK_SIZE.next_multiple_of(fs_block_size);
        let header = BlockZero::alloc(0, block_size);

        Ok(Self {
            header,
            current_block: None,
            flush_queue: VecDeque::new(),
            file,
            block_size,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = DBFile::open(&path)?;
        let fs_block_size = FileSystem::block_size(&path)?;
        let default_block_size = WAL_BLOCK_SIZE.next_multiple_of(fs_block_size);

        // Read block 0 (global header)
        let mut header_buf: BlockZero = BlockZero::new(default_block_size);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(header_buf.as_mut())?;

        // Usar el block_size del archivo, o el default si es 0
        let block_size = header_buf.metadata().wal_header.block_size as usize;
        let block_size = if block_size == 0 {
            default_block_size
        } else {
            block_size
        };

        Ok(Self {
            header: header_buf,
            current_block: None, // If needed, will be allocated on push.
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
        self.header = BlockZero::alloc(0, self.block_size);
        self.current_block = None;
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

/// Result of the analysis phase.
///
/// The objective of the analysis phase is to discover which transactions will need a redo or an undo
#[derive(Debug, Default)]
pub struct AnalysisResult {
    /// Transactions that need to be undo
    pub needs_undo: HashSet<TransactionId>,
    /// Transactions that need to be redo
    pub needs_redo: HashSet<TransactionId>,
    /// Map of the last lsn for each transaction.
    pub lsn_chains: HashMap<TransactionId, Vec<Lsn>>,
    /// Map from transaction ID to its last LSN
    pub insert_ops: HashMap<Lsn, Insert>,
    /// Map from transaction ID to its last LSN
    pub delete_ops: HashMap<Lsn, Delete>,
    /// Update ops
    pub update_ops: HashMap<Lsn, Update>,
    /// The starting point for redo
    pub start_redo_lsn: Option<Lsn>,
}
impl AnalysisResult {
    pub(crate) fn try_iter_lsn(&self, id: &TransactionId) -> Option<std::slice::Iter<'_, Lsn>> {
        self.lsn_chains.get(id).map(|vec| vec.iter())
    }
}
impl WriteAheadLog {
    /// Maximum record size that can fit in a single block
    pub(crate) fn max_record_size(&self) -> usize {
        WalBlock::usable_space(self.block_size as usize) as usize
    }

    pub(crate) fn last_lsn(&self) -> Option<Lsn> {
        self.header.last_lsn()
    }

    /// Runs the analysis phase of the ARIES recovery protocol.
    pub(crate) fn run_analysis(&mut self) -> io::Result<AnalysisResult> {
        let mut result = AnalysisResult::default();

        let total_blocks = self.header.metadata().wal_header.total_blocks;
        if total_blocks == 0 {
            return Ok(result);
        }

        let mut reader = WalReader::new(
            &mut self.file,
            self.block_size * 4, // Read ahead 4 blocks
            self.block_size,
            total_blocks,
        )?;

        while let Some(record) = reader.next_ref()? {
            let tid = record.tid();
            let lsn = record.lsn();
            let record_type = record.log_type();

            // Track the last LSN for each transaction
            result
                .lsn_chains
                .entry(tid)
                .or_insert_with(Vec::new)
                .push(lsn);

            // Set [start_redo_lsn] to the first LSN we see
            if result.start_redo_lsn.is_none() {
                result.start_redo_lsn = Some(lsn);
            }

            match record_type {
                // By default all transactions will need to be undo unless the have committed.
                RecordType::Begin => {
                    result.needs_undo.insert(tid);
                }
                RecordType::Commit => {
                    result.needs_undo.remove(&tid);
                    result.needs_redo.insert(tid);
                }
                RecordType::Abort => {
                    result.needs_undo.insert(tid);
                }
                RecordType::End => {
                    result.needs_undo.remove(&tid);
                    result.needs_redo.remove(&tid);
                }
                RecordType::Delete => {
                    let undo_content = Box::from(record.undo_payload());
                    let oid = record
                        .metadata()
                        .object_id
                        .expect("Object id must be set for delete operations");
                    let row_id = record
                        .metadata()
                        .row_id
                        .expect("Row id must be set for delete operations");
                    let delete = Delete::new(oid, row_id, undo_content);
                    result.delete_ops.insert(lsn, delete);
                }
                RecordType::Update => {
                    let undo_content = Box::from(record.undo_payload());
                    let redo_content = Box::from(record.redo_payload());
                    let oid = record
                        .metadata()
                        .object_id
                        .expect("Object id must be set for update operations");
                    let row_id = record
                        .metadata()
                        .row_id
                        .expect("Row id must be set for update operations");
                    let update = Update::new(oid, row_id, undo_content, redo_content);
                    result.update_ops.insert(lsn, update);
                }
                RecordType::Insert => {
                    let redo_content = Box::from(record.redo_payload());
                    let oid = record
                        .metadata()
                        .object_id
                        .expect("Object id must be set for insert operations");
                    let row_id = record
                        .metadata()
                        .row_id
                        .expect("Row id must be set for insert operations");
                    let insert = Insert::new(oid, row_id, redo_content);
                    result.insert_ops.insert(lsn, insert);
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn push(&mut self, record: OwnedRecord) -> io::Result<()> {
        let lsn = record.lsn();
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

        let lsn = record.lsn();

        // Update global stats in header
        if self.header.metadata().wal_header.global_start_lsn.is_none() {
            self.header.metadata_mut().wal_header.global_start_lsn = Some(lsn);
        }
        self.header.metadata_mut().wal_header.global_last_lsn = Some(lsn);
        self.header.metadata_mut().wal_header.total_entries += 1;

        // Try to write to block zero first
        if self.current_block.is_none() {
            if self.header.available_space() >= record_size {
                self.header.try_push(lsn, record)?;
                return Ok(());
            }
            // Block zero is full, create first current_block
            self.current_block = Some(WalBlock::new(self.block_size));
        }

        // Write to current_block
        let block = self.current_block.as_mut().unwrap();

        if block.available_space() < record_size {
            self.rotate_block()?;
        }

        self.current_block.as_mut().unwrap().try_push(lsn, record)?;

        Ok(())
    }

    fn get_next_block(&mut self) -> BlockId {
        let total_blocks = &mut self.header.metadata_mut().wal_header.total_blocks;
        let current = *total_blocks;
        *total_blocks += 1;
        current
    }

    fn get_next_lsn(&mut self) -> Lsn {
        let mut last = self.header.metadata_mut().last_lsn();

        match last.as_mut() {
            Some(lsn) => {
                *lsn += 1;
                *lsn
            }
            None => {
                last = Some(0);
                0
            }
        }
    }

    fn rotate_block(&mut self) -> io::Result<()> {
        if let Some(full_block) = self.current_block.take() {
            self.flush_queue.push_back(full_block);
        }
        let next_id = self.get_next_block();
        self.current_block = Some(WalBlock::alloc(next_id, self.block_size));
        Ok(())
    }

    pub fn perform_flush(&mut self) -> io::Result<()> {
        // Block 0 always exists, additional blocks start at index 1
        let mut block_number: u64 = 1;
        let mut write_offset = self.block_size as u64;

        // Flush queued blocks
        while let Some(block) = self.flush_queue.pop_front() {
            self.file.seek(SeekFrom::Start(write_offset))?;
            self.file.write_all(block.as_ref())?;
            block_number += 1;
            write_offset += self.block_size as u64;
        }

        // Flush current block if it has data
        if let Some(ref block) = self.current_block {
            if block.metadata().used_bytes > 0 {
                self.file.seek(SeekFrom::Start(write_offset))?;
                self.file.write_all(block.as_ref())?;
                block_number += 1;
            }
        }

        // Update header metadata
        self.header.metadata_mut().wal_header.total_blocks = block_number;

        if let Some(block) = self.current_block.take() {
            self.header.metadata_mut().wal_header.last_block_used =
                block.metadata().used_bytes as u32;
        } else {
            self.header.metadata_mut().wal_header.last_block_used =
                self.header.metadata().block_header.used_bytes as u32;
        }

        // Write block zero (siempre al principio)
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(self.header.as_ref())?;

        self.file.sync_all()
    }

    fn write_header(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(self.header.as_ref())?;
        Ok(())
    }

    pub fn stats(&self) -> WalStats {
        let header = self.header.metadata();
        WalStats {
            start_lsn: header.wal_header.global_start_lsn.unwrap_or_default(),
            last_lsn: header.wal_header.global_last_lsn.unwrap_or_default(),
            total_entries: header.wal_header.total_entries,
            total_blocks: header.wal_header.total_blocks,
            pending_blocks: self.flush_queue.len(),
            block_size: self.block_size,
        }
    }

    pub(crate) fn reader(&mut self, read_ahead_amount: usize) -> io::Result<WalReader<'_>> {
        WalReader::new(
            &mut self.file,
            self.block_size * read_ahead_amount, // Small read-ahead to test reloading
            self.block_size,
            self.header.metadata().wal_header.total_blocks,
        )
    }
}

#[derive(Debug, Clone)]
pub struct WalStats {
    pub start_lsn: Lsn,
    pub last_lsn: Lsn,
    pub total_blocks: u64,
    pub total_entries: u32,
    pub pending_blocks: usize,
    pub block_size: usize,
}

pub(crate) struct WalReader<'a> {
    file: &'a mut DBFile,
    header: BlockZero,
    block_queue: Vec<WalBlock>,
    read_ahead_size: usize,
    current_block_offset: usize,
    current_block_index: Option<usize>, // None = reading from header, Some(i) = reading from block_queue[i]
    file_offset: u64,
    total_blocks: u64,
    block_size: usize,
}

impl<'a> WalReader<'a> {
    pub(crate) fn new(
        file: &'a mut DBFile,
        read_ahead_size: usize,
        block_size: usize,
        total_blocks: u64,
    ) -> io::Result<Self> {
        // Read block zero (header)
        let mut header = BlockZero::new(block_size);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(header.as_mut())?;

        let read_ahead_size = read_ahead_size.next_multiple_of(block_size);
        let num_blocks_to_read =
            (read_ahead_size / block_size).min(total_blocks.saturating_sub(1) as usize);

        let mut block_queue = Vec::with_capacity(num_blocks_to_read);
        let mut file_offset: u64 = block_size as u64;

        // Pre-load blocks (excluding block zero which is the header)
        for _ in 0..num_blocks_to_read {
            if file_offset >= (total_blocks as u64 * block_size as u64) {
                break;
            }
            let mut block = WalBlock::new(block_size);
            file.seek(SeekFrom::Start(file_offset))?;
            file.read_exact(block.as_mut())?;
            block_queue.push(block);
            file_offset += block_size as u64;
        }

        Ok(Self {
            file,
            header,
            block_queue,
            read_ahead_size,
            current_block_offset: 0,
            current_block_index: None, // Start reading from header
            file_offset,
            total_blocks,
            block_size,
        })
    }

    fn header_used_bytes(&self) -> usize {
        self.header.metadata().block_header.used_bytes as usize
    }

    fn reload_blocks(&mut self) -> io::Result<bool> {
        let last_valid_offset = self.total_blocks as u64 * self.block_size as u64;

        if self.file_offset >= last_valid_offset {
            return Ok(false);
        }

        self.block_queue.clear();
        let num_blocks = self.read_ahead_size / self.block_size;

        for _ in 0..num_blocks {
            if self.file_offset >= last_valid_offset {
                break;
            }

            // Allocates an uninitialized block, which will be read later, initializing its header and content.
            let mut block = WalBlock::new(self.block_size);
            self.file.seek(SeekFrom::Start(self.file_offset))?;
            self.file.read_exact(block.as_mut())?;
            self.block_queue.push(block);
            self.file_offset += self.block_size as u64;
        }

        Ok(!self.block_queue.is_empty())
    }

    pub(crate) fn next_ref(&mut self) -> io::Result<Option<RecordRef<'_>>> {
        loop {
            match self.current_block_index {
                // Reading from header (block zero)
                None => {
                    if self.current_block_offset >= self.header_used_bytes() {
                        // Header exhausted, move to block queue
                        if self.block_queue.is_empty() {
                            if !self.reload_blocks()? {
                                return Ok(None);
                            }
                        }
                        self.current_block_index = Some(0);
                        self.current_block_offset = 0;
                        continue; // Re-evaluate with new state
                    }

                    let record = self.header.record(self.current_block_offset as u64);
                    self.current_block_offset += record.total_size();
                    return Ok(Some(record));
                }

                // Reading from block queue
                Some(idx) => {
                    // Check if we need more blocks
                    if idx >= self.block_queue.len() {
                        if !self.reload_blocks()? {
                            return Ok(None);
                        }
                        self.current_block_index = Some(0);
                        self.current_block_offset = 0;
                        continue; // Re-evaluate with new state
                    }

                    let used = self.block_queue[idx].metadata().used_bytes as usize;

                    if self.current_block_offset >= used {
                        // Move to next block
                        self.current_block_index = Some(idx + 1);
                        self.current_block_offset = 0;
                        continue; // Re-evaluate with new state
                    }

                    let record = self.block_queue[idx].record(self.current_block_offset as u64);
                    self.current_block_offset += record.total_size();
                    return Ok(Some(record));
                }
            }
        }
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        self.flush().unwrap(); // Force panic if we fail to flush
    }
}
