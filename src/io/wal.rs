use crate::io::disk::{Buffer, FileOps};
use crate::serialization::Serializable;
use crate::types::{Key, LogId, PageId, TxId, VarlenaType};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

// Log type representation.
// Redo logs store the database state after the transaction.
// Used for recovery after failures and eventual flushing to disk.
// Undo logs store the previous database state before the transaction started.
// Used for rollback of changes.
crate::serializable_enum!(pub(crate) enum LogRecordType: u8 {
    Update = 0x00,
    Commit = 0x01,
    Abort = 0x02,
    End = 0x03 // Signifies end of commit or abort
});

#[derive(Debug, Clone)]
pub(crate) struct WalEntry {
    /// Auto-incrementing log sequence number
    pub(crate) lsn: LogId,
    pub(crate) txid: TxId, // Id of the current transaction
    /// Log id of the previous LSN of the current transaction.
    pub(crate) prev_lsn: LogId,
    pub(crate) page_id: PageId,
    pub(crate) log_type: LogRecordType,

    pub(crate) slot_id: u16,
    /// Content in the cell BEFORE the TX started.
    pub(crate) undo_content: VarlenaType,
    /// Content in the cell AFTER the TX started.
    pub(crate) redo_content: VarlenaType,
}

impl Serializable for WalEntry {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // Read each field in the order they appear in the struct
        let lsn = LogId::read_from(reader)?;
        let txid = TxId::read_from(reader)?;
        let prev_lsn = LogId::read_from(reader)?;
        let page_id = PageId::read_from(reader)?;

        // Read log_type (assuming LogRecordType uses u8 representation)
        let mut log_type_byte = [0u8; 1];
        reader.read_exact(&mut log_type_byte)?;
        let log_type = LogRecordType::try_from(log_type_byte[0])?;

        // Read slot_id (2 bytes, big-endian)
        let mut slot_id_bytes = [0u8; 2];
        reader.read_exact(&mut slot_id_bytes)?;
        let slot_id = u16::from_be_bytes(slot_id_bytes);

        // Read undo and redo content
        let undo_content = VarlenaType::read_from(reader)?;
        let redo_content = VarlenaType::read_from(reader)?;

        Ok(Self {
            lsn,
            txid,
            prev_lsn,
            page_id,
            log_type,
            slot_id,
            undo_content,
            redo_content,
        })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        // Write each field in the same order as read_from
        self.lsn.write_to(writer)?;
        self.txid.write_to(writer)?;
        self.prev_lsn.write_to(writer)?;
        self.page_id.write_to(writer)?;

        // Write log_type as a byte
        writer.write_all(&[self.log_type as u8])?;

        // Write slot_id (2 bytes, big-endian)
        writer.write_all(&self.slot_id.to_be_bytes())?;

        // Write undo and redo content
        self.undo_content.write_to(writer)?;
        self.redo_content.write_to(writer)?;

        Ok(())
    }
}

pub(crate) struct WriteAheadLog {
    file: File,
    buffer: Buffer,
    buffer_size: usize,
    entries_since_flush: usize,
}

impl WriteAheadLog {
    pub fn set_bufsize(&mut self, bufsize: usize) {
        self.buffer_size = bufsize;
        let cap = bufsize.saturating_sub(self.buffer.capacity());
        self.buffer.reserve_capacity(cap);
    }
}

impl FileOps for WriteAheadLog {
    fn create(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let file = File::create(path)?;

        Ok(Self {
            file,
            buffer: Buffer::with_capacity(0),
            buffer_size: 0,
            entries_since_flush: 0,
        })
    }

    /// Open an existent WAL
    fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let file = File::open(&path)?;

        let buffer = Buffer::open(&path)?;
        let buffer_size = buffer.capacity();

        Ok(Self {
            file,
            buffer,
            buffer_size,
            entries_since_flush: 0,
        })
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.truncate()?;
        self.buffer.truncate()?;
        self.entries_since_flush = 0;
        Ok(())
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        File::remove(&path)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

impl Write for WriteAheadLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buffer.seek(SeekFrom::Start(0))?;
        self.file.write_all(self.buffer.as_slice())?;
        self.buffer.truncate()?;
        self.file.flush()?;
        Ok(())
    }
}

impl Read for WriteAheadLog {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.buffer.read(buf)
    }
}

impl Seek for WriteAheadLog {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.buffer.seek(pos)
    }
}

impl WriteAheadLog {
    pub(crate) fn append(&mut self, log: WalEntry) -> std::io::Result<()> {
        log.write_to(self)?;
        Ok(())
    }
}

pub(crate) struct WalIterator<'a> {
    wal: &'a mut WriteAheadLog,
    finished: bool,
}

impl<'a> IntoIterator for &'a mut WriteAheadLog {
    type Item = io::Result<WalEntry>;
    type IntoIter = WalIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.buffer.seek(SeekFrom::Start(0)).unwrap();
        WalIterator {
            wal: self,
            finished: false,
        }
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = io::Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match WalEntry::read_from(&mut self.wal) {
            Ok(entry) => Some(Ok(entry)),
            Err(err) => {
                // EOF indicates that we reached the end of the buffer
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    self.finished = true;
                    None
                } else {
                    self.finished = true;
                    Some(Err(err))
                }
            }
        }
    }
}

pub(crate) fn create_update_entry(
    page_id: PageId,
    slot_id: u16,
    transaction_id: TxId,
    prev_lsn: Option<LogId>,
    undo_content: VarlenaType,
    redo_content: VarlenaType,
) -> WalEntry {
    let lsn = LogId::new_key();
    let log_type = LogRecordType::Update;

    WalEntry {
        lsn,
        txid: transaction_id,
        page_id,
        slot_id,
        log_type,
        prev_lsn: prev_lsn.unwrap_or(LogId::from(0)),
        undo_content,
        redo_content,
    }
}

pub(crate) fn create_delete_entry(
    page_id: PageId,
    slot_id: u16,
    transaction_id: TxId,
    prev_lsn: Option<LogId>,
) -> WalEntry {
    let lsn = LogId::new_key();
    let log_type = LogRecordType::Update;

    WalEntry {
        lsn,
        txid: transaction_id,
        page_id,
        slot_id,
        log_type,
        prev_lsn: prev_lsn.unwrap_or(LogId::from(0)),
        undo_content: VarlenaType::from_raw_bytes(&[], None),
        redo_content: VarlenaType::from_raw_bytes(&[], None),
    }
}
