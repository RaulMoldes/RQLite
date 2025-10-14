use crate::io::disk::{Buffer, FileOps};
use crate::serialization::Serializable;
use crate::types::{
    byte::{FALSE, TRUE},
    PageId, RowId, VarlenaType,
};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct PutEntry {
    pub(crate) page_id: PageId,
    pub(crate) row_id: RowId,
    pub(crate) content: VarlenaType,
}

impl Serializable for PutEntry {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let page_id = PageId::read_from(reader)?;
        let row_id = RowId::read_from(reader)?;
        let content = VarlenaType::read_from(reader)?;
        Ok(Self {
            page_id,
            row_id,
            content,
        })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        self.page_id.write_to(writer)?;
        self.row_id.write_to(writer)?;
        self.content.write_to(writer)?;
        Ok(())
    }
}

pub(crate) struct DeleteEntry {
    pub(crate) page_id: PageId,
    pub(crate) row_id: RowId,
}

impl Serializable for DeleteEntry {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let page_id = PageId::read_from(reader)?;
        let row_id = RowId::read_from(reader)?;

        Ok(Self { page_id, row_id })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        self.page_id.write_to(writer)?;
        self.row_id.write_to(writer)?;

        Ok(())
    }
}

pub(crate) enum WalEntryType {
    Put(PutEntry),
    Delete(DeleteEntry),
}

impl Serializable for WalEntryType {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let byte = crate::types::byte::Byte::read_from(reader)?;
        match byte {
            TRUE => {
                let put = PutEntry::read_from(reader)?;
                Ok(Self::Put(put))
            },
            FALSE => {
                let delete = DeleteEntry::read_from(reader)?;
                Ok(Self::Delete(delete))

            }
            _ => {
                Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Found an invalid data marker in WAL file: {}, it should be {} (PUT) or {} (DELETE)", byte, TRUE, FALSE
                ),
            ))
            }
        }
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        match self {
            WalEntryType::Put(PutEntry {
                page_id,
                row_id,
                content,
            }) => {
                // Write a marker
                TRUE.write_to(writer)?;
                page_id.write_to(writer)?;
                row_id.write_to(writer)?;
                content.write_to(writer)?;
                Ok(())
            }
            WalEntryType::Delete(DeleteEntry { page_id, row_id }) => {
                FALSE.write_to(writer)?;
                page_id.write_to(writer)?;
                row_id.write_to(writer)?;
                Ok(())
            }
        }
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
    pub fn log_put(
        &mut self,
        page_id: PageId,
        row_id: RowId,
        content: VarlenaType,
    ) -> io::Result<()> {
        let entry = WalEntryType::Put(PutEntry {
            page_id,
            row_id,
            content,
        });

        entry.write_to(&mut self.buffer)?;
        self.entries_since_flush += 1;

        if self.entries_since_flush >= self.buffer_size {
            self.compact()?;
            self.auto_flush()?;
        }

        Ok(())
    }

    pub fn log_delete(&mut self, page_id: PageId, row_id: RowId) -> io::Result<()> {
        let entry = WalEntryType::Delete(DeleteEntry { page_id, row_id });

        entry.write_to(&mut self.buffer)?;
        self.entries_since_flush += 1;

        if self.entries_since_flush >= self.buffer_size {
            self.compact()?;
            self.auto_flush()?;
        }

        Ok(())
    }

    pub fn auto_flush(&mut self) -> io::Result<()> {
        if self.entries_since_flush == 0 {
            return Ok(());
        }

        self.flush()?;
        self.sync_all()?;
        self.entries_since_flush = 0;

        Ok(())
    }

    pub fn compact(&mut self) -> io::Result<()> {
        self.buffer.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();

        loop {
            match WalEntryType::read_from(&mut self.buffer) {
                Ok(entry) => entries.push(entry),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        if entries.is_empty() {
            return Ok(());
        }

        // Deduplicate
        let mut latest: HashMap<(PageId, RowId), WalEntryType> = HashMap::new();
        for entry in entries {
            let key = match &entry {
                WalEntryType::Put(put) => (put.page_id, put.row_id),
                WalEntryType::Delete(del) => (del.page_id, del.row_id),
            };
            latest.insert(key, entry);
        }

        // Rewrite the buffer
        self.buffer.truncate()?;

        for entry in latest.into_values() {
            entry.write_to(&mut self.buffer)?;
        }
        Ok(())
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        let _ = self.compact();
        let _ = self.auto_flush();
        let _ = self.sync_all();
    }
}
