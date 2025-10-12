use crate::header::Header;
use crate::io::cache::MemoryPool;
use crate::io::disk::{Buffer, FileOps};
use crate::io::frames::create_frame;
use crate::io::frames::{Frame, IOFrame, PageFrame};
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::types::VarlenaType;
use crate::types::{byte::FALSE, byte::TRUE};
use crate::types::{Key, RowId};
use crate::{PageType, HEADER_SIZE};

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const __WAL: &str = "rqlite-wal.db";
const __JOURNAL: &str = "rqlite-journal.db";

pub(crate) struct __PutEntry {
    pub(crate) page_id: PageId,
    pub(crate) row_id: RowId,
    pub(crate) content: VarlenaType,
}

impl Serializable for __PutEntry {
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

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.page_id.write_to(writer)?;
        self.row_id.write_to(writer)?;
        self.content.write_to(writer)?;
        Ok(())
    }
}

pub(crate) struct __DeleteEntry {
    pub(crate) page_id: PageId,
    pub(crate) row_id: RowId,
}

impl Serializable for __DeleteEntry {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let page_id = PageId::read_from(reader)?;
        let row_id = RowId::read_from(reader)?;

        Ok(Self { page_id, row_id })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.page_id.write_to(writer)?;
        self.row_id.write_to(writer)?;

        Ok(())
    }
}

pub(crate) enum __WalEntryType {
    Put(__PutEntry),
    Delete(__DeleteEntry),
}

impl Serializable for __WalEntryType {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let byte = crate::types::byte::Byte::read_from(reader)?;
        match byte {
            TRUE => {
                let put = __PutEntry::read_from(reader)?;
                Ok(Self::Put(put))
            },
            FALSE => {
                let delete = __DeleteEntry::read_from(reader)?;
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

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            __WalEntryType::Put(__PutEntry {
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
            __WalEntryType::Delete(__DeleteEntry { page_id, row_id }) => {
                FALSE.write_to(writer)?;
                page_id.write_to(writer)?;
                row_id.write_to(writer)?;
                Ok(())
            }
        }
    }
}

/// Implementation of SQLite pager. Reference: [https://sqlite.org/src/file/src/pager.c]
pub struct Pager<F: FileOps, M: MemoryPool> {
    file: F,
    cache: M,
    journal: Journal,
    wal: WriteAheadLog,
    header: Option<Header>,
}

impl<F: FileOps, M: MemoryPool> Read for Pager<F, M> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.file.read_exact(buf)
    }
}

impl<F: FileOps, M: MemoryPool> Seek for Pager<F, M> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl<F: FileOps, M: MemoryPool> Write for Pager<F, M> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        while let Some(frame) = self.cache.evict(None) {
            if frame.is_dirty() {
                self.journal.push_page(frame)?;
            }
        }

        let mut journalled_pages: Vec<IOFrame> = Vec::new();

        for page in self.journal.iter() {
            journalled_pages.push(page);
        }

        for page in journalled_pages {
            self.write_page(page)?;
        }

        self.file.flush()
    }
}

impl<F: FileOps, M: MemoryPool> FileOps for Pager<F, M> {
    fn create(path: impl AsRef<std::path::Path>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let file = F::create(&path)?;
        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        let journal = Journal::create(journal_path)?;
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        let wal = WriteAheadLog::create(wal_path)?;

        let cache = M::init();
        Ok(Self {
            file,
            wal,
            journal,
            cache,
            header: None,
        })
    }

    fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        let wal = WriteAheadLog::open(wal_path)?;

        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        let journal = Journal::open(journal_path)?;

        let file = F::open(path)?;

        let cache = M::init();

        Ok(Self {
            file,
            wal,
            journal,
            cache,
            header: None,
        })
    }

    fn remove(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        F::remove(wal_path)?;

        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        F::remove(journal_path)?;
        F::remove(path)?;
        Ok(())
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.file.truncate()?;
        self.wal.truncate()?;
        self.journal.truncate()?;
        self.cache.clear();
        self.header = None;
        Ok(())
    }

    /// Todo: write dirty pages to disk here.
    fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()?;
        self.wal.sync_all()?;
        self.journal.sync_all()?;

        Ok(())
    }
}

impl<F: FileOps, M: MemoryPool> Pager<F, M> {
    pub(crate) fn start(&mut self, cache_capacity: usize) -> std::io::Result<()> {
        self.seek(std::io::SeekFrom::Start(0))?;
        let header = Header::read_from(self)?;
        self.header = Some(header);
        self.cache.set_capacity(cache_capacity);

        Ok(())
    }

    pub(crate) fn page_size(&self) -> u32 {
        self.header
            .as_ref()
            .expect("Header is not set. Must first open the pager in order to read the page size")
            .page_size
    }

    pub(crate) fn try_get_from_cache(&mut self, page_number: PageId) -> Option<IOFrame> {
        if let Some(frame) = self.cache.get(&page_number) {
            return Some(frame);
        }

        None
    }

    pub(crate) fn cache_page(&mut self, page: &IOFrame) -> Option<IOFrame> {
        self.cache.cache(page.clone())
    }

    /// Write a page directly to the disk
    pub(crate) fn write_page(&mut self, page: IOFrame) -> std::io::Result<()> {
        let offset = u32::from(page.id()) * self.page_size() + HEADER_SIZE as u32;
        // Get the current buffer position: equivalent to ftell
        if self.stream_position()? != offset as u64 {
            self.seek(SeekFrom::Start(offset as u64))?;
        }
        page.write_to(self)?;
        Ok(())
    }

    /// Read a range of pages directly from the disk
    pub(crate) fn read_pages(
        &mut self,
        start_page: PageId,
        num_pages: u32,
    ) -> std::io::Result<Vec<IOFrame>> {
        let mut requested = Vec::with_capacity(num_pages as usize);
        let start_offset = u32::from(start_page) * self.page_size() + HEADER_SIZE as u32;

        self.seek(std::io::SeekFrom::Start(start_offset as u64))?;

        for _ in 0..num_pages {
            let page = PageFrame::read_from(self)?;
            requested.push(page);
        }
        Ok(requested)
    }

    pub(crate) fn get(
        &mut self,
        page_number: PageId,
        required_pages: u32,
    ) -> std::io::Result<Vec<IOFrame>> {
        let mut pages = Vec::with_capacity(required_pages as usize);
        let mut req_page_number = page_number;
        for i in 0..required_pages {
            req_page_number = page_number + PageId::from(i);
            // If the page is on the cache, take it from the cache
            if let Some(handle) = self.try_get_from_cache(req_page_number) {
                pages.push(handle);
                continue;
            }
            // If we reach this part it means  that he page was not on the cache and therefore we need to spin the disk for the rest
            break;
        }

        // Early return if we reached the requested limit
        if pages.len() as u32 == required_pages {
            return Ok(pages);
        }

        let pages_left = required_pages - pages.len() as u32;

        let from_disk = self.read_pages(req_page_number, pages_left)?;

        // Try cache all pages that came from disk
        for page in from_disk.iter() {
            if let Some(evicted) = self.cache_page(page) {
                if evicted.is_dirty() {
                    self.journal.push_page(evicted)?;
                }
            }
        }

        pages.extend(from_disk);

        Ok(pages)
    }

    pub(crate) fn get_single(&mut self, page_number: PageId) -> std::io::Result<IOFrame> {
        Ok(self.get(page_number, 1)?[0].clone())
    }

    pub(crate) fn alloc_page(&mut self, page_type: PageType) -> io::Result<IOFrame> {
        let new_page_id = PageId::new_key();
        let frame = create_frame(new_page_id, page_type, self.page_size(), None)?;
        if let Some(evicted) = self.cache_page(&frame) {
            if evicted.is_dirty() {
                self.journal.push_page(evicted)?;
            }
        }
        Ok(frame)
    }

    fn log_tuple(
        &mut self,
        page_number: PageId,
        row_id: RowId,
        new_content: Option<VarlenaType>,
    ) -> io::Result<()> {
        let entry = if let Some(content) = new_content {
            __WalEntryType::Put(__PutEntry {
                page_id: page_number,
                row_id,
                content,
            })
        } else {
            __WalEntryType::Delete(__DeleteEntry {
                page_id: page_number,
                row_id,
            })
        };

        entry.write_to(&mut self.wal)?;
        Ok(())
    }
}

pub struct Journal {
    page_size: u32,
    buffer: Buffer,
    file: File,
}

impl Journal {
    pub fn push_page(&mut self, page: IOFrame) -> io::Result<()> {
        page.write_to(&mut self.buffer)?;
        Ok(())
    }

    fn push_pages(&mut self, pages: Vec<IOFrame>) -> io::Result<()> {
        for page in pages {
            self.push_page(page)?;
        }
        Ok(())
    }

    pub fn set_page_size(&mut self, page_size: u32) {
        self.page_size = page_size
    }

    pub fn iter(&mut self) -> JournalIterator<'_> {
        JournalIterator { journal: self }
    }
}

impl FileOps for Journal {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = File::create(&path)?;
        let buffer = Buffer::create(&path)?;
        Ok(Self {
            page_size: 0,
            buffer,
            file,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let buffer = Buffer::open(&path)?;
        // The buffer will automatically read the contents of the file.
        let file = File::open(&path)?;
        Ok(Self {
            page_size: 0,
            file,
            buffer,
        })
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        File::remove(path)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.buffer.truncate()?;
        self.file.truncate()
    }
}

impl Write for Journal {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.truncate()?;
        self.file.seek(SeekFrom::Start(0))?;

        self.file.write_all(self.buffer.as_slice())?;
        self.buffer.truncate()?;
        Ok(())
    }
}

impl Seek for Journal {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.buffer.seek(pos)
    }
}

impl Read for Journal {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.buffer.read(buf)
    }
}

pub(crate) struct JournalIterator<'a> {
    journal: &'a mut Journal,
}

impl<'a> Iterator for JournalIterator<'a> {
    type Item = IOFrame;

    fn next(&mut self) -> Option<Self::Item> {
        IOFrame::read_from(&mut self.journal).ok()
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
        let entry = __WalEntryType::Put(__PutEntry {
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
        let entry = __WalEntryType::Delete(__DeleteEntry { page_id, row_id });

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
            match __WalEntryType::read_from(&mut self.buffer) {
                Ok(entry) => entries.push(entry),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        if entries.is_empty() {
            return Ok(());
        }

        // Deduplicate
        let mut latest: HashMap<(PageId, RowId), __WalEntryType> = HashMap::new();
        for entry in entries {
            let key = match &entry {
                __WalEntryType::Put(put) => (put.page_id, put.row_id),
                __WalEntryType::Delete(del) => (del.page_id, del.row_id),
            };
            latest.insert(key, entry);
        }

        // Rewrite the buffer
        self.buffer.truncate()?;

        for entry in latest.values() {
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
