use crate::header::Header;
use crate::io::cache::MemoryPool;
use crate::io::disk::{Buffer, FileOps};
use std::clone::Clone;
use crate::io::frames::IOFrame;
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::types::VarlenaType;

use crate::types::{Key, RowId};
use crate::{PageType, RQLiteConfig, HEADER_SIZE};

use crate::io::wal::{DeleteEntry, PutEntry, WalEntryType, WriteAheadLog};

use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const __WAL: &str = "rqlite-wal.db";
const __JOURNAL: &str = "rqlite-journal.db";

fn page_offset(page_id: PageId, page_size: u32) -> u32 {
    u32::from(page_id) * page_size + HEADER_SIZE as u32
}
/// Implementation of SQLite pager. Reference: [https://sqlite.org/src/file/src/pager.c]
pub struct Pager<F: FileOps, M: MemoryPool> {
    file: F,
    pub(crate) cache: M,
    journal: Journal,
    wal: WriteAheadLog,
    header: Header,
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
                println!("EL FRAME NO ESTABA DIRTY");
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
        let mut file = F::create(path.as_ref())?;
        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        let journal = Journal::create(journal_path)?;

        // Read the header of the DB and cache it.
        file.seek(SeekFrom::Start(0))?;
        let header = Header::default();
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        let wal = WriteAheadLog::create(wal_path)?;

        let cache = M::init();
        Ok(Self {
            file,
            wal,
            journal,
            cache,
            header,
        })
    }

    fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut file = F::open(path.as_ref())?;

        // Read the header of the DB and cache it.
        file.seek(SeekFrom::Start(0))?;
        let header = Header::read_from(&mut file)?;
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        let wal = WriteAheadLog::create(wal_path)?;

        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        let journal = Journal::create(journal_path)?;

        let cache = M::init();

        Ok(Self {
            file,
            wal,
            journal,
            cache,
            header,
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
    pub(crate) fn from_config(
        config: RQLiteConfig,
        path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self> {
        let mut file = F::create(path.as_ref())?;
        let mut journal_path = PathBuf::from(path.as_ref());
        journal_path.set_file_name(__JOURNAL);
        let journal = Journal::create(journal_path)?;

        // Read the header of the DB and cache it.
        file.seek(SeekFrom::Start(0))?;
        let header = Header::create(config);
        let mut cache = M::init();
        cache.set_capacity(header.default_cache_size as usize);
        header.clone().write_to(&mut file)?;
        let mut wal_path = PathBuf::from(path.as_ref());
        wal_path.set_file_name(__WAL);
        let wal = WriteAheadLog::create(wal_path)?;



        Ok(Self {
            file,
            wal,
            journal,
            cache,
            header,
        })
    }
    pub(crate) fn start_with(&mut self, cache_capacity: usize) -> std::io::Result<()> {
        self.cache.set_capacity(cache_capacity);

        Ok(())
    }

    pub(crate) fn clear_cache(&mut self) {
        let remaining = self.cache.clear();
        for page in remaining {
            self.write_page(page).unwrap();
        }
    }

    pub(crate) fn page_size(&self) -> u32 {
        self.header.page_size
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
        let offset = page_offset(page.id(), self.page_size());
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
        let start_offset = page_offset(start_page, self.page_size());

        self.seek(std::io::SeekFrom::Start(start_offset as u64))?;

        for _ in 0..num_pages {
            let page = IOFrame::read_from(self)?;
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
        let frame = IOFrame::create(new_page_id, page_type, self.page_size(), None);
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
            WalEntryType::Put(PutEntry {
                page_id: page_number,
                row_id,
                content,
            })
        } else {
            WalEntryType::Delete(DeleteEntry {
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
        println!("PAGINA ESCRITA EN EL JOURNAL {}", page.id());
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
