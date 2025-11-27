use crate::{
    AxmosDBConfig, DEFAULT_CACHE_SIZE, DEFAULT_PAGE_SIZE, PAGE_ALIGNMENT,
    io::{
        MemBuffer,
        cache::PageCache,
        disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
        frames::MemFrame,
        wal::{LogRecord, WriteAheadLog},
    },
    make_shared,
    storage::{
        buffer::BufferWithMetadata,
        page::{DatabaseHeader, Header, MemPage, OverflowPage, Page, PageZero},
    },
    types::{PAGE_ZERO, PageId},
};

use std::io::{self, Error as IoError, ErrorKind, Read, Seek, Write};

/// Implementation of a pager.
#[derive(Debug)]
pub struct Pager {
    file: DBFile,
    wal: WriteAheadLog, // TODO: Implement recoverability from the [WAL].
    cache: PageCache,
    header: DatabaseHeader,
}

impl Write for Pager {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Read for Pager {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for Pager {
    fn seek(&mut self, pos: std::io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
impl FileOperations for Pager {
    fn create(path: impl AsRef<std::path::Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = DBFile::create(&path)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::create(wal_path)?;
        let cache = PageCache::with_capacity(DEFAULT_CACHE_SIZE as usize);
        let header = DatabaseHeader::alloc(DEFAULT_PAGE_SIZE);

        Ok(Self {
            file,
            cache,
            wal,
            header,
        })
    }

    fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut file = DBFile::open(&path)?;
        let block_size = FileSystem::block_size(&path)?;
        let header = Self::load_db_header(&mut file, block_size)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::open(wal_path)?;
        let cache = PageCache::with_capacity(header.cache_size as usize);

        // TODO. MUST WRITE THE RECOVER FUNCTION TO REPLAY THE WAL HERE.
        Ok(Self {
            file,
            cache,
            wal,
            header,
        })
    }

    fn remove(path: impl AsRef<std::path::Path>) -> io::Result<()> {
        DBFile::remove(&path)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        WriteAheadLog::remove(&wal_path)?;
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.wal.sync_all()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.truncate()?;
        self.cache.clear();
        self.wal.truncate()?;
        Ok(())
    }
}

impl Pager {
    pub(crate) fn from_config(
        config: AxmosDBConfig,
        path: impl AsRef<std::path::Path>,
    ) -> io::Result<Self> {
        let mut pager = Pager::create(path)?;
        pager.init(config)?;
        Ok(pager)
    }
    pub(crate) fn init(&mut self, config: AxmosDBConfig) -> io::Result<()> {
        self.cache
            .set_capacity(config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE) as usize);

        let mut page_zero: PageZero = PageZero::alloc(config.page_size);
        page_zero.metadata_mut().page_size = config.page_size;
        page_zero.metadata_mut().incremental_vacuum_mode = config.incremental_vacuum_mode;
        page_zero.metadata_mut().min_keys = config.min_keys;
        page_zero.metadata_mut().text_encoding = config.text_encoding;
        page_zero.metadata_mut().cache_size = config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE);

        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(page_zero.as_ref())?;

        self.header = *page_zero.metadata();

        Ok(())
    }

    pub(crate) fn commit(&mut self) -> io::Result<()> {
        self.wal.flush()?;
        Ok(())
    }

    // TODO. MUST REPLAY THE WAL APPLYING ALL UNDO CHANGES
    pub(crate) fn rollback(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub fn push_to_log(&mut self, record: LogRecord) -> io::Result<()> {
        self.wal.push(record)
    }

    pub(crate) fn load_db_header(f: &mut DBFile, block_size: usize) -> io::Result<DatabaseHeader> {
        f.seek(std::io::SeekFrom::Start(0))?;
        let mut buf = MemBuffer::alloc(block_size, block_size)?;
        f.read_exact(&mut buf)?;

        // SAFETY: This is safe since the header is always written at the beggining of the file and alignment requirements are guaranteed.
        let header = unsafe {
            std::ptr::read(
                buf.as_ref()[..std::mem::size_of::<DatabaseHeader>()].as_ptr()
                    as *const DatabaseHeader,
            )
        };

        Ok(header)
    }

    pub(crate) fn config(&self) -> AxmosDBConfig {
        AxmosDBConfig {
            incremental_vacuum_mode: self.header.incremental_vacuum_mode,
            min_keys: self.header.min_keys,
            text_encoding: self.header.text_encoding,
            cache_size: Some(self.header.cache_size),
            page_size: self.page_size(),
        }
    }

    /// Compute the offset at which a page is placed given the database header size and the page id.
    /// [`PAGEZERO`] contains the header.
    ///
    /// Then all pages are placed sequentially, and as all pages are equal size, one can get the offset by computing the page id * size.
    ///
    /// So the layout of the database file is the following:
    ///
    /// [PAGE ZERO          ] Offset 0
    /// [PAGE 1             ] Offset 1 * Page size
    /// [PAGE 2             ] Offset 2 * Page size
    /// [PAGE 3             ] Offset 3 * Page size
    /// [....               ]
    fn page_offset(&self, page_id: PageId) -> u64 {
        u64::from(page_id) * self.page_size() as u64
    }

    fn min_keys_per_page(&self) -> usize {
        self.header.min_keys as usize
    }

    /// Write a block directly to the underlying disk using DirectIO.
    /// Must ensure that the content buffer is aligned.
    /// A block can contain [N] pages.
    /// As the page size is always aligned to the BLOCK SIZE for Direct IO, it should be safe to write and read back this way.
    fn write_block_unchecked(
        &mut self,
        start_page_number: PageId,
        content: &mut [u8],
    ) -> io::Result<()> {
        let offset = self.page_offset(start_page_number);
        debug_assert!(
            (offset as usize).is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content offset. Must be aligned!"
        );
        debug_assert!(
            (content.as_ptr() as usize).is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content ptr. Must be aligned!"
        );
        debug_assert!(
            content.len().is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content length. Must at least write a full page !"
        );
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(content)?;

        Ok(())
    }

    /// Read a block back from the disk into an output buffer.
    fn read_block_unchecked(
        &mut self,
        start_page_number: PageId,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        debug_assert!(
            (buffer.as_ptr() as usize).is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content ptr. Must be aligned!"
        );
        debug_assert!(
            buffer.len().is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content length. Must at least read a full page !"
        );
        let offset = self.page_offset(start_page_number);
        debug_assert!(
            (offset as usize).is_multiple_of(PAGE_ALIGNMENT as usize),
            "Invalid content offset. Must be aligned!"
        );
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    pub fn alloc_frame<P: Page>(&mut self) -> io::Result<MemFrame<MemPage>>
    where
        MemPage: From<P>,
        BufferWithMetadata<P::Header>: Into<MemPage>,
    {
        let free_page = self.header.first_free_page;
        let last_free = self.header.last_free_page;
        let (id, mut mem_page) = if free_page.is_valid() {
            let mut page = self.read_page::<OverflowPage>(free_page)?;
            debug_assert_eq!(
                free_page,
                page.read().page_number(),
                "MEMORY CORRUPTION. FREE PAGE ID SHOULD MATCH READ PAGE ID."
            );
            let next = page
                .try_with_variant::<OverflowPage, _, _, _>(|op| op.metadata().next)
                .unwrap();

            self.header.first_free_page = next;

            if last_free == free_page {
                self.header.last_free_page = PAGE_ZERO;
            };

            page.write().reinit_as::<P>();
            debug_assert_eq!(
                free_page,
                page.read().page_number(),
                "ERROR. EL NUMERO DE LA PAGINA LIBRE NO COINCIDE"
            );

            (free_page, page)
        } else {
            let page = P::alloc(self.page_size());
            let id = page.page_number();
            (id, MemFrame::new(MemPage::from(page)))
        };
        mem_page.mark_dirty();
        debug_assert_eq!(
            id,
            mem_page.read().page_number(),
            "ERROR. PAGE NUMBER DOES NOT MATCH THE PHYSICAL ID IN MEMORY"
        );

        Ok(mem_page)
    }

    pub fn cache_frame(&mut self, frame: MemFrame<MemPage>) -> io::Result<PageId> {
        let id = frame.read().page_number();
        if let Some(mut evicted) = self.cache.insert(id, frame) {
            let evicted_id = evicted.read().page_number();

            if evicted.is_dirty() {
                self.write_block_unchecked(evicted_id, evicted.write().as_mut())?;
            };
        };
        Ok(id)
    }

    pub fn read_page<P: Page>(&mut self, id: PageId) -> io::Result<MemFrame<MemPage>>
    where
        MemPage: From<P>,
    {
        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };

        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer = MemBuffer::alloc(self.page_size() as usize, PAGE_ALIGNMENT as usize)?;
        self.read_block_unchecked(id, &mut buffer)?;

        let page = P::try_from((&buffer, PAGE_ALIGNMENT as usize)).map_err(|msg| {
            IoError::new(
                ErrorKind::InvalidData,
                format!("Failed to convert page from bytes: {msg}"),
            )
        })?;

        debug_assert_eq!(
            page.page_number(),
            id,
            "PAGE NUMBER READ FROM DISK SHOULD MATCH THE ASKED PAGE NUMBER. OTHERWISE INDICATES MEMORY CORRUPTION"
        );

        let mem_page = MemFrame::new(MemPage::from(page));
        if let Some(mut evicted) = self.cache.insert(id, mem_page)
            && evicted.is_dirty()
        {
            let evicted_id = evicted.read().page_number();
            debug_assert!(
                self.cache.get(&evicted_id).is_none(),
                "MEMORY CORRUPTION DETECTED. EVICTED PAGE SHOULD NOT BE KEPT ON CACHE"
            );
            debug_assert_eq!(
                evicted.read().page_number(),
                evicted_id,
                "PAGE NUMBER READ FROM DISK SHOULD MATCH THE ASKED PAGE NUMBER. OTHERWISE INDICATES MEMORY CORRUPTION"
            );
            self.write_block_unchecked(evicted_id, evicted.write().as_mut())?;
        };

        Ok(self.cache.get(&id).unwrap())
    }

    pub fn dealloc_page<P: Page>(&mut self, id: PageId) -> io::Result<()>
    where
        MemPage: From<P>,
    {
        let last_free_page = self.header.last_free_page;
        let first_free_page = self.header.first_free_page;

        // Set the first free page in the header
        if !first_free_page.is_valid() {
            self.header.first_free_page = id;
        };

        if last_free_page.is_valid() {
            // Read the free page and set the next page.
            let mut previous_page = self.read_page::<OverflowPage>(last_free_page)?;
            // The previous page should be a free page.
            // SAFETY: It should be safe to unwrap the result as we have already read the page as a free page.

            previous_page
                .try_with_variant_mut::<OverflowPage, _, _, _>(|free| {
                    free.metadata_mut().next = id;
                })
                .unwrap(); // This should not panic as we have already read the page as a free page.
        };
        let mut mem_page = self.read_page::<P>(id)?;
        debug_assert!(
            mem_page.read().page_number() == id,
            "MEMORY CORRUPTION. ID IN CACHE DOES NOT MATCH PHYSICAL ID"
        );
        self.header.last_free_page = id;
        mem_page.write().dealloc();

        // Probably not necessary
        self.write_block_unchecked(id, mem_page.write().as_mut())?;

        Ok(())
    }
}

make_shared! {SharedPager, Pager}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::page::{BTREE_PAGE_HEADER_SIZE, BtreePage};
    use serial_test::serial;
    use std::path::Path;
    use tempfile::tempdir;

    fn create_test_pager(path: impl AsRef<Path>, cache_size: usize) -> io::Result<Pager> {
        let dir = tempdir()?;
        let path = dir.path().join(&path);

        let config = crate::AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(cache_size as u16),
            incremental_vacuum_mode: crate::IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: crate::TextEncoding::Utf8,
        };

        let mut pager = Pager::create(&path)?;
        pager.init(config)?;
        Ok(pager)
    }

    #[test]
    #[serial]
    fn test_single_page_rw() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let mut page1 = BtreePage::alloc(page_size);
        let page_id = page1.metadata().page_number;
        assert!(page_id != crate::types::PAGE_ZERO, "Invalid page number!");
        let result = pager.write_block_unchecked(page_id, page1.as_mut());
        assert!(result.is_ok(), "Page writing to disk failed!");
        let mut buf = MemBuffer::alloc(pager.page_size() as usize, PAGE_ALIGNMENT as usize)?;
        pager.read_block_unchecked(page_id, &mut buf)?;
        assert_eq!(
            buf.as_ref(),
            page1.as_ref(),
            "Retrieved content should match the allocated page!!"
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn test_multiple_pages_rw() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let mut pages = Vec::new();
        let num_pages = 10;
        let total_size = num_pages * page_size;

        let mut raw_buffer = MemBuffer::alloc(total_size as usize, PAGE_ALIGNMENT as usize)?;

        let mut offset = 0;
        // Accumulate all pages in our buffers.
        for i in 0..num_pages {
            let page = BtreePage::alloc(page_size);
            let page_id = page.metadata().page_number;
            assert!(
                page_id != crate::types::PAGE_ZERO,
                "Invalid page number for Pg {i}!"
            );
            let page_bytes = page.as_ref();
            raw_buffer.as_mut()[offset..offset + page_bytes.len()].copy_from_slice(page_bytes);
            offset += page_bytes.len();
            pages.push(page);
        }
        // When writing a lot of pages we need to ensure the block size is aligned.
        let total_page_size: usize = pages.iter().map(|p| p.as_ref().len()).sum();
        assert_eq!(
            total_page_size, total_size as usize,
            "Total size was not as expected"
        );
        pager.write_block_unchecked(pages[0].metadata().page_number, &mut raw_buffer)?;

        for page in pages {
            // Try to read page 1 back.
            let mut buf =
                MemBuffer::alloc(page.metadata().page_size as usize, PAGE_ALIGNMENT as usize)?;

            pager.read_block_unchecked(page.metadata().page_number, buf.as_mut())?;
            assert_eq!(
                buf.as_ref(),
                page.as_ref(),
                "Retrieved content should match the allocated page!!"
            );
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn test_cache_eviction() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let cache_size = 2; // Set a small cache capacity to force eviction.
        pager.cache = PageCache::with_capacity(cache_size);

        // Assigns more pages than those that would fit in the cache
        let mut page_ids = Vec::new();
        for _ in 0..2 {
            let frame = pager.alloc_frame::<BtreePage>()?;
            let id = pager.cache_frame(frame)?;

            page_ids.push(id);
        }
        {
            // Write some content on page 1.
            let mut page1 = pager.read_page::<BtreePage>(page_ids[0])?;

            page1
                .try_with_variant_mut::<BtreePage, _, _, _>(|p| {
                    p.metadata_mut().right_child = PageId::from(311);
                })
                .unwrap();
        };

        // Allocate a new page.
        let frame = pager.alloc_frame::<BtreePage>()?;
        let id = pager.cache_frame(frame)?;
        page_ids.push(id);

        // First page should have been expelled.
        assert!(pager.cache.get(&page_ids[0]).is_none());
        let read_page = pager.read_page::<BtreePage>(page_ids[0])?;

        read_page
            .try_with_variant::<BtreePage, _, _, _>(|p| {
                assert_eq!(p.metadata().right_child, PageId::from(311));
            })
            .unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    #[cfg(not(miri))]
    fn test_dirty_page() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        pager.cache = PageCache::with_capacity(1); // Cache pequeña

        // Create a dirty page
        let frame = pager.alloc_frame::<BtreePage>()?;
        let id1 = pager.cache_frame(frame)?;
        {
            let mut frame = pager.cache.get(&id1).unwrap();
            frame.mark_dirty();
        }

        // Force eviction with a dirty page
        let frame = pager.alloc_frame::<BtreePage>()?;
        let _id_2 = pager.cache_frame(frame)?;

        // Verify it has been written out to disk.
        let mut buf = MemBuffer::alloc(pager.page_size() as usize, PAGE_ALIGNMENT as usize)?;
        pager.read_block_unchecked(id1, buf.as_mut())?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_dealloc_page() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;

        // Allocate and deallocate pages
        let frame = pager.alloc_frame::<BtreePage>()?;
        let id1 = pager.cache_frame(frame)?;

        let frame = pager.alloc_frame::<BtreePage>()?;
        let id2 = pager.cache_frame(frame)?;

        pager.dealloc_page::<BtreePage>(id1)?;

        // Verify they are marked as free
        assert_eq!(pager.header.first_free_page, id1);
        assert_eq!(pager.header.last_free_page, id1);

        // Deallocate the second page
        pager.dealloc_page::<BtreePage>(id2)?;

        // Check that the free list was properly built.
        let page1 = pager.read_page::<OverflowPage>(id1)?;

        let next_free = page1
            .try_with_variant::<OverflowPage, _, _, _>(|free| free.metadata().next)
            .unwrap();

        assert_eq!(next_free, id2);
        assert_eq!(pager.header.last_free_page, id2);

        Ok(())
    }

    #[test]
    #[serial]
    fn test_page_reusability() -> io::Result<()> {
        let mut pager = create_test_pager("test.db", 2)?;
        let num_pages = 350;
        let num_cells = 15;
        let mut ids: std::collections::HashSet<PageId> =
            std::collections::HashSet::with_capacity(num_pages as usize);

        for _ in 0..num_pages {
            let frame = pager.alloc_frame::<BtreePage>()?;
            let id = pager.cache_frame(frame)?;
            ids.insert(id);
        }

        // Allocate and deallocate pages
        for i in ids.iter() {
            let mut page = pager.read_page::<BtreePage>(*i)?;
            for i in 0..num_cells {
                let cell = crate::storage::cell::Cell::new(b"Hello");
                page.try_with_variant_mut::<BtreePage, _, _, _>(|p| {
                    p.push(cell.clone());
                })
                .unwrap();
            }

            let mut removed = Vec::new();

            page.try_with_variant_mut::<BtreePage, _, _, _>(|p| {
                removed.extend(p.drain(..));
                p.metadata_mut().free_space_ptr = 4096 - BTREE_PAGE_HEADER_SIZE as u32;
            })
            .unwrap();

            pager.dealloc_page::<BtreePage>(*i)?;
        }

        // Allocate and deallocate pages
        for _ in 0..num_pages {
            let frame = pager.alloc_frame::<BtreePage>()?;
            let id = pager.cache_frame(frame)?;
            assert!(ids.contains(&id), "SHOULD REUSE A DEALLOCATED PAGE");
            let read_page = pager.read_page::<BtreePage>(id)?;
            read_page
                .try_with_variant::<BtreePage, _, _, _>(|page| {
                    assert!(
                        page.page_number() == id,
                        "MEMORY CORRUPTION. ASKED FOR PAGE {id} BUT GOT {}",
                        page.page_number()
                    );
                    assert!(page.is_empty(), "PAGE SHOULD BE EMPTY AFTER REALLOCATING");
                })
                .unwrap();
        }

        Ok(())
    }
}
