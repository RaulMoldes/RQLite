use crate::{
    AxmosDBConfig, DEFAULT_CACHE_SIZE,  PAGE_ALIGNMENT,
    io::{
        cache::PageCache,
        disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
        frames::{ MemFrame},
        wal::WriteAheadLog,
    },
    make_shared,
    storage::{
        buffer::{Allocatable, MemBlock},
        latches::PageLatch,
        page::{ OverflowPage, OverflowPageHeader, PageZero, PageZeroHeader},
        wal::OwnedRecord,
    },
    types::{PAGE_ZERO, PageId},
};

use std::{
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},

    path::Path,

};

/// Implementation of a pager.
#[derive(Debug)]
pub struct Pager {
    file: DBFile,
    wal: WriteAheadLog, // TODO: Implement recoverability from the [WAL].
    cache: PageCache,
    page_zero: Option<MemFrame>, // Page zero is always in memory
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
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
impl FileOperations for Pager {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = DBFile::create(&path)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::create(wal_path)?;
        let cache = PageCache::with_capacity(DEFAULT_CACHE_SIZE as usize);

        Ok(Self {
            file,
            cache,
            wal,
            page_zero: None,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = DBFile::open(&path)?;
        let block_size = FileSystem::block_size(&path)?;
        let cache = PageCache::with_capacity(DEFAULT_CACHE_SIZE);
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::open(wal_path)?;

        let mut pager = Self {
            file,
            cache,
            wal,
            page_zero: None,
        };

        let mut page_zero = pager.load_page_zero(block_size)?;
        let actual_block_size = page_zero.metadata().page_size;

        // If the target page size happens to be greater than the filesystem block size we might have to reload.
        if actual_block_size > block_size as u32 {
            page_zero = pager.load_page_zero(actual_block_size as usize)?;
        };

        pager.page_zero = Some(MemFrame::from(page_zero));

        // TODO. MUST WRITE THE RECOVER FUNCTION TO REPLAY THE WAL HERE.
        Ok(pager)
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
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
    fn page_offset(page_id: PageId, page_size: u64) -> u64 {
        u64::from(page_id) * page_size
    }

    pub(crate) fn from_config(config: AxmosDBConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        let mut pager = Pager::create(path)?;
        let page_zero = pager.alloc_page_zero(config)?;
        pager.page_zero = Some(MemFrame::from(page_zero));
        Ok(pager)
    }

    /// Allocates page zero from the provided configuration
    pub(crate) fn alloc_page_zero(&mut self, config: AxmosDBConfig) -> io::Result<PageZero> {
        self.cache.set_capacity(config.cache_size as usize);

        let mut page_zero: PageZero = PageZero::alloc(config);
        page_zero.metadata_mut().page_size = config.page_size;
        page_zero.metadata_mut().min_keys = config.min_keys_per_page;
        page_zero.metadata_mut().cache_size = config.cache_size;
        page_zero.metadata_mut().num_siblings_per_side = config.num_siblings_per_side;

        self.write_block(PAGE_ZERO, page_zero.as_ref())?;

        Ok(page_zero)
    }

    /// Push a record to the write ahead log (implementation is not complete)
    pub(crate) fn push_to_log(&mut self, record: OwnedRecord) -> io::Result<()> {
        self.wal.push(record)
    }

    /// Load block zero from disk
    pub(crate) fn load_page_zero(&mut self, block_size: usize) -> io::Result<PageZero> {
        self.load_block::<PageZeroHeader>(0, block_size)
    }

    /// Load a block from disk at the given offset and of the given size.
    pub(crate) fn load_block<M>(
        &mut self,
        offset: u64,
        block_size: usize,
    ) -> io::Result<MemBlock<M>> {
        let mut block: MemBlock<M> = MemBlock::new(block_size);
        self.read_block(PAGE_ZERO, block.as_mut());
        Ok(block)
    }

    /// Apply a read-only callback to any page type
    fn with_page<P, F, R>(&self, frame: &MemFrame, f: F) -> io::Result<R>
    where
        F: FnOnce(&P) -> R,
        for<'a> &'a P: TryFrom<&'a PageLatch, Error = IoError>,
    {
        let latch = frame.read();
        let page: &P = (&latch).try_into()?;
        Ok(f(page))
    }

    /// Apply a mutable callback to any page type
    fn with_page_mut<P, F, R>(&self, frame: &MemFrame, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut P) -> R,
        for<'a> &'a mut P: TryFrom<&'a mut PageLatch, Error = IoError>,
    {
        let mut latch = frame.write();
        let page: &mut P = (&mut latch).try_into()?;
        Ok(f(page))
    }

    /// Apply a fallible read-only callback to any page type
    fn try_with_page<P, F, R>(&self, frame: &MemFrame, f: F) -> io::Result<R>
    where
        F: FnOnce(&P) -> io::Result<R>,
        for<'a> &'a P: TryFrom<&'a PageLatch, Error = IoError>,
    {
        let latch = frame.read();
        let page: &P = (&latch).try_into()?;
        f(page)
    }

    /// Apply a fallible mutable callback to any page type
    fn try_with_page_mut<P, F, R>(&self, frame: &MemFrame, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut P) -> io::Result<R>,
        for<'a> &'a mut P: TryFrom<&'a mut PageLatch, Error = IoError>,
    {
        let mut latch = frame.write();
        let page: &mut P = (&mut latch).try_into()?;
        f(page)
    }

    fn page_zero_frame(&self) -> io::Result<&MemFrame> {
        self.page_zero
            .as_ref()
            .ok_or(IoError::new(ErrorKind::Other, "pager not initialized"))
    }

    // Utility to execute a callback on page zero.
    fn with_page_zero<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&PageZero) -> R,
    {
        self.with_page::<PageZero, _, _>(self.page_zero_frame()?, f)
    }

    // Utility to execute a mutable callback on page zero.
    fn with_page_zero_mut<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut PageZero) -> R,
    {
        self.with_page_mut::<PageZero, _, _>(self.page_zero_frame()?, f)
    }

    fn page_size(&self) -> io::Result<usize> {
        self.with_page_zero(|pz| pz.metadata().page_size as usize)
    }

    fn min_keys_per_page(&self) -> io::Result<usize> {
        self.with_page_zero(|pz| pz.metadata().min_keys as usize)
    }

    fn first_free_page(&self) -> io::Result<Option<PageId>> {
        self.with_page_zero(|pz| pz.metadata().first_free_page)
    }

    fn last_free_page(&self) -> io::Result<Option<PageId>> {
        self.with_page_zero(|pz| pz.metadata().last_free_page)
    }

    fn try_set_first_free_page(&mut self, value: Option<PageId>) -> io::Result<()> {
        self.with_page_zero_mut(|pz| pz.metadata_mut().first_free_page = value)?;
        Ok(())
    }

    fn try_set_last_free_page(&mut self, value: Option<PageId>) -> io::Result<()> {
        self.with_page_zero_mut(|pz| pz.metadata_mut().last_free_page = value)?;
        Ok(())
    }

    /// Validates that the provided value is aligned.
    fn validate_alignment_of(value: usize, align: usize) -> io::Result<()> {
        // Offset in the file must be aligned
        if !(value).is_multiple_of(align) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("invalid value {}: must be {}-byte aligned", value, align),
            ));
        }
        Ok(())
    }

    /// Write a block directly to the underlying disk using DirectIO.
    /// The caller must ensure that the size of the provided buffer is aligned for [O_DIRECT].
    fn write_block(&mut self, start_page_number: PageId, content: &[u8]) -> io::Result<()> {
        let size = self.page_size()?;
        let offset = Self::page_offset(start_page_number, size as u64);

        Self::validate_alignment_of(offset as usize, PAGE_ALIGNMENT as usize)?;
        // Content pointer must also be aligned.
        Self::validate_alignment_of(content.as_ptr() as usize, PAGE_ALIGNMENT as usize)?;
        Self::validate_alignment_of(content.len() as usize, PAGE_ALIGNMENT as usize)?;

        // Seek in the file to the targete offset and write the content
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(content)?;

        Ok(())
    }

    /// Read a block back from the disk into an output buffer.
    fn read_block(&mut self, start_page_number: PageId, buffer: &mut [u8]) -> io::Result<()> {
        // Content pointer must also be aligned.
        let size = self.page_size()?;
        Self::validate_alignment_of(buffer.as_ptr() as usize, PAGE_ALIGNMENT as usize)?;
        Self::validate_alignment_of(buffer.len() as usize, PAGE_ALIGNMENT as usize)?;
        let offset = Self::page_offset(start_page_number, size as u64);
        Self::validate_alignment_of(offset as usize, PAGE_ALIGNMENT as usize)?;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    /// Allocate a new page with the given <H> header.
    pub(crate) fn allocate_page<H>(&mut self) -> io::Result<MemFrame>
    where
        MemFrame: From<MemBlock<H>>,
        MemBlock<H>: Allocatable,
    {
        let (id, mut mem_page) = if let Some(page_id) = self.first_free_page()? {

            let page = self.read_page::<OverflowPageHeader>(page_id)?;
            let next = self.with_page::<OverflowPage, _, _>(&page, |overflow| overflow.next())?;

            self.try_set_first_free_page(next)?;

            if let Some(last_id) = self.last_free_page()?
                && last_id == page_id
            {
                self.try_set_last_free_page(None)?;
            }


            (page_id, page.reinit_as::<H>())
        } else {
            // TODO: Think how to best do this part
            let page = MemBlock::<H>::alloc(self.page_size()? as u32);
            let frame = MemFrame::from(page);
            let id = frame.page_number();
            (id, frame)
        };
        mem_page.mark_dirty();

        Ok(mem_page)
    }

    /// Adds a frame to the cache, possibly evicting another one from it and writing its contents to disk thereby.
    ///
    /// As we are doing write ahead logging instead of shadow paging, we can safely perform NO-STEAL, NO FORCE behaviour.
    /// - **NO-STEAL:** Dirty pages are not written to disk immediately upon modification unless eviction occurs.
    /// - **NO-FORCE:** Writes to disk do not happen automatically at transaction commit; only when evicted.
    pub(crate) fn cache_frame(&mut self, frame: MemFrame) -> io::Result<PageId> {
        let id = frame.page_number();
        if let Some(evicted) = self.cache.insert(id, frame)? {
            let evicted_id = evicted.page_number();

            if evicted.is_dirty() {
                self.write_block(evicted_id, evicted.write().as_mut())?;
            };
        };
        Ok(id)
    }

    /// Reads a page from the cache, if it is not found there it goes to the disk to get its contents
    ///
    /// The marker [H] indicates the type of page that is going to be read.
    ///
    /// After reading it, if any page was evicted, goes to the disk to write its contents.
    ///
    /// See [Pager::write_block]
    pub(crate) fn read_page<H>(&mut self, id: PageId) -> io::Result<MemFrame>
    where
        MemFrame: From<MemBlock<H>>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            let frame = self
                .page_zero
                .as_ref()
                .ok_or(IoError::new(ErrorKind::Other, "Pager not initialized!"))?
                .clone();
            return Ok(frame);
        };

        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };

        let page_size = self.page_size()?;

        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer: MemBlock<H> = MemBlock::new(page_size);
        self.read_block(id, buffer.as_mut())?;
        let page = buffer.cast::<H>();

        let mem_page = MemFrame::from(page);
        if let Some(evicted) = self.cache.insert(id, mem_page)?
            && evicted.is_dirty()
        {
            let evicted_id = evicted.page_number();
            self.write_block(evicted_id, evicted.write().as_mut())?;
        };

        Ok(self.cache.get(&id).unwrap())
    }

    pub(crate) fn dealloc_page<H>(&mut self, id: PageId) -> io::Result<()>
    where
        MemFrame: From<MemBlock<H>>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot deallocate PageZero!",
            ));
        };

        let first_free_page = self.first_free_page()?;

        // Set the first free page in the header
        if first_free_page.is_none() {
            self.try_set_first_free_page(Some(id));
        };

        if let Some(last_free_id) = self.last_free_page()? {
            // Read the free page and set the next page.
            let previous_page = self.read_page::<OverflowPageHeader>(last_free_id)?;

            self.with_page_mut::<OverflowPage, _, _>(&previous_page, |overflow| {
                overflow.metadata_mut().next = Some(last_free_id);
            })?;
        };

        let mem_page = self.read_page::<H>(id)?.dealloc();
        self.try_set_first_free_page(Some(id));

        // Probably not necessary
        self.write_block(id, mem_page.write().as_mut())?;

        Ok(())
    }
}

make_shared! {SharedPager, Pager}
/*#[cfg(test)]
mod pager_tests {

    use super::*;
    use crate::{
        storage::{
            cell::OwnedCell,
            page::{BTREE_PAGE_HEADER_SIZE, BtreePage},
        },
        test_utils::test_pager,
    };
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_single_page_rw() -> io::Result<()> {
        let (mut pager, f) = test_pager()?;
        let page_size = pager.page_size();
        let mut page1 = BtreePage::alloc(page_size);
        let page_id = page1.metadata().page_number;
        assert!(page_id != crate::types::PAGE_ZERO, "Invalid page number!");
        let result = pager.write_block_unchecked(page_id, page1.as_mut());
        assert!(result.is_ok(), "Page writing to disk failed!");
        let mut buf: MemBlock<()> = MemBlock::new(pager.page_size() as usize);
        pager.read_block_unchecked(page_id, buf.as_mut())?;
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
        let (mut pager, f) = test_pager()?;
        let page_size = pager.page_size();
        let mut pages = Vec::new();
        let num_pages = 10;
        let total_size = num_pages * page_size;

        let mut raw_buffer: MemBlock<()> = MemBlock::new(total_size as usize);

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
        pager.write_block_unchecked(pages[0].metadata().page_number, raw_buffer.as_mut())?;

        for page in pages {
            // Try to read page 1 back.
            let mut buf: MemBlock<()> = MemBlock::new(page.metadata().page_size as usize);

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
        let (mut pager, f) = test_pager()?;
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
    fn test_dirty_page() -> io::Result<()> {
        let (mut pager, f) = test_pager()?;
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
        let mut buf: MemBlock<()> = MemBlock::new(pager.page_size() as usize);
        pager.read_block_unchecked(id1, buf.as_mut())?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_dealloc_page() -> io::Result<()> {
        let (mut pager, f) = test_pager()?;

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
        let (mut pager, f) = test_pager()?;
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
                let cell = OwnedCell::new(b"Hello");
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
*/
