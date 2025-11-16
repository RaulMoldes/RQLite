use crate::io::cache::PageCache;
use crate::io::disk::{DBFile, FileOperations, FileSystem, allocate_aligned};
use crate::io::frames::MemFrame;
use crate::io::wal::WriteAheadLog;
use crate::storage::buffer::BufferWithMetadata;
use crate::storage::page::{MemPage, OverflowPage, Page, PageZero};
use crate::types::{PAGE_ZERO, PageId};
use crate::{AxmosDBConfig, DEFAULT_CACHE_SIZE, PAGE_ALIGNMENT};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::io::{Read, Seek, Write};
use std::sync::Arc;

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
fn page_offset(page_id: PageId, page_size: u32) -> u64 {
    u64::from(page_id) * page_size as u64
}
/// Implementation of a pager.
#[derive(Debug)]
pub struct Pager {
    file: DBFile,
    // write_ahead_log: WriteAheadLog, // TODO: Implement recoverability from the [WAL].
    cache: PageCache,
    page_zero: PageZero,
}

impl Pager {
    pub(crate) fn from_config(
        config: AxmosDBConfig,
        path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self> {
        // Allocate the file and the cache.
        let mut file = DBFile::create(&path)?;
        let cache =
            PageCache::with_capacity(config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE) as usize);

        let mut page_zero: PageZero = PageZero::alloc(config.page_size);
        page_zero.metadata_mut().page_size = config.page_size;
        page_zero.metadata_mut().incremental_vacuum_mode = config.incremental_vacuum_mode;
        page_zero.metadata_mut().rw_version = config.read_write_version;
        page_zero.metadata_mut().text_encoding = config.text_encoding;
        page_zero.metadata_mut().cache_size = config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE);

        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(page_zero.as_ref())?;

        Ok(Self {
            file,
            cache,
            page_zero,
        })
    }

    /// Write a block directly to the underlying disk using DirectIO.
    /// Must ensure that the content buffer is aligned.
    /// A block can contain [N] pages.
    /// As the page size is always aligned to the BLOCK SIZE for Direct IO, it should be safe to write and read back this way.
    fn write_block_unchecked(
        &mut self,
        start_page_number: PageId,
        content: &mut [u8],
    ) -> std::io::Result<()> {
        let offset = page_offset(start_page_number, self.page_zero.metadata().page_size);
        debug_assert!((offset as usize).is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content offset. Must be aligned!");
        debug_assert!((content.as_ptr() as usize).is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content ptr. Must be aligned!");
        debug_assert!(content.len().is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content length. Must at least write a full page !");
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(content)?;

        Ok(())
    }

    /// Read a block back from the disk into an output buffer.
    fn read_block_unchecked(
        &mut self,
        start_page_number: PageId,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {

         debug_assert!((buffer.as_ptr() as usize).is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content ptr. Must be aligned!");
         debug_assert!(buffer.len().is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content length. Must at least read a full page !");
        let offset = page_offset(start_page_number, self.page_zero.metadata().page_size);
        debug_assert!((offset as usize).is_multiple_of(PAGE_ALIGNMENT as usize), "Invalid content offset. Must be aligned!");
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    pub fn page_size(&self) -> u32 {
        self.page_zero.metadata().page_size
    }

    pub fn alloc_page<P: Page>(&mut self) -> std::io::Result<PageId>
    where
        MemPage: From<P>,
        BufferWithMetadata<P::Header>: Into<MemPage>,
    {
        let free_page = self.page_zero.metadata().first_free_page;
        let last_free = self.page_zero.metadata().last_free_page;
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

            self.page_zero.metadata_mut().first_free_page = next;

            if last_free == free_page {
                self.page_zero.metadata_mut().last_free_page = PAGE_ZERO;
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
        if let Some(mut evicted) = self.cache.insert(id, mem_page) {
            let evicted_id = evicted.read().page_number();

            if evicted.is_dirty() {
                self.write_block_unchecked(evicted_id, evicted.write().as_mut())?;
            };
        };
        Ok(id)
    }

    pub fn read_page<P: Page>(&mut self, id: PageId) -> std::io::Result<MemFrame<MemPage>>
    where
        MemPage: From<P>,
    {
        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };

        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer = allocate_aligned(PAGE_ALIGNMENT as usize, self.page_size() as usize)?;
        self.read_block_unchecked(id, &mut buffer)?;

        let page = P::try_from((&buffer, PAGE_ALIGNMENT as usize)).map_err(|msg| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
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

    pub fn dealloc_page<P: Page>(&mut self, id: PageId) -> std::io::Result<()>
    where
        MemPage: From<P>,
    {
        let last_free_page = self.page_zero.metadata().last_free_page;
        let first_free_page = self.page_zero.metadata().first_free_page;

        // Set the first free page in the header
        if !first_free_page.is_valid() {
            self.page_zero.metadata_mut().first_free_page = id;
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
        self.page_zero.metadata_mut().last_free_page = id;
        mem_page.write().dealloc();
        self.write_block_unchecked(id, mem_page.write().as_mut())?;

        Ok(())
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct SharedPager(Arc<RwLock<Pager>>);

impl From<Arc<RwLock<Pager>>> for SharedPager {
    fn from(value: Arc<RwLock<Pager>>) -> Self {
        Self(Arc::clone(&value))
    }
}

impl From<Pager> for SharedPager {
    fn from(value: Pager) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }
}

impl Clone for SharedPager {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl SharedPager {
    pub fn read(&self) -> RwLockReadGuard<'_, Pager> {
        self.0.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, Pager> {
        self.0.write()
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

unsafe impl Send for Pager {}
unsafe impl Sync for Pager {}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::io::disk::FileSystem;
    use crate::storage::page::{BTREE_PAGE_HEADER_SIZE, BtreePage};
    use serial_test::serial;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;
    use tempfile::tempdir;

    fn create_test_pager(path: impl AsRef<Path>, cache_size: usize) -> std::io::Result<Pager> {
        let dir = tempdir()?;
        let path = dir.path().join(&path);

        let config = crate::AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(cache_size as u16),
            incremental_vacuum_mode: crate::IncrementalVaccum::Disabled,
            read_write_version: crate::ReadWriteVersion::Legacy,
            text_encoding: crate::TextEncoding::Utf8,
        };

        Pager::from_config(config, &path)
    }

    #[test]
    #[serial]
    fn pager_creation() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        pager.file.seek(SeekFrom::Start(0))?;
        let mut buf = allocate_aligned(PAGE_ALIGNMENT as usize, pager.page_size() as usize)?;
        pager.file.read_exact(&mut buf)?;
        assert_eq!(buf, pager.page_zero.as_ref());

        Ok(())
    }
    #[test]
    #[serial]
    fn test_single_page_rw() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let mut page1 = BtreePage::alloc(page_size);
        let page_id = page1.metadata().page_number;
        assert!(page_id != crate::types::PAGE_ZERO, "Invalid page number!");
        let result = pager.write_block_unchecked(page_id, page1.as_mut());
        assert!(result.is_ok(), "Page writing to disk failed!");
        let mut buf = allocate_aligned(PAGE_ALIGNMENT as usize, pager.page_size() as usize)?;
        pager.read_block_unchecked(page_id, &mut buf)?;
        assert_eq!(
            buf,
            page1.as_ref(),
            "Retrieved content should match the allocated page!!"
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn test_multiple_pages_rw() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let mut pages = Vec::new();
        let num_pages = 10;
        let total_size = num_pages * page_size;


        let mut raw_buffer = allocate_aligned(PAGE_ALIGNMENT as usize, total_size as usize)?;

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
            raw_buffer[offset..offset + page_bytes.len()].copy_from_slice(page_bytes);
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
             let mut buf = allocate_aligned(PAGE_ALIGNMENT as usize, page.metadata().page_size as usize)?;

            pager.read_block_unchecked(page.metadata().page_number, &mut buf)?;
            assert_eq!(
                buf,
                page.as_ref(),
                "Retrieved content should match the allocated page!!"
            );
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn test_cache_eviction() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        let page_size = pager.page_size();
        let cache_size = 2; // Set a small cache capacity to force eviction.
        pager.cache = PageCache::with_capacity(cache_size);

        // Assigns more pages than those that would fit in the cache
        let mut page_ids = Vec::new();
        for _ in 0..2 {
            let id = pager.alloc_page::<BtreePage>()?;

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
        let id = pager.alloc_page::<BtreePage>()?;
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
    fn test_dirty_page() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;
        pager.cache = PageCache::with_capacity(1); // Cache pequeña

        // Create a dirty page
        let id1 = pager.alloc_page::<BtreePage>()?;
        {
            let mut frame = pager.cache.get(&id1).unwrap();
            frame.mark_dirty();
        }

        // Force eviction with a dirty page
        let _id2 = pager.alloc_page::<BtreePage>()?;

        // Verify it has been written out to disk.
        let mut buf = allocate_aligned(PAGE_ALIGNMENT as usize, pager.page_size() as usize)?;
        pager.read_block_unchecked(id1, &mut buf)?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_dealloc_page() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 16)?;

        // Allocate and deallocate pages
        let id1 = pager.alloc_page::<BtreePage>()?;
        let id2 = pager.alloc_page::<BtreePage>()?;

        pager.dealloc_page::<BtreePage>(id1)?;

        // Verify they are marked as free
        assert_eq!(pager.page_zero.metadata().first_free_page, id1);
        assert_eq!(pager.page_zero.metadata().last_free_page, id1);

        // Deallocate the second page
        pager.dealloc_page::<BtreePage>(id2)?;

        // Check that the free list was properly built.
        let page1 = pager.read_page::<OverflowPage>(id1)?;

        let next_free = page1
            .try_with_variant::<OverflowPage, _, _, _>(|free| free.metadata().next)
            .unwrap();

        assert_eq!(next_free, id2);
        assert_eq!(pager.page_zero.metadata().last_free_page, id2);

        Ok(())
    }

    #[test]
    #[serial]
    fn test_page_reusability() -> std::io::Result<()> {
        let mut pager = create_test_pager("test.db", 2)?;
        let num_pages = 350;
        let num_cells = 15;
        let mut ids: std::collections::HashSet<PageId> =
            std::collections::HashSet::with_capacity(num_pages as usize);

        for _ in 0..num_pages {
            let id = pager.alloc_page::<BtreePage>()?;
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
            let id = pager.alloc_page::<BtreePage>()?;
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
