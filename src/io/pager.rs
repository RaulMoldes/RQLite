use std::io::{Read, Seek, Write};

use crate::io::cache::PageCache;
use crate::io::disk::{DirectIO, FileOperations};
use crate::io::frames::MemFrame;
use crate::storage::buffer::AllocatorKind;
use crate::storage::page::{ OverflowPage, MemPage, Page, PageZero};
use crate::types::{PageId, PAGE_ZERO};
use crate::{RQLiteConfig, DEFAULT_CACHE_SIZE, PAGE_ALIGNMENT};

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
    (u32::from(page_id) * page_size) as u64
}
/// Implementation of SQLite pager. Reference: [https://sqlite.org/src/file/src/pager.c]
pub struct Pager {
    file: DirectIO,
    cache: PageCache,
    page_zero: PageZero,
}

impl Pager {
    pub(crate) fn from_config(
        config: RQLiteConfig,
        path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self> {
        // Allocate the file and the cache.
        let mut file = DirectIO::create(path, super::disk::FOpenMode::ReadWrite)?;
        let cache =
            PageCache::with_capacity(config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE) as usize);

        assert!(
            DirectIO::validate_alignment(config.page_size as usize),
            "Page size: {} must be aligned to BLOCK SIZE {} for Direct IO.",
            config.page_size,
            DirectIO::BLOCK_SIZE
        );

        let mut page_zero: PageZero =
            PageZero::alloc(config.page_size, AllocatorKind::GlobalAllocator);
        page_zero.metadata_mut().page_size = config.page_size;
        page_zero.metadata_mut().incremental_vacuum_mode = config.incremental_vacuum_mode;
        page_zero.metadata_mut().rw_version = config.read_write_version;
        page_zero.metadata_mut().text_encoding = config.text_encoding;
        page_zero.metadata_mut().cache_size = config.cache_size.unwrap_or(DEFAULT_CACHE_SIZE);

        assert_eq!(
            page_zero.as_mut().as_ptr() as usize % DirectIO::BLOCK_SIZE,
            0
        );
        assert_eq!(page_zero.as_mut().len() % DirectIO::BLOCK_SIZE, 0);
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
        self.file.seek(std::io::SeekFrom::Start(offset))?;

        assert!(
            DirectIO::validate_alignment(content.as_ptr() as usize),
            "Buffer size: {} must be aligned to BLOCK SIZE {} for Direct IO.",
            content.len(),
            DirectIO::BLOCK_SIZE
        );
        assert!(
            DirectIO::validate_alignment(content.len()),
            "Buffer size: {} must be aligned to BLOCK SIZE {} for Direct IO.",
            content.len(),
            DirectIO::BLOCK_SIZE
        );
        self.file.write_all(content)?;

        Ok(())
    }

    /// Read a block back from the disk into an output buffer.
    fn read_block_unchecked(
        &mut self,
        start_page_number: PageId,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        let offset = page_offset(start_page_number, self.page_zero.metadata().page_size);
        assert!(
            DirectIO::validate_alignment(buffer.len()),
            "Buffer size: {} must be aligned to BLOCK SIZE {} for Direct IO.",
            buffer.len(),
            DirectIO::BLOCK_SIZE
        );
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    fn page_size(&self) -> u32 {
        self.page_zero.metadata().page_size
    }

    fn alloc_page<P: Page>(&mut self) -> std::io::Result<PageId>
    where
        MemPage: From<P>,
    {
        let page = P::alloc(self.page_size(), AllocatorKind::GlobalAllocator);
        let id = page.page_number();
        let mem_page = MemFrame::new(MemPage::from(page));
        if let Some(evicted) = self.cache.insert(id, mem_page) {
            if evicted.is_dirty() {
                let id = evicted.read().page_number();
                self.write_block_unchecked(id, evicted.write().as_mut())?;
            }
        };
        Ok(id)
    }

    fn read_page<P: Page>(&mut self, id: PageId) -> std::io::Result<MemFrame<MemPage>>
    where
        MemPage: From<P>,
    {
        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };
        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer = vec![0u8; self.page_size() as usize];
        self.read_block_unchecked(id, &mut buffer)?;

        let page = P::try_from((
            &buffer,
            PAGE_ALIGNMENT as usize,
            AllocatorKind::GlobalAllocator,
        ))
        .map_err(|msg| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to convert page from bytes: {msg}"),
            )
        })?;

        Ok(MemFrame::new(MemPage::from(page)))
    }

    fn dealloc_page<P: Page>(&mut self, id: PageId) -> std::io::Result<()>
    where
        MemPage: From<P>,
    {
        let last_free_page = self.page_zero.metadata().last_free_page;
        let first_free_page = self.page_zero.metadata().first_free_page;

        // Set the first free page in the header
        if first_free_page == PAGE_ZERO {
            self.page_zero.metadata_mut().first_free_page = id;
        };

        if last_free_page != PAGE_ZERO {
            // Read the free page and set the next page.
            let previous_page = self.read_page::<OverflowPage>(last_free_page)?;
            // The previous page should be a free page.
            // SAFETY: It should be safe to unwrap the result as we have already read the page as a free page.
            previous_page
                .try_with_variant_mut::<OverflowPage, _, _, _>(|free| {
                    free.metadata_mut().next = id;
                })
                .unwrap(); // This should not panic as we have already read the page as a free page.
        };

        let mem_page = self.read_page::<P>(id)?;
        // As the page should be currently empty, we can mark it as free.
        // We need to append it to the free list, and that would be everything.
        mem_page.write().dealloc();
        self.page_zero.metadata_mut().last_free_page = id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::BtreePage;

    use super::*;
    use serial_test::serial;
    use std::io::{Read, Seek, SeekFrom};
    use tempfile::tempdir;

    fn create_test_pager() -> std::io::Result<Pager> {
        let dir = tempdir()?;
        let path = dir.path().join("test.db");

        let config = crate::RQLiteConfig {
            page_size: 4096,
            cache_size: Some(16),
            incremental_vacuum_mode: crate::IncrementalVaccum::Disabled,
            read_write_version: crate::ReadWriteVersion::Legacy,
            text_encoding: crate::TextEncoding::Utf8,
        };

        Pager::from_config(config, &path)
    }

    #[test]
    #[serial]
    fn pager_creation() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;
        pager.file.seek(SeekFrom::Start(0))?;
        let buf = DirectIO::alloc_aligned(pager.page_size() as usize)?;
        pager.file.read_exact(buf)?;
        assert_eq!(buf, pager.page_zero.as_ref());
        unsafe {
            DirectIO::free_aligned(buf.as_mut_ptr());
        }
        Ok(())
    }
    #[test]
    #[serial]
    fn test_single_page_rw() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;
        let page_size = pager.page_size();
        let mut page1 = BtreePage::alloc(page_size, AllocatorKind::GlobalAllocator);
        let page_id = page1.metadata().page_number;
        assert!(page_id != crate::types::PAGE_ZERO, "Invalid page number!");
        let result = pager.write_block_unchecked(page_id, page1.as_mut());
        assert!(result.is_ok(), "Page writing to disk failed!");
        let buf = DirectIO::alloc_aligned(pager.page_size() as usize)?;
        pager.read_block_unchecked(page_id, buf)?;
        assert_eq!(
            buf,
            page1.as_ref(),
            "Retrieved content should match the allocated page!!"
        );
        unsafe {
            DirectIO::free_aligned(buf.as_mut_ptr());
        }
        Ok(())
    }

    #[test]
    #[serial]
    fn test_multiple_pages_rw() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;
        let page_size = pager.page_size();
        let mut pages = Vec::new();
        let num_pages = 10;
        let total_size = num_pages * page_size;
        assert!(
            (total_size as usize).is_multiple_of(DirectIO::BLOCK_SIZE),
            "Total size should be aligned to block size for direct io"
        );
        // Create a dynamic buffer
        let raw_buffer = DirectIO::alloc_aligned(total_size as usize)?;
        let mut offset = 0;
        // Accumulate all pages in our buffers.
        for i in 0..num_pages {
            let page = BtreePage::alloc(page_size, AllocatorKind::GlobalAllocator);
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
        pager.write_block_unchecked(pages[0].metadata().page_number, raw_buffer)?;

        for page in pages {
            // Try to read page 1 back.
            let buf = DirectIO::alloc_aligned(page.metadata().page_size as usize)?;

            pager.read_block_unchecked(page.metadata().page_number, buf)?;
            assert_eq!(
                buf,
                page.as_ref(),
                "Retrieved content should match the allocated page!!"
            );

            unsafe {
                DirectIO::free_aligned(buf.as_mut_ptr());
            }
        }

        unsafe { DirectIO::free_aligned(raw_buffer.as_mut_ptr()) };

        Ok(())
    }

    #[test]
    #[serial]
    fn test_cache_eviction() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;
        let page_size = pager.page_size();
        let cache_size = 2; // Set a small cache capacity to force eviction.
        pager.cache = PageCache::with_capacity(cache_size);

        // Assigns more pages than those that would fit in the cache
        let mut page_ids = Vec::new();
        for _ in 0..3 {
            let id = pager.alloc_page::<BtreePage>()?;
            page_ids.push(id);
        }

        // First page should have been expelled.
        assert!(pager.cache.get(&page_ids[0]).is_none());

        Ok(())
    }

    #[test]
    #[serial]
    fn test_dirty_page() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;
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
        let buf = DirectIO::alloc_aligned(pager.page_size() as usize)?;
        pager.read_block_unchecked(id1, buf)?;

        unsafe { DirectIO::free_aligned(buf.as_mut_ptr()) };
        Ok(())
    }

    #[test]
    #[serial]
    fn test_dealloc_page() -> std::io::Result<()> {
        let mut pager = create_test_pager()?;

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
}
