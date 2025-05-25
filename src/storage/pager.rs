//! # Pager Module
//!
//! This module implements the Pager, which manages page-level operations for the SQLite database.
//! It provides a safe abstraction over disk I/O and implements a buffer pool for caching pages.
//! The Pager ensures that pages are properly managed through RAII guards that automatically
//! unpin pages when they go out of scope.
//! You could call the Pager as the storage manager, as it handles all the low-level disk operations,
//!
//! However, the pager does not handle btree operations directly, therefore I consider it a lower-level component
//! that is used by the BTree modules to manage pages.
//! Sqlite uses the pager to manage transactions too. This is done by writing the original page contents to a journal file.
//! I have implemented a simple journal mechanism that stores the original pages in memory. Initially i was not going to add the journal thing to my storage engine implementation,
//! because you know, they do not actually pay me for this, but creating a simpler journal in memory can be enough in most cases. Probably in the future will create a more robust journal with disk support.
//! Sqlite pager: https://www.sqlite.org/src/tree/pager.c
//!
//! To ensure page pinning and unpinning is safe, we use RAII guards that automatically pins a page when created
//! and unpins it when dropped. This prevents deadlocks and ensures that pages are not evicted while in use.
//!
//! I have been about two months to implement this guard thing, and i am probably not the most experienced Rust developer,
//! so if you have any suggestions on how to improve this code, please let me know.
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use std::sync::{Arc, Mutex};

use super::cache::{AddPageResult, BufferPool};
use super::disk::DiskManager;
use crate::header::Header;
use crate::page::{BTreePage, ByteSerializable, FreePage, OverflowPage, Page, PageType};

/// Internal structure that manages the actual pager state
/// This is wrapped in Arc<Mutex<>> to allow safe sharing between guards
/// I create this inner structure also to allow not taking mutable references to the actual pager when it is not actually needed.
struct PagerInner {
    disk_manager: DiskManager,
    page_cache: BufferPool,
    page_size: u32,
    journal_pages: Vec<(u32, Page)>,
    reserved_space: u8,
    dirty: bool,
}

/// RAII guard for immutable page access
/// Automatically unpins the page when dropped
pub struct PageGuard {
    page_number: u32,
    pager: Arc<Mutex<PagerInner>>,
    /// We used to store a raw pointer to avoid lifetime issues, but ensure safety through pinning
    /// I found this PhantomData thing is more memory safe and idiomatic
    _phantom: std::marker::PhantomData<Page>, // Aparently this is needed to avoid lifetime issues
                                              //https://doc.rust-lang.org/std/marker/struct.PhantomData.html
}

impl PageGuard {
    /// Gets a reference to the page
    pub fn page(&self) -> &Page {
        // This is a bit tricky - we need to ensure the page stays valid
        // We'll use a callback-based approach to ensure safety.
        // See the page callback methods in the pager for details.
        // As long as the higher level code uses the get_page_callback and get_page_mut_callback methods,
        // this is perfectly safe. The callback is basically a closure that takes a reference to the page,
        // executes some code, and returns a result. This ensures that the pages re kept pinned while in use, avoiding possible lifetime issues
        unsafe {
            // We know the page is pinned and won't be evicted
            let inner = self.pager.lock().unwrap();
            let page_ptr = inner.page_cache.get_page_ref(self.page_number).unwrap();
            // Extend the lifetime - safe because the page is pinned
            std::mem::transmute::<&Page, &Page>(page_ptr)
        }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        // This ensures the page is unpinned when the guard goes out of scope
        if let Ok(mut inner) = self.pager.lock() {
            let _ = inner.page_cache.unpin_page(self.page_number);
        }
    }
}
/// RAII guard for mutable page access
/// Automatically unpins the page when dropped
pub struct PageGuardMut {
    page_number: u32,
    pager: Arc<Mutex<PagerInner>>,
    _phantom: std::marker::PhantomData<Page>,
}

impl PageGuardMut {
    /// Gets a reference to the page
    pub fn page(&self) -> &Page {
        unsafe {
            let inner = self.pager.lock().unwrap();
            let page_ptr = inner.page_cache.get_page_ref(self.page_number).unwrap();
            std::mem::transmute::<&Page, &Page>(page_ptr)
        }
    }

    /// Gets a mutable reference to the page
    pub fn page_mut(&mut self) -> &mut Page {
        unsafe {
            let mut inner = self.pager.lock().unwrap();
            let page_ptr = inner.page_cache.get_page_mut_ref(self.page_number).unwrap();
            std::mem::transmute::<&mut Page, &mut Page>(page_ptr)
        }
    }
}

impl Deref for PageGuardMut {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page()
    }
}

impl DerefMut for PageGuardMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.page_mut()
    }
}

impl Drop for PageGuardMut {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.pager.lock() {
            let _ = inner.page_cache.unpin_page(self.page_number);
        }
    }
}

/// The Pager manages page-level operations on the database
#[derive(Clone)]
pub struct Pager {
    inner: Arc<Mutex<PagerInner>>,
}

impl Pager {
    /// Opens an existing database file
    ///
    /// # Parameters
    /// * `path` - Path to the database file
    /// * `buffer_pool_size` - Optional size of the buffer pool (default: 1000 pages)
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or if the header is invalid
    ///
    /// # Returns
    /// A new Pager instance
    pub fn open<P: AsRef<Path>>(path: P, buffer_pool_size: Option<usize>) -> io::Result<Self> {
        let mut disk_manager = DiskManager::open(path)?;
        let header = disk_manager.read_header()?;

        let inner = PagerInner {
            disk_manager,
            page_cache: BufferPool::new(buffer_pool_size.unwrap_or(1000)),
            journal_pages: Vec::new(),
            page_size: header.page_size,
            reserved_space: header.reserved_space,
            dirty: false,
        };

        Ok(Pager {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Creates a new database file
    ///
    /// # Parameters
    /// * `path` - Path where to create the database file
    /// * `page_size` - Size of each page in bytes (must be a power of 2 between 512 and 65536)
    /// * `buffer_pool_size` - Optional size of the buffer pool (default: 1000 pages)
    /// * `reserved_space` - Reserved space at the end of each page
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or if the page size is invalid
    ///
    /// # Returns
    /// A new Pager instance
    pub fn create<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        buffer_pool_size: Option<usize>,
        reserved_space: u8,
    ) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;

        let mut header = disk_manager.read_header()?;
        header.reserved_space = reserved_space;
        disk_manager.write_header(&header)?;

        let inner = PagerInner {
            disk_manager,
            page_cache: BufferPool::new(buffer_pool_size.unwrap_or(1000)),
            page_size,
            journal_pages: Vec::new(),
            reserved_space,
            dirty: false,
        };

        Ok(Pager {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Gets a page with automatic unpinning when the guard is dropped
    ///
    /// # Parameters
    /// * `page_number` - The page number to retrieve (1-based)
    /// * `expected_type` - Optional expected page type for validation
    ///
    /// # Errors
    /// Returns an error if:
    /// - The page number is out of range
    /// - The page cannot be loaded from disk
    /// - The page type doesn't match the expected type
    ///
    /// # Returns
    /// A PageGuard that provides access to the page and automatically unpins it when dropped
    ///
    /// NOTE: not recommended to use this method directly, use the get_page_callback method instead
    pub fn get_page(
        &self,
        page_number: u32,
        expected_type: Option<PageType>,
    ) -> io::Result<PageGuard> {
        // println!("get_page: {}", page_number);

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        // Load page if not in cache
        if !inner.page_cache.contains_page_simple(page_number) {
            // Load the page from disk
            Self::load_page(&mut inner, page_number)?;
            // Create and return the guard
        }

        // Validate page type if specified
        if let Some(expected) = expected_type {
            inner
                .page_cache
                .validate_page_type(page_number, expected)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        // Page is already in cache, just pin it
        inner.page_cache.pin_page_for_guard(page_number)?;

        Ok(PageGuard {
            page_number,
            pager: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Gets a mutable page with automatic unpinning when the guard is dropped
    ///
    /// # Parameters
    /// * `page_number` - The page number to retrieve (1-based)
    /// * `expected_type` - Optional expected page type for validation
    ///
    /// # Errors
    /// Returns an error if:
    /// - The page number is out of range
    /// - The page cannot be loaded from disk
    /// - The page type doesn't match the expected type
    ///
    /// # Returns
    /// A PageGuardMut that provides mutable access to the page and automatically unpins it when dropped.
    ///
    /// NOTE: not recommended to use this method directly, use the get_page_mut_callback method instead
    pub fn get_page_mut(
        &self,
        page_number: u32,
        expected_type: Option<PageType>,
    ) -> io::Result<PageGuardMut> {
        let page_for_journal = {
            let mut inner = self.inner.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
            })?;

            // Load page if not in cache
            if !inner.page_cache.contains_page_simple(page_number) {
                Self::load_page(&mut inner, page_number)?;
            }

            // Validate page type if specified
            if let Some(expected) = expected_type {
                inner
                    .page_cache
                    .validate_page_type(page_number, expected)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            }

            // Add to journal for transaction support
            let result = inner.page_cache.get_page_for_journal(page_number);

            if let Some(page) = result {
                Some(page.clone())
            } else {
                None
            }
        };

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        // Add the page to the journal if it was not already there
        if let Some(page) = page_for_journal {
            inner.journal_pages.push((page_number, page.clone()));
        }

        // Pin the page for mutable access
        inner.page_cache.pin_page_for_guard_mut(page_number)?;
        // We are dirty now
        inner.dirty = true;

        // Create and return the guard
        Ok(PageGuardMut {
            page_number,
            pager: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Gets a page using a callback to avoid lifetime issues
    ///
    /// # Parameters
    /// * `page_number` - The page number to retrieve
    /// * `expected_type` - Optional expected page type
    /// * `f` - Callback function that receives the page reference
    ///
    /// # Errors
    /// Returns an error if the page cannot be loaded or doesn't match the expected type
    ///
    /// # Returns
    /// The result of the callback function
    pub fn get_page_callback<F, R>(
        &self,
        page_number: u32,
        expected_type: Option<PageType>,
        f: F,
    ) -> io::Result<R>
    where
        F: FnOnce(&Page) -> R,
    {
        let guard = self.get_page(page_number, expected_type)?;
        Ok(f(guard.page()))
    }

    /// Gets a mutable page using a callback to avoid lifetime issues
    ///
    /// # Parameters
    /// * `page_number` - The page number to retrieve
    /// * `expected_type` - Optional expected page type
    /// * `f` - Callback function that receives the mutable page reference
    ///
    /// # Errors
    /// Returns an error if the page cannot be loaded or doesn't match the expected type
    ///
    /// # Returns
    /// The result of the callback function
    pub fn get_page_mut_callback<F, R>(
        &self,
        page_number: u32,
        expected_type: Option<PageType>,
        f: F,
    ) -> io::Result<R>
    where
        F: FnOnce(&mut Page) -> io::Result<R>,
    {
        let mut guard = self.get_page_mut(page_number, expected_type)?;
        f(&mut guard.page_mut())
    }

    /// Gets the header of the database
    ///
    /// # Errors
    /// Returns an error if the header cannot be read
    ///
    /// # Returns
    /// The database header
    pub fn get_header(&self) -> io::Result<Header> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;
        inner.disk_manager.read_header()
    }

    /// Updates the header of the database
    ///
    /// # Parameters
    /// * `header` - The new header to write
    ///
    /// # Errors
    /// Returns an error if the header cannot be written
    pub fn update_header(&self, header: &Header) -> io::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;
        inner.disk_manager.write_header(header)?;
        inner.dirty = true;
        Ok(())
    }

    /// Creates a new B-Tree page
    ///
    /// # Parameters
    /// * `page_type` - Type of B-Tree page to create
    /// * `right_most_page` - For interior pages, the rightmost child page number
    ///
    /// # Errors
    /// Returns an error if the page cannot be created or written to disk
    ///
    /// # Returns
    /// The page number of the newly created page
    pub fn create_btree_page(
        &self,
        page_type: PageType,
        right_most_page: Option<u32>,
    ) -> io::Result<u32> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        let page_number = inner.disk_manager.allocate_pages(1)?;
        let btree_page = BTreePage::new(
            page_type,
            inner.page_size,
            page_number,
            inner.reserved_space,
            right_most_page,
        )?;

        let page = Page::BTree(btree_page);
        let buffer = Self::serialize_page(&inner, &page)?;
        inner.disk_manager.write_page(page_number, &buffer)?;

        // Add to cache
        inner.page_cache.add_page(page_number, page, false);

        Ok(page_number)
    }

    /// Creates a new overflow page
    ///
    /// # Parameters
    /// * `next_page` - Page number of the next overflow page (0 if last)
    /// * `data` - Data to store in the overflow page
    ///
    /// # Errors
    /// Returns an error if:
    /// - The data is too large for an overflow page
    /// - The page cannot be created or written to disk
    ///
    /// # Returns
    /// The page number of the newly created page
    pub fn create_overflow_page(&self, next_page: u32, data: Vec<u8>) -> io::Result<u32> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        let max_data_size = inner.page_size as usize - 13; // Reserve 4 bytes for the next page pointer
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Data is too big for an overflow page: {} bytes, maximum {} bytes",
                    data.len(),
                    max_data_size
                ),
            ));
        }

        let page_number = inner.disk_manager.allocate_pages(1)?;
        let overflow_page = OverflowPage::new(next_page, data, inner.page_size, page_number)?;

        let page = Page::Overflow(overflow_page);
        let buffer = Self::serialize_page(&inner, &page)?;
        inner.disk_manager.write_page(page_number, &buffer)?;
        inner.disk_manager.sync()?;
        Ok(page_number)
    }

    /// Creates a new free page
    ///
    /// # Parameters
    /// * `next_page` - Page number of the next free page in the list (0 if last)
    ///
    /// # Errors
    /// Returns an error if the page cannot be created or written to disk
    ///
    /// # Returns
    /// The page number of the newly created page
    pub fn create_free_page(&self, next_page: u32) -> io::Result<u32> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        let page_number = inner.disk_manager.allocate_pages(1)?;
        let free_page = FreePage::new(next_page, inner.page_size, page_number);

        let page = Page::Free(free_page);
        let buffer = Self::serialize_page(&inner, &page)?;
        inner.disk_manager.write_page(page_number, &buffer)?;

        Ok(page_number)
    }

    /// Begins a new transaction
    ///
    /// # Errors
    /// Currently always succeeds
    pub fn begin_transaction(&self) -> io::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;
        inner.journal_pages.clear();
        inner.dirty = true;
        Ok(())
    }

    /// Commits the current transaction
    ///
    /// # Errors
    /// Returns an error if the flush operation fails
    pub fn commit_transaction(&self) -> io::Result<()> {
        self.flush()?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;
        inner.journal_pages.clear();
        Ok(())
    }

    /// Rolls back the current transaction
    ///
    /// # Errors
    /// Returns an error if pages cannot be restored from the journal
    pub fn rollback_transaction(&self) -> io::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        // Restore pages from journal
        // Cloning the pages may not be the most efficient way, but it's safe
        let journal_pages = inner.journal_pages.clone();

        for (page_number, page) in journal_pages.iter() {
            let buffer = Self::serialize_page(&inner, page)?;
            inner.disk_manager.write_page(*page_number, &buffer)?;

            // Update cache with restored page
            inner
                .page_cache
                .update_page(*page_number, page.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        inner.page_cache.mark_clean_all();
        inner.dirty = false;
        inner.journal_pages.clear();
        Ok(())
    }

    /// Flushes all dirty pages to disk
    ///
    /// # Errors
    /// Returns an error if pages cannot be written to disk
    pub fn flush(&self) -> io::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        if !inner.dirty {
            return Ok(());
        }

        // Get all dirty pages
        let dirty_pages = inner
            .page_cache
            .get_dirty_pages()
            .into_iter()
            .map(|(n, p)| (n, p.clone()))
            .collect::<Vec<_>>();

        // Write each dirty page to disk
        for (page_number, page) in dirty_pages {
            inner.page_cache.prepare_page_for_write(page_number);

            let buffer = Self::serialize_page(&inner, &page)?;
            inner.disk_manager.write_page(page_number, &buffer)?;

            inner.page_cache.finish_page_write(page_number);
            inner.page_cache.mark_clean(page_number);
        }

        inner.disk_manager.sync()?;
        inner.dirty = false;
        Ok(())
    }

    /// Closes the pager, flushing any pending changes
    ///
    /// # Errors
    /// Returns an error if the flush operation fails
    pub fn close(self) -> io::Result<()> {
        self.flush() // ?? Maybe we should not flush here?
    }

    /// Gets the total number of pages in the database
    ///
    /// # Errors
    /// Returns an error if the page count cannot be determined
    ///
    /// # Returns
    /// The total number of pages
    pub fn page_count(&self) -> io::Result<u32> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;
        inner.disk_manager.page_count()
    }

    // Private helper methods

    /// Loads a page from disk into the cache
    fn load_page(inner: &mut PagerInner, page_number: u32) -> io::Result<()> {
        let page_count = inner.disk_manager.page_count()?;
        if page_number == 0 || page_number > page_count {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page number out of range: {}", page_number),
            ));
        }

        // Read page from disk
        let mut buffer = vec![0u8; inner.page_size as usize];
        inner.disk_manager.read_page(page_number, &mut buffer)?;

        // Parse the page
        let page = Self::parse_page(inner, page_number, &buffer)?;
        // Create and return the guard

        // Add to cache, handling eviction if necessary.
        // There is a risk of deadlock here if the cache is full and we try to evict a page
        // that is currently being accessed. This should be handled by the cache itself.

        // Deadlock
        // We cannot know if an evicted page is dirty or not, so we need to release it
        // Solved at the cache level, by returning early when there are no candidates for eviction
        match inner.page_cache.add_page(page_number, page, false) {
            AddPageResult::Added | AddPageResult::Evicted(_, _, false) => {
                return Ok(());
            }
            AddPageResult::Evicted(evicted_page_number, buffer, true) => {
                inner
                    .disk_manager
                    .write_page(evicted_page_number, &buffer)?;
            }
            AddPageResult::Rejected => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Buffer pool is full and cannot evict a page",
                ));
            }
        };
        Ok(())
    }

    /// Parses a page from a buffer
    fn parse_page(inner: &PagerInner, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Empty buffer"));
        }

        // Determine page type from first byte
        match buffer[0] {
            0x02 | 0x05 | 0x0A | 0x0D => {
                // B-Tree page
                let mut cursor = std::io::Cursor::new(buffer);
                let mut btree_page = BTreePage::read_from(&mut cursor)?;
                btree_page.page_number = page_number;
                btree_page.page_size = inner.page_size;
                btree_page.reserved_space = inner.reserved_space;
                Ok(Page::BTree(btree_page))
            }
            0x10 => {
                // Overflow page
                let mut cursor = std::io::Cursor::new(&buffer[1..]); // Skip type byte
                                                                     // println!("Parsing overflow page");
                let mut overflow_page = OverflowPage::read_from(&mut cursor)?;
                overflow_page.page_number = page_number;
                overflow_page.page_size = inner.page_size;
                Ok(Page::Overflow(overflow_page))
            }
            0x00 => {
                // Free page
                let mut cursor = std::io::Cursor::new(&buffer[1..]); // Skip type byte
                let mut free_page = FreePage::read_from(&mut cursor)?;
                free_page.page_number = page_number;
                free_page.page_size = inner.page_size;
                Ok(Page::Free(free_page))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown page type: {:#x}", buffer[0]),
            )),
        }
    }

    /// Serializes a page to a buffer
    fn serialize_page(inner: &PagerInner, page: &Page) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; inner.page_size as usize];
        let mut cursor = std::io::Cursor::new(&mut buffer[..]);
        page.write_to(&mut cursor)?;
        Ok(buffer)
    }
}

/// Ensure the pager properly cleans up on drop
impl Drop for Pager {
    fn drop(&mut self) {
        // Try to flush any pending changes
        // Probably more ACID to use self.rollback_transaction() here, but as I am not implementing transactions still let's keep it like this.
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{BTreeCell, TableLeafCell};
    use tempfile::tempdir;

    #[test]
    fn test_pager_create_and_open() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        {
            let pager = Pager::create(&db_path, 4096, None, 0).unwrap();
            assert_eq!(pager.page_count().unwrap(), 1);
        }

        // Open the existing database
        {
            let pager = Pager::open(&db_path, None).unwrap();
            assert_eq!(pager.page_count().unwrap(), 1);
        }
    }

    #[test]
    fn test_page_guard_automatic_unpinning() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, Some(10), 0).unwrap();

        // Create a B-Tree page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Test scoped access with automatic unpinning
        {
            let guard = pager
                .get_page(page_number, Some(PageType::TableLeaf))
                .unwrap();
            assert_eq!(guard.page().page_number(), page_number);

            // Check that page is pinned
            let inner = pager.inner.lock().unwrap();
            assert!(inner.page_cache.is_pinned(page_number));
        } // guard is dropped here, page should be unpinned

        // Verify page is unpinned after guard is dropped
        {
            let inner = pager.inner.lock().unwrap();
            assert!(!inner.page_cache.is_pinned(page_number));
        }
    }

    #[test]
    fn test_page_guard_mut_automatic_unpinning() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, Some(10), 0).unwrap();

        // Create a B-Tree page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Test mutable access with automatic unpinning
        {
            let mut guard = pager
                .get_page_mut(page_number, Some(PageType::TableLeaf))
                .unwrap();

            // Modify the page
            match guard.page_mut() {
                Page::BTree(btree_page) => {
                    // Add a cell to verify mutation
                    let cell = BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: 10,
                        row_id: 1,
                        payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        overflow_page: None,
                    });
                    btree_page.add_cell(cell).unwrap();
                }
                _ => panic!("Expected BTree page"),
            }

            // Check that page is pinned and dirty
            let inner = pager.inner.lock().unwrap();
            assert!(inner.page_cache.is_pinned(page_number));
            assert!(inner.page_cache.is_dirty(page_number));
        } // guard is dropped here

        // Verify page is unpinned but still dirty
        {
            let inner = pager.inner.lock().unwrap();
            assert!(!inner.page_cache.is_pinned(page_number));
            assert!(inner.page_cache.is_dirty(page_number));
        }
    }

    #[test]
    fn test_transaction_rollback() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, Some(10), 0).unwrap();

        // Create a page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Begin transaction
        pager.begin_transaction().unwrap();

        // Modify the page
        {
            let mut guard = pager.get_page_mut(page_number, None).unwrap();
            match guard.page_mut() {
                Page::BTree(btree_page) => {
                    let cell = BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: 10,
                        row_id: 1,
                        payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        overflow_page: None,
                    });
                    btree_page.add_cell(cell).unwrap();
                }
                _ => panic!("Expected BTree page"),
            }
        }

        // Verify the page has a cell
        {
            let guard = pager.get_page(page_number, None).unwrap();
            match guard.page() {
                Page::BTree(btree_page) => {
                    assert_eq!(btree_page.header.cell_count, 1);
                }
                _ => panic!("Expected BTree page"),
            }
        }

        // Rollback transaction
        pager.rollback_transaction().unwrap();

        // Verify the page was restored (no cells)
        {
            let guard = pager.get_page(page_number, None).unwrap();
            match guard.page() {
                Page::BTree(btree_page) => {
                    assert_eq!(btree_page.header.cell_count, 0);
                }
                _ => panic!("Expected BTree page"),
            }
        }
    }

    #[test] // Apparently, if you forget to drop the guard the buffer pool will also return it to you, causing a potential deadlock. Must fix this bug,
    fn test_page_eviction_with_guards() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        // Create pager with small buffer pool
        let pager = Pager::create(&db_path, 4096, Some(2), 0).unwrap();

        // Create 3 pages (more than buffer pool can hold)
        let page1 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        let page2 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        let page3 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Access all pages, causing eviction
        {
            let _guard1 = pager.get_page(page1, None).unwrap();

            let _guard2 = pager.get_page(page2, None).unwrap();

            // Page 1 should not be evicted when accessing page 3, because it is still in use
            let result = pager.get_page(page3, None);
            assert!(result.is_err()); // Should fail because page 1 is still in use
            drop(_guard1); // Drop guard1 to allow eviction
            let guard3 = pager.get_page(page3, None).unwrap();
            // All pages should still be accessible through guards
            assert!(guard3.page().page_number() == page3);
        }
    }

    #[test]
    fn test_concurrent_page_access() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Multiple immutable guards to same page should work
        {
            let guard1 = pager.get_page(page_number, None).unwrap();
            let guard2 = pager.get_page(page_number, None).unwrap();

            assert_eq!(guard1.page().page_number(), page_number);
            assert_eq!(guard2.page().page_number(), page_number);

            // Pin count should be 2
            let inner = pager.inner.lock().unwrap();
            assert_eq!(inner.page_cache.pin_count(page_number), 2);
        }

        // Verify unpinning
        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(inner.page_cache.pin_count(page_number), 0);
        }
    }

    #[test]
    fn test_page_type_validation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Create a table leaf page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        // Should succeed with correct type
        {
            let guard = pager
                .get_page(page_number, Some(PageType::TableLeaf))
                .unwrap();
            assert_eq!(guard.page().page_type(), PageType::TableLeaf);
        }

        // Should fail with incorrect type
        let result = pager.get_page(page_number, Some(PageType::TableInterior));
        assert!(result.is_err());
    }

    #[test]
    fn test_overflow_page_creation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Create overflow page with valid data
        let data = vec![0xAA; 1000];
        let page_number = pager.create_overflow_page(1, data.clone()).unwrap();

        // Read it back
        {
            let guard = pager
                .get_page(page_number, Some(PageType::Overflow))
                .unwrap();
            match guard.page() {
                Page::Overflow(overflow) => {
                    assert_eq!(overflow.data[..1000], data);
                    assert_eq!(overflow.next_page, 1);
                }
                _ => panic!("Expected overflow page"),
            }
        }

        // Test data too large
        let large_data = vec![0xBB; 5000]; // Too large for page size 4096
        let result = pager.create_overflow_page(0, large_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_free_page_creation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Create free page
        let page_number = pager.create_free_page(42).unwrap();

        // Read it back
        {
            let guard = pager.get_page(page_number, Some(PageType::Free)).unwrap();
            match guard.page() {
                Page::Free(free) => {
                    assert_eq!(free.next_page, 42);
                }
                _ => panic!("Expected free page"),
            }
        }
    }

    #[test]
    fn test_flush_dirty_pages() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Create and modify a page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

        {
            let mut guard = pager.get_page_mut(page_number, None).unwrap();
            match guard.page_mut() {
                Page::BTree(btree_page) => {
                    let cell = BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: 5,
                        row_id: 42,
                        payload: vec![1, 2, 3, 4, 5],
                        overflow_page: None,
                    });
                    btree_page.add_cell(cell).unwrap();
                }
                _ => panic!("Expected BTree page"),
            }
        }

        // Flush to disk
        pager.flush().unwrap();

        // Verify page is no longer dirty
        {
            let inner = pager.inner.lock().unwrap();
            assert!(!inner.page_cache.is_dirty(page_number));
        }

        // Reopen database and verify changes persisted
        drop(pager);
        let pager2 = Pager::open(&db_path, None).unwrap();

        {
            let guard = pager2.get_page(page_number, None).unwrap();
            match guard.page() {
                Page::BTree(btree_page) => {
                    assert_eq!(btree_page.header.cell_count, 1);
                }
                _ => panic!("Expected BTree page"),
            }
        }
    }

    #[test]
    fn test_header_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 100).unwrap();

        // Get header
        let header = pager.get_header().unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.reserved_space, 100);

        // Update header
        let mut new_header = header.clone();
        new_header.user_version = 42;
        pager.update_header(&new_header).unwrap();

        // Verify update
        let updated_header = pager.get_header().unwrap();
        assert_eq!(updated_header.user_version, 42);
    }

    #[test]
    fn test_page_count() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Initial page count (includes header page)
        assert_eq!(pager.page_count().unwrap(), 1);

        // Create some pages
        pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        pager.create_overflow_page(0, vec![1, 2, 3]).unwrap();

        // Verify page count
        assert_eq!(pager.page_count().unwrap(), 4);
    }

    #[test]
    fn test_invalid_page_access() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(&db_path, 4096, None, 0).unwrap();

        // Try to access non-existent page
        let result = pager.get_page(999, None);
        assert!(result.is_err());

        // Try to access page 0 (invalid)
        let result = pager.get_page(0, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_cleanup() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        {
            let pager = Pager::create(&db_path, 4096, None, 0).unwrap();
            let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();

            // Modify page
            {
                let mut guard = pager.get_page_mut(page_number, None).unwrap();
                match guard.page_mut() {
                    Page::BTree(btree_page) => {
                        let cell = BTreeCell::TableLeaf(TableLeafCell {
                            payload_size: 3,
                            row_id: 1,
                            payload: vec![1, 2, 3],
                            overflow_page: None,
                        });
                        btree_page.add_cell(cell).unwrap();
                    }
                    _ => panic!("Expected BTree page"),
                }
            }
            // Pager dropped here, should flush changes
        }

        // Verify changes were persisted
        let pager2 = Pager::open(&db_path, None).unwrap();
        let guard = pager2.get_page(2, None).unwrap(); // Page 2 (after header page)
        match guard.page() {
            Page::BTree(btree_page) => {
                assert_eq!(btree_page.header.cell_count, 1);
            }
            _ => panic!("Expected BTree page"),
        }
    }
}
