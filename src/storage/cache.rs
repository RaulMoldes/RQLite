//! storage/cache.rs
//! This module contains a buffer pool implementation.
//! The buffer pool is basically a circular buffer that holds pages in memory.
//! The CMU Database Group claim that implementing a buffer pool is crucial for database performance and reliability.
//! Other systems like LevelDB, are more in favor of using the OS page cache directly,
//! PostgreSQL, on the other hand has somewhat of a hybrid approach.
//! It uses the OS page cache for most operations, but also has a buffer pool for managing dirty pages and eviction.
//! Finally, SQLite has a simple buffer poolthat uses a LRU eviction policy.
//! It is a naive but effective approach for most use cases.
//! 
//! I started implementing a buffer pool with that LRU eviction policy, but I want to enhance it with better page lifecycle management,
//! better statistics tracking, and more robust error handling. I think this is one of the easisest parts of the storage engine to implement (caching is easy compared to B-Tree implementation and handling concurreny control)
//! 
//! Therefore , my current implementation uses a LRU-K policy. 
//! When we need to evict a page, we first try to find all the possible candidates in the buffer.
//! Then we sort them by their last accessed time and evict the least recently used page.
//! We also track the pin count, dirty status, and last accessed time of each page.
//! A page cannot be selected as a candidate for eviction if it is pinned or has been written to recently (dirty).
//! If the page is dirty but unpinned, we should write its contents to disk before evicting it. This part is handled by the `Pager`
//! 
//! I am also considering leveraging Rust RAII (Resource Acquisition Is Initialization) patterns to manage page lifecycles more effectively.
//! Recommended lecture to watch: https://www.youtube.com/watch?v=aoewwZwVmv4

use crate::page::{ByteSerializable, Page, PageType};

use std::collections::{HashMap, VecDeque};
use std::io;
use std::vec::Vec;



/// Result of adding a page to the buffer pool
pub enum AddPageResult {
    /// Page was successfully added, no eviction needed
    Added,
    /// Page was successfully added, another page was evicted
    /// Contains (page_number, page, was_dirty)
    Evicted(u32, Vec<u8>, bool),
    /// Page could not be added because buffer is full and no pages can be evicted
    /// This should be handled as an OOM error in practice
    Rejected,
}

/// Buffer Frame represents a single page in the buffer pool.
/// We hold additional metadata to manage the page lifecycle,
/// such as pin count, dirty status, last accessed time, and write protection.
#[derive(Debug)]
pub struct BufferFrame {
    page: Page,
    pin_count: u32,
    is_dirty: bool,
    /// Track when the page was last accessed for better LRU
    last_accessed: std::time::Instant,
    /// Track if the page is being written to prevent concurrent access issues
    is_being_written: bool,
}

impl BufferFrame {
    pub fn new(page: Page) -> Self {
        BufferFrame {
            page,
            pin_count: 0,
            is_dirty: false,
            last_accessed: std::time::Instant::now(),
            is_being_written: false,
        }
    }

    pub fn pin(&mut self) {
       
        self.pin_count += 1;
        self.last_accessed = std::time::Instant::now();
    }

    pub fn unpin(&mut self) -> bool {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        self.pin_count == 0
    }

    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.last_accessed = std::time::Instant::now();
    }

    pub fn reset_dirty(&mut self) {
        self.is_dirty = false;
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        self.last_accessed = std::time::Instant::now();
        &mut self.page
    }

    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn last_accessed(&self) -> std::time::Instant {
        self.last_accessed
    }

    pub fn set_being_written(&mut self, writing: bool) {
        self.is_being_written = writing;
    }

    pub fn is_being_written(&self) -> bool {
        self.is_being_written
    }
}

/// BufferPool represents a pool of pages in memory.
/// It uses a LRU-K eviction policy to manage pages efficiently.
pub struct BufferPool {
    max_pages: usize,
    frames: HashMap<u32, BufferFrame>,
    lru_list: VecDeque<u32>, // Queue of pages in LRU order
    /// Statistics for monitoring buffer pool performance
    stats: BufferPoolStats, // Basic statistics just for tracking
}

#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub pages_evicted: u64,
    pub pages_written: u64,
    pub pin_operations: u64,
    pub unpin_operations: u64,
}

impl BufferPoolStats {
    // Compute the cache hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }
}

impl BufferPool {
    pub fn new(max_pages: usize) -> Self {
        BufferPool {
            max_pages,
            frames: HashMap::with_capacity(max_pages),
            lru_list: VecDeque::with_capacity(max_pages),
            stats: BufferPoolStats::default(),
        }
    }

    /// Add a page to the buffer pool.
    /// If the page already exists, it will be updated (increasing pin count).
    /// If the buffer is full, it will try to evict a page using LRU-K policy.
    /// Returns an `AddPageResult` indicating the outcome of the operation.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// * `page`: The page to be added.
    /// * `pin`: Whether to pin the page immediately after adding it.
    /// # Returns
    /// An `AddPageResult` indicating whether the page was added, evicted, or rejected.
    pub fn add_page(&mut self, page_number: u32, page: Page, pin: bool) -> AddPageResult {
        // If page already exists, just update it
        if self.frames.contains_key(&page_number) {
            if let Some(frame) = self.frames.get_mut(&page_number) {
                frame.page = page;
                if pin {
                    frame.pin();
                    self.stats.pin_operations += 1; // Update statistics
                }
            }
            return AddPageResult::Added;
        }

        // Check if we need to evict
        if self.frames.len() >= self.max_pages {
            // Try to evict a page
            let evicted = self.evict_page();
            
            match evicted {
                Some((evicted_num, evicted_page)) => {
                    // Get dirty status before the page is gone
                    let was_dirty = self.frames.get(&evicted_num)
                        .map(|f| f.is_dirty())
                        .unwrap_or(false);
                    
                    // Now add the new page
                    let mut frame = BufferFrame::new(page);
                    if pin {
                        frame.pin();
                        self.stats.pin_operations += 1;
                    } else {
                        // If we are not requested to pin the page, we add it to the LRU list
                        // This is important to keep the LRU order correct
                        self.lru_list.push_back(page_number);
                    }
                    self.frames.insert(page_number, frame);

                    // I am not sure if this could be handled by the Pager to avoid logic duplication and keep this more SOLID.
                    // However I have been running into issues when releasing the evicted page to the Pager previousle, although this should be already fixed with the new implementation with guards and callbacks.
                    // Anyway, I do not think this can cause any problem.
                    let size = evicted_page.page_size();
                    let mut buffer = vec![0u8; size as usize];
                    evicted_page.write_to(&mut buffer).expect("Failed to serialize page to buffer");
                    
                    
                    return AddPageResult::Evicted(evicted_num, buffer, was_dirty);
                }
                None => {
                    // Could not evict any page, page is rejected
                    return AddPageResult::Rejected;
                }
            }
        }

        // Buffer has space, just add the page
        let mut frame = BufferFrame::new(page);
        if pin {
            frame.pin();
            self.stats.pin_operations += 1;
        } else {
            self.lru_list.push_back(page_number);
        }
        self.frames.insert(page_number, frame);
        
        AddPageResult::Added
    }

    

    /// Check if a page is in the buffer pool.
    /// Recently added statistic tracing for cache hits and misses.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// A boolean indicating whether the page is in the buffer pool.
    pub fn contains_page(&mut self, page_number: u32) -> bool {
        let contains = self.frames.contains_key(&page_number);
        if contains {
            self.stats.cache_hits += 1;
        } else {
            self.stats.cache_misses += 1;
        }
        contains
    }

    /// I recently moved this logic from th Pager to here to keep the validation in memory.
    /// Validate the page type of a given page in the buffer pool.
    /// This method checks if the page type matches the expected type.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// * `expected_type`: The expected page type to validate against.
    /// # Returns
    /// A `Result` indicating success or an error message if the validation fails.
    pub fn validate_page_type(&self, page_number: u32, expected_type: PageType) -> Result<(), String> {
        if let Some(frame) = self.frames.get(&page_number) {
            let actual_type = frame.page().page_type();
            if actual_type != expected_type {
                return Err(format!(
                    "Page type mismatch for page {}: expected {:?}, found {:?}",
                    page_number, expected_type, actual_type
                ));
            }
        } else {
            return Err(format!("Page {} not found in buffer pool", page_number));
        }
        Ok(())
    }

    /// Get a page from the buffer pool.
    /// This method retrieves a page by its number, updates its last accessed time,
    /// and increments the pin count.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// An `Option` containing a reference to the page if it exists, or `None` if it does not.
    pub fn get_page(&mut self, page_number: u32) -> Option<&Page> {
        if !self.frames.contains_key(&page_number) {
            return None;
        }

        self.touch_page(page_number);
        
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            self.stats.pin_operations += 1;
            return Some(frame.page());
        }

        None
    }

    /// Similar to `get_page`, but returns a mutable reference to the page.
    /// This method retrieves a mutable reference to a page by its number,
    /// updates its last accessed time, and increments the pin count.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// An `Option` containing a mutable reference to the page if it exists, or `None` if it does not.
    pub fn get_page_mut(&mut self, page_number: u32) -> Option<&mut Page> {
        if !self.frames.contains_key(&page_number) {
            return None;
        }
        // Touch the page to update its last accessed time.
        // Insired by Linux `touch` command.
        self.touch_page(page_number);

        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            frame.mark_dirty();
            self.stats.pin_operations += 1;
            return Some(frame.page_mut());
        }

        None
    }

    /// Unpin a page in the buffer pool.
    /// This method decreases the pin count of a page,
    /// and if the pin count reaches zero, it adds the page back to the LRU list.
    /// Imprtant to add the page back to the LRU list, because it might have been removed during eviction,
    ///  and remember that when a page is added, we might not have added it.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// A boolean indicating whether the unpin operation was successful.
    pub fn unpin_page(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            let was_pinned = frame.is_pinned();
            let is_now_unpinned = frame.unpin();
            // println!("Unpinning page {}", page_number);
            if was_pinned {
              
                self.stats.unpin_operations += 1;
            }

            if is_now_unpinned && !self.lru_list.contains(&page_number) {
                // If not pinned anymore, add to LRU list
                self.lru_list.push_back(page_number);
            }
            return true;
        }
        false
    }

    /// Method to forcefully unpin a page. I added to allow for emergency unpinning,
    /// Not really needed but keeping it for now.
    /// This method sets the pin count of a page to zero,
    /// and if the page is not already in the LRU list, it adds it back.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// A boolean indicating whether the force unpin operation was successful.
    pub fn force_unpin_page(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin_count = 0;
            if !self.lru_list.contains(&page_number) {
                self.lru_list.push_back(page_number);
            }
            return true;
        }
        false
    }

    /// Get all pinned pages in the buffer pool.
    /// This method returns a vector of page numbers that are currently pinned.
    /// # Returns
    /// A vector of page numbers that are pinned.
    pub fn get_pinned_pages(&self) -> Vec<u32> {
        self.frames
            .iter()
            .filter(|(_, frame)| frame.is_pinned())
            .map(|(page_number, _)| *page_number)
            .collect()
    }


    /// Smart eviction that considers page access patterns.
    /// This replaces the original eviction logic with a more sophisticated approach based on LRU-K policy.
    /// Finds the candidates for eviction based on their last accessed time and pin status.
    /// If no candidates are found, it means the buffer is full and therefore returns `None`.
    fn evict_page(&mut self) -> Option<(u32, Page)> {
    
        // First, try to find the least recently used unpinned page
        let mut candidates: Vec<_> = self.lru_list
            .iter()
            .filter_map(|&page_number| {
                self.frames.get(&page_number).map(|frame| (page_number, frame.last_accessed(), frame.is_pinned()))
            })
            .filter(|(page_number, _,_)| {
                self.frames.get(page_number).is_some_and(|frame| !frame.is_pinned())
            })
            .collect();
       
        //println!("Candidates for eviction: {:?}", candidates); DEBUG

        // No candidates found, buffer is full.
        // If you attempted to force an eviction, a deadlock could occur because you could be holding the same `PageGuard`
        // that is going to be evicted. `PageGuards` would not realease their owned pages, and you will be attempting to evict a page that is somewhere referenced.
        // We are evitating this here by checking if there are guards pinning pages.
        if candidates.is_empty() {
            return None
        }
        
        // println!("Candidates for eviction: {:?}", candidates);
        // Sort by last accessed time (oldest first)
        candidates.sort_by_key(|(_, last_accessed,_)| *last_accessed);
        
        if let Some((page_number, _,_)) = candidates.first() {
            
            let page_number =*page_number;
            
            
            // Remove from LRU list
            self.lru_list.retain(|&p| p != page_number);
           
            // Remove and return the page
            if let Some(frame) = self.frames.remove(&page_number) {
                self.stats.pages_evicted += 1;
                
                return Some((page_number, frame.page.clone()));
            }
        }
        None
        
        // Fallback to original eviction logic
        //self.evict_page_original()
        // Legacy code, not used anymore.
    }


    /// Mark a page as dirty.
    /// This method sets the dirty flag for a page, indicating that it has been modified and needs to be written back to disk.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// A boolean indicating whether the page was successfully marked as dirty.
    pub fn mark_dirty(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.mark_dirty();
            return true;
        }
        false
    }

    /// Basically the opposite of `mark_dirty`.
    pub fn mark_clean(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.reset_dirty();
            return true;
        }
        false
    }

    /// Mark all pages in the buffer pool as clean.
    pub fn mark_clean_all(&mut self) {
        for frame in self.frames.values_mut() {
            frame.reset_dirty();
        }
    }

    /// Get all dirty pages in the buffer pool.
    pub fn get_dirty_pages(&self) -> Vec<(u32, &Page)> {
        self.frames
            .iter()
            .filter(|(_, frame)| frame.is_dirty() && !frame.is_being_written())
            .map(|(page_number, frame)| (*page_number, frame.page()))
            .collect()
    }

    /// Method used to remove a page from the buffer pool.
    /// This method removes a page by its number, ensuring that it is not pinned or being written to.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    /// # Returns
    /// An `Option` containing the removed page if it was successfully removed, or `None` if the page could not be removed (e.g., if it is pinned or being written to).
    pub fn remove_page(&mut self, page_number: u32) -> Option<Page> {
        // Check if the page is in the buffer pool
        if let Some(frame) = self.frames.get(&page_number) {
            // Cannot remove a pinned page
            if frame.is_pinned() {
                return None;
            }
            
            // Cannot remove a page being written
            if frame.is_being_written() {
                return None;
            }
        } else {
            return None;
        }

        // Remove the page from the LRU list
        self.lru_list.retain(|&p| p != page_number);

        // Remove and return the page
        self.frames.remove(&page_number).map(|frame| frame.page)
    }

    /// Touch page to update its last accessed time.
    /// This method is used to mark a page as recently accessed,
    /// updating its last accessed time and moving it to the back of the LRU list if it is not pinned.
    /// # Parameters
    /// * `page_number`: The unique identifier for the page.
    fn touch_page(&mut self, page_number: u32) {
        // Remove the page from the LRU list
        self.lru_list.retain(|&p| p != page_number);

        // Add the page to the back of the LRU list if it's not pinned
        if let Some(frame) = self.frames.get_mut(&page_number) {
            if !frame.is_pinned() {
                self.lru_list.push_back(page_number);
            }
            frame.last_accessed = std::time::Instant::now();
        }
    }

    /// Utility method to check if a page is dirty
    pub fn is_dirty(&self, page_number: u32) -> bool {
        self.frames
            .get(&page_number)
            .map_or(false, |frame| frame.is_dirty())
    }

    /// Utility method to check if a page is pinned
    pub fn is_pinned(&self, page_number: u32) -> bool {
        self.frames
            .get(&page_number)
            .map_or(false, |frame| frame.is_pinned())
    }

    /// Utility method to get the pin count of a page
    pub fn pin_count(&self, page_number: u32) -> u32 {
        self.frames
            .get(&page_number)
            .map_or(0, |frame| frame.pin_count())
    }


    // ---------------------------- TESTING AND DEBUGGING METHODS ---------------------------- //

    /// Update page content. This method allows updating the content of a page in the buffer pool.
    /// I just used it for testing on the Pager, but it can be useful in other scenarios as well.
    pub fn update_page(&mut self, page_number: u32, page: Page) -> Result<(), String> {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.page = page;
            frame.mark_dirty();
            Ok(())
        } else {
            Err(format!("Page {} not found in buffer pool", page_number))
        }
    }

    /// Get current buffer pool statistics.
    pub fn get_stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = BufferPoolStats::default();
    }


    /// Get current capacity utilization
    pub fn utilization(&self) -> f64 {
        self.frames.len() as f64 / self.max_pages as f64
    }

    /// Get maximum number of pages this buffer pool can hold
    pub fn max_pages(&self) -> usize {
        self.max_pages
    }

    /// Get current number of pages in the buffer pool
    pub fn page_count(&self) -> usize {
        self.frames.len()
    }

    /// Get number of pinned pages
    pub fn pinned_page_count(&self) -> usize {
        self.frames.values().filter(|frame| frame.is_pinned()).count()
    }

    /// Get number of dirty pages
    pub fn dirty_page_count(&self) -> usize {
        self.frames.values().filter(|frame| frame.is_dirty()).count()
    }

    /// Validate buffer pool integrity (for debugging)
    pub fn validate_integrity(&self) -> Result<(), String> {
        // Check that all pages in LRU list exist in frames
        for &page_number in &self.lru_list {
            if !self.frames.contains_key(&page_number) {
                return Err(format!("Page {} in LRU list but not in frames", page_number));
            }
        }

        // Check that no pinned pages are in LRU list
        for &page_number in &self.lru_list {
            if let Some(frame) = self.frames.get(&page_number) {
                if frame.is_pinned() && self.max_pages <= self.frames.len() {// This is only a problem if we have reached the frame limit.
                    return Err(format!("Pinned page {} found in LRU list", page_number));
                }
            }
        }

        // Check that frame count doesn't exceed maximum
        if self.frames.len() > self.max_pages {
            return Err(format!(
                "Frame count {} exceeds maximum {}",
                self.frames.len(),
                self.max_pages
            ));
        }

        Ok(())
    }


    /// The subsequent methods were added after the last refactoring in the Pager to support better page lifecycle management,
    /// better statistics tracking, and more robust error handling. I also started using RAII patterns to manage page lifecycles more effectively.
    /// Prepare page for writing (mark as being written)
    pub fn prepare_page_for_write(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.set_being_written(true);
            true
        } else {
            false
        }
    }

    /// Finish writing page (unmark as being written)
    pub fn finish_page_write(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.set_being_written(false);
            self.stats.pages_written += 1;
            true
        } else {
            false
        }
    }

    /// Force cleanup of all unpinned pages (emergency use)
    pub fn force_cleanup(&mut self) -> usize {
        let unpinned_pages: Vec<u32> = self.frames
            .iter()
            .filter(|(_, frame)| !frame.is_pinned())
            .map(|(page_number, _)| *page_number)
            .collect();

        let count = unpinned_pages.len();
        for page_number in unpinned_pages {
            self.frames.remove(&page_number);
            self.lru_list.retain(|&p| p != page_number);
        }

        count
    }

    /// Additional methods for RAII guard support

    /// Simpler contains check without statistics. More efficient for quick checks. To be used in actual Pager code.
    pub fn contains_page_simple(&self, page_number: u32) -> bool {
        self.frames.contains_key(&page_number)
    }

    /// Pin a page for use with guards. This decouples the pinning logic from the get_page method,
    /// allowing for more flexible page management.
    pub fn pin_page_for_guard(&mut self, page_number: u32) -> io::Result<()> {
        
        if let Some(frame) = self.frames.get_mut(&page_number) {
           
            frame.pin();
            // println!("Pinning page {} for guard", page_number);
            // println!("Page {} is now pinned. Pin count is {}", page_number, frame.pin_count());
            // println!("Page {} is pinned: {}", page_number, frame.is_pinned());  
            self.stats.pin_operations += 1;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ))
        }
    }

    /// Pin a page for mutable use with guards
    pub fn pin_page_for_guard_mut(&mut self, page_number: u32) -> io::Result<()> {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            frame.mark_dirty();
            self.stats.pin_operations += 1;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ))
        }
    }

    /// Get page reference for guard (immutable)
    pub fn get_page_ref(&self, page_number: u32) -> Option<&Page> {
        self.frames.get(&page_number).map(|frame| frame.page())
    }

    /// Get page reference for guard (mutable)
    pub fn get_page_mut_ref(&mut self, page_number: u32) -> Option<&mut Page> {
        self.frames.get_mut(&page_number).map(|frame| frame.page_mut())
    }

    /// Get page for journal (cloning). This method is used to get a page for journaling purposes,
    /// ensuring that the page is not modified in the buffer pool. See the `Pager` for more details.
    pub fn get_page_for_journal(&self, page_number: u32) -> Option<Page> {
        self.frames.get(&page_number).map(|frame| frame.page().clone())
    }
}

#[cfg(test)]
mod buffer_pool_tests {
    use super::*;
    use crate::page::{BTreePage, BTreePageHeader, Page, PageType};

    fn create_test_page(page_number: u32) -> Page {
        let header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        let btree_page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size: 4096,
            page_number,
            reserved_space: 0,
        };
        Page::BTree(btree_page)
    }

    #[test]
    fn test_enhanced_buffer_pool_stats() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Test cache miss
        assert!(!pool.contains_page(1));
        assert_eq!(pool.get_stats().cache_misses, 1);
        
        // Add page
        pool.add_page(1, page1, false);
        
        // Test cache hit
        assert!(pool.contains_page(1));
        assert_eq!(pool.get_stats().cache_hits, 1);
        
        // Test pin operation
        pool.get_page(1);
        assert_eq!(pool.get_stats().pin_operations, 1);
        
        // Test unpin operation
        pool.unpin_page(1);
        assert_eq!(pool.get_stats().unpin_operations, 1);
    }

    #[test]
    fn test_smart_eviction() {
        let mut pool = BufferPool::new(2);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        // Add two pages
        pool.add_page(1, page1, false);
        std::thread::sleep(std::time::Duration::from_millis(10));
        pool.add_page(2, page2, false);
        
        // Access page 1 to make it more recently used
        pool.get_page(1);
        pool.unpin_page(1);
        
        // Add page 3, should evict page 2 (least recently used)
        let evicted = pool.add_page(3, page3, false);
        match evicted {
            AddPageResult::Evicted(evicted_page_number, _, _) => {
                assert_eq!(evicted_page_number, 2);
            }
            _ => panic!("Expected page eviction"),
        }
        
        // Verify stats
        assert_eq!(pool.get_stats().pages_evicted, 1);
    }

    #[test]
    fn test_pinned_pages_tracking() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        
        pool.add_page(1, page1, true);
        pool.add_page(2, page2, false);
        
        let pinned_pages = pool.get_pinned_pages();
        assert_eq!(pinned_pages.len(), 1);
        assert!(pinned_pages.contains(&1));
    }

    #[test]
    fn test_force_unpin() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, true);
        assert!(pool.is_pinned(1));
        assert_eq!(pool.pin_count(1), 1);
        
        // Force unpin
        pool.force_unpin_page(1);
        assert!(!pool.is_pinned(1));
        assert_eq!(pool.pin_count(1), 0);
    }

    #[test]
    fn test_integrity_validation() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Should pass integrity check
        assert!(pool.validate_integrity().is_ok());
        
        // Pin the page
        pool.get_page(1);
        
        // Should still pass integrity check
        assert!(pool.validate_integrity().is_ok());
    }

    #[test]
    fn test_utilization_metrics() {
        let mut pool = BufferPool::new(4);
        assert_eq!(pool.utilization(), 0.0);
        
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        
        pool.add_page(1, page1, false);
        assert_eq!(pool.utilization(), 0.25);
        
        pool.add_page(2, page2, false);
        assert_eq!(pool.utilization(), 0.5);
        
        assert_eq!(pool.page_count(), 2);
        assert_eq!(pool.pinned_page_count(), 0);
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn test_write_protection() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Prepare page for writing
        pool.prepare_page_for_write(1);
        
        // Page should not be in dirty pages list while being written
        let dirty_pages = pool.get_dirty_pages();
        assert_eq!(dirty_pages.len(), 0);
        
        // Cannot remove page while being written
        let removed = pool.remove_page(1);
        assert!(removed.is_none());
        
        // Finish writing
        pool.finish_page_write(1);
        assert_eq!(pool.get_stats().pages_written, 1);
    }

    #[test]
    fn test_force_cleanup() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        pool.add_page(1, page1, true);  // Pinned
        pool.add_page(2, page2, false); // Not pinned
        pool.add_page(3, page3, false); // Not pinned
        
        assert_eq!(pool.page_count(), 3);
        
        // Force cleanup should remove unpinned pages
        let removed_count = pool.force_cleanup();
        assert_eq!(removed_count, 2);
        assert_eq!(pool.page_count(), 1);
        assert!(pool.contains_page(1)); // Pinned page should remain
    }

    #[test]
    fn test_page_type_validation() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Valid type should pass
        assert!(pool.validate_page_type(1, PageType::TableLeaf).is_ok());
        
        // Invalid type should fail
        let result = pool.validate_page_type(1, PageType::TableInterior);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("type mismatch"));
    }
}