pub mod errors;
pub use errors::*;

use std::thread;

pub(crate) const DEFAULT_NUM_WORKERS: usize = 4;

/// The default size for the thread pool is obtained from [thread::available_parallelism]
/// It is the standard since rust 1.59. We use 4 workers as a fallback in case the API call fails.
pub(crate) fn default_num_workers() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(DEFAULT_NUM_WORKERS)
}

/// Magic string that identifies the AxmosDB file format.
pub(crate) const AXMO: u32 = 0x41584D4F;

/// Maximum page size is 64 KiB.
pub(crate) const MIN_PAGE_SIZE: u32 = 4096;
pub(crate) const MAX_PAGE_SIZE: u32 = 65536;

/// Cache size goes in units of number of pages.
pub(crate) const DEFAULT_CACHE_SIZE: usize = 10000;
pub(crate) const DEFAULT_PAGE_SIZE: u32 = MIN_PAGE_SIZE; // 4KB
pub(crate) const DEFAULT_POOL_SIZE: u16 = 10; // Ten workers by default.
pub(crate) const DEFAULT_BTREE_MIN_KEYS: u8 = 3;
pub(crate) const DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE: u8 = 3;

/// Cells are aligned to 64 bits.
pub(crate) const CELL_ALIGNMENT: u8 = 64;

/// [`O_DIRECT`] flag in Linux systems requires that buffers are aligned to at least the logical block size of the block device which is typically 4096 bytes.
/// TODO: There should be a way to query this value to the system dynamically.
/// Must read: https://stackoverflow.com/questions/53902811/why-direct-i-o-requires-alignments
pub(crate) const PAGE_ALIGNMENT: u32 = MIN_PAGE_SIZE;

#[derive(Debug, Clone, Copy)]
pub struct AxmosDBConfig {
    pub(crate) page_size: u32,
    pub(crate) cache_size: u16,
    pub(crate) pool_size: u8,
    pub(crate) num_siblings_per_side: u8,
    pub(crate) min_keys_per_page: u8,
}

impl Default for AxmosDBConfig {
    fn default() -> Self {
        Self {
            pool_size: default_num_workers() as u8,
            num_siblings_per_side: 2,
            min_keys_per_page: 3,
            cache_size: DEFAULT_CACHE_SIZE as u16,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

impl AxmosDBConfig {
    pub(crate) fn new(
        page_size: u32,
        cache_size: u16,
        pool_size: u8,
        min_keys_per_page: u8,
        num_siblings_per_side: u8,
    ) -> Self {
        let page_size = page_size
            .next_power_of_two()
            .clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        Self {
            pool_size,
            min_keys_per_page,
            num_siblings_per_side,
            cache_size,
            page_size,
        }
    }
}
