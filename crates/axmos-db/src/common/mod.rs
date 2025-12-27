//pub mod errors;
//pub use errors::*;

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
pub(crate) const MAGIC: u64 = 0x41786D6F734442;

/// Maximum page size is 64 KiB.
pub(crate) const MIN_PAGE_SIZE: usize = 4096;
pub(crate) const MAX_PAGE_SIZE: usize = 65536;

/// Cache size goes in units of number of pages.
pub(crate) const DEFAULT_CACHE_SIZE: usize = 10000;
pub(crate) const DEFAULT_PAGE_SIZE: usize = MIN_PAGE_SIZE; // 4KB
pub(crate) const DEFAULT_POOL_SIZE: usize = 10; // Ten workers by default.
pub(crate) const DEFAULT_BTREE_MIN_KEYS: usize = 3;
pub(crate) const DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE: usize = 3;

/// Cells are aligned to 64 bits (8 bytes).
pub(crate) const CELL_ALIGNMENT: usize = 8;

/// [`O_DIRECT`] flag in Linux systems requires that buffers are aligned to at least the logical block size of the block device which is typically 4096 bytes.
/// TODO: There should be a way to query this value to the system dynamically.
/// Must read: https://stackoverflow.com/questions/53902811/why-direct-i-o-requires-alignments
pub(crate) const PAGE_ALIGNMENT: usize = MIN_PAGE_SIZE;

#[derive(Debug, Clone, Copy)]
pub struct DBConfig {
    pub(crate) page_size: usize,
    pub(crate) cache_size: usize,
    pub(crate) pool_size: usize,
    pub(crate) num_siblings_per_side: usize,
    pub(crate) min_keys_per_page: usize,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            pool_size: default_num_workers(),
            num_siblings_per_side: 2,
            min_keys_per_page: 3,
            cache_size: DEFAULT_CACHE_SIZE,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

impl DBConfig {
    pub(crate) fn new(
        page_size: usize,
        cache_size: usize,
        pool_size: usize,
        min_keys_per_page: usize,
        num_siblings_per_side: usize,
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
