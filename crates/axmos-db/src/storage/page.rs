use std::{fmt::Debug, mem};

use crate::{
    DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, DEFAULT_POOL_SIZE, ObjectId,
    TransactionId, bytemuck_slice,
    common::{
        DBConfig, DEFAULT_CACHE_SIZE, DEFAULT_PAGE_SIZE, MAGIC, MAX_PAGE_SIZE, MIN_PAGE_SIZE,
        PAGE_ALIGNMENT,
    },
    storage::{
        Allocatable, AvailableSpace, BtreeMetadata, Identifiable,
        cell::{CellHeader, Slot},
        core::{buffer::MemBlock, traits::ComposedMetadata},
    },
    types::{PAGE_ZERO, PageId},
};

pub(crate) const SIGNATURE_SIZE: usize = 64; // Ed25519
pub(crate) const SIGNATURE_OFFSET: usize = PAGE_ZERO_HEADER_SIZE - SIGNATURE_SIZE;

/// Slotted page header.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub(crate) struct BtreePageHeader {
    pub(crate) page_number: PageId,
    pub(crate) right_child: Option<PageId>,
    pub(crate) next_sibling: Option<PageId>,
    pub(crate) previous_sibling: Option<PageId>,
    pub(crate) free_space_ptr: u32,
    pub(crate) page_size: u32,
    pub(crate) free_space: u32,
    pub(crate) padding: u32,
    pub(crate) num_slots: u16,
}
pub(crate) const BTREE_PAGE_HEADER_SIZE: usize = mem::size_of::<BtreePageHeader>();

bytemuck_slice!(BtreePageHeader);

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(8))]
pub(crate) struct OverflowPageHeader {
    pub(crate) page_number: PageId,
    pub(crate) next: Option<PageId>,
    pub(crate) num_bytes: u32,
    pub(crate) padding: u32,
}
pub(crate) const OVERFLOW_HEADER_SIZE: usize = mem::size_of::<OverflowPageHeader>();

bytemuck_slice!(OverflowPageHeader);

impl OverflowPageHeader {
    pub(crate) fn new(page_number: PageId, size: u32) -> Self {
        let effective_size = size.next_multiple_of(PAGE_ALIGNMENT as u32);
        Self {
            page_number,
            next: None,
            num_bytes: effective_size.saturating_sub(OVERFLOW_HEADER_SIZE as u32),
            padding: effective_size.saturating_sub(size),
        }
    }
}

/// Page Zero must store a little bit more data about the database itself.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C, align(8))]
pub(crate) struct PageZeroHeader {
    pub(crate) magic: u64,
    pub(crate) page_number: PageId,
    pub(crate) first_free_page: Option<PageId>,
    pub(crate) last_free_page: Option<PageId>,
    pub(crate) total_pages: u64,
    pub(crate) last_created_transaction: TransactionId,
    pub(crate) last_committed_transaction: TransactionId,
    pub(crate) last_stored_object: ObjectId,
    pub(crate) page_size: u32,
    pub(crate) free_pages: u32,
    pub(crate) padding: u32,
    pub(crate) cache_size: u16,
    pub(crate) min_keys: u8,
    pub(crate) num_siblings_per_side: u8,
    pub(crate) reserved: [u8; SIGNATURE_SIZE], // 64 bytes are reserved for the signature.
}
pub(crate) const PAGE_ZERO_HEADER_SIZE: usize = mem::size_of::<PageZeroHeader>();

bytemuck_slice!(PageZeroHeader);

impl Default for PageZeroHeader {
    fn default() -> Self {
        Self::new(
            PAGE_ZERO,
            DEFAULT_PAGE_SIZE as u32,
            DEFAULT_BTREE_MIN_KEYS as u8,
            DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE as u8,
            DEFAULT_CACHE_SIZE as u16,
        )
    }
}

impl PageZeroHeader {
    pub(crate) fn new(
        page_number: PageId,
        page_size: u32,
        min_keys: u8,
        num_siblings_per_side: u8,
        cache_size: u16,
    ) -> Self {
        let aligned_size = page_size.next_multiple_of(PAGE_ALIGNMENT as u32);
        Self {
            magic: MAGIC,
            page_number,
            page_size: aligned_size,
            padding: aligned_size.saturating_sub(page_size),
            last_created_transaction: 0,
            last_committed_transaction: 0,
            last_stored_object: 0,
            total_pages: 1,
            free_pages: 0,
            first_free_page: None,
            last_free_page: None,
            min_keys, // DEFAULTS TO THREE
            num_siblings_per_side,
            cache_size,
            reserved: [0u8; SIGNATURE_SIZE],
        }
    }

    pub(crate) fn from_config(config: DBConfig) -> Self {
        Self::new(
            PAGE_ZERO,
            config.page_size as u32,
            config.min_keys_per_page as u8,
            config.num_siblings_per_side as u8,
            config.cache_size as u16,
        )
    }
}

impl From<DBConfig> for PageZeroHeader {
    fn from(value: DBConfig) -> Self {
        Self::from_config(value)
    }
}

impl BtreePageHeader {
    fn new(page_number: PageId, page_size: u32) -> Self {
        let aligned_size = page_size.next_multiple_of(PAGE_ALIGNMENT as u32);

        Self {
            page_number,
            num_slots: 0,
            page_size: aligned_size,
            free_space: aligned_size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            free_space_ptr: aligned_size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            right_child: None,
            padding: aligned_size.saturating_sub(page_size),
            next_sibling: None,
            previous_sibling: None,
        }
    }
}

pub(crate) type BtreePage = MemBlock<BtreePageHeader>;
pub(crate) type PageZero = MemBlock<PageZeroHeader>;
pub(crate) type OverflowPage = MemBlock<OverflowPageHeader>;

impl Identifiable for PageZeroHeader {
    type IdType = PageId;

    fn id(&self) -> Self::IdType {
        self.page_number
    }
}

impl Allocatable for PageZeroHeader {
    const MAX_SIZE: usize = MAX_PAGE_SIZE;
    const MIN_SIZE: usize = MIN_PAGE_SIZE;

    fn alloc(id: Self::IdType, size: usize) -> Self {
        Self::new(
            id,
            size as u32,
            DEFAULT_BTREE_MIN_KEYS as u8,
            DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE as u8,
            DEFAULT_POOL_SIZE as u16,
        )
    }
}

impl AvailableSpace for BtreePage {
    fn available_space(&self) -> usize {
        self.metadata().free_space as usize
    }
}

impl Identifiable for OverflowPageHeader {
    type IdType = PageId;
    fn id(&self) -> Self::IdType {
        self.page_number
    }
}

impl Allocatable for OverflowPageHeader {
    const MAX_SIZE: usize = MAX_PAGE_SIZE;
    const MIN_SIZE: usize = MIN_PAGE_SIZE;

    fn alloc(id: Self::IdType, size: usize) -> Self {
        Self::new(id, size as u32)
    }
}

impl Identifiable for BtreePageHeader {
    type IdType = PageId;
    fn id(&self) -> Self::IdType {
        self.page_number
    }
}

impl Allocatable for BtreePageHeader {
    const MAX_SIZE: usize = MAX_PAGE_SIZE;
    const MIN_SIZE: usize = MIN_PAGE_SIZE;

    fn alloc(id: Self::IdType, size: usize) -> Self {
        Self::new(id, size as u32)
    }
}

impl AvailableSpace for OverflowPage {
    fn available_space(&self) -> usize {
        Self::usable_space(self.capacity()) - self.metadata().num_bytes as usize
    }
}

/// Only [PageZero] and [BtreePage] are [ComposedBuffer] since Overflow pages do not have cells.
impl ComposedMetadata for BtreePageHeader {
    type ItemHeader = CellHeader;
}

impl BtreeMetadata for BtreePageHeader {
    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16 {
        (self.num_slots() as usize * mem::size_of::<Slot>() + mem::size_of::<Self>()) as u16
    }

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> usize {
        self.num_slots as usize
    }

    /// Get the free space in the page.
    fn free_space(&self) -> u32 {
        self.free_space
    }

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32 {
        self.free_space_ptr
    }

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId> {
        self.right_child
    }

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32) {
        self.free_space += space;
    }

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32) {
        self.free_space -= space;
    }

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32) {
        self.free_space_ptr -= bytes;
    }

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32) {
        self.free_space_ptr += bytes;
    }

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32) {
        self.free_space_ptr = offset;
    }

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32) {
        self.free_space = bytes;
    }
    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: usize) {
        self.num_slots = num as u16;
    }

    /// Add slots to the page
    fn add_slots(&mut self, num: usize) {
        self.num_slots += num as u16;
    }

    /// Remove slots from the page
    fn remove_slots(&mut self, num: usize) {
        self.num_slots -= num as u16;
    }

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId> {
        self.next_sibling
    }
    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: Option<PageId>) {
        self.next_sibling = next;
    }

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId> {
        self.previous_sibling
    }

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, prev: Option<PageId>) {
        self.previous_sibling = prev;
    }

    fn set_right_child(&mut self, new_right: Option<PageId>) {
        self.right_child = new_right;
    }
}

impl PageZero {
    /// Allocate a new [`PageZero`] with the given configuration, initializing the header accordingly.
    pub(crate) fn from_config(config: DBConfig) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&config.page_size),
            "page size {} is not a value between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}",
            config.page_size
        );

        // The buffer is internally aligned to [`PAGE_ALIGNMENT`]
        let mut buffer = MemBlock::<PageZeroHeader>::new(config.page_size as usize);
        *buffer.metadata_mut() = PageZeroHeader::from_config(config);

        buffer
    }
}

impl BtreePage {
    pub(crate) fn dealloc(self) -> OverflowPage {
        let number = self.id();
        // Casts the page into an overflowpage and overwrites the header maintaining our current page id. This is necessary because the pager needs to work with consistent page numbers.
        let mut converted: MemBlock<OverflowPageHeader> = self.cast();
        let size = converted.size();

        let header = OverflowPageHeader::new(number, size as u32);
        *converted.metadata_mut() = header;

        converted.data_mut().fill(0);

        converted
    }
}

/// Overflow page logic goes here.
/// The good thing with overflow pages is that we can reuse its structure with free pages too, since they just have the header and the data content.
impl OverflowPage {
    /// Returns the page number (page id) of this overflow page.
    pub(crate) fn page_number(&self) -> PageId {
        self.metadata().page_number
    }

    /// Returns a read-only reference to the payload (not the entire data).
    /// TODO: Add a push bytes method.
    pub(crate) fn effective_data(&self) -> &[u8] {
        &self.data()[..self.metadata().num_bytes as usize]
    }

    fn is_used(&self) -> bool {
        self.metadata().num_bytes == 0
    }

    /// Push an array of bytes into the data part of the overflow page.
    pub(crate) fn push_bytes(&mut self, bytes: &[u8]) {
        assert!(
            !self.is_used(),
            "Cannot push data twice into an overflow page."
        );
        self.metadata_mut().num_bytes = bytes.len() as u32;
        self.data_mut()[..bytes.len()].copy_from_slice(bytes);
    }

    /// Returns the next overflow page in the chain.
    pub(crate) fn next(&self) -> Option<PageId> {
        self.metadata().next
    }
    /// Sets the next overflow page in the chain.
    pub(crate) fn set_next(&mut self, next: PageId) {
        self.metadata_mut().next = Some(next);
    }
}

impl From<BtreePage> for OverflowPage {
    fn from(page: BtreePage) -> OverflowPage {
        page.dealloc()
    }
}

impl From<OverflowPage> for BtreePage {
    fn from(page: OverflowPage) -> BtreePage {
        let page_number = page.id();
        let page_size = page.size();
        let mut new_page = page.cast::<BtreePageHeader>();
        *new_page.metadata_mut() = BtreePageHeader::new(page_number, page_size as u32);
        new_page
    }
}

unsafe impl Send for PageZero {}
unsafe impl Sync for PageZero {}

unsafe impl Send for BtreePage {}
unsafe impl Sync for BtreePage {}

unsafe impl Send for OverflowPage {}
unsafe impl Sync for OverflowPage {}
