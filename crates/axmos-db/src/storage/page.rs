use std::{
    fmt::Debug,
    io::{Error as IoError, ErrorKind},
    iter, mem,
    ops::{Bound, RangeBounds},
    ptr::NonNull,
    slice,
};

use crate::{
    DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, as_slice,
    configs::{
        AXMO, AxmosDBConfig, CELL_ALIGNMENT, DEFAULT_CACHE_SIZE, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
        MIN_PAGE_SIZE, PAGE_ALIGNMENT,
    },
    storage::{
        buffer::{Allocatable, MemBlock},
        cell::{CELL_HEADER_SIZE, CellHeader, CellMut, CellRef, OwnedCell, Slot},
        ops::PageOps,
    },
    types::{PAGE_ZERO, PageId},
};

pub(crate) const SIGNATURE_SIZE: usize = 64; // Ed25519
pub(crate) const SIGNATURE_OFFSET: usize = PAGE_ZERO_HEADER_SIZE - SIGNATURE_SIZE;

/// Slotted page header.
#[derive(Debug)]
#[repr(C, align(64))]
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

as_slice!(BtreePageHeader);

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(64))]
pub(crate) struct OverflowPageHeader {
    pub(crate) page_number: PageId,
    pub(crate) next: Option<PageId>,
    pub(crate) num_bytes: u32,
    pub(crate) padding: u32,
}
pub(crate) const OVERFLOW_HEADER_SIZE: usize = mem::size_of::<OverflowPageHeader>();

as_slice!(OverflowPageHeader);

impl OverflowPageHeader {
    pub(crate) fn new(page_number: PageId, size: u32) -> Self {
        let effective_size = size.next_multiple_of(PAGE_ALIGNMENT);
        Self {
            page_number,
            next: None,
            num_bytes: effective_size.saturating_sub(OVERFLOW_HEADER_SIZE as u32),
            padding: effective_size.saturating_sub(size),
        }
    }
}

impl Default for OverflowPageHeader {
    fn default() -> Self {
        let id = PageId::new();
        Self::new(id, DEFAULT_PAGE_SIZE)
    }
}

/// Page Zero must store a little bit more data about the database itself.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C, align(64))]
pub(crate) struct PageZeroHeader {
    pub(crate) page_number: PageId,
    pub(crate) right_child: Option<PageId>,
    pub(crate) next_sibling: Option<PageId>,
    pub(crate) previous_sibling: Option<PageId>,
    pub(crate) first_free_page: Option<PageId>,
    pub(crate) last_free_page: Option<PageId>,
    pub(crate) axmo: u32,
    pub(crate) page_size: u32,
    pub(crate) total_pages: u32,
    pub(crate) free_pages: u32,
    pub(crate) free_space_ptr: u32,
    pub(crate) free_space: u32,
    pub(crate) padding: u32,
    pub(crate) num_slots: u16,
    pub(crate) cache_size: u16,
    pub(crate) min_keys: u8,
    pub(crate) num_siblings_per_side: u8,
    pub(crate) reserved: [u8; SIGNATURE_SIZE], // 64 bytes are reserved for the signature.
}
pub(crate) const PAGE_ZERO_HEADER_SIZE: usize = mem::size_of::<PageZeroHeader>();

as_slice!(PageZeroHeader);

impl Default for PageZeroHeader {
    fn default() -> Self {
        Self::new(
            DEFAULT_PAGE_SIZE,
            DEFAULT_BTREE_MIN_KEYS,
            DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE,
            DEFAULT_CACHE_SIZE as u16,
        )
    }
}

impl PageZeroHeader {
    pub(crate) fn new(
        page_size: u32,
        min_keys: u8,
        num_siblings_per_side: u8,
        cache_size: u16,
    ) -> Self {
        let aligned_size = page_size.next_multiple_of(PAGE_ALIGNMENT);
        Self {
            page_number: PAGE_ZERO,
            num_slots: 0,
            page_size: aligned_size,
            free_space: aligned_size.saturating_sub(PAGE_ZERO_HEADER_SIZE as u32),
            free_space_ptr: aligned_size.saturating_sub(PAGE_ZERO_HEADER_SIZE as u32),
            right_child: None,
            padding: aligned_size.saturating_sub(page_size),
            next_sibling: None,
            previous_sibling: None,
            axmo: AXMO,
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

    pub(crate) fn from_config(config: AxmosDBConfig) -> Self {
        Self::new(
            config.page_size,
            config.min_keys_per_page,
            config.num_siblings_per_side,
            config.cache_size,
        )
    }
}

impl From<AxmosDBConfig> for PageZeroHeader {
    fn from(value: AxmosDBConfig) -> Self {
        Self::from_config(value)
    }
}

impl Default for BtreePageHeader {
    fn default() -> Self {
        let id = PageId::new();
        Self::new(id, DEFAULT_PAGE_SIZE)
    }
}

impl BtreePageHeader {
    fn new(page_number: PageId, page_size: u32) -> Self {
        let aligned_size = page_size.next_multiple_of(PAGE_ALIGNMENT);

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

impl PageOps for PageZero {
    const HEADER_SIZE: usize = PAGE_ZERO_HEADER_SIZE;

    fn total_capacity(&self) -> usize {
        self.capacity()
    }

    fn total_usable_space(&self, size: usize) -> usize {
        Self::usable_space(size)
    }

    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16 {
        (self.num_slots() as usize * Slot::SIZE + PAGE_ZERO_HEADER_SIZE) as u16
    }

    fn page_number(&self) -> PageId {
        self.metadata().page_number
    }

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> u16 {
        self.metadata().num_slots
    }

    /// Get a valid pointer to the start of the slot array (after the header)
    fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.data.cast(), self.num_slots() as usize)
    }

    /// Get a cell at a given offset in the page.
    unsafe fn cell_at_offset(&self, offset: u16) -> NonNull<CellHeader> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }

    /// Get the free space in the page.
    fn free_space(&self) -> u32 {
        self.metadata().free_space
    }

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32 {
        self.metadata().free_space_ptr
    }

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId> {
        self.metadata().right_child
    }

    /// Write the content of a cell at a given offset in the page.
    ///
    /// # SAFETY
    ///
    /// This function is unsafe.
    /// The caller must ensure the offset is valid (respects [CELL_ALIGNMENT]) and be aware that this function does not check if you are replacing the content of an existing cell.
    unsafe fn write_cell_unchecked(&mut self, offset: u32, cell: CellRef<'_>) {
        let total_size = cell.total_size() as u32;
        let storage_size = cell.storage_size() as u32;
        // Write new cell.
        // SAFETY: `last_used_offset` keeps track of where the last cell was
        // written. By substracting the total size of the new cell to
        // `last_used_offset` we get a valid pointer within the page where we
        // write the new cell.
        unsafe {
            let dest = self.data.byte_add(offset as usize).cast::<u8>().as_ptr();
            let dest_slice = slice::from_raw_parts_mut(dest, cell.total_size());
            cell.write_to(dest_slice);
        };
    }

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32) {
        self.metadata_mut().free_space += space;
    }

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32) {
        self.metadata_mut().free_space -= space;
    }

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32) {
        self.metadata_mut().free_space_ptr -= bytes;
    }

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32) {
        self.metadata_mut().free_space_ptr += bytes;
    }

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32) {
        self.metadata_mut().free_space_ptr = offset;
    }

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32) {
        self.metadata_mut().free_space = bytes;
    }
    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots = num;
    }

    /// Add slots to the page
    fn add_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots += num;
    }

    /// Remove slots from the page
    fn remove_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots -= num;
    }

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId> {
        self.metadata().next_sibling
    }
    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: PageId) {
        self.metadata_mut().next_sibling = Some(next);
    }

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId> {
        self.metadata().previous_sibling
    }

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, next: PageId) {
        self.metadata_mut().previous_sibling = Some(next);
    }
}

impl PageZero {
    /// Allocate a new [`PageZero`] with the given configuration, initializing the header accordingly.
    pub(crate) fn alloc(config: AxmosDBConfig) -> Self {
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

impl PageOps for BtreePage {
    const HEADER_SIZE: usize = BTREE_PAGE_HEADER_SIZE;

    fn total_capacity(&self) -> usize {
        self.capacity()
    }

    fn total_usable_space(&self, size: usize) -> usize {
        Self::usable_space(size)
    }

    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16 {
        (self.num_slots() as usize * Slot::SIZE + BTREE_PAGE_HEADER_SIZE) as u16
    }

    fn page_number(&self) -> PageId {
        self.metadata().page_number
    }

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> u16 {
        self.metadata().num_slots
    }

    /// Get a valid pointer to the start of the slot array (after the header)
    fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.data.cast(), self.num_slots() as usize)
    }

    /// Get a cell at a given offset in the page.
    unsafe fn cell_at_offset(&self, offset: u16) -> NonNull<CellHeader> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }

    /// Get the free space in the page.
    fn free_space(&self) -> u32 {
        self.metadata().free_space
    }

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32 {
        self.metadata().free_space_ptr
    }

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId> {
        self.metadata().right_child
    }

    /// Write the content of a cell at a given offset in the page.
    ///
    /// # SAFETY
    ///
    /// This function is unsafe.
    /// The caller must ensure the offset is valid (respects [CELL_ALIGNMENT]) and be aware that this function does not check if you are replacing the content of an existing cell.
    unsafe fn write_cell_unchecked(&mut self, offset: u32, cell: CellRef<'_>) {
        let total_size = cell.total_size() as u32;
        let storage_size = cell.storage_size() as u32;
        // Write new cell.
        // SAFETY: `last_used_offset` keeps track of where the last cell was
        // written. By substracting the total size of the new cell to
        // `last_used_offset` we get a valid pointer within the page where we
        // write the new cell.
        unsafe {
            let dest = self.data.byte_add(offset as usize).cast::<u8>().as_ptr();
            let dest_slice = slice::from_raw_parts_mut(dest, cell.total_size());
            cell.write_to(dest_slice);
        };
    }

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32) {
        self.metadata_mut().free_space += space;
    }

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32) {
        self.metadata_mut().free_space -= space;
    }

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32) {
        self.metadata_mut().free_space_ptr -= bytes;
    }

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32) {
        self.metadata_mut().free_space_ptr += bytes;
    }

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32) {
        self.metadata_mut().free_space_ptr = offset;
    }

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32) {
        self.metadata_mut().free_space = bytes;
    }
    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots = num;
    }

    /// Add slots to the page
    fn add_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots += num;
    }

    /// Remove slots from the page
    fn remove_slots(&mut self, num: u16) {
        self.metadata_mut().num_slots -= num;
    }

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId> {
        self.metadata().next_sibling
    }
    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: PageId) {
        self.metadata_mut().next_sibling = Some(next);
    }

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId> {
        self.metadata().previous_sibling
    }

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, next: PageId) {
        self.metadata_mut().previous_sibling = Some(next);
    }
}

impl Allocatable for BtreePage {
    /// Allocate a new [`BtreePage`] of the given size, initializing the header accordingly
    fn alloc(page_size: u32) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size),
            "page size {page_size} is not a value between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        );
        let id = PageId::new();
        let mut buffer = MemBlock::<BtreePageHeader>::new(page_size as usize);
        *buffer.metadata_mut() = BtreePageHeader::new(id, page_size);

        buffer
    }
}

impl BtreePage {
    pub(crate) fn dealloc(self) -> OverflowPage {
        let number = self.page_number();
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
impl Allocatable for OverflowPage {
    /// Alocate a new [OverflowPage] of the given [page_size]
    fn alloc(page_size: u32) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size),
            "page size {page_size} is not a value between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        );

        let mut buf = MemBlock::<OverflowPageHeader>::new(page_size as usize);
        let id = PageId::new();
        *buf.metadata_mut() = OverflowPageHeader::new(id, page_size);

        buf
    }
}

impl OverflowPage {
    /// Size of the header in an overflow page
    pub(crate) const HEADER_SIZE: usize = OVERFLOW_HEADER_SIZE;

    /// Returns the page number (page id) of this overflow page.
    pub(crate) fn page_number(&self) -> PageId {
        self.metadata().page_number
    }

    /// Returns a read-only reference to the payload (not the entire data).
    /// TODO: Add a push bytes method.
    pub(crate) fn payload(&self) -> &[u8] {
        &self.data()[..self.metadata().num_bytes as usize]
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
        page.cast::<BtreePageHeader>()
    }
}

unsafe impl Send for PageZero {}
unsafe impl Sync for PageZero {}

unsafe impl Send for BtreePage {}
unsafe impl Sync for BtreePage {}

unsafe impl Send for OverflowPage {}
unsafe impl Sync for OverflowPage {}
