use std::{
    collections::BinaryHeap,
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    iter,
    ops::{Bound, RangeBounds},
    ptr::{self, NonNull},
};

use crate::{
    configs::{
        AXMO, AxmosDBConfig, CELL_ALIGNMENT, DEFAULT_CACHE_SIZE, DEFAULT_PAGE_SIZE,
        IncrementalVaccum, MAX_PAGE_SIZE, MIN_PAGE_SIZE, PAGE_ALIGNMENT, TextEncoding,
    },
    storage::{
        buffer::BufferWithMetadata,
        cell::{CELL_HEADER_SIZE, Cell, Slot},
        latches::Latch,
    },
    types::{PAGE_ZERO, PageId},
};
use std::ops::{Deref, DerefMut};

fn max_payload_size_in(usable_space: usize) -> usize {
    (usable_space - CELL_HEADER_SIZE - Slot::SIZE) & !(CELL_ALIGNMENT as usize - 1)
}

#[inline]
fn align_down(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    value & !(align - 1)
}

/// Slotted page header.
#[derive(Debug)]
#[repr(C, align(64))]
pub(crate) struct BtreePageHeader {
    pub(crate) page_number: PageId,
    pub(crate) right_child: PageId,
    pub(crate) next_sibling: PageId,
    pub(crate) previous_sibling: PageId,
    pub(crate) free_space_ptr: u32,
    pub(crate) page_size: u32,
    pub(crate) free_space: u32,
    pub(crate) padding: u32,
    pub(crate) num_slots: u16,
}
pub(crate) const BTREE_PAGE_HEADER_SIZE: usize = std::mem::size_of::<BtreePageHeader>();

crate::as_slice!(BtreePageHeader);
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(64))]
pub(crate) struct OverflowPageHeader {
    pub(crate) page_number: PageId,
    pub(crate) next: PageId,
    pub(crate) num_bytes: u32,
    pub(crate) padding: u32,
}
pub(crate) const OVERFLOW_HEADER_SIZE: usize = std::mem::size_of::<OverflowPageHeader>();

crate::as_slice!(OverflowPageHeader);

impl Header for OverflowPageHeader {
    fn page_number(&self) -> PageId {
        self.page_number
    }

    fn alloc(size: u32) -> Self {
        let effective_size = size.next_multiple_of(PAGE_ALIGNMENT);
        debug_assert!(
            std::mem::size_of::<OverflowPageHeader>().is_multiple_of(CELL_ALIGNMENT as usize),
            "Header size is not a multiple of cell alignment. Must ensure header is aligned"
        );

        Self {
            page_number: PageId::new(),
            next: PAGE_ZERO,
            num_bytes: effective_size.saturating_sub(OVERFLOW_HEADER_SIZE as u32),
            padding: effective_size.saturating_sub(size),
        }
    }

    fn init(size: u32, page_number: PageId) -> Self {
        let effective_size = size.next_multiple_of(PAGE_ALIGNMENT);
        debug_assert!(
            std::mem::size_of::<OverflowPageHeader>().is_multiple_of(CELL_ALIGNMENT as usize),
            "Header size is not a multiple of cell alignment. Must ensure header is aligned"
        );

        Self {
            page_number,
            next: PAGE_ZERO,
            num_bytes: effective_size,
            padding: effective_size.saturating_sub(size),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C, align(64))]
pub(crate) struct DatabaseHeader {
    pub(crate) first_free_page: PageId,
    pub(crate) last_free_page: PageId,
    pub(crate) axmo: u32,
    pub(crate) page_size: u32,
    pub(crate) total_pages: u32,
    pub(crate) free_pages: u32,
    pub(crate) cache_size: u16,
    pub(crate) min_keys: u8,
    pub(crate) text_encoding: crate::TextEncoding,
    pub(crate) incremental_vacuum_mode: crate::IncrementalVaccum,
}
pub(crate) const DB_HEADER_SIZE: usize = std::mem::size_of::<DatabaseHeader>();

crate::as_slice!(DatabaseHeader);

impl Header for DatabaseHeader {
    fn page_number(&self) -> PageId {
        PAGE_ZERO
    }

    fn alloc(size: u32) -> Self {
        Self::init(size, PAGE_ZERO)
    }

    fn init(__size: u32, __page_number: PageId) -> Self {
        debug_assert!(
            std::mem::size_of::<DatabaseHeader>().is_multiple_of(CELL_ALIGNMENT as usize),
            "Header size is not a multiple of cell alignment. Must ensure header is aligned"
        );
        Self {
            axmo: AXMO,
            page_size: DEFAULT_PAGE_SIZE,
            total_pages: 1,
            free_pages: 0,
            first_free_page: PAGE_ZERO,
            last_free_page: PAGE_ZERO,
            text_encoding: TextEncoding::Utf8,
            min_keys: 3, // DEFAULTS TO THREE
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            cache_size: DEFAULT_CACHE_SIZE,
        }
    }
}

impl DatabaseHeader {
    fn from_config(config: AxmosDBConfig) -> Self {
        todo!()
    }
}

impl Header for BtreePageHeader {
    fn page_number(&self) -> PageId {
        self.page_number
    }

    fn alloc(size: u32) -> Self {
        let aligned_size = size.next_multiple_of(PAGE_ALIGNMENT);

        debug_assert!(
            std::mem::size_of::<BtreePageHeader>().is_multiple_of(CELL_ALIGNMENT as usize),
            "Header size is not a multiple of cell alignment. Must ensure header is aligned"
        );

        Self {
            page_number: PageId::new(),
            num_slots: 0,
            page_size: aligned_size,
            free_space: size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            free_space_ptr: size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            right_child: PAGE_ZERO,
            padding: aligned_size.saturating_sub(size),
            next_sibling: PAGE_ZERO,
            previous_sibling: PAGE_ZERO,
        }
    }
    fn init(size: u32, page_number: PageId) -> Self {
        let aligned_size = size.next_multiple_of(PAGE_ALIGNMENT);

        debug_assert!(
            std::mem::size_of::<BtreePageHeader>().is_multiple_of(CELL_ALIGNMENT as usize),
            "Header size is not a multiple of cell alignment. Must ensure header is aligned"
        );
        Self {
            page_number,
            num_slots: 0,
            page_size: aligned_size,
            free_space: size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            free_space_ptr: size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32),
            right_child: PAGE_ZERO,
            padding: aligned_size.saturating_sub(size),
            next_sibling: PAGE_ZERO,
            previous_sibling: PAGE_ZERO,
        }
    }
}
impl BtreePageHeader {
    pub(crate) fn content_start_ptr(&self) -> u16 {
        (self.num_slots as usize * Slot::SIZE + BTREE_PAGE_HEADER_SIZE) as u16
    }
}

pub(crate) type BtreePage = BufferWithMetadata<BtreePageHeader>;

impl Page for BtreePage {
    type Header = BtreePageHeader;

    fn alloc(size: u32) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "page size {size} is not a value between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        );

        let mut buffer = BufferWithMetadata::<BtreePageHeader>::new_unchecked(
            size as usize,
            PAGE_ALIGNMENT as usize,
        );

        buffer.set_len(size as usize);
        *buffer.metadata_mut() = BtreePageHeader::alloc(size);

        buffer
    }

    fn page_number(&self) -> PageId {
        self.metadata().page_number
    }
}

impl BtreePage {
    pub(crate) fn max_allowed_payload_size(&self) -> u16 {
        max_payload_size_in(self.capacity()) as u16
    }

    pub(crate) fn overflow_threshold(page_size: usize) -> usize {
        Self::usable_space(page_size).saturating_mul(3).div_ceil(4) as usize
    }

    pub(crate) fn underflow_threshold(page_size: usize) -> usize {
        Self::usable_space(page_size).div_ceil(4) as usize
    }

    pub(crate) fn ideal_max_payload_size(page_size: usize, min_cells: usize) -> usize {
        debug_assert!(
            min_cells > 0,
            "if you're not gonna store any cells then why are you even calling this function?"
        );

        let ideal_size = max_payload_size_in(Self::usable_space(page_size) as usize / min_cells);

        debug_assert!(
            ideal_size > 0,
            "page size {page_size} is too small to store {min_cells} cells"
        );

        ideal_size
    }

    pub(crate) fn num_slots(&self) -> u16 {
        self.metadata().num_slots
    }

    pub(crate) fn max_slot_index(&self) -> Slot {
        Slot(self.metadata().num_slots)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.num_slots() == 0
    }

    fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.data.cast(), self.num_slots() as usize)
    }

    fn slot_array(&self) -> &[u16] {
        unsafe { self.slot_array_non_null().as_ref() }
    }

    fn slot_array_mut(&mut self) -> &mut [u16] {
        unsafe { self.slot_array_non_null().as_mut() }
    }

    unsafe fn cell_at_offset(&self, offset: u16) -> NonNull<Cell> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }

    /// Returns a pointer to the [`Cell`] located at the given slot.
    pub(crate) fn get_cell_at(&self, index: Slot) -> NonNull<Cell> {
        debug_assert!(
            index.0 <= self.num_slots(),
            "slot index {index} out of bounds for slot array of length {}",
            self.num_slots()
        );

        unsafe { self.cell_at_offset(self.slot_array()[usize::from(index)]) }
    }

    /// Read-only reference to a cell.
    pub(crate) fn cell(&self, index: Slot) -> &Cell {
        let cell = self.get_cell_at(index);
        // SAFETY: Same as [`Self::cell_at_offset`].
        unsafe { cell.as_ref() }
    }

    /// Mutable reference to a cell.
    pub(crate) fn cell_mut(&mut self, index: Slot) -> &mut Cell {
        let mut cell = self.get_cell_at(index);
        // SAFETY: Same as [`Self::cell_at_offset`].
        unsafe { cell.as_mut() }
    }

    /// Returns an owned cell by cloning it.
    fn owned_cell(&self, index: Slot) -> Cell {
        self.cell(index).clone()
    }

    /// Returns the child at the given `index`.
    pub(crate) fn child(&self, index: Slot) -> PageId {
        if index == self.num_slots() {
            self.metadata().right_child
        } else {
            self.cell(index).metadata().left_child()
        }
    }

    /// Iterates over all the children pointers in this page.
    pub(crate) fn iter_children(&self) -> impl DoubleEndedIterator<Item = PageId> + '_ {
        let len = if self.is_leaf() {
            0
        } else {
            self.num_slots() + 1
        };

        for i in 0..len {}
        (0..len).map(|i| self.child(Slot(i)))
    }

    pub(crate) fn iter_cells(&self) -> impl DoubleEndedIterator<Item = &Cell> + '_ {
        let len = self.num_slots();
        (0..len).map(|i| self.cell(Slot(i)))
    }

    /// Returns `true` if this page is underflow
    pub(crate) fn has_underflown(&self) -> bool {
        !self.can_release_space(Slot::SIZE)
    }

    /// Returns `true` if this page has overflown.
    /// We need at least 2 bytes for the last cell slot and 4 extra bytes to store the pointer to an overflow page
    pub(crate) fn has_overflown(&self) -> bool {
        !self.has_space_for(Slot::SIZE + CELL_HEADER_SIZE + std::mem::size_of::<PageId>())
    }

    pub(crate) fn has_space_for(&self, additional_space: usize) -> bool {
        let occupied_space = self
            .size()
            .saturating_sub(self.metadata().free_space as usize)
            + additional_space;
        let max_allowed = Self::overflow_threshold(self.size());
        occupied_space <= max_allowed
    }

    pub(crate) fn can_release_space(&self, removable_space: usize) -> bool {
        let occupied_space = self
            .size()
            .saturating_sub(self.metadata().free_space as usize)
            - removable_space;
        let min_allowed = Self::underflow_threshold(self.size());
        occupied_space >= min_allowed
    }
    pub(crate) fn is_leaf(&self) -> bool {
        !self.metadata().right_child.is_valid()
    }

    pub(crate) fn is_interior(&self) -> bool {
        self.metadata().right_child.is_valid()
    }

    /// Number of used bytes in this page.
    pub(crate) fn used_bytes(&self) -> u32 {
        self.capacity() as u32 - self.metadata().free_space
    }

    /// Number of free bytes in this page.
    pub(crate) fn effective_free_space(&self) -> u32 {
        self.metadata().free_space_ptr - self.metadata().content_start_ptr() as u32
    }

    /// Adds `cell` to this page, possibly overflowing the page.
    pub(crate) fn push(&mut self, cell: Cell) {
        self.insert(Slot(self.num_slots()), cell);
    }

    /// Attempts to insert the given `cell` in this page.
    pub(crate) fn insert(&mut self, index: Slot, cell: Cell) -> Slot {
        assert!(
            cell.data.len() <= self.max_allowed_payload_size() as usize,
            "attempt to store payload of size {} when max allowed payload size is {}",
            cell.data.len(),
            self.max_allowed_payload_size()
        );

        assert!(
            index <= Slot(self.num_slots()),
            "index {index} out of bounds for page of length {}",
            self.num_slots()
        );

        let cell_storage_size = cell.storage_size() as u32;
        let cell_total_size = cell.total_size() as u32;

        if cell_storage_size > self.metadata().free_space {
            panic!("Buffer overflow. Attempted to insert with overflow on a btreepage.")
        };

        // Space between the end of the slot array and the closest cell.
        let available_space = self.effective_free_space();

        // We can fit the new cell but we have to defragment the page first.
        if available_space < cell_storage_size {
            self.defragment();
        }

        let offset = self.metadata().free_space_ptr - cell.total_size() as u32;

        debug_assert!(
            (cell.total_size() as usize).is_multiple_of(CELL_ALIGNMENT as usize),
            "Cell is not aligned!"
        );
        debug_assert!(
            offset + cell.total_size() as u32 <= self.capacity() as u32,
            "BUFFER OVERFLOW, might overwrite the header of the next page: capacity: {}, cell offset: {}, cell size: {}.!",
            self.capacity(),
            offset,
            cell.total_size()
        );

        debug_assert!(
            (offset as usize).is_multiple_of(CELL_ALIGNMENT as usize),
            "Offset is not aligned! {offset}"
        );

        // Write new cell.
        // SAFETY: `last_used_offset` keeps track of where the last cell was
        // written. By substracting the total size of the new cell to
        // `last_used_offset` we get a valid pointer within the page where we
        // write the new cell.
        unsafe {
            let cell_ptr = self.cell_at_offset(offset.try_into().unwrap());
            cell_ptr.write(cell);
        };

        // Update header.
        self.metadata_mut().free_space_ptr -= cell_total_size;

        self.metadata_mut().free_space -= cell_storage_size;

        // Add new slot.
        self.metadata_mut().num_slots += 1;

        // If the index is not the last one, shift slots to the right.
        if index < self.max_slot_index() {
            let end = self.metadata().num_slots as usize - 1;
            self.slot_array_mut()
                .copy_within(usize::from(index)..end, usize::from(index) + 1);
        };

        // Set offset.
        self.slot_array_mut()[usize::from(index)] = offset.try_into().unwrap();

        index
    }

    /// Tries to replace the cell pointed by the given slot `index` with the
    /// `new_cell`.
    pub(crate) fn replace(&mut self, index: Slot, new_cell: Cell) -> Cell {
        let old_cell = self.cell(index);

        // There's no way we can fit the new cell in this page, even if we
        // remove the one that has to be replaced.
        if self.metadata().free_space as u16 + old_cell.total_size() < new_cell.total_size() {
            panic!("Attempted to insert a cell on a page that was already full!");
        }

        // Case 1: The new cell is smaller than the old cell. This is the best
        // case scenario because we can simply overwrite the datas without
        // doing much else.
        if new_cell.total_size() <= old_cell.total_size() {
            // If new_cell is smaller we gain some extra bytes.
            let free_bytes = (old_cell.total_size() - new_cell.total_size()) as u32;

            // Copy the old cell to return it.
            let owned_cell = self.owned_cell(index);

            // Overwrite the datas of the old cell.
            let old_cell = self.cell_mut(index);
            old_cell.data_mut()[..new_cell.data.len()].copy_from_slice(new_cell.data());
            *old_cell.metadata_mut() = *new_cell.metadata();
            old_cell.set_len(new_cell.len());

            self.metadata_mut().free_space_ptr += free_bytes;
            self.metadata_mut().free_space += free_bytes;

            return owned_cell;
        }

        // Case 2: The new cell fits in this page but we have to remove the old
        // one and potentially defragment the page. Worst case scenario.
        let old = self.remove(index);

        self.insert(index, new_cell);

        old
    }

    /// Removes the cell pointed by the given slot `index`.
    pub(crate) fn remove(&mut self, index: Slot) -> Cell {
        let len = self.num_slots();

        assert!(
            index < self.max_slot_index(),
            "index {index} out of range for length {len}"
        );

        let cell = self.owned_cell(index);

        // Remove the index as if we removed from a Vec.
        self.slot_array_mut()
            .copy_within(usize::from(index) + 1..len as usize, usize::from(index));

        // Add new free space.
        // Cannot put more free space here as it would cause a misalignment.
        // Must correct later through defragmentation.
        // Thanks [miri]
        // self.metadata_mut().free_space_ptr += cell.storage_size() as u32;
        self.metadata_mut().free_space += cell.storage_size() as u32;

        // Decrease length.
        self.metadata_mut().num_slots -= 1;

        cell
    }

    /// Slides cells towards the right to eliminate fragmentation.
    fn defragment(&mut self) {
        let mut offsets = BinaryHeap::from_iter(
            self.slot_array()
                .iter()
                .enumerate()
                .map(|(i, offset)| (*offset, i)),
        );

        let mut destination_offset = self.size() - BTREE_PAGE_HEADER_SIZE;

        while let Some((offset, i)) = offsets.pop() {
            // SAFETY: Calling [`Self::cell_at_offset`] is safe here because we
            // obtained all the offsets from the slot array, which should always
            // be in a valid state. If that holds true, then casting the
            // dereferencing the cell pointer should be safe as well.
            unsafe {
                let cell = self.cell_at_offset(offset);
                let size = cell.as_ref().total_size();

                destination_offset -= size as usize;

                cell.cast::<u8>().copy_to(
                    self.data.byte_add(destination_offset).cast::<u8>(),
                    size as usize,
                );
            }
            self.slot_array_mut()[i] = destination_offset as u16;
        }

        self.metadata_mut().free_space_ptr = destination_offset as u32;
    }

    /// Works like [`Vec::drain`] execept it doesn't remove elements unless
    /// consumed.
    pub(crate) fn drain(
        &mut self,
        range: impl RangeBounds<usize>,
    ) -> impl Iterator<Item = Cell> + '_ {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Excluded(i) => i + 1,
            Bound::Included(i) => *i,
        };

        let end = match range.end_bound() {
            Bound::Unbounded => self.num_slots() as usize,
            Bound::Excluded(i) => *i,
            Bound::Included(i) => i + 1,
        };

        let mut drain_index = start;
        let mut slot_index = start;

        iter::from_fn(move || {
            // Copy cells until we reach the end.
            if drain_index < end {
                let cell = self.owned_cell(Slot(slot_index as _));
                slot_index += 1;
                drain_index += 1;
                Some(cell)
            } else {
                // Now compute gained space and shift slots towards the left.
                self.metadata_mut().free_space += (start..slot_index)
                    .map(|slot| self.cell(Slot(slot as u16)).storage_size())
                    .sum::<u16>() as u32;

                self.slot_array_mut().copy_within(slot_index.., start);

                self.metadata_mut().num_slots -= (slot_index - start) as u16;

                None
            }
        })
    }
}

pub(crate) type OverflowPage = BufferWithMetadata<OverflowPageHeader>;
pub(crate) type PageZero = BufferWithMetadata<DatabaseHeader>;

impl Page for OverflowPage {
    type Header = OverflowPageHeader;
    fn alloc(size: u32) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "page size {size} is not a value between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        );
        let mut buf = BufferWithMetadata::<OverflowPageHeader>::new_unchecked(
            size as usize,
            PAGE_ALIGNMENT as usize,
        );

        // Pages cannot grow like cells do. Therefore we automatically set the length at the beginning and leave it as is.
        buf.set_len(size as usize);

        *buf.metadata_mut() = OverflowPageHeader::alloc(size);

        buf
    }

    fn page_number(&self) -> PageId {
        self.metadata().page_number
    }
}

impl OverflowPage {
    /// Returns a read-only reference to the payload (not the entire data).
    pub(crate) fn payload(&self) -> &[u8] {
        &self.data()[..self.metadata().num_bytes as usize]
    }
}

impl Page for PageZero {
    type Header = DatabaseHeader;
    /// Creates a new page in memory.
    fn alloc(size: u32) -> Self {
        let mut buf = BufferWithMetadata::<DatabaseHeader>::new_unchecked(
            size as usize,
            PAGE_ALIGNMENT as usize,
        );
        buf.set_len(DB_HEADER_SIZE);
        *buf.metadata_mut() = DatabaseHeader::alloc(size);

        buf
    }

    fn page_number(&self) -> PageId {
        PAGE_ZERO
    }
}

/// Serves as a wrapper to hold multiple types of pages.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum MemPage {
    Zero(PageZero),
    Overflow(OverflowPage),
    Btree(BtreePage),
}

pub(crate) trait Page: Into<MemPage> + AsRef<[u8]> + AsMut<[u8]>
where
    Self: for<'a> TryFrom<(&'a [u8], usize), Error = &'static str>,
{
    type Header: Header;

    fn alloc(size: u32) -> Self;
    fn page_number(&self) -> PageId;
}

pub(crate) trait Header: AsRef<[u8]> {
    fn page_number(&self) -> PageId;
    fn init(size: u32, page_number: PageId) -> Self;
    fn alloc(size: u32) -> Self;
}

impl MemPage {
    pub(crate) fn page_number(&self) -> PageId {
        match self {
            Self::Btree(p) => p.page_number(),
            Self::Overflow(p) => p.page_number(),
            Self::Zero(p) => PAGE_ZERO,
        }
    }

    /// Converts this page into another type.
    pub(crate) fn reinit_as<P>(&mut self)
    where
        P: Page,
        BufferWithMetadata<P::Header>: Into<MemPage>,
    {
        let number = self.page_number();
        // SAFETY: We basically move out of self temporarily to create the new
        // type and then write the new variant back. If we already have mutable
        // access to self this should be "safe". At this point I already hate
        // the word "safe".
        unsafe {
            let mem_page = ptr::from_mut(self);

            let mut converted: BufferWithMetadata<_> = match mem_page.read() {
                Self::Zero(zero) => panic!(
                    "Attempted to reinitialize page zero. This is not valid. Page Zero is a reserved page to store the database header!"
                ),
                Self::Overflow(overflow) => overflow.cast(),
                Self::Btree(page) => page.cast(),
            };

            let size = converted.size();

            let header = P::Header::init(size as u32, number);
            *converted.metadata_mut() = header;
            converted.set_len(size);
            converted.data_mut().fill(0);
            mem_page.write(converted.into())
        }
    }

    // Free pages have the same header as overflow pages.
    // We are reusing the same header type to reduce the boiler plate.
    // It is the responsability of the database header to distinguish between pages that are used for the free list and pages that are actual overflow pages.
    pub(crate) fn dealloc(&mut self) {
        self.reinit_as::<OverflowPage>();
    }

    pub(crate) fn is_free_page(&self) -> bool {
        matches!(self, MemPage::Overflow(_))
    }

    /// Returns `true` if the page is in overflow state.
    pub(crate) fn has_overflown(&self) -> bool {
        match self {
            Self::Btree(page) => page.has_overflown(),
            _ => false,
        }
    }
}

impl AsRef<[u8]> for MemPage {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Zero(page) => page.as_ref(),
            Self::Overflow(page) => page.as_ref(),
            Self::Btree(page) => page.as_ref(),
        }
    }
}

impl AsMut<[u8]> for MemPage {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Zero(page) => page.as_mut(),
            Self::Overflow(page) => page.as_mut(),
            Self::Btree(page) => page.as_mut(),
        }
    }
}

impl From<BtreePage> for MemPage {
    fn from(page: BtreePage) -> MemPage {
        MemPage::Btree(page)
    }
}

impl From<OverflowPage> for MemPage {
    fn from(page: OverflowPage) -> MemPage {
        MemPage::Overflow(page)
    }
}

impl From<PageZero> for MemPage {
    fn from(page: PageZero) -> MemPage {
        MemPage::Zero(page)
    }
}

impl<'p> TryFrom<&'p Latch<MemPage>> for &'p BtreePage {
    type Error = IoError;

    fn try_from(latch: &'p Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref() {
            MemPage::Btree(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into BtreePage"),
            )),
        }
    }
}

impl<'p> TryFrom<&'p mut Latch<MemPage>> for &'p mut BtreePage {
    type Error = IoError;

    fn try_from(latch: &'p mut Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref_mut() {
            MemPage::Btree(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into BtreePage"),
            )),
        }
    }
}

impl<'p> TryFrom<&'p Latch<MemPage>> for &'p PageZero {
    type Error = IoError;

    fn try_from(latch: &'p Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref() {
            MemPage::Zero(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into PageZero"),
            )),
        }
    }
}

impl<'p> TryFrom<&'p mut Latch<MemPage>> for &'p mut PageZero {
    type Error = IoError;

    fn try_from(latch: &'p mut Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref_mut() {
            MemPage::Zero(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into PageZero"),
            )),
        }
    }
}

impl<'p> TryFrom<&'p Latch<MemPage>> for &'p OverflowPage {
    type Error = IoError;

    fn try_from(latch: &'p Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref() {
            MemPage::Overflow(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into OverflowPage"),
            )),
        }
    }
}

impl<'p> TryFrom<&'p mut Latch<MemPage>> for &'p mut OverflowPage {
    type Error = IoError;

    fn try_from(latch: &'p mut Latch<MemPage>) -> Result<Self, Self::Error> {
        match latch.deref_mut() {
            MemPage::Overflow(page) => Ok(page),
            other => Err(IoError::new(
                IoErrorKind::InvalidData,
                format!("attempt to convert {other:?} into OverflowPage"),
            )),
        }
    }
}

unsafe impl Send for MemPage {}
unsafe impl Sync for MemPage {}

#[cfg(test)]
crate::dynamic_buffer_tests!(
    page_dyn,
    BtreePageHeader,
    size = DEFAULT_PAGE_SIZE as usize,
    align = PAGE_ALIGNMENT as usize
);

#[cfg(test)]
crate::dynamic_buffer_tests!(
    page_dyn_large,
    BtreePageHeader,
    size = MAX_PAGE_SIZE as usize, // Test for the largest possible page size.
    align = PAGE_ALIGNMENT as usize
);

#[cfg(test)]
crate::dynamic_buffer_tests!(
    ovf_page_dyn,
    OverflowPageHeader,
    size = DEFAULT_PAGE_SIZE as usize,
    align = PAGE_ALIGNMENT as usize
);
#[cfg(test)]
crate::static_buffer_tests!(
    page_sta,
    BtreePageHeader,
    size = DEFAULT_PAGE_SIZE as usize,
    align = PAGE_ALIGNMENT as usize
);
#[cfg(test)]
crate::static_buffer_tests!(
    ovf_page_sta,
    OverflowPageHeader,
    size = DEFAULT_PAGE_SIZE as usize,
    align = PAGE_ALIGNMENT as usize
);
#[cfg(test)]
crate::static_buffer_tests!(
    page_zero,
    DatabaseHeader,
    size = DB_HEADER_SIZE,
    align = PAGE_ALIGNMENT as usize
);

/// Specific tests for BtreePages.
#[cfg(test)]
mod btree_page_tests {
    use super::*;
    use crate::storage::cell::{Cell, Slot};
    use crate::types::PageId;

    #[test]
    fn test_page_allocation() {
        let mut page = BtreePage::alloc(MIN_PAGE_SIZE + 1);
        assert_eq!(page.len() as u32, MIN_PAGE_SIZE + 1);
        // Verify initial state
        assert_eq!(page.num_slots(), 0);
        assert_eq!(
            page.capacity(),
            MIN_PAGE_SIZE as usize + 1 - BTREE_PAGE_HEADER_SIZE
        );
        assert!(page.is_leaf());
        assert!(page.metadata().page_number != PAGE_ZERO);
        assert!(!page.is_interior());
        assert!(!page.has_overflown());
        assert!(page.has_underflown());
        assert_eq!(
            page.metadata().padding,
            PAGE_ALIGNMENT * 2 - (MIN_PAGE_SIZE + 1)
        );

        // Check free space calculation
        let expected_free_space = MIN_PAGE_SIZE + 1 - BTREE_PAGE_HEADER_SIZE as u32;

        assert_eq!(page.metadata().free_space, expected_free_space);
        assert_eq!(page.metadata().free_space_ptr, expected_free_space);

        let header = page.metadata_mut();

        // Test siblings
        header.next_sibling = PageId::from(100);
        header.previous_sibling = PageId::from(50);
        assert_eq!(header.next_sibling, PageId::from(100));
        assert_eq!(header.previous_sibling, PageId::from(50));

        // Test content start pointer
        assert_eq!(header.content_start_ptr(), BTREE_PAGE_HEADER_SIZE as u16);
    }

    #[test]
    fn test_cell_ops() {
        let mut page = BtreePage::alloc(MIN_PAGE_SIZE);

        // Test push
        let cell1 = Cell::new(b"first");

        let cell2 = Cell::new(b"second");

        assert!(cell1.len() == b"first".len());
        page.push(cell1);

        page.push(cell2);

        assert_eq!(page.num_slots(), 2);
        assert_eq!(page.cell(Slot(0)).used(), b"first");
        assert_eq!(page.cell(Slot(1)).used(), b"second");

        // Test insert at specific position
        let cell3 = Cell::new(b"middle");
        page.insert(Slot(1), cell3.clone());

        assert_eq!(page.num_slots(), 3);
        assert_eq!(page.cell(Slot(1)).used(), b"middle");
        assert_eq!(page.cell(Slot(2)).used(), b"second");

        // Test remove
        let removed = page.remove(Slot(1));
        assert_eq!(removed.used(), b"middle");
        assert_eq!(page.num_slots(), 2);

        // Add initial cell
        page.push(Cell::new(b"original"));

        // Replace with smaller cell.
        // Case 1: Requires just overwriting the bytes f the old cell with the bytes if the new one.
        // Also needs to set the new length to properly manage the padding.
        let smaller = Cell::new(b"new");

        let old = page.replace(Slot(2), smaller);
        assert_eq!(old.used(), b"original");
        assert_eq!(page.cell(Slot(2)).used(), b"new");

        // Replace with larger cell
        // Case 2: Requires removal of the old cell in order to make up space for the new one.
        let larger = Cell::new(b"much longer replacement text");
        page.replace(Slot(1), larger);
        assert_eq!(page.cell(Slot(1)).used(), b"much longer replacement text");

        // The other cells should still be valid.
        assert_eq!(page.cell(Slot(0)).used(), b"first");
        assert_eq!(page.cell(Slot(2)).used(), b"new");
    }

    #[test]
    fn test_free_space() {
        let mut page = BtreePage::alloc(MIN_PAGE_SIZE);
        let initial_free = page.metadata().free_space;

        // Add cell and check free space decreases
        let cell = Cell::new(b"test data");
        let storage_size = cell.storage_size();
        page.push(cell);

        assert_eq!(
            page.metadata().free_space,
            initial_free - storage_size as u32
        );

        // Remove cell and check free space increases
        page.remove(Slot(0));
        assert_eq!(page.metadata().free_space, initial_free);
    }

    #[test]
    fn test_defragmentation() {
        let mut page = BtreePage::alloc(4096);

        // Add multiple cells
        for i in 0..10 {
            page.push(Cell::new(format!("cell {i}").as_bytes()));
        }

        // Remove some cells to create fragmentation
        page.remove(Slot(2));
        page.remove(Slot(4));
        page.remove(Slot(6));

        let free_space = page.metadata().free_space;
        let effective = page.effective_free_space();

        // After removing cells, effective free space might be less than total free space
        // due to fragmentation

        // Try to insert a cell that requires defragmentation
        let large_cell = Cell::new(&[0u8; 100]);
        if large_cell.storage_size() <= free_space as u16
            && large_cell.storage_size() > effective as u16
        {
            // This should trigger defragmentation internally
            page.insert(Slot(0), large_cell);

            // After defragmentation, the cell should be inserted successfully
            assert!(page.num_slots() > 0);
        }
    }

    crate::test_page_casting!(
        test_btree_overflow,
        Btree,
        BtreePage,
        Overflow,
        OverflowPage
    );
    crate::test_page_casting!(
        test_overflow_btree,
        Overflow,
        OverflowPage,
        Btree,
        BtreePage
    );
}
