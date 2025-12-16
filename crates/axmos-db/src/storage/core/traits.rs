use crate::{
    BlockId, CELL_ALIGNMENT, LogId,
    storage::{
        cell::{CELL_HEADER_SIZE, CellHeader, CellMut, CellRef, OwnedCell, Slot},
        wal::{OwnedRecord, RecordHeader, RecordRef},
    },
    types::PageId,
};

use std::{
    collections::BinaryHeap,
    io::{self, Error as IoError, ErrorKind},
    iter, mem,
    ops::{Bound, RangeBounds},
    ptr::NonNull,
};

pub(crate) trait Identifiable {
    type IdType;

    fn id(&self) -> Self::IdType;
}

/// Trait for defining frames that are allocatable by the pager.
/// Most structures are going to need to be allocated in some way, and header initialization must be done separately.
pub(crate) trait Buffer {
    /// Header type
    type Header: AsRef<[u8]> + Copy + Identifiable;

    /// Size of this buffer's header.
    const HEADER_SIZE: usize = mem::size_of::<Self::Header>();

    /// Id of the buffer.
    fn id(&self) -> <Self::Header as Identifiable>::IdType {
        self.header().id()
    }

    /// Get the header of the buffer.
    fn header(&self) -> &Self::Header;

    /// Get a mutable reference to the header of the buffer
    fn header_mut(&mut self) -> &mut Self::Header;

    fn data_non_null(&self) -> NonNull<[u8]>;

    /// Get the data in the buffer
    fn data(&self) -> &[u8];

    /// Get a mutable reference to the data in the buffer
    fn data_mut(&mut self) -> &mut [u8];

    /// Compute the total capacity in the buffer to add more DataItems.
    fn capacity(&self) -> usize;

    /// Compute the total usable space in the buffer
    fn usable_space(&self, size: usize) -> usize;
}

pub(crate) trait BufferOps: Buffer {
    /// Current available space in the buffer.
    fn available_space(&self) -> usize;

    /// Allocate the buffer.
    fn alloc(size: u32) -> Self;
}

pub(crate) trait BtreeMetadata:
    ComposedMetadata<ItemHeader = CellHeader> + Identifiable<IdType = PageId>
{
    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16;

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> u16;

    /// Get the free space in the page.
    fn free_space(&self) -> u32;

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32;

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId>;

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32);

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32);

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32);

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32);

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32);

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32);

    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: u16);

    /// Add slots to the page
    fn add_slots(&mut self, num: u16);

    /// Remove slots from the page
    fn remove_slots(&mut self, num: u16);

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId>;

    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: PageId);

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId>;

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, prev: PageId);
}

pub(crate) trait WalMetadata:
    ComposedMetadata<ItemHeader = RecordHeader> + Identifiable<IdType = BlockId>
{
    /// Setters for start and last lsn
    fn set_last_lsn(&mut self, lsn: LogId);

    fn set_start_lsn(&mut self, lsn: LogId);

    /// Getter for last and start lsn
    fn last_lsn(&self) -> Option<LogId>;

    fn start_lsn(&self) -> Option<LogId>;
    /// Increments the write offset of this block.
    fn increment_write_offset(&mut self, added_bytes: usize);

    fn total_used_bytes(&self) -> usize;
}

/// Trait to represent a memory array that is composed of other inner entries.
pub(crate) trait ComposedBuffer: Buffer + BufferOps {
    type DataItemHeader;

    /// Writes the item to the specific offset.
    unsafe fn write_item_to_offset<'a>(
        &self,
        offset: u64,
        item: impl Writable<Header = Self::DataItemHeader>,
    );

    /// Get a cell at a given offset in the page.
    unsafe fn item_at_offset<'a>(&self, offset: u64) -> NonNull<Self::DataItemHeader>;
}

/// Trait for defining operations that are applicable only to wal buffers.
pub(crate) trait WalBuffer:
    ComposedBuffer<DataItemHeader = RecordHeader> + Buffer<Header: WalMetadata>
{
    /// Reconstruct a record view at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    fn record(&self, offset: u64) -> RecordRef<'_> {
        unsafe { RecordRef::from_raw(self.item_at_offset(offset)) }
    }

    /// Reconstruct an owned record at a given offset in the block
    ///
    /// # SAFETY
    ///
    /// See [WalBlock::record_at_offset]
    fn record_owned(&self, offset: u64) -> OwnedRecord {
        unsafe { OwnedRecord::from_ref(RecordRef::from_raw(self.item_at_offset(offset))) }
    }

    /// Get a mutable reference to the write offset in the block
    fn current_write_offset(&mut self) -> &mut [u8] {
        let offset = self.header().total_used_bytes();
        &mut self.data_mut()[offset..]
    }

    /// Append a record to the block (no checkings made)
    fn append_record_uncheked<R: Writable<Header = RecordHeader>>(&mut self, item: R) {
        let dest = self.current_write_offset();
        item.write_to(dest);
        self.header_mut().increment_write_offset(item.total_size());
    }

    // Attempts to push a record at the end of the block.
    // If the record does not fit, returns an error.
    fn try_push<R: Writable<Header = RecordHeader>>(
        &mut self,
        lsn: LogId,
        record: R,
    ) -> io::Result<LogId> {
        let record_size = record.total_size();

        if self.available_space() < record_size {
            return Err(IoError::new(
                ErrorKind::StorageFull,
                "Data too large for this block.",
            ));
        };

        self.append_record_uncheked(record);

        if self.header().start_lsn().is_none() {
            self.header_mut().set_start_lsn(lsn);
        };
        self.header_mut().set_last_lsn(lsn);

        Ok(lsn)
    }
}

/// Trait for defining items that can be written to buffers.
/// Returns the bytes written.
pub(crate) trait Writable {
    /// Header of the item. It is necessary because we need it in order to be able to recornstruct the bytes from a non null pointer to the header.
    type Header: Copy;

    fn total_size(&self) -> usize;

    /// Write the item to a byte slice.
    fn write_to(&self, buffer: &mut [u8]) -> usize;
}

/// Trait for defining operations applicable to pages that are used on btree operations.
/// All btree pages are composed of cells.
pub(crate) trait BtreeBuffer:
    ComposedBuffer<DataItemHeader = CellHeader> + Buffer<Header: BtreeMetadata>
{
    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16 {
        self.header().content_start_ptr()
    }

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> u16 {
        self.header().num_slots()
    }

    /// Get a valid pointer to the start of the slot array (after the header)
    fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.data_non_null().cast(), self.num_slots() as usize)
    }

    /// Get the free space in the page.
    fn free_space(&self) -> u32 {
        self.header().free_space()
    }

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32 {
        self.header().free_space_pointer()
    }

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId> {
        self.header().right_child()
    }

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32) {
        self.header_mut().add_free_space(space);
    }

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32) {
        self.header_mut().remove_free_space(space);
    }

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32) {
        self.header_mut().free_space_pointer_up(bytes);
    }

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32) {
        self.header_mut().free_space_pointer_down(bytes);
    }

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32) {
        self.header_mut().reset_free_space_pointer(offset);
    }

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32) {
        self.header_mut().reset_free_space(bytes);
    }

    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: u16) {
        self.header_mut().reset_num_slots(num);
    }

    /// Add slots to the page
    fn add_slots(&mut self, num: u16) {
        self.header_mut().add_slots(num);
    }

    /// Remove slots from the page
    fn remove_slots(&mut self, num: u16) {
        self.header_mut().remove_slots(num);
    }

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId> {
        self.header().next_sibling()
    }

    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: PageId) {
        self.header_mut().set_next_sibling(next);
    }

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId> {
        self.header().prev_sibling()
    }

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, prev: PageId) {
        self.header_mut().set_prev_sibling(prev);
    }

    /// Compute the maximum payload size that can fit in a single page (given the usable space on that page).
    /// Must account for the header size and the slot size (total storage size in the page), and align down the value.
    fn max_payload_size_in(usable_space: usize) -> usize {
        (usable_space - CELL_HEADER_SIZE - Slot::SIZE) & !(CELL_ALIGNMENT as usize - 1)
    }

    /// Max allowed payload size for a cell to be inserted on this [Page]
    fn max_allowed_payload_size(&self) -> u16 {
        Self::max_payload_size_in(self.capacity()) as u16
    }

    /// Returns a pointer to the [OwnedCell] located at the given [Slot].
    fn get_cell_at(&self, index: Slot) -> NonNull<CellHeader> {
        debug_assert!(
            index.0 <= self.num_slots(),
            "slot index {index} out of bounds for slot array of length {}",
            self.num_slots()
        );

        unsafe { self.item_at_offset(self.slot_array()[usize::from(index)] as u64) }
    }

    /// Threshold in the page to consider it as overflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn overflow_threshold(&self, page_size: usize) -> usize {
        self.usable_space(page_size).saturating_mul(3).div_ceil(4)
    }

    /// Threshold in the page to consider it as underflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn underflow_threshold(&self, page_size: usize) -> usize {
        self.usable_space(page_size).div_ceil(4)
    }

    /// Ideal payload size for this page.
    /// Useful when allocating an overflow chain and we have to decide how much data we will store on the first page.
    fn ideal_max_payload_size(&self, page_size: usize, min_cells: usize) -> usize {
        debug_assert!(
            min_cells > 0,
            "if you're not gonna store any cells then why are you even calling this function?"
        );

        let ideal_size =
            Self::max_payload_size_in(self.usable_space(page_size) as usize / min_cells);

        debug_assert!(
            ideal_size > 0,
            "page size {page_size} is too small to store {min_cells} cells"
        );

        ideal_size
    }

    /// Returns the highest slot number in this page.
    fn max_slot_index(&self) -> Slot {
        Slot(self.num_slots())
    }

    /// Number of used bytes in this page.
    fn used_bytes(&self) -> usize {
        self.capacity() - self.free_space() as usize
    }

    /// Number of free bytes in this page.
    fn effective_free_space(&self) -> u32 {
        self.free_space_pointer() - self.content_start_ptr() as u32
    }

    /// Returns the child at the given [Slot].
    /// Interior pages only
    fn child(&self, index: Slot) -> Option<PageId> {
        if index == self.num_slots() {
            self.right_child()
        } else {
            self.cell(index).left_child()
        }
    }

    /// Check whether this is a leaf btree page.
    fn is_leaf(&self) -> bool {
        !self.right_child().is_some()
    }
    /// Check whether this is an interior btree page.
    fn is_interior(&self) -> bool {
        !self.is_leaf()
    }

    /// Returns `true` if this page is underflow
    fn has_underflown(&self) -> bool {
        !self.can_release_space(Slot::SIZE)
    }

    /// Returns `true` if this page has overflown.
    /// We need at least 2 bytes for the last cell slot and 4 extra bytes to store the pointer to an overflow page
    fn has_overflown(&self) -> bool {
        !self.has_space_for(Slot::SIZE + CELL_HEADER_SIZE + mem::size_of::<PageId>())
    }

    /// Read-only reference to a cell.
    fn cell(&self, index: Slot) -> CellRef<'_> {
        unsafe { CellRef::from_raw(self.get_cell_at(index)) }
    }

    /// Mutable reference to a cell.
    fn cell_mut(&mut self, index: Slot) -> CellMut<'_> {
        unsafe { CellMut::from_raw(self.get_cell_at(index)) }
    }

    /// Returns an owned cell by cloning it.
    fn owned_cell(&self, index: Slot) -> OwnedCell {
        OwnedCell::from_ref(self.cell(index))
    }

    fn is_empty(&self) -> bool {
        self.num_slots() == 0
    }

    /// Iterates over all the children pointers in this page.
    fn iter_children(&self) -> impl DoubleEndedIterator<Item = PageId> + '_ {
        let len = if self.is_leaf() {
            0
        } else {
            self.num_slots() + 1
        };
        (0..len).filter_map(move |i| self.child(Slot(i)))
    }
    /// Iterates over all the cells in this page.
    fn iter_cells(&self) -> impl DoubleEndedIterator<Item = CellRef<'_>> + '_ {
        let len = self.num_slots();
        (0..len).map(|i| self.cell(Slot(i)))
    }

    /// Check whether the page will overflow if we append the additional requested size.
    fn has_space_for(&self, additional_space: usize) -> bool {
        let occupied_space = self.used_bytes() + additional_space;
        let max_allowed = self.overflow_threshold(self.capacity());
        occupied_space <= max_allowed
    }

    /// Check whether the page will underflow if we remove the requested size.
    fn can_release_space(&self, removable_space: usize) -> bool {
        let occupied_space = self.used_bytes() - removable_space;
        let min_allowed = self.underflow_threshold(self.capacity());
        occupied_space >= min_allowed
    }

    /// Reference to the slot array
    fn slot_array(&self) -> &[u16] {
        unsafe { self.slot_array_non_null().as_ref() }
    }

    /// Mutable reference to the slot array
    fn slot_array_mut(&mut self) -> &mut [u16] {
        unsafe { self.slot_array_non_null().as_mut() }
    }

    /// Adds [OwnedCell] to the end of this page.
    fn push(&mut self, cell: OwnedCell) -> io::Result<()> {
        self.insert(Slot(self.num_slots()), cell)?;
        Ok(())
    }

    /// Attempts to insert the given  [OwnedCell] in this page.
    ///
    /// Fails if the computed offset is not aligned to the required [CELL_ALIGNMENT]
    /// or if the cell does not fit in the page.
    fn insert(&mut self, index: Slot, cell: OwnedCell) -> io::Result<Slot> {
        if cell.data().len() > self.max_allowed_payload_size() as usize {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "attempt to store payload of size {} when max allowed payload size is {}",
                    cell.data().len(),
                    self.max_allowed_payload_size()
                ),
            ));
        };

        if index > Slot(self.num_slots()) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "index {index} out of bounds for page of length {}",
                    self.num_slots()
                ),
            ));
        };

        let cell_storage_size = cell.storage_size() as u32;
        let cell_total_size = cell.total_size() as u32;

        // Space between the end of the slot array and the closest cell.
        let available_space = self.effective_free_space();

        // We can fit the new cell but we have to defragment the page first.
        if available_space < cell_storage_size {
            self.defragment();
        }

        let offset = self.free_space_pointer() - cell.total_size() as u32;

        if !(cell_total_size as usize).is_multiple_of(CELL_ALIGNMENT as usize) {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "attempted to insert with an unaligned cell size.",
            ));
        };

        if (offset + cell_total_size as u32 > self.capacity() as u32)
            || cell_storage_size > self.free_space()
        {
            return Err(IoError::new(
                ErrorKind::StorageFull,
                "Buffer overflow. Attempted to insert with overflow on a btreepage.",
            ));
        };

        // Store the cell
        unsafe { self.write_item_to_offset(offset as u64, cell.as_cell_ref()) };

        // Update header.
        self.free_space_pointer_up(cell_total_size);
        self.remove_free_space(cell_storage_size);

        // Add new slot.
        self.add_slots(1);

        // If the index is not the last one, shift slots to the right.
        if index < self.max_slot_index() {
            let end = self.num_slots() as usize - 1;
            self.slot_array_mut()
                .copy_within(usize::from(index)..end, usize::from(index) + 1);
        };

        // Set offset.
        self.slot_array_mut()[usize::from(index)] = offset.try_into().unwrap();

        Ok(index)
    }

    /// Tries to replace the cell pointed by the given slot `index` with the
    /// `new_cell`.
    fn replace(&mut self, index: Slot, new_cell: OwnedCell) -> io::Result<OwnedCell> {
        let old_cell = self.cell(index);

        // There's no way we can fit the new cell in this page, even if we
        // remove the one that has to be replaced.
        if self.free_space() as usize + old_cell.total_size() < new_cell.total_size() {
            return Err(IoError::new(
                ErrorKind::StorageFull,
                "Buffer overflow. Attempted to insert with overflow on a btreepage.",
            ));
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
            let mut old_cell = self.cell_mut(index);
            old_cell.data_mut()[..new_cell.data().len()].copy_from_slice(new_cell.data());
            *old_cell.metadata_mut() = *new_cell.metadata();

            self.free_space_pointer_down(free_bytes);
            self.add_free_space(free_bytes);

            return Ok(owned_cell);
        };

        // Case 2: The new cell fits in this page but we have to remove the old
        // one and potentially defragment the page. Worst case scenario.
        let old = self.remove(index)?;

        self.insert(index, new_cell)?;

        Ok(old)
    }

    /// Removes the [OwnedCell] pointed by the given [Slot]
    fn remove(&mut self, index: Slot) -> io::Result<OwnedCell> {
        let len = self.num_slots();

        if index > Slot(self.num_slots()) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "index {index} out of bounds for page of length {}",
                    self.num_slots()
                ),
            ));
        };

        let cell = self.owned_cell(index);

        // Remove the index as if we removed from a Vec.
        self.slot_array_mut()
            .copy_within(usize::from(index) + 1..len as usize, usize::from(index));

        // Add new free space.
        // Cannot put more free space here as it would cause a misalignment.
        // Must correct later through defragmentation.
        // Thanks [miri]
        self.add_free_space(cell.storage_size() as u32);

        // Decrease length.
        self.remove_slots(1);

        Ok(cell)
    }

    /// Slides cells towards the right to eliminate fragmentation.
    fn defragment(&mut self) {
        let mut offsets = BinaryHeap::from_iter(
            self.slot_array()
                .iter()
                .enumerate()
                .map(|(i, offset)| (*offset, i)),
        );

        let mut destination_offset = self.capacity();

        while let Some((offset, i)) = offsets.pop() {
            // SAFETY: Calling [`Self::cell_at_offset`] is safe here because we
            // obtained all the offsets from the slot array, which should always
            // be in a valid state. If that holds true, then casting the
            // dereferencing the cell pointer should be safe as well.
            unsafe {
                let cell = self.item_at_offset(offset as u64);
                let cell_ref = CellRef::from_raw(cell);
                let size = cell_ref.total_size();
                destination_offset -= size as usize;
                self.write_item_to_offset(destination_offset as u64, cell_ref);
            }
            self.slot_array_mut()[i] = destination_offset as u16;
        }

        self.reset_free_space_pointer(destination_offset as u32);
    }

    /// Works like [`Vec::drain`] execept it doesn't remove elements unless
    /// consumed.
    fn drain(&mut self, range: impl RangeBounds<usize>) -> impl Iterator<Item = OwnedCell> + '_ {
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
                self.add_free_space(
                    (start..slot_index)
                        .map(|slot| self.cell(Slot(slot as u16)).storage_size())
                        .sum::<usize>() as u32,
                );

                self.slot_array_mut().copy_within(slot_index.., start);

                self.remove_slots((slot_index - start) as u16);

                None
            }
        })
    }
}

pub(crate) trait ComposedMetadata {
    type ItemHeader;
}

pub(crate) trait WritableLayout {
    type Header: AsRef<[u8]> + Copy;

    fn header(&self) -> &Self::Header;
    fn data(&self) -> &[u8];
}

/// Automates the implementation of writable types, for all types that implement Writable Layout.
/// For example (cells and records):
///
/// writable_layout!(OwnedCell, CellHeader, header, payload)
impl<T: WritableLayout> Writable for T {
    type Header = T::Header;

    fn total_size(&self) -> usize {
        mem::size_of::<Self::Header>() + self.data().len()
    }

    fn write_to(&self, buffer: &mut [u8]) -> usize {
        let header_size = mem::size_of::<Self::Header>();
        buffer[..header_size].copy_from_slice(self.header().as_ref());
        buffer[header_size..self.total_size()].copy_from_slice(self.data());
        self.total_size()
    }
}

/// Macro to automate the implementation of writable layout types.
#[macro_export]
macro_rules! writable_layout {
    ($ty:ty, $header:ty, $header_field:ident, $data_field:ident) => {
        impl $crate::storage::core::traits::WritableLayout for $ty {
            type Header = $header;
            fn header(&self) -> &Self::Header {
                &self.$header_field
            }
            fn data(&self) -> &[u8] {
                &self.$data_field
            }
        }
    };
    ($ty:ty, $header:ty, $header_field:ident, $data_field:ident, lifetime) => {
        impl $crate::storage::core::traits::WritableLayout for $ty {
            type Header = $header;
            fn header(&self) -> &Self::Header {
                self.$header_field
            }
            fn data(&self) -> &[u8] {
                self.$data_field
            }
        }
    };
}
