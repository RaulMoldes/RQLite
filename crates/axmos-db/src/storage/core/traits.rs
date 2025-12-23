use crate::{
    BlockId, Lsn,
    storage::{
        cell::{CellHeader, CellMut, CellRef, OwnedCell},
        wal::{OwnedRecord, RecordHeader, RecordRef},
    },
    types::PageId,
};

use std::{io, mem, ops::RangeBounds, ptr::NonNull};

pub(crate) trait Identifiable {
    type IdType: Copy;
    fn id(&self) -> Self::IdType;
}

pub(crate) trait Allocatable: Identifiable {
    const MAX_SIZE: usize;
    const MIN_SIZE: usize;

    fn alloc(id: Self::IdType, size: usize) -> Self;
}

/// Trait for defining frames that are allocatable by the pager.
/// Most structures are going to need to be allocated in some way, and header initialization must be done separately.
pub(crate) trait Buffer: Identifiable + Allocatable {
    type Header: Identifiable<IdType = Self::IdType> + Allocatable;
    const HEADER_SIZE: usize;
}

pub(crate) trait AvailableSpace: Buffer {
    /// Current available space in the buffer.
    fn available_space(&self) -> usize;
}

pub(crate) trait BtreeMetadata:
    ComposedMetadata<ItemHeader = CellHeader> + Identifiable<IdType = PageId> + Allocatable
{
    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16;

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> usize;

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
    fn reset_num_slots(&mut self, num: usize);

    /// Add slots to the page
    fn add_slots(&mut self, num: usize);

    /// Remove slots from the page
    fn remove_slots(&mut self, num: usize);

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId>;

    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: Option<PageId>);

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId>;

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, prev: Option<PageId>);

    /// Setter for the right child value.
    fn set_right_child(&mut self, new_right: Option<PageId>);
}

pub(crate) trait WalMetadata:
    ComposedMetadata<ItemHeader = RecordHeader> + Identifiable<IdType = BlockId> + Allocatable
{
    /// Setters for last lsn
    fn set_last_lsn(&mut self, lsn: Lsn);

    /// Setters for start lsn
    fn set_start_lsn(&mut self, lsn: Lsn);

    /// Getter for last and start lsn
    fn last_lsn(&self) -> Option<Lsn>;

    /// Getter for last and start lsn
    fn start_lsn(&self) -> Option<Lsn>;

    /// Increments the write offset of this block.
    fn increment_write_offset(&mut self, added_bytes: usize);

    /// Returns the total used bytes in this block.
    fn used_bytes(&self) -> usize;
}

/// Trait to represent a memory array that is composed of other inner entries.
pub(crate) trait ComposedBuffer: Buffer {
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
pub(crate) trait WalOps:
    ComposedBuffer<DataItemHeader = RecordHeader> + WalMetadata
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
    fn current_write_offset(&mut self) -> &mut [u8];

    /// Append a record to the block (no checkings made)
    fn append_record_uncheked<R: Writable<Header = RecordHeader>>(&mut self, item: R)
    where
        Self: AvailableSpace;

    // Attempts to push a record at the end of the block.
    // If the record does not fit, returns an error.
    fn try_push<R: Writable<Header = RecordHeader>>(
        &mut self,
        lsn: Lsn,
        record: R,
    ) -> io::Result<Lsn>
    where
        Self: AvailableSpace;
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
pub(crate) trait BtreeOps:
    ComposedBuffer<DataItemHeader = CellHeader> + BtreeMetadata
{
    /// Get a non null pointer to the slot array
    fn slot_array_non_null(&self) -> NonNull<[u16]>;
    /// Compute the maximum payload size that can fit in a single page (given the usable space on that page).
    /// Must account for the header size and the slot size (total storage size in the page), and align down the value.
    fn max_payload_size_in(usable_space: usize) -> usize;

    /// Max allowed payload size for a cell to be inserted on this [Page]
    fn max_allowed_payload_size(&self) -> u16;

    /// Returns a pointer to the [OwnedCell] located at the given [index].
    fn get_cell_at(&self, index: usize) -> NonNull<CellHeader>;

    /// Threshold in the page to consider it as overflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn overflow_threshold(page_size: usize) -> usize;
    /// Threshold in the page to consider it as underflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn underflow_threshold(page_size: usize) -> usize;

    /// Ideal payload size for this page.
    /// Useful when allocating an overflow chain and we have to decide how much data we will store on the first page.
    fn ideal_max_payload_size(page_size: usize, min_cells: usize) -> usize;

    /// Number of used bytes in this page.
    fn used_bytes(&self) -> usize;
    /// Number of free bytes in this page.
    fn effective_free_space(&self) -> u32;

    /// Returns the child at the given [index].
    /// Interior pages only
    fn child(&self, index: usize) -> Option<PageId>;

    /// Check whether this is a leaf btree page.
    fn is_leaf(&self) -> bool;
    /// Check whether this is an interior btree page.
    fn is_interior(&self) -> bool;

    /// Returns `true` if this page is underflow
    fn has_underflown(&self) -> bool;

    /// Returns `true` if this page has overflown.
    /// We need at least 2 bytes for the last cell slot and 4 extra bytes to store the pointer to an overflow page
    fn has_overflown(&self) -> bool;

    /// Read-only reference to a cell.
    fn cell(&self, index: usize) -> CellRef<'_>;

    /// Mutable reference to a cell.
    fn cell_mut(&mut self, index: usize) -> CellMut<'_>;

    /// Returns an owned cell by cloning it.
    fn owned_cell(&self, index: usize) -> OwnedCell;

    fn is_empty(&self) -> bool;

    /// Iterates over all the children pointers in this page.
    fn iter_children(&self) -> impl DoubleEndedIterator<Item = PageId> + '_;
    /// Iterates over all the cells in this page.
    fn iter_cells(&self) -> impl DoubleEndedIterator<Item = CellRef<'_>> + '_;
    /// Check whether the page will overflow if we append the additional requested size.
    fn has_space_for(&self, additional_space: usize) -> bool;

    /// Check whether the page will underflow if we remove the requested size.
    fn can_release_space(&self, removable_space: usize) -> bool;

    /// Reference to the slot array
    fn slot_array(&self) -> &[u16];

    /// Mutable reference to the slot array
    fn slot_array_mut(&mut self) -> &mut [u16];

    /// Adds [OwnedCell] to the end of this page.
    fn push(&mut self, cell: OwnedCell) -> io::Result<()>;

    /// Attempts to insert the given  [OwnedCell] in this page.
    ///
    /// Fails if the computed offset is not aligned to the required [CELL_ALIGNMENT]
    /// or if the cell does not fit in the page.
    fn insert(&mut self, index: usize, cell: OwnedCell) -> io::Result<usize>;

    /// Tries to replace the cell pointed by the given slot `index` with the
    /// `new_cell`.
    fn replace(&mut self, index: usize, new_cell: OwnedCell) -> io::Result<OwnedCell>;

    /// Removes the [OwnedCell] pointed by the given [Slot]
    fn remove(&mut self, index: usize) -> io::Result<OwnedCell>;

    /// Slides cells towards the right to eliminate fragmentation.
    fn defragment(&mut self);

    /// Works like [`Vec::drain`] execept it doesn't remove elements unless
    /// consumed.
    fn drain(&mut self, range: impl RangeBounds<usize>) -> impl Iterator<Item = OwnedCell> + '_;
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
