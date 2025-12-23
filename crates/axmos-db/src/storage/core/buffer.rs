use std::{
    alloc::{AllocError, Allocator, Global, Layout},
    any,
    collections::BinaryHeap,
    fmt::{self, Debug},
    io::{self, Error as IoError, ErrorKind},
    iter,
    mem::{self, ManuallyDrop},
    ops::{Bound, RangeBounds},
    ptr::NonNull,
    slice,
};

use crate::{
    CELL_ALIGNMENT, PAGE_ALIGNMENT,
    storage::{
        BtreeMetadata, BtreeOps, WalMetadata, WalOps,
        cell::{CELL_HEADER_SIZE, CellHeader, CellMut, CellRef, OwnedCell, Slot},
        core::traits::{
            Allocatable, AvailableSpace, Buffer, ComposedBuffer, ComposedMetadata, Identifiable,
            Writable,
        },
        wal::RecordHeader,
    },
    types::{Lsn, PageId},
};

pub struct MemBlock<M> {
    /// Total size of the buffer (size of header + size of data).
    size: u32, // We need four bytes to store the max size of a page (2^32) which would not fit in 2 bytes.
    /// Pointer to the header located at the beginning of the buffer.
    pub(crate) metadata: NonNull<M>,
    /// Pointer to the data located right after the header.
    pub(crate) data: NonNull<[u8]>,
}

impl<M> MemBlock<M> {
    // This function must be used to allocate 'owned' Buffers.
    fn allocate_zeroed(size: usize) -> Result<NonNull<[u8]>, AllocError> {
        assert!(
            PAGE_ALIGNMENT as usize >= mem::align_of::<M>(),
            "Alignment {PAGE_ALIGNMENT} is too small for type {} which requires {} bytes",
            any::type_name::<M>(),
            mem::align_of::<M>(),
        );
        assert!(
            size >= mem::size_of::<M>(),
            "Attempted to allocate {} of insufficient size: size of {} is {} while allocation size is {}",
            any::type_name::<Self>(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
            size,
        );

        Global.allocate_zeroed(Layout::from_size_align(size, PAGE_ALIGNMENT as usize).unwrap())
    }

    /// Allocate a zeroed buffer.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity)
    }

    /// Returns how many bytes can still be written without reallocating.
    pub fn capacity(&self) -> usize {
        Self::usable_space(self.size as usize) as usize
    }

    pub fn metadata_size(&self) -> usize {
        mem::size_of::<M>()
    }

    /// Returns the actual size of the buffer
    pub fn size(&self) -> usize {
        self.size as usize
    }

    /// Returns a new buffer of the given size where all the bytes are 0.
    ///
    /// If the fields of the header need values other than 0 for initialization
    /// then they should be written manually after this function call.
    pub fn new(size: usize) -> Self {
        let ptr = Self::allocate_zeroed(size).expect("Allocation error. Unable to allocate buffer");
        unsafe { Self::from_non_null(ptr) }
    }

    /// Constructs a new buffer from the given [`NonNull`] pointer.
    ///
    /// SAFETY: This functions checks internally that the pointer is of enough len() and is aligned to the required alignment by the header struct, so as long as this checks are passed, it should be safe to use.
    pub unsafe fn from_non_null(pointer: NonNull<[u8]>) -> Self {
        assert!(
            pointer.len() >= mem::size_of::<M>(),
            "attempt to construct {} from invalid pointer of size {} when size of {} is {}",
            any::type_name::<Self>(),
            pointer.len(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
        );

        assert!(
            pointer.is_aligned_to(PAGE_ALIGNMENT as usize),
            "attempt to create {} from unaligned pointer {:?}",
            any::type_name::<Self>(),
            pointer
        );

        let data = NonNull::slice_from_raw_parts(
            unsafe { pointer.byte_add(mem::size_of::<M>()).cast::<u8>() },
            Self::usable_space(pointer.len()) as usize,
        );

        Self {
            metadata: pointer.cast(),
            data,
            size: pointer.len() as u32,
        }
    }

    /// Transforms this BUFFER into another page with a different METADATA type.
    pub fn cast<T>(self) -> MemBlock<T> {
        let Self {
            metadata,
            data,
            size,
        } = self;

        assert!(
            size as usize > mem::size_of::<T>(),
            "cannot cast {} of total size {size} to {} where the size of {} is {}",
            any::type_name::<Self>(),
            any::type_name::<MemBlock<T>>(),
            any::type_name::<T>(),
            mem::size_of::<T>(),
        );

        // std::mem::forget does not actually run the destructor of [`BufferWIthMetadata`].
        // Instead, it will create (intentionally) a memory leak that will allow us to create a page of a new type.
        mem::forget(self);

        let metadata = metadata.cast();

        let data = unsafe {
            NonNull::slice_from_raw_parts(
                metadata.byte_add(mem::size_of::<T>()).cast::<u8>(),
                MemBlock::<T>::usable_space(size as usize) as usize,
            )
        };

        MemBlock {
            metadata,
            data,
            size,
        }
    }

    /// Number of bytes that can be used to store data.
    pub fn usable_space(size: usize) -> usize {
        size - mem::size_of::<M>()
    }

    /// Returns a read-only reference to the header.
    pub fn metadata(&self) -> &M {
        unsafe { self.metadata.as_ref() }
    }

    /// Returns a mutable reference to the header.
    pub fn metadata_mut(&mut self) -> &mut M {
        unsafe { self.metadata.as_mut() }
    }

    /// Returns a read-only reference to the data part of the page.
    pub fn data(&self) -> &[u8] {
        unsafe { self.data.as_ref() }
    }

    /// Returns a mutable reference to the data of the page.
    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe { self.data.as_mut() }
    }

    /// Returns a [`NonNull`] pointer to the entire in-memory buffer.
    fn as_non_null(&self) -> NonNull<[u8]> {
        NonNull::slice_from_raw_parts(self.metadata.cast::<u8>(), self.size as usize)
    }

    /// Returns a byte slice of the entire buffer including its header.
    fn as_slice(&self) -> &[u8] {
        unsafe { self.as_non_null().as_ref() }
    }

    /// Returns a mutable byte slice of the entire buffer including its header.
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { self.as_non_null().as_mut() }
    }

    /// Consumes [`self`] and returns a pointer to the underlying memory buffer.
    pub fn into_non_null(self) -> NonNull<[u8]> {
        ManuallyDrop::new(self).as_non_null()
    }
}

impl<M> AsRef<[u8]> for MemBlock<M> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<M> AsMut<[u8]> for MemBlock<M> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<M> PartialEq for MemBlock<M> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

/// Clone the block by allocating a new one with the same alignment and size and copying all the content into it.
impl<M> Clone for MemBlock<M> {
    fn clone(&self) -> Self {
        let mut cloned = Self::new(self.size as usize);
        cloned.size = self.size;

        cloned.as_slice_mut().copy_from_slice(self.as_slice());

        assert_eq!(cloned.data(), self.data());
        cloned
    }
}

impl<M: Debug> Debug for MemBlock<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("size", &self.size)
            .field("metadata", self.metadata())
            .field("data content", &self.data())
            .finish()
    }
}

/// Destructor.
/// As noted above, this destructor is only intended to be run for fully owned buffers.
/// Buffers intended to be a portion of a bigger one should be deallocated using [std::mem::forget].
/// Otherwise, dropping the buffer portion would invalidate the owned pointer.
impl<M> Drop for MemBlock<M> {
    fn drop(&mut self) {
        unsafe {
            Global.deallocate(
                self.metadata.cast(),
                Layout::from_size_align(self.size as usize, PAGE_ALIGNMENT as usize).unwrap(),
            )
        }
    }
}

/// Copies the content of the given slice into a new memory buffer
impl<M> TryFrom<&[u8]> for MemBlock<M> {
    type Error = AllocError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let header_size = mem::size_of::<M>();

        if value.len() < header_size {
            return Err(AllocError);
        }

        let mut ptr = Self::allocate_zeroed(value.len())?;

        unsafe {
            ptr.as_mut().copy_from_slice(value);
            Ok(Self::from_non_null(ptr))
        }
    }
}

impl<M> ComposedBuffer for MemBlock<M>
where
    Self: Buffer,
    M: ComposedMetadata,
{
    type DataItemHeader = M::ItemHeader;

    // Writes a new item to a composed buffer.
    // SAFETY: `last_used_offset` keeps track of where the last cell was
    // written. By substracting the total size of the new cell to
    // `last_used_offset` we get a valid pointer within the page where we
    // write the new cell.
    unsafe fn write_item_to_offset(
        &self,
        offset: u64,
        item: impl Writable<Header = Self::DataItemHeader>,
    ) {
        unsafe {
            let dest = self.data.byte_add(offset as usize).cast::<u8>().as_ptr();
            let dest_slice = slice::from_raw_parts_mut(dest, item.total_size());

            item.write_to(dest_slice);
        }
    }

    unsafe fn item_at_offset(&self, offset: u64) -> NonNull<Self::DataItemHeader> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }
}

impl<M> Buffer for MemBlock<M>
where
    M: Identifiable + Allocatable,
{
    type Header = M;
    const HEADER_SIZE: usize = std::mem::size_of::<M>();
}

/// All Buffers whose headers are identifiable will automatically be identifiable.
impl<M> Identifiable for MemBlock<M>
where
    M: Identifiable,
{
    type IdType = <M as Identifiable>::IdType;

    fn id(&self) -> Self::IdType {
        self.metadata().id()
    }
}

impl<M> Allocatable for MemBlock<M>
where
    M: Allocatable,
{
    const MAX_SIZE: usize = M::MAX_SIZE;
    const MIN_SIZE: usize = M::MIN_SIZE;

    fn alloc(id: Self::IdType, size: usize) -> Self {
        assert!(
            (Self::MIN_SIZE..=Self::MAX_SIZE).contains(&size),
            "size {} is not a value between {} and {}",
            size,
            Self::MIN_SIZE,
            Self::MAX_SIZE
        );

        // The buffer is internally aligned to [`PAGE_ALIGNMENT`]
        let mut buffer = MemBlock::<M>::new(size as usize);
        *buffer.metadata_mut() = M::alloc(id, size);

        buffer
    }
}

impl<M> WalOps for MemBlock<M>
where
    M: WalMetadata,
{
    /// Get a mutable reference to the write offset in the block
    fn current_write_offset(&mut self) -> &mut [u8] {
        let offset = self.metadata().used_bytes();
        &mut self.data_mut()[offset..]
    }

    /// Append a record to the block (no checkings made)
    fn append_record_uncheked<R: Writable<Header = RecordHeader>>(&mut self, item: R)
    where
        Self: AvailableSpace,
    {
        let dest = self.current_write_offset();
        item.write_to(dest);
        self.metadata_mut()
            .increment_write_offset(item.total_size());
    }

    // Attempts to push a record at the end of the block.
    // If the record does not fit, returns an error.
    fn try_push<R: Writable<Header = RecordHeader>>(
        &mut self,
        lsn: Lsn,
        record: R,
    ) -> io::Result<Lsn>
    where
        Self: AvailableSpace,
    {
        let record_size = record.total_size();

        if self.available_space() < record_size {
            return Err(IoError::new(
                ErrorKind::StorageFull,
                "Data too large for this block.",
            ));
        };

        self.append_record_uncheked(record);

        if self.metadata().start_lsn().is_none() {
            self.metadata_mut().set_start_lsn(lsn);
        };
        self.metadata_mut().set_last_lsn(lsn);

        Ok(lsn)
    }
}

impl<M> ComposedMetadata for MemBlock<M>
where
    M: ComposedMetadata,
{
    type ItemHeader = M::ItemHeader;
}

impl<M> WalMetadata for MemBlock<M>
where
    M: WalMetadata,
{
    fn increment_write_offset(&mut self, added_bytes: usize) {
        self.metadata_mut().increment_write_offset(added_bytes);
    }

    fn last_lsn(&self) -> Option<Lsn> {
        self.metadata().last_lsn()
    }

    fn set_last_lsn(&mut self, lsn: Lsn) {
        self.metadata_mut().set_last_lsn(lsn);
    }

    fn set_start_lsn(&mut self, lsn: Lsn) {
        self.metadata_mut().set_start_lsn(lsn);
    }

    fn start_lsn(&self) -> Option<Lsn> {
        self.metadata().start_lsn()
    }

    fn used_bytes(&self) -> usize {
        self.metadata().used_bytes()
    }
}

impl<M> BtreeMetadata for MemBlock<M>
where
    M: BtreeMetadata,
{
    /// Obtain the offset at which the content data starts (after the slot array)
    fn content_start_ptr(&self) -> u16 {
        self.metadata().content_start_ptr()
    }

    /// Obtain the number of slots in the page.
    fn num_slots(&self) -> usize {
        self.metadata().num_slots()
    }

    /// Get the free space in the page.
    fn free_space(&self) -> u32 {
        self.metadata().free_space()
    }

    /// Get the free space pointer.
    /// On the slotted pages architecture, slots grow towards the end of the page while the cells grow upwards towards the beginning. The free space pointer is the pointer from the beginning of the page where the last inserted cell data ends.
    fn free_space_pointer(&self) -> u32 {
        self.metadata().free_space_pointer()
    }

    /// Get the rightmost child of the page if it is set.
    fn right_child(&self) -> Option<PageId> {
        self.metadata().right_child()
    }

    /// Utilities to update tracking stats.
    /// Increase the amount of free space in the page.
    fn add_free_space(&mut self, space: u32) {
        self.metadata_mut().add_free_space(space);
    }

    /// Utilities to update tracking stats.
    /// Decrease the amount of free space in the page.
    fn remove_free_space(&mut self, space: u32) {
        self.metadata_mut().remove_free_space(space);
    }

    /// Utilities to update tracking stats.
    /// Move upwards the free space pointer (cell was removed)
    fn free_space_pointer_up(&mut self, bytes: u32) {
        self.metadata_mut().free_space_pointer_up(bytes);
    }

    /// Move downwards the free space pointer (cell was added)
    fn free_space_pointer_down(&mut self, bytes: u32) {
        self.metadata_mut().free_space_pointer_down(bytes);
    }

    /// Reset the free space pointer to a new value
    fn reset_free_space_pointer(&mut self, offset: u32) {
        self.metadata_mut().reset_free_space_pointer(offset);
    }

    /// Reset the free space to a new value
    fn reset_free_space(&mut self, bytes: u32) {
        self.metadata_mut().reset_free_space(bytes);
    }

    /// Reset the num slots to a new value
    fn reset_num_slots(&mut self, num: usize) {
        self.metadata_mut().reset_num_slots(num);
    }

    /// Add slots to the page
    fn add_slots(&mut self, num: usize) {
        self.metadata_mut().add_slots(num);
    }

    /// Remove slots from the page
    fn remove_slots(&mut self, num: usize) {
        self.metadata_mut().remove_slots(num);
    }

    /// Get the page's next sibling (useful when iterating over the btree during scans.)
    fn next_sibling(&self) -> Option<PageId> {
        self.metadata().next_sibling()
    }

    /// Set the next sibling value
    fn set_next_sibling(&mut self, next: Option<PageId>) {
        self.metadata_mut().set_next_sibling(next);
    }

    /// Get the page's next sibling (useful when reverse iterating over the btree during scans.)
    fn prev_sibling(&self) -> Option<PageId> {
        self.metadata().prev_sibling()
    }

    /// Set the prev sibling value
    fn set_prev_sibling(&mut self, prev: Option<PageId>) {
        self.metadata_mut().set_prev_sibling(prev);
    }

    /// Set the prev sibling value
    fn set_right_child(&mut self, new_right: Option<PageId>) {
        self.metadata_mut().set_right_child(new_right);
    }
}

impl<M> BtreeOps for MemBlock<M>
where
    M: BtreeMetadata,
{
    /// Get a valid pointer to the start of the slot array (after the header)
    fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.data.cast(), self.num_slots() as usize)
    }

    /// Compute the maximum payload size that can fit in a single page (given the usable space on that page).
    /// Must account for the header size and the slot size (total storage size in the page), and align down the value.
    fn max_payload_size_in(usable_space: usize) -> usize {
        (usable_space - CELL_HEADER_SIZE - mem::size_of::<Slot>()) & !(CELL_ALIGNMENT as usize - 1)
    }

    /// Max allowed payload size for a cell to be inserted on this [Page]
    fn max_allowed_payload_size(&self) -> u16 {
        Self::max_payload_size_in(self.capacity()) as u16
    }

    /// Returns a pointer to the [OwnedCell] located at the given [Slot].
    fn get_cell_at(&self, index: usize) -> NonNull<CellHeader> {
        debug_assert!(
            index <= self.num_slots(),
            "slot index {index} out of bounds for slot array of length {}",
            self.num_slots()
        );

        unsafe { self.item_at_offset(self.slot_array()[usize::from(index)] as u64) }
    }

    /// Threshold in the page to consider it as overflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn overflow_threshold(page_size: usize) -> usize {
        Self::usable_space(page_size).saturating_mul(3).div_ceil(4)
    }

    /// Threshold in the page to consider it as underflow state (see [Bplustree::balance] for details).
    /// The threshold is set to three quarters of the total size in page for cells.
    fn underflow_threshold(page_size: usize) -> usize {
        Self::usable_space(page_size).div_ceil(4)
    }

    /// Ideal payload size for this page.
    /// Useful when allocating an overflow chain and we have to decide how much data we will store on the first page.
    fn ideal_max_payload_size(page_size: usize, min_cells: usize) -> usize {
        debug_assert!(
            min_cells > 0,
            "if you're not gonna store any cells then why are you even calling this function?"
        );

        let ideal_size =
            Self::max_payload_size_in(Self::usable_space(page_size) as usize / min_cells);

        debug_assert!(
            ideal_size > 0,
            "page size {page_size} is too small to store {min_cells} cells"
        );

        ideal_size
    }

    /// Number of used bytes in this page.
    fn used_bytes(&self) -> usize {
        self.capacity().saturating_sub(self.free_space() as usize)
    }

    /// Number of free bytes in this page.
    fn effective_free_space(&self) -> u32 {
        self.free_space_pointer()
            .saturating_sub(self.content_start_ptr() as u32)
    }

    /// Returns the child at the given [Slot].
    /// Interior pages only
    fn child(&self, index: usize) -> Option<PageId> {
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
        !self.can_release_space(mem::size_of::<Slot>())
    }

    /// Returns `true` if this page has overflown.
    /// We need at least 2 bytes for the last cell slot and 4 extra bytes to store the pointer to an overflow page
    fn has_overflown(&self) -> bool {
        !self.has_space_for(mem::size_of::<Slot>() + CELL_HEADER_SIZE + mem::size_of::<PageId>())
    }

    /// Read-only reference to a cell.
    fn cell(&self, index: usize) -> CellRef<'_> {
        unsafe { CellRef::from_raw(self.get_cell_at(index)) }
    }

    /// Mutable reference to a cell.
    fn cell_mut(&mut self, index: usize) -> CellMut<'_> {
        unsafe { CellMut::from_raw(self.get_cell_at(index)) }
    }

    /// Returns an owned cell by cloning it.
    fn owned_cell(&self, index: usize) -> OwnedCell {
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
        (0..len).filter_map(move |i| self.child(i))
    }
    /// Iterates over all the cells in this page.
    fn iter_cells(&self) -> impl DoubleEndedIterator<Item = CellRef<'_>> + '_ {
        let len = self.num_slots();
        (0..len).map(|i| self.cell(i))
    }

    /// Check whether the page will overflow if we append the additional requested size.
    fn has_space_for(&self, additional_space: usize) -> bool {
        let occupied_space = self.used_bytes() + additional_space;
        let max_allowed = Self::overflow_threshold(self.capacity());
        occupied_space <= max_allowed
    }

    /// Check whether the page will underflow if we remove the requested size.
    fn can_release_space(&self, removable_space: usize) -> bool {
        let occupied_space = self.used_bytes().saturating_sub(removable_space);
        let min_allowed = Self::underflow_threshold(self.capacity());
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
        self.insert(self.num_slots(), cell)?;
        Ok(())
    }

    /// Attempts to insert the given  [OwnedCell] in this page.
    ///
    /// Fails if the computed offset is not aligned to the required [CELL_ALIGNMENT]
    /// or if the cell does not fit in the page.
    fn insert(&mut self, index: usize, cell: OwnedCell) -> io::Result<usize> {
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

        if index > self.num_slots() {
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

        if self.free_space_pointer() < cell_total_size || cell_storage_size > self.free_space() {
            return Err(IoError::new(
                ErrorKind::StorageFull,
                format!(
                    "Buffer overflow. Attempted to insert with overflow on a btreepage: id: {}. Free space: {}. Cell total size {}. Cell storage size {}. Capacity: {}. Write offset in page: {}.",
                    self.id(),
                    self.free_space(),
                    cell_total_size,
                    cell_storage_size,
                    self.capacity(),
                    self.free_space_pointer()
                ),
            ));
        }

        let offset = self.free_space_pointer() - cell.total_size() as u32;

        if !(cell_total_size as usize).is_multiple_of(CELL_ALIGNMENT as usize)
            || !(offset as usize).is_multiple_of(CELL_ALIGNMENT as usize)
        {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "attempted to insert with an unaligned cell size.",
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
        if index < self.num_slots() {
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
    fn replace(&mut self, index: usize, new_cell: OwnedCell) -> io::Result<OwnedCell> {
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
    fn remove(&mut self, index: usize) -> io::Result<OwnedCell> {
        let len = self.num_slots();

        if index > self.num_slots() {
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
                let cell = self.owned_cell(slot_index as _);
                slot_index += 1;
                drain_index += 1;
                Some(cell)
            } else {
                // Now compute gained space and shift slots towards the left.
                self.add_free_space(
                    (start..slot_index)
                        .map(|slot| self.cell(slot).storage_size())
                        .sum::<usize>() as u32,
                );

                self.free_space_pointer_down(
                    (start..slot_index)
                        .map(|slot| self.cell(slot).total_size())
                        .sum::<usize>() as u32,
                );

                self.slot_array_mut().copy_within(slot_index.., start);

                self.remove_slots(slot_index - start);

                None
            }
        })
    }
}

// Macro to create aligned buffers inline.
#[macro_export]
macro_rules! aligned_buf {
    ($size:expr) => {{
        use $crate::storage::core::buffer::MemBlock;

        let buffer: MemBlock<()> = MemBlock::new($size);
        buffer
    }};

    ($size:expr, $val: expr) => {{
        use $crate::storage::core::buffer::MemBlock;

        let mut buffer: MemBlock<()> = MemBlock::new($size);

        buffer.data_mut().fill($val);

        buffer
    }};
}
