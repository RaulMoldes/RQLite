use crate::{
    CELL_ALIGNMENT, bytemuck_slice,
    storage::{WritableLayout, core::buffer::AlignedPayload, tuple::Tuple},
    tree::cell_ops::{Buildable, KeyBytes},
    types::PageId,
    writable_layout,
};

use std::{fmt::Debug, mem, ptr::NonNull, slice};

// Slot offset of a cell
pub(crate) type Slot = u16;

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(64))]
pub struct CellHeader {
    /// Return the left child of the cell
    left_child: Option<PageId>,
    // Size here represents the total size of the cell, including padding.
    size: u64, // We do not know how much data are we going to store on a cell. When creating overflow pages it can become quite big so it is best to be prepared.
    /// Keys offset in the payload
    keys_start: u32,
    // End of the keys in the payload
    keys_end: u32,
    /// Effective size of the cell payload
    effective_size: u32,
    // Wether this is an overflow cell or not.
    is_overflow: bool,
}

bytemuck_slice!(CellHeader);
pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();

impl CellHeader {
    pub fn new(
        keys_start: usize,
        keys_end: usize,
        payload_size: usize,
        is_overflow: bool,
        effective_size: usize,
    ) -> Self {
        CellHeader {
            left_child: None,
            size: payload_size as u64,
            is_overflow,
            keys_start: keys_start as u32,
            keys_end: keys_end as u32,
            effective_size: effective_size as u32,
        }
    }

    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.effective_size as usize
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }
    #[inline]
    pub fn keys_start(&self) -> usize {
        self.keys_start as usize
    }

    #[inline]
    pub fn keys_end(&self) -> usize {
        self.keys_end as usize
    }

    #[inline]
    pub fn left_child(&self) -> Option<PageId> {
        self.left_child
    }

    #[inline]
    pub fn set_left_child(&mut self, child: PageId) {
        self.left_child = Some(child);
    }

    #[inline]
    pub fn set_overflow(&mut self, is_overflow: bool) {
        self.is_overflow = is_overflow;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CellRef<'a> {
    header: &'a CellHeader,
    data: &'a [u8],
}

impl<'a> CellRef<'a> {
    /// Constructs a CellRef from a raw pointer to a CellHeader.
    ///
    /// # Safety
    /// - `ptr` must point to a valid, aligned `CellHeader`
    /// - The memory region after the header must be at least `header.size` bytes
    /// - The lifetime `'a` must not outlive the backing memory
    pub unsafe fn from_raw(ptr: NonNull<CellHeader>) -> Self {
        unsafe {
            let header = ptr.as_ref();
            let data_ptr = ptr.byte_add(CELL_HEADER_SIZE).cast::<u8>().as_ptr();
            let data = slice::from_raw_parts(data_ptr, header.size as usize);
            Self { header, data }
        }
    }

    #[inline]
    pub fn metadata(&self) -> &CellHeader {
        self.header
    }

    /// Full data slice including padding.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data
    }

    /// Payload without padding.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.data[..self.len()]
    }

    /// Length of the actual data excluding the padding
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
    }

    #[inline]
    pub fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.payload()[self.header.keys_start()..self.header().keys_end()])
    }

      #[inline]
    pub fn keys_start(&self) -> usize {
        self.header.keys_start() as usize
    }

    #[inline]
    pub fn keys_end(&self) -> usize {
        self.header.keys_end() as usize
    }

    /// Storage size (header + data + slot pointer).
    #[inline]
    pub fn storage_size(&self) -> usize {
        mem::size_of::<CellHeader>() + self.data().len() + mem::size_of::<Slot>()
    }

    /// Optional left child if set
    #[inline]
    pub fn left_child(&self) -> Option<PageId> {
        self.header.left_child
    }

    /// Whether this cell is overflow
    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    /// Returns the overflow page if this is an overflow cell.
    /// Returns PAGE_ZERO if not an overflow cell.
    /// Overflow page pointer is stored at the end of the cell (last four bytes) on overflow cells.
    pub fn overflow_page(&self) -> Option<PageId> {
        if !self.header.is_overflow {
            return None;
        }

        let start = self.len() - mem::size_of::<PageId>();
        Some(PageId::from_be_bytes(
            self.payload()[start..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        ))
    }
}

impl PartialEq for CellRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.data == other.data
    }
}

/// Mutable reference to a cell
#[derive(Debug)]
pub struct CellMut<'a> {
    header: &'a mut CellHeader,
    data: &'a mut [u8],
}

impl<'a> CellMut<'a> {
    /// Constructs a CellMut from a raw pointer to a CellHeader.
    ///
    /// # Safety
    /// - `ptr` must point to a valid, aligned `CellHeader`
    /// - The memory region after the header must be at least `header.size` bytes
    /// - The lifetime `'a` must not outlive the backing memory
    /// - No other references to this memory region may exist
    pub unsafe fn from_raw(ptr: NonNull<CellHeader>) -> Self {
        unsafe {
            let header = ptr.cast::<CellHeader>().as_mut();
            let size = header.size;
            let data_ptr = ptr.byte_add(CELL_HEADER_SIZE).cast::<u8>().as_ptr();
            let data = slice::from_raw_parts_mut(data_ptr, size as usize);

            Self { header, data }
        }
    }

    #[inline]
    /// Coerces the mutable reference into a shared one
    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: self.header,
            data: self.data,
        }
    }

    #[inline]
    /// See [CellRef<'_>::metadata]
    pub fn metadata(&self) -> &CellHeader {
        self.header
    }

    #[inline]
    /// See [CellRef<'_>::metadata]
    pub fn metadata_mut(&mut self) -> &mut CellHeader {
        self.header
    }

      #[inline]
    pub fn keys_start(&self) -> usize {
        self.header.keys_start() as usize
    }

    #[inline]
    pub fn keys_end(&self) -> usize {
        self.header.keys_end() as usize
    }

    #[inline]
    /// See [CellRef<'_>::data]
    pub fn data(&self) -> &[u8] {
        self.data
    }

    #[inline]
    /// See [CellRef<'_>::data]
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.data
    }

    #[inline]
    /// See [CellRef<'_>::payload]
    pub fn payload(&self) -> &[u8] {
        &self.data[..self.len()]
    }

    #[inline]
    /// See [CellRef<'_>::payload]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        &mut self.data[..len]
    }

    /// Length of the actual data excluding the padding
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
    }

    #[inline]
    pub fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.payload()[self.header.keys_start()..self.header().keys_end()])
    }

    #[inline]
    /// See [CellRef<'_>::storage_size]
    pub fn storage_size(&self) -> usize {
        mem::size_of::<CellHeader>() + self.data().len() + mem::size_of::<Slot>()
    }

    #[inline]
    /// See [CellRef<'_>::left_child]
    pub fn left_child(&self) -> Option<PageId> {
        self.header.left_child
    }

    #[inline]
    /// See [CellRef<'_>::is_overflow]
    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    #[inline]
    /// See [CellRef<'_>::overflow_page]
    pub fn overflow_page(&self) -> Option<PageId> {
        self.as_cell_ref().overflow_page()
    }

    #[inline]
    /// Set the left child of the cell
    pub fn set_left_child(&mut self, child: Option<PageId>) {
        self.header.left_child = child;
    }
}

/// Cell is a self-contained data structure made up of a header and a payload.
/// The payload is stored in an AlignedPayload for proper memory alignment.
///
/// When writing to a page, alignment requirements [CELL_ALIGNMENT] are respected.
#[derive(Debug, Clone)]
pub struct OwnedCell {
    header: CellHeader,
    payload: AlignedPayload,
}

impl OwnedCell {
    /// Zero-copy from Tuple.
    ///
    /// Since aligned payload is always aligned to [CELL_ALIGNMENT] and [CellHeader] is also,
    /// no padding computation will be needed.
    pub fn from_tuple(tuple: Tuple<'_>) -> Self {
        let (keys_start, keys_end) = tuple.key_bounds();
        let payload = tuple.into_data();
        let effective_size = payload.len();
        let size = payload.len().next_multiple_of(CELL_ALIGNMENT);

        let header = CellHeader::new(keys_start, keys_end, size, false, effective_size);

        Self { header, payload }
    }

    /// Creates a new owned cell with explicit key bounds
    pub fn new_with_keys(data: &[u8], keys_start: usize, keys_end: usize) -> Self {
        let effective_size = data.len();
        let padding = Self::compute_padding(effective_size);
        let padded_size = effective_size + padding;

        let mut payload = AlignedPayload::alloc_aligned(padded_size).unwrap();
        payload.data_mut()[..effective_size].copy_from_slice(data);

        let header = CellHeader::new(keys_start, keys_end, padded_size, false, effective_size);

        Self { header, payload }
    }

    /// Creates a new overflow cell.
    pub fn new_overflow(
        data: &[u8],
        overflow_page: PageId,
        keys_start: usize,
        keys_end: usize,
    ) -> Self {
        let data_size = data.len() + mem::size_of::<PageId>();
        let padding = Self::compute_padding(data_size);
        let padded_size = data_size + padding;

        let mut payload = AlignedPayload::alloc_aligned(padded_size).unwrap();
        payload.data_mut()[..data.len()].copy_from_slice(data);
        payload.data_mut()[data.len()..data.len() + mem::size_of::<PageId>()]
            .copy_from_slice(&overflow_page.to_be_bytes());

        let header = CellHeader::new(keys_start, keys_end, padded_size, true, data_size);

        Self { header, payload }
    }

    /// Computes the padding required to align [payload_size] to [CELL_ALIGNMENT]
    #[inline]
    fn compute_padding(payload_size: usize) -> usize {
        let total_unaligned = payload_size + CELL_HEADER_SIZE;
        let total_aligned = total_unaligned.next_multiple_of(CELL_ALIGNMENT as usize);
        total_aligned - CELL_HEADER_SIZE - payload_size
    }

    /// Creates an OwnedCell from a CellRef by copying its data.
    pub fn from_ref(cell: CellRef<'_>) -> Self {
        let effective_size = cell.len();
        let padded_size = cell.data().len();

        let mut payload = AlignedPayload::alloc_aligned(padded_size).unwrap();
        payload.data_mut().copy_from_slice(cell.data());

        Self {
            header: *cell.metadata(),
            payload,
        }
    }

    #[inline]
    pub fn keys_start(&self) -> usize {
        self.header.keys_start() as usize
    }

    #[inline]
    pub fn keys_end(&self) -> usize {
        self.header.keys_end() as usize
    }

    /// Creates an OwnedCell from a CellMut by copying its data.
    pub fn from_ref_mut(cell: CellMut<'_>) -> Self {
        let padded_size = cell.data().len();

        let mut payload = AlignedPayload::alloc_aligned(padded_size).unwrap();
        payload.data_mut().copy_from_slice(cell.data());

        Self {
            header: *cell.metadata(),
            payload,
        }
    }

    #[inline]
    pub fn header(&self) -> &CellHeader {
        &self.header
    }

    #[inline]
    pub fn metadata(&self) -> &CellHeader {
        &self.header
    }

    #[inline]
    pub fn metadata_mut(&mut self) -> &mut CellHeader {
        &mut self.header
    }

    /// Full data slice including padding
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.payload.data()
    }

    /// Mutable data slice including padding
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.payload.data_mut()
    }

    /// Payload without padding
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.payload.data()[..self.len()]
    }

    /// Mutable payload without padding
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        &mut self.payload.data_mut()[..len]
    }

    /// Effective length (without padding)
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
    }

    #[inline]
    pub fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.payload()[self.header.keys_start()..self.header.keys_end()])
    }

    /// Total size (header + padded payload)
    #[inline]
    pub fn total_size(&self) -> usize {
        CELL_HEADER_SIZE + self.header.size as usize
    }

    /// Storage size (header + padded payload + slot pointer)
    #[inline]
    pub fn storage_size(&self) -> usize {
        self.total_size() + mem::size_of::<Slot>()
    }

    #[inline]
    pub fn left_child(&self) -> Option<PageId> {
        self.header.left_child
    }

    #[inline]
    pub fn set_left_child(&mut self, child: Option<PageId>) {
        self.header.left_child = child;
    }

    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    pub fn overflow_page(&self) -> Option<PageId> {
        if !self.header.is_overflow {
            return None;
        }

        let start = self.len() - mem::size_of::<PageId>();
        Some(PageId::from_be_bytes(
            self.payload()[start..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        ))
    }

    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: &self.header,
            data: self.payload.data(),
        }
    }

    pub fn as_cell_mut(&mut self) -> CellMut<'_> {
        CellMut {
            header: &mut self.header,
            data: self.payload.data_mut(),
        }
    }
}

impl PartialEq for OwnedCell {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.payload() == other.payload()
    }
}

impl From<CellRef<'_>> for OwnedCell {
    fn from(cell: CellRef<'_>) -> Self {
        Self::from_ref(cell)
    }
}

impl From<CellMut<'_>> for OwnedCell {
    fn from(cell: CellMut<'_>) -> Self {
        Self::from_ref_mut(cell)
    }
}

impl<'a> From<&'a OwnedCell> for CellRef<'a> {
    fn from(c: &'a OwnedCell) -> Self {
        c.as_cell_ref()
    }
}

impl<'a> From<&'a mut OwnedCell> for CellMut<'a> {
    fn from(c: &'a mut OwnedCell) -> Self {
        c.as_cell_mut()
    }
}

impl From<Tuple<'_>> for OwnedCell {
    fn from(tuple: Tuple<'_>) -> Self {
        Self::from_tuple(tuple)
    }
}

writable_layout!(OwnedCell, CellHeader, header, payload);
writable_layout!(CellRef<'_>, CellHeader, header, data, lifetime);
writable_layout!(CellMut<'_>, CellHeader, header, data, lifetime);
