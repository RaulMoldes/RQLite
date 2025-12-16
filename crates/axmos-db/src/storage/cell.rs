use crate::{
    CELL_ALIGNMENT, as_slice, scalar,
    types::{PageId, UInt16},
    writable_layout,
};

use std::{fmt::Debug, mem, ptr::NonNull, slice};

// Slot offset of a cell
scalar! {
    pub struct Slot(u16);
}

impl From<UInt16> for Slot {
    fn from(value: UInt16) -> Self {
        Self(value.0)
    }
}

impl From<Slot> for UInt16 {
    fn from(value: Slot) -> UInt16 {
        UInt16(value.0)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(8))]
pub struct CellHeader {
    left_child: Option<PageId>,
    // Size here represents the total size of the cell, including padding.
    size: u64, // We do not know how much data are we going to store on a cell. When creating overflow pages it can become quite big so it is best to be prepared.
    padding: u8,
    is_overflow: bool,
}

as_slice!(CellHeader);
pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();

impl CellHeader {
    pub fn new(padding: usize, payload_size: usize, is_overflow: bool) -> Self {
        CellHeader {
            left_child: None,
            size: (payload_size + padding) as u64,
            is_overflow,
            padding: padding as u8,
        }
    }

    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn padding(&self) -> u8 {
        self.padding
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

    /// Length of actual payload (excluding padding).
    #[inline]
    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    /// Storage size (header + data + slot pointer).
    #[inline]
    pub fn storage_size(&self) -> usize {
        mem::size_of::<CellHeader>() + self.data().len() + Slot::SIZE
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

    #[inline]
    /// See [CellRef<'_>::len]
    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    #[inline]
    /// See [CellRef<'_>::storage_size]
    pub fn storage_size(&self) -> usize {
        mem::size_of::<CellHeader>() + self.data().len() + Slot::SIZE
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
    pub fn set_left_child(&mut self, child: PageId) {
        self.header.left_child = Some(child);
    }
}

/// Cell is a self-contained data structure that is made up of a header and a payload in the heap.
/// When writing to a page, we need to make sure to match alignment requirements [CELL_ALIGNMENT]
/// Note that alignment is computed taking into account the size of the header, that is the [OwnedCell::total_size].
///
/// We do it by first computing the total unaligned size that the cell will occupy (payload + header),
/// then aligning to the next valid offset, and finally substracting the header back.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedCell {
    header: CellHeader,
    payload: Box<[u8]>,
}

impl OwnedCell {
    /// Creates a new owned cell from a payload slice.
    pub fn new(payload: &[u8]) -> Self {
        let padding = Self::compute_padding(payload.len());
        let padded_size = payload.len() + padding;

        let mut data = vec![0u8; padded_size].into_boxed_slice();
        data[..payload.len()].copy_from_slice(payload);

        let header = CellHeader::new(padding, payload.len(), false);

        Self {
            header,
            payload: data,
        }
    }

    /// Creates a new overflow cell.
    pub fn new_overflow(payload: &[u8], overflow_page: PageId) -> Self {
        let payload_size = payload.len() + mem::size_of::<PageId>();
        let padding = Self::compute_padding(payload_size);
        let padded_size = payload_size + padding;

        let mut data = vec![0u8; padded_size].into_boxed_slice();
        data[..payload.len()].copy_from_slice(payload);
        data[payload.len()..payload.len() + mem::size_of::<PageId>()]
            .copy_from_slice(&overflow_page.to_be_bytes());

        let header = CellHeader::new(padding, payload_size, true);

        Self {
            header,
            payload: data,
        }
    }

    #[inline]
    /// Computes the padding required in order to align the given [payload_size]
    /// to [CELL_ALIGNMENT]
    fn compute_padding(payload_size: usize) -> usize {
        let padded_size = (payload_size + CELL_HEADER_SIZE)
            .next_multiple_of(CELL_ALIGNMENT as usize)
            - CELL_HEADER_SIZE;
        padded_size - payload_size
    }

    /// Creates an OwnedCell from a CellRef by copying its data.
    pub fn from_ref(cell: CellRef<'_>) -> Self {
        Self {
            header: *cell.metadata(),
            payload: cell.data().into(),
        }
    }

    /// Creates an OwnedCell from a CellRef by copying its data.
    pub fn from_ref_mut(cell: CellMut<'_>) -> Self {
        Self {
            header: *cell.metadata(),
            payload: cell.data().into(),
        }
    }

    #[inline]
    /// See [CellRef<'_>::metadata]
    pub fn metadata(&self) -> &CellHeader {
        &self.header
    }

    #[inline]
    /// See [CellRef<'_>::metadata]
    pub fn metadata_mut(&mut self) -> &mut CellHeader {
        &mut self.header
    }

    #[inline]
    /// Full data slice including padding
    pub fn data(&self) -> &[u8] {
        &self.payload
    }

    #[inline]
    /// Mutable data slice including padding.
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }

    #[inline]
    /// Payload without padding.
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.len()]
    }

    #[inline]
    /// Mutable payload without padding.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        &mut self.payload[..len]
    }

    #[inline]
    /// See [CellRef<'_>::len]
    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    #[inline]
    /// See [CellRef<'_>::total_size]
    pub fn total_size(&self) -> usize {
        CELL_HEADER_SIZE + self.header.size as usize
    }

    #[inline]
    /// See [CellRef<'_>::storage_size]
    pub fn storage_size(&self) -> usize {
        self.total_size() + Slot::SIZE
    }

    #[inline]
    /// See [CellRef<'_>::left_child]
    pub fn left_child(&self) -> Option<PageId> {
        self.header.left_child
    }

    #[inline]
    /// Set the left child to a new value.
    pub fn set_left_child(&mut self, child: PageId) {
        self.header.left_child = Some(child);
    }

    #[inline]
    /// See [CellRef<'_>::is_overflow]
    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    /// See [CellRef<'_>::overflow_page]
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

    /// Get a reference to the cell
    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: &self.header,
            data: &self.payload,
        }
    }
    /// Get a mutable reference to the cell
    pub fn as_cell_mut(&mut self) -> CellMut<'_> {
        CellMut {
            header: &mut self.header,
            data: &mut self.payload,
        }
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
        CellRef {
            header: &c.header,
            data: &c.payload,
        }
    }
}

impl<'a> From<&'a mut OwnedCell> for CellMut<'a> {
    fn from(c: &'a mut OwnedCell) -> Self {
        CellMut {
            header: &mut c.header,
            data: &mut c.payload,
        }
    }
}

writable_layout!(OwnedCell, CellHeader, header, payload);
writable_layout!(CellRef<'_>, CellHeader, header, data, lifetime);
writable_layout!(CellMut<'_>, CellHeader, header, data, lifetime);
