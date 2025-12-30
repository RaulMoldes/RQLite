use crate::{
    CELL_ALIGNMENT, bytemuck_slice,
    storage::{
        core::{
            buffer::{Payload, PayloadMut, PayloadRef},
            traits::WritableLayout,
        },
        tuple::Tuple,
    },
    types::PageId,
};

use std::{fmt::Debug, mem, ptr::NonNull, slice};

// Slot offset of a cell
pub(crate) type Slot = u16;

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(C, align(8))]
pub struct CellHeader {
    /// Return the left child of the cell
    left_child: Option<PageId>,
    // Size here represents the total size of the cell, including padding.
    size: u64, // We do not know how much data are we going to store on a cell. When creating overflow pages it can become quite big so it is best to be prepared.
    /// Effective size of the cell payload
    effective_size: u32,
    // Wether this is an overflow cell or not.
    is_overflow: bool,
}

bytemuck_slice!(CellHeader);
pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();

impl CellHeader {
    pub fn new(payload_size: usize, effective_size: usize, is_overflow: bool) -> Self {
        CellHeader {
            left_child: None,
            size: payload_size as u64,
            is_overflow,

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
    data: PayloadRef<'a>,
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
            let effective_size = header.len();
            let data_ptr = ptr.byte_add(CELL_HEADER_SIZE).cast::<u8>().as_ptr();
            let data = PayloadRef::new(
                slice::from_raw_parts(data_ptr, header.size as usize),
                effective_size,
            );
            Self { header, data }
        }
    }

    #[inline]
    pub fn metadata(&self) -> &CellHeader {
        self.header
    }

    /// Full data slice including padding.
    #[inline]
    pub fn payload_ref(&self) -> PayloadRef<'_> {
        self.data
    }

    /// Effective data without padding
    #[inline]
    pub fn full_data(&self) -> &[u8] {
        &self.data.full_data()
    }

    #[inline]
    pub fn effective_data(&self) -> &[u8] {
        self.data.effective_data()
    }

    /// Length of the actual data excluding the padding
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
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
            self.effective_data()[start..]
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
    data: PayloadMut<'a>,
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
            let effective_size = header.len();
            let data_ptr = ptr.byte_add(CELL_HEADER_SIZE).cast::<u8>().as_ptr();
            let data = PayloadMut::new(
                slice::from_raw_parts_mut(data_ptr, size as usize),
                effective_size,
            );

            Self { header, data }
        }
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
    /// Coerces the mutable reference into a shared one
    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: self.header,
            data: self.data.as_payload_ref(),
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

    /// Effective data without padding
    #[inline]
    pub fn full_data(&self) -> &[u8] {
        &self.data.full_data()
    }

    /// Mutable effective data without padding
    #[inline]
    pub fn full_data_mut(&mut self) -> &mut [u8] {
        self.data.full_data_mut()
    }

    #[inline]
    /// See [CellRef<'_>::payload]
    pub fn payload_ref(&self) -> PayloadRef<'_> {
        self.data.as_payload_ref()
    }

    #[inline]
    /// See [CellRef<'_>::payload]
    pub fn payload_mut(&mut self) -> PayloadMut<'_> {
        let effective_size = self.len();
        PayloadMut::new(self.data.full_data_mut(), effective_size)
    }

    #[inline]
    pub fn effective_data(&self) -> &[u8] {
        self.data.effective_data()
    }

    /// Mutable effective data without padding
    #[inline]
    pub fn effective_data_mut(&mut self) -> &mut [u8] {
        self.data.effective_data_mut()
    }

    /// Length of the actual data excluding the padding
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
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
    payload: Payload,
}

impl From<Tuple> for OwnedCell {
    fn from(value: Tuple) -> Self {
        Self::from_tuple(value)
    }
}

impl OwnedCell {
    /// Zero copy from aligned payload (zero alloc)
    pub fn from_aligned_payload(payload: Payload, is_overflow: bool) -> Self {
        let effective_size = payload.len();
        let size = payload.capacity();

        let header = CellHeader::new(size, effective_size, is_overflow);

        Self { header, payload }
    }

    /// Zero-copy from Tuple.
    ///
    /// Since aligned payload is always aligned to [CELL_ALIGNMENT] and [CellHeader] is also,
    /// no padding computation will be needed.
    pub fn from_tuple(tuple: Tuple) -> Self {
        let payload = tuple.into_data();
        Self::from_aligned_payload(payload, false)
    }

    /// Creates a new owned cell with explicit key bounds
    pub fn new(data: &[u8]) -> Self {
        let effective_size = data.len();
        let padding = Self::compute_padding(effective_size);
        let padded_size = effective_size + padding;

        let mut payload = Payload::alloc_aligned(effective_size).unwrap();
        payload.effective_data_mut().copy_from_slice(data);

        Self::from_aligned_payload(payload, false)
    }

    /// Creates a new overflow cell.
    pub fn new_overflow(data: &[u8], overflow_page: PageId) -> Self {
        let effective_size = data.len() + mem::size_of::<PageId>();
        let padding = Self::compute_padding(effective_size);
        let padded_size = effective_size + padding;

        let mut payload = Payload::alloc_aligned(effective_size).unwrap();
        payload.effective_data_mut()[..data.len()].copy_from_slice(data);
        payload.effective_data_mut()[data.len()..effective_size]
            .copy_from_slice(&overflow_page.to_be_bytes());

        Self::from_aligned_payload(payload, true)
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
        let mut payload = Payload::alloc_aligned(cell.len()).unwrap();
        payload.effective_data_mut()[..cell.len()].copy_from_slice(cell.effective_data());

        Self {
            header: *cell.metadata(),
            payload,
        }
    }

    /// Creates an OwnedCell from a CellMut by copying its data.
    pub fn from_ref_mut(cell: CellMut<'_>) -> Self {
        let mut payload = Payload::alloc_aligned(cell.len()).unwrap();
        payload.effective_data_mut()[..cell.len()].copy_from_slice(cell.effective_data());

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

    /// Full payload slice including padding
    #[inline]
    pub fn payload_ref(&self) -> PayloadRef<'_> {
        self.payload.as_payload_ref()
    }

    /// Mutable full payload including padding
    #[inline]
    pub fn payload_mut(&mut self) -> PayloadMut<'_> {
        self.payload.as_payload_mut()
    }

    /// Effective data without padding
    #[inline]
    pub fn full_data(&self) -> &[u8] {
        &self.payload.full_data()
    }

    /// Mutable effective data without padding
    #[inline]
    pub fn full_data_mut(&mut self) -> &mut [u8] {
        self.payload.full_data_mut()
    }

    /// Effective data without padding
    #[inline]
    pub fn effective_data(&self) -> &[u8] {
        &self.payload.effective_data()
    }

    /// Mutable effective data without padding
    #[inline]
    pub fn effective_data_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        self.payload.effective_data_mut()
    }

    /// Effective length (without padding)
    #[inline]
    pub fn len(&self) -> usize {
        self.header.len()
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
            self.effective_data()[start..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        ))
    }

    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: &self.header,
            data: self.payload.as_payload_ref(),
        }
    }

    pub fn as_cell_mut(&mut self) -> CellMut<'_> {
        CellMut {
            header: &mut self.header,
            data: self.payload.as_payload_mut(),
        }
    }
}

impl PartialEq for OwnedCell {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.payload_ref() == other.payload_ref()
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

impl WritableLayout for OwnedCell {
    type Header = CellHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn content(&self) -> &[u8] {
        &self.payload.full_data()
    }
}

impl<'a> WritableLayout for CellRef<'a> {
    type Header = CellHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn content(&self) -> &[u8] {
        &self.data.full_data()
    }
}

impl<'a> WritableLayout for CellMut<'a> {
    type Header = CellHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn content(&self) -> &[u8] {
        &self.data.full_data()
    }
}
