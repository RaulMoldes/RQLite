use crate::{
    CELL_ALIGNMENT, scalar,
    types::{PageId, UInt16},
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
#[repr(C)]
pub struct CellHeader {
    left_child: PageId,
    size: u16,
    padding: u8,
    is_overflow: bool,
}

pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();

impl CellHeader {
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    pub fn size(&self) -> u16 {
        self.size
    }

    pub fn padding(&self) -> u8 {
        self.padding
    }

    pub fn left_child(&self) -> PageId {
        self.left_child
    }

    pub fn set_left_child(&mut self, child: PageId) {
        self.left_child = child;
    }

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

    pub fn metadata(&self) -> &CellHeader {
        self.header
    }

    /// Full data slice including padding.
    pub fn data(&self) -> &[u8] {
        self.data
    }

    /// Payload without padding.
    pub fn payload(&self) -> &[u8] {
        &self.data[..self.len()]
    }

    /// Length of actual payload (excluding padding).
    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    /// Total size in bytes (header + data).
    pub fn total_size(&self) -> usize {
        CELL_HEADER_SIZE + self.header.size as usize
    }

    /// Storage size (header + data + slot pointer).
    pub fn storage_size(&self) -> usize {
        self.total_size() + Slot::SIZE
    }

    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }

    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    /// Returns the overflow page if this is an overflow cell.
    /// Returns PAGE_ZERO if not an overflow cell.
    pub fn overflow_page(&self) -> PageId {
        if !self.header.is_overflow {
            return PageId::from(0);
        }

        let start = self.len() - std::mem::size_of::<PageId>();
        PageId::from_be_bytes(
            self.payload()[start..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        )
    }
}

impl PartialEq for CellRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.data == other.data
    }
}

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

    pub fn as_ref(&self) -> CellRef<'_> {
        CellRef {
            header: self.header,
            data: self.data,
        }
    }

    pub fn metadata(&self) -> &CellHeader {
        self.header
    }

    pub fn metadata_mut(&mut self) -> &mut CellHeader {
        self.header
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.data
    }

    pub fn payload(&self) -> &[u8] {
        &self.data[..self.len()]
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        &mut self.data[..len]
    }

    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    pub fn total_size(&self) -> usize {
        CELL_HEADER_SIZE + self.header.size as usize
    }

    pub fn storage_size(&self) -> usize {
        self.total_size() + Slot::SIZE
    }

    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }

    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    pub fn overflow_page(&self) -> PageId {
        self.as_ref().overflow_page()
    }

    pub fn set_left_child(&mut self, child: PageId) {
        self.header.left_child = child;
    }

    pub fn set_overflow(&mut self, is_overflow: bool) {
        self.header.is_overflow = is_overflow;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedCell {
    header: CellHeader,
    payload: Box<[u8]>,
}

impl OwnedCell {
    /// Creates a new cell from a payload slice.
    pub fn new(payload: &[u8]) -> Self {
        let padded_size = (payload.len() + CELL_HEADER_SIZE)
            .next_multiple_of(CELL_ALIGNMENT as usize)
            - CELL_HEADER_SIZE;
        let padding = padded_size - payload.len();

        let mut data = vec![0u8; padded_size].into_boxed_slice();
        data[..payload.len()].copy_from_slice(payload);

        let header = CellHeader {
            left_child: PageId::from(0),
            size: padded_size as u16,
            is_overflow: false,
            padding: padding as u8,
        };

        Self {
            header,
            payload: data,
        }
    }

    /// Creates a new overflow cell.
    pub fn new_overflow(payload: &[u8], overflow_page: PageId) -> Self {
        let total_payload_len = payload.len() + mem::size_of::<PageId>();
        let padded_size = (total_payload_len + CELL_HEADER_SIZE)
            .next_multiple_of(CELL_ALIGNMENT as usize)
            - CELL_HEADER_SIZE;
        let padding = padded_size - total_payload_len;

        let mut data = vec![0u8; padded_size].into_boxed_slice();
        data[..payload.len()].copy_from_slice(payload);
        data[payload.len()..payload.len() + mem::size_of::<PageId>()]
            .copy_from_slice(&overflow_page.to_be_bytes());

        let header = CellHeader {
            left_child: PageId::from(0),
            size: padded_size as u16,
            is_overflow: true,
            padding: padding as u8,
        };

        Self {
            header,
            payload: data,
        }
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

    pub fn metadata(&self) -> &CellHeader {
        &self.header
    }

    pub fn metadata_mut(&mut self) -> &mut CellHeader {
        &mut self.header
    }

    /// Full data slice including padding.
    pub fn data(&self) -> &[u8] {
        &self.payload
    }

    /// Mutable data slice including padding.
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }

    /// Payload without padding.
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.len()]
    }

    /// Length of actual payload (excluding padding).
    pub fn len(&self) -> usize {
        self.header.size as usize - self.header.padding as usize
    }

    /// Total size in bytes (header + data).
    pub fn total_size(&self) -> usize {
        CELL_HEADER_SIZE + self.header.size as usize
    }

    /// Storage size (header + data + slot pointer).
    pub fn storage_size(&self) -> usize {
        self.total_size() + Slot::SIZE
    }

    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }

    pub fn set_left_child(&mut self, child: PageId) {
        self.header.left_child = child;
    }

    pub fn is_overflow(&self) -> bool {
        self.header.is_overflow
    }

    pub fn overflow_page(&self) -> PageId {
        if !self.header.is_overflow {
            return PageId::from(0);
        }

        let start = self.len() - mem::size_of::<PageId>();
        PageId::from_be_bytes(
            self.payload()[start..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        )
    }

    /// Writes this cell into a byte buffer at the given offset.
    /// Returns the number of bytes written.
    ///
    /// # Panics
    /// Panics if the buffer doesn't have enough space.
    pub fn write_to(&self, buffer: &mut [u8]) -> usize {
        let header_bytes = unsafe {
            slice::from_raw_parts(
                &self.header as *const CellHeader as *const u8,
                CELL_HEADER_SIZE,
            )
        };

        buffer[..CELL_HEADER_SIZE].copy_from_slice(header_bytes);
        buffer[CELL_HEADER_SIZE..self.total_size()].copy_from_slice(&self.payload);

        self.total_size()
    }

    pub fn as_cell_ref(&self) -> CellRef<'_> {
        CellRef {
            header: &self.header,
            data: &self.payload,
        }
    }

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
