use std::{alloc::Layout, fmt::Debug, mem, ptr::NonNull};


#[cfg(test)]
use crate::{dynamic_buffer_tests, static_buffer_tests};

use crate::configs::CELL_ALIGNMENT;
use crate::{scalar, sized};
use crate::{storage::buffer::BufferWithMetadata, types::PageId};

// Slot offset of a cell
scalar! {
    pub struct Slot(u16);
}

scalar! {
    pub struct PageOffset(u16);
}

pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();

sized! {
    #[derive(Debug, PartialEq, Clone, Copy)]
    #[repr(C)]
    pub(crate) struct CellHeader {
        size: u16, //  Size is the totla size of the cell
        is_overflow: bool,
        padding: u8, // Padding is the padding added at the end to match alignment requirements.
        // Therefore one can get the usable size by substracting size - padding.
        left_child: PageId,
    };
    pub const CELL_HEADER_SIZE
}

impl CellHeader {
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    pub fn new(size: usize, padding: usize, left_child: Option<PageId>) -> Self {
        Self {
            size: size as u16,
            padding: padding as u8,
            is_overflow: false,
            left_child: left_child.unwrap_or(PageId::from(0)),
        }
    }

    pub fn new_overflow(size: usize, padding: usize, left_child: Option<PageId>) -> Self {
        Self {
            size: size as u16,
            padding: padding as u8,
            is_overflow: true,
            left_child: left_child.unwrap_or(PageId::from(0)),
        }
    }

    pub(crate) fn left_child(&self) -> PageId {
        self.left_child
    }
}

/// A cell is a structure that stores a single BTree entry.
///
/// Each cell stores the binary entry (AKA payload or key) and a pointer to the
/// BTree node that contains cells with keys smaller than that stored in the
/// cell itself.
pub type Cell = BufferWithMetadata<CellHeader>;

impl Cell {
    pub fn new(payload: &[u8]) -> Self {
        // Align payload size to CELL_ALIGNMENT
        let payload_size = payload.len().next_multiple_of(CELL_ALIGNMENT as usize);

        // Create buffer with total size = header + aligned payload
        let mut buffer = BufferWithMetadata::<CellHeader>::with_capacity(
            payload_size, // usable space for data
            CELL_ALIGNMENT as usize,
        );

        // Initialize header
        let header = CellHeader {
            size: payload_size as u16,
            is_overflow: false,
            padding: (payload_size.saturating_sub(payload.len())) as u8,
            left_child: PageId::from(0),
        };
        *buffer.metadata_mut() = header;

        // Copy payload and update length
        buffer.push_bytes(payload);

        buffer
    }

    /// Creates a cell from a fragment of a page (zero-copy when possible)
    pub unsafe fn from_page_fragment(ptr: NonNull<[u8]>, alignment: usize) -> Self {
        // Create buffer from existing memory
        let mut buffer = BufferWithMetadata::<CellHeader>::from_non_null(ptr, alignment);

        // The header should already be initialized in the page memory
        // Set the length based on the header's size field
        let size = buffer.metadata().size;
        buffer.set_len(size as usize);
        buffer
    }

    /// Creates a new overflow cell by extending the `payload` buffer with the
    /// `overflow_page` number.
    pub fn new_overflow(payload: &[u8], overflow_page: PageId) -> Self {
        let mut buffer = Self::new(payload);

        if buffer.metadata().padding >= std::mem::size_of::<PageId>() as u8 {
            buffer.metadata_mut().padding -= std::mem::size_of::<PageId>() as u8;
        } else {
            let required_capacity = (std::mem::size_of::<PageId>()
                - buffer.metadata().padding as usize)
                + buffer.capacity();
            buffer.metadata_mut().padding -= std::mem::size_of::<PageId>() as u8;
            buffer.grow(required_capacity);
        };

        buffer.push_bytes(&overflow_page.to_be_bytes());
        buffer.metadata_mut().is_overflow = true;
        buffer
    }

    /// Returns the first overflow page of this cell.
    pub fn overflow_page(&self) -> PageId {
        if !self.metadata().is_overflow {
            return PageId::from(0);
        }

        // If the cell constains an overflow page we can extract it from the last four bytes of its content data.
        PageId::from_be_bytes(
            self.used()[self.len() - mem::size_of::<PageId>()..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        )
    }

    /// Total size of the cell including the header.
    pub fn total_size(&self) -> u16 {
        (CELL_HEADER_SIZE + self.size()) as u16
    }

    /// Total size of the cell including the header and the slot array pointer
    /// needed to store the offset.
    pub fn storage_size(&self) -> u16 {
        (self.total_size() as usize + SLOT_SIZE) as u16
    }

    /// See [`BtreePage`] for details.
    pub fn aligned_size_of(data: &[u8]) -> u16 {
        Layout::from_size_align(data.len(), CELL_ALIGNMENT as usize)
            .unwrap()
            .pad_to_align()
            .size() as u16
    }

    /// Split the cell so that the payload
    pub fn split_payload(&mut self, max_size: usize) -> Option<Cell> {
        // Ensure we have data to split
        if self.len() <= max_size {
            // Why the fuck are you calling this method even if you are not big enough to be splitted??
            return None;
        };

        let new_size = self.size().saturating_sub(max_size);
        let new_aligned = new_size.next_multiple_of(CELL_ALIGNMENT as usize);
        let padding = new_aligned.saturating_sub(new_size);

        let overflow_metadata = CellHeader::new_overflow(new_size, padding, None);

        // Get the overflow portion
        let overflow = Cell::from_slice_exact(
            &self.used()[max_size..],
            overflow_metadata,
            CELL_ALIGNMENT as usize,
        );

        // Similar to [`Vec::shrink_to_fit()`] but this way it obliges me to explicitly specify the new length and capacity which is better for myself.
        self.set_len(max_size);
        // Truncate the current cell to the split point
        self.shrink_to(max_size);
        Some(overflow)
    }
}

#[cfg(test)]
dynamic_buffer_tests!(
    cell_dyn,
    CellHeader,
    size = 64,
    align = CELL_ALIGNMENT.into()
);

#[cfg(test)]
static_buffer_tests!(
    cell_sta,
    CellHeader,
    size = 64,
    align = CELL_ALIGNMENT.into()
);

#[cfg(test)]
mod cell_tests {
    use super::*;

    #[test]
    fn test_cell_creation() {
        // Validates normal cell creation from payload.
        let payload = b"hello world";
        let cell = Cell::new(payload);

        // Check metadata
        assert_eq!(
            cell.metadata().size as usize,
            payload.len().next_multiple_of(CELL_ALIGNMENT as usize)
        );
        assert!(!cell.metadata().is_overflow);

        // Check data content
        assert_eq!(&cell.used()[..payload.len()], payload);

        // Check padding is correctly added
        assert_eq!(cell.size() % CELL_ALIGNMENT as usize, 0);
        assert_eq!(cell.len(), payload.len(), "Payload size mismatch.");

        let expected_total_size = CELL_HEADER_SIZE + cell.size();
        let expected_storage_size = expected_total_size + SLOT_SIZE;

        assert_eq!(cell.total_size() as usize, expected_total_size);
        assert_eq!(cell.storage_size() as usize, expected_storage_size);
    }

    #[test]
    fn test_cell_overflow_creation() {
        let payload = b"overflow test";
        let overflow_page = PageId::from(42);
        let cell = Cell::new_overflow(payload, overflow_page);

        assert!(cell.metadata().is_overflow);
        assert_eq!(cell.overflow_page(), overflow_page);

        let data_len = cell.len() - mem::size_of::<PageId>();
        assert_eq!(&cell.used()[..data_len], payload);

        let expected_total_size = CELL_HEADER_SIZE + cell.size();
        let expected_storage_size = expected_total_size + SLOT_SIZE;

        assert_eq!(cell.total_size() as usize, expected_total_size);
        assert_eq!(cell.storage_size() as usize, expected_storage_size);
    }

    #[test]
    fn test_from_page_fragment() {
        // TODO. Will add this test once [`Page`] is completely implemented.
    }

    #[test]
    fn test_cell_split() {
        // Validates normal cell creation from payload.
        let payload =
            b"hello world, I am a very big cell which is going to be splitted on this test!";
        let mut cell = Cell::new(payload);
        // Check metadata
        assert_eq!(
            cell.metadata().size as usize,
            payload.len().next_multiple_of(CELL_ALIGNMENT as usize)
        );
        assert!(!cell.metadata().is_overflow);

        // Check data content
        assert_eq!(&cell.used()[..payload.len()], payload);

        // Check padding is correctly added
        assert_eq!(cell.size() % CELL_ALIGNMENT as usize, 0);
        assert_eq!(cell.len(), payload.len(), "Payload size mismatch.");

        let expected_total_size = CELL_HEADER_SIZE + cell.size();
        let expected_storage_size = expected_total_size + SLOT_SIZE;

        let original_capacity = cell.capacity();
        let original_size = cell.size();

        assert_eq!(cell.total_size() as usize, expected_total_size);
        assert_eq!(cell.storage_size() as usize, expected_storage_size);
        let current_size = cell.len();
        let max_size = 12; // Randomly chosen number.
        let remaining = cell.split_payload(max_size);
        // Cell must have shrunk to [max_size]
        assert_eq!(cell.capacity(), max_size);
        assert_eq!(cell.data().len(), max_size);
        assert_eq!(cell.size(), max_size + std::mem::size_of::<CellHeader>());
        assert!(remaining.is_some());
        // We need to get the cell back to original capacity
        cell.grow(original_capacity);
        // Let's see if we can reconstruct the cell.
        cell.push_bytes(remaining.unwrap().used());
        // Cell capacity and size should have been set back.
        assert_eq!(cell.capacity(), original_capacity);
        assert_eq!(cell.size(), original_size);
        // The result should be that we have exactly the same data as at the beginning.
        assert_eq!(cell.used().len(), payload.len());
    }
}
