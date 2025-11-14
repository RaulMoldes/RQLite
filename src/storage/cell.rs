use std::{alloc::Layout, fmt::Debug};

use crate::types::UInt16;
#[cfg(test)]
use crate::{dynamic_buffer_tests, static_buffer_tests};

use crate::configs::CELL_ALIGNMENT;
use crate::scalar;
use crate::{storage::buffer::BufferWithMetadata, types::PageId};

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
pub(crate) struct CellHeader {
    pub left_child: PageId,
    size: u16, //  Size is the totla size of the cell
    is_overflow: bool,
    padding: u8, // Padding is the padding added at the end to match alignment requirements.
                 // Therefore one can get the usable size by substracting size - padding.
}
pub const CELL_HEADER_SIZE: usize = std::mem::size_of::<CellHeader>();

impl CellHeader {
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    pub fn new(size: usize, padding: usize, left_child: Option<PageId>) -> Self {
        Self {
            left_child: left_child.unwrap_or(PageId::from(0)),
            size: size as u16,
            padding: padding as u8,
            is_overflow: false,
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
        let cell_size =
            (payload.len() + CELL_HEADER_SIZE).next_multiple_of(CELL_ALIGNMENT as usize);
        // Align payload size to CELL_ALIGNMENT
        let payload_size = cell_size - CELL_HEADER_SIZE;

        // Create buffer with total size = header + aligned payload
        let mut buffer = BufferWithMetadata::<CellHeader>::with_capacity(
            cell_size, // usable space for data
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
            self.used()[self.len() - std::mem::size_of::<PageId>()..]
                .try_into()
                .expect("failed deserializing overflow page number"),
        )
    }

    /// Total size of the cell including the header.
    pub fn total_size(&self) -> u16 {
        self.size() as u16
    }

    /// Total size of the cell including the header and the slot array pointer
    /// needed to store the offset.
    pub fn storage_size(&self) -> u16 {
        (self.total_size() as usize + Slot::SIZE) as u16
    }

    /// See [`BtreePage`] for details.
    pub fn aligned_size_of(data: &[u8]) -> u16 {
        Layout::from_size_align(data.len(), CELL_ALIGNMENT as usize)
            .unwrap()
            .pad_to_align()
            .size() as u16
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
            payload.len() + cell.metadata().padding as usize
        );
        assert!(!cell.metadata().is_overflow);

        // Check data content
        assert_eq!(&cell.used()[..payload.len()], payload);

        // Check padding is correctly added
        assert_eq!(cell.size() % CELL_ALIGNMENT as usize, 0);
        assert_eq!(cell.len(), payload.len(), "Payload size mismatch.");

        let expected_total_size = cell.size();
        let expected_storage_size = expected_total_size + Slot::SIZE;

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

        let data_len = cell.len() - std::mem::size_of::<PageId>();
        assert_eq!(&cell.used()[..data_len], payload);

        let expected_total_size = cell.size();
        let expected_storage_size = expected_total_size + Slot::SIZE;

        assert_eq!(cell.total_size() as usize, expected_total_size);
        assert_eq!(cell.storage_size() as usize, expected_storage_size);
    }
}
