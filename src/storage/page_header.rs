use crate::serialization::Serializable;
use crate::types::{LogId, PageId};
use crate::PAGE_SIZE;
use std::fmt;
use std::io::{self, Read, Write};

/// Page header size is fixed on purpose
/// This way you do not need to compute the total size at runtime.
pub(crate) const PAGE_HEADER_SIZE: u16 = 28;

/// As said on [crate::storage::slot.rs] slot size is also fixed.
pub(crate) const SLOT_SIZE: u16 = 2;

/// Trait that all pages (header + buffer with data)
/// Will implement, although values will be computed at the header leve, which is where metadata lives.
pub(crate) trait HeaderOps {
    /// Return the type of the current page.
    fn type_of(&self) -> PageType;

    /// Return the page size. This allows standard pages to create overflowpages when needed.
    fn page_size(&self) -> u16;

    /// Returns a pointer to the start of the data content on the given page.
    fn content_start(&self) -> u16;

    /// Returns a pointer to the start of the free space at the page data content.
    fn free_space_start(&self) -> u16;

    /// Returns the total number of cells in the page.
    fn cell_count(&self) -> u16;

    /// Whether the page is or not an overflow page.
    fn is_overflow(&self) -> bool;

    /// Return the id of the page.
    fn id(&self) -> PageId;

    /// Return the availble space (bytes) in the page.
    fn free_space(&self) -> u16 {
        self.free_space_start().saturating_sub(self.content_start())
    }

    fn min_cell_size(&self, min_payload_factor: u8) -> u16 {
        let size = self.page_size() as u32;
        (size
            .saturating_mul(min_payload_factor as u32)
            .div_ceil(u8::MAX as u32)) as u16
    }

    /// Shortcut to get the next overflow page in the overflow chain.
    fn get_next_overflow(&self) -> Option<PageId>;

    /// Max cell size, to determine when to create an overflow page.
    /// The logic is the following:
    /// If the cell does not fit on a page, but does not excede the maximum size, we create a new page.
    /// If the cell excedes the max_cell_size, we insert what is available and then create an overflow chain to insert the rest of the data.
    fn max_cell_size(&self, max_payload_factor: u8) -> u16 {
        let size = self.page_size() as u32;
        (size
            .saturating_mul(max_payload_factor as u32)
            .div_ceil(u8::MAX as u32)) as u16
    }

    /// Setter for the page type.
    fn set_type(&mut self, page_type: super::PageType);

    /// Set the pointer to the next overflow page.
    fn set_next_overflow(&mut self, overflowpage: PageId);

    fn set_free_space_ptr(&mut self, ptr: u16);
    fn set_cell_count(&mut self, count: u16);
    fn set_content_start_ptr(&mut self, content_start: u16);
}

// 1-byte Page enum.
// Byte(0x00) is reserved for FALSE marker and Byte(0x01) is reserved for TRUE marker.
// Therefore we start from Byte(2)
crate::serializable_enum!(pub(crate) enum PageType: u8 {
    IndexInterior = 0x02,
    TableInterior = 0x05,
    IndexLeaf = 0x0A,
    TableLeaf = 0x0D,
    Overflow = 0x10,
    Free = 0x00,
});

/// [`PageHeader`] data structure, containing page metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PageHeader {
    /// Total page size in bytes.
    /// It is a fixed value for an entire database, but we need the metadata here for fast access inside a single page.
    pub(crate) page_size: u32,

    /// PageId or page number of this page.
    pub(crate) page_number: PageId,

    /// Right most page.
    /// For interior pages, it represents the right-most-child in the B+Tree.
    /// On the other hand, for leaf pages, it represents the next page in the linked list.
    pub(crate) right_most_page: PageId,

    /// Pointer to the next overflow page. Both this and the previous field will point to Page zero (`PageId::from(0)`) when unset, as anyone can point to page zero at runtime. This can be checked by calling the [`is_valid`] member function of the [`PageId`] data structure.
    pub(crate) next_overflow_page: PageId,

    /// Pointer to the start of the free space in the page.
    pub(crate) free_space_ptr: u16,

    /// Total number of cells in the page.
    pub(crate) cell_count: u16,

    /// Pointer to the start of the data content in the page.
    pub(crate) content_start_ptr: u16,

    /// Type of this page.
    pub(crate) page_type: PageType,

    /// Id of most recent log record of this page
    /// ARIES recovery protocol: https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F12/lectures/lec05-aries-rev.pdf
    pub(crate) page_lsn: LogId,

    /// Reserved space to align to 28 bytes
    pub(crate) reserved_space: u8,
}

/// Unsure about this, but for now, note that we do not deserialize or deserialize the page type altogether with the header.
/// The idea is to have a separate data structure (maybe a page enum) that can  handle page method dispatch at runtime.
/// This enum should ideally read the byte marker and deserialize the corresponding page depending on that.
impl Serializable for PageHeader {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; (PAGE_HEADER_SIZE - 1) as usize];

        reader.read_exact(&mut buffer[0..16])?;

        let page_number = PageId::from(u32::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3],
        ]));
        let page_size = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        let right_most_page = PageId::from(u32::from_be_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11],
        ]));
        let next_overflow_page = PageId::from(u32::from_be_bytes([
            buffer[12], buffer[13], buffer[14], buffer[15],
        ]));

        reader.read_exact(&mut buffer[16..22])?;

        let free_space_ptr = u16::from_be_bytes([buffer[16], buffer[17]]);
        let cell_count = u16::from_be_bytes([buffer[18], buffer[19]]);
        let content_start_ptr = u16::from_be_bytes([buffer[20], buffer[21]]);

        let page_lsn = LogId::from(u32::from_be_bytes([
            buffer[22], buffer[23], buffer[24], buffer[25],
        ]));

        reader.read_exact(&mut buffer[26..27])?;
        let reserved_space = buffer[26];

        Ok(PageHeader {
            page_number,
            page_size,
            page_type: PageType::Free,
            next_overflow_page,
            free_space_ptr,
            cell_count,
            page_lsn,
            content_start_ptr,
            right_most_page,
            reserved_space,
        })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&[self.page_type as u8])?;
        self.page_number.write_to(writer)?;
        writer.write_all(&self.page_size.to_be_bytes())?;
        self.right_most_page.write_to(writer)?;
        self.next_overflow_page.write_to(writer)?;
        writer.write_all(&self.free_space_ptr.to_be_bytes())?;
        writer.write_all(&self.cell_count.to_be_bytes())?;
        writer.write_all(&self.content_start_ptr.to_be_bytes())?;
        self.page_lsn.write_to(writer)?;
        writer.write_all(&[self.reserved_space])?;
        Ok(())
    }
}

impl fmt::Display for PageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "B-Tree Page Header:")?;
        writeln!(f, "  Type: {:?}", self.page_type)?;
        writeln!(f, "  Cell Count: {}", self.cell_count)?;
        writeln!(f, "  Free space pointer: {}", self.free_space_ptr)?;
        writeln!(f, "  Content start pointer: {}", self.content_start_ptr)?;
        writeln!(f, " Page Log Sequence Number: {}", self.page_lsn)?;
        if self.next_overflow_page != PageId::from(0) {
            writeln!(f, "  Next Overflow Page: {}", self.next_overflow_page)?;
        }
        if self.right_most_page != PageId::from(0) {
            writeln!(f, "  Right Most Page: {}", self.right_most_page)?;
        }
        Ok(())
    }
}

impl HeaderOps for PageHeader {
    fn type_of(&self) -> PageType {
        self.page_type
    }

    fn page_size(&self) -> u16 {
        self.page_size as u16
    }

    fn content_start(&self) -> u16 {
        self.content_start_ptr
    }

    fn set_next_overflow(&mut self, overflowpage: PageId) {
        self.next_overflow_page = overflowpage
    }

    fn free_space_start(&self) -> u16 {
        self.free_space_ptr
    }

    fn cell_count(&self) -> u16 {
        self.cell_count
    }

    fn is_overflow(&self) -> bool {
        matches!(self.type_of(), PageType::Overflow)
    }

    fn id(&self) -> PageId {
        self.page_number
    }

    fn get_next_overflow(&self) -> Option<PageId> {
        if self.next_overflow_page != PageId::from(0) {
            return Some(self.next_overflow_page);
        }
        None
    }

    fn set_type(&mut self, page_type: PageType) {
        self.page_type = page_type
    }

    fn set_cell_count(&mut self, count: u16) {
        self.cell_count = count
    }

    fn set_content_start_ptr(&mut self, content_start: u16) {
        self.content_start_ptr = content_start
    }

    fn set_free_space_ptr(&mut self, ptr: u16) {
        self.free_space_ptr = ptr
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE,
            page_number: PageId::from(0),
            right_most_page: PageId::from(0),
            next_overflow_page: PageId::from(0),
            page_lsn: LogId::from(0),
            free_space_ptr: (PAGE_SIZE) as u16,
            cell_count: 0,
            content_start_ptr: PAGE_HEADER_SIZE,
            page_type: PageType::Free,
            reserved_space: 0,
        }
    }
}

impl PageHeader {
    pub(crate) fn new(
        page_number: PageId,
        page_size: u32,
        page_type: PageType,
        right_most_page: Option<PageId>,
    ) -> Self {
        Self {
            page_size,
            page_number,
            right_most_page: right_most_page.unwrap_or(PageId::from(0)),
            next_overflow_page: PageId::from(0),
            // NOTE that cells are inserted at the end of the page.
            // Therefore the free space pointer grows towards the beginning of the page.
            free_space_ptr: (PAGE_SIZE) as u16,
            page_lsn: LogId::from(0),
            cell_count: 0,
            content_start_ptr: PAGE_HEADER_SIZE,
            page_type,
            reserved_space: 0,
        }
    }
}
