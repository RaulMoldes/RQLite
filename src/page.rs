//! # Page Module
//!
//! This module defines the structures and functions for handling B-Tree pages, overflow pages, and free pages in a SQLite database.
//! It includes the representation of page headers, cell types, and methods for reading and writing pages.
//!
//! BASIC PAGE STRUCTURE
//! Each page in a SQLite database withsists of a header and a series of cells. The header withtains metadata about the page, such as its type, size, and offsets to the cells.
//! After the header, the cells are stored in a withtiguous block of memory. Each cell has an associated offset in the page, which is stored in a separate array (slot array).
//! The slot array is a list of offsets that point to the start of each cell in the page. Therefore to access a cell, we only need its physical identifier, which is made up of the page_number + the slot_number.
//! IMPORTANT: The slots grow from the beginning of the page to the end, while the cells grow from the end of the page to the beginning.
//! Therefore a page becomes full when the slot array reaches the point where the cells start (free space pointer).
//! --------------------------------------------------------
//! | Page Header (B-Tree)                                 |
//! |------------------------------------------------------|
//! | Slot Array (Offsets to Cells)                        |
//! |------------------------------------------------------|
//! |                                                      |
//! |                        DATA                          |
//! |                                                      |
//! |------------------------------------------------------|
use crate::header::HEADER_SIZE;

use std::fmt;
use std::io::Cursor;
use std::io::{self, Read, Write};

/// Page Types on SQLite. There are basically two types of pages:
/// Table pages: A table page is a B-Tree page that stores data from a table.
/// Index pages: An index page is a B-Tree page that stores data from an index.
/// Each btree page can be either a leaf page or an interior page (see btree module).
/// Special pages include overflow pages and free pages.
/// Overflow pages are used to store data that does not fit in the main page.
/// Free pages are used to keep track of the free space in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Index interior page (Interior of the B-Tree index)
    IndexInterior = 0x02,
    /// Table interior page (Interior of the B-Tree table)
    TableInterior = 0x05,
    /// Leaf index page (Leaf of the B-Tree index)
    IndexLeaf = 0x0A,
    /// Leaf table page (Leaf of the B-Tree table)
    TableLeaf = 0x0D,
    /// Overflow page (used for large data)
    Overflow = 0x10,
    /// Free page (used for free space management)
    Free = 0x00,
}

impl PageType {
    /// Builds a `PageType` from a byte marker.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(PageType::IndexInterior),
            0x05 => Some(PageType::TableInterior),
            0x0A => Some(PageType::IndexLeaf),
            0x0D => Some(PageType::TableLeaf),
            0x10 => Some(PageType::Overflow),
            0x00 => Some(PageType::Free),
            _ => None, // Not a valid page type
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            PageType::IndexInterior => 0x02,
            PageType::TableInterior => 0x05,
            PageType::IndexLeaf => 0x0A,
            PageType::TableLeaf => 0x0D,
            PageType::Overflow => 0x10,
            PageType::Free => 0x00,
        }
    }

    /// Returns true if the page is an interior page.
    pub fn is_interior(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::TableInterior)
    }

    /// Returns true if the page is a leaf page.
    pub fn is_leaf(&self) -> bool {
        matches!(self, PageType::IndexLeaf | PageType::TableLeaf)
    }

    /// Returns true if the page is part of an index.
    pub fn is_index(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::IndexLeaf)
    }

    /// Returns true if the page is part of a table.
    pub fn is_table(&self) -> bool {
        matches!(self, PageType::TableInterior | PageType::TableLeaf)
    }

    /// Returns true if the page is an overflow page.
    pub fn is_overflow(&self) -> bool {
        matches!(self, PageType::Overflow)
    }

    /// Returns true if the page is a free page.
    pub fn is_free(&self) -> bool {
        matches!(self, PageType::Free)
    }
}

/// Custom trait for serializing and deserializing data to and from byte streams.
/// This trait is used to read and write data in a binary format.
/// It is implemented for various types, including B-Tree page headers and cells.
pub trait ByteSerializable {
    /// Reads a value from a byte stream.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized;
    /// Writes a value to a byte stream.
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;
}

/// Represents the header of a B-Tree page.
#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// Type of btree page (table, index, leaf or interior).
    pub page_type: PageType,
    /// Offset to the first free block in the page (free space pointer).
    pub first_free_block_offset: u16,
    /// Total number of slots (cells) in the page.
    pub cell_count: u16,
    /// Offset to the point where the content starts (after the header).
    pub content_start_offset: u16,
    /// Number of fragmented free bytes.
    pub fragmented_free_bytes: u8,
    /// For Btree interior pages, the page number of the rightmost child.
    pub right_most_page: Option<u32>,
}

impl BTreePageHeader {
    /// Creates a new header for a Leaf B-Tree page.
    /// # Parameters
    /// * `page_type` - Type of the page (must be a leaf type).
    /// # Panics
    /// Panics if the page type is not a leaf type.
    pub fn new_leaf(page_type: PageType) -> Self {
        if !page_type.is_leaf() {
            panic!("Expected a leaf page type");
        }

        BTreePageHeader {
            page_type,
            first_free_block_offset: 0,
            cell_count: 0,
            content_start_offset: 0, // Se actualizará al añadir celdas
            fragmented_free_bytes: 0,
            right_most_page: None,
        }
    }

    /// Creates a new header for an interior page type.
    /// # Parameters
    /// * `page_type` - Type of the page (must be an interior type).
    /// * right_most_page - Page number of the rightmost child.
    /// # Panics
    /// Panics if the page type is not an interior type.
    pub fn new_interior(page_type: PageType, right_most_page: u32) -> Self {
        if !page_type.is_interior() {
            panic!("Expected an interior page type");
        }

        BTreePageHeader {
            page_type,
            first_free_block_offset: 0,
            cell_count: 0,
            content_start_offset: 0, // Se actualizará al añadir celdas
            fragmented_free_bytes: 0,
            right_most_page: Some(right_most_page),
        }
    }

    /// Calculates the total size of the page header in bytes.
    pub fn size(&self) -> usize {
        if self.page_type.is_leaf() {
            8 // Leaf pages: type (1) + first_free (2) + cell_count (2) + content_start (2) + fragmented_bytes (1)
        } else {
            12 //For interior pages we need to add the right_most_page (4)
        }
    }
}

impl ByteSerializable for BTreePageHeader {
    /// Reads a header from a source, which must implement the trait Read.
    /// # Parameters
    /// * `reader` - Source from which to read the header.
    /// # Errors
    /// Returns an error if the header cannot be read or if the page type is unknown.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; 12]; // Buffer to read the header
        reader.read_exact(&mut buffer[0..1])?; // Read the page type

        let page_type = PageType::from_byte(buffer[0]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid btree page type: {:#04x}", buffer[0]),
            )
        })?;

        // Read common fields
        // Read the first free block offset, cell count, content start offset, and fragmented free bytes
        reader.read_exact(&mut buffer[1..8])?;
        let first_free_block_offset = u16::from_be_bytes([buffer[1], buffer[2]]);
        let cell_count = u16::from_be_bytes([buffer[3], buffer[4]]);
        let content_start_offset = u16::from_be_bytes([buffer[5], buffer[6]]);
        let fragmented_free_bytes = buffer[7];

        // Read the rightmost page number if the page type is interior
        let right_most_page = if page_type.is_interior() {
            reader.read_exact(&mut buffer[8..12])?;
            Some(u32::from_be_bytes([
                buffer[8], buffer[9], buffer[10], buffer[11],
            ]))
        } else {
            None
        };

        Ok(BTreePageHeader {
            page_type,
            first_free_block_offset,
            cell_count,
            content_start_offset,
            fragmented_free_bytes,
            right_most_page,
        })
    }

    /// Write the header to a writer, which must implement the trait Write.
    /// # Parameters
    /// * `writer` - Writer to which to write the header.
    /// # Errors
    /// Returns an error if the header cannot be written.
    ///
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write the page type
        writer.write_all(&[self.page_type as u8])?;

        // Write common fields
        writer.write_all(&self.first_free_block_offset.to_be_bytes())?;
        writer.write_all(&self.cell_count.to_be_bytes())?;
        writer.write_all(&self.content_start_offset.to_be_bytes())?;
        writer.write_all(&[self.fragmented_free_bytes])?;

        // Write the rightmost page number if the page type is interior
        if let Some(right_most) = self.right_most_page {
            writer.write_all(&right_most.to_be_bytes())?;
        }

        Ok(())
    }
}

/// Display trait implementation for BTreePageHeader
impl fmt::Display for BTreePageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "B-Tree Page Header:")?;
        writeln!(f, "  Type: {:?}", self.page_type)?;
        writeln!(f, "  Cell Count: {}", self.cell_count)?;
        writeln!(f, "  content Start Offset: {}", self.content_start_offset)?;

        if let Some(right_most) = self.right_most_page {
            writeln!(f, "  Right Most Page: {}", right_most)?;
        }

        Ok(())
    }
}

/// Each cell on a B-Tree page can be of different types.
/// Table leaf cells is where the actual data is stored.
#[derive(Debug, Clone)]
pub struct TableLeafCell {
    /// Size of the payload in bytes.
    pub payload_size: u64,
    /// Physical row_id (row identifier).
    pub row_id: i64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: Vec<u8>,
    /// Pointer to the overflow page that stores therest of the data if it does not fit in this page.
    pub overflow_page: Option<u32>,
}

/// Table interior cells are used to store the keys that define the boundaries between child pages.
/// Used for efficient searching and navigation in the B-Tree structure.
/// (See BTree module for more details).
#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    /// Page_number (pointer) to the left_child.
    pub left_child_page: u32,
    /// Key that defines the boundary between the left and right child..
    pub key: i64,
}

/// Each cell in a B-Tree index leaf page withtains a payload and a rowid.
#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    /// Size of the payload in bytes.
    pub payload_size: u64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    /// In this case it is the index key.
    /// (If you created an index on column a of table t the payload will be the value of a for each row).
    pub payload: Vec<u8>,
    /// References to the page of overflow (if the payload does not fit in this page). This is very rare but can happen if we try to index on very large columns.
    pub overflow_page: Option<u32>,
}

/// Each cell in a B-Tree index interior page withtains a pointer to the left child and a key.
#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    /// Page_number (pointer) to the left child.
    pub left_child_page: u32,
    /// Payload size in bytes. In index cells, we also store the key. This is the main difference from the table cells.
    pub payload_size: u64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: Vec<u8>,
    /// References to the page of overflow (if the payload does not fit in this page).
    pub overflow_page: Option<u32>,
}

/// Represents a cell in a B-Tree page.
#[derive(Debug, Clone)]
pub enum BTreeCell {
    /// Table leaf cell.
    TableLeaf(TableLeafCell),
    /// Interior table cell.
    TableInterior(TableInteriorCell),
    /// Index leaf cell.
    IndexLeaf(IndexLeafCell),
    /// Interior index cell.
    IndexInterior(IndexInteriorCell),
}

impl BTreeCell {
    /// Calculates the size of the cell in bytes.
    pub fn size(&self) -> usize {
        match self {
            BTreeCell::TableLeaf(cell) => {
                // Calculate the varint size for the payload size and row_id (See varint module for details).
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                let rowid_size = crate::utils::varint_size(cell.row_id);

                varint_size
                    + rowid_size
                    + cell.payload.len()
                    + if cell.overflow_page.is_some() { 4 } else { 0 } // We add 4 bytes for the overflow page if it exists
            }
            BTreeCell::TableInterior(cell) => {
                4 + crate::utils::varint_size(cell.key) // Add 4 bytes for the left_child_page and varint size for the key
            }
            BTreeCell::IndexLeaf(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);

                varint_size + cell.payload.len() + if cell.overflow_page.is_some() { 4 } else { 0 }
            }
            BTreeCell::IndexInterior(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);

                4 + varint_size + cell.payload.len() + // 4 bytes for the left_child_page
                // Add the varint size for the payload
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            }
        }
    }
}

/// Represents a B-Tree page.
#[derive(Debug, Clone)]
pub struct BTreePage {
    /// Header of the B-Tree page.
    pub header: BTreePageHeader,
    /// SLot array (offsets to the cells).
    pub cell_indices: Vec<u16>,
    /// Cells stored in the page.
    pub cells: Vec<BTreeCell>,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
    /// Reserved space at the end of each page.
    pub reserved_space: u8,
}

impl BTreePage {
    /// Creates a new btree page
    /// # Parameters:
    /// * page_type - Type of the page (must be a leaf or interior type).
    /// * page_size - Size of the page in bytes.
    /// * page_number - Page number.
    /// * reserved_space - Reserved space at the end of each page.
    /// * right_most_page - Page number of the rightmost child (only for interior pages).
    /// # Errors
    /// Returns an error if the page type is not valid or if the right_most_page is not set for a leaf page.
    /// # Panics
    /// Panics if the page type is not valid or if the right_most_page is not set for a leaf page.
    pub fn new(
        page_type: PageType,
        page_size: u32,
        page_number: u32,
        reserved_space: u8,
        right_most_page: Option<u32>,
    ) -> io::Result<Self> {
        // Check if the page type is valid
        let header = if page_type.is_leaf() {
            if right_most_page.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "The right_most_page should not be set for leaf pages",
                ));
            }
            BTreePageHeader::new_leaf(page_type) // Create a new leaf page header
        } else if let Some(right_most) = right_most_page {
            BTreePageHeader::new_interior(page_type, right_most)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The right_most_page should be set for interior pages",
            ));
        };

        // Initialize the B-Tree page
        let mut page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size,
            page_number,
            reserved_space,
        };

        // Inicialize the content start offset
        page.update_content_start_offset();

        Ok(page)
    }

    /// Sets the content start offset based on the page size and reserved space.
    pub fn update_content_start_offset(&mut self) {
        self.header.content_start_offset = self.page_size as u16 - self.reserved_space as u16;
    }

    /// Addse a cell to the B-Tree page.
    /// # Parameters
    /// * `cell` - The cell to add to the page.
    /// # Errors
    /// Returns an error if the cell cannot be added due to insufficient space or if the cell type is incompatible with the page type.
    /// # Panics
    /// Panics if the cell type is incompatible with the page type.
    /// # Returns
    /// Returns the index of the cell in the slot array.
    /// # Notes
    /// The cell is added to the page and the content start offset is updated. T
    pub fn add_cell(&mut self, cell: BTreeCell) -> io::Result<u16> {
        // Verify the type compatibility
        // Check if the cell type is compatible with the page type
        match (&self.header.page_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {}
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {}
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {}
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {}
            _ => {
                // You cannot add a cell to an overflow page or free page
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Cell type incompatible with this page: {:?}",
                        self.header.page_type
                    ),
                ));
            }
        }

        // Compute the required space to store the cell
        let cell_size = cell.size();
        let cell_index_size = 2; // 2 additional bytes for the cell offset in the slot array

        // Compute the available space in the page
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * cell_index_size; // Current space occupied by indices
                                                                           // Total used space in the page. If we are at page 1 we need to add the header size, as the database header is stored on page 1.
        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
        } else {
            header_size + cell_indices_size
        };

        // Calculate the available space for the new cell
        // The content start offset is the point where the content starts (after the header and the slot array).
        let content_start = self.header.content_start_offset as usize;
        let available_space = content_start - used_space - cell_index_size; // Restar el espacio para el nuevo índice

        if cell_size > available_space {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Not enough bytes to store the cell: needed {} bytes, available {} bytes",
                    cell_size, available_space
                ),
            ));
        }

        // Update the content start offset
        self.header.content_start_offset -= cell_size as u16;

        // Append the cell index to the slot array
        self.cell_indices.push(self.header.content_start_offset);

        // Append the cell to the cells vector
        self.cells.push(cell);

        // Update the first free block offset
        self.header.cell_count += 1;

        Ok((self.cell_indices.len() - 1) as u16)
    }

    /// Returns the free space on the page .
    pub fn free_space(&self) -> usize {
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * 2; // 2 bytes per index

        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
        } else {
            header_size + cell_indices_size
        };

        let content_size = self.page_size as usize - self.header.content_start_offset as usize;

        self.page_size as usize - used_space - content_size - self.reserved_space as usize
    }
}

// Updated implementation for BTreePage
impl ByteSerializable for BTreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read the header
        let header = BTreePageHeader::read_from(reader)?;

        // Create a new BTreePage with default values
        let mut page = BTreePage {
            header: header.clone(),
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size: 0,      // Will be set later
            page_number: 0,    // Will be set later
            reserved_space: 0, // Will be set later
        };

        // Read cell indices
        let cell_count = header.cell_count as usize;
        let mut cell_indices = Vec::with_capacity(cell_count);

        for _ in 0..cell_count {
            let mut buffer = [0u8; 2];
            reader.read_exact(&mut buffer)?;
            let index = u16::from_be_bytes(buffer);
            cell_indices.push(index);
        }

        page.cell_indices = cell_indices;

        // Read the entire remaining data to have access to all cells
        let mut remaining_data = Vec::new();
        reader.read_to_end(&mut remaining_data)?;

        // Deserialize each cell using its offset
        let header_size = page.header.size();
        let indices_size = page.cell_indices.len() * 2; // 2 bytes per index
        let content_start = header_size + indices_size;

        for &cell_index in &page.cell_indices {
            // Calculate the actual offset in the remaining_data
            let cell_offset = cell_index as usize - content_start;

            if cell_offset >= remaining_data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cell offset out of range: {}", cell_offset),
                ));
            }

            // Create a cursor at the cell position
            let mut cell_cursor = Cursor::new(&remaining_data[cell_offset..]);

            // Deserialize the cell based on the page type
            let cell = match header.page_type {
                PageType::TableLeaf => {
                    // Read payload size
                    let (payload_size, payload_size_bytes) =
                        crate::utils::decode_varint(&mut cell_cursor)?;

                    // Read rowid
                    let (row_id, rowid_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;

                    // Calculate the position after reading varints
                    let header_bytes = payload_size_bytes + rowid_bytes;

                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);

                    let mut payload = vec![0u8; local_size];
                    let payload_start = cell_cursor.position() as usize;
                    payload.copy_from_slice(
                        &remaining_data[cell_offset + payload_start..][..local_size],
                    );

                    // Check if there's an overflow page (if payload doesn't fit)
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: payload_size as u64,
                        row_id,
                        payload,
                        overflow_page,
                    })
                }
                PageType::TableInterior => {
                    if cell_offset + 4 > remaining_data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Buffer too small to read interior cell",
                        ));
                    }

                    // Read left child page
                    let left_child_page = u32::from_be_bytes([
                        remaining_data[cell_offset],
                        remaining_data[cell_offset + 1],
                        remaining_data[cell_offset + 2],
                        remaining_data[cell_offset + 3],
                    ]);

                    // Read key
                    let mut key_cursor = Cursor::new(&remaining_data[cell_offset + 4..]);
                    let (key, _) = crate::utils::decode_varint(&mut key_cursor)?;

                    BTreeCell::TableInterior(TableInteriorCell {
                        left_child_page,
                        key,
                    })
                }
                PageType::IndexLeaf => {
                    // Read payload size
                    let (payload_size, payload_size_bytes) =
                        crate::utils::decode_varint(&mut cell_cursor)?;

                    // Calculate header bytes
                    let header_bytes = payload_size_bytes;

                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);

                    let mut payload = vec![0u8; local_size];
                    let payload_start = cell_cursor.position() as usize;
                    payload.copy_from_slice(
                        &remaining_data[cell_offset + payload_start..][..local_size],
                    );

                    // Check if there's an overflow page
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    BTreeCell::IndexLeaf(IndexLeafCell {
                        payload_size: payload_size as u64,
                        payload,
                        overflow_page,
                    })
                }
                PageType::IndexInterior => {
                    if cell_offset + 4 > remaining_data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Buffer too small to read interior cell",
                        ));
                    }

                    // Read left child page
                    let left_child_page = u32::from_be_bytes([
                        remaining_data[cell_offset],
                        remaining_data[cell_offset + 1],
                        remaining_data[cell_offset + 2],
                        remaining_data[cell_offset + 3],
                    ]);

                    // Read payload size
                    let mut payload_cursor = Cursor::new(&remaining_data[cell_offset + 4..]);
                    let (payload_size, payload_size_bytes) =
                        crate::utils::decode_varint(&mut payload_cursor)?;

                    // Calculate header bytes
                    let header_bytes = 4 + payload_size_bytes;

                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);

                    let mut payload = vec![0u8; local_size];
                    payload.copy_from_slice(
                        &remaining_data[cell_offset + header_bytes..][..local_size],
                    );

                    // Check if there's an overflow page
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    BTreeCell::IndexInterior(IndexInteriorCell {
                        left_child_page,
                        payload_size: payload_size as u64,
                        payload,
                        overflow_page,
                    })
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unexpected page type for B-Tree: {:?}", header.page_type),
                    ));
                }
            };

            page.cells.push(cell);
        }

        Ok(page)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write the header
        self.header.write_to(writer)?;

        // Write cell indices
        for &idx in &self.cell_indices {
            writer.write_all(&idx.to_be_bytes())?;
        }

        // Calculate the start of the content area
        let header_size = self.header.size();
        let indices_size = self.cell_indices.len() * 2; // 2 bytes per index
        let content_start = header_size + indices_size;

        // Create a buffer for the content area - we'll fill in cells at their specific offsets
        let mut content_buffer = vec![0u8; self.page_size as usize - content_start];

        // Write each cell to its position in the content buffer
        for (i, cell) in self.cells.iter().enumerate() {
            if i >= self.cell_indices.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Cell index out of range",
                ));
            }

            let cell_index = self.cell_indices[i] as usize;
            if cell_index < content_start || cell_index >= self.page_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid cell index: {}", cell_index),
                ));
            }

            let buffer_offset = cell_index - content_start;

            // We need to keep track of the size of the buffer.
            let buffer_size = content_buffer.len();

            // Create a cursor at the cell position
            let mut cell_cursor = Cursor::new(&mut content_buffer[buffer_offset..]);

            // Write the cell - note this doesn't include the cell type as it's determined by the page type
            match cell {
                BTreeCell::TableLeaf(leaf_cell) => {
                    // Write payload size
                    crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;

                    // Write rowid
                    crate::utils::encode_varint(leaf_cell.row_id, &mut cell_cursor)?;

                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size - buffer_offset - current_pos;
                    let payload_size = leaf_cell.payload.len().min(available_space);

                    if payload_size > 0 {
                        cell_cursor.write_all(&leaf_cell.payload[..payload_size])?;
                    }

                    // Write overflow page if present
                    if let Some(overflow_page) = leaf_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                }
                BTreeCell::TableInterior(interior_cell) => {
                    // Write left child page
                    cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;

                    // Write key
                    crate::utils::encode_varint(interior_cell.key, &mut cell_cursor)?;
                }
                BTreeCell::IndexLeaf(leaf_cell) => {
                    // Write payload size
                    crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;

                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size - buffer_offset - current_pos;
                    let payload_size = leaf_cell.payload.len().min(available_space);

                    if payload_size > 0 {
                        cell_cursor.write_all(&leaf_cell.payload[..payload_size])?;
                    }

                    // Write overflow page if present
                    if let Some(overflow_page) = leaf_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                }
                BTreeCell::IndexInterior(interior_cell) => {
                    // Write left child page
                    cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;

                    // Write payload size
                    crate::utils::encode_varint(
                        interior_cell.payload_size as i64,
                        &mut cell_cursor,
                    )?;

                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size - buffer_offset - current_pos;
                    let payload_size = interior_cell.payload.len().min(available_space);

                    if payload_size > 0 {
                        cell_cursor.write_all(&interior_cell.payload[..payload_size])?;
                    }

                    // Write overflow page if present
                    if let Some(overflow_page) = interior_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                }
            }
        }

        // Write the content buffer
        writer.write_all(&content_buffer)?;

        Ok(())
    }
}

/// Represents an overflow page.
#[derive(Debug, Clone)]
pub struct OverflowPage {
    /// Next overflow page number (0 if it is the last one). Overflow pages are linked together. in a linked list, allowing us to store super-large tuples.
    /// The first page is the one that stores the first part of the data.
    pub next_page: u32,
    /// Data stored in the overflow page.
    pub data: Vec<u8>,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
}

impl OverflowPage {
    /// Creates a new overflow page.
    ///
    /// # Parameters
    /// * `next_page` - Number of the next overflow page (0 if it is the last one), no one can point to page 0 as this page is not used.
    /// * `data` - Data to be stored in the overflow page.
    /// * `page_size` - Size of the page in bytes.
    /// * `page_number` - Page number.
    /// # Errors
    /// Returns an error if the data size exceeds the maximum page size.
    /// # Notes
    /// The maximum size of the data is limited by the page size minus 4 bytes for the next_page pointer.
    pub fn new(
        next_page: u32,
        data: Vec<u8>,
        page_size: u32,
        page_number: u32,
    ) -> io::Result<Self> {
        let max_data_size = page_size as usize - 13; // 4 bytes para next_page, 4 bytes for page_size, 4 bytes for page_number
        // The extra byte is for the page type.
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Data too big for the overflow page: {} bytes, max is {} bytes",
                    data.len(),
                    max_data_size
                ),
            ));
        }

        Ok(OverflowPage {
            next_page,
            data,
            page_size,
            page_number,
        })
    }
}

// Implementation for OverflowPage.
// Note that I am using fixed length integers for the page number and size.
// I prefer to use fixed length integers for page metadata to avoid
// issues with endianness and to make the code more readable. It also makes it much easier to serialize the whole page,
// as we can just read first the fixed length data and then fill the data vector with th rest of the buffer.
impl ByteSerializable for OverflowPage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read next page pointer
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let next_page = u32::from_be_bytes(buffer);

        // Read the page_size:
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let page_size = u32::from_be_bytes(buffer);
        //  println!("Page size: {}", page_size);
        // // Read the page_number:
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let page_number = u32::from_be_bytes(buffer);
        //   println!("Page number: {}", page_number);
        // Read data
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        // println!("Data size: {}", data.len());
        //  println!("Data: {:?}", data);

        // Create overflow page
        Ok(OverflowPage {
            next_page,
            data,
            page_size,
            page_number,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write next page pointer

        writer.write_all(&self.next_page.to_be_bytes())?;
        // Write page size
        writer.write_all(&self.page_size.to_be_bytes())?;
        // Write page number
        writer.write_all(&self.page_number.to_be_bytes())?;
        // println!("Writing overflow page: {:?}", self.data);

        // Write data
        writer.write_all(&self.data)?;

        println!("Overflow page data: {:?}", self.data);

        Ok(())
    }
}

/// Represents a free page in the database.
#[derive(Debug, Clone)]
pub struct FreePage {
    /// Next free page number (0 if it is the last one).
    /// Free pages are also a linked list. This mimics the behaviour of MMAP in modern operating systems.
    /// However on memory allocators, the free list are a double linked list.
    pub next_page: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
}

impl FreePage {
    /// Creates a new free page.
    ///
    /// # Parameters
    /// * `next_page` - Number of the next free page (0 if it is the last one).
    /// * `page_size` - Size of the page in bytes.
    /// * `page_number` - Page number or page id.
    pub fn new(next_page: u32, page_size: u32, page_number: u32) -> Self {
        FreePage {
            next_page,
            page_size,
            page_number,
        }
    }
}

// Implementation for FreePage
impl ByteSerializable for FreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read next page pointer
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let next_page = u32::from_be_bytes(buffer);

        // Read the page size
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let page_size = u32::from_be_bytes(buffer);

        // Read the page number
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let page_number = u32::from_be_bytes(buffer);

        // Create free page
        Ok(FreePage {
            next_page,
            page_size,
            page_number,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write next page pointer
        writer.write_all(&self.next_page.to_be_bytes())?;
        // Write page size
        writer.write_all(&self.page_size.to_be_bytes())?;
        // Write page number
        writer.write_all(&self.page_number.to_be_bytes())?;

        Ok(())
    }
}

/// Represents a generic page in the database of any type.
#[derive(Debug, Clone)]
pub enum Page {
    /// Btree page.
    BTree(BTreePage),
    /// Overflow page.
    Overflow(OverflowPage),
    /// Free page.
    Free(FreePage),
}

impl Page {
    /// Returns the page_number (Just a Java getter)
    pub fn page_number(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_number,
            Page::Overflow(page) => page.page_number,
            Page::Free(page) => page.page_number,
        }
    }
    /// Returns the page type.
    pub fn page_type(&self) -> PageType {
        match self {
            Page::BTree(page) => page.header.page_type,
            Page::Overflow(_) => PageType::Overflow,
            Page::Free(_) => PageType::Free,
        }
    }
    /// Returns tha page size.
    pub fn page_size(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_size,
            Page::Overflow(page) => page.page_size,
            Page::Free(page) => page.page_size,
        }
    }
}

// Implementation for Page enum
impl ByteSerializable for Page {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read the first byte to determine the page type
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;

        match buffer[0] {
            // B-Tree page types
            0x02 | 0x05 | 0x0A | 0x0D => {
                // Put back the first byte
                let mut combined_reader = Cursor::new(buffer.to_vec()).chain(reader);

                // Parse as B-Tree page
                let btree_page = BTreePage::read_from(&mut combined_reader)?;
                Ok(Page::BTree(btree_page))
            }
            // Overflow page
            0x10 => {
                // Skip the first byte and parse as Overflow page
                println!("Reading overflow page");
                let overflow_page = OverflowPage::read_from(reader)?;
                Ok(Page::Overflow(overflow_page))
            }
            // Free page
            0x00 => {
                let free_page = FreePage::read_from(reader)?;
                Ok(Page::Free(free_page))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page type: {}", buffer[0]),
            )),
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write the page type based on the enum variant
        match self {
            // No need to write the type, as it is already written in the header
            Page::BTree(_btree_page) => Ok(()), //
            Page::Overflow(_overflow_page) => writer.write_all(&[0x10]),
            Page::Free(_free_page) => writer.write_all(&[0x00]),
        }?;
        match self {
            Page::BTree(btree_page) => btree_page.write_to(writer),
            Page::Overflow(overflow_page) => overflow_page.write_to(writer),
            Page::Free(free_page) => free_page.write_to(writer),
        }
    }
}

// Implementations for withverting Page to BTreePage, OverflowPage and FreePage
impl From<Page> for BTreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot withvert to BTreePage: page is not of type BTree"),
        }
    }
}
// Implementation to withvert Page to OverflowPage and FreePage
impl From<Page> for OverflowPage {
    fn from(page: Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot withvert to OverflowPage: page is not of type Overflow"),
        }
    }
}
// Implementation to withvert from Page to FreePage
impl From<Page> for FreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot withvert to FreePage: page is not of type Free"),
        }
    }
}

// The same traits to be able to withvert to &BTreePage, &OverflowPage and &FreePage for &Page.
// This is useful to avoid copying the page when we only need a reference to it.
impl<'a> From<&'a Page> for &'a BTreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot withvert to &BTreePage: page is not of type BTree"),
        }
    }
}
// Same for a &mut reference
impl<'a> From<&'a mut Page> for &'a mut BTreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot withvert to &mut BTreePage: page is not of type BTree"),
        }
    }
}

// Same for OverflowPage and FreePage
impl<'a> From<&'a Page> for &'a OverflowPage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot withvert to &OverflowPage: page is not of type Overflow"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut OverflowPage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot withvert to &mut OverflowPage: page is not of type Overflow"),
        }
    }
}

impl<'a> From<&'a Page> for &'a FreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot withvert to &FreePage: page is not of type Free"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut FreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot withvert to &mut FreePage: page is not of type Free"),
        }
    }
}

/// Tests for the page module.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_type_from_byte() {
        assert_eq!(PageType::from_byte(0x02), Some(PageType::IndexInterior));
        assert_eq!(PageType::from_byte(0x05), Some(PageType::TableInterior));
        assert_eq!(PageType::from_byte(0x0A), Some(PageType::IndexLeaf));
        assert_eq!(PageType::from_byte(0x0D), Some(PageType::TableLeaf));
        assert_eq!(PageType::from_byte(0x10), Some(PageType::Overflow));
        assert_eq!(PageType::from_byte(0x00), Some(PageType::Free));
    }

    #[test]
    fn test_page_type_properties() {
        assert!(PageType::IndexInterior.is_interior());
        assert!(PageType::TableInterior.is_interior());
        assert!(!PageType::IndexLeaf.is_interior());
        assert!(!PageType::TableLeaf.is_interior());

        assert!(!PageType::IndexInterior.is_leaf());
        assert!(!PageType::TableInterior.is_leaf());
        assert!(PageType::IndexLeaf.is_leaf());
        assert!(PageType::TableLeaf.is_leaf());

        assert!(PageType::IndexInterior.is_index());
        assert!(PageType::IndexLeaf.is_index());
        assert!(!PageType::TableInterior.is_index());
        assert!(!PageType::TableLeaf.is_index());

        assert!(!PageType::IndexInterior.is_table());
        assert!(!PageType::IndexLeaf.is_table());
        assert!(PageType::TableInterior.is_table());
        assert!(PageType::TableLeaf.is_table());
    }

    #[test]
    fn test_btree_page_header_new_leaf() {
        let header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        assert_eq!(header.page_type, PageType::TableLeaf);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.right_most_page, None);
    }

    #[test]
    fn test_btree_page_header_new_interior() {
        let header = BTreePageHeader::new_interior(PageType::TableInterior, 42);
        assert_eq!(header.page_type, PageType::TableInterior);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.right_most_page, Some(42));
    }

    #[test]
    #[should_panic]
    fn test_btree_page_header_new_leaf_with_wrong_type() {
        BTreePageHeader::new_leaf(PageType::TableInterior);
    }

    #[test]
    #[should_panic]
    fn test_btree_page_header_new_interior_with_wrong_type() {
        BTreePageHeader::new_interior(PageType::TableLeaf, 42);
    }

    #[test]
    fn test_btree_page_header_size() {
        let leaf_header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        assert_eq!(leaf_header.size(), 8);

        let interior_header = BTreePageHeader::new_interior(PageType::TableInterior, 42);
        assert_eq!(interior_header.size(), 12);
    }

    #[test]
    fn test_btree_page_header_serialization() {
        // Test encabezado de página hoja
        let leaf_header = BTreePageHeader {
            page_type: PageType::TableLeaf,
            first_free_block_offset: 0x1234,
            cell_count: 42,
            content_start_offset: 0x5678,
            fragmented_free_bytes: 5,
            right_most_page: None,
        };

        let mut buffer = Vec::new();
        leaf_header.write_to(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 8);
        assert_eq!(buffer[0], PageType::TableLeaf as u8);

        let mut cursor = Cursor::new(buffer);
        let read_header = BTreePageHeader::read_from(&mut cursor).unwrap();

        assert_eq!(read_header.page_type, PageType::TableLeaf);
        assert_eq!(read_header.first_free_block_offset, 0x1234);
        assert_eq!(read_header.cell_count, 42);
        assert_eq!(read_header.content_start_offset, 0x5678);
        assert_eq!(read_header.fragmented_free_bytes, 5);
        assert_eq!(read_header.right_most_page, None);

        // Test the interior page header
        let interior_header = BTreePageHeader {
            page_type: PageType::TableInterior,
            first_free_block_offset: 0x1234,
            cell_count: 42,
            content_start_offset: 0x5678,
            fragmented_free_bytes: 5,
            right_most_page: Some(0x12345678),
        };

        let mut buffer = Vec::new();
        interior_header.write_to(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 12);
        assert_eq!(buffer[0], PageType::TableInterior as u8);

        let mut cursor = Cursor::new(buffer);
        let read_header = BTreePageHeader::read_from(&mut cursor).unwrap();

        assert_eq!(read_header.page_type, PageType::TableInterior);
        assert_eq!(read_header.first_free_block_offset, 0x1234);
        assert_eq!(read_header.cell_count, 42);
        assert_eq!(read_header.content_start_offset, 0x5678);
        assert_eq!(read_header.fragmented_free_bytes, 5);
        assert_eq!(read_header.right_most_page, Some(0x12345678));
    }

    #[test]
    fn test_btree_page_new() {
        // Create a leaf page
        let leaf_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        assert_eq!(leaf_page.header.page_type, PageType::TableLeaf);
        assert_eq!(leaf_page.header.right_most_page, None);
        assert_eq!(leaf_page.header.content_start_offset, 4096);
        assert_eq!(leaf_page.page_size, 4096);
        assert_eq!(leaf_page.page_number, 1);
        assert_eq!(leaf_page.reserved_space, 0);

        // Create an interior page
        let interior_page = BTreePage::new(PageType::TableInterior, 4096, 2, 0, Some(42)).unwrap();

        assert_eq!(interior_page.header.page_type, PageType::TableInterior);
        assert_eq!(interior_page.header.right_most_page, Some(42));
        assert_eq!(interior_page.header.content_start_offset, 4096);
        assert_eq!(interior_page.page_size, 4096);
        assert_eq!(interior_page.page_number, 2);
        assert_eq!(interior_page.reserved_space, 0);
    }

    #[test]
    fn test_btree_page_with_invalid_parameters() {
        // Attempt to create a leaf page with a right_most_page
        let result = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, Some(42));

        assert!(result.is_err());

        // Attempt to create an interior page with a right_most_page of None
        let result = BTreePage::new(PageType::TableInterior, 4096, 2, 0, None);

        assert!(result.is_err());
    }

    #[test]
    fn test_btree_page_free_space() {
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Verify the initial free space
        let header_size = page.header.size();
        let initial_free_space = 4096 - HEADER_SIZE - header_size;
        assert_eq!(page.free_space(), initial_free_space);

        // Create a dummy cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 1,
            payload: vec![0; 10],
            overflow_page: None,
        });

        // Add the cell and check the free space again
        page.add_cell(cell).unwrap();

        // Calculate the expected free space after adding the cell
        let varint_size = crate::utils::varint_size(10); // payload_size
        let rowid_size = crate::utils::varint_size(1); // row_id
        let payload_size = 10; // payload
        let cell_size = varint_size + rowid_size + payload_size;
        let cell_index_size = 2; // 2 bytes for cell index

        let expected_free_space = initial_free_space - cell_size - cell_index_size;
        assert_eq!(page.free_space(), expected_free_space);
    }

    #[test]
    fn test_overflow_page_new() {
        // Data that does not exceed the maximum size
        let data = vec![0; 4083]; // 4096 - 13 bytes
        let result = OverflowPage::new(0, data, 4096, 3);
        assert!(result.is_ok());

        // Data that exceeds the maximum size
        let data = vec![0; 4084]; // 1 byte more
        let result = OverflowPage::new(0, data, 4096, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_free_page_new() {
        let page = FreePage::new(42, 4096, 3);
        assert_eq!(page.next_page, 42);
        assert_eq!(page.page_size, 4096);
        assert_eq!(page.page_number, 3);
    }

    #[test]
    fn test_page_methods() {
        // Test Page::page_number
        let btree_page =
            Page::BTree(BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap());
        assert_eq!(btree_page.page_number(), 1);

        let overflow_page = Page::Overflow(OverflowPage::new(0, vec![0; 100], 4096, 2).unwrap());
        assert_eq!(overflow_page.page_number(), 2);

        let free_page = Page::Free(FreePage::new(0, 4096, 3));
        assert_eq!(free_page.page_number(), 3);

        // Test Page::page_size
        assert_eq!(btree_page.page_size(), 4096);
        assert_eq!(overflow_page.page_size(), 4096);
        assert_eq!(free_page.page_size(), 4096);
    }

    #[test]
    fn test_page_type_withversion() {
        // withvert bytes to PageType
        assert_eq!(PageType::from_byte(0x02), Some(PageType::IndexInterior));
        assert_eq!(PageType::from_byte(0x05), Some(PageType::TableInterior));
        assert_eq!(PageType::from_byte(0x0A), Some(PageType::IndexLeaf));
        assert_eq!(PageType::from_byte(0x0D), Some(PageType::TableLeaf));
        assert_eq!(PageType::from_byte(0x10), Some(PageType::Overflow));
        assert_eq!(PageType::from_byte(0x00), Some(PageType::Free));
        assert_eq!(PageType::from_byte(0xFF), None); // Valor inválido
    }

    #[test]
    fn test_btree_page_serialization() {
        // Create a BTreePage of type TableLeaf
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Add some cells
        let cell1 = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 1,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        let cell2 = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 5,
            row_id: 2,
            payload: vec![11, 12, 13, 14, 15],
            overflow_page: None,
        });

        // Add the cells to the page
        page.add_cell(cell1.clone()).unwrap();
        page.add_cell(cell2.clone()).unwrap();

        // Verify the state of the page before serialization
        assert_eq!(page.header.cell_count, 2);
        assert_eq!(page.cells.len(), 2);
        assert_eq!(page.cell_indices.len(), 2);

        // Serialize the page
        let mut buffer = Vec::new();
        page.write_to(&mut buffer).unwrap();

        // Verify that the buffer withtains data (not empty)
        assert!(!buffer.is_empty());

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = BTreePage::read_from(&mut cursor).unwrap();

        // Verify the state of the page after deserialization
        assert_eq!(read_page.header.page_type, PageType::TableLeaf);
        assert_eq!(read_page.header.cell_count, 2);
        assert_eq!(read_page.cells.len(), 2);
        assert_eq!(read_page.cell_indices.len(), 2);

        // Verify the cells
        match &read_page.cells[0] {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.payload_size, 10);
                assert_eq!(leaf_cell.row_id, 1);
                assert_eq!(leaf_cell.payload, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
                assert_eq!(leaf_cell.overflow_page, None);
            }
            _ => panic!("Expected TableLeaf"),
        }

        match &read_page.cells[1] {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.payload_size, 5);
                assert_eq!(leaf_cell.row_id, 2);
                assert_eq!(leaf_cell.payload, vec![11, 12, 13, 14, 15]);
                assert_eq!(leaf_cell.overflow_page, None);
            }
            _ => panic!("Expected TableLeaf"),
        }
    }

    #[test]
    fn test_btree_interior_page_serialization() {
        // Create a BTreePage of type TableInterior
        let mut page = BTreePage::new(
            PageType::TableInterior,
            4096,
            1,
            0,
            Some(0x12345678), // rightmost page
        )
        .unwrap();

        // Add some cells
        let cell1 = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 0x11111111,
            key: 100,
        });

        let cell2 = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 0x22222222,
            key: 200,
        });

        page.add_cell(cell1.clone()).unwrap();
        page.add_cell(cell2.clone()).unwrap();

        // Verify the state of the page before serialization
        assert_eq!(page.header.cell_count, 2);
        assert_eq!(page.cells.len(), 2);
        assert_eq!(page.cell_indices.len(), 2);
        assert_eq!(page.header.right_most_page, Some(0x12345678));

        // Serialize
        let mut buffer = Vec::new();
        page.write_to(&mut buffer).unwrap();

        // Verify that the buffer withtains data (not empty)
        assert!(!buffer.is_empty());

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = BTreePage::read_from(&mut cursor).unwrap();

        // Verify the state of the page after deserialization
        assert_eq!(read_page.header.page_type, PageType::TableInterior);
        assert_eq!(read_page.header.cell_count, 2);
        assert_eq!(read_page.cells.len(), 2);
        assert_eq!(read_page.cell_indices.len(), 2);
        assert_eq!(read_page.header.right_most_page, Some(0x12345678));

        // Verify the cells
        match &read_page.cells[0] {
            BTreeCell::TableInterior(interior_cell) => {
                assert_eq!(interior_cell.left_child_page, 0x11111111);
                assert_eq!(interior_cell.key, 100);
            }
            _ => panic!("Expected TableInterior"),
        }

        match &read_page.cells[1] {
            BTreeCell::TableInterior(interior_cell) => {
                assert_eq!(interior_cell.left_child_page, 0x22222222);
                assert_eq!(interior_cell.key, 200);
            }
            _ => panic!("Expected TableInterior"),
        }
    }

    #[test]
    fn test_overflow_page_serialization() {
        // Create a new overflow page
        let overflow_page = OverflowPage::new(
            0x12345678,                          // next_page
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], // data
            4096,                                // page_size
            2,                                   // page_number
        )
        .unwrap();

        // Serialize the page
        let mut buffer = Vec::new();
        overflow_page.write_to(&mut buffer).unwrap();

        // Verify that the buffer withtains data (not empty)
        assert!(!buffer.is_empty());

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = OverflowPage::read_from(&mut cursor).unwrap();

        // Verify the values
        assert_eq!(read_page.next_page, 0x12345678);
        assert_eq!(read_page.data, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_free_page_serialization() {
        // Create a new free page
        let free_page = FreePage::new(
            0x12345678, // next_page
            4096,       // page_size
            3,          // page_number
        );

        // Serialize the page
        let mut buffer = Vec::new();
        free_page.write_to(&mut buffer).unwrap();

        // Verify that the buffer withtains data (not empty)
        assert!(!buffer.is_empty());

        // Verify that the first 4 bytes correspond to the next_page
        assert_eq!(
            u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]),
            0x12345678
        );

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = FreePage::read_from(&mut cursor).unwrap();

        // Verify the values
        assert_eq!(read_page.next_page, 0x12345678);
    }

    #[test]
    fn test_page_enum_serialization() {
        // 1. Create a new BTreePage
        let mut btree_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Add a cell to the BTreePage
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        btree_page.add_cell(cell).unwrap();

        // Create Page::BTree
        let page_btree = Page::BTree(btree_page);

        // Serialize
        let mut buffer = Vec::new();
        page_btree.write_to(&mut buffer).unwrap();

        // Verify that the buffer withtains data (not empty)
        assert!(!buffer.is_empty());

        // Verify that the first byte is the correct page type
        assert_eq!(buffer[0], 0x0D); // TableLeaf

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = Page::read_from(&mut cursor).unwrap();

        // Verify the deserialized page
        match &read_page {
            Page::BTree(page) => {
                assert_eq!(page.header.page_type, PageType::TableLeaf);
                assert_eq!(page.header.cell_count, 1);
            }
            _ => panic!("Expected Page::BTree"),
        }

        // 2. Create a new Overflow
        let overflow_page = OverflowPage::new(
            0x12345678,                          // next_page
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], // data
            4096,                                // page_size
            2,                                   // page_number
        )
        .unwrap();

        // Create a Page::Overflow
        let page_overflow = Page::Overflow(overflow_page);

        //Serialize the page
        let mut buffer = Vec::new();
        page_overflow.write_to(&mut buffer).unwrap();

        //Verify the buffer is not empty
        assert!(!buffer.is_empty());

        //Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_overflow = Page::read_from(&mut cursor).unwrap();

        if let Page::Overflow(overflow_page) = read_overflow {
            assert_eq!(overflow_page.next_page, 0x12345678);
            assert_eq!(overflow_page.data, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        } else {
            panic!("Expected Page::Overflow");
        }

        // 3. Create aa página Free
        let free_page = FreePage::new(
            0x12345678, // next_page
            4096,       // page_size
            3,          // page_number
        );

        // Create a Page::Free
        let page_free = Page::Free(free_page);

        // Serialize the page
        let mut buffer = Vec::new();
        page_free.write_to(&mut buffer).unwrap();

        // Verify the buffer is not empty
        assert!(!buffer.is_empty());

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_free = Page::read_from(&mut cursor).unwrap();

        if let Page::Free(free_page) = read_free {
            assert_eq!(free_page.next_page, 0x12345678);
        } else {
            panic!("Expected Page::Free");
        }
    }

    #[test]
    fn test_page_from_withversions() {
        // Create sample pages
        let btree_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        let overflow_page = OverflowPage::new(0x12345678, vec![1, 2, 3, 4, 5], 4096, 2).unwrap();

        let free_page = FreePage::new(0x12345678, 4096, 3);

        // Create Page enums
        let page_btree = Page::BTree(btree_page.clone());
        let page_overflow = Page::Overflow(overflow_page.clone());
        let page_free = Page::Free(free_page.clone());

        // Attempt withversions from Page to BTreePage, OverflowPage and FreePage
        let btree_ref: &BTreePage = (&page_btree).into();
        let overflow_ref: &OverflowPage = (&page_overflow).into();
        let free_ref: &FreePage = (&page_free).into();

        assert_eq!(btree_ref.page_number, 1);
        assert_eq!(overflow_ref.page_number, 2);
        assert_eq!(free_ref.page_number, 3);

        // Attempt withversions from Page to owned types
        let btree_owned: BTreePage = page_btree.into();
        let overflow_owned: OverflowPage = page_overflow.into();
        let free_owned: FreePage = page_free.into();

        assert_eq!(btree_owned.page_number, 1);
        assert_eq!(overflow_owned.page_number, 2);
        assert_eq!(free_owned.page_number, 3);
    }

    #[test]
    #[should_panic(expected = "Cannot withvert to BTreePage: page is not of type BTree")]
    fn test_invalid_page_withversion_btree() {
        let free_page = FreePage::new(0, 4096, 1);
        let page = Page::Free(free_page);

        // This withversion should panic
        let _: BTreePage = page.into();
    }

    #[test]
    #[should_panic(expected = "Cannot withvert to OverflowPage: page is not of type Overflow")]
    fn test_invalid_page_withversion_overflow() {
        let free_page = FreePage::new(0, 4096, 1);
        let page = Page::Free(free_page);

        // This withversion should panic
        let _: OverflowPage = page.into();
    }

    #[test]
    #[should_panic(expected = "Cannot withvert to FreePage: page is not of type Free")]
    fn test_invalid_page_withversion_free() {
        let btree_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();
        let page = Page::BTree(btree_page);

        // This withversion should panic
        let _: FreePage = page.into();
    }
    #[test]
    fn test_btree_page_add_cell_validation() {
        // Create a new BTreePage of type TableLeaf
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Attempt to add a cell of the wrong type (TableInterior)
        let wrong_cell = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 0x12345678,
            key: 42,
        });

        let result = page.add_cell(wrong_cell);
        assert!(result.is_err());

        // Verify the error message
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Cell type incompatible with this page"));

        // Create a correct cell for the page type
        let correct_cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        // Add the correct cell
        let result = page.add_cell(correct_cell);
        assert!(result.is_ok());
    }

    #[test]
    fn test_btree_page_with_reserved_space() {
        // Create a  BTreePage with reserved space
        let reserved_space = 100;
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, reserved_space, None).unwrap();

        // Verify the initial content_start_offset
        assert_eq!(
            page.header.content_start_offset,
            4096 - reserved_space as u16
        );

        // Add a cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        page.add_cell(cell).unwrap();

        // Serialize the page
        let mut buffer = Vec::new();
        page.write_to(&mut buffer).unwrap();

        // Verify the size of the buffer
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 4096);

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = BTreePage::read_from(&mut cursor).unwrap();

        // Verify the content_start_offset
        assert_eq!(
            read_page.header.content_start_offset,
            page.header.content_start_offset
        );
    }

    #[test]
    fn test_multiple_page_roundtrip_serialization() {
        // Create pages of all types

        // 1. BTree TableLeaf
        let mut table_leaf = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Add a cell
        let leaf_cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        table_leaf.add_cell(leaf_cell).unwrap();

        // 2. BTree TableInterior
        let mut table_interior =
            BTreePage::new(PageType::TableInterior, 4096, 2, 0, Some(0x12345678)).unwrap();

        // Add a cell
        let interior_cell = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 0x11111111,
            key: 100,
        });

        table_interior.add_cell(interior_cell).unwrap();

        // 3. BTree IndexLeaf
        let mut index_leaf = BTreePage::new(PageType::IndexLeaf, 4096, 3, 0, None).unwrap();

        // Add a cell
        let index_leaf_cell = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        index_leaf.add_cell(index_leaf_cell).unwrap();

        // 4. BTree IndexInterior
        let mut index_interior =
            BTreePage::new(PageType::IndexInterior, 4096, 4, 0, Some(0x87654321)).unwrap();

        // Add a cell
        let index_interior_cell = BTreeCell::IndexInterior(IndexInteriorCell {
            left_child_page: 0x22222222,
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        index_interior.add_cell(index_interior_cell).unwrap();

        // 5. Overflow Page
        let overflow = OverflowPage::new(1, vec![1, 2, 3, 4, 5], 4096, 5).unwrap();

        // 6. Free Page
        let free = FreePage::new(1, 4096, 6);

        // Enums Page
        let pages = vec![
            Page::BTree(table_leaf),
            Page::BTree(table_interior),
            Page::BTree(index_leaf),
            Page::BTree(index_interior),
            Page::Overflow(overflow),
            Page::Free(free),
        ];

        // Serialize and deserialize
        for (i, page) in pages.iter().enumerate() {
            let mut buffer = Vec::new();
            page.write_to(&mut buffer).unwrap();
            if i == 4 {
                println!("Page 4: {:?}", page);
            }
            // Verify the buffer is not empty
            assert!(!buffer.is_empty());

            // Deserialize the page
            let mut cursor = Cursor::new(&buffer[..]);
            let read_page = Page::read_from(&mut cursor).unwrap();
            if i == 4 {
                println!("Read Page 4: {:?}", read_page);
            }
            // Verify the type of the read page matches the original
            match (page, &read_page) {
                (Page::BTree(_), Page::BTree(_)) => {}
                (Page::Overflow(_), Page::Overflow(_)) => {}
                (Page::Free(_), Page::Free(_)) => {}
                _ => panic!("Tipo de página incorrecto para el índice {}", i),
            }

            // Verify specific properties of each page type
            match &read_page {
                Page::BTree(btree_page) => match i {
                    0 => assert_eq!(btree_page.header.page_type, PageType::TableLeaf),
                    1 => assert_eq!(btree_page.header.page_type, PageType::TableInterior),
                    2 => assert_eq!(btree_page.header.page_type, PageType::IndexLeaf),
                    3 => assert_eq!(btree_page.header.page_type, PageType::IndexInterior),
                    _ => panic!("Índice inesperado"),
                },
                Page::Overflow(_) => assert_eq!(i, 4),
                Page::Free(_) => assert_eq!(i, 5),
            }
        }
    }

    #[test]
    fn test_page_getters() {
        // Create Pages of different types
        let btree_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        let overflow_page = OverflowPage::new(0, vec![1, 2, 3], 4096, 2).unwrap();

        let free_page = FreePage::new(0, 4096, 3);

        // Create Page enums
        let page_btree = Page::BTree(btree_page);
        let page_overflow = Page::Overflow(overflow_page);
        let page_free = Page::Free(free_page);

        // Test the method page_number()
        assert_eq!(page_btree.page_number(), 1);
        assert_eq!(page_overflow.page_number(), 2);
        assert_eq!(page_free.page_number(), 3);

        // Test the method page_size()
        assert_eq!(page_btree.page_size(), 4096);
        assert_eq!(page_overflow.page_size(), 4096);
        assert_eq!(page_free.page_size(), 4096);
    }

    #[test]
    fn test_btree_page_with_sqlite_values() {
        use crate::utils::serialization::{serialize_values, SqliteValue};

        // Create a new BTreePage of type TableLeaf
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // Create a set of SQLite values
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello, SQLite!".to_string()),
            SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            SqliteValue::Float(std::f64::consts::PI),
            SqliteValue::Null,
        ];

        // Serialize the values into a payload
        let mut payload = Vec::new();
        serialize_values(&values, &mut payload).unwrap();

        // Create a cell with the payload
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: payload.len() as u64,
            row_id: 1,
            payload: payload.clone(),
            overflow_page: None,
        });

        // Add the cell to the page
        page.add_cell(cell).unwrap();

        // Serialize the page
        let mut buffer = Vec::new();
        page.write_to(&mut buffer).unwrap();

        // Deserialize the page
        let mut cursor = Cursor::new(buffer);
        let read_page = BTreePage::read_from(&mut cursor).unwrap();

        // Verify the cell was added correctly
        assert_eq!(read_page.cells.len(), 1);

        match &read_page.cells[0] {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.row_id, 1);
                assert_eq!(leaf_cell.payload_size as usize, payload.len());
                assert_eq!(leaf_cell.payload, payload);
            }
            _ => panic!("Expected TableLeaf"),
        }
    }

    #[test]
    fn test_btree_cell_size() {
        // Test TableLeaf wihout overflow
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        // Size = varint(payload_size) + varint(row_id) + payload.len()
        let expected_size = crate::utils::varint_size(10) + crate::utils::varint_size(42) + 10;
        assert_eq!(cell.size(), expected_size);

        // Test TableLeaf with overflow
        let cell_with_overflow = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: Some(0x12345678),
        });

        // Size = varint(payload_size) + varint(row_id) + payload.len() + 4 (overflow_page)
        let expected_size = crate::utils::varint_size(10) + crate::utils::varint_size(42) + 10 + 4;
        assert_eq!(cell_with_overflow.size(), expected_size);

        // Test TableInterior
        let interior_cell = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 0x12345678,
            key: 42,
        });

        // Size = 4 (left_child_page) + varint(key)
        let expected_size = 4 + crate::utils::varint_size(42);
        assert_eq!(interior_cell.size(), expected_size);

        // Test IndexLeaf without overflow
        let index_leaf = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        // Size = varint(payload_size) + payload.len()
        let expected_size = crate::utils::varint_size(10) + 10;
        assert_eq!(index_leaf.size(), expected_size);

        // Test IndexLeaf with overflow
        let index_leaf_overflow = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: Some(0x12345678),
        });

        // Size = varint(payload_size) + payload.len() + 4 (overflow_page)
        let expected_size = crate::utils::varint_size(10) + 10 + 4;
        assert_eq!(index_leaf_overflow.size(), expected_size);

        // Test IndexInterior without overflow
        let index_interior = BTreeCell::IndexInterior(IndexInteriorCell {
            left_child_page: 0x12345678,
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        // Size = 4 (left_child_page) + varint(payload_size) + payload.len()
        let expected_size = 4 + crate::utils::varint_size(10) + 10;
        assert_eq!(index_interior.size(), expected_size);

        // Test IndexInterior with overflow
        let index_interior_overflow = BTreeCell::IndexInterior(IndexInteriorCell {
            left_child_page: 0x12345678,
            payload_size: 10,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: Some(0x12345678),
        });

        // Size = 4 (left_child_page) + varint(payload_size) + payload.len() + 4 (overflow_page)
        let expected_size = 4 + crate::utils::varint_size(10) + 10 + 4;
        assert_eq!(index_interior_overflow.size(), expected_size);
    }

    #[test]
    fn test_overflow_page_with_large_data() {
        // Create a new OverflowPage with data that does not exceed the maximum size
        let max_data_size = 4096 - 13; // 4096 (size) - 4 (next_page)
        let data = vec![0; max_data_size];

        let page_result = OverflowPage::new(0, data.clone(), 4096, 1);
        assert!(page_result.is_ok());

        let page = page_result.unwrap();
        assert_eq!(page.data.len(), max_data_size);

        // Attempt to create a page with data that exceeds the maximum size
        let too_large_data = vec![0; max_data_size + 1];

        let page_result = OverflowPage::new(0, too_large_data, 4096, 1);
        assert!(page_result.is_err());

        // Verify the error message
        let error = page_result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Data too big for the overflow page"));
    }

    #[test]
    fn test_btree_page_update_content_start_offset() {
        // Create a new empty BTreePage of type TableLeaf
        let mut page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        // THe content_start_offset should be initialized to the page size
        assert_eq!(page.header.content_start_offset, 4096);

        // Simulate adding a cell
        page.update_content_start_offset();

        // EThe offset must be the same as the page size since no cells have been added yet
        assert_eq!(page.header.content_start_offset, 4096);

        // Attempt reserved_space
        let mut page_with_reserved = BTreePage::new(
            PageType::TableLeaf,
            4096,
            1,
            100, // 100 bytes
            None,
        )
        .unwrap();

        // The content_start_offset must be adjusted to account for reserved space
        assert_eq!(page_with_reserved.header.content_start_offset, 4096 - 100);

        // Update and verify the outcome
        page_with_reserved.update_content_start_offset();
        assert_eq!(page_with_reserved.header.content_start_offset, 4096 - 100);
    }

    #[test]
    fn test_page_number_and_size_getters() {
        // Create Pages of different types
        let btree_page = BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap();

        let overflow_page = OverflowPage::new(0, vec![1, 2, 3], 4096, 2).unwrap();

        let free_page = FreePage::new(0, 4096, 3);

        // Create Page enums
        let page_btree = Page::BTree(btree_page);
        let page_overflow = Page::Overflow(overflow_page);
        let page_free = Page::Free(free_page);

        // Test the method page_number()
        assert_eq!(page_btree.page_number(), 1);
        assert_eq!(page_overflow.page_number(), 2);
        assert_eq!(page_free.page_number(), 3);

        // Test the method page_size()
        assert_eq!(page_btree.page_size(), 4096);
        assert_eq!(page_overflow.page_size(), 4096);
        assert_eq!(page_free.page_size(), 4096);
    }
}
