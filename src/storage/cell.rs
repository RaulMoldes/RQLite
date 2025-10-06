use crate::serialization::Serializable;
use crate::types::byte::{Byte, FALSE, TRUE};
use crate::types::varlena::VarlenaType;
use crate::types::PageId;
use crate::types::RQLiteType;
use crate::types::RowId;
use std::fmt::Debug;
use std::io::{self, Read, Write};

fn has_overflow<R: Read>(reader: &mut R) -> io::Result<bool> {
    let byte = Byte::read_from(reader)?;
    Ok(byte == TRUE)
}

/// Trait for all cells to implement
pub trait Cell: Clone + Send + Sync {
    fn size(&self) -> usize;
}

/// Each cell on a B-Tree page can be of different types.
/// Table leaf cells is where the actual data is stored.
#[derive(Debug, Clone)]
pub struct TableLeafCell {
    /// Physical row_id (row identifier).
    pub row_id: RowId,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: VarlenaType,
    /// Pointer to the overflow page that stores the rest of the data if it does not fit in this page.
    pub overflow_page: Option<PageId>,
}

impl TableLeafCell {
    /// Setter for the overflow page pointer for a table leaf cell.
    pub(crate) fn set_overflow(&mut self, id: PageId) {
        self.overflow_page = Some(id);
    }
}

impl Cell for TableLeafCell {
    fn size(&self) -> usize {
        // The overflow page pointer if only serialized if set. If not set, we avoid serializing it.
        // Therefore the size of a cell is variable, depending on it having an overflow page to point to.
        let overflow_size = if let Some(page) = self.overflow_page {
            page.size_of()
        } else {
            0
        };

        self.row_id.size_of() + self.payload.size_of() + overflow_size + TRUE.size_of()
    }
}

impl Serializable for TableLeafCell {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write flags byte.
        // It is better to use a byte marker than serializing an invalid overflow page,
        // Cells can be of variable size, and 1 byte <<< 4 bytes (size of a page id).
        let flag = if self.overflow_page.is_some() {
            TRUE
        } else {
            FALSE
        };
        flag.write_to(writer)?;

        // Write cell content
        self.row_id.write_to(writer)?;

        // Write payload as a chunk of bytes.
        self.payload.write_to(writer)?;

        // Write the overflow page if present
        if let Some(overflow_page) = self.overflow_page {
            overflow_page.write_to(writer)?;
        }

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let has_overflow = has_overflow(reader)?;

        let row_id = RowId::read_from(reader)?;

        let payload = VarlenaType::read_from(reader)?;

        // Read overflow page if flag is set
        let overflow_page = if has_overflow {
            Some(PageId::read_from(reader)?)
        } else {
            None
        };

        Ok(TableLeafCell {
            row_id,
            payload,
            overflow_page,
        })
    }
}

/// Table interior cells are used to store the keys that define the boundaries between child pages.
#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    /// Page_number (pointer) to the left_child.
    pub left_child_page: PageId,
    /// Key that defines the boundary between the left and right child.
    pub key: RowId, // RowId
}

impl Cell for TableInteriorCell {
    fn size(&self) -> usize {
        self.left_child_page.size_of() + self.key.size_of()
    }
}

impl Serializable for TableInteriorCell {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.left_child_page.write_to(writer)?;
        self.key.write_to(writer)?;

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // Read left_child_page
        let left_child_page = PageId::read_from(reader)?;
        // Read key
        let key = RowId::read_from(reader)?;

        Ok(TableInteriorCell {
            left_child_page,
            key,
        })
    }
}

impl TableInteriorCell {
    pub fn new(left_child_page: PageId, key: RowId) -> Self {
        Self {
            left_child_page,
            key,
        }
    }
}

/// Each cell in a B-Tree index leaf page contains a payload and a rowid.
#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    /// Payload content in bytes (Index Key)
    pub payload: VarlenaType,
    /// References to the page of overflow
    pub overflow_page: Option<PageId>,
}

impl Cell for IndexLeafCell {
    fn size(&self) -> usize {
        let overflow_size = if let Some(page) = self.overflow_page {
            page.size_of()
        } else {
            0
        };

        TRUE.size_of() + self.payload.size_of() + overflow_size
    }
}

impl Serializable for IndexLeafCell {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write flags byte (bit 0 = has overflow)
        let flags = if self.overflow_page.is_some() {
            TRUE
        } else {
            FALSE
        };
        flags.write_to(writer)?;

        // Write payload
        self.payload.write_to(writer)?;

        // Write overflow page if present
        if let Some(overflow_page) = self.overflow_page {
            overflow_page.write_to(writer)?;
        }

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // Read flags
        let has_overflow = has_overflow(reader)?;

        // Read payload
        let payload = VarlenaType::read_from(reader)?;

        // Read overflow page if flag is set
        let overflow_page = if has_overflow {
            Some(PageId::read_from(reader)?)
        } else {
            None
        };

        Ok(IndexLeafCell {
            payload,
            overflow_page,
        })
    }
}

impl IndexLeafCell {
    pub fn new(payload: VarlenaType) -> Self {
        Self {
            payload,
            overflow_page: None,
        }
    }

    pub(crate) fn set_overflow(&mut self, id: PageId) {
        self.overflow_page = Some(id);
    }
}

/// Each cell in a B-Tree index interior page contains a pointer to the left child and a key.
#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    /// Page_number (pointer) to the left child.
    pub left_child_page: PageId,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: VarlenaType,
    /// References to the page of overflow (if the payload does not fit in this page).
    pub overflow_page: Option<PageId>,
}

impl Cell for IndexInteriorCell {
    fn size(&self) -> usize {
        let overflow_size = if let Some(overflow_page) = self.overflow_page {
            overflow_page.size_of()
        } else {
            0
        };

        TRUE.size_of() // flags byte
            + self.left_child_page.size_of() // left_child_page
            + self.payload.size_of()
            + overflow_size
    }
}

impl IndexInteriorCell {
    pub(crate) fn set_overflow(&mut self, id: PageId) {
        self.overflow_page = Some(id);
    }
}

impl Serializable for IndexInteriorCell {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write flag byte
        let flag = if self.overflow_page.is_some() {
            TRUE
        } else {
            FALSE
        };
        flag.write_to(writer)?;

        // Write left_child_page and payload size
        self.left_child_page.write_to(writer)?;
        self.payload.write_to(writer)?;

        // Write overflow page if present
        if let Some(overflow_page) = self.overflow_page {
            overflow_page.write_to(writer)?;
        }

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // Read flag
        let has_overflow = has_overflow(reader)?;

        // Read left_child_page
        let left_child_page = PageId::read_from(reader)?;

        // Read payload
        let payload = VarlenaType::read_from(reader)?;

        // Read overflow page if flag is set
        let overflow_page = if has_overflow {
            Some(PageId::read_from(reader)?)
        } else {
            None
        };

        Ok(IndexInteriorCell {
            left_child_page,
            payload,
            overflow_page,
        })
    }
}

impl IndexInteriorCell {
    pub fn new(left_child_page: PageId, payload: VarlenaType) -> Self {
        Self {
            left_child_page,
            payload,
            overflow_page: None,
        }
    }
}
