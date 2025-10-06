use crate::cell::Cell;
use crate::types::{
    byte::{FALSE, TRUE},
    Byte,
};
use crate::Serializable;
use crate::SLOT_SIZE;
use std::io::{self, Read, Write};

/// Compute the total size that a new cell will occupy, considering the size of a single slot
pub(crate) fn cell_append_size<C: Cell>(cell: &C) -> usize {
    SLOT_SIZE + cell.size()
}

/// 3 byte data structure consisting of an offset in the page where the given cell is and a delete marker.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Slot {
    /// Offset in the page.
    pub(crate) offset: u16,
    /// Delete marker.
    pub(crate) delete_marker: Byte,
}

impl Slot {
    pub(crate) fn new(offset: u16) -> Self {
        Self {
            offset,
            delete_marker: FALSE,
        }
    }

    pub(crate) fn set_offset(&mut self, offset: u16) {
        self.offset = offset
    }

    pub(crate) fn delete(&mut self) {
        self.delete_marker = TRUE
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.delete_marker == TRUE
    }
}

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset && self.delete_marker == other.delete_marker
    }
}

impl Eq for Slot {}

impl Serializable for Slot {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer)?;
        let offset = u16::from_be_bytes(buffer);
        let delete_marker = Byte::read_from(reader)?;
        Ok(Self {
            offset,
            delete_marker,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.offset.to_be_bytes())?;
        self.delete_marker.write_to(writer)?;
        Ok(())
    }
}
