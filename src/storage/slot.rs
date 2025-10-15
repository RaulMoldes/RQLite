use crate::cell::Cell;
use crate::Serializable;
use crate::SLOT_SIZE;
use std::io::{self, Read, Write};

/// Compute the total size that a new cell will occupy, considering the size of a single slot
pub(crate) fn cell_append_size<C: Cell>(cell: &C) -> u16 {
    SLOT_SIZE + cell.size()
}

/// 3 byte data structure consisting of an offset in the page where the given cell is and a delete marker.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Slot {
    /// Offset in the page.
    pub(crate) offset: u16,
}

impl Slot {
    pub(crate) fn new(offset: u16) -> Self {
        Self { offset }
    }

    pub(crate) fn set_offset(&mut self, offset: u16) {
        self.offset = offset
    }
}

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
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

        Ok(Self { offset })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.offset.to_be_bytes())?;

        Ok(())
    }
}
