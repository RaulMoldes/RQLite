use crate::cell::Cell;
use crate::page_header::PageHeader;
use crate::page_header::PageType;
use crate::serialization::Serializable;
use crate::storage::{cell_append_size, Slot};
use crate::types::PageId;
use crate::HeaderOps;
use crate::PAGE_HEADER_SIZE;
use crate::SLOT_SIZE;
use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};

/// Trait for functions common to all BTreePages.
pub(crate) trait BTreePageOps<BTreeCellType: Cell> {
    /// Get a cell at a specific position in the slot array
    fn get_cell_at(&self, id: u16) -> Option<BTreeCellType>;

    /// Append a cell to this btreepage.
    fn add_cell(&mut self, cell: BTreeCellType) -> io::Result<u16>;

    /// Take the cells inside this page.
    fn take_cells(&mut self) -> Vec<BTreeCellType>;
}

#[derive(Clone)]
pub(crate) struct BTreePage<BTreeCellType: Cell> {
    /// Header of the page containing page metadata.
    pub(crate) header: PageHeader,
    /// An array of slots representing cell offsets in the page and their status (deleted or not)
    /// Each slot is deliberately 3 bytes. Therefore if we know how many slots we have [`cell_count()`], we can iterate over the slot array and lookup where each cell is in the BtreeMap.
    /// If the cell slot is marked as deleted, we can simply skip it and go to the next.
    pub(crate) cell_indices: Vec<Slot>,
    /// The cells btreemap is a map of offset -> cell.
    /// Therefore whenever you want to get a cell from the page the process is the following:
    /// Cells can be identified either by a [`RowId`] or by their physical identifier.
    /// The physical identifier is nothing more than the [`PageId`] + position in the slot array.
    /// This allows to uniquely identify a cell in the database.
    pub(crate) cells: BTreeMap<u16, BTreeCellType>,
}

/// All pages must implement [HeaderOps], as all pages have a Header.
impl<BTreeCellType: Cell + Serializable> HeaderOps for BTreePage<BTreeCellType> {
    fn cell_count(&self) -> usize {
        self.header.cell_count()
    }

    fn content_start(&self) -> usize {
        self.header.content_start()
    }

    fn free_space(&self) -> usize {
        self.header.free_space()
    }

    fn id(&self) -> crate::types::PageId {
        self.header.id()
    }

    fn is_overflow(&self) -> bool {
        self.header.is_overflow()
    }

    fn page_size(&self) -> usize {
        self.header.page_size()
    }

    fn type_of(&self) -> super::PageType {
        self.header.type_of()
    }

    fn get_next_overflow(&self) -> Option<crate::types::PageId> {
        self.header.get_next_overflow()
    }

    fn set_next_overflow(&mut self, overflowpage: PageId) {
        self.header.set_next_overflow(overflowpage);
    }

    fn set_type(&mut self, page_type: super::PageType) {
        self.header.set_type(page_type);
    }

    fn free_space_start(&self) -> usize {
        self.header.free_space_start()
    }
}

impl<BTreeCellType: Cell + Serializable> Default for BTreePage<BTreeCellType> {
    fn default() -> Self {
        BTreePage {
            header: PageHeader::default(),
            cell_indices: Vec::new(),
            cells: BTreeMap::new(),
        }
    }
}

impl<BTreeCellType: Cell + Serializable> BTreePageOps<BTreeCellType> for BTreePage<BTreeCellType> {
    /// Logic to get a cell at a specific position.
    /// As said, the position in the slot array is the canonical way to get a cell in a specific [BTreePage].
    /// So, we go get the cell offset first, then lookup in the btreemap to get the cell.
    /// If the slot has been marked as deleted, we simply ignore it.
    fn get_cell_at(&self, id: u16) -> Option<BTreeCellType> {
        if let Some(slot) = self.cell_indices.get(id as usize) {
            return self.cells.get(&slot.offset).cloned();
        };
        None
    }

    // BTreeMap does not include a drain method, so we need to use std::mem::take here.
    // TODO: currently this does not check if cells are deleted or not. We need to implement that part manually.
    fn take_cells(&mut self) -> Vec<BTreeCellType> {
        let old_cells = std::mem::take(&mut self.cells);
        self.cell_indices.clear();
        self.header.content_start_ptr = PAGE_HEADER_SIZE as u16;
        self.header.free_space_ptr = self.page_size() as u16;
        self.header.cell_count = 0;
        old_cells.into_values().collect()
    }

    /// Add a cell, returning the offset at which was inserted.
    fn add_cell(&mut self, cell: BTreeCellType) -> io::Result<u16> {
        // The cell append size is the total cell size + SLOT_SIZE.
        let cell_size = cell_append_size(&cell);
        let free_space = self.free_space();

        // Simple check to validate that there is enough space for this cell.
        if free_space < cell_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Not enough bytes to store the cell: needed {} bytes, available {} bytes",
                    cell_size, free_space
                ),
            ));
        }

        // Decrement the free space ptr and increment the start-of-content ptr.
        self.header.free_space_ptr -= cell.size() as u16;
        self.header.content_start_ptr += SLOT_SIZE as u16;
        // Increment the cell count.
        self.header.cell_count += 1;
        // Finally insert the cell at the offset in the btreemap
        self.cells.insert(self.free_space_start() as u16, cell);
        // Return the location to where the cell is offseted.
        Ok(self.free_space_start() as u16)
    }
}

impl<C: Cell + Serializable> Serializable for BTreePage<C> {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // First, deserialize the header.
        let header = PageHeader::read_from(reader)?;

        let mut cell_indices = Vec::with_capacity(header.cell_count());

        // Deserialize all slots.
        for _ in 0..header.cell_count() {
            let index = Slot::read_from(reader)?;
            cell_indices.push(index);
        }

        // Calculate how many bytes to read
        let content_start = header.content_start();
        let remaining_bytes = header.page_size() - content_start;
        dbg!(remaining_bytes);
        // Read exactly the remaining bytes for the page into a temp buffer.
        let mut remaining_data = vec![0u8; remaining_bytes];
        reader.read_exact(&mut remaining_data)?;
        dbg!(header.cell_count());
    dbg!(header.content_start());
    dbg!(header.page_size());
    dbg!(remaining_bytes);


        let mut cells = BTreeMap::new();
        for &cell_index in cell_indices.iter() {
              dbg!(cell_index.offset);
            let cell_offset = cell_index.offset as usize - header.content_start();
            dbg!(cell_offset);
    dbg!(remaining_data.len());
            if cell_offset >= remaining_data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cell offset out of range: {}", cell_offset),
                ));
            }

            let mut cell_cursor = Cursor::new(&remaining_data[cell_offset..]);
            let cell = C::read_from(&mut cell_cursor)?;
            cells.insert(cell_index.offset, cell);
        }

        Ok(BTreePage {
            header,
            cell_indices,
            cells,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;

        for &idx in &self.cell_indices {
            idx.write_to(writer)?;
        }
        let buffer_size = self.page_size() - self.content_start();
        dbg!(buffer_size);
        let mut content_buffer = vec![0u8; buffer_size];

        for (offset, cell) in self.cells.iter() {
            let buffer_offset = *offset as usize - self.content_start();

            let mut cell_cursor = Cursor::new(&mut content_buffer[buffer_offset..]);
            cell.write_to(&mut cell_cursor)?;
        }

        writer.write_all(&content_buffer)?;
        Ok(())
    }
}

impl<C: Cell> std::fmt::Debug for BTreePage<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("Page");
        dbg.field("header", &self.header)
            .field("cell_indices_len", &self.cell_indices.len())
            .field("cells_len", &self.cells.len())
            .finish()?;

        writeln!(f, "Free space pointer: {:?}", self.header.free_space_ptr)?;
        writeln!(
            f,
            "Content start pointer: {:?}",
            self.header.content_start_ptr
        )?;

        for (offset, cell) in &self.cells {
            writeln!(f, "  Cell at offset {} (size: {:?})", offset, cell.size())?;
            writeln!(f, "Cell {}", cell.key())?;
        }

        Ok(())
    }
}
/// Utilities to easily create [`BTreePages`]
impl<C: Cell> BTreePage<C> {
    pub fn create(
        page_id: PageId,
        page_size: u32,
        page_type: PageType,
        right_most_page: Option<PageId>,
    ) -> Self {
        let header = PageHeader::new(page_id, page_size, page_type, right_most_page);
        Self {
            header,
            cell_indices: Vec::new(),
            cells: BTreeMap::new(),
        }
    }

    pub fn create_with_data(
        page_id: PageId,
        page_size: u32,
        page_type: PageType,
        right_most_page: Option<PageId>,
        cell_slots: Vec<Slot>,
        cells: BTreeMap<u16, C>,
    ) -> Self {
        let header = PageHeader::new(page_id, page_size, page_type, right_most_page);
        Self {
            header,
            cell_indices: cell_slots,
            cells,
        }
    }
}

unsafe impl<C: Cell> Send for BTreePage<C> {}
unsafe impl<C: Cell> Sync for BTreePage<C> {}
