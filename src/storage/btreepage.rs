use crate::cell::Cell;
use crate::page_header::PageHeader;
use crate::page_header::PageType;
use crate::serialization::Serializable;
use crate::storage::Slot;
use crate::types::PageId;
use crate::HeaderOps;
use crate::PAGE_HEADER_SIZE;
use crate::SLOT_SIZE;
use std::io::{self, Cursor, Read, Write};

/// Trait for functions common to all BTreePages.
pub(crate) trait BTreePageOps {
    /// Check if a new cell fits inside the page.
    fn fits_in(&self, additional_size: u16) -> bool
    where
        Self: HeaderOps;

    /// Check if we can remove a cell from a page
    fn can_remove(&self, cell_size: u16) -> bool
    where
        Self: HeaderOps;

    fn reset_stats(&mut self)
    where
        Self: HeaderOps;

    fn get_overflow_size(&self) -> u16
    where
        Self: HeaderOps;

    fn get_underflow_size(&self) -> u16
    where
        Self: HeaderOps;

    fn is_on_underflow_state(&self) -> bool
    where
        Self: HeaderOps,
    {
        self.get_underflow_size() > 0
    }

    fn is_on_overflow_state(&self) -> bool
    where
        Self: HeaderOps,
    {
        self.get_overflow_size() > 0
    }

    // This function is used to compact the cells after deletion has happened.
    // When you delete a cell at some point, the offsets and the free space pointer can get fucked up.
    // For example.
    fn unfuck_cell_offsets(&mut self, removed_offset: u16, removed_size: u16);
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
    pub(crate) cells: Vec<BTreeCellType>,
}

/// All pages must implement [HeaderOps], as all pages have a Header.
impl<BTreeCellType: Cell + Serializable> HeaderOps for BTreePage<BTreeCellType> {
    fn cell_count(&self) -> u16 {
        self.header.cell_count()
    }

    fn content_start(&self) -> u16 {
        self.header.content_start()
    }

    fn free_space(&self) -> u16 {
        self.header.free_space()
    }

    fn id(&self) -> crate::types::PageId {
        self.header.id()
    }

    fn is_overflow(&self) -> bool {
        self.header.is_overflow()
    }

    fn page_size(&self) -> u16 {
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

    fn free_space_start(&self) -> u16 {
        self.header.free_space_start()
    }

    fn set_cell_count(&mut self, count: u16) {
        self.header.set_cell_count(count);
    }

    fn set_content_start_ptr(&mut self, content_start: u16) {
        self.header.set_content_start_ptr(content_start);
    }

    fn set_free_space_ptr(&mut self, ptr: u16) {
        self.header.set_free_space_ptr(ptr);
    }
}

impl<BTreeCellType: Cell + Serializable> Default for BTreePage<BTreeCellType> {
    fn default() -> Self {
        BTreePage {
            header: PageHeader::default(),
            cell_indices: Vec::new(),
            cells: Vec::new(),
        }
    }
}

impl<BTreeCellType: Cell> BTreePageOps for BTreePage<BTreeCellType> {
    fn can_remove(&self, cell_size: u16) -> bool
    where
        Self: HeaderOps,
    {
        let minimum_used_space = self.page_size().div_ceil(2);
        let used_space = self.page_size() - self.free_space();
        (used_space - cell_size - SLOT_SIZE >= minimum_used_space) && self.cell_count() > 1
    }

    fn reset_stats(&mut self)
    where
        Self: HeaderOps,
    {
        let actual_cell_count = self.cell_indices.len() as u16;
        let actual_cells_size: u16 = self.cells.iter().map(|c| c.size()).sum();
        let actual_slots_size = actual_cell_count * SLOT_SIZE;

        self.set_free_space_ptr(self.page_size().saturating_sub(actual_cells_size));
        self.set_cell_count(actual_cell_count);
        self.set_content_start_ptr(PAGE_HEADER_SIZE + actual_slots_size);
    }

    fn get_overflow_size(&self) -> u16
    where
        Self: HeaderOps,
    {
        let actual_cells_size = self.cells.iter().map(|c| c.size()).sum();
        let page_size = self.page_size();
        let actual_slots_size = self.cell_indices.len() as u16 * SLOT_SIZE;
        PAGE_HEADER_SIZE
            .saturating_add(actual_slots_size)
            .saturating_add(actual_cells_size)
            .saturating_sub(self.page_size())
    }

    fn get_underflow_size(&self) -> u16
    where
        Self: HeaderOps,
    {
        let actual_cells_size = self.cells.iter().map(|c| c.size()).sum();
        let page_size = self.page_size();
        let actual_slots_size = self.cell_indices.len() as u16 * SLOT_SIZE;
        page_size
            .div_ceil(2)
            .saturating_sub(PAGE_HEADER_SIZE)
            .saturating_sub(actual_slots_size)
            .saturating_sub(actual_cells_size)
    }

    fn fits_in(&self, additional_size: u16) -> bool
    where
        Self: HeaderOps,
    {
        let maximum_used_space = self.page_size();
        let used_space = self.page_size() - self.free_space();
        used_space + additional_size + SLOT_SIZE <= maximum_used_space
    }

    fn unfuck_cell_offsets(&mut self, removed_offset: u16, removed_size: u16) {
        // Identify those slots that need to be unfucked.
        for slot in self.cell_indices.iter_mut() {
            if slot.offset < removed_offset {
                let old_offset = slot.offset;
                let new_offset = old_offset + removed_size;
            }
        }
    }
}

impl<C: Cell + Serializable> Serializable for BTreePage<C> {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        // First, deserialize the header.
        let header = PageHeader::read_from(reader)?;

        let mut cell_indices = Vec::with_capacity(header.cell_count() as usize);

        // Deserialize all slots.
        for _ in 0..header.cell_count() {
            let index = Slot::read_from(reader)?;
            cell_indices.push(index);
        }

        // Calculate how many bytes to read
        let content_start = header.content_start();
        let remaining_bytes = header.page_size() - content_start;

        // Read exactly the remaining bytes for the page into a temp buffer.
        let mut remaining_data = vec![0u8; remaining_bytes as usize];
        reader.read_exact(&mut remaining_data)?;

        let mut cells = Vec::new();
        for &cell_index in cell_indices.iter() {
            let cell_offset = (cell_index.offset - header.content_start()) as usize;
            if cell_offset >= remaining_data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cell offset out of range: {}", cell_offset),
                ));
            }

            let mut cell_cursor = Cursor::new(&remaining_data[cell_offset..]);
            let cell = C::read_from(&mut cell_cursor)?;
            cells.push(cell);
        }

        Ok(BTreePage {
            header,
            cell_indices,
            cells,
        })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        let buffer_size = self.page_size() - self.content_start();
        let content_start = self.content_start();
        self.header.write_to(writer)?;

        for &idx in &self.cell_indices {
            idx.write_to(writer)?;
        }

        let mut content_buffer = vec![0u8; buffer_size as usize];

        for (i, cell) in self.cells.into_iter().enumerate() {
            let offset = self.cell_indices[i].offset;
            let buffer_offset = (offset - content_start) as usize;

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

        for (i, cell) in self.cells.iter().enumerate() {
            let offset = self.cell_indices[i].offset;
            writeln!(f, "  Cell at offset {} (size: {:?})", offset, cell.size())?;
            writeln!(f, "Cell {}", cell.key())?;
        }

        Ok(())
    }
}
/// Utilities to easily create [`BTreePages`]
impl<C: Cell> BTreePage<C>
where
    Self: HeaderOps,
{
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
            cells: Vec::new(),
        }
    }

    /// Logic to get a cell at a specific position.
    /// As said, the position in the slot array is the canonical way to get a cell in a specific [BTreePage].
    /// So, we go get the cell offset first, then lookup in the btreemap to get the cell.
    /// If the slot has been marked as deleted, we simply ignore it.
    pub(crate) fn get_cell_at(&self, id: u16) -> Option<C> {
        if let Some(__slot) = self.cell_indices.get(id as usize) {
            return self.cells.get(id as usize).cloned();
        };
        None
    }

    /// Finds the appropiate position for a cell.
    /// According to SQLITE documentation:
    ///
    /// Attempt to move the b-tree write cursor to an entry with a key that matches the new key being inserted.
    ///
    /// If a matching entry is found, then the operation is a replace. Otherwise, if the key is not found, an insert.
    ///
    pub(crate) fn find_slot_for_cell(&self, key: &C::Key) -> usize {
        self.cells
            .iter()
            .position(|slot| key <= &slot.key())
            .unwrap_or(self.cell_indices.len())
    }

    pub(crate) fn contains_key(&self, key: &C::Key) -> bool {
        let pos = self.find_slot_for_cell(key);
        self.cells
            .get(pos)
            .map(|existing| &existing.key() == key)
            .unwrap_or(false)
    }

    // Documentation of the insert or replace algo:
    // https://sqlite.org/btreemodule.html#balance_siblings (See Inserting or Replacing cells.)
    pub(crate) fn insert_or_replace_cell(&mut self, cell: C) {
        let cell_size = cell.size() + SLOT_SIZE;

        // Decrement the free space ptr and increment the start-of-content ptr.
        let offset = self.header.free_space_ptr - cell.size();
        let pos = self.find_slot_for_cell(&cell.key());

        // Check if the cell exists.
        let exists = self
            .cells
            .get(pos)
            .map(|existing| existing.key() == cell.key())
            .unwrap_or(false);

        if exists {
            // Replace the cell according to SQLITE documentation
            self.cell_indices[pos] = Slot::new(offset);
            self.cells[pos] = cell;
        } else {
            // Insert again.
            self.cell_indices.insert(pos, Slot::new(offset));
            self.cells.insert(pos, cell);
        }

        self.reset_stats();
    }

    pub(crate) fn delete_cell(&mut self, key: &C::Key) -> Option<C> {
        if let Some(pos) = self.cells.iter().position(|slot| slot.key() == *key) {
            let slot = self.cell_indices.remove(pos);
            let cell = self.cells.remove(pos);

            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size());
            };

            self.reset_stats();
            return Some(cell);
        };
        None
    }

    // BTreeMap does not include a drain method, so we need to use std::mem::take here.
    pub(crate) fn take_cells(&mut self) -> Vec<C> {
        let old_cells = std::mem::take(&mut self.cells);
        self.cell_indices.clear();
        self.header.content_start_ptr = PAGE_HEADER_SIZE;
        self.header.free_space_ptr = self.page_size();
        self.header.cell_count = 0;
        old_cells.into_iter().collect()
    }
}

unsafe impl<C: Cell> Send for BTreePage<C> {}
unsafe impl<C: Cell> Sync for BTreePage<C> {}
