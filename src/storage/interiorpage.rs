use crate::btreepage::BTreePageOps;
use crate::storage::cell::Cell;
use crate::storage::cell::IndexInteriorCell;
use crate::storage::cell::TableInteriorCell;
use crate::storage::overflowpage::Overflowable;
use crate::types::{Key, PageId, RowId, Splittable, VarlenaType};
use crate::{BTreePage, HeaderOps, OverflowPage};
use std::io;

pub(crate) type TableInteriorPage = BTreePage<TableInteriorCell>;
pub(crate) type IndexInteriorPage = BTreePage<IndexInteriorCell>;

/// Trait for all interior pages (index and table) to implement.
/// The functionality is similar to that of leaf pages.
/// The main difference is that on interior pages you return pointers to other pages,
/// while on leaf pages you return the actual data content.
pub(crate) trait InteriorPageOps<BTreeCellType: Cell>
where
    Self: HeaderOps + BTreePageOps,
{
    /// Set the right most child
    fn set_rightmost_child(&mut self, page_id: PageId);
    /// Get the right most child for navigation.
    fn get_rightmost_child(&self) -> Option<PageId>;

    /// Insert a child at a given key.
    fn insert_child(&mut self, cell: BTreeCellType) -> io::Result<()>;

    /// [TRY INSERT]: Insert a child at a given key with prebious overflow check.
    fn try_insert_children(&mut self, cells: Vec<BTreeCellType>) -> io::Result<()> {
        let size: u16 = cells.iter().map(|c| c.size()).sum();

        if size >= self.free_space() {
            return Err(
                std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Attempted to insert with overflow: Required size: {}, page_size: {}, free space: {}", size, self.page_size(), self.free_space()
                ),
            )
            );
        };
        for cell in cells {
            self.insert_child(cell)?;
        }
        Ok(())
    }

    /// Find a child on the page given by its key. If it does not find it, will return the rightmost child
    fn find_child(&self, key: &BTreeCellType::Key) -> PageId;
    /// Remove a child given by its page id.
    fn take_child(&mut self, page_id: PageId) -> Option<BTreeCellType>;
    /// Take interior cells.
    fn take_interior_cells(&mut self) -> Vec<BTreeCellType>;
    /// [TRY_POP: Tries to remove cells. Before removing it it checks if it will underflow.
    fn try_pop_children(&mut self, required_size: u16) -> Option<Vec<BTreeCellType>>;
    /// Get a cell from interior page.
    fn get_cell_at_interior(&self, id: u16) -> Option<BTreeCellType>;
    /// Get the siblings for a specific child.
    fn get_siblings_for(&self, child_id: PageId) -> (Option<PageId>, Option<PageId>);
}

impl InteriorPageOps<TableInteriorCell> for TableInteriorPage {
    /// Get the child at the right of the page
    fn get_rightmost_child(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            return Some(self.header.right_most_page);
        }
        None
    }

    fn get_siblings_for(&self, child_id: PageId) -> (Option<PageId>, Option<PageId>) {
        let mut position: Option<usize> = None;

        // First, find the child position.
        for (i, slot) in self.cells.iter().enumerate() {
            if child_id == slot.left_child().unwrap() {
                position = Some(i);
                break;
            }
        }

        // Position was found, then try get the left and right childs
        if let Some(pos) = position {
            // Special case, position  is the last one
            if pos == self.cell_count() as usize - 1 {
                return (
                    self.get_cell_at_interior((pos - 1) as u16)
                        .and_then(|p| p.left_child()),
                    self.get_rightmost_child(),
                );
            } else {
                return (
                    self.get_cell_at_interior((pos - 1) as u16)
                        .and_then(|p| p.left_child()),
                    self.get_cell_at_interior((pos + 1) as u16)
                        .and_then(|p| p.left_child()),
                );
            }
        } else if let Some(right_most) = self.get_rightmost_child() {
            if right_most == child_id {
                return (
                    self.get_cell_at_interior(self.cell_count() - 1)
                        .and_then(|p| p.left_child()),
                    None,
                );
            }
        }
        (None, None)
    }

    /// Setter for the right most child
    fn set_rightmost_child(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    fn take_interior_cells(&mut self) -> Vec<TableInteriorCell> {
        self.take_cells()
    }
    fn get_cell_at_interior(&self, id: u16) -> Option<TableInteriorCell> {
        self.get_cell_at(id)
    }

    /// Find a child.
    /// The idea is to iterate over the slot array, skipping deleted slots.
    /// If we find a key that is bigger than target, navigate to the left. If not, navigate to the right. This creates. a left-biased distribution which is preferred on B+Trees.
    fn find_child(&self, key: &RowId) -> PageId {
        for slot in &self.cells {
            if key <= &slot.key {
                return slot.left_child_page;
            }
        }
        self.header.right_most_page
    }

    /// Insert a child given by its [PageId] and with a given key.
    /// Internally creates a new cell for insertion.
    fn insert_child(&mut self, cell: TableInteriorCell) -> io::Result<()> {
        self.insert_or_replace_cell(cell);
        Ok(())
    }

    /// Remove a child given by its PageId. Returns the cell to the caller.
    /// The caller can either ignore the returned cell or do something with it. Typically, if the cell contains an overflow chain, you would remove also all pages in the chain, adding them to the freelist.
    fn take_child(&mut self, page_id: PageId) -> Option<TableInteriorCell> {
        if let Some(pos) = self
            .cells
            .iter()
            .position(|slot| slot.left_child_page == page_id)
        {
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

    fn try_pop_children(&mut self, required_size: u16) -> Option<Vec<TableInteriorCell>> {
        if !self.can_remove(required_size) {
            return None;
        };

        let mut total_size = 0;
        let mut popped_cells = Vec::new();

        while total_size < required_size {
            let slot = self.cell_indices.pop().unwrap();
            let cell = self.cells.pop().unwrap();

            // This should not be called on each iteration since we are popping at the end of the queue of cells.
            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size());
            };

            self.reset_stats();
            total_size += cell.size();
            popped_cells.push(cell);
        }

        Some(popped_cells)
    }
}

/// All explanations at the top are valid for [IndexPages], with the difference that they use IndexCells and Varlena as the Key type.
impl InteriorPageOps<IndexInteriorCell> for IndexInteriorPage {
    fn get_rightmost_child(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            return Some(self.header.right_most_page);
        }
        None
    }

    fn try_pop_children(&mut self, required_size: u16) -> Option<Vec<IndexInteriorCell>> {
        if !self.can_remove(required_size) {
            return None;
        };

        let mut total_size = 0u16;
        let mut popped_cells = Vec::new();

        while total_size < required_size {
            let slot = self.cell_indices.pop().unwrap();
            let cell = self.cells.pop().unwrap();

            // This should not be called on each iteration since we are popping at the end of the queue of cells
            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size());
            };

            self.reset_stats();
            total_size += cell.size();
            popped_cells.push(cell);
        }

        Some(popped_cells)
    }

    fn take_interior_cells(&mut self) -> Vec<IndexInteriorCell> {
        self.take_cells()
    }
    fn get_cell_at_interior(&self, id: u16) -> Option<IndexInteriorCell> {
        self.get_cell_at(id)
    }

    fn set_rightmost_child(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    fn get_siblings_for(&self, child_id: PageId) -> (Option<PageId>, Option<PageId>) {
        let mut position: Option<usize> = None;

        // First, find the child position.
        for (i, slot) in self.cells.iter().enumerate() {
            if child_id == slot.left_child().unwrap() {
                position = Some(i);
                break;
            }
        }

        // Position was found, then try get the left and right childs
        if let Some(pos) = position {
            // Special case, position  is the last one
            if pos == self.cell_count() as usize - 1 {
                return (
                    self.get_cell_at_interior((pos - 1) as u16)
                        .and_then(|p| p.left_child()),
                    self.get_rightmost_child(),
                );
            } else {
                return (
                    self.get_cell_at_interior((pos - 1) as u16)
                        .and_then(|p| p.left_child()),
                    self.get_cell_at_interior((pos + 1) as u16)
                        .and_then(|p| p.left_child()),
                );
            }
        } else if let Some(right_most) = self.get_rightmost_child() {
            if right_most == child_id {
                return (
                    self.get_cell_at_interior(self.cell_count() - 1)
                        .and_then(|p| p.left_child()),
                    None,
                );
            }
        }
        (None, None)
    }

    fn find_child(&self, key: &VarlenaType) -> PageId {
        for slot in &self.cells {
            if key <= &slot.payload {
                return slot.left_child_page;
            }
        }
        self.header.right_most_page
    }

    /// Remove a child given by its PageId. Returns the page id to the caller.
    fn take_child(&mut self, page_id: PageId) -> Option<IndexInteriorCell> {
        if let Some(pos) = self
            .cells
            .iter()
            .position(|slot| slot.left_child().unwrap() == page_id)
        {
            let slot = self.cell_indices.remove(pos);
            let cell = self.cells.remove(pos);
            self.reset_stats();
            return Some(cell);
        }
        None
    }

    fn insert_child(&mut self, cell: IndexInteriorCell) -> io::Result<()> {
        self.insert_or_replace_cell(cell);
        Ok(())
    }
}

/// For interior pages, only index pages are overflowable.
/// Table interior pages are not as they do not include data content.
impl Overflowable for IndexInteriorPage {
    type Content = IndexInteriorCell;
    type LeafContent = IndexInteriorCell;
    type InteriorContent = IndexInteriorCell;

    fn try_insert_with_overflow(
        &mut self,
        mut content: Self::Content,
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert_child(content)?;
            return Ok(None);
        };

        let fitting_size = self.min_cell_size(min_payload_factor);
        let remaining = content.payload.split_at(fitting_size);
        let new_id = PageId::new_key();
        content.set_overflow(new_id);
        self.set_next_overflow(new_id);

        // Insert or replace the cell
        self.insert_or_replace_cell(content);
        let overflowpage = OverflowPage::create(new_id, self.page_size() as u32, None);
        Ok(Some((overflowpage, remaining)))
    }
}
