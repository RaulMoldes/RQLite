use crate::storage::cell::Cell;
use crate::storage::cell::IndexInteriorCell;
use crate::storage::cell::TableInteriorCell;
use crate::storage::overflowpage::Overflowable;
use crate::storage::Slot;
use crate::types::{Key, PageId, Splittable, VarlenaType};
use crate::PageType;
use crate::{BTreePage, BTreePageOps, HeaderOps, OverflowPage};
use crate::SLOT_SIZE;
use std::io;

pub(crate) type TableInteriorPage = BTreePage<TableInteriorCell>;
pub(crate) type IndexInteriorPage = BTreePage<IndexInteriorCell>;

/// Trait for all interior pages (index and table) to implement.
/// The functionality is similar to that of leaf pages.
/// The main difference is that on interior pages you return pointers to other pages,
/// while on leaf pages you return the actual data content.
pub(crate) trait InteriorPageOps<BTreeCellType: Cell> {
    /// Type of key for this interior page.
    /// It is the [RowId] for Table Pages and the payload (VarlenaType) for Index Pages.
    type KeyType: Ord;
    /// Set the right most child
    fn set_rightmost_child(&mut self, page_id: PageId);
    /// Get the right most child for navigation.
    fn get_rightmost_child(&self) -> Option<PageId>;
    /// Insert a child at a given key.
    fn insert_child(&mut self, key: Self::KeyType, left_child: PageId) -> io::Result<()>;
    /// Find a child on the page given by its key. If it does not find it, will return the rightmost child
    fn find_child(&self, key: &Self::KeyType) -> PageId;
    /// Remove a child given by its key.
    fn remove_child(&mut self, key: &Self::KeyType) -> Option<PageId>;
    /// Take interior cells.
    fn take_interior_cells(&mut self) -> Vec<BTreeCellType>;
    /// Get a cell from interior page.
    fn get_cell_at_interior(&self, id: u16) -> Option<BTreeCellType>;
}

impl InteriorPageOps<TableInteriorCell> for TableInteriorPage {
    type KeyType = crate::types::RowId;

    /// Get the child at the right of the page
    fn get_rightmost_child(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            return Some(self.header.right_most_page);
        }
        None
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
    fn find_child(&self, key: &Self::KeyType) -> PageId {
        for slot in &self.cell_indices {
            if let Some(cell) = self.cells.get(&slot.offset) {
                if key < &cell.key {
                    return cell.left_child_page;
                }
            }
        }
        self.header.right_most_page
    }

    /// Insert a child given by its [PageId] and with a given key.
    /// Internally creates a new cell for insertion.
    fn insert_child(&mut self, key: Self::KeyType, left_child: PageId) -> io::Result<()> {
        let new_cell = TableInteriorCell::new(left_child, key);
        let new_offset = self.add_cell(new_cell)?;

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(cell) = self.cells.get(&slot.offset) {
                    cell.key > key
                } else {
                    false
                }
            })
            .unwrap_or(self.cell_indices.len());

        self.cell_indices.insert(pos, Slot::new(new_offset));

        Ok(())
    }

    /// Remove a child given by its key. Returns the page id to the caller.
    fn remove_child(&mut self, key: &Self::KeyType) -> Option<PageId> {
        if let Some(pos) = self.cell_indices.iter().position(|slot| {
            if let Some(cell) = self.cells.get(&slot.offset) {
                &cell.key == key
            } else {
                false
            }
        }) {
            let slot = self.cell_indices.remove(pos);
            let cell = self.cells.remove(&slot.offset).unwrap();
            self.header.cell_count -= 1;
            self.header.content_start_ptr -= SLOT_SIZE as u16;
            self.header.free_space_ptr -= cell.size() as u16;


            Some(cell.left_child_page)
        } else {
            None
        }
    }
}

/// All explanations at the top are valid for [IndexPages], with the difference that they use IndexCells and Varlena as the Key type.
impl InteriorPageOps<IndexInteriorCell> for IndexInteriorPage {
    type KeyType = VarlenaType;

    fn get_rightmost_child(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            return Some(self.header.right_most_page);
        }
        None
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

    fn find_child(&self, key: &Self::KeyType) -> PageId {
        for slot in &self.cell_indices {
            if let Some(cell) = self.cells.get(&slot.offset) {
                if key < &cell.payload {
                    return cell.left_child_page;
                }
            }
        }
        self.header.right_most_page
    }

    fn insert_child(&mut self, key: Self::KeyType, left_child: PageId) -> io::Result<()> {
        let new_cell = crate::storage::cell::IndexInteriorCell::new(left_child, key.clone());
        let new_offset = self.add_cell(new_cell)?;

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(cell) = self.cells.get(&slot.offset) {
                    cell.payload > key
                } else {
                    false
                }
            })
            .unwrap_or(self.cell_indices.len());

        self.cell_indices.insert(pos, Slot::new(new_offset));

        Ok(())
    }

    fn remove_child(&mut self, key: &Self::KeyType) -> Option<PageId> {
        if let Some(pos) = self.cell_indices.iter().position(|slot| {
            if let Some(cell) = self.cells.get(&slot.offset) {
                &cell.payload == key
            } else {
                false
            }
        }) {
            let slot = self.cell_indices.remove(pos);
            let cell = self.cells.remove(&slot.offset).unwrap();
            self.header.cell_count -= 1;
            self.header.content_start_ptr -= SLOT_SIZE as u16;
            self.header.free_space_ptr -= cell.size() as u16;
            Some(cell.left_child_page)
        } else {
            None
        }
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
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert_child(content.payload, content.left_child_page)?;
            return Ok(None);
        };

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(cell) = self.cells.get(&slot.offset) {
                    cell.payload > content.payload
                } else {
                    false
                }
            })
            .unwrap_or(self.cell_indices.len());

        let fitting_size = self.available_space(max_payload_factor);
        let remaining = content.payload.split_at(fitting_size);
        let new_id = PageId::new_key();
        content.set_overflow(new_id);
        self.set_next_overflow(new_id);
        let new_offset = self.add_cell(content)?;
        self.cell_indices.insert(pos, Slot::new(new_offset));
        let overflowpage =
            OverflowPage::create(new_id, self.page_size() as u32, PageType::Overflow, None);
        Ok(Some((overflowpage, remaining)))
    }
}
