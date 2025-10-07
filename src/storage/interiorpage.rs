use crate::storage::cell::Cell;
use crate::storage::cell::IndexInteriorCell;
use crate::storage::cell::TableInteriorCell;
use crate::storage::overflowpage::Overflowable;
use crate::storage::Slot;
use crate::types::{Key, PageId, Splittable, VarlenaType};
use crate::PageType;
use crate::{BTreePage, BTreePageOps, HeaderOps, OverflowPage};
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
    /// Merge with right sibling
    fn merge_with_next_interior(&mut self, separator_key: Self::KeyType, right_sibling: Self);
    /// Split in half when the page has overflown.
    fn split_interior(&mut self) -> (Self::KeyType, Self);
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

    /// Find a child.
    /// The idea is to iterate over the slot array, skipping deleted slots.
    /// If we find a key that is bigger than target, navigate to the left. If not, navigate to the right. This creates. a left-biased distribution which is preferred on B+Trees.
    fn find_child(&self, key: &Self::KeyType) -> PageId {
        for slot in &self.cell_indices {
            if slot.is_deleted() {
                continue;
            }
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
                if slot.is_deleted() {
                    false
                } else if let Some(cell) = self.cells.get(&slot.offset) {
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
            if slot.is_deleted() {
                false
            } else if let Some(cell) = self.cells.get(&slot.offset) {
                &cell.key == key
            } else {
                false
            }
        }) {
            let slot = self.cell_indices.get_mut(pos).unwrap();
            slot.delete();
            let removed_cell = self.cells.get(&slot.offset).unwrap();
            Some(removed_cell.left_child_page)
        } else {
            None
        }
    }

    /// Merge with right sibling at a separator key, combining cells.
    fn merge_with_next_interior(&mut self, separator_key: Self::KeyType, right_sibling: Self) {
        let separator_cell = TableInteriorCell::new(self.header.right_most_page, separator_key);

        if let Ok(separator_offset) = self.add_cell(separator_cell) {
            self.cell_indices.push(Slot::new(separator_offset));
        }

        for (_, cell) in right_sibling.cells {
            if let Err(e) = self.insert_child(cell.key, cell.left_child_page) {
                panic!("Unable to insert child while merging {}", e);
            }
        }
        self.header.right_most_page = right_sibling.header.right_most_page;
        self.header.cell_count = self.cell_indices.len() as u16;
    }

    /// Split in half, promoting the split key to the parent and creating a new page at the right.
    fn split_interior(&mut self) -> (Self::KeyType, Self) {
        let mid = self.cell_count().div_ceil(2);

        let mid_slot = &self.cell_indices[mid].clone();
        let promoted_cell = self.cells.remove(&mid_slot.offset).unwrap();
        let split_offset = self
            .cell_indices
            .split_off(mid + 1)
            .first()
            .map(|s| s.offset)
            .unwrap_or(u16::MAX);
        let new_cells = self.cells.split_off(&split_offset);

        let new_id = PageId::new_key();
        let page_type = PageType::TableInterior;

        let right_most_page = if self.header.right_most_page.is_valid() {
            Some(self.header.right_most_page)
        } else {
            None
        };

        self.header.right_most_page = promoted_cell.left_child_page;

        self.cells.remove(&mid_slot.offset);
        self.cell_indices.pop();
        self.header.cell_count = self.cell_indices.len() as u16;

        let mut new_page =
            TableInteriorPage::create(new_id, self.page_size() as u32, page_type, right_most_page);

        for cell in new_cells.values() {
            if let Err(e) = new_page.insert_child(cell.key, cell.left_child_page) {
                panic!("Unable to insert cell {}", e);
            }
        }

        (promoted_cell.key, new_page)
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

    fn set_rightmost_child(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    fn find_child(&self, key: &Self::KeyType) -> PageId {
        for slot in &self.cell_indices {
            if slot.is_deleted() {
                continue;
            }

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
                if slot.is_deleted() {
                    false
                } else if let Some(cell) = self.cells.get(&slot.offset) {
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
            if slot.is_deleted() {
                false
            } else if let Some(cell) = self.cells.get(&slot.offset) {
                &cell.payload == key
            } else {
                false
            }
        }) {
            let slot = self.cell_indices.get_mut(pos).unwrap();
            slot.delete();
            let removed_cell = self.cells.get(&slot.offset).unwrap();
            Some(removed_cell.left_child_page)
        } else {
            None
        }
    }

    /// TODO: Create a method for bulk inserting new cells which should be more efficient.
    fn merge_with_next_interior(&mut self, separator_key: Self::KeyType, right_sibling: Self) {
        let separator_cell = IndexInteriorCell::new(self.header.right_most_page, separator_key);

        if let Ok(separator_offset) = self.add_cell(separator_cell) {
            self.cell_indices.push(Slot::new(separator_offset));
        }

        for (_, cell) in right_sibling.cells {
            if let Err(e) = self.insert_child(cell.payload, cell.left_child_page) {
                panic!("Unable to insert child while merging {}", e);
            }
        }
        self.header.right_most_page = right_sibling.header.right_most_page;
        self.header.cell_count = self.cell_indices.len() as u16;
    }

    fn split_interior(&mut self) -> (Self::KeyType, Self) {
        let mid = self.cell_count().div_ceil(2);

        let mid_slot = &self.cell_indices[mid].clone();
        let promoted_cell = self.cells.remove(&mid_slot.offset).unwrap();
        let split_offset = self
            .cell_indices
            .split_off(mid + 1)
            .first()
            .map(|s| s.offset)
            .unwrap_or(u16::MAX);
        let new_cells = self.cells.split_off(&split_offset);

        let new_id = PageId::new_key();
        let page_type = PageType::IndexInterior;

        let right_most_page = if self.header.right_most_page.is_valid() {
            Some(self.header.right_most_page)
        } else {
            None
        };

        self.header.right_most_page = promoted_cell.left_child_page;

        self.cells.remove(&mid_slot.offset);
        self.cell_indices.pop();
        self.header.cell_count = self.cell_indices.len() as u16;

        let mut new_page =
            IndexInteriorPage::create(new_id, self.page_size() as u32, page_type, right_most_page);

        for cell in new_cells.values() {
            if let Err(e) = new_page.insert_child(cell.payload.clone(), cell.left_child_page) {
                panic!("Unable to insert cell {}", e);
            }
        }

        (promoted_cell.payload, new_page)
    }
}

/// For interior pages, only index pages are overflowable.
/// Table interior pages are not as they do not include data content.
impl Overflowable for IndexInteriorPage {
    type Content = IndexInteriorCell;

    fn try_insert_with_overflow(
        &mut self,
        mut content: Self::Content,
        max_payload_factor: u16,
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
