use crate::storage::cell::Cell;
use crate::storage::cell::IndexLeafCell;
use crate::storage::cell::TableLeafCell;
use crate::storage::overflowpage::{OverflowPage, Overflowable};
use crate::storage::slot::Slot;
use crate::types::Splittable;
use crate::types::VarlenaType;
use crate::types::{Key, PageId, RowId};
use crate::SLOT_SIZE;
use crate::{BTreePage, HeaderOps};

/// LEAF PAGE:
pub(crate) type TableLeafPage = BTreePage<TableLeafCell>;
pub(crate) type IndexLeafPage = BTreePage<IndexLeafCell>;

/// Trait for LeafPages to implement.
/// On Table Btrees, the Leaf page is where the data content resides.
/// On Index Btrees, the Leaf page is the last in the index chain and contain pointers to actual data.
pub(crate) trait LeafPageOps<BTreeCellType: Cell> {
    /// Type of key for the leaf page. Is a VarlenaType for Index Leaf pages and a RowId for Table Leaf pages.
    type KeyType: Ord;

    /// Set the next sibling in the linked list of leaf pages.
    fn set_next(&mut self, page_id: PageId);

    /// Get the next sibling in the linked list of leaf pages.
    fn get_next(&self) -> Option<PageId>;

    /// [INSERT]: Find a slot to insert a cell in the page with a given key.
    fn insert(&mut self, key: Self::KeyType, cell: BTreeCellType) -> std::io::Result<()>;

    /// [SAFE INSERT]: With embedded overflow check.
    fn try_insert(
        &mut self,
        key: Self::KeyType,
        cell: BTreeCellType,
        max_payload_fraction: f32,
    ) -> std::io::Result<()>;

    /// [FIND]: find a cell in this page. If not found, return None. The caller should know what to do in that case. Normally, the BTree engine should look in the next sibling on the linked list.
    fn find(&self, key: &Self::KeyType) -> Option<&BTreeCellType>;

    /// [REMOVE]: remove a cell in this page, given by its key.
    fn remove(&mut self, key: &Self::KeyType) -> Option<BTreeCellType>;

    /// [TRY_POP_BACK]: Tries to remove the last cell. Before removing it it checks if it will underflow.
    fn try_pop_back_leaf(&mut self, min_payload_factor: f32) -> Option<BTreeCellType>;

    /// [TRY_POP_FRONT]: Tries to remove the first cell. Before removing it it checks if it will underflow.
    fn try_pop_front_leaf(&mut self, min_payload_factor: f32) -> Option<BTreeCellType>;
    /// Take interior cells.
    fn take_leaf_cells(&mut self) -> Vec<BTreeCellType>;
    /// Get cells at a given leaf.
    fn get_cell_at_leaf(&self, id: u16) -> Option<BTreeCellType>;
}

impl LeafPageOps<TableLeafCell> for TableLeafPage {
    type KeyType = RowId;

    fn get_next(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            Some(self.header.right_most_page)
        } else {
            None
        }
    }

    fn set_next(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    /// Find a key in this page.
    /// Iterate over the cell indices, skipping deleted ones.
    /// Once you find the exact key, return the cell containing it.
    /// If not found, return none. The caller is responsible to navigate to the next page at that point.
    fn find(&self, key: &Self::KeyType) -> Option<&TableLeafCell> {
        for slot in &self.cell_indices {
            if let Some(cell) = self.cells.get(&slot.offset) {
                if &cell.row_id == key {
                    return Some(cell);
                }
            }
        }
        None
    }

    // Try to insert, previously checking so as not to get on overflow state.
    fn try_insert(
        &mut self,
        key: Self::KeyType,
        cell: TableLeafCell,
        max_payload_fraction: f32,
    ) -> std::io::Result<()> {
        let size = cell.size() + SLOT_SIZE;
        let current_used_space = self.page_size() - self.free_space();
        if current_used_space + size >= (max_payload_fraction * self.page_size() as f32) as usize {
            return Err(
                std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Attempted to insert with overflow: Required size: {}, page_size: {}, max payload fraction: {}", size, self.page_size(), max_payload_fraction
                ),
            )
            );
        };

        self.insert(key, cell)
    }

    /// Remove a cell given by is key. Has the same find logic as [`find`]
    fn remove(&mut self, key: &Self::KeyType) -> Option<TableLeafCell> {
        if let Some(pos) = self.cell_indices.iter().position(|slot| {
            if let Some(cell) = self.cells.get(&slot.offset) {
                &cell.row_id == key
            } else {
                false
            }
        }) {
            let slot = self.cell_indices.remove(pos);
            let cell = self.cells.remove(&slot.offset).unwrap();
            self.header.cell_count -= 1;
            self.header.content_start_ptr -= SLOT_SIZE as u16;

            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
            } else {
                self.header.free_space_ptr -= cell.size() as u16;
            };

            Some(cell)
        } else {
            None
        }
    }

    fn try_pop_back_leaf(&mut self, min_payload_factor: f32) -> Option<TableLeafCell> {
        let mut size = usize::MAX;

        if let Some(last_slot) = self.cell_indices.last() {
            if let Some(last_cell) = self.cells.get(&last_slot.offset) {
                size = last_cell.size() + SLOT_SIZE
            };
        };

        if self.cell_count() <= 1
            || self.free_space() + size
                >= (self.page_size() as f32 - (self.page_size() as f32 * min_payload_factor))
                    as usize
        {
            return None;
        };

        let slot = self.cell_indices.pop().unwrap();
        let cell = self.cells.remove(&slot.offset).unwrap();

        self.header.cell_count -= 1;
        self.header.content_start_ptr -= SLOT_SIZE as u16;
        if slot.offset != self.header.free_space_ptr {
            self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
        } else {
            self.header.free_space_ptr += cell.size() as u16;
        };
        Some(cell)
    }

    fn try_pop_front_leaf(&mut self, min_payload_factor: f32) -> Option<TableLeafCell> {
        let mut size = usize::MAX;

        if let Some(first_slot) = self.cell_indices.first() {
            if let Some(first_cell) = self.cells.get(&first_slot.offset) {
                size = first_cell.size() + SLOT_SIZE
            };
        };

        if self.cell_count() <= 1
            || self.free_space() + size
                >= (self.page_size() as f32 - (self.page_size() as f32 * min_payload_factor))
                    as usize
        {
            return None;
        };

        let slot = self.cell_indices.remove(0);
        let cell = self.cells.remove(&slot.offset).unwrap();

        self.header.cell_count -= 1;
        self.header.content_start_ptr -= SLOT_SIZE as u16;
        if slot.offset != self.header.free_space_ptr {
            self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
        } else {
            self.header.free_space_ptr += cell.size() as u16;
        };

        Some(cell)
    }

    fn take_leaf_cells(&mut self) -> Vec<TableLeafCell> {
        self.take_cells()
    }
    fn get_cell_at_leaf(&self, id: u16) -> Option<TableLeafCell> {
        self.get_cell_at(id)
    }

    /// Insert a cell with a given key on this page.
    /// Needs to find the target position when inserting the slot to ensure that cells are ordered.
    fn insert(&mut self, key: Self::KeyType, cell: TableLeafCell) -> std::io::Result<()> {
        let new_offset = self.add_cell(cell)?;

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(existing_cell) = self.cells.get(&slot.offset) {
                    existing_cell.row_id >= key
                } else {
                    false
                }
            })
            .unwrap_or(self.cell_indices.len());

        self.cell_indices.insert(pos, Slot::new(new_offset));

        Ok(())
    }
}

/// Implementation of the [LeafPageOps] for the [IndexLeafPage].
/// Explanations for [TableLeafPage] are also valid here, the only difference is that the comparator key for [IndexLeafCell] is actually [VarlenaType], instead of [RowId]
impl LeafPageOps<IndexLeafCell> for IndexLeafPage {
    type KeyType = VarlenaType;

    fn find(&self, key: &Self::KeyType) -> Option<&IndexLeafCell> {
        for slot in &self.cell_indices {
            if let Some(cell) = self.cells.get(&slot.offset) {
                if &cell.payload == key {
                    return Some(cell);
                }
            }
        }
        None
    }

    fn get_next(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            Some(self.header.right_most_page)
        } else {
            None
        }
    }

    fn set_next(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    // Try to insert, previously checking so as not to get on overflow state.
    fn try_insert(
        &mut self,
        key: Self::KeyType,
        cell: IndexLeafCell,
        max_payload_fraction: f32,
    ) -> std::io::Result<()> {
        let size = cell.size() + SLOT_SIZE;
        let current_used_space = self.page_size() - self.free_space();
        if current_used_space + size >= (max_payload_fraction * self.page_size() as f32) as usize {
            return Err(
                std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Attempted to insert with overflow: Required size: {}, page_size: {}, max payload fraction: {}", size, self.page_size(), max_payload_fraction
                ),
            )
            );
        };

        self.insert(key, cell)
    }

    fn try_pop_back_leaf(&mut self, min_payload_factor: f32) -> Option<IndexLeafCell> {
        let mut size = usize::MAX;

        if let Some(last_slot) = self.cell_indices.last() {
            if let Some(last_cell) = self.cells.get(&last_slot.offset) {
                size = last_cell.size()
            };
        };

        if self.cell_count() <= 1
            || self.free_space() + size
                >= (self.page_size() as f32 - (self.page_size() as f32 * min_payload_factor))
                    as usize
        {
            return None;
        };

        let slot = self.cell_indices.pop().unwrap();
        let cell = self.cells.remove(&slot.offset).unwrap();
        self.header.cell_count -= 1;
        self.header.content_start_ptr -= SLOT_SIZE as u16;

        if slot.offset != self.header.free_space_ptr {
            self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
        } else {
            self.header.free_space_ptr += cell.size() as u16;
        };
        Some(cell)
    }

    fn try_pop_front_leaf(&mut self, min_payload_factor: f32) -> Option<IndexLeafCell> {
        let mut size = usize::MAX;

        if let Some(first_slot) = self.cell_indices.first() {
            if let Some(last_cell) = self.cells.get(&first_slot.offset) {
                size = last_cell.size()
            };
        };

        if self.cell_count() <= 1
            || self.free_space() + size
                >= (self.page_size() as f32 - (self.page_size() as f32 * min_payload_factor))
                    as usize
        {
            return None;
        };

        let slot = self.cell_indices.remove(0);
        let cell = self.cells.remove(&slot.offset).unwrap();

        self.header.cell_count -= 1;
        self.header.content_start_ptr -= SLOT_SIZE as u16;
        if slot.offset != self.header.free_space_ptr {
            self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
        } else {
            self.header.free_space_ptr += cell.size() as u16;
        };
        Some(cell)
    }

    fn take_leaf_cells(&mut self) -> Vec<IndexLeafCell> {
        self.take_cells()
    }
    fn get_cell_at_leaf(&self, id: u16) -> Option<IndexLeafCell> {
        self.get_cell_at(id)
    }

    fn remove(&mut self, key: &Self::KeyType) -> Option<IndexLeafCell> {
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
            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size() as u16);
            } else {
                self.header.free_space_ptr += cell.size() as u16;
            };

            Some(cell)
        } else {
            None
        }
    }

    fn insert(&mut self, key: Self::KeyType, cell: IndexLeafCell) -> std::io::Result<()> {
        let new_offset = self.add_cell(cell)?;

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(existing_cell) = self.cells.get(&slot.offset) {
                    existing_cell.payload > key
                } else {
                    false
                }
            })
            .unwrap_or(self.cell_indices.len());

        self.cell_indices.insert(pos, Slot::new(new_offset));
        Ok(())
    }
}

/// Both [IndexLeafPage] and [TableLeafPage] are [Overflowable].
/// This means they are allowed to create overflow pages when the content insertion size is
/// bigger than [max_cell_size].
/// TODO: need to review is recursively creating the overflow chain is the best idea here.
impl Overflowable for IndexLeafPage {
    type Content = IndexLeafCell;
    type InteriorContent = IndexLeafCell;
    type LeafContent = IndexLeafCell;

    fn try_insert_with_overflow(
        &mut self,
        mut content: Self::Content,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        // Extra four bytes for pointers to pages
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert(content.payload.clone(), content)?;
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

        let overflowpage = OverflowPage::create(new_id, self.page_size() as u32, None);
        Ok(Some((overflowpage, remaining)))
    }
}

impl Overflowable for TableLeafPage {
    type Content = TableLeafCell;
    type InteriorContent = TableLeafCell;
    type LeafContent = TableLeafCell;

    fn try_insert_with_overflow(
        &mut self,
        mut content: Self::Content,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert(content.row_id, content)?;
            return Ok(None);
        };

        let pos = self
            .cell_indices
            .iter()
            .position(|slot| {
                if let Some(cell) = self.cells.get(&slot.offset) {
                    cell.row_id > content.row_id
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

        let overflowpage = OverflowPage::create(new_id, self.page_size() as u32, None);
        Ok(Some((overflowpage, remaining)))
    }
}
