use crate::btreepage::BTreePageOps;
use crate::storage::cell::Cell;
use crate::storage::cell::IndexLeafCell;
use crate::storage::cell::TableLeafCell;
use crate::storage::overflowpage::{OverflowPage, Overflowable};
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
pub(crate) trait LeafPageOps<BTreeCellType: Cell>
where
    Self: HeaderOps + BTreePageOps,
    BTreeCellType::Key: Ord,
{
    /// Set the next sibling in the linked list of leaf pages.
    fn set_next(&mut self, page_id: PageId);

    /// Get the next sibling in the linked list of leaf pages.
    fn get_next(&self) -> Option<PageId>;

    /// [INSERT]: Find a slot to insert a cell in the page with a given key.
    fn insert(&mut self, cell: BTreeCellType) -> std::io::Result<()>;

    /// [SCAN]:  all the cells in the page
    fn scan<'a>(
        &'a self,
        start_key: <BTreeCellType as Cell>::Key,
    ) -> impl Iterator<Item = &'a BTreeCellType> + 'a
    where
        BTreeCellType: 'a;

    /// [SAFE INSERT]: With embedded overflow check.
    fn try_insert(&mut self, cells: Vec<BTreeCellType>) -> std::io::Result<()> {
        let size: u16 = cells.iter().map(|c| c.size() + SLOT_SIZE).sum();

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
            self.insert(cell)?;
        }

        Ok(())
    }

    /// [FIND]: find a cell in this page. If not found, return None. The caller should know what to do in that case. Normally, the BTree engine should look in the next sibling on the linked list.
    fn find(&self, key: &BTreeCellType::Key) -> Option<&BTreeCellType>;

    /// [REMOVE]: remove a cell in this page, given by its key.
    fn remove(&mut self, key: &BTreeCellType::Key) -> Option<BTreeCellType>;

    /// [TRY_POP]: Tries to remove cells. Before removing it it checks if it will underflow.
    fn try_pop(&mut self, required_size: u16) -> Option<Vec<BTreeCellType>>;

    /// Take interior cells.
    fn take_leaf_cells(&mut self) -> Vec<BTreeCellType>;
    /// Get cells at a given leaf.
    fn get_cell_at_leaf(&self, id: u16) -> Option<BTreeCellType>;
}

impl LeafPageOps<TableLeafCell> for TableLeafPage {
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

    fn scan<'a>(
        &'a self,
        start_key: <TableLeafCell as Cell>::Key,
    ) -> impl Iterator<Item = &'a TableLeafCell> + 'a
    where
        TableLeafCell: 'a,
    {
        self.cells
            .iter()
            .skip_while(move |cell| cell.key() < start_key)
    }

    /// Find a key in this page.
    /// Iterate over the cell indices, skipping deleted ones.
    /// Once you find the exact key, return the cell containing it.
    /// If not found, return none. The caller is responsible to navigate to the next page at that point.
    fn find(&self, key: &RowId) -> Option<&TableLeafCell> {
        self.cells
            .iter()
            .find(|&slot| &slot.key() == key)
            .map(|v| v as _)
    }

    /// Remove a cell given by is key. Has the same find logic as [`find`]
    fn remove(&mut self, key: &RowId) -> Option<TableLeafCell> {
        self.delete_cell(key)
    }

    fn try_pop(&mut self, required_size: u16) -> Option<Vec<TableLeafCell>> {
        if !self.can_remove(required_size) {
            return None;
        };

        let mut total_size = 0;
        let mut popped_cells = Vec::new();
        while total_size < required_size {
            let slot = self.cell_indices.pop().unwrap();
            let cell = self.cells.pop().unwrap();

            // This should not be called at each iteration since we are popping at the end
            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size());
            };

            self.reset_stats();
            total_size += cell.size();
            popped_cells.push(cell)
        }

        Some(popped_cells)
    }

    fn take_leaf_cells(&mut self) -> Vec<TableLeafCell> {
        self.take_cells()
    }
    fn get_cell_at_leaf(&self, id: u16) -> Option<TableLeafCell> {
        self.get_cell_at(id)
    }

    /// Insert a cell with a given key on this page.
    /// Needs to find the target position when inserting the slot to ensure that cells are ordered.
    fn insert(&mut self, cell: TableLeafCell) -> std::io::Result<()> {
        self.insert_or_replace_cell(cell);
        Ok(())
    }
}

/// Implementation of the [LeafPageOps] for the [IndexLeafPage].
/// Explanations for [TableLeafPage] are also valid here, the only difference is that the comparator key for [IndexLeafCell] is actually [VarlenaType], instead of [RowId]
impl LeafPageOps<IndexLeafCell> for IndexLeafPage {
    fn find(&self, key: &VarlenaType) -> Option<&IndexLeafCell> {
        self.cells
            .iter()
            .find(|&slot| &slot.key() == key)
            .map(|v| v as _)
    }

    fn get_next(&self) -> Option<PageId> {
        if self.header.right_most_page.is_valid() {
            Some(self.header.right_most_page)
        } else {
            None
        }
    }

    fn scan<'a>(
        &'a self,
        start_key: <IndexLeafCell as Cell>::Key,
    ) -> impl Iterator<Item = &'a IndexLeafCell> + 'a
    where
        IndexLeafCell: 'a,
    {
        self.cells
            .iter()
            .skip_while(move |cell| cell.key() < start_key)
    }

    fn set_next(&mut self, page_id: PageId) {
        self.header.right_most_page = page_id
    }

    fn try_pop(&mut self, required_size: u16) -> Option<Vec<IndexLeafCell>> {
        if !self.can_remove(required_size) {
            return None;
        };

        let mut total_size = 0;
        let mut popped_cells = Vec::new();
        while total_size < required_size {
            let slot = self.cell_indices.pop().unwrap();
            let cell = self.cells.pop().unwrap();

            if slot.offset != self.header.free_space_ptr {
                self.unfuck_cell_offsets(slot.offset, cell.size());
            };

            self.reset_stats();
            total_size += cell.size();
            popped_cells.push(cell)
        }

        Some(popped_cells)
    }

    fn take_leaf_cells(&mut self) -> Vec<IndexLeafCell> {
        self.take_cells()
    }
    fn get_cell_at_leaf(&self, id: u16) -> Option<IndexLeafCell> {
        self.get_cell_at(id)
    }

    fn remove(&mut self, key: &VarlenaType) -> Option<IndexLeafCell> {
        self.delete_cell(key)
    }

    fn insert(&mut self, cell: IndexLeafCell) -> std::io::Result<()> {
        self.insert_or_replace_cell(cell);
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
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        // Extra four bytes for pointers to pages
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert(content)?;
            return Ok(None);
        };

        let fitting_size = self.min_cell_size(min_payload_factor);
        let remaining = content.payload.split_at(fitting_size);
        let new_id = PageId::new_key();

        content.set_overflow(new_id);
        self.set_next_overflow(new_id);

        // Insert the cell
        self.insert_or_replace_cell(content);

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
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        if self.max_cell_size(max_payload_factor) >= content.size() {
            self.insert(content)?;
            return Ok(None);
        };

        let fitting_size = self.min_cell_size(min_payload_factor);
        let remaining = content.payload.split_at(fitting_size);

        let new_id = PageId::new_key();
        content.set_overflow(new_id);

        self.set_next_overflow(new_id);

        // Insert the cell
        self.insert_or_replace_cell(content);

        let overflowpage = OverflowPage::create(new_id, self.page_size() as u32, None);
        Ok(Some((overflowpage, remaining)))
    }
}
