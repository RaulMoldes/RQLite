use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::pager::Pager;
use crate::io::IOFrame;
use crate::io::{Frame, TableFrame};
use crate::storage::Cell;
use crate::storage::{BTreePageOps, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::{PageId, RowId, VarlenaType};
use crate::TableLeafCell;
use crate::{
    overflowpage, HeaderOps, OverflowPage, RQLiteTablePage, TableInteriorCell, TableInteriorPage,
    TableLeafPage,
};
fn split_cells_by_size<BTreeCellType: Cell>(
    mut taken_cells: Vec<BTreeCellType>,
) -> (Vec<BTreeCellType>, Vec<BTreeCellType>) {
    taken_cells.sort_by_key(|cell| cell.key());
    let sizes: Vec<usize> = taken_cells.iter().map(|cell| cell.size()).collect();
    let total_size: usize = sizes.iter().sum();
    let half_size = total_size.div_ceil(2);

    // Look for the point where the accumulated result crosses the mid point.
    let mut acc = 0;
    let split_index = sizes
        .iter()
        .position(|&s| {
            acc += s;
            acc >= half_size
        })
        .unwrap_or(taken_cells.len().div_ceil(2));

    // Divide the vector
    let (left, right) = taken_cells.split_at(split_index);
    (left.to_vec(), right.to_vec())
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum BTreeType {
    Table,
    Index,
}

pub(crate) struct BTree<K, V> {
    pub(crate) root: PageId,
    pub(crate) min_payload_fraction: u16,
    pub(crate) max_payload_fraction: u16,
    pub(crate) tree_type: BTreeType,
    pub(crate) key_marker: std::marker::PhantomData<K>,
    pub(crate) val_marker: std::marker::PhantomData<V>,
}

/// Information about a split that needs to be propagated upward
#[derive(Debug)]
struct SplitInfo {
    /// The key that was promoted during the split
    promoted_key: RowId,
    /// The page id of the left child after split
    left_child: PageId,
    /// The page id of the newly created right child
    right_child: PageId,
}

impl<K, V> BTree<K, V>
where
    K: Ord,
    V: Cell,
{
    pub(crate) fn create_table<F: FileOps, M: MemoryPool>(
        pager: &mut Pager<F, M>,
        max_payload_fraction: u16,
        min_payload_fraction: u16,
    ) -> std::io::Result<Self> {
        let id = pager.alloc_page(crate::PageType::TableLeaf)?.id();
        Ok(Self {
            root: id,
            tree_type: BTreeType::Table,
            max_payload_fraction,
            min_payload_fraction,
            key_marker: std::marker::PhantomData,
            val_marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn create_index<F: FileOps, M: MemoryPool>(
        pager: &mut Pager<F, M>,
        max_payload_fraction: u16,
        min_payload_fraction: u16,
    ) -> std::io::Result<Self> {
        let id = pager.alloc_page(crate::PageType::IndexLeaf)?.id();
        Ok(Self {
            root: id,
            tree_type: BTreeType::Index,
            max_payload_fraction,
            min_payload_fraction,
            key_marker: std::marker::PhantomData,
            val_marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn is_table(&self) -> bool {
        matches!(self.tree_type, BTreeType::Table)
    }

    pub(crate) fn is_index(&self) -> bool {
        matches!(self.tree_type, BTreeType::Index)
    }

    pub(crate) fn debug_assert_page_type(&self, frame: &IOFrame) {
        debug_assert!(
            frame.is_table() && self.is_table() || frame.is_index() && self.is_index(),
            "Invalid frame: the tree is of type {:?}, but frame is of type: {:?}",
            self.tree_type,
            frame.page_type()
        );
    }

    /// Search for a cell by its RowId
    pub(crate) fn search<F: FileOps, M: MemoryPool>(
        &self,
        key: RowId,
        pager: &mut Pager<F, M>,
    ) -> Option<TableLeafCell> {
        let root_page = pager.get_single(self.root).unwrap();
        self.debug_assert_page_type(&root_page);
        let traversal = self.traverse(key, pager);
        let leaf_frame_buf = pager.get_single(*traversal.last().unwrap()).unwrap();
        let leaf_frame = TableFrame::try_from(leaf_frame_buf).unwrap();

        let guard = leaf_frame.read();
        guard.find(&key).cloned()
    }

    /// Navigate down to the leaf that should contain the key
    fn traverse<F: FileOps, M: MemoryPool>(
        &self,
        key: RowId,
        pager: &mut Pager<F, M>,
    ) -> Vec<PageId> {
        let mut traversal = Vec::new();
        traversal.push(self.root);
        let mut current_page = pager.get_single(self.root).expect(&format!(
            "Pointer to root node is dangling: {} !",
            self.root
        ));

        self.debug_assert_page_type(&current_page);
        loop {
            if current_page.is_leaf() {
                return traversal;
            }
            let id = current_page.id();
            traversal.push(id);
            let table_frame = TableFrame::try_from(current_page)
                .expect(&format!("Failed to convert page to table frame: {} !", id));

            let child_id = {
                let guard = table_frame.read();
                guard.find_child(&key)
            };

            current_page = pager
                .get_single(child_id)
                .expect(&format!("Pointer to node is dangling: {} !", id));
        }
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    /// Insert, allowing to overflow.
    fn insert<F: FileOps, M: MemoryPool>(
        &mut self,
        key: RowId,
        value: TableLeafCell,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        let root_page = pager.get_single(self.root)?;
        self.debug_assert_page_type(&root_page);
        let traversal = self.traverse(key, pager);
        let leaf_frame_buf = pager.get_single(*traversal.last().unwrap())?;
        let leaf_frame = TableFrame::try_from(leaf_frame_buf)?;

        // Check if inserting the page will produce an overflow.
        let will_overflow = {
            let read_guard = leaf_frame.read();
            read_guard.max_cell_size(self.max_payload_fraction) <= value.size()
        };

        if will_overflow {
            let mut write_guard = leaf_frame.write();
            if let Some((overflow_page, content)) =
                write_guard.try_insert_with_overflow_leaf(value, self.max_payload_fraction)?
            {
                self.create_overflow_chain(overflow_page, content, pager)?;
            }
        } else {
            let mut write_guard = leaf_frame.write();
            write_guard.insert(key, value)?;
        }

        self.store_frame(leaf_frame, pager)?;
        self.rebalance_after_insertion(traversal, pager)?;

        Ok(())
    }

    fn store_frame<F: FileOps, M: MemoryPool, T>(
        &self,
        frame: T,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()>
    where
        IOFrame: TryFrom<T, Error = std::io::Error>,
    {
        let io_frame = IOFrame::try_from(frame)?;
        pager.cache_page(&io_frame);
        Ok(())
    }

    pub(crate) fn rebalance_after_insertion<F: FileOps, M: MemoryPool>(
        &mut self,
        mut traversal: Vec<PageId>,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        traversal.reverse();

        if traversal.len() == 1 {
            let node = traversal[0];
            let parent = traversal[0];
            self.rebalance_leaf(node, parent, pager)?;
            return Ok(());
        }

        for i in 0..traversal.len() {
            let node = traversal[i];
            let parent = traversal[i + 1];

            if i == 0 {
                self.rebalance_leaf(node, parent, pager)?;
            } else {
                self.rebalance_interior(node, parent, pager)?;
            }
        }
        Ok(())
    }

    fn rebalance_leaf<F: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        let frame = pager.get_single(caller_id)?;
        let caller = TableFrame::try_from(frame)?;
        assert!(
            caller.is_leaf(),
            "Cannot call rebalance-leaf on an interior node. Use rebalance-interior instead"
        );
        let is_overflow_state = {
            let read_guard = caller.read();
            read_guard.is_on_overflow_state(self.max_payload_fraction)
        };

        if !is_overflow_state {
            return Ok(());
        }

        // Root has overflown
        if self.is_root(caller.id()) {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_cells_leaf();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();

            let new_root = TableFrame::try_from(pager.alloc_page(crate::PageType::TableInterior)?)?;
            let new_sibling = TableFrame::try_from(pager.alloc_page(crate::PageType::TableLeaf)?)?;

            let mut root_guard = new_root.write();
            root_guard.insert_child(propagated.row_id, write_guard.id())?;
            root_guard.set_rightmost_child(new_sibling.id());
            self.set_root(new_root.id());

            for cell in left_cells {
                write_guard.insert(cell.row_id, cell)?;
            }

            let mut new_sibling_guard = new_sibling.write();
            for cell in right_cells {
                new_sibling_guard.insert(cell.row_id, cell)?;
            }
        } else {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_cells_leaf();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();
            let new_sibling = TableFrame::try_from(pager.alloc_page(crate::PageType::TableLeaf)?)?;

            let parent_buf = pager.get_single(parent_id)?;
            let parent_frame = TableFrame::try_from(parent_buf)?;

            let right_most_some = parent_frame
                .read()
                .get_rightmost_child()
                .filter(|child| child != &caller_id); // If we are the right most child we can proceed as if no right most child was set.

            if let Some(right_most) = right_most_some {
                // No need to fix the right most child pointer. We can proceed.
                // As there was an already set right most child pointer, different from the splitted node, we can be sure that the keys on that right most child node will be >> than any key on the new sibling node, as those keys came from a node that was previously a child of this parent we are fixing, and was left to the right most child of this specific parent.
                let mut parent_guard = parent_frame.write();
                parent_guard.insert_child(propagated.row_id, write_guard.id())?;
            } else {
                let mut parent_guard = parent_frame.write();
                parent_guard.set_rightmost_child(new_sibling.id());
                parent_guard.insert_child(propagated.row_id, write_guard.id())?;
            }

            self.store_frame(parent_frame, pager)?;
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn rebalance_interior<F: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        let frame = pager.get_single(caller_id)?;
        let caller = TableFrame::try_from(frame)?;
        assert!(
            caller.is_interior(),
            "Cannot call rebalance-leaf on a leaf node. Use rebalance-interior instead"
        );
        let is_overflow_state = {
            let read_guard = caller.read();
            read_guard.is_on_overflow_state(self.max_payload_fraction)
        };

        if !is_overflow_state {
            return Ok(());
        }

        // Root has overflown
        if self.is_root(caller.id()) {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_cells_interior();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();

            let new_root = TableFrame::try_from(pager.alloc_page(crate::PageType::TableInterior)?)?;
            let new_sibling =
                TableFrame::try_from(pager.alloc_page(crate::PageType::TableInterior)?)?;

            let mut root_guard = new_root.write();
            root_guard.insert_child(propagated.key, write_guard.id())?;
            root_guard.set_rightmost_child(new_sibling.id());
            self.set_root(new_root.id());

            for cell in left_cells {
                write_guard.insert_child(cell.key, cell.left_child_page)?;
            }

            let mut new_sibling_guard = new_sibling.write();
            for cell in right_cells {
                new_sibling_guard.insert_child(cell.key, cell.left_child_page)?;
            }
        } else {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_cells_leaf();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();
            let new_sibling =
                TableFrame::try_from(pager.alloc_page(crate::PageType::TableInterior)?)?;

            let parent_buf = pager.get_single(parent_id)?;
            let parent_frame = TableFrame::try_from(parent_buf)?;

            let right_most_some = parent_frame
                .read()
                .get_rightmost_child()
                .filter(|child| child != &caller_id); // If we are the right most child we can proceed as if no right most child was set.

            if let Some(right_most) = right_most_some {
                let mut parent_guard = parent_frame.write();
                parent_guard.insert_child(propagated.row_id, write_guard.id())?;
            } else {
                let mut parent_guard = parent_frame.write();
                parent_guard.set_rightmost_child(new_sibling.id());
                parent_guard.insert_child(propagated.row_id, write_guard.id())?;
            }

            self.store_frame(parent_frame, pager)?;
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn create_overflow_chain<F: FileOps, M: MemoryPool>(
        &self,
        mut overflow_page: OverflowPage,
        mut content: VarlenaType,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        while let Some((new_page, remaining_content)) =
            overflow_page.try_insert_with_overflow(content, self.max_payload_fraction)?
        {
            self.store_page(crate::RQLitePage::Overflow(overflow_page), pager)?;
            overflow_page = new_page;
            content = remaining_content;
        }
        Ok(())
    }

    /// Save a table page to the pager
    fn store_page<F: FileOps, M: MemoryPool>(
        &self,
        page: crate::RQLitePage,
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<()> {
        use crate::serialization::Serializable;

        let page_id = page.id();
        let page_type = page.type_of();
        let mut buffer = Vec::with_capacity(pager.page_size() as usize);
        page.write_to(&mut buffer)?;

        // Create a frame and cache it.
        let frame = crate::io::PageFrame::from_buffer(page_id, page_type, buffer);

        pager.cache_page(&frame);
        Ok(())
    }
}

#[cfg(test)]
mod btree_tests {
    use super::*;
    use crate::header::Header;
    use crate::io::cache::StaticPool;
    use crate::io::disk::Buffer;
    use crate::io::pager::Pager;
    use crate::serialization::Serializable;
    use crate::types::{PageId, RowId};
    use crate::TableLeafCell;
    use std::io::{Read, Seek, Write};

    // Helper para crear un pager de prueba con Buffer (in-memory)
    fn setup_test_pager() -> Pager<Buffer, StaticPool> {
        let mut pager = Pager::<Buffer, StaticPool>::create("test.db").unwrap();

        // Inicializar el header
        let header = Header::default();
        // Escribir header al pager
        pager.seek(std::io::SeekFrom::Start(0)).unwrap();
        header.write_to(&mut pager).unwrap();

        // Iniciar el pager con capacidad de cache
        pager.start(100).unwrap();

        pager
    }

    // Helper para crear células de prueba simples
    fn create_test_cell(row_id: RowId, data: Vec<u8>) -> TableLeafCell {
        let payload = VarlenaType::from_raw_bytes(&data, None);
        TableLeafCell {
            row_id,
            payload,
            overflow_page: None,
        }
    }

    #[test]
    fn test_btree_creation() {
        let mut pager = setup_test_pager();
        let btree = BTree::<RowId, TableLeafCell>::create_table(
            &mut pager, 255, // max_payload_fraction
            32,  // min_payload_fraction
        )
        .unwrap();

        assert_eq!(btree.tree_type, BTreeType::Table);
        assert!(u32::from(btree.root) > 0);
    }

    #[test]
    fn test_single_insert_and_search() {
        let mut pager = setup_test_pager();
        let mut btree = BTree::<RowId, TableLeafCell>::create_table(&mut pager, 255, 32).unwrap();

        let row_id = RowId::from(1);
        let cell = create_test_cell(row_id, vec![1, 2, 3, 4]);

        // Insert
        btree.insert(row_id, cell.clone(), &mut pager).unwrap();

        // Search
        let found = btree.search(row_id, &mut pager);
        assert!(found.is_some(), "No se encontró la célula insertada");
        assert_eq!(found.unwrap().row_id, row_id);
    }
}
