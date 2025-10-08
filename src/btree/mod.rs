mod debug;

use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::pager::Pager;
use crate::io::IOFrame;
use crate::io::{Frame, IndexFrame, OverflowFrame, TableFrame};
use crate::storage::Cell;
use crate::storage::{InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::{PageId, RowId, Splittable, VarlenaType};
use crate::{HeaderOps, OverflowPage};
use crate::{
    IndexInteriorCell, IndexLeafCell, RQLiteIndexPage, RQLiteTablePage, TableInteriorCell,
    TableLeafCell,
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

#[derive(Debug)]
pub(crate) struct BTree<K, Vl, Vi, P, F> {
    pub(crate) root: PageId,
    pub(crate) min_payload_fraction: f32,
    pub(crate) max_payload_fraction: f32,
    pub(crate) tree_type: BTreeType,
    __key_marker: std::marker::PhantomData<K>,
    __val_leaf_marker: std::marker::PhantomData<Vl>,
    __val_int_marker: std::marker::PhantomData<Vi>,
    __page_marker: std::marker::PhantomData<P>,
    __frame_marker: std::marker::PhantomData<F>,
}

impl<K, Vl, Vi, P, F> BTree<K, Vl, Vi, P, F>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
    Vl: Cell<Key = K>,
    Vi: Cell<Key = K>,
    P: Send
        + Sync
        + HeaderOps
        + InteriorPageOps<Vi, KeyType = K>
        + LeafPageOps<Vl, KeyType = K>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    F: Frame<P> + TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<F, Error = std::io::Error>,
{
    pub(crate) fn create<FI: FileOps, M: MemoryPool>(
        pager: &mut Pager<FI, M>,
        tree_type: BTreeType,
        max_payload_fraction: f32,
        min_payload_fraction: f32,
    ) -> std::io::Result<Self> {
        let page_type = match tree_type {
            BTreeType::Table => crate::PageType::TableLeaf,
            BTreeType::Index => crate::PageType::IndexLeaf,
        };
        let id = pager.alloc_page(page_type)?.id();
        Ok(Self {
            root: id,
            tree_type,
            max_payload_fraction,
            min_payload_fraction,
            __key_marker: std::marker::PhantomData,
            __val_leaf_marker: std::marker::PhantomData,
            __val_int_marker: std::marker::PhantomData,
            __page_marker: std::marker::PhantomData,
            __frame_marker: std::marker::PhantomData,
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

    pub(crate) fn interior_page_type(&self) -> crate::PageType {
        match self.tree_type {
            BTreeType::Table => crate::PageType::TableInterior,
            BTreeType::Index => crate::PageType::IndexInterior,
        }
    }

    pub(crate) fn leaf_page_type(&self) -> crate::PageType {
        match self.tree_type {
            BTreeType::Table => crate::PageType::TableLeaf,
            BTreeType::Index => crate::PageType::IndexLeaf,
        }
    }

    /// Search for a cell by its Key
    pub(crate) fn search<FI: FileOps, M: MemoryPool>(
        &self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> Option<Vl> {
        let root_page = pager.get_single(self.root).unwrap();
        self.debug_assert_page_type(&root_page);
        let traversal = self.traverse(key, pager);

        let leaf_frame_buf = pager.get_single(*traversal.last().unwrap()).unwrap();
        assert!(
            leaf_frame_buf.is_leaf(),
            "Error, traversing did not reach a leaf page ID: {}, TYPE: {:?}",
            leaf_frame_buf.id(),
            leaf_frame_buf.page_type()
        );

        let leaf_frame = F::try_from(leaf_frame_buf).unwrap();

        let guard = leaf_frame.read();

        if let Some(mut cell) = guard.find(&key).cloned() {
            if let Some(overflowpage) = cell.overflow_page() {
                // If there is an overflow page, traverse the linked list of overflow pages to reconstruct the data.
                let mut current_overflow_id = Some(overflowpage);

                while let Some(overflow_id) = current_overflow_id {
                    // Leer la overflow page
                    let io_frame = pager.get_single(overflow_id).unwrap();
                    let overflow_frame = OverflowFrame::try_from(io_frame).unwrap();

                    current_overflow_id = if let Some(payload) = cell.data_mut() {
                        let ovf_guard = overflow_frame.read();
                        let overflow_data = ovf_guard.data.clone();
                        payload.merge_with(overflow_data);
                        ovf_guard.get_next_overflow()
                    } else {
                        None
                    }
                }
            }
            return Some(cell);
        }
        None
    }

    /// Navigate down to the leaf that should contain the key
    fn traverse<FI: FileOps, M: MemoryPool>(
        &self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> Vec<PageId> {
        let mut traversal = Vec::new();
        let mut current_page = pager
            .get_single(self.root)
            .expect("Pointer to root node is dangling!");

        self.debug_assert_page_type(&current_page);
        loop {
            if current_page.is_leaf() {
                traversal.push(current_page.id());
                return traversal;
            }
            let id = current_page.id();

            traversal.push(id);
            let table_frame =
                F::try_from(current_page).expect("Failed to convert page to table frame!");

            let child_id = {
                let guard = table_frame.read();
                guard.find_child(&key)
            };

            current_page = pager
                .get_single(child_id)
                .expect("Pointer to node is dangling!");
        }
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    /// Insert, allowing to overflow.
    fn insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        value: Vl,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let root_page = pager.get_single(self.root)?;

        self.debug_assert_page_type(&root_page);
        let traversal = self.traverse(key, pager);

        let leaf_frame_buf = pager.get_single(*traversal.last().unwrap())?;

        let leaf_frame = F::try_from(leaf_frame_buf)?;


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

    /// Delete, allowing to underflow
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let root_page = pager.get_single(self.root)?;
        self.debug_assert_page_type(&root_page);
        let traversal = self.traverse(key, pager);
        let leaf_frame_buf = pager.get_single(*traversal.last().unwrap())?;
        let leaf_frame = F::try_from(leaf_frame_buf)?;
        {
            let mut leaf_guard = leaf_frame.write();
            leaf_guard.remove(&key);
        }

        self.store_frame(leaf_frame, pager)?;
        // TODO: Missing rebalance after delete.
        Ok(())
    }

    fn store_frame<FI: FileOps, M: MemoryPool>(
        &self,
        frame: F,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let io_frame = IOFrame::try_from(frame)?;

        pager.cache_page(&io_frame);
        Ok(())
    }

    pub(crate) fn rebalance_after_insertion<FI: FileOps, M: MemoryPool>(
        &mut self,
        mut traversal: Vec<PageId>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        traversal.reverse();

        for current_index in 0..traversal.len() {
            let next_index = (current_index + 1).clamp(0, traversal.len() - 1);
            let node = traversal[current_index];
            let parent = traversal[next_index];

            if current_index == 0 {
                self.rebalance_leaf(node, parent, pager)?;
            } else {
                self.rebalance_interior(node, parent, pager)?;
            }
        }
        Ok(())
    }

    fn rebalance_leaf<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let frame = pager.get_single(caller_id)?;
        assert!(
            frame.is_leaf(),
            "Cannot call rebalance-leaf on an interior node. Use rebalance-interior instead"
        );
        let caller = F::try_from(frame)?;
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
            let taken_cells = write_guard.take_leaf_cells();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();

            let new_root = F::try_from(pager.alloc_page(self.interior_page_type())?)?;
            let new_sibling = F::try_from(pager.alloc_page(self.leaf_page_type())?)?;
            {
                let mut root_guard = new_root.write();
                root_guard.insert_child(propagated.key(), write_guard.id())?;
                root_guard.set_rightmost_child(new_sibling.id());
                self.set_root(new_root.id());

                for cell in left_cells {
                    write_guard.insert(cell.key(), cell)?;
                }

                let mut new_sibling_guard = new_sibling.write();
                for cell in right_cells {
                    new_sibling_guard.insert(cell.key(), cell)?;
                }
            }
            self.store_frame(new_sibling, pager)?;
            self.store_frame(new_root, pager)?;
        } else {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_leaf_cells();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();
            let new_sibling = F::try_from(pager.alloc_page(self.leaf_page_type())?)?;

            let parent_buf = pager.get_single(parent_id)?;
            let parent_frame = F::try_from(parent_buf)?;

            let right_most_some = parent_frame
                .read()
                .get_rightmost_child()
                .filter(|child| child != &caller_id); // If we are the right most child we can proceed as if no right most child was set.

            if let Some(right_most) = right_most_some {
                // No need to fix the right most child pointer. We can proceed.
                // As there was an already set right most child pointer, different from the splitted node, we can be sure that the keys on that right most child node will be >> than any key on the new sibling node, as those keys came from a node that was previously a child of this parent we are fixing, and was left to the right most child of this specific parent.
                let mut parent_guard = parent_frame.write();
                parent_guard.insert_child(propagated.key(), write_guard.id())?;
                let propagated_right = right_cells.last().unwrap();
                parent_guard.insert_child(propagated_right.key(), new_sibling.id())?;

                for cell in left_cells {
                    write_guard.insert(cell.key(), cell)?;
                }

                let mut new_sibling_guard = new_sibling.write();

                for cell in right_cells {
                    new_sibling_guard.insert(cell.key(), cell)?;
                }
            } else {
                let mut parent_guard = parent_frame.write();
                parent_guard.set_rightmost_child(new_sibling.id());
                parent_guard.insert_child(propagated.key(), write_guard.id())?;

                for cell in left_cells {
                    write_guard.insert(cell.key(), cell)?;
                }

                let mut new_sibling_guard = new_sibling.write();
                for cell in right_cells {
                    new_sibling_guard.insert(cell.key(), cell)?;
                }
            }

            self.store_frame(new_sibling, pager)?;
            self.store_frame(parent_frame, pager)?;
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn rebalance_interior<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let frame = pager.get_single(caller_id)?;
        assert!(
            frame.is_interior(),
            "Cannot call rebalance-leaf on a non leaf node. Use rebalance-interior instead"
        );
        let caller = F::try_from(frame)?;

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
            let taken_cells = write_guard.take_interior_cells();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();

            let new_root = F::try_from(pager.alloc_page(self.interior_page_type())?)?;
            let new_sibling = F::try_from(pager.alloc_page(self.interior_page_type())?)?;
            {
                let mut root_guard = new_root.write();
                root_guard.insert_child(propagated.key(), write_guard.id())?;
                root_guard.set_rightmost_child(new_sibling.id());
                self.set_root(new_root.id());

                for cell in left_cells {
                    write_guard.insert_child(cell.key(), cell.left_child().unwrap())?;
                }

                let mut new_sibling_guard = new_sibling.write();
                for cell in right_cells {
                    new_sibling_guard.insert_child(cell.key(), cell.left_child().unwrap())?;
                }
            }
            self.store_frame(new_sibling, pager)?;
            self.store_frame(new_root, pager)?;
        } else {
            let mut write_guard = caller.write();
            let taken_cells = write_guard.take_interior_cells();
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            let propagated = right_cells.first().unwrap();
            let new_sibling = F::try_from(pager.alloc_page(self.interior_page_type())?)?;

            let parent_buf = pager.get_single(parent_id)?;
            let parent_frame = F::try_from(parent_buf)?;

            let right_most_some = parent_frame
                .read()
                .get_rightmost_child()
                .filter(|child| child != &caller_id); // If we are the right most child we can proceed as if no right most child was set.

            if let Some(right_most) = right_most_some {
                let mut parent_guard = parent_frame.write();
                parent_guard.insert_child(propagated.key(), write_guard.id())?;
                let propagated_right = right_cells.last().unwrap();
                parent_guard.insert_child(propagated_right.key(), new_sibling.id())?;

                for cell in left_cells {
                    write_guard.insert_child(cell.key(), cell.left_child().unwrap())?;
                }

                let mut new_sibling_guard = new_sibling.write();
                for cell in right_cells {
                    new_sibling_guard.insert_child(cell.key(), cell.left_child().unwrap())?;
                }
            } else {
                let mut parent_guard = parent_frame.write();
                parent_guard.set_rightmost_child(new_sibling.id());
                parent_guard.insert_child(propagated.key(), write_guard.id())?;
            }
            self.store_frame(new_sibling, pager)?;
            self.store_frame(parent_frame, pager)?;
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn create_overflow_chain<FI: FileOps, M: MemoryPool>(
        &self,
        mut overflow_page: OverflowPage,
        mut content: VarlenaType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        while let Some((new_page, remaining_content)) =
            overflow_page.try_insert_with_overflow(content, self.max_payload_fraction)?
        {
            self.store_page(crate::RQLitePage::Overflow(overflow_page), pager)?;
            overflow_page = new_page;
            content = remaining_content;
        }

        self.store_page(crate::RQLitePage::Overflow(overflow_page), pager)?;

        Ok(())
    }

    /// Save a table page to the pager
    fn store_page<FI: FileOps, M: MemoryPool>(
        &self,
        page: crate::RQLitePage,
        pager: &mut Pager<FI, M>,
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

    async fn autovacuum<FI: FileOps, M: MemoryPool>(&self, pager: &mut Pager<FI, M>) {}
}

pub(crate) type TableBTree =
    BTree<RowId, TableLeafCell, TableInteriorCell, RQLiteTablePage, TableFrame>;

pub(crate) type IndexBTree =
    BTree<VarlenaType, IndexLeafCell, IndexInteriorCell, RQLiteIndexPage, IndexFrame>;

#[cfg(test)]
mod btree_tests {
    use super::*;
    use crate::header::Header;
    use crate::io::cache::StaticPool;
    use crate::io::disk::Buffer;
    use crate::io::pager::Pager;
    use crate::serialization::Serializable;
    use crate::types::RowId;
    use std::io::Seek;

    // Helper to create a test pager in memory
    fn setup_test_pager() -> Pager<Buffer, StaticPool> {
        let mut pager = Pager::<Buffer, StaticPool>::create("test.db").unwrap();

        // Initialize the header
        let header = Header::default();
        // Write the header to the pager
        pager.seek(std::io::SeekFrom::Start(0)).unwrap();
        header.write_to(&mut pager).unwrap();

        // Init the pager with cache capacity
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
    fn test_single_record() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        let row_id = RowId::from(1);
        let cell = create_test_cell(row_id, vec![1, 2, 3, 4]);
        btree.print_tree(&mut pager).unwrap();
        // Insert
        btree.insert(row_id, cell.clone(), &mut pager).unwrap();
        btree.print_tree(&mut pager).unwrap();

        // Search
        let found = btree.search(row_id, &mut pager);
        assert!(found.is_some(), "Inserted cell was not found!");
        assert_eq!(found.unwrap().row_id, row_id);


    }

    #[test]
    fn test_multiple_sequential_inserts() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        // Insert 100 registers
        for i in 1..=100 {
            let row_id = RowId::from(i);
            let data = vec![i as u8; 10];
            let cell = create_test_cell(row_id, data);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        // Verify all can be found
        for i in 1..=100 {
            let row_id = RowId::from(i);
            let found = btree.search(row_id, &mut pager);
            assert!(found.is_some(), "No se encontró el row_id: {}", i);
            assert_eq!(found.unwrap().row_id, row_id);
        }
    }

    #[test]
    fn test_overflow() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.3, 0.1).unwrap();

        let row_id = RowId::from(1);

        // Create a big payload
        let large_data: Vec<u8> = (0..2048).map(|i| (i % 256) as u8).collect();
        let cell = create_test_cell(row_id, large_data.clone());

        btree.insert(row_id, cell, &mut pager).unwrap();

        let found = btree.search(row_id, &mut pager);
        assert!(found.is_some());
        assert_eq!(found.unwrap().payload.as_bytes(), large_data.as_slice());
    }

    #[test]
    fn test_root_split() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.3, 0.1).unwrap();

        let original_root = btree.root;

        // Insert enough cells to force overflow state at some point.
        let cell_size = 500;

        for i in 1..=10 {
            let row_id = RowId::from(i);
            let data = vec![i as u8; cell_size];
            let cell = create_test_cell(row_id, data);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        // Verify that the root changed.
        assert_ne!(
            btree.root, original_root,
            "Root should have changed after split"
        );

        // Verify the new root is an interior page.
        let new_root_frame = pager.get_single(btree.root).unwrap();
        assert!(
            new_root_frame.is_interior(),
            "Root should be an interior page."
        );

        btree.print_tree(&mut pager).unwrap();

        // Verify all cells can still be found.
        for i in 1..=10 {
            let row_id = RowId::from(i);
            let found = btree.search(row_id, &mut pager);
            assert!(
                found.is_some(),
                "Cell with row_id {} should exist after split",
                i
            );
        }
    }
}
