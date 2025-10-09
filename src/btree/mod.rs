mod debug;

use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::pager::Pager;
use crate::io::IOFrame;
use crate::io::{Frame, IndexFrame, OverflowFrame, TableFrame};
use crate::storage::Cell;
use crate::storage::{InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::{PageId, RowId, Splittable, VarlenaType};
use crate::SLOT_SIZE;
use crate::{HeaderOps, OverflowPage};
use crate::{
    IndexInteriorCell, IndexLeafCell, RQLiteIndexPage, RQLiteTablePage, TableInteriorCell,
    TableLeafCell,
};

fn compute_total_size<BTreeCellType: Cell>(cells: &[BTreeCellType]) -> usize {
    cells.iter().map(|c| SLOT_SIZE + c.size()).sum()
}

fn check_valid_size<BTreeCellType: Cell>(
    cells: &[BTreeCellType],
    max_payload_fraction: f32,
    min_payload_fraction: f32,
    page_size: usize,
) -> bool {
    let total_size: usize = cells.iter().map(|c| SLOT_SIZE + c.size()).sum();
    (total_size + crate::PAGE_HEADER_SIZE <= (max_payload_fraction * page_size as f32) as usize)
        && (total_size + crate::PAGE_HEADER_SIZE
            >= (min_payload_fraction * page_size as f32) as usize)
}

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

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum RebalanceMethod {
    PostDelete,
    PostInsert,
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
    Vi: Cell<Key = K, Data = PageId>,
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

                    current_overflow_id = if let Some(payload) = cell.payload_mut() {
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


    fn try_get_frame<FI: FileOps, M: MemoryPool>(&self, id: PageId, pager: &mut Pager<FI, M>) -> std::io::Result<F>{
        let buf = pager.get_single(id)?;
        F::try_from(buf)
    }

    fn get_frame_unchecked<FI: FileOps, M: MemoryPool>(&self, id: PageId, pager: &mut Pager<FI, M>) -> F {
        let buf = pager.get_single(id).unwrap();
        F::try_from(buf).unwrap()
    }

    /// Insert, allowing to overflow.
    fn insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        value: Vl,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {

        // Navigate from the root to the leaf page.
        let traversal = self.traverse(key, pager);
        let leaf_frame = self.try_get_frame(*traversal.last().unwrap(), pager)?;


        // Check if inserting the page will produce an overflow.
        // Get the read lock only on this scope to minimize the time we are acquiring it.
        let will_overflow = {
            let leaf_lock = leaf_frame.read();
            leaf_lock.max_cell_size(self.max_payload_fraction) <= value.size()
        };


        // Now acquire the exclusive lock for the whole time that the insertion lasts, as we want to ensure no one else acquires it at mid time..
        if will_overflow {
            let mut write_lock = leaf_frame.write();

            if let Some((overflow_page, content)) =
                write_lock.try_insert_with_overflow_leaf(value, self.max_payload_fraction)?
            {
                self.create_overflow_chain(overflow_page, content, pager)?;
            }
        } else {
            let mut write_lock = leaf_frame.write();
            write_lock.insert(key, value)?;
        }


        // Overflow chain built, now store the current frame to the pager.
        // The possible overflow pages that have been created are stored within the
        // [`create_overflow_chain`]  function.
        self.store_frame(leaf_frame, pager)?;
        self.rebalance(traversal, pager, RebalanceMethod::PostInsert)?;

        Ok(())
    }

    /// Delete, allowing to underflow
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {

        // Start from the root and traverse to the leaf
        let traversal = self.traverse(key, pager);
        // Collect the leaf as the last node in the traversal.
        let leaf_frame =self.try_get_frame(*traversal.last().unwrap(), pager)?;
        {
            let mut leaf_guard = leaf_frame.write();
            leaf_guard.remove(&key);
        } // Release the lock and inmediately store the frame.
        self.store_frame(leaf_frame, pager)?;
        self.rebalance(traversal, pager, RebalanceMethod::PostDelete)?;
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

    pub(crate) fn rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        mut traversal: Vec<PageId>,
        pager: &mut Pager<FI, M>,
        method: RebalanceMethod,
    ) -> std::io::Result<()> {

        // First reverse the traversal.
        // We are going to traverse the tree in reverse topo order, fixing everything we fuck up on the way downwards as we go back towards the root.
        traversal.reverse();

        // Start traversing the tree upwards.
        for current_index in 0..traversal.len() {

            // Next index will be equal to the current index if there is only one node to traverse.
            let next_index = (current_index + 1).clamp(0, traversal.len() - 1);
            let node = traversal[current_index];
            let parent = traversal[next_index];

            if current_index == 0 {
                // First item. It will always be a leaf node so we need to call rebalance for a leaf on this case.
                match method {
                    RebalanceMethod::PostInsert => {
                        self.rebalance_post_insert_leaf(node, parent, pager)?
                    }
                    RebalanceMethod::PostDelete => self.rebalance_post_delete_leaf(node, parent, pager)?,
                };
            } else {
                // The rest will always be interior nodes.
                match method {
                    RebalanceMethod::PostInsert => {
                        self.rebalance_post_insert_interior(node, parent, pager)?
                    },
                    RebalanceMethod::PostDelete => self.rebalance_post_delete_interior(node, parent, pager)?,
                };
            }
        }
        Ok(())
    }


    /// Attempts to borrow the cells from the siblings at both sides of a node.
    /// It is preferred to borrow than to merge as merging is more costly and also fucks up more things on the tree.
    /// The siblings are obtained by asking the parent.
    fn try_borrow_from_leaf_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller: &F,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {

        // Obtain the parent and the siblings. We only need a shared lock for that.
        let parent = self.try_get_frame(parent_id, pager)?;

        // Borrow the write guard on the parent. The lock is hold until this function ends to avoid any other thread to enter this critical section. We need to avoid any other else to looking/writing on the left sibling and the right sibling and also on ourselves, therefore it is more straightforward to just grab the lock on the parent from the beginning even though we are not going to modify it.
        let parent_lock = parent.write();
        let (left_sibling, right_sibling) = parent_lock.get_siblings_for(caller.id());

        // Grab the lock also on the main child which is the one is going to be changed.
        let mut caller_lock = caller.write();

        // If there is a right sibling, we first borrow from it by popping at the front.
        if let Some(right) = right_sibling {
            let right_page = self.try_get_frame(right, pager)?;

            // While we are on underflow state, we keep borrowing.
            while caller_lock.is_on_underflow_state(self.min_payload_fraction) {

                // Pop the cell at the front.
                if let Some(popped_cell) =
                    right_page.write().try_pop_front_leaf(self.min_payload_fraction)
                {
                    caller_lock
                        .insert(popped_cell.key(), popped_cell)?;
                } else {
                    // At the point we cannot borrow more, we stop trying, to avoid fucking up our sibling.
                    break;
                }
            }
            self.store_frame(right_page, pager)?;
        }

        // If we are still on underflow state, we need to go to the left node.
        if caller_lock.is_on_underflow_state(self.min_payload_fraction) {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(left) = left_sibling {
                let left_page = self.try_get_frame(left, pager)?;

                while caller_lock.is_on_underflow_state(self.min_payload_fraction) {

                    if let Some(popped_cell) =
                        left_page.write().try_pop_back_leaf(self.min_payload_fraction)
                    {
                        caller_lock
                            .insert(popped_cell.key(), popped_cell)?;
                    } else {
                        break;
                    }
                }

                self.store_frame(left_page, pager)?;
            }
        }
        Ok(())
    }

    fn try_borrow_from_interior_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller: &F,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let parent = self.try_get_frame(parent_id, pager)?;
        let (left_sibling, right_sibling) = parent.read().get_siblings_for(caller.id());

        // Borrow the lock on the parent and the caller.
        let parent_lock = parent.write();
        let mut caller_lock = caller.write();

        // If there is a right sibling, we first borrow from it by popping at the front, since we avoid the need to fix all pointers later.
        if let Some(right) = right_sibling {
            let right_page = self.try_get_frame(right, pager)?;
            // While we are on underflow state, we keep borrowing.
            while caller_lock.is_on_underflow_state(self.min_payload_fraction) {
                let mut right_page_lock = right_page.write();
                // Pop the cell at the front.
                if let Some(popped_cell) =
                    right_page_lock.try_pop_front_interior(self.min_payload_fraction)
                {
                    // As we will be inserting at our right, we need to make sure to not fuck up our right most child.
                    if let Some(right_child_ours) = caller_lock.get_rightmost_child() {
                        caller_lock.insert_child(popped_cell.key(), right_child_ours)?;
                        caller_lock.set_rightmost_child(popped_cell.left_child().unwrap());
                    } else {
                        caller_lock
                            .insert_child(popped_cell.key(), popped_cell.left_child().unwrap())?;
                    }
                } else {
                    // At the point we cannot borrow more, we stop trying, to avoid fucking up our sibling.
                    break;
                }
            }
            self.store_frame(right_page, pager)?;
        }

        // If we are still on underflow state, we need to go to the left node.
        if caller_lock.is_on_underflow_state(self.min_payload_fraction) {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(left) = left_sibling {
                let left_page = self.try_get_frame(left, pager)?;

                while caller_lock.is_on_underflow_state(self.min_payload_fraction) {
                    let mut left_page_lock = left_page.write();
                    // Problem is that here we might be fucking up the right most child pointer of our sibling, so we need to unfuck it.
                    if let Some(popped_cell) =
                        left_page_lock.try_pop_back_interior(self.min_payload_fraction)
                    {
                        if let Some(right_child_left) = left_page_lock.get_rightmost_child() {
                            left_page_lock.set_rightmost_child(popped_cell.left_child().unwrap());

                            caller_lock.insert_child(popped_cell.key(), right_child_left)?;
                        } else {
                            caller_lock.insert_child(
                                popped_cell.key(),
                                popped_cell.left_child().unwrap(),
                            )?;
                        }
                    } else {
                        break;
                    }
                }

                self.store_frame(left_page, pager)?;
            }
        }
        Ok(())
    }


    // Converts the right most child of an interior page into an InteriorCell (Vi).
    // Allowing to insert it on interior nodes later.
    // The new cell will include a copy of the first key in the right most child and the child key itself, allowing to propagate that cell to the parent.
    fn collect_right_most_child<FI: FileOps, M: MemoryPool>(
        &mut self,
        page_number: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<Vi> {
        // Get the first child of this page.
        let child_frame = pager.get_single(page_number)?;

        let new_cell = if child_frame.is_interior() {
            let child = F::try_from(child_frame)?;
            let first_key = child.read().get_cell_at_interior(0).unwrap().key();
            Vi::create(first_key, Some(child.id()))
        } else {
            let child = F::try_from(child_frame)?;
            let first_key = child.read().get_cell_at_leaf(0).unwrap().key();
            Vi::create(first_key, Some(child.id()))
        };

        Ok(new_cell)
    }

    fn rebalance_post_delete_leaf<FI: FileOps, M: MemoryPool>(
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
        let (is_underflow, cell_count) = {
            let read_guard = caller.read();
            (
                read_guard.is_on_underflow_state(self.min_payload_fraction),
                read_guard.cell_count(),
            )
        };

        if !is_underflow || self.is_root(caller.id()) {
            return Ok(());
        }

        // PHASE 1: We first need to check if we can borrow from our siblings.
        self.try_borrow_from_leaf_siblings(&caller, parent_id, pager)?;

        // PHASE 2: If still on underflow state, we are fucked up.
        let still_underflow = caller
            .read()
            .is_on_underflow_state(self.min_payload_fraction);

        // NO OTHER OPPORTUNITY THAN MERGING WITH OUR FRIENDS AND REMOVING OUR KEY FROM THE PARENT.
        if still_underflow {

            let parent = self.try_get_frame(parent_id, pager)?;
            let parent_cell_count = parent.read().cell_count();

            {
                // We want to make sure no one reads this part of the tree while we are here.
                // We will be modifying the siblings, the parent and the caller node and therefore no other thread must surpass this critical section.
                let mut parent_lock = parent.write();
                let (left_sibling, right_sibling) = parent_lock.get_siblings_for(caller.id());


                // Detach the caller from the tree.
                let mut all_cells = caller.write().take_leaf_cells();
                parent_lock.remove_child(caller.id());


                // Also, if there are left or right siblings, detach them from the tree too and collect their cells.
                if let Some(left) = left_sibling {
                    let left_frame = pager.get_single(left)?;
                    let left_page = F::try_from(left_frame)?;
                    all_cells.extend(left_page.write().take_leaf_cells());
                    parent_lock.remove_child(left);
                    self.store_frame(left_page, pager)?;

                };
                if let Some(right) = right_sibling {
                    let right_frame = pager.get_single(right)?;
                    let right_page = F::try_from(right_frame)?;
                    all_cells.extend(right_page.write().take_leaf_cells());
                    parent_lock.remove_child(right);
                    self.store_frame(right_page, pager)?;

                };



                // If collected size fits in a single page we are done.
                let total_size_valid = check_valid_size(
                    &all_cells,
                    self.max_payload_fraction,
                    self.min_payload_fraction,
                    caller.read().page_size(),
                );



                if total_size_valid {

                    // All cells fit in a single page, therefore we put them on the caller and that's all folks.
                    all_cells.sort_by_key(|cell| cell.key());
                    let last_cell = all_cells.pop().unwrap();
                    parent_lock.insert_child(last_cell.key(), caller_id)?;
                    for cell in all_cells {
                        caller
                            .write()
                            .insert(cell.key(), cell)?;
                    }

                    return Ok(());
                }

                // Cells do not fit on a single page. Therefore we need to redistribute.
                // We have collected cells of three different pages, therefore if we distribute them accross two, we 'should' end with a balanced tree.
                // Now we can proceed distributing the cells.
                let (mut splitted_left, mut splitted_right) = split_cells_by_size(all_cells);


                // This should mostly never happen but I am unsure if it can occur.
                debug_assert!(check_valid_size(&splitted_left, self.max_payload_fraction, self.min_payload_fraction,   caller.read().page_size()), "Oh no , size for left cells is not valid");
                debug_assert!(check_valid_size(&splitted_right, self.max_payload_fraction, self.min_payload_fraction,   caller.read().page_size()), "Oh no , size for right cells is not valid");

                let last_cell = splitted_left.pop().unwrap();
                parent_lock.insert_child(last_cell.key(), caller_id)?;

                for cell in splitted_left {
                    caller.write().insert(cell.key(), cell)?;
                }

                let new_sibling = F::try_from(pager.alloc_page(self.interior_page_type())?)?;
                let last_cell = splitted_right.pop().unwrap();
                parent_lock.insert_child(last_cell.key(), new_sibling.id())?;
                for cell in splitted_right {
                    new_sibling
                        .write()
                        .insert(cell.key(), cell)?;
                }
                self.store_frame(new_sibling, pager)?;
            }

            self.store_frame(parent, pager)?;
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn rebalance_post_delete_interior<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let frame = pager.get_single(caller_id)?;
        assert!(
            frame.is_interior(),
            "Cannot call rebalance-interior on an non-interior node. Use rebalance-leaf instead"
        );
        let caller = F::try_from(frame)?;
        let (is_underflow, cell_count, right_child) = {
            let read_guard = caller.read();
            (
                read_guard.is_on_underflow_state(self.min_payload_fraction),
                read_guard.cell_count(),
                read_guard.get_rightmost_child(),
            )
        };

        if !is_underflow && !self.is_root(caller.id()) {
            return Ok(());
        }

        // Root has underflow
        if self.is_root(caller.id()) {
            // Root is generally allowed to underflow. The only case where we need to modify when calling in the root is when it only has one child. This can happen when.
            // A) The root has only one cell and no right child.
            // B) The root has a right child and no cells.
            if cell_count == 1 && right_child.is_none() {
                let mut write_guard = caller.write();
                let cells = write_guard.take_interior_cells();
                assert!(
                    cells.len() == 1,
                    "The root should only have one cell at this point!"
                );
                self.set_root(cells[0].left_child().unwrap());
                // Deallocate the node here.
                // pager.dealloc_page(caller.id()) (NOT IMPLEMENTED YET)
            } else if cell_count == 0 && right_child.is_some() {
                self.set_root(right_child.unwrap());
                // Deallocate the node here.
                // pager.dealloc_page(caller.id()) (NOT IMPLEMENTED YET)
            }
            // Any other case there is nothing to do on the root.
        } else {
            // We are not the root, but we are known to be interior nodes.

            // PHASE 1: We first need to check if we can borrow from our siblings.
            self.try_borrow_from_interior_siblings(&caller, parent_id, pager)?;

            // PHASE 2: If still on underflow state, we are fucked up.
            let still_underflow = caller
                .read()
                .is_on_underflow_state(self.min_payload_fraction);

            // NO OTHER OPPORTUNITY THAN MERGING WITH OUR FRIENDS AND REMOVING OUR KEY FROM THE PARENT.
            if still_underflow {
                let parent_frame = self.try_get_frame(parent_id, pager)?;
                let parent_cell_count = parent_frame.read().cell_count();

                {
                    // We want to make sure no one reads this part of the tree while we are here.
                    let mut parent_lock = parent_frame.write();
                    let (left_sibling, right_sibling) = parent_lock.get_siblings_for(caller.id());
                    let mut all_cells = caller.write().take_interior_cells();
                    // Remove the invalid child at the beginning from the parent.
                    parent_lock.remove_child(caller.id());

                    let left_page_right = if let Some(left) = left_sibling {
                        let left_page = self.try_get_frame(left, pager)?;
                        all_cells.extend(left_page.write().take_interior_cells());
                        let left_right_most = left_page.read().get_rightmost_child();
                        parent_lock.remove_child(left);
                        self.store_frame(left_page, pager)?;
                        left_right_most
                    } else {
                        None
                    };

                    let right_page_right = if let Some(right) = right_sibling {
                        let right_page= self.try_get_frame(right, pager)?;
                        all_cells.extend(right_page.write().take_interior_cells());
                        let right_right_most = right_page.read().get_rightmost_child();
                        parent_lock.remove_child(right);
                        self.store_frame(right_page, pager)?;
                        right_right_most
                    } else {
                        None
                    };

                    // Fix all pointers of the three pages we likely fucked up.
                    // Collect all the right most children of all the nodes and insert them back in the list.
                    // Later during node rebuild, the nodes will be placed properly in the correct positions.
                    let my_child = caller.read().get_rightmost_child();
                    if let Some(child) = left_page_right {
                        // Get the first child of this page.
                        let new_cell = self.collect_right_most_child(child, pager)?;
                        all_cells.push(new_cell)
                    }

                    if let Some(child) = my_child {
                        let new_cell = self.collect_right_most_child(child, pager)?;
                        all_cells.push(new_cell)
                    }

                    if let Some(child) = right_page_right {
                        let new_cell = self.collect_right_most_child(child, pager)?;
                        all_cells.push(new_cell)
                    }

                    // If collected size fits in a single page we are done.
                    let total_size_valid = check_valid_size(
                        &all_cells,
                        self.max_payload_fraction,
                        self.min_payload_fraction,
                        caller.read().page_size(),
                    );


                    // If the total size is perfectly valid, we can determine we have finished.
                    if total_size_valid {
                        // All cells fit in a single page, therefore we put them on the caller and that's all folks.
                        all_cells.sort_by_key(|cell| cell.key());
                        let last_cell = all_cells.pop().unwrap();
                        caller
                            .write()
                            .set_rightmost_child(last_cell.left_child().unwrap());
                        parent_lock.insert_child(last_cell.key(), caller_id)?;
                        for cell in all_cells {
                            caller
                                .write()
                                .insert_child(cell.key(), cell.left_child().unwrap())?;
                        }

                        return Ok(());
                    }

                    // Now we can proceed distributing the cells.
                    let (mut splitted_left, mut splitted_right) = split_cells_by_size(all_cells);

                    let last_cell = splitted_left.pop().unwrap();
                    caller
                        .write()
                        .set_rightmost_child(last_cell.left_child().unwrap());
                    parent_lock.insert_child(last_cell.key(), caller_id)?;

                    for cell in splitted_left {
                        caller
                            .write()
                            .insert_child(cell.key(), cell.left_child().unwrap())?;
                    }

                    let new_sibling = F::try_from(pager.alloc_page(self.interior_page_type())?)?;
                    let last_cell = splitted_right.pop().unwrap();
                    new_sibling
                        .write()
                        .set_rightmost_child(last_cell.left_child().unwrap());
                    parent_lock.insert_child(last_cell.key(), new_sibling.id())?;

                    for cell in splitted_right {
                        new_sibling
                            .write()
                            .insert_child(cell.key(), cell.left_child().unwrap())?;
                    }
                    self.store_frame(new_sibling, pager)?;
                }

                self.store_frame(parent_frame, pager)?;
            }
        }

        self.store_frame(caller, pager)?;

        Ok(())
    }

    fn rebalance_post_insert_leaf<FI: FileOps, M: MemoryPool>(
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

    fn rebalance_post_insert_interior<FI: FileOps, M: MemoryPool>(
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


     #[test]
    fn test_insert_duplicate_keys() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        let row_id = RowId::from(1);
        let cell1 = create_test_cell(row_id, vec![1; 10]);
        let cell2 = create_test_cell(row_id, vec![2; 10]);

        btree.insert(row_id, cell1, &mut pager).unwrap();
        btree.insert(row_id, cell2, &mut pager).unwrap();

        let found = btree.search(row_id, &mut pager).unwrap();
        // Should have the latest inserted value
        assert_eq!(found.payload.as_bytes()[0], 2);
    }


     #[test]
    fn test_insert_varying_sizes() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        //
        for i in 1..=50 {
            let row_id = RowId::from(i);
            let size = (i * 20) as usize; // Tamaños de 20 a 1000 bytes
            let cell = create_test_cell(row_id, vec![i as u8; size]);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        for i in 1..=50 {
            let row_id = RowId::from(i);
            let found = btree.search(row_id, &mut pager);
            assert!(found.is_some());
            assert_eq!(found.unwrap().payload.as_bytes().len(), (i * 20) as usize);
        }
    }


     #[test]
    fn test_delete_single_element() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        let row_id = RowId::from(1);
        let cell = create_test_cell(row_id, vec![1; 10]);
        btree.insert(row_id, cell, &mut pager).unwrap();

        btree.delete(row_id, &mut pager).unwrap();

        assert!(btree.search(row_id, &mut pager).is_none());
    }


     #[test]
    fn test_delete_all_elements() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        for i in 1..=50 {
            let row_id = RowId::from(i);
            let cell = create_test_cell(row_id, vec![i as u8; 10]);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        for i in 1..=50 {
            let row_id = RowId::from(i);
            btree.delete(row_id, &mut pager).unwrap();
        }

        for i in 1..=50 {
            let row_id = RowId::from(i);
            assert!(btree.search(row_id, &mut pager).is_none());
        }
    }

    #[test]
    fn test_delete_and_reinsert() {
        let mut pager = setup_test_pager();
        let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

        for i in 1..=50 {
            let row_id = RowId::from(i);
            let cell = create_test_cell(row_id, vec![i as u8; 10]);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        // Eliminar la mitad
        for i in 1..=25 {
            let row_id = RowId::from(i);
            btree.delete(row_id, &mut pager).unwrap();
        }

        // Reinsertar con datos diferentes
        for i in 1..=25 {
            let row_id = RowId::from(i);
            let cell = create_test_cell(row_id, vec![i as u8 + 100; 10]);
            btree.insert(row_id, cell, &mut pager).unwrap();
        }

        // Verificar los nuevos datos
        for i in 1..=25 {
            let row_id = RowId::from(i);
            let found = btree.search(row_id, &mut pager).unwrap();
            assert_eq!(found.payload.as_bytes()[0], i as u8 + 100);
        }
    }

}
