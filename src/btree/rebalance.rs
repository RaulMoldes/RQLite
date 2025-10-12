use super::BTree;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;


use super::{BorrowOpType, LockingOps, split_cells_by_size_evenly, RebalanceMethod, InsertOps, DeleteOps};

/// BTree rebalance operations go here.
pub(crate) trait RebalanceOps<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
    Vl: Cell<Key = K>,
    Vi: Cell<Key = K, Data = PageId>,
    P: Send
        + Sync
        + HeaderOps
        + BTreePageOps
        + InteriorPageOps<Vi, KeyType = K>
        + LeafPageOps<Vl, KeyType = K>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
{
    // Utility to check if we can borrow a cell from a source node to a destination node.
    /// The op type marker indicates if the borrowing happens atthe front of the node (first cell), or at the back of it.
    /// Nodeids for source and destination are optional since this is intended to be used after the [get_siblings_for_node] call, which may return None if the node does not have siblings.
    fn can_borrow<FI: FileOps, M: MemoryPool>(
        &self,
        source_node: PageId,
        dest_node: PageId,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> bool;

    /// Try to release part of the data to your siblings.
    fn try_release_to_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Try to release part of the data to your siblings.
    fn try_borrow_from_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Attempt to borrow data from a source node to a destination node.
    fn try_borrow<FI: FileOps, M: MemoryPool>(
        &mut self,
        source_node: PageId,
        dest_node: PageId,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<bool>;


    fn force_rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Main rebalancing algo.
    fn rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        traversal: &mut [PageId],
        pager: &mut Pager<FI, M>,
        method: RebalanceMethod,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> RebalanceOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
    Vl: Cell<Key = K>,
    Vi: Cell<Key = K, Data = PageId>,
    P: Send
        + Sync
        + HeaderOps
        + BTreePageOps
        + InteriorPageOps<Vi, KeyType = K>
        + LeafPageOps<Vl, KeyType = K>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
{
    fn can_borrow<FI: FileOps, M: MemoryPool>(
        &self,
        source_node: PageId,
        dest_node: PageId,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> bool {
        // Check the type of the node.
        let is_leaf = self.is_node_leaf(source_node, pager);
        let is_interior = self.is_node_interior(source_node, pager);
        // Acquire read-only locks.
        let (source_rlock, _) = self.acquire_readonly(source_node, pager);
        let (dest_rlock, _) = self.acquire_readonly(dest_node, pager);
        // Validate that both nodes are the same type.
        assert_eq!(source_rlock.type_of(), dest_rlock.type_of(), "Cannot send cells between nodes of different types!. Source is of type {}, but destination is of type {}",source_rlock.type_of(), dest_rlock.type_of());
        let cell_count = source_rlock.cell_count() as u16;

        // Compute the size we are about to insert.
        let additional_size = if is_leaf {
            let cell = match op_type {
                BorrowOpType::Front => source_rlock.get_cell_at_leaf(0).unwrap(),
                BorrowOpType::Back => source_rlock.get_cell_at_leaf(cell_count - 1).unwrap(),
            };
            cell.size()
        } else if is_interior {
            let cell = match op_type {
                BorrowOpType::Front => source_rlock.get_cell_at_interior(0).unwrap(),
                BorrowOpType::Back => source_rlock.get_cell_at_interior(cell_count - 1).unwrap(),
            };
            cell.size()
        } else {
            panic!(
                "Invalid type of node. Node was not a valid Btree node: {}",
                source_rlock.type_of()
            )
        };

        let fits = dest_rlock.fits_in(additional_size, self.max_payload_fraction);
        // Get the final result based on op type and the size we are about to insert.
        (matches!(op_type, BorrowOpType::Front)
            && source_rlock.can_pop_at_front(self.max_payload_fraction)
            && fits)
            || (matches!(op_type, BorrowOpType::Back)
                && source_rlock.can_pop_at_back(self.max_payload_fraction)
                && fits)
    }

    // Attempt to borrow a single cell from a source node to a dest node.
    fn try_borrow<FI: FileOps, M: MemoryPool>(
        &mut self,
        source_node: PageId,
        dest_node: PageId,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<bool> {
        if !self.can_borrow(source_node, dest_node, op_type, pager) {
            return Ok(false);
        }
        let is_leaf = self.is_node_leaf(source_node, pager);

        if is_leaf {
            let (mut source_wlock, source_frame) = self.acquire_exclusive(source_node, pager);

            let some_cell = match op_type {
                BorrowOpType::Front => source_wlock.try_pop_front_leaf(self.min_payload_fraction),
                BorrowOpType::Back => source_wlock.try_pop_back_leaf(self.min_payload_fraction),
            };

            if let Some(popped_cell) = some_cell {
                let (mut dest_wlock, dest_frame) = self.acquire_exclusive(dest_node, pager);
                // Acquire the write lock atomically and insert the cell.
                // It should be safe to do it as we have already checked it, but double checking is no issue too.
                if dest_wlock
                    .try_insert(
                        popped_cell.key(),
                        popped_cell.clone(),
                        self.max_payload_fraction,
                    )
                    .is_err()
                {
                    // If we do not succeed on our attempt to insert the cell we just force the re-insertion on the caller.
                    source_wlock.insert(popped_cell.key(), popped_cell)?;
                    // We need to make sure to downgrade the locks at the end of each iteration to avoid deadlocks. This should be made easier when i introduce the lock manager.
                    return Ok(false);
                }

                // Release the lock accordingly.
                self.store_and_release(source_frame, source_wlock, pager)?;
                self.store_and_release(dest_frame, dest_wlock, pager)?;
                return Ok(true);
            }
        // For interior nodes, insert and popping functions have different signature. Additionally, we should use the
        } else {
            let (mut source_wlock, source_frame) = self.acquire_exclusive(source_node, pager);

            let some_cell = match op_type {
                BorrowOpType::Front => {
                    source_wlock.try_pop_front_interior(self.min_payload_fraction)
                }
                BorrowOpType::Back => source_wlock.try_pop_back_interior(self.min_payload_fraction),
            };

            if let Some(popped_cell) = some_cell {
                let (mut dest_wlock, dest_frame) = self.acquire_exclusive(dest_node, pager);
                // Acquire the write lock atomically and insert the cell.
                // It should be safe to do it as we have already checked it, but double checking is no issue too.
                if dest_wlock
                    .try_insert_child(popped_cell.clone(), self.max_payload_fraction)
                    .is_err()
                {
                    // If we do not succeed on our attempt to insert the cell we just force the re-insertion on the caller.
                    source_wlock.insert_child(popped_cell)?;
                    // We need to make sure to downgrade the locks at the end of each iteration to avoid deadlocks. This should be made easier when i introduce the lock manager.
                    return Ok(false);
                }

                self.fix_parent_pointers(source_frame, source_wlock, pager)?;
                self.fix_parent_pointers(dest_frame, dest_wlock, pager)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Attempt to release data to your siblings on a leaf node.
    fn try_release_to_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Grab the exclusive lock on the parent, since we are going to be fucking up our siblings in the midtime we do not want any reader to access this section of the tree

        let (parent_wlock, parent_frame) = self.acquire_exclusive(parent_id, pager);
        let (left_sibling, right_sibling) = parent_wlock.get_siblings_for(caller_id);
        let is_leaf = self.is_node_leaf(caller_id, pager);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(left) = left_sibling {
            let mut stop = false;
            // While we are on overflow state, we keep borrowing.
            while !stop {
                let result = self.try_borrow(caller_id, left, BorrowOpType::Front, pager)?;
                let still_overflow = {
                    let (caller_rlock, _) = self.acquire_readonly(caller_id, pager);
                    caller_rlock.is_on_overflow_state(self.max_payload_fraction)
                };

                stop = !result || !still_overflow
            }
        }

        let still_overflow = {
            let (caller_rlock, _) = self.acquire_readonly(caller_id, pager);
            caller_rlock.is_on_overflow_state(self.max_payload_fraction)
        };

        // If we are still on overflow state, we need to go to the right node.
        if still_overflow {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(right) = right_sibling {
                let mut stop = false;
                // Start the loop.
                while !stop {
                    let result = self.try_borrow(caller_id, right, BorrowOpType::Back, pager)?;
                    let still_overflow = {
                        let (caller_rlock,_) = self.acquire_readonly(caller_id, pager);
                        caller_rlock.is_on_overflow_state(self.max_payload_fraction)
                    };

                    stop = !result || !still_overflow
                }
            }
        }

        // Fix the pointers in the parent node. This will also consume the lock
        self.fix_parent_pointers(parent_frame,parent_wlock, pager)?;
        Ok(())
    }

    fn try_borrow_from_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Grab the exclusive lock on the parent, since we are going to be fucking up our siblings in the midtime we do not want any reader to access this section of the tree

        let (parent_wlock, parent_frame) = self.acquire_exclusive(parent_id, pager);
        let (left_sibling, right_sibling) = parent_wlock.get_siblings_for(caller_id);
        let is_leaf = self.is_node_leaf(caller_id, pager);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(left) = left_sibling {
            let mut stop = false;
            // While we are on overflow state, we keep borrowing.
            while !stop {
                let result = self.try_borrow(left, caller_id, BorrowOpType::Back, pager)?;
                let still_overflow = {
                    let (caller_rlock, _) = self.acquire_readonly(caller_id, pager);
                    caller_rlock.is_on_overflow_state(self.max_payload_fraction)
                };

                stop = result && still_overflow
            }
        }

        let still_overflow = {
            let (caller_rlock, _) = self.acquire_readonly(caller_id, pager);
            caller_rlock.is_on_overflow_state(self.max_payload_fraction)
        };

        // If we are still on overflow state, we need to go to the right node.
        if still_overflow {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(right) = right_sibling {
                let mut stop = false;
                // Start the loop.
                while !stop {
                    let result = self.try_borrow(right, caller_id, BorrowOpType::Front, pager)?;
                    let still_overflow = {
                        let (caller_rlock, _) = self.acquire_readonly(caller_id, pager);
                        caller_rlock.is_on_overflow_state(self.max_payload_fraction)
                    };

                    stop = result && still_overflow
                }
            }
        }

        // Fix the pointers in the parent node. This will also consume the lock
        self.fix_parent_pointers(parent_frame, parent_wlock, pager)?;
        Ok(())
    }




    fn force_rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>{
        let is_leaf = self.is_node_leaf(caller_id, pager);


        let (mut parent_wlock, parent_frame) = self.acquire_exclusive(parent_id, pager);
        let mut parent_cells = parent_wlock.take_interior_cells();

        // Collect all the parent node's cells.
        // Also make sure to collect the right most child to guarantee we also take it into account.
        if let Some(child) = parent_wlock.get_rightmost_child() {
            let child_cell = self.collect_right_most_child(child, pager)?;
            parent_cells.push(child_cell);
        };

        if is_leaf {
            let mut all_cells= Vec::new();
            let mut all_childs = Vec::new();

            for cell in parent_cells {
                let child_id = cell.left_child().unwrap();

                let (mut child_wlock, child_frame) = self.acquire_exclusive(child_id, pager);
                let child_cells = child_wlock.take_leaf_cells();
                all_cells.extend(child_cells);
                all_childs.push(child_id);
                self.store_and_release(child_frame, child_wlock, pager)?;

            }

            let splitted_groups = split_cells_by_size_evenly(all_cells, all_childs.len());

            for (i, child) in all_childs.iter().enumerate() {
                let cells_for_child = &splitted_groups[i];
                let (mut child_wlock, child_frame) = self.acquire_exclusive(*child, pager);
                for cell in cells_for_child {
                    child_wlock.insert(cell.key(), cell.clone())?;
                }
                self.store_and_release(child_frame, child_wlock, pager)?;
                let max_key = cells_for_child.last().unwrap().key();
                let cell = Vi::create(max_key, Some(*child));
                parent_wlock.insert_child(cell)?;
            }


        }
        else {
            let mut all_cells= Vec::new();
            let mut all_childs = Vec::new();

            for cell in parent_cells {
                let child_id = cell.left_child().unwrap();

                let (mut child_wlock, child_frame) = self.acquire_exclusive(child_id, pager);
                let child_cells = child_wlock.take_interior_cells();
                all_cells.extend(child_cells);
                all_childs.push(child_id);
                self.fix_parent_pointers(child_frame, child_wlock, pager)?;

            }

            let splitted_groups = split_cells_by_size_evenly(all_cells, all_childs.len());

            for (i, child) in all_childs.iter().enumerate() {
                let cells_for_child = &splitted_groups[i];
                let (mut child_wlock, child_frame) = self.acquire_exclusive(*child, pager);
                for cell in cells_for_child {

                    child_wlock.insert_child(cell.clone())?;

                }
                self.fix_parent_pointers(child_frame, child_wlock, pager)?;
                let max_key = cells_for_child.last().unwrap().key();
                let cell = Vi::create(max_key, Some(*child));
                parent_wlock.insert_child(cell)?;
            }

        }

        self.fix_parent_pointers(parent_frame, parent_wlock, pager)?;
        Ok(())
    }



    fn rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        traversal: &mut [PageId],
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


                // First item. It will always be a leaf node so we need to call rebalance for a leaf on this case.
                match method {
                    RebalanceMethod::PostInsert => {
                        self.rebalance_post_insert(node, parent, pager)?
                    }
                    RebalanceMethod::PostDelete => {
                        self.rebalance_post_delete(node, parent, pager)?
                    }
                };

        }
        Ok(())
    }


}
