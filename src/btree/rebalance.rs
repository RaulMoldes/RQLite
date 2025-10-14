use super::BTree;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;
use crate::PageType;
use std::collections::BTreeMap;

use super::{
    redistribute_cells, BorrowOpType, DeleteOps, InsertOps, RebalanceMethod, TraverseStack,
};

/// BTree rebalance operations go here.
pub(crate) trait RebalanceOps<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq + std::fmt::Debug,
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
        traversal: &mut TraverseStack<P>,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> bool;

    /// Try to release part of the data to your siblings.
    fn try_release_to_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Try to release part of the data to your siblings.
    fn try_borrow_from_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Attempt to borrow data from a source node to a destination node.
    fn try_borrow<FI: FileOps, M: MemoryPool>(
        &mut self,
        source_node: PageId,
        dest_node: PageId,
        traversal: &mut TraverseStack<P>,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<bool>;

    fn force_rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Main rebalancing algo.
    fn rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
        method: RebalanceMethod,
    ) -> std::io::Result<()>;

    fn redistribute_pointers<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    fn collect_right_most_child<FI: FileOps, M: MemoryPool>(
        &mut self,
        page_number: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<Vi>;
}

impl<K, Vl, Vi, P> RebalanceOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq + std::fmt::Debug,
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
        traversal: &mut TraverseStack<P>,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> bool {
        // Check the type of the node.
        let is_leaf = traversal.is_node_leaf(&source_node);
        let is_interior = traversal.is_node_interior(&source_node);

        // Validate that both nodes are the same type.
        assert_eq!(traversal.write(&source_node).type_of(), traversal.write(&dest_node).type_of(), "Cannot send cells between nodes of different types!. Source is of type {}, but destination is of type {}",traversal.write(&source_node).type_of(), traversal.write(&dest_node).type_of());
        let cell_count = traversal.write(&source_node).cell_count() as u16;

        // Compute the size we are about to insert.
        let additional_size = if is_leaf {
            let cell = match op_type {
                BorrowOpType::Front => traversal.write(&source_node).get_cell_at_leaf(0).unwrap(),
                BorrowOpType::Back => traversal
                    .write(&source_node)
                    .get_cell_at_leaf(cell_count - 1)
                    .unwrap(),
            };
            cell.size()
        } else if is_interior {
            let cell = match op_type {
                BorrowOpType::Front => traversal
                    .write(&source_node)
                    .get_cell_at_interior(0)
                    .unwrap(),
                BorrowOpType::Back => traversal
                    .write(&source_node)
                    .get_cell_at_interior(cell_count - 1)
                    .unwrap(),
            };
            cell.size()
        } else {
            panic!(
                "Invalid type of node. Node was not a valid Btree node: {}",
                traversal.write(&source_node).type_of()
            )
        };

        let fits = traversal
            .write(&dest_node)
            .fits_in(additional_size, self.max_payload_fraction);
        // Get the final result based on op type and the size we are about to insert.
        (matches!(op_type, BorrowOpType::Front)
            && traversal
                .write(&source_node)
                .can_pop_at_front(self.max_payload_fraction)
            && fits)
            || (matches!(op_type, BorrowOpType::Back)
                && traversal
                    .write(&source_node)
                    .can_pop_at_back(self.max_payload_fraction)
                && fits)
    }

    // Attempt to borrow a single cell from a source node to a dest node.
    fn try_borrow<FI: FileOps, M: MemoryPool>(
        &mut self,
        source_node: PageId,
        dest_node: PageId,
        traversal: &mut TraverseStack<P>,
        op_type: BorrowOpType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<bool> {
        if !self.can_borrow(source_node, dest_node, traversal, op_type, pager) {
            return Ok(false);
        }

        let is_leaf = traversal.is_node_leaf(&source_node);
        if is_leaf {
            let some_cell = match op_type {
                BorrowOpType::Front => traversal
                    .write(&source_node)
                    .try_pop_front_leaf(self.min_payload_fraction),
                BorrowOpType::Back => traversal
                    .write(&source_node)
                    .try_pop_back_leaf(self.min_payload_fraction),
            };

            if let Some(popped_cell) = some_cell {
                // Acquire the write lock atomically and insert the cell.
                // It should be safe to do it as we have already checked it, but double checking is no issue too.
                if traversal
                    .write(&dest_node)
                    .try_insert(
                        popped_cell.key(),
                        popped_cell.clone(),
                        self.max_payload_fraction,
                    )
                    .is_err()
                {
                    // If we do not succeed on our attempt to insert the cell we just force the re-insertion on the caller.
                    traversal
                        .write(&source_node)
                        .insert(popped_cell.key(), popped_cell)?;
                    // We need to make sure to downgrade the locks at the end of each iteration to avoid deadlocks. This should be made easier when i introduce the lock manager.
                    return Ok(false);
                }

                return Ok(true);
            }
        // For interior nodes, insert and popping functions have different signature. Additionally, we should use the
        } else {
            let some_cell = match op_type {
                BorrowOpType::Front => traversal
                    .write(&source_node)
                    .try_pop_front_interior(self.min_payload_fraction),
                BorrowOpType::Back => traversal
                    .write(&source_node)
                    .try_pop_back_interior(self.min_payload_fraction),
            };

            if let Some(popped_cell) = some_cell {
                // Acquire the write lock atomically and insert the cell.
                // It should be safe to do it as we have already checked it, but double checking is no issue too.
                if traversal
                    .write(&dest_node)
                    .try_insert_child(popped_cell.clone(), self.max_payload_fraction)
                    .is_err()
                {
                    // If we do not succeed on our attempt to insert the cell we just force the re-insertion on the caller.
                    traversal.write(&source_node).insert_child(popped_cell)?;
                    // We need to make sure to downgrade the locks at the end of each iteration to avoid deadlocks. This should be made easier when i introduce the lock manager.
                    return Ok(false);
                }

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
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Grab the exclusive lock on the parent, since we are going to be fucking up our siblings in the midtime we do not want any reader to access this section of the tree

        let (left_sibling, right_sibling) = traversal.write(&parent_id).get_siblings_for(caller_id);
        let is_leaf = traversal.is_node_leaf(&caller_id);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(left) = left_sibling {
            traversal.track_wlatch(left, pager);
            let mut stop = false;
            // While we are on overflow state, we keep borrowing.
            while !stop {
                let result =
                    self.try_borrow(caller_id, left, traversal, BorrowOpType::Front, pager)?;

                if result {

                    let caller_count = traversal.write(&caller_id).cell_count();
                    let left_count = traversal.write(&left).cell_count();

                };

                let still_overflow = {
                    let caller_wlock = traversal.write(&caller_id);
                    caller_wlock.is_on_overflow_state(self.max_payload_fraction)
                };

                stop = !result || !still_overflow
            }
        }

        let still_overflow = {
            let caller_wlock = traversal.write(&caller_id);
            caller_wlock.is_on_overflow_state(self.max_payload_fraction)
        };

        // If we are still on overflow state, we need to go to the right node.
        if still_overflow {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(right) = right_sibling {
                traversal.track_wlatch(right, pager);
                let mut stop = false;
                // Start the loop.
                while !stop {
                    let result =
                        self.try_borrow(caller_id, right, traversal, BorrowOpType::Back, pager)?;
                    let still_overflow = {
                        let caller_wlock = traversal.write(&caller_id);
                        caller_wlock.is_on_overflow_state(self.max_payload_fraction)
                    };

                    stop = !result || !still_overflow
                }
            }
        }
        Ok(())
    }

    fn try_borrow_from_siblings<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Grab the exclusive lock on the parent, since we are going to be fucking up our siblings in the midtime we do not want any reader to access this section of the tree

        let (left_sibling, right_sibling) = traversal.write(&parent_id).get_siblings_for(caller_id);
        let is_leaf = traversal.is_node_leaf(&caller_id);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(left) = left_sibling {
            traversal.track_wlatch(left, pager);
            let mut stop = false;
            // While we are on overflow state, we keep borrowing.
            while !stop {
                let result =
                    self.try_borrow(left, caller_id, traversal, BorrowOpType::Back, pager)?;
                let still_overflow = {
                    let caller_wlock = traversal.write(&caller_id);
                    caller_wlock.is_on_overflow_state(self.max_payload_fraction)
                };

                stop = result && still_overflow
            }
        }

        let still_overflow = {
            let caller_wlock = traversal.write(&caller_id);
            caller_wlock.is_on_overflow_state(self.max_payload_fraction)
        };

        // If we are still on overflow state, we need to go to the right node.
        if still_overflow {
            // If there is a left sibling, try borrow at the back of it.
            if let Some(right) = right_sibling {
                traversal.track_wlatch(right, pager);
                let mut stop = false;
                // Start the loop.
                while !stop {
                    let result =
                        self.try_borrow(right, caller_id, traversal, BorrowOpType::Front, pager)?;
                    let still_overflow = {
                        let caller_wlock = traversal.write(&caller_id);
                        caller_wlock.is_on_overflow_state(self.max_payload_fraction)
                    };

                    stop = result && still_overflow
                }
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
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<Vi> {
        // Get the first child of this page.
        traversal.track_wlatch(page_number, pager);

        let new_cell = if traversal.is_node_interior(&page_number) {
            let first_key = traversal
                .write(&page_number)
                .get_cell_at_interior(0)
                .unwrap()
                .key();
            Vi::create(first_key, Some(page_number))
        } else {
            let first_key = traversal
                .write(&page_number)
                .get_cell_at_leaf(0)
                .unwrap()
                .key();
            Vi::create(first_key, Some(page_number))
        };

        Ok(new_cell)
    }

    fn force_rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let is_leaf = traversal.is_node_leaf(&caller_id);

        let mut parent_cells = traversal.write(&parent_id).take_interior_cells();

        let page_size = traversal.write(&parent_id).page_size();

        // Collect all the parent node's cells.
        // Also make sure to collect the right most child to guarantee we also take it into account.
        if let Some(child) = traversal.write(&parent_id).get_rightmost_child() {
            let child_cell = self.collect_right_most_child(child, traversal, pager)?;
            parent_cells.push(child_cell);
        };

        if is_leaf {
            let mut all_cells = Vec::new();
            let mut all_childs = Vec::new();

            // For each cell on the parent keys, we grab the lock on the left child.
            // Track it on the traversal stack and take its cells.
            for cell in parent_cells {
                let child_id = cell.left_child().unwrap();
                traversal.track_wlatch(child_id, pager);
                let child_wlock = traversal.write(&child_id);
                let child_cells = child_wlock.take_leaf_cells();
                all_cells.extend(child_cells);
                all_childs.push(child_id);
            }

            // This call will split all the cells of the children on evently sized groups, ensuring the tree will rebalance properly.
            let splitted_groups = redistribute_cells(
                &mut all_cells,
                self.max_payload_fraction,
                self.min_payload_fraction,
                page_size,
            );
            let num_groups = splitted_groups.len();

            // If there are more groups than children we need to allocate more pages.
            while num_groups > all_childs.len() {
                let new_child = traversal.allocate_for_write(self.leaf_page_type(), pager)?;
                all_childs.push(new_child);
            }

            for (i, child_id) in all_childs.iter().enumerate() {
                if i <= num_groups.saturating_sub(1) {
                    let cells_for_child = &splitted_groups[i];
                    let total_size_for_child: usize =
                        cells_for_child.iter().map(|c| c.size()).sum();


                    for cell in cells_for_child {
                        let child_wlock = traversal.write(child_id);
                        child_wlock.insert(cell.key(), cell.clone())?;
                    }
                    if i < num_groups.saturating_sub(1) {
                        let next_child = all_childs[i + 1];
                        traversal.write(child_id).set_next(next_child);
                        let max_key = cells_for_child.last().unwrap().key();
                        let cell = Vi::create(max_key, Some(*child_id));
                        traversal.write(&parent_id).insert_child(cell)?;
                    } else {
                        traversal.write(&parent_id).set_rightmost_child(*child_id);
                    }
                } else {
                    // OTHERWISE WE NEED TO DEALLOCATE THE CHILD (TODO, NOT YET IMPLEMENTED IN THE PAGER)
                }
            }
        } else {
            let mut all_cells = Vec::new();
            let mut all_childs = Vec::new();

            for cell in parent_cells {
                let child_id = cell.left_child().unwrap();
                traversal.track_wlatch(child_id, pager);
                let child_wlock = traversal.write(&child_id);
                // Same as before but for interior children
                let child_cells = child_wlock.take_interior_cells();
                all_cells.extend(child_cells);
                all_childs.push(child_id);
            }

            let splitted_groups = redistribute_cells(
                &mut all_cells,
                self.max_payload_fraction,
                self.min_payload_fraction,
                page_size,
            );
            let num_groups = splitted_groups.len();
                        // If there are more groups than children we need to allocate more pages.
            while num_groups > all_childs.len() {
                let new_child = traversal.allocate_for_write(self.interior_page_type(), pager)?;
                all_childs.push(new_child);
            }

            for (i, child_id) in all_childs.iter().enumerate() {
                if i <= num_groups.saturating_sub(1) {
                    let cells_for_child = &splitted_groups[i];
                    let num_cells_for_child = cells_for_child.len();

                    for cell in cells_for_child {
                        let child_wlock = traversal.write(child_id);
                        child_wlock.insert_child(cell.clone())?;
                    }

                    if i < num_groups.saturating_sub(1) {
                        let max_key = cells_for_child.last().unwrap().key();
                        let cell = Vi::create(max_key, Some(*child_id));
                        traversal.write(&parent_id).insert_child(cell)?;
                    } else {
                        traversal.write(&parent_id).set_rightmost_child(*child_id);
                    }
                } else {
                    // OTHERWISE WE NEED TO DEALLOCATE THE CHILD (TODO, NOT YET IMPLEMENTED IN THE PAGER)
                }
            }
        }

        // Might not be needed
        self.redistribute_pointers(parent_id, traversal, pager)?;

        Ok(())
    }

    fn redistribute_pointers<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let mut max_keys: BTreeMap<K, PageId> = BTreeMap::new();

        // FIRST MUTABLE SCOPE
        let node_ids = {
            let w_lock = traversal.write(&id);

            let mut node_ids: Vec<PageId> = w_lock
                .take_interior_cells()
                .iter()
                .map(|vi| vi.left_child().unwrap())
                .collect(); // The most straightforward is to take all cells.

            if let Some(right_most) = w_lock.get_rightmost_child() {
                w_lock.set_rightmost_child(PageId::from(0));
                node_ids.push(right_most);
            }

            node_ids
        };

        // PASS 1: Figures out which are the max key of each node.
        // Builds a [HashMap] in order to later move the cells to their specific places.
        for (i, node_id) in node_ids.iter().enumerate() {
            let next_node_id = if i < (node_ids.len().saturating_sub(1)) {
                Some(node_ids[i + 1])
            } else {
                None
            };

            // We only require reading from the children here so there is no point in storing the write lock
            traversal.track_wlatch(*node_id, pager);
            let child_w_lock = traversal.write(node_id);
            let child_cell_count = child_w_lock.cell_count() as u16;
            if child_cell_count < 1 {
                continue;
            };

            if let Some(next) = next_node_id {
                child_w_lock.set_next(next);
            };

            let max_key = match child_w_lock.type_of() {
                PageType::TableLeaf => child_w_lock
                    .get_cell_at_leaf(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::IndexLeaf => child_w_lock
                    .get_cell_at_leaf(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::TableInterior => child_w_lock
                    .get_cell_at_interior(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::IndexInterior => child_w_lock
                    .get_cell_at_interior(child_cell_count - 1)
                    .unwrap()
                    .key(),
                _ => panic!(
                    "Invalid children page type. {} is not a Btree  valid page",
                    child_w_lock.type_of()
                ),
            };
            // Release the read latch before storing the key.

            max_keys.insert(max_key, *node_id);
        }

        // The child at the last is the one with the highest keys, then we do not need to insert it.
        // Instead, it will become the next right-most-child of the parent.
        if let Some((last_key, last_page)) = max_keys.pop_last() {
            let w_lock = traversal.write(&id);
            w_lock.set_rightmost_child(last_page);
        }

        // PASS 2: The [max_keys] BTreeMap should be now sorted based on max-key.
        // Then we can unfuck the parent by inserting the max key pointing to the corresponding child.
        for (propagated_key, page_id) in max_keys.into_iter() {
            let new_cell = Vi::create(propagated_key, Some(page_id));
            let w_lock = traversal.write(&id);
            w_lock.insert_child(new_cell)?;
        }

        Ok(())
    }

    fn rebalance<FI: FileOps, M: MemoryPool>(
        &mut self,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
        method: RebalanceMethod,
    ) -> std::io::Result<()> {
        let mut nodes_rebalance: Vec<PageId> = traversal.iter().copied().collect();
        nodes_rebalance.reverse();

        // Start traversing the tree upwards.
        for (index, child) in nodes_rebalance.iter().enumerate() {
            // First item. It will always be a leaf node so we need to call rebalance for a leaf on this case.
            let parent_node = if self.is_root(*child) {
                debug_assert!(index == nodes_rebalance.len() - 1);
                None
            } else {
                debug_assert!(index < nodes_rebalance.len() - 1);
                Some(nodes_rebalance[index + 1])
            };

            match method {
                RebalanceMethod::PostInsert => {
                    self.rebalance_post_insert(*child, parent_node, traversal, pager)?
                }
                RebalanceMethod::PostDelete => {
                    self.rebalance_post_delete(*child, parent_node, traversal, pager)?
                }
            };
        }

        Ok(())
    }
}
