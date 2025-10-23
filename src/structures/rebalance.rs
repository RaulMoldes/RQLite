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
    redistribute_cells, split_cells_by_size, DeleteOps, InsertOps, RebalanceMethod, TraverseStack,
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
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: From<PageFrame<P>>,
{
    // Utility to check if we can borrow a cell from a source node to a destination node.
    /// The op type marker indicates if the borrowing happens atthe front of the node (first cell), or at the back of it.
    /// Nodeids for source and destination are optional since this is intended to be used after the [get_siblings_for_node] call, which may return None if the node does not have siblings.
    fn can_borrow<FI: FileOps, M: MemoryPool>(
        &self,
        source_node: PageId,
        dest_node: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> (bool, u16);

    /// According to SQLITE documentation:
    ///
    /// The balance deeper sub-algorithm is used when the root page of a b-tree is overfull. It creates a new page and copies the entire contents of the overfull root page to it.
    /// The root page is then zeroed and the new page installed as its only child.
    /// The balancing algorithm is then run on the new child page (in case it is overfull).
    ///
    /// NOTE for me: The way I have implemented the B+Tree algo, the new allocated root child will always be overfull. In consequence, this algorithm distributes the cells between itself and a new node which will become the new RIGHT-MOST-CHILD of its parent.
    fn balance_deeper<FI: FileOps, M: MemoryPool>(
        &mut self,
        root: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Try to release part of the data to your right sibling.
    fn try_release_to_sibling<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Try to borrow part of the data from your left sibling.
    fn try_borrow_from_sibling<FI: FileOps, M: MemoryPool>(
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
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    fn balance_siblings<FI: FileOps, M: MemoryPool>(
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
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: From<PageFrame<P>>,
{
    fn can_borrow<FI: FileOps, M: MemoryPool>(
        &self,
        source_node: PageId,
        dest_node: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> (bool, u16) {
        // Check the type of the node.
        let is_leaf = traversal.is_node_leaf(&source_node);
        let is_interior = traversal.is_node_interior(&source_node);

        let source_is_overflow = traversal.write(&source_node).is_on_overflow_state();
        let dest_is_underflow = traversal.write(&dest_node).is_on_underflow_state();

        // Validate that both nodes are the same type.
        assert_eq!(traversal.write(&source_node).type_of(), traversal.write(&dest_node).type_of(), "Cannot send cells between nodes of different types!. Source is of type {}, but destination is of type {}",traversal.write(&source_node).type_of(), traversal.write(&dest_node).type_of());
        let cell_count = traversal.write(&source_node).cell_count();

        if !source_is_overflow && !dest_is_underflow {
            return (false, 0u16);
        };

        // Compute the size we are about to insert.
        let additional_size = if source_is_overflow {
            traversal.write(&source_node).get_overflow_size()
        } else {
            traversal.write(&dest_node).get_underflow_size()
        };

        let fits = traversal.write(&dest_node).fits_in(additional_size);

        let can_pop = traversal.write(&source_node).can_remove(additional_size);
        // Get the final result based on op type and the size we are about to insert.
        (fits && can_pop, additional_size)
    }

    fn balance_deeper<FI: FileOps, M: MemoryPool>(
        &mut self,
        root: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Main node (old root) that will get splitted in two is the caller id here.
        let old_root_type = traversal.write(&root).type_of();
        let is_leaf = traversal.is_node_leaf(&root);
        // Create two new pages, grabbing the locks mutably.
        // I prefer to grab the locks here from the beginning as it facilitates writing this and there is no deadlock risk, since we are the ones creating the nodes.
        let new_root = traversal.allocate_for_write(self.interior_page_type(), pager)?;
        let sibling = traversal.allocate_for_write(old_root_type, pager)?;

        // Take the cells outside the root node in order to redistribute them in two new child nodes that are going to be created. Here we need a new root and a new leaf.
        // The new root will have as children the old one (ourselves, caller) and our new sibling.
        // We also need to make sure to fix the pointers at the end.

        // CASE A: we are splitting a leaf node.
        if is_leaf {
            // If it is the root, there is no point in grabbing the lock to the parent as that would cause a deadlock since if we are the root [parent_id == caller_id].
            // Then, we grab the lock with mutable access as usual.
            let taken_cells = traversal.write(&root).take_leaf_cells();
            // The split cells by size function splits the cells evenly to make sure the tree stays balanced afterwards.
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            // To maintain our left-biased distribution in the tree, we need to make sure that the propagated key is always a copy of the max key in the left side of the tree.
            // Remember this is a [BPLUSTREE] not a [BTREE], so the data resides only on the leaf nodes.
            let propagated_key = left_cells.last().unwrap().key();
            // Create the mid cell that will be propagated to the created root node and which acts as a separator key between the left and right siblings.
            let propagated = Vi::create(propagated_key, Some(root));
            traversal.write(&new_root).insert_child(propagated)?;
            // Also, set the right most child to point to the new node.
            traversal.write(&new_root).set_rightmost_child(sibling);

            // Now its time to insert the cells in their proper locations.
            // Left half contains the left side keys (key <= propagated_key)
            // So they will be inserted at the left side of the root.
            for cell in left_cells {
                traversal.write(&root).insert(cell)?;
            }

            for cell in right_cells {
                traversal.write(&sibling).insert(cell)?;
            }

            // Then, also we need to set the right most node of the leaf node to maintain our linked list of leaf nodes (for scan searches).
            traversal.write(&root).set_next(sibling);
        }
        // CASE B: we are splitting an interior node
        else {
            let taken_cells = traversal.write(&root).take_interior_cells();
            // The split cells by size function splits the cells evenly to make sure the tree stays balanced afterwards.
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            // To maintain our left-biased distribution in the tree, we need to make sure that the propagated key is always a copy of the max key in the left side of the tree.
            let propagated_key = left_cells.last().unwrap().key();
            // Create the mid cell that will be propagated to the created root node and which acts as a separator key between the left and right siblings.
            let propagated = Vi::create(propagated_key, Some(root));
            traversal.write(&new_root).insert_child(propagated)?;
            // Also, set the right most child to point to the new node.
            traversal.write(&new_root).set_rightmost_child(sibling);
            // Now its time to insert the cells in their proper locations.
            // Left half contains the left side keys (key <= propagated_key)
            // So they will be inserted at the left side of the root.
            for cell in left_cells {
                traversal.write(&root).insert_child(cell)?;
            }

            for cell in right_cells {
                traversal.write(&sibling).insert_child(cell)?;
            }
        }

        self.set_root(new_root);

        Ok(())
    }

    // Attempt to borrow a single cell from a source node to a dest node.
    fn try_borrow<FI: FileOps, M: MemoryPool>(
        &mut self,
        source_node: PageId,
        dest_node: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let (can_borrow, borrow_size) = self.can_borrow(source_node, dest_node, traversal, pager);

        if !can_borrow {
            return Ok(());
        }

        let is_leaf = traversal.is_node_leaf(&source_node);
        if is_leaf {
            if let Some(popped_cells) = traversal.write(&source_node).try_pop(borrow_size) {
                // Acquire the write lock atomically and insert the cell.
                // It should be safe to do it as we have already checked it, but double checking is no issue too.
                if traversal
                    .write(&dest_node)
                    .try_insert(popped_cells.clone())
                    .is_err()
                {
                    // If we do not succeed on our attempt to insert the cell we just force the re-insertion on the caller.

                    for popped_cell in popped_cells {
                        traversal.write(&source_node).insert(popped_cell)?;
                    }
                    // We need to make sure to downgrade the locks at the end of each iteration to avoid deadlocks. This should be made easier when i introduce the lock manager.
                }
            }
        }
        Ok(())
    }

    /// Attempt to release data to your right sibling on a leaf node.
    fn try_release_to_sibling<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let is_leaf = traversal.is_node_leaf(&caller_id);

        if !is_leaf {
            return Ok(());
        };

        let (_, right_sibling) = traversal.write(&parent_id).get_siblings_for(caller_id);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(right) = right_sibling {
            traversal.track_wlatch(right, pager);
            self.try_borrow(caller_id, right, traversal, pager)?;
        };
        Ok(())
    }

    fn try_borrow_from_sibling<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Grab the exclusive lock on the parent, since we are going to be fucking up our siblings in the midtime we do not want any reader to access this section of the tree

        let (left_sibling, _) = traversal.write(&parent_id).get_siblings_for(caller_id);
        let is_leaf = traversal.is_node_leaf(&caller_id);

        // If there is a left sibling, we first borrow from it by popping from ourseleves at the front, since we avoid the need to fix all pointers later.
        if let Some(left) = left_sibling {
            traversal.track_wlatch(left, pager);
            self.try_borrow(left, caller_id, traversal, pager)?;
        };

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

    /// This is my implementation of https://sqlite.org/btreemodule.html#balance_siblings
    /// According to SQLITE documentation:
    /// The balance-siblings algorithm shall redistribute the b-tree cells currently stored on a overfull or underfull page and up to two sibling pages,
    /// adding or removing siblings as required,
    /// such that no sibling page is overfull and the minimum possible number of sibling pages is used to store the redistributed b-tree cells.
    fn balance_siblings<FI: FileOps, M: MemoryPool>(
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
                page_size as usize,
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

                    for cell in cells_for_child {
                        let child_wlock = traversal.write(child_id);
                        child_wlock.insert(cell.clone())?;
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
                page_size as usize,
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
            let child_cell_count = child_w_lock.cell_count();
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
