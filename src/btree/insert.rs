use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::{split_cells_by_size, BTree, RebalanceMethod, RebalanceOps, SearchOps, TraverseStack};

pub(crate) trait InsertOps<K, Vl, Vi, P>
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
    IOFrame: From<PageFrame<P>>,
{
    /// Insert a cell at a leaf page given by the key.
    /// Will generally allow the page to overflow temporarily.
    /// The overflow state will be fixed later by the [rebalance] methods.
    fn insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        value: Vl,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Split the root, creating a new one when it overflows.
    fn split_root<FI: FileOps, M: MemoryPool>(
        &mut self,
        root: PageId,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    /// Rebalance after inserting on a leaf node.
    fn rebalance_post_insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: Option<PageId>,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> InsertOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
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
    IOFrame: From<PageFrame<P>>,
{
    /// Insert, allowing to overflow.
    fn insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        value: Vl,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Navigate from the root to the leaf page.
        let mut traversal = self.traverse(key, pager, super::search::TraverseMode::Write);
        let leaf_node = *traversal.last().unwrap();

        // Check if inserting the page will produce an overflow.
        let will_overflow = traversal
            .write(&leaf_node)
            .max_cell_size(self.max_payload_fraction)
            <= value.size();

        let will_need_rebalance = !traversal
            .write(&leaf_node)
            .fits_in(value.size(), self.max_payload_fraction);

        if !will_need_rebalance {
            traversal.release_until(leaf_node, pager)?;
        };

        if will_overflow {
            if let Some((overflow_page, content)) = traversal
                .write(&leaf_node)
                .try_insert_with_overflow_leaf(value, self.max_payload_fraction)?
            {
                self.create_overflow_chain(overflow_page, content, pager)?;
            }
        } else {
            traversal.write(&leaf_node).insert(key, value)?;
        }

        let needs_rebalance = traversal
            .write(&leaf_node)
            .is_on_overflow_state(self.max_payload_fraction);

        if !needs_rebalance {
            traversal.cleanup(pager)?;

            return Ok(());
        }

        // Overflow chain built, now store the current frame to the pager.
        // The possible overflow pages that have been created are stored within the
        // [`create_overflow_chain`]  function.
        // TODO: this could be improved further if we avoid releasing the lock until the rebalance method finishes.
        // Currently there is a gap between when the insert lock is released and the rebalance lock is acquired, which could cause race conditions if a reader enters in the middle.
        // Check if rebalancing is needed.

        self.rebalance(&mut traversal, pager, RebalanceMethod::PostInsert)?;

        traversal.cleanup(pager)?;

        Ok(())
    }

    fn split_root<FI: FileOps, M: MemoryPool>(
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
                traversal.write(&root).insert(cell.key(), cell)?;
            }

            for cell in right_cells {
                traversal.write(&sibling).insert(cell.key(), cell)?;
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

    /// Apply rebalancing method after inserting on a leaf node.
    /// The basic idea is the following:
    /// First, we attempt to insert part of our data on our right and left siblings.
    /// This must be donde very carefully, since we do not want to fuck them up.
    /// Once that is done, we most likely have missed our parent's pointers, so we call the [store_and_release_ordered] method on the parent to redistribute the parent cells in the proper way.
    fn rebalance_post_insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: Option<PageId>,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Rapid check to see if we need overflow.
        // The way the algorithm is designed, the caller will always have a lock in the corresponding write table at this point.
        let is_overflow_state = {
            let w_lock = traversal.write(&caller_id);
            w_lock.is_on_overflow_state(self.max_payload_fraction)
        };

        if !is_overflow_state {
            return Ok(());
        }

        // Root has overflown, then we need to split it moving half the cells to a new node and increasing tree height.

        if self.is_root(caller_id) {
            self.split_root(caller_id, traversal, pager)?;
        } else {
            // As always , we make fast checks without acquiring the exclusive lock to make sure other threads can actually use it.
            // If we are not the root the parent id should be set.
            assert!(
                parent_id.is_some(),
                "Cannot rebalance without the parent information"
            );
            self.try_release_to_siblings(caller_id, parent_id.unwrap(), traversal, pager)?;

            let is_leaf = traversal.is_node_leaf(&caller_id);
            let is_interior = traversal.is_node_interior(&caller_id);

            let still_overflow = traversal
                .write(&caller_id)
                .is_on_overflow_state(self.max_payload_fraction);

            // If we are not anymore on overflow state we can return early, releasing all locks in consequence.
            if !still_overflow {
                self.redistribute_pointers(parent_id.unwrap(), traversal, pager)?;
                return Ok(());
            }

            // We are on overflow state, so we need to continue rebalancing
            // First part of this algo is to split the main node into two halves.
            // We need to take the cells out in order for that to happen.
            // For this part, we create a new sibling with an exclusive lock and fix the pointer in the linked list of leaf nodes.
            // Then, we can simply insert corresponding cells on each node and finally [un-fuck] the fucked up pointers in the parent using our [store_and_release_ordered] function.
            //let page_type = traversal.write(&caller_id).type_of();
            // let sibling_id = traversal.allocate_for_write(page_type, pager)?;
            // Acquire a shared lock on the parent to get the right most child.

            // This splits the node, propagating the cell in the middle.
            self.force_rebalance(caller_id, parent_id.unwrap(), traversal, pager)?;
        }

        Ok(())
    }
}
