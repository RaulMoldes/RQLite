use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{Frame, IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::{split_cells_by_size, BTree, LockingOps, RebalanceMethod, RebalanceOps, SearchOps};

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
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
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

    /// Rebalance after inserting on a leaf node.
    fn rebalance_post_insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> InsertOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
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
    /// Insert, allowing to overflow.
    fn insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        value: Vl,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Navigate from the root to the leaf page.
        let mut traversal = self.traverse(key, pager);
        let leaf_node = traversal.last().unwrap();

        // Acquire the exclusive lock from the beginning of the transaction.
        let (mut w_lock, frame) = self.acquire_exclusive(*leaf_node, pager);

        // Check if inserting the page will produce an overflow.
        let will_overflow = w_lock.max_cell_size(self.max_payload_fraction) <= value.size();

        if will_overflow {
            if let Some((overflow_page, content)) =
                w_lock.try_insert_with_overflow_leaf(value, self.max_payload_fraction)?
            {
                self.create_overflow_chain(overflow_page, content, pager)?;
            }
        } else {
            w_lock.insert(key, value)?;
        }

        self.store_and_release(frame, w_lock, pager)?;
        // Overflow chain built, now store the current frame to the pager.
        // The possible overflow pages that have been created are stored within the
        // [`create_overflow_chain`]  function.
        // TODO: this could be improved further if we avoid releasing the lock until the rebalance method finishes.
        // Currently there is a gap between when the insert lock is released and the rebalance lock is acquired, which could cause race conditions if a reader enters in the middle.
        // Check if rebalancing is needed.
        self.rebalance(&mut traversal, pager, RebalanceMethod::PostInsert)?;

        Ok(())
    }

    /// Apply rebalancing method after inserting on a leaf node.
    /// The basic idea is the following:
    /// First, we attempt to insert part of our data on our right and left siblings.
    /// This must be donde very carefully, since we do not want to fuck them up.
    /// Once that is done, we most likely have missed our parent's pointers, so we call the [fix_parent_pointers] method on the parent to redistribute the parent cells in the proper way.
    fn rebalance_post_insert<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Rapid check to see if we need overflow.
        // The way the algorithm is designed, the caller will always have a lock in the corresponding write table at this point.
        let is_overflow_state = {
            let (r_lock, _) = self.acquire_readonly(caller_id, pager);
            r_lock.is_on_overflow_state(self.max_payload_fraction)
        };

        if !is_overflow_state {
            return Ok(());
        }

        // Root has overflown, then we need to split it moving half the cells to a new node and increasing tree height.
        if self.is_root(caller_id) {
            self.split_root(caller_id, pager)?;
        } else {
            // As always , we make fast checks without acquiring the exclusive lock to make sure other threads can actually use it.

            self.try_release_to_siblings(caller_id, parent_id, pager)?;

            let is_leaf = self.is_node_leaf(caller_id, pager);
            let is_interior = self.is_node_interior(caller_id, pager);
            // As always , we make fast checks without acquiring the exclusive lock to make sure other threads can actually use it.
            let (r_lock, frame) = self.acquire_shared(caller_id, pager);
            let still_overflow = r_lock.is_on_overflow_state(self.max_payload_fraction);

            // If we are not anymore on overflow state we can return early, releasing all locks in consequence.
            if !still_overflow {
                return Ok(());
            }

            // We are on overflow state, so we need to upgrade the shared lock.
            // First part of this algo is to split the main node into two halves.
            // We need to take the cells out in order for that to happen.
            // For this part, we create a new sibling with an exclusive lock and fix the pointer in the linked list of leaf nodes.
            // Then, we can simply insert corresponding cells on each node and finally [un-fuck]the fucked up pointers in the parent using our [fix_parent_pointers] function.
            let mut w_lock = self.upgrade_shared(r_lock);
            let sibling = self.alloc_page(w_lock.type_of(), pager)?;
            let mut sibling_w_lock = sibling.write();
            // Acquire a shared lock on the parent to get the right most child.
            let (mut parent_w_lock, parent_frame) = self.acquire_exclusive(parent_id, pager);

            // This splits the node, obtaining the cell in the middle. Probably can be included in a function that can be reused.
            let propagated_cell = if is_leaf {
                let taken_cells = w_lock.take_leaf_cells();
                let (left_cells, right_cells) = split_cells_by_size(taken_cells);

                // Fix the pointers in the linked list.
                if let Some(next_page) = w_lock.get_next() {
                    sibling_w_lock.set_next(next_page);
                    w_lock.set_next(sibling.id());
                }

                // We need to insert a notfication in the parent for him to 'know' that the sibling we have created is also his child. The most straightforward way is to insert the propagated key pointing to the new sibling. Then, the fix parent pointers function will take care of fixing everything up.
                let propagated_key = right_cells.last().unwrap().key();
                let propagated_id = sibling_w_lock.id();
                for cell in left_cells {
                    w_lock.insert(cell.key(), cell)?;
                }

                for cell in right_cells {
                    sibling_w_lock.insert(cell.key(), cell)?;
                }

                self.store_and_release(frame, w_lock, pager)?;
                self.store_and_release(sibling, sibling_w_lock, pager)?;

                Vi::create(propagated_key, Some(propagated_id))
            } else if is_interior {
                let taken_cells = w_lock.take_interior_cells();
                let (left_cells, right_cells) = split_cells_by_size(taken_cells);

                // We need to insert a notfication in the parent for him to 'know' that the sibling we have created is also his child. The most straightforward way is to insert the propagated key pointing to the new sibling. Then, the fix parent pointers function will take care of fixing everything up.
                let propagated_key = right_cells.last().unwrap().key();
                let propagated_id = sibling_w_lock.id();

                for cell in left_cells {
                    w_lock.insert_child(cell)?;
                }

                for cell in right_cells {
                    sibling_w_lock.insert_child(cell)?;
                }
                self.fix_parent_pointers(frame, w_lock, pager)?;
                self.fix_parent_pointers(sibling, sibling_w_lock, pager)?;
                Vi::create(propagated_key, Some(propagated_id))
            } else {
                panic!(
                    "Invalid page type. {}, is not a valid page type for Btree pages",
                    w_lock.type_of()
                );
            };
            parent_w_lock.insert_child(propagated_cell)?;
            self.fix_parent_pointers(parent_frame, parent_w_lock, pager)?;
        }

        Ok(())
    }
}
