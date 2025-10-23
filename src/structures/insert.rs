use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::{BTree, RebalanceMethod, RebalanceOps, SearchOps, TraverseStack};

pub(crate) trait InsertOps<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
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
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
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

        let will_need_rebalance = !traversal.write(&leaf_node).fits_in(value.size());

        if !will_need_rebalance {
            traversal.release_until(leaf_node, pager)?;
        };

        if will_overflow {
            if let Some((overflow_page, content)) =
                traversal.write(&leaf_node).try_insert_with_overflow_leaf(
                    value,
                    self.max_payload_fraction,
                    self.min_payload_fraction,
                )?
            {
                self.create_overflow_chain(overflow_page, content, pager)?;
            }
        } else {
            traversal.write(&leaf_node).insert(value)?;
        }

        let needs_rebalance = traversal.write(&leaf_node).is_on_overflow_state();

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
            w_lock.is_on_overflow_state()
        };

        if !is_overflow_state {
            return Ok(());
        }

        // Root has overflown, then we need to split it moving half the cells to a new node and increasing tree height.
        if self.is_root(caller_id) {
            self.balance_deeper(caller_id, traversal, pager)?;
        } else {
            // As always , we make fast checks without acquiring the exclusive lock to make sure other threads can actually use it.
            // If we are not the root the parent id should be set.
            assert!(
                parent_id.is_some(),
                "Cannot rebalance without the parent information"
            );

            self.try_release_to_sibling(caller_id, parent_id.unwrap(), traversal, pager)?;

            let still_overflow = traversal.write(&caller_id).is_on_overflow_state();

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
            // This splits the node, propagating the cell in the middle.
            self.balance_siblings(caller_id, parent_id.unwrap(), traversal, pager)?;
        }

        Ok(())
    }
}
