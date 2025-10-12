use super::BTree;
use crate::btree::LockingOps;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::{RebalanceOps, SearchOps, RebalanceMethod};

/// BTree  delete operations go here.
pub(crate) trait DeleteOps<K, Vl, Vi, P>
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
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    fn rebalance_post_delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> DeleteOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
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
    /// Delete, allowing to underflow
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Start from the root and traverse to the leaf
        let mut traversal = self.traverse(key, pager);
        // Collect the leaf as the last node in the traversal.
        let (mut leaf_wlock, leaf_frame) = self.acquire_exclusive(*traversal.last().unwrap(), pager);
        leaf_wlock.remove(&key);
        self.store_and_release(leaf_frame, leaf_wlock, pager)?;

        self.rebalance(&mut traversal, pager, RebalanceMethod::PostDelete)?;
        Ok(())
    }

    fn rebalance_post_delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let (is_underflow, cell_count) = {
            let (r_lock, _) = self.acquire_readonly(caller_id, pager);
            (
                r_lock.is_on_underflow_state(self.min_payload_fraction),
                r_lock.cell_count(),
            )
        };

        let is_leaf = self.is_node_leaf(caller_id, pager);
        let is_interior = self.is_node_interior(caller_id, pager);
        let is_root = self.is_root(caller_id);

        if !is_underflow || is_root  {
            return Ok(());
        }

        // PHASE 1: We first need to check if we can borrow from our siblings.
        self.try_borrow_from_siblings(caller_id, parent_id, pager)?;

        // PHASE 2: If still on underflow state, we are fucked up.
        let still_underflow = {
            let (r_lock, _) = self.acquire_readonly(caller_id, pager);
            r_lock.is_on_underflow_state(self.min_payload_fraction)
        };

        // NO OTHER OPPORTUNITY THAN MERGING WITH OUR FRIENDS
        if still_underflow {
            self.force_rebalance(caller_id, parent_id, pager)?;
        }

        Ok(())
    }
}
