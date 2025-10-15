use super::BTree;
use crate::btree::search::TraverseMode;
use crate::btree::TraverseStack;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::{RebalanceMethod, RebalanceOps, SearchOps};

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
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: From<PageFrame<P>>,
{
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;

    fn rebalance_post_delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: Option<PageId>,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> DeleteOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
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
    /// Delete, allowing to underflow
    fn delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Start from the root and traverse to the leaf
        let mut traversal = self.traverse(key, pager, TraverseMode::Write);
        let last_id = *traversal.last().unwrap();

        let cell_size = traversal.write(&last_id).find(&key).unwrap().size();
        let will_underflow = !traversal.write(&last_id).can_remove(cell_size);

        if !will_underflow {
            traversal.release_until(last_id, pager)?;
        };

        traversal.write(&last_id).remove(&key);
        self.rebalance(&mut traversal, pager, RebalanceMethod::PostDelete)?;
        traversal.cleanup(pager)?;
        Ok(())
    }

    fn rebalance_post_delete<FI: FileOps, M: MemoryPool>(
        &mut self,
        caller_id: PageId,
        parent_id: Option<PageId>,
        traversal: &mut TraverseStack<P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let (is_underflow, cell_count) = {
            let r_lock = traversal.write(&caller_id);
            (r_lock.is_on_underflow_state(), r_lock.cell_count())
        };

        let is_leaf = traversal.is_node_leaf(&caller_id);
        let is_interior = traversal.is_node_interior(&caller_id);
        let is_root = self.is_root(caller_id);

        if !is_underflow || is_root {
            return Ok(());
        }

        // PHASE 1: We first need to check if we can borrow from our siblings.
        self.try_borrow_from_sibling(caller_id, parent_id.unwrap(), traversal, pager)?;

        // PHASE 2: If still on underflow state, we are fucked up.
        let still_underflow = {
            let r_lock = traversal.write(&caller_id);
            r_lock.is_on_underflow_state()
        };

        // NO OTHER OPPORTUNITY THAN MERGING WITH OUR FRIENDS
        if still_underflow {
            self.balance_siblings(caller_id, parent_id.unwrap(), traversal, pager)?;
        }

        Ok(())
    }
}
