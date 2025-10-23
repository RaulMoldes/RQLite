use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use super::locking::TraverseStack;
use super::BTree;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[repr(u8)]
pub(crate) enum TraverseMode {
    Read,
    Upgradable,
    Write,
}
pub(crate) trait SearchOps<K, Vl, Vi, P>
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
    /// Search for a cell by its Key.
    /// If the cell happens to have a linked overflow chain, merge all the pages in the chain to return the reconstructed payload
    fn search<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> Option<Vl>;

    /// Scan the tree for a list of cells.
    fn scan<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        num_records: usize,
        pager: &mut Pager<FI, M>,
    ) -> Vec<Vl>;

    /// Navigate down to the leaf that should contain the key
    /// Accumulates the visited pages in a [`Vec<PageId>`] in order to be able to later traverse backwards without recursing.
    fn traverse<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
        mode: TraverseMode,
    ) -> TraverseStack<P>;
}

impl<K, Vl, Vi, P> SearchOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
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
    /// Search for a cell by its Key.
    /// If the cell happens to have a linked overflow chain, merge all the pages in the chain to return the reconstructed payload.
    fn search<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
    ) -> Option<Vl> {
        let traversal = self.traverse(key, pager, TraverseMode::Read);
        if let Some(last_node) = traversal.last() {
            let r_lock = traversal.read(last_node);
            if let Some(mut cell) = r_lock.find(&key).cloned() {
                // Modifies the cell in place traversing the linked list of overflow pages in case there is one.
                self.try_reconstruct_overflow_payload(&mut cell, pager).ok();
                return Some(cell);
            }
        }
        None
    }

    /// Scan the tree, collecting [num_records] cells using the pointers that exist between leaves.
    /// Need to test what happens when the tree grows over height == 2, but for now this seems valid.
    /// The method navigates to the first cell, identified by key, and then scans over all leaf pages collecting as many cells as required.
    /// Once it has collected enough, it stops the scan,
    ///
    /// TODO: It might be useful to add a parameter to determine if we want to scan the whole database until the end. Currently this can be done by setting num_records to a very large value.
    fn scan<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        num_records: usize,
        pager: &mut Pager<FI, M>,
    ) -> Vec<Vl> {
        let mut traversal = self.traverse(key, pager, TraverseMode::Read);
        let mut output = Vec::with_capacity(num_records);

        let last_node = match traversal.last() {
            Some(node) => node,
            None => return output,
        };

        let mut current_page = Some(*last_node);

        while let Some(page_id) = current_page {
            traversal.track_rlatch(page_id, pager);
            let r_lock = traversal.read(&page_id);

            // Collect cells till we reach the limit
            let remaining = num_records - output.len();
            output.extend(r_lock.scan(key).take(remaining).cloned());

            // If limit is reached, terminate
            if output.len() >= num_records {
                return output;
            }

            current_page = r_lock.get_next();
        }

        output
    }

    fn traverse<FI: FileOps, M: MemoryPool>(
        &mut self,
        key: K,
        pager: &mut Pager<FI, M>,
        mode: TraverseMode,
    ) -> TraverseStack<P> {
        let mut traversal = TraverseStack::default();
        let mut current_id = self.root;

        // The idea here is to traverse the tree downwards acquiring upgradable locks as needed.
        // Once we visit a node, we inmediately release the readonly lock to ensure other threads are able to visit that part of the tree and maximize parallelism.
        loop {
            // Collect the visited node in the traversal.
            match mode {
                TraverseMode::Read => traversal.track_rlatch(current_id, pager),
                TraverseMode::Upgradable => traversal.track_slatch(current_id, pager),
                TraverseMode::Write => traversal.track_wlatch(current_id, pager),
            }

            // If we have reached a leaf, return.
            // The last lock will be released here.
            if traversal.is_node_leaf(&current_id) {
                return traversal;
            }
            // If we are not a leaf, navigate to the corresponding child.
            // After getting out of scope the lock of the parent will be released, and the lock on the child will be acquired.
            let child_id = traversal.navigate(&current_id, key).unwrap();
            current_id = child_id;
        }
    }
}
