mod debug;
mod delete;
mod insert;
mod locking;
mod rebalance;
mod search;
mod utils;

#[cfg(test)]
mod tests;

use super::PageType;
use crate::btreepage::BTreePageOps;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{Frame, OverflowFrame};
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::Cell;
use crate::storage::{InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::{PageId, RowId, Splittable, VarlenaType};
use crate::{HeaderOps, OverflowPage};
use crate::{
    IndexInteriorCell, IndexLeafCell, RQLiteIndexPage, RQLiteTablePage, TableInteriorCell,
    TableLeafCell,
};

use parking_lot::{ArcRwLockWriteGuard, RawRwLock};

use std::collections::BTreeMap;

pub(crate) use delete::DeleteOps;
pub(crate) use insert::InsertOps;
pub(crate) use locking::LockingOps;
pub(crate) use rebalance::RebalanceOps;
pub(crate) use search::SearchOps;
pub(crate) use utils::*;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum BTreeType {
    Table,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum BorrowOpType {
    Front,
    Back,
}
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum RebalanceMethod {
    PostDelete,
    PostInsert,
}

#[derive(Debug)]
pub(crate) struct BTree<K, Vl, Vi, P: Send + Sync> {
    pub(crate) root: PageId,
    pub(crate) min_payload_fraction: f32,
    pub(crate) max_payload_fraction: f32,
    pub(crate) tree_type: BTreeType,
    __key_marker: std::marker::PhantomData<K>,
    __val_leaf_marker: std::marker::PhantomData<Vl>,
    __val_int_marker: std::marker::PhantomData<Vi>,
    __page_marker: std::marker::PhantomData<P>,
}

impl<K, Vl, Vi, P> BTree<K, Vl, Vi, P>
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
        })
    }

    pub(crate) fn is_table(&self) -> bool {
        matches!(self.tree_type, BTreeType::Table)
    }

    pub(crate) fn is_index(&self) -> bool {
        matches!(self.tree_type, BTreeType::Index)
    }

    pub(crate) fn debug_assert_page_type(&self, frame: &PageFrame<P>) {
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

    pub(crate) fn alloc_page<FI: FileOps, M: MemoryPool>(
        &self,
        page_type: PageType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<PageFrame<P>> {
        PageFrame::try_from(pager.alloc_page(page_type)?)
    }

    /// Reconstruct the overflow payload of a cell.
    /// Modifies the cell in place, creating a super big cell with all the merged payload content.
    /// The cell can then be used by the system for retrieving data to users.
    fn try_reconstruct_overflow_payload<C: Cell<Key = K>, FI: FileOps, M: MemoryPool>(
        &self,
        cell: &mut C,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        if let Some(overflowpage) = cell.overflow_page() {
            // If there is an overflow page, traverse the linked list of overflow pages to reconstruct the data.
            let mut current_overflow_id = Some(overflowpage);
            while let Some(overflow_id) = current_overflow_id {
                // Read the Overflow page
                let overflow_frame = self.try_get_overflow_frame(overflow_id, pager).unwrap();
                current_overflow_id = if let Some(payload) = cell.payload_mut() {
                    // There is no problem in relasing the lock inmediately on overflow pages since we have grabbed the lock of the main btree page that holds the content.
                    // No one will be able to enter any of the pages in the overflow list for write until we have released it.
                    let ovf_guard = overflow_frame.read();
                    let overflow_data = ovf_guard.data_owned().unwrap();
                    payload.merge_with(overflow_data);
                    ovf_guard.get_next_overflow()
                } else {
                    None
                }
            }
        }

        Ok(())
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    /// This is used exclusively for overflow pages as these pages are kinda special and we cannot use the tipical methods that are used on BTreePages.
    fn try_get_overflow_frame<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<OverflowFrame> {
        let buf = pager.get_single(id)?;
        OverflowFrame::try_from(buf)
    }

    /// Utility to get a page from the [Pager].
    fn try_get_frame<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<PageFrame<P>> {
        let buf = pager.get_single(id)?;
        PageFrame::try_from(buf)
    }



    fn fix_parent_pointers<FI: FileOps, M: MemoryPool>(
        &mut self,

        frame: PageFrame<P>,
        // Receives a exclusive lock on the parent which will be upgraded later.
        mut w_lock: ArcRwLockWriteGuard<RawRwLock, P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let mut max_keys: BTreeMap<K, PageId> = BTreeMap::new();

        let mut node_ids: Vec<PageId> = w_lock
            .take_interior_cells()
            .iter()
            .map(|vi| vi.left_child().unwrap())
            .collect(); // The most straightforward is to take all cells.

        if let Some(right_most) = w_lock.get_rightmost_child() {
            w_lock.set_rightmost_child(PageId::from(0));
            node_ids.push(right_most);
        }

        // PASS 1: Figures out which are the max key of each node.
        // Builds a [HashMap] in order to later move the cells to their specific places.
        for node_id in node_ids.drain(..) {
            let (child_r_lock,_) = self.acquire_readonly(node_id, pager); // We only require reading from the children here so there is no point in storing the write lock

            let child_cell_count = child_r_lock.cell_count() as u16;
            if child_cell_count < 1 {
                continue;
            };

            let max_key = match child_r_lock.type_of() {
                PageType::TableLeaf => child_r_lock
                    .get_cell_at_leaf(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::IndexLeaf => child_r_lock
                    .get_cell_at_leaf(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::TableInterior => child_r_lock
                    .get_cell_at_interior(child_cell_count - 1)
                    .unwrap()
                    .key(),
                PageType::IndexInterior => child_r_lock
                    .get_cell_at_interior(child_cell_count - 1)
                    .unwrap()
                    .key(),
                _ => panic!(
                    "Invalid children page type. {} is not a Btree  valid page",
                    child_r_lock.type_of()
                ),
            };
            max_keys.insert(max_key, node_id);
        }

        // The child at the last is the one with the highest keys, then we do not need to insert it.
        // Instead, it will become the next right-most-child of the parent.
        if let Some((last_key, last_page)) = max_keys.pop_last() {
            w_lock.set_rightmost_child(last_page);
        }

        // PASS 2: The [max_keys] BTreeMap should be now sorted based on max-key.
        // Then we can unfuck the parent by inserting the max key pointing to the corresponding child.
        for (propagated_key, page_id) in max_keys.into_iter() {
            let new_cell = Vi::create(propagated_key, Some(page_id));
            w_lock.insert_child(new_cell)?;
        }

        self.store_and_release(frame, w_lock, pager)?;

        Ok(())
    }

    fn split_root<FI: FileOps, M: MemoryPool>(
        &mut self,
        root: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Main node (old root) that will get splitted in two.
        let (mut w_lock, frame) = self.acquire_exclusive(root, pager);
        // Create two new pages, grabbing the locks mutably.
        // I prefer to grab the locks here from the beginning as it facilitates writing this and there is no deadlock risk, since we are the ones creating the nodes.
        let root = self.alloc_page(self.interior_page_type(), pager)?;
        let sibling = self.alloc_page(w_lock.type_of(), pager)?;
        // Grab the lock on the new root:
        let mut root_w_lock = root.write();
        // Finally, grab the lock on the righmost node, inserting all the keys there.
        let mut sibling_w_lock = sibling.write();

        // Take the cells outside the root node in order to redistribute them in two new child nodes that are going to be created. Here we need a new root and a new leaf.
        // The new root will have as children the old one (ourselves, caller) and our new sibling.
        // We also need to make sure to fix the pointers at the end.

        // CASE A: we are splitting a leaf node.
        if matches!(w_lock.type_of(), PageType::IndexLeaf)
            || matches!(w_lock.type_of(), PageType::TableLeaf)
        {
            // If it is the root, there is no point in grabbing the lock to the parent as that would cause a deadlock since if we are the root [parent_id == caller_id].
            // Then, we grab the lock with mutable access as usual.
            let taken_cells = w_lock.take_leaf_cells();
            // The split cells by size function splits the cells evenly to make sure the tree stays balanced afterwards.
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            // To maintain our left-biased distribution in the tree, we need to make sure that the propagated key is always a copy of the max key in the left side of the tree.
            // Remember this is a [BPLUSTREE] not a [BTREE], so the data resides only on the leaf nodes.
            let propagated_key = left_cells.last().unwrap().key();
            // Create the mid cell that will be propagated to the created root node and which acts as a separator key between the left and right siblings.
            let propagated = Vi::create(propagated_key, Some(w_lock.id()));
            root_w_lock.insert_child(propagated)?;
            // Also, set the right most child to point to the new node.
            root_w_lock.set_rightmost_child(sibling.id());

            // Now its time to insert the cells in their proper locations.
            // Left half contains the left side keys (key <= propagated_key)
            // So they will be inserted at the left side of the root.
            for cell in left_cells {
                w_lock.insert(cell.key(), cell)?;
            }

            for cell in right_cells {
                sibling_w_lock.insert(cell.key(), cell)?;
            }

            // Then, also we need to set the right most node of the leaf node to maintain our linked list of leaf nodes (for scan searches).
            w_lock.set_next(sibling.id());
            self.store_and_release(frame, w_lock, pager)?;
            self.store_and_release(sibling, sibling_w_lock, pager)?;
        }
        // CASE B: we are splitting an interior node
        else if matches!(w_lock.type_of(), PageType::IndexInterior)
            || matches!(w_lock.type_of(), PageType::TableInterior)
        {
            let taken_cells = w_lock.take_interior_cells();
            // The split cells by size function splits the cells evenly to make sure the tree stays balanced afterwards.
            let (left_cells, right_cells) = split_cells_by_size(taken_cells);
            // To maintain our left-biased distribution in the tree, we need to make sure that the propagated key is always a copy of the max key in the left side of the tree.
            let propagated_key = left_cells.last().unwrap().key();
            // Create the mid cell that will be propagated to the created root node and which acts as a separator key between the left and right siblings.
            let propagated = Vi::create(propagated_key, Some(w_lock.id()));
            root_w_lock.insert_child(propagated)?;
            // Also, set the right most child to point to the new node.
            root_w_lock.set_rightmost_child(sibling.id());
            // Now its time to insert the cells in their proper locations.
            // Left half contains the left side keys (key <= propagated_key)
            // So they will be inserted at the left side of the root.
            for cell in left_cells {
                w_lock.insert_child(cell)?;
            }

            for cell in right_cells {
                sibling_w_lock.insert_child(cell)?;
            }

            // Finally, unfuck the pointers in the old root, as they are interior nodes it is safer to store them this way.
            self.fix_parent_pointers(frame, w_lock, pager)?;
            self.fix_parent_pointers(sibling, sibling_w_lock, pager)?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid PageType for root node: {}", w_lock.type_of()),
            ));
        }


        self.set_root(root.id());
        // Finally, unfuck the pointers in the new root.
        self.fix_parent_pointers(root, root_w_lock, pager)?;

        Ok(())
    }

    pub(crate) fn is_node_leaf<FI: FileOps, M: MemoryPool>(
        &self,
        caller_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> bool {
        let page_type = {
            let (r_lock, _) = self.acquire_shared(caller_id, pager);
            r_lock.type_of()
        };

        matches!(page_type, PageType::IndexLeaf) || matches!(page_type, PageType::TableLeaf)
    }

    pub(crate) fn is_node_interior<FI: FileOps, M: MemoryPool>(
        &self,
        caller_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> bool {
        let page_type = {
            let (r_lock, _) = self.acquire_shared(caller_id, pager);
            r_lock.type_of()
        };

        matches!(page_type, PageType::IndexInterior) || matches!(page_type, PageType::TableInterior)
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
        let child = self.try_get_frame(page_number, pager)?;
        let new_cell = if child.is_interior() {
            let first_key = child.read().get_cell_at_interior(0).unwrap().key();
            Vi::create(first_key, Some(child.id()))
        } else {
            let first_key = child.read().get_cell_at_leaf(0).unwrap().key();
            Vi::create(first_key, Some(child.id()))
        };

        Ok(new_cell)
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
            self.store_overflow_page(overflow_page, pager)?;
            overflow_page = new_page;
            content = remaining_content;
        }

        self.store_overflow_page(overflow_page, pager)?;

        Ok(())
    }

    /// Save a table page to the pager
    fn store_overflow_page<FI: FileOps, M: MemoryPool>(
        &self,
        page: OverflowPage,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        use crate::serialization::Serializable;

        let page_id = page.id();
        let page_type = page.type_of();
        let mut buffer = Vec::with_capacity(pager.page_size() as usize);
        page.write_to(&mut buffer)?;

        // Create a frame and cache it.
        let frame = PageFrame::from_buffer_with_metadata(buffer, false, page_id, page_type);
        pager.cache_page(&frame);
        Ok(())
    }
}

pub(crate) type TableBTree = BTree<RowId, TableLeafCell, TableInteriorCell, RQLiteTablePage>;

pub(crate) type IndexBTree = BTree<VarlenaType, IndexLeafCell, IndexInteriorCell, RQLiteIndexPage>;
