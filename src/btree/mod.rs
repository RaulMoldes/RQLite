mod debug;
mod delete;
mod insert;
mod locking;
mod rebalance;
mod search;
mod utils;

#[cfg(test)]
mod tests;

use crate::btreepage::BTreePageOps;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{Frame, OverflowFrame};
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::Cell;
use crate::storage::{InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::{PageId, RQLiteType, RowId, Splittable, VarlenaType};
use crate::{HeaderOps, OverflowPage};
use crate::{
    IndexInteriorCell, IndexLeafCell, RQLiteIndexPage, RQLiteTablePage, TableInteriorCell,
    TableLeafCell,
};

pub(crate) use delete::DeleteOps;
pub(crate) use insert::InsertOps;
pub(crate) use locking::TraverseStack;
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
                let overflow_frame = try_get_overflow_frame(overflow_id, pager).unwrap();
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
