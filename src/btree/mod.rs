use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::pager::Pager;
use crate::io::{Frame,  TableFrame};
use crate::storage::Cell;
use crate::storage::{InteriorPageOps, LeafPageOps};
use crate::types::{PageId, RowId};
use crate:: TableLeafCell;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub(crate) enum BTreeType {
    Table,
    Index,
}

pub(crate) struct BTree<K, V> {
    /// Id of the root page.
    pub(crate) root: PageId,
    pub(crate) tree_type: BTreeType,
    pub(crate) key_marker: std::marker::PhantomData<K>,
    pub(crate) val_marker: std::marker::PhantomData<V>,
}

impl<K, V> BTree<K, V>
where
    K: Ord,
    V: Cell,
{
    pub(crate) fn create_table<F: FileOps, M: MemoryPool>(
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<Self> {
        let id = pager.alloc_page(crate::PageType::TableLeaf)?.id();
        Ok(Self {
            root: id,
            tree_type: BTreeType::Table,
            key_marker: std::marker::PhantomData,
            val_marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn create_index<F: FileOps, M: MemoryPool>(
        pager: &mut Pager<F, M>,
    ) -> std::io::Result<Self> {
        let id = pager.alloc_page(crate::PageType::IndexLeaf)?.id();
        Ok(Self {
            root: id,
            tree_type: BTreeType::Index,
            key_marker: std::marker::PhantomData,
            val_marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn is_table(&self) -> bool {
        matches!(self.tree_type, BTreeType::Table)
    }

    pub(crate) fn is_index(&self) -> bool {
        matches!(self.tree_type, BTreeType::Index)
    }

    pub(crate) fn debug_assert_page_type(&self, frame: &crate::io::IOFrame) {
        debug_assert!(
            frame.is_table() && self.is_table() || frame.is_index() && self.is_index(),
            "Invalid frame: the tree is of type {:?}, but frame is of type: {:?}",
            self.tree_type,
            frame.page_type()
        );
    }

    pub(crate) fn search_table<F: FileOps, M: MemoryPool>(
        &self,
        key: RowId,
        pager: &mut Pager<F, M>,
    ) -> Option<TableLeafCell> {
        // Obtain the root page
        let root_page = pager.get_single(self.root).ok()?;

        self.debug_assert_page_type(&root_page);

        // Navigate to the corresponding leaf and return the cell in that key
        let leaf_frame = self.find_leaf_page(key, root_page, pager)?;

        // Look for the cell in the leaf
        let guard = leaf_frame.read();
        guard.find(&key).cloned()
    }

    fn find_leaf_page<F: FileOps, M: MemoryPool>(
        &self,
        key: RowId,
        mut current_frame: crate::io::IOFrame,
        pager: &mut Pager<F, M>,
    ) -> Option<TableFrame> {
        // If it is already a leaf, return
        if current_frame.is_leaf() {
            return TableFrame::try_from(current_frame).ok();
        }

        // Navigate through the subsequent pages.
        loop {
            let table_frame = TableFrame::try_from(current_frame).ok()?;

            // if it is a leaf, terminate.
            if table_frame.is_leaf() {
                return Some(table_frame);
            }

            // If it is interior, jump to the child
            let child_id = {
                let guard = table_frame.read();
                guard.find_child(&key)
            };

            // Go to the next page.
            current_frame = pager.get_single(child_id).ok()?;
        }
    }

}
