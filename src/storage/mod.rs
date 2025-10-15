pub mod btreepage;
pub mod cell;
pub mod free_page;
pub mod guards;
pub mod header;
pub mod interiorpage;
pub mod leafpage;
pub mod overflowpage;
pub mod page_header;
pub mod slot;

use crate::btreepage::BTreePageOps;
use crate::serialization::Serializable;
use crate::types::{PageId, RQLiteType, RowId, VarlenaType};
use crate::{
    impl_btree_page_ops, impl_header_ops_enum, impl_interior_page_ops, impl_leaf_page_ops,
};
pub(crate) use btreepage::*;
pub(crate) use cell::*;
pub(crate) use free_page::*;
pub(crate) use interiorpage::*;
pub(crate) use leafpage::*;
pub(crate) use overflowpage::*;
pub(crate) use page_header::*;
pub(crate) use slot::*;

#[cfg(test)]
mod tests;

pub(crate) enum RQLiteTablePage {
    Leaf(TableLeafPage),
    Interior(TableInteriorPage),
}

impl std::fmt::Debug for RQLiteTablePage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RQLiteTablePage::Leaf(page) => page.fmt(f),
            RQLiteTablePage::Interior(page) => page.fmt(f),
        }
    }
}

impl std::fmt::Debug for RQLiteIndexPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RQLiteIndexPage::Leaf(page) => page.fmt(f),
            RQLiteIndexPage::Interior(page) => page.fmt(f),
        }
    }
}

impl std::fmt::Debug for OverflowPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page id {}", self.id())?;
        write!(f, "Effective data len {}", self.data.effective_size())?;
        write!(f, "Data len {}", self.data.size_of())?;
        Ok(())
    }
}
impl RQLiteTablePage {
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self, RQLiteTablePage::Leaf(_))
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(self, RQLiteTablePage::Interior(_))
    }
}

impl RQLiteIndexPage {
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self, RQLiteIndexPage::Leaf(_))
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(self, RQLiteIndexPage::Interior(_))
    }
}

pub(crate) enum RQLiteIndexPage {
    Leaf(IndexLeafPage),
    Interior(IndexInteriorPage),
}

impl Overflowable for RQLiteIndexPage {
    type Content = IndexLeafCell;
    type InteriorContent = IndexInteriorCell;
    type LeafContent = IndexLeafCell;

    fn try_insert_with_overflow_interior(
        &mut self,
        content: IndexInteriorCell,
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteIndexPage::Leaf(_) => panic!("Invalid content. Content type must be IndexLeafCell when inserting on IndexLeafPages"),
            RQLiteIndexPage::Interior(page) => page.try_insert_with_overflow(content,max_payload_factor, min_payload_factor)
        }
    }

    fn try_insert_with_overflow_leaf(
        &mut self,
        content: IndexLeafCell,
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteIndexPage::Interior(_) => panic!("Invalid content. Content type must be IndexInteriorCell when inserting on IndexInteriorPages"),
            RQLiteIndexPage::Leaf(page) => page.try_insert_with_overflow(content, max_payload_factor, min_payload_factor)
        }
    }
}

impl Overflowable for RQLiteTablePage {
    type Content = TableLeafCell;
    type InteriorContent = TableInteriorCell;
    type LeafContent = TableLeafCell;

    fn try_insert_with_overflow_leaf(
        &mut self,
        content: TableLeafCell,
        max_payload_factor: u8,
        min_payload_factor: u8,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteTablePage::Interior(_) => {
                panic!("Invalid content. Table interior pages are not overflowable.")
            }
            RQLiteTablePage::Leaf(page) => {
                page.try_insert_with_overflow(content, max_payload_factor, min_payload_factor)
            }
        }
    }
}

unsafe impl Send for RQLiteIndexPage {}
unsafe impl Send for RQLiteTablePage {}

unsafe impl Sync for RQLiteIndexPage {}
unsafe impl Sync for RQLiteTablePage {}

impl_header_ops_enum!(RQLiteTablePage { Leaf, Interior });
impl_header_ops_enum!(RQLiteIndexPage { Leaf, Interior });

impl_leaf_page_ops!(RQLiteTablePage<TableLeafCell> {
    KeyType = RowId,
    Leaf = Leaf,
    Interior = Interior,
});

impl_interior_page_ops!(RQLiteTablePage<TableInteriorCell> {
    KeyType = RowId,
    Leaf = Leaf,
    Interior = Interior,
});

impl_btree_page_ops!(RQLiteTablePage {
    Leaf = Leaf,
    Interior = Interior,
});

impl_btree_page_ops!(RQLiteIndexPage {
    Leaf = Leaf,
    Interior = Interior,
});

impl_leaf_page_ops!(RQLiteIndexPage<IndexLeafCell> {
    KeyType = VarlenaType,
    Leaf = Leaf,
    Interior = Interior,
});

impl_interior_page_ops!(RQLiteIndexPage<IndexInteriorCell> {
    KeyType = VarlenaType,
    Leaf = Leaf,
    Interior = Interior,
});

impl RQLiteIndexPage {
    pub fn serialize<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RQLiteIndexPage::Interior(page) => page.write_to(writer)?,
            RQLiteIndexPage::Leaf(page) => page.write_to(writer)?,
        }
        Ok(())
    }
}

impl RQLiteTablePage {
    pub fn serialize<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RQLiteTablePage::Interior(page) => page.write_to(writer)?,
            RQLiteTablePage::Leaf(page) => page.write_to(writer)?,
        }
        Ok(())
    }
}
