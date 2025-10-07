pub mod btreepage;
pub mod cell;
pub mod header;
pub mod interiorpage;
pub mod leafpage;
pub mod overflowpage;
pub mod page_header;
pub mod slot;

pub(crate) use btreepage::*;
pub(crate) use cell::*;
pub(crate) use interiorpage::*;
pub(crate) use leafpage::*;
pub(crate) use overflowpage::*;
pub(crate) use page_header::*;
pub(crate) use slot::*;

use crate::serialization::Serializable;
use crate::types::{PageId, RowId, VarlenaType};
use crate::{impl_header_ops, impl_interior_page_ops, impl_leaf_page_ops};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) enum RQLiteTablePage {
    Leaf(TableLeafPage),
    Interior(TableInteriorPage),
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

impl RQLitePage {
    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            RQLitePage::Table(page) => page.is_leaf(),
            RQLitePage::Index(page) => page.is_leaf(),
            RQLitePage::Overflow(_) => false,
        }
    }

    pub(crate) fn is_interior(&self) -> bool {
        match self {
            RQLitePage::Table(page) => page.is_interior(),
            RQLitePage::Index(page) => page.is_interior(),
            RQLitePage::Overflow(_) => false,
        }
    }
}

pub(crate) enum RQLiteIndexPage {
    Leaf(IndexLeafPage),
    Interior(IndexInteriorPage),
}

impl RQLiteIndexPage {
    pub(crate) fn try_insert_with_overflow_interior(
        &mut self,
        content: IndexInteriorCell,
        max_payload_factor: u16,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteIndexPage::Leaf(_) => panic!("Invalid content. Content type must be IndexLeafCell when inserting on IndexLeafPages"),
            RQLiteIndexPage::Interior(page) => page.try_insert_with_overflow(content, max_payload_factor)
        }
    }

    pub(crate) fn try_insert_with_overflow_leaf(
        &mut self,
        content: IndexLeafCell,
        max_payload_factor: u16,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteIndexPage::Interior(_) => panic!("Invalid content. Content type must be IndexInteriorCell when inserting on IndexInteriorPages"),
            RQLiteIndexPage::Leaf(page) => page.try_insert_with_overflow(content, max_payload_factor)
        }
    }
}

impl RQLiteTablePage {
    pub(crate) fn try_insert_with_overflow_leaf(
        &mut self,
        content: TableLeafCell,
        max_payload_factor: u16,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            RQLiteTablePage::Interior(_) => {
                panic!("Invalid content. Table interior pages are not overflowable.")
            }
            RQLiteTablePage::Leaf(page) => {
                page.try_insert_with_overflow(content, max_payload_factor)
            }
        }
    }
}

pub(crate) enum RQLitePage {
    Index(RQLiteIndexPage),
    Table(RQLiteTablePage),
    Overflow(OverflowPage),
}

impl Serializable for RQLitePage {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut type_buffer = [0u8; 1];
        reader.read_exact(&mut type_buffer)?;
        let page_type = PageType::try_from(type_buffer[0]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize PageType from read buffer: {}", e),
            )
        })?;

        match page_type {
            PageType::Overflow => {
                let mut page = OverflowPage::read_from(reader)?;
                page.set_type(PageType::Overflow);
                Ok(RQLitePage::Overflow(page))
            }
            PageType::IndexInterior => {
                let mut page = IndexInteriorPage::read_from(reader)?;
                page.set_type(PageType::IndexInterior);
                Ok(RQLitePage::Index(RQLiteIndexPage::Interior(page)))
            }
            PageType::IndexLeaf => {
                let mut page = IndexLeafPage::read_from(reader)?;
                page.set_type(PageType::IndexLeaf);
                Ok(RQLitePage::Index(RQLiteIndexPage::Leaf(page)))
            }
            PageType::TableInterior => {
                let mut page = TableInteriorPage::read_from(reader)?;
                page.set_type(PageType::TableInterior);
                Ok(RQLitePage::Table(RQLiteTablePage::Interior(page)))
            }
            PageType::TableLeaf => {
                let mut page = TableLeafPage::read_from(reader)?;
                page.set_type(PageType::TableLeaf);
                Ok(RQLitePage::Table(RQLiteTablePage::Leaf(page)))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Page Type: {:?}", page_type),
            )),
        }
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RQLitePage::Overflow(page) => {
                // Write page type first
                writer.write_all(&[PageType::Overflow as u8])?;
                // Then the page data
                page.write_to(writer)
            }
            RQLitePage::Index(index_page) => match index_page {
                RQLiteIndexPage::Leaf(page) => {
                    writer.write_all(&[PageType::IndexLeaf as u8])?;
                    page.write_to(writer)
                }
                RQLiteIndexPage::Interior(page) => {
                    writer.write_all(&[PageType::IndexInterior as u8])?;
                    page.write_to(writer)
                }
            },
            RQLitePage::Table(table_page) => match table_page {
                RQLiteTablePage::Leaf(page) => {
                    writer.write_all(&[PageType::TableLeaf as u8])?;
                    page.write_to(writer)
                }
                RQLiteTablePage::Interior(page) => {
                    writer.write_all(&[PageType::TableInterior as u8])?;
                    page.write_to(writer)
                }
            },
        }
    }
}

impl RQLitePage {
    pub(crate) fn create(
        page_id: PageId,
        page_size: u32,
        page_type: PageType,
        right_most_page: Option<PageId>,
    ) -> std::io::Result<Self> {
        match page_type {
            PageType::Overflow => {
                let page = OverflowPage::create(page_id, page_size, page_type, right_most_page);
                Ok(RQLitePage::Overflow(page))
            }
            PageType::IndexInterior => {
                let page =
                    IndexInteriorPage::create(page_id, page_size, page_type, right_most_page);
                Ok(RQLitePage::Index(RQLiteIndexPage::Interior(page)))
            }
            PageType::IndexLeaf => {
                let page = IndexLeafPage::create(page_id, page_size, page_type, right_most_page);
                Ok(RQLitePage::Index(RQLiteIndexPage::Leaf(page)))
            }
            PageType::TableInterior => {
                let page =
                    TableInteriorPage::create(page_id, page_size, page_type, right_most_page);
                Ok(RQLitePage::Table(RQLiteTablePage::Interior(page)))
            }
            PageType::TableLeaf => {
                let page = TableLeafPage::create(page_id, page_size, page_type, right_most_page);
                Ok(RQLitePage::Table(RQLiteTablePage::Leaf(page)))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Page Type: {:?}", page_type),
            )),
        }
    }
}

impl From<RQLitePage> for RQLiteIndexPage {
    fn from(value: RQLitePage) -> Self {
        match value {
            RQLitePage::Index(index) => index,
            _ => panic!("Invalid page variant. Expected IndexPage"),
        }
    }
}

impl From<RQLitePage> for RQLiteTablePage {
    fn from(value: RQLitePage) -> Self {
        match value {
            RQLitePage::Table(table) => table,
            _ => panic!("Invalid page variant. Expected TablePage"),
        }
    }
}

impl From<RQLitePage> for OverflowPage {
    fn from(value: RQLitePage) -> Self {
        match value {
            RQLitePage::Overflow(ovf) => ovf,
            _ => panic!("Invalid page variant. Expected OverflowPage"),
        }
    }
}

impl From<RQLiteTablePage> for RQLitePage {
    fn from(value: RQLiteTablePage) -> Self {
        RQLitePage::Table(value)
    }
}

impl From<RQLiteIndexPage> for RQLitePage {
    fn from(value: RQLiteIndexPage) -> Self {
        RQLitePage::Index(value)
    }
}

impl From<OverflowPage> for RQLitePage {
    fn from(value: OverflowPage) -> Self {
        RQLitePage::Overflow(value)
    }
}

unsafe impl Send for RQLiteIndexPage {}
unsafe impl Send for RQLiteTablePage {}
unsafe impl Send for RQLitePage {}

unsafe impl Sync for RQLiteIndexPage {}
unsafe impl Sync for RQLiteTablePage {}
unsafe impl Sync for RQLitePage {}

impl_header_ops!(RQLiteTablePage { Leaf, Interior });
impl_header_ops!(RQLiteIndexPage { Leaf, Interior });
impl_header_ops!(RQLitePage {
    Index,
    Table,
    Overflow
});

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

impl Overflowable for RQLitePage {
    type Content = VarlenaType;
    fn try_insert_with_overflow(
        &mut self,
        content: Self::Content,
        max_payload_factor: u16,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        match self {
            Self::Overflow(page) => page.try_insert_with_overflow(content, max_payload_factor),
            _ => panic!("Cannot insert with overflow on non-overflow pages. Use try_insert_with_overflow_interior or try_insert_with_overflow_leaf instead.")
        }
    }
}

impl RQLiteTablePage {
    pub(crate) fn get_cell_at_leaf(&self, id: u16) -> Option<TableLeafCell> {
        match self {
            RQLiteTablePage::Leaf(page) => page.get_cell_at(id),
            _ => None,
        }
    }

    pub(crate) fn get_cell_at_interior(&self, id: u16) -> Option<TableInteriorCell> {
        match self {
            RQLiteTablePage::Interior(page) => page.get_cell_at(id),
            _ => None,
        }
    }

    pub(crate) fn take_cells_leaf(&mut self) -> Vec<TableLeafCell> {
        match self {
            RQLiteTablePage::Leaf(page) => page.take_cells(),
            _ => panic!("Invalid page type. For non leaf cellsuse take_cells_interior"),
        }
    }

    pub(crate) fn take_cells_interior(&mut self) -> Vec<TableInteriorCell> {
        match self {
            RQLiteTablePage::Interior(page) => page.take_cells(),
            _ => panic!("Invalid page type. For non leaf cellsuse take_cells_leaf"),
        }
    }
}

impl Serializable for RQLiteTablePage {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut type_buffer = [0u8; 1];
        reader.read_exact(&mut type_buffer)?;
        let page_type = PageType::try_from(type_buffer[0]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize PageType from read buffer: {}", e),
            )
        })?;

        match page_type {
            PageType::TableInterior => {
                let mut page = TableInteriorPage::read_from(reader)?;
                page.set_type(PageType::TableInterior);
                Ok(RQLiteTablePage::Interior(page))
            }
            PageType::TableLeaf => {
                let mut page = TableLeafPage::read_from(reader)?;
                page.set_type(PageType::TableLeaf);
                Ok(RQLiteTablePage::Leaf(page))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Page Type: {:?}", page_type),
            )),
        }
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RQLiteTablePage::Leaf(page) => {
                writer.write_all(&[PageType::TableLeaf as u8])?;
                page.write_to(writer)
            }
            RQLiteTablePage::Interior(page) => {
                writer.write_all(&[PageType::TableInterior as u8])?;
                page.write_to(writer)
            }
        }
    }
}

impl Serializable for RQLiteIndexPage {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut type_buffer = [0u8; 1];
        reader.read_exact(&mut type_buffer)?;
        let page_type = PageType::try_from(type_buffer[0]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize PageType from read buffer: {}", e),
            )
        })?;

        match page_type {
            PageType::IndexInterior => {
                let mut page = IndexInteriorPage::read_from(reader)?;
                page.set_type(PageType::IndexInterior);
                Ok(RQLiteIndexPage::Interior(page))
            }
            PageType::TableLeaf => {
                let mut page = IndexLeafPage::read_from(reader)?;
                page.set_type(PageType::TableLeaf);
                Ok(RQLiteIndexPage::Leaf(page))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Page Type: {:?}", page_type),
            )),
        }
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RQLiteIndexPage::Leaf(page) => {
                writer.write_all(&[PageType::TableLeaf as u8])?;
                page.write_to(writer)
            }
            RQLiteIndexPage::Interior(page) => {
                writer.write_all(&[PageType::TableInterior as u8])?;
                page.write_to(writer)
            }
        }
    }
}
