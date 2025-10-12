use super::*;
use crate::serialization::Serializable;
use crate::storage::guards::*;
use std::io::Cursor;
use std::sync::Arc;

/// I really do not know a better way to do this.
impl TryFrom<IOFrame> for TableFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty();
        let page_type = io_frame.page_type();
        match Arc::try_unwrap(io_frame.into_inner()) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner();
                let mut reader = Cursor::new(buffer);

                let table_page = match page_type {
                    PageType::TableLeaf => {
                        let page = TableLeafPage::read_from(&mut reader)?;
                        RQLiteTablePage::Leaf(page)
                    }
                    PageType::TableInterior => {
                        let page = TableInteriorPage::read_from(&mut reader)?;
                        RQLiteTablePage::Interior(page)
                    }
                    _ => panic!(
                        "Invalid page type. Expected some of {} or {}, but found {}",
                        PageType::TableLeaf,
                        PageType::TableInterior,
                        page_type
                    ),
                };
                Ok(PageFrame::from_existent(table_page, is_dirty))
            }
            Err(arc) => {
                let guard = arc.read();
                let mut reader = Cursor::new(&guard[..]);

                let table_page = match page_type {
                    PageType::TableLeaf => {
                        let page = TableLeafPage::read_from(&mut reader)?;
                        RQLiteTablePage::Leaf(page)
                    }
                    PageType::TableInterior => {
                        let page = TableInteriorPage::read_from(&mut reader)?;
                        RQLiteTablePage::Interior(page)
                    }
                    _ => panic!(
                        "Invalid page type. Expected some of {} or {}, but found {}",
                        PageType::TableLeaf,
                        PageType::TableInterior,
                        page_type
                    ),
                };

                Ok(PageFrame::from_existent(table_page, is_dirty))
            }
        }
    }
}

impl TryFrom<IOFrame> for IndexFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty();
        let page_type = io_frame.page_type();
        match Arc::try_unwrap(io_frame.into_inner()) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner();
                let mut reader = Cursor::new(buffer);

                let index_page = match page_type {
                    PageType::IndexLeaf => {
                        let page = IndexLeafPage::read_from(&mut reader)?;

                        RQLiteIndexPage::Leaf(page)
                    }
                    PageType::IndexInterior => {
                        let page = IndexInteriorPage::read_from(&mut reader)?;

                        RQLiteIndexPage::Interior(page)
                    }
                    _ => panic!(
                        "Invalid page type. Expected some of {} or {}, but found {}",
                        PageType::IndexLeaf,
                        PageType::IndexInterior,
                        page_type
                    ),
                };

                Ok(PageFrame::from_existent(index_page, is_dirty))
            }
            Err(arc) => {
                let guard = arc.read();
                let mut reader = std::io::Cursor::new(&guard[..]);

                let index_page = match page_type {
                    PageType::IndexLeaf => {
                        let page = IndexLeafPage::read_from(&mut reader)?;
                        RQLiteIndexPage::Leaf(page)
                    }
                    PageType::IndexInterior => {
                        let page = IndexInteriorPage::read_from(&mut reader)?;

                        RQLiteIndexPage::Interior(page)
                    }
                    _ => panic!(
                        "Invalid page type. Expected some of {} or {}, but found {}",
                        PageType::IndexLeaf,
                        PageType::IndexInterior,
                        page_type
                    ),
                };

                Ok(PageFrame::from_existent(index_page, is_dirty))
            }
        }
    }
}

impl TryFrom<IOFrame> for OverflowFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty();
        let page_type = io_frame.page_type();
        match Arc::try_unwrap(io_frame.into_inner()) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner();
                let mut reader = Cursor::new(buffer);

                let overflow_page = match page_type {
                    PageType::Overflow => OverflowPage::read_from(&mut reader)?,
                    _ => panic!(
                        "Invalid page type. Expected some of {}, but found {}",
                        PageType::Overflow,
                        page_type
                    ),
                };
                Ok(PageFrame::from_existent(overflow_page, is_dirty))
            }
            Err(arc) => {
                let guard = arc.read();
                let mut reader = std::io::Cursor::new(&guard[..]);
                let overflow_page = match page_type {
                    PageType::Overflow => OverflowPage::read_from(&mut reader)?,
                    _ => panic!(
                        "Invalid page type. Expected some of {}, but found {}",
                        PageType::Overflow,
                        page_type
                    ),
                };
                Ok(PageFrame::from_existent(overflow_page, is_dirty))
            }
        }
    }
}

impl TryFrom<TableFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(table_frame: TableFrame) -> Result<Self, Self::Error> {
        let is_dirty = table_frame.is_dirty();
        let buffer = TablePageRead(table_frame.read()).into();
        Ok(PageFrame::from_buffer_with_metadata(
            buffer,
            is_dirty,
            table_frame.id(),
            table_frame.page_type(),
        ))
    }
}

impl TryFrom<IndexFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(index_frame: IndexFrame) -> Result<Self, Self::Error> {
        let is_dirty = index_frame.is_dirty();
        let buffer = IndexPageRead(index_frame.read()).into();
        Ok(PageFrame::from_buffer_with_metadata(
            buffer,
            is_dirty,
            index_frame.id(),
            index_frame.page_type(),
        ))
    }
}

impl TryFrom<OverflowFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(overflow_frame: OverflowFrame) -> Result<Self, Self::Error> {
        let is_dirty = overflow_frame.is_dirty();
        let buffer = OvfPageRead(overflow_frame.read()).into();

        Ok(PageFrame::from_buffer_with_metadata(
            buffer,
            is_dirty,
            overflow_frame.id(),
            overflow_frame.page_type(),
        ))
    }
}
