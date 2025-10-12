pub(crate) mod base;
pub(crate) mod convert;
pub(crate) use base::{Frame, IOFrame, PageFrame};

use crate::serialization::Serializable;
use crate::types::PageId;
use crate::{FreePage, OverflowPage, PageType, RQLiteIndexPage, RQLiteTablePage};
use crate::{IndexInteriorPage, IndexLeafPage, TableInteriorPage, TableLeafPage};

pub(crate) type TableFrame = PageFrame<RQLiteTablePage>;
pub(crate) type IndexFrame = PageFrame<RQLiteIndexPage>;
pub(crate) type OverflowFrame = PageFrame<OverflowPage>;

use parking_lot::RwLock;
use std::sync::Arc;

pub(crate) fn create_frame(
    page_id: PageId,
    page_type: PageType,
    page_size: u32,
    right_most_page: Option<PageId>,
) -> std::io::Result<IOFrame> {
    // Allocate a vector of size page_size.
    let mut buffer = Vec::with_capacity(page_size as usize);

    match page_type {
        PageType::IndexInterior => {
            let page = IndexInteriorPage::create(page_id, page_size, page_type, right_most_page);
            page.write_to(&mut buffer)?;
        }
        PageType::IndexLeaf => {
            let page = IndexLeafPage::create(page_id, page_size, page_type, right_most_page);

            page.write_to(&mut buffer)?;
        }

        PageType::TableInterior => {
            let page = TableInteriorPage::create(page_id, page_size, page_type, right_most_page);

            page.write_to(&mut buffer)?;
        }
        PageType::TableLeaf => {
            let page = TableLeafPage::create(page_id, page_size, page_type, right_most_page);

            page.write_to(&mut buffer)?;
        }
        PageType::Overflow => {
            let page = OverflowPage::create(page_id, page_size, None);

            page.write_to(&mut buffer)?;
        }
        PageType::Free => {
            let page = FreePage::create(page_id, page_size, None);
            page.write_to(&mut buffer)?;
        }
    };

    let frame = PageFrame::new(page_id, page_type, Arc::new(RwLock::new(buffer)));
    Ok(frame)
}
