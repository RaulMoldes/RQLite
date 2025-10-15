// Re-exporting the necessary components for external use
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::{
    FreePage, HeaderOps, IndexInteriorPage, IndexLeafPage, OverflowPage, RQLiteIndexPage,
    RQLiteTablePage, TableInteriorPage, TableLeafPage,
};

use crate::storage::guards::{ReadOnlyLatch, UpgradableLatch, WriteLatch};
use crate::PageType;
use parking_lot::RwLock;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

pub(crate) struct PageFrame<P> {
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avoid race conditions.
    page: Arc<RwLock<P>>,
    // dirty flag
    is_dirty: Arc<AtomicBool>,
}

impl<P> Clone for PageFrame<P> {
    fn clone(&self) -> Self {
        PageFrame {
            page: Arc::clone(&self.page),
            is_dirty: Arc::clone(&self.is_dirty),
        }
    }
}

impl<P> PageFrame<P>
where
    P: HeaderOps,
{
    pub(crate) fn page_type(&self) -> PageType {
        self.page.read().type_of()
    }

    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self.page_type(), PageType::IndexLeaf)
            || matches!(self.page_type(), PageType::TableLeaf)
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(self.page_type(), PageType::IndexInterior)
            || matches!(self.page_type(), PageType::TableInterior)
    }

    pub(crate) fn is_table(&self) -> bool {
        matches!(self.page_type(), PageType::TableInterior)
            || matches!(self.page_type(), PageType::TableLeaf)
    }

    pub(crate) fn is_index(&self) -> bool {
        matches!(self.page_type(), PageType::IndexInterior)
            || matches!(self.page_type(), PageType::IndexLeaf)
    }

    pub(crate) fn is_overflow(&self) -> bool {
        matches!(self.page_type(), PageType::Overflow)
    }
}

pub(crate) trait Frame<P: Send + Sync>: std::fmt::Debug {
    fn id(&self) -> PageId;
    fn is_free(&self) -> bool;
    fn is_dirty(&self) -> bool;
    fn into_inner(self) -> Arc<RwLock<P>>;
    fn read(&self) -> ReadOnlyLatch<P>;
    fn read_for_upgrade(&self) -> UpgradableLatch<P>;
    fn write(&self) -> WriteLatch<P>;

    fn mark_dirty(&mut self);
}

impl<P> PageFrame<P> {
    pub(crate) fn new(page: P) -> Self {
        Self {
            page: Arc::new(RwLock::new(page)),
            is_dirty: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<P: Send + Sync + std::fmt::Debug> std::fmt::Debug for PageFrame<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.page.read().fmt(f)
    }
}
impl<P> Frame<P> for PageFrame<P>
where
    P: Send + Sync + std::fmt::Debug + HeaderOps,
{
    fn id(&self) -> PageId {
        self.page.read().id()
    }

    fn into_inner(self) -> std::sync::Arc<RwLock<P>> {
        self.page
    }

    fn is_free(&self) -> bool {
        Arc::strong_count(&self.page) <= 1
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    fn mark_dirty(&mut self) {
        self.is_dirty.store(true, Ordering::SeqCst);
    }

    fn read(&self) -> ReadOnlyLatch<P> {
        ReadOnlyLatch::lock(&self.page)
    }

    fn read_for_upgrade(&self) -> UpgradableLatch<P> {
        UpgradableLatch::lock(&self.page)
    }

    fn write(&self) -> WriteLatch<P> {
        WriteLatch::lock(&self.page)
    }
}

// In memory buffer for the page cache.
pub(crate) enum IOFrame {
    Table(PageFrame<RQLiteTablePage>),
    Index(PageFrame<RQLiteIndexPage>),
    Overflow(PageFrame<OverflowPage>),
    Free(PageFrame<FreePage>),
}
impl Clone for IOFrame {
    fn clone(&self) -> Self {
        match self {
            IOFrame::Free(p) => IOFrame::Free(p.clone()),
            IOFrame::Overflow(p) => IOFrame::Overflow(p.clone()),
            IOFrame::Table(p) => IOFrame::Table(p.clone()),
            IOFrame::Index(p) => IOFrame::Index(p.clone()),
        }
    }
}

impl IOFrame {
    pub(crate) fn page_type(&self) -> PageType {
        match self {
            IOFrame::Table(p) => p.page_type(),
            IOFrame::Index(p) => p.page_type(),
            IOFrame::Overflow(p) => p.page_type(),
            IOFrame::Free(p) => p.page_type(),
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            IOFrame::Table(p) => p.is_leaf(),
            IOFrame::Index(p) => p.is_leaf(),
            IOFrame::Overflow(p) => p.is_leaf(),
            IOFrame::Free(p) => p.is_leaf(),
        }
    }

    pub(crate) fn is_interior(&self) -> bool {
        match self {
            IOFrame::Table(p) => p.is_interior(),
            IOFrame::Index(p) => p.is_interior(),
            IOFrame::Overflow(p) => p.is_interior(),
            IOFrame::Free(p) => p.is_interior(),
        }
    }

    pub(crate) fn is_table(&self) -> bool {
        match self {
            IOFrame::Table(p) => p.is_table(),
            IOFrame::Index(p) => p.is_table(),
            IOFrame::Overflow(p) => p.is_table(),
            IOFrame::Free(p) => p.is_table(),
        }
    }

    pub(crate) fn is_index(&self) -> bool {
        match self {
            IOFrame::Table(p) => p.is_index(),
            IOFrame::Index(p) => p.is_index(),
            IOFrame::Overflow(p) => p.is_index(),
            IOFrame::Free(p) => p.is_index(),
        }
    }

    pub(crate) fn is_overflow(&self) -> bool {
        match self {
            IOFrame::Table(p) => p.is_overflow(),
            IOFrame::Index(p) => p.is_overflow(),
            IOFrame::Overflow(p) => p.is_overflow(),
            IOFrame::Free(p) => p.is_overflow(),
        }
    }
}

impl IOFrame {
    pub(crate) fn create(
        page_id: PageId,
        page_type: PageType,
        page_size: u32,
        right_most_page: Option<PageId>,
    ) -> Self {
        match page_type {
            PageType::Free => IOFrame::Free(PageFrame::new(FreePage::create(
                page_id,
                page_size,
                right_most_page,
            ))),
            PageType::Overflow => IOFrame::Overflow(PageFrame::new(OverflowPage::create(
                page_id,
                page_size,
                right_most_page,
            ))),
            PageType::IndexInterior => IOFrame::Index(PageFrame::new(RQLiteIndexPage::Interior(
                IndexInteriorPage::create(page_id, page_size, page_type, right_most_page),
            ))),
            PageType::IndexLeaf => IOFrame::Index(PageFrame::new(RQLiteIndexPage::Leaf(
                IndexLeafPage::create(page_id, page_size, page_type, right_most_page),
            ))),
            PageType::TableInterior => IOFrame::Table(PageFrame::new(RQLiteTablePage::Interior(
                TableInteriorPage::create(page_id, page_size, page_type, right_most_page),
            ))),
            PageType::TableLeaf => IOFrame::Table(PageFrame::new(RQLiteTablePage::Leaf(
                TableLeafPage::create(page_id, page_size, page_type, right_most_page),
            ))),
        }
    }
    pub(crate) fn id(&self) -> PageId {
        match self {
            IOFrame::Free(p) => p.id(),
            IOFrame::Overflow(p) => p.id(),
            IOFrame::Table(p) => p.id(),
            IOFrame::Index(p) => p.id(),
        }
    }

    pub(crate) fn is_free(&self) -> bool {
        match self {
            IOFrame::Free(p) => p.is_free(),
            IOFrame::Overflow(p) => p.is_free(),
            IOFrame::Table(p) => p.is_free(),
            IOFrame::Index(p) => p.is_free(),
        }
    }

    pub(crate) fn is_dirty(&self) -> bool {
        match self {
            IOFrame::Free(p) => p.is_dirty(),
            IOFrame::Overflow(p) => p.is_dirty(),
            IOFrame::Table(p) => p.is_dirty(),
            IOFrame::Index(p) => p.is_dirty(),
        }
    }

    pub(crate) fn mark_dirty(&mut self) {
        match self {
            IOFrame::Free(p) => p.mark_dirty(),
            IOFrame::Overflow(p) => p.mark_dirty(),
            IOFrame::Table(p) => p.mark_dirty(),
            IOFrame::Index(p) => p.mark_dirty(),
        }
    }
}
/// IOFRAMES are serialized when writing their contents to the disk.
/// Therefore they are not dirty when we read them back.
/// However, we need a way to
impl Serializable for IOFrame {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;

        let page_type = PageType::try_from(buffer[0]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize PageType from read buffer: {}", e),
            )
        })?;

        match page_type {
            PageType::TableLeaf => {
                let mut page = TableLeafPage::read_from(reader)?;
                let id = page.id();
                page.set_type(page_type);

                let inner = RQLiteTablePage::Leaf(page);

                Ok(IOFrame::Table(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
            PageType::TableInterior => {
                let mut page = TableInteriorPage::read_from(reader)?;
                let id = page.id();
                page.set_type(page_type);
                let inner = RQLiteTablePage::Interior(page);

                Ok(IOFrame::Table(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
            PageType::IndexLeaf => {
                let mut page = IndexLeafPage::read_from(reader)?;
                let id = page.id();
                page.set_type(page_type);
                let inner = RQLiteIndexPage::Leaf(page);

                Ok(IOFrame::Index(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
            PageType::IndexInterior => {
                let mut page = IndexInteriorPage::read_from(reader)?;
                page.set_type(page_type);
                let id = page.id();

                let inner = RQLiteIndexPage::Interior(page);

                Ok(IOFrame::Index(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
            PageType::Overflow => {
                let mut inner = OverflowPage::read_from(reader)?;
                let id = inner.id();
                inner.set_type(page_type);

                Ok(IOFrame::Overflow(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
            PageType::Free => {
                let mut inner = FreePage::read_from(reader)?;
                let id = inner.id();
                inner.set_type(page_type);

                Ok(IOFrame::Free(PageFrame {
                    page: Arc::new(RwLock::new(inner)),
                    is_dirty: Arc::new(AtomicBool::new(false)),
                }))
            }
        }
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        match self {
            IOFrame::Free(page) => Arc::try_unwrap(page.page)
                .expect("More than one reference to the shared pointer!")
                .into_inner()
                .write_to(writer)?,
            IOFrame::Overflow(page) => Arc::try_unwrap(page.page)
                .expect("More than one reference to the shared pointer!")
                .into_inner()
                .write_to(writer)?,
            IOFrame::Table(page) => Arc::try_unwrap(page.page)
                .expect("More than one reference to the shared pointer!")
                .into_inner()
                .serialize(writer)?,
            IOFrame::Index(page) => Arc::try_unwrap(page.page)
                .expect("More than one reference to the shared pointer!")
                .into_inner()
                .serialize(writer)?,
        };

        Ok(())
    }
}

impl From<PageFrame<RQLiteTablePage>> for IOFrame {
    fn from(frame: PageFrame<RQLiteTablePage>) -> Self {
        IOFrame::Table(frame)
    }
}

impl From<PageFrame<RQLiteIndexPage>> for IOFrame {
    fn from(frame: PageFrame<RQLiteIndexPage>) -> Self {
        IOFrame::Index(frame)
    }
}

impl From<PageFrame<OverflowPage>> for IOFrame {
    fn from(frame: PageFrame<OverflowPage>) -> Self {
        IOFrame::Overflow(frame)
    }
}

impl From<PageFrame<FreePage>> for IOFrame {
    fn from(frame: PageFrame<FreePage>) -> Self {
        IOFrame::Free(frame)
    }
}

impl TryFrom<IOFrame> for PageFrame<RQLiteTablePage> {
    type Error = std::io::Error;

    fn try_from(value: IOFrame) -> Result<Self, Self::Error> {
        match value {
            IOFrame::Table(p) => Ok(p),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "IOFrame is not of type Table!",
            )),
        }
    }
}

impl TryFrom<IOFrame> for PageFrame<RQLiteIndexPage> {
    type Error = std::io::Error;

    fn try_from(value: IOFrame) -> Result<Self, Self::Error> {
        match value {
            IOFrame::Index(p) => Ok(p),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "IOFrame is not of type Index!",
            )),
        }
    }
}

impl TryFrom<IOFrame> for PageFrame<OverflowPage> {
    type Error = std::io::Error;

    fn try_from(value: IOFrame) -> Result<Self, Self::Error> {
        match value {
            IOFrame::Overflow(p) => Ok(p),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "IOFrame is not of type Overflow!",
            )),
        }
    }
}

impl TryFrom<IOFrame> for PageFrame<FreePage> {
    type Error = std::io::Error;

    fn try_from(value: IOFrame) -> Result<Self, Self::Error> {
        match value {
            IOFrame::Free(p) => Ok(p),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "IOFrame is not of type Free!",
            )),
        }
    }
}
