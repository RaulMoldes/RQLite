//! # Storage Module
//! This module provides the storage layer for the database, including buffer management,
//! disk management, and paging. It is responsible for managing how data is stored and retrieved
//! from disk, as well as caching frequently accessed data in memory to improve performance.
pub mod cache;
pub mod disk;
pub mod pager;

#[cfg(test)]
mod tests;
// Re-exporting the necessary components for external use
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::HeaderOps;

use crate::OverflowPage;
use crate::PageType;
use crate::{RQLiteIndexPage, RQLitePage, RQLiteTablePage};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, RwLock};

pub(crate) struct PageFrame<P> {
    id: PageId,
    // Store the page type to be able to check for conversions between frames at runtime.
    page_type: PageType,
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avoid race conditions.
    page: Arc<RwLock<P>>,
    // dirty flag
    is_dirty: AtomicBool,
}

impl<P> Clone for PageFrame<P> {
    fn clone(&self) -> Self {
        PageFrame {
            id: self.id,
            page_type: self.page_type,
            page: Arc::clone(&self.page),
            is_dirty: AtomicBool::new(self.is_dirty.load(Ordering::SeqCst)),
        }
    }
}

impl<P> PageFrame<P> {
    pub(crate) fn page_type(&self) -> PageType {
        self.page_type
    }

    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self.page_type, PageType::IndexLeaf)
            || matches!(self.page_type, PageType::TableLeaf)
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(self.page_type, PageType::IndexInterior)
            || matches!(self.page_type, PageType::TableInterior)
    }

    pub(crate) fn is_table(&self) -> bool {
        matches!(self.page_type, PageType::TableInterior)
            || matches!(self.page_type, PageType::TableLeaf)
    }

    pub(crate) fn is_index(&self) -> bool {
        matches!(self.page_type, PageType::IndexInterior)
            || matches!(self.page_type, PageType::IndexLeaf)
    }

    pub(crate) fn is_overflow(&self) -> bool {
        matches!(self.page_type, PageType::Overflow)
    }
}

pub(crate) trait Frame<P: Send + Sync>: std::fmt::Debug {
    fn id(&self) -> PageId;
    fn is_free(&self) -> bool;
    fn is_dirty(&self) -> bool;
    fn read(&self) -> std::sync::RwLockReadGuard<'_, P>;
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, P>;
}

impl<P> PageFrame<P> {
    fn new(id: PageId, page_type: PageType, page: Arc<RwLock<P>>) -> Self {
        Self {
            id,
            page_type,
            page: Arc::clone(&page),
            is_dirty: AtomicBool::new(false),
        }
    }

    pub(crate) fn from_page(page: P) -> Self
    where
        P: HeaderOps,
    {
        Self {
            id: page.id(),
            page_type: page.type_of(),
            page: Arc::new(RwLock::new(page)),
            is_dirty: AtomicBool::new(false),
        }
    }

    pub(crate) fn from_buffer(page_id: PageId, page_type: PageType, page: P) -> Self {
        Self {
            id: page_id,
            page_type,
            page: Arc::new(RwLock::new(page)),
            is_dirty: AtomicBool::new(false),
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
    P: Send + Sync + std::fmt::Debug,
{
    fn id(&self) -> PageId {
        self.id
    }

    fn is_free(&self) -> bool {
        Arc::strong_count(&self.page) <= 1
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, P> {
        self.page.read().unwrap()
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, P> {
        self.is_dirty.store(true, Ordering::SeqCst);
        self.page.write().unwrap()
    }
}

pub(crate) type IOFrame = PageFrame<Vec<u8>>; // In memory buffer for the cache.

impl Serializable for IOFrame {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let page_id = PageId::read_from(reader)?;
        let mut type_buffer = [0u8; 1];
        reader.read_exact(&mut type_buffer)?;
        let page_type = PageType::try_from(type_buffer[0]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize PageType from read buffer: {}", e),
            )
        })?;

        let mut dirty_buffer = [0u8; 1];
        reader.read_exact(&mut dirty_buffer)?;
        let is_dirty = match dirty_buffer[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid dirty flag value: {}", dirty_buffer[0]),
                ))
            }
        };

        // We store a maker to indicate the size of the data content.
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let data_len = u64::from_le_bytes(len_bytes) as usize;
        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        Ok(PageFrame {
            id: page_id,
            page_type,
            page: Arc::new(RwLock::new(data)),
            is_dirty: AtomicBool::new(is_dirty),
        })
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.id.write_to(writer)?;
        writer.write_all(&[self.page_type as u8])?;
        let dirty_marker: u8 = if self.is_dirty.load(Ordering::SeqCst) {
            1
        } else {
            0
        };
        writer.write_all(&[dirty_marker])?;
        let guard = self.page.read().unwrap();
        let len_bytes = (guard.len() as u64).to_le_bytes();
        writer.write_all(&len_bytes)?;
        writer.write_all(&guard)?;
        Ok(())
    }
}
pub(crate) type TableFrame = PageFrame<RQLiteTablePage>;
pub(crate) type IndexFrame = PageFrame<RQLiteIndexPage>;
pub(crate) type OverflowFrame = PageFrame<OverflowPage>;

/// I really do not know a better way to do this.
impl TryFrom<IOFrame> for TableFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty.load(Ordering::SeqCst);

        match Arc::try_unwrap(io_frame.page) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner().unwrap();
                let mut cursor = std::io::Cursor::new(buffer);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let table_page = RQLiteTablePage::from(dyn_page);

                Ok(PageFrame {
                    id: table_page.id(),
                    page_type: table_page.type_of(),
                    page: Arc::new(RwLock::new(table_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
            Err(arc) => {
                let guard = arc.read().unwrap();
                let mut cursor = std::io::Cursor::new(&guard[..]);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let table_page = RQLiteTablePage::from(dyn_page);

                Ok(PageFrame {
                    id: table_page.id(),
                    page_type: table_page.type_of(),
                    page: Arc::new(RwLock::new(table_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
        }
    }
}

impl TryFrom<IOFrame> for IndexFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty.load(Ordering::SeqCst);

        match Arc::try_unwrap(io_frame.page) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner().unwrap();
                let mut cursor = std::io::Cursor::new(buffer);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let index_page = RQLiteIndexPage::from(dyn_page);

                Ok(PageFrame {
                    id: index_page.id(),
                    page_type: index_page.type_of(),
                    page: Arc::new(RwLock::new(index_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
            Err(arc) => {
                let guard = arc.read().unwrap();
                let mut cursor = std::io::Cursor::new(&guard[..]);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let index_page = RQLiteIndexPage::from(dyn_page);

                Ok(PageFrame {
                    id: index_page.id(),
                    page_type: index_page.type_of(),
                    page: Arc::new(RwLock::new(index_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
        }
    }
}

impl TryFrom<IOFrame> for OverflowFrame {
    type Error = std::io::Error;

    fn try_from(io_frame: IOFrame) -> Result<Self, Self::Error> {
        let is_dirty = io_frame.is_dirty.load(Ordering::SeqCst);

        match Arc::try_unwrap(io_frame.page) {
            Ok(rwlock) => {
                let buffer = rwlock.into_inner().unwrap();
                let mut cursor = std::io::Cursor::new(buffer);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let overflow_page = OverflowPage::from(dyn_page);

                Ok(PageFrame {
                    id: overflow_page.id(),
                    page_type: overflow_page.type_of(),
                    page: Arc::new(RwLock::new(overflow_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
            Err(arc) => {
                let guard = arc.read().unwrap();
                let mut cursor = std::io::Cursor::new(&guard[..]);
                let dyn_page: RQLitePage = Serializable::read_from(&mut cursor)?;
                let overflow_page = OverflowPage::from(dyn_page);

                Ok(PageFrame {
                    id: overflow_page.id(),
                    page_type: overflow_page.type_of(),
                    page: Arc::new(RwLock::new(overflow_page)),
                    is_dirty: AtomicBool::new(is_dirty),
                })
            }
        }
    }
}

impl TryFrom<TableFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(table_frame: TableFrame) -> Result<Self, Self::Error> {
        use crate::serialization::Serializable;

        let page_id = table_frame.id;
        let page_type = table_frame.page_type;
        let is_dirty = table_frame.is_dirty.load(Ordering::SeqCst);

        let mut buffer = Vec::new();

        {
            let guard = table_frame.page.read().unwrap();
            guard.write_to(&mut buffer)?;
        }

        Ok(PageFrame {
            id: page_id,
            page_type,
            page: Arc::new(RwLock::new(buffer)),
            is_dirty: AtomicBool::new(is_dirty),
        })
    }
}

impl TryFrom<IndexFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(index_frame: IndexFrame) -> Result<Self, Self::Error> {
        use crate::serialization::Serializable;

        let page_id = index_frame.id;
        let page_type = index_frame.page_type;
        let is_dirty = index_frame.is_dirty.load(Ordering::SeqCst);

        let mut buffer = Vec::new();
        {
            let guard = index_frame.page.read().unwrap();
            guard.write_to(&mut buffer)?;
        }

        Ok(PageFrame {
            id: page_id,
            page_type,
            page: Arc::new(RwLock::new(buffer)),
            is_dirty: AtomicBool::new(is_dirty),
        })
    }
}

impl TryFrom<OverflowFrame> for IOFrame {
    type Error = std::io::Error;

    fn try_from(overflow_frame: OverflowFrame) -> Result<Self, Self::Error> {
        use crate::serialization::Serializable;

        let page_id = overflow_frame.id;
        let page_type = overflow_frame.page_type;
        let is_dirty = overflow_frame.is_dirty.load(Ordering::SeqCst);
        let mut buffer = Vec::new();

        {
            let guard = overflow_frame.page.read().unwrap();
            guard.write_to(&mut buffer)?;
        }

        Ok(PageFrame {
            id: page_id,
            page_type,
            page: Arc::new(RwLock::new(buffer)),
            is_dirty: AtomicBool::new(is_dirty),
        })
    }
}

pub(crate) fn create_frame(
    page_id: PageId,
    page_type: PageType,
    page_size: u32,
    right_most_page: Option<PageId>,
) -> std::io::Result<IOFrame> {
    // Allocate a vector of size page_size.
    let mut buffer = Vec::with_capacity(page_size as usize);
    // Create a dyn page.
    let dyn_page = RQLitePage::create(page_id, page_size, page_type, right_most_page)?;
    dyn_page.write_to(&mut buffer)?;
    let frame = PageFrame::new(page_id, page_type, Arc::new(RwLock::new(buffer)));
    Ok(frame)
}
