// Re-exporting the necessary components for external use
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::HeaderOps;

use crate::PageType;
use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock,
};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

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
    fn into_inner(self) -> std::sync::Arc<RwLock<P>>;
    fn read(&self) -> ArcRwLockReadGuard<RawRwLock, P>;
    fn read_for_upgrade(&self) -> ArcRwLockUpgradableReadGuard<RawRwLock, P>;
    fn write(&self) -> ArcRwLockWriteGuard<RawRwLock, P>;
}

impl<P> PageFrame<P> {
    pub(crate) fn new(id: PageId, page_type: PageType, page: Arc<RwLock<P>>) -> Self {
        Self {
            id,
            page_type,
            page: Arc::clone(&page),
            is_dirty: AtomicBool::new(false),
        }
    }

    pub(crate) fn from_existent(page: P, is_dirty: bool) -> Self
    where
        P: HeaderOps,
    {
        Self {
            id: page.id(),
            page_type: page.type_of(),
            page: Arc::new(RwLock::new(page)),
            is_dirty: AtomicBool::new(is_dirty),
        }
    }

    pub(crate) fn from_buffer_with_metadata(
        page: P,
        is_dirty: bool,
        id: PageId,
        page_type: PageType,
    ) -> Self {
        Self {
            id,
            page_type,
            page: Arc::new(RwLock::new(page)),
            is_dirty: AtomicBool::new(is_dirty),
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

    fn into_inner(self) -> std::sync::Arc<RwLock<P>> {
        self.page
    }

    fn is_free(&self) -> bool {
        Arc::strong_count(&self.page) <= 1
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    fn read(&self) -> ArcRwLockReadGuard<RawRwLock, P> {
        self.page.read_arc()
    }

    fn read_for_upgrade(&self) -> ArcRwLockUpgradableReadGuard<RawRwLock, P> {
        RwLock::upgradable_read_arc(&self.page)
    }

    fn write(&self) -> ArcRwLockWriteGuard<RawRwLock, P> {
        self.is_dirty.store(true, Ordering::SeqCst);
        self.page.write_arc()
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
        let guard = self.page.read();
        let len_bytes = (guard.len() as u64).to_le_bytes();
        writer.write_all(&len_bytes)?;
        writer.write_all(&guard)?;
        Ok(())
    }
}
