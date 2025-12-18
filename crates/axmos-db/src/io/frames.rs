use crate::{
    storage::{
        Identifiable,
        core::{buffer::MemBlock, traits::Buffer},
        page::{BtreePage, OverflowPage, PageZero},
    },
    types::PageId,
};

use std::{
    io::{self, Error as IoError, ErrorKind},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};

#[derive(Debug)]
pub(crate) struct ReadLatch<P>(ArcRwLockReadGuard<RawRwLock, P>);

impl<P> ReadLatch<P> {
    pub(crate) fn new(lock: &Arc<RwLock<P>>) -> Self {
        Self(lock.read_arc())
    }
}

impl<P> Deref for ReadLatch<P> {
    type Target = P;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct WriteLatch<P>(ArcRwLockWriteGuard<RawRwLock, P>);

impl<P> WriteLatch<P> {
    pub(crate) fn new(lock: &Arc<RwLock<P>>) -> Self {
        Self(lock.write_arc())
    }
}

impl<P> Deref for WriteLatch<P> {
    type Target = P;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P> DerefMut for WriteLatch<P> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub(crate) struct Frame<P: Buffer> {
    id: <<P as Buffer>::Header as Identifiable>::IdType,
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avObjectId race conditions.
    pub inner: Arc<RwLock<P>>,
    // dirty flag
    is_dirty: Arc<AtomicBool>,
}

impl<P> Clone for Frame<P>
where
    P: Buffer,
{
    fn clone(&self) -> Self {
        Frame {
            id: self.id,
            inner: Arc::clone(&self.inner),
            is_dirty: Arc::clone(&self.is_dirty),
        }
    }
}

impl<P> Frame<P>
where
    P: Buffer,
{
    pub(crate) fn new(inner: P) -> Self {
        let id = inner.id();
        Self {
            id,
            inner: Arc::new(RwLock::new(inner)),
            is_dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn deep_copy(&self) -> P
    where
        P: Clone,
    {
        let guard = self.inner.read();
        guard.clone()
    }
}

impl<P> Frame<P>
where
    P: Send + Sync + Buffer,
{
    pub(crate) fn is_free(&self) -> bool {
        Arc::strong_count(&self.inner) <= 1
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_dirty(&self) {
        self.is_dirty.store(true, Ordering::SeqCst);
    }

    pub(crate) fn read(&self) -> ReadLatch<P> {
        ReadLatch::new(&self.inner)
    }

    pub(crate) fn write(&self) -> WriteLatch<P> {
        WriteLatch::new(&self.inner)
    }
}

pub(crate) type OverflowFrame = Frame<OverflowPage>;
pub(crate) type BtreeFrame = Frame<BtreePage>;
pub(crate) type PageZeroFrame = Frame<PageZero>;

/// Dynamic dispatch for frames in the cache.
#[derive(Clone)]
pub(crate) enum MemFrame {
    Btree(BtreeFrame),
    Overflow(OverflowFrame),
    Zero(PageZeroFrame),
}

impl MemFrame {
    pub(crate) fn page_number(&self) -> PageId {
        match self {
            Self::Btree(p) => p.id,
            Self::Zero(p) => p.id,
            Self::Overflow(p) => p.id,
        }
    }

    /// Reinitializes this frame as another type.
    /// Uses Arc in order to validate that we are the only reference to the page, so this is safe.
    pub(crate) fn reinit_as<H>(self) -> Self
    where
        MemBlock<H>: From<BtreePage> + From<OverflowPage>,
        MemFrame: From<MemBlock<H>>,
    {
        match self {
            Self::Zero(_) => panic!("Attempted to reinitialize page zero. This is not valid."),
            Self::Btree(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");
                let page = lock.into_inner();
                Self::from(MemBlock::<H>::from(page))
            }
            Self::Overflow(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");
                let page = lock.into_inner();
                Self::from(MemBlock::<H>::from(page))
            }
        }
    }

    /// Marks this frame as deallocated by rewriting header as overflow
    /// The frame remains the same variant but memory is reinterpreted
    pub(crate) fn dealloc(self) -> Self {
        match self {
            Self::Zero(_) => panic!("Attempted to deallocate page zero. This is not valid."),
            Self::Btree(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot dealloc: other references to this page exist");
                let page = lock.into_inner();
                let overflow = page.dealloc();
                Self::from(overflow)
            }
            Self::Overflow(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot dealloc: other references to this page exist");
                let mut page = lock.into_inner();
                page.data_mut().fill(0);
                Self::from(page)
            }
        }
    }
}

impl From<BtreeFrame> for MemFrame {
    fn from(value: BtreeFrame) -> Self {
        Self::Btree(value)
    }
}

impl From<OverflowFrame> for MemFrame {
    fn from(value: OverflowFrame) -> Self {
        Self::Overflow(value)
    }
}

impl From<PageZeroFrame> for MemFrame {
    fn from(value: PageZeroFrame) -> Self {
        Self::Zero(value)
    }
}

// Dynamic dispatch for frame methods.
impl MemFrame {
    #[inline]
    pub(crate) fn is_free(&self) -> bool {
        match self {
            MemFrame::Btree(f) => f.is_free(),
            MemFrame::Overflow(f) => f.is_free(),
            MemFrame::Zero(f) => f.is_free(),
        }
    }

    #[inline]
    pub(crate) fn is_dirty(&self) -> bool {
        match self {
            MemFrame::Btree(f) => f.is_dirty(),
            MemFrame::Overflow(f) => f.is_dirty(),
            MemFrame::Zero(f) => f.is_dirty(),
        }
    }

    #[inline]
    pub(crate) fn mark_dirty(&self) {
        match self {
            MemFrame::Btree(f) => f.mark_dirty(),
            MemFrame::Overflow(f) => f.mark_dirty(),
            MemFrame::Zero(f) => f.mark_dirty(),
        }
    }
}

impl From<PageZero> for MemFrame {
    fn from(value: PageZero) -> Self {
        Self::Zero(Frame::new(value))
    }
}

impl From<BtreePage> for MemFrame {
    fn from(value: BtreePage) -> Self {
        Self::Btree(Frame::new(value))
    }
}

impl From<OverflowPage> for MemFrame {
    fn from(value: OverflowPage) -> Self {
        Self::Overflow(Frame::new(value))
    }
}

pub(crate) trait AsReadLatch<P> {
    fn as_read_latch(&self) -> io::Result<ReadLatch<P>>;
}

pub(crate) trait AsWriteLatch<P> {
    fn as_write_latch(&self) -> io::Result<WriteLatch<P>>;
}

impl AsReadLatch<BtreePage> for MemFrame {
    fn as_read_latch(&self) -> io::Result<ReadLatch<BtreePage>> {
        match self {
            MemFrame::Btree(f) => Ok(f.read()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected BtreePage frame",
            )),
        }
    }
}

impl AsWriteLatch<BtreePage> for MemFrame {
    fn as_write_latch(&self) -> io::Result<WriteLatch<BtreePage>> {
        match self {
            MemFrame::Btree(f) => Ok(f.write()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected BtreePage frame",
            )),
        }
    }
}

impl AsReadLatch<OverflowPage> for MemFrame {
    fn as_read_latch(&self) -> io::Result<ReadLatch<OverflowPage>> {
        match self {
            MemFrame::Overflow(f) => Ok(f.read()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected OverflowPage frame",
            )),
        }
    }
}

impl AsWriteLatch<OverflowPage> for MemFrame {
    fn as_write_latch(&self) -> io::Result<WriteLatch<OverflowPage>> {
        match self {
            MemFrame::Overflow(f) => Ok(f.write()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected OverflowPage frame",
            )),
        }
    }
}

impl AsReadLatch<PageZero> for MemFrame {
    fn as_read_latch(&self) -> io::Result<ReadLatch<PageZero>> {
        match self {
            MemFrame::Zero(f) => Ok(f.read()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected PageZero frame",
            )),
        }
    }
}

impl AsWriteLatch<PageZero> for MemFrame {
    fn as_write_latch(&self) -> io::Result<WriteLatch<PageZero>> {
        match self {
            MemFrame::Zero(f) => Ok(f.write()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected PageZero frame",
            )),
        }
    }
}

impl MemFrame {
    #[inline]
    pub(crate) fn try_read<P>(&self) -> io::Result<ReadLatch<P>>
    where
        Self: AsReadLatch<P>,
    {
        self.as_read_latch()
    }

    #[inline]
    pub(crate) fn try_write<P>(&self) -> io::Result<WriteLatch<P>>
    where
        Self: AsWriteLatch<P>,
    {
        self.mark_dirty();
        self.as_write_latch()
    }
}

impl MemFrame {
    /// Read-only access to raw bytes
    pub(crate) fn with_bytes<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        match self {
            Self::Btree(b) => f(b.read().as_ref()),
            Self::Overflow(b) => f(b.read().as_ref()),
            Self::Zero(b) => f(b.read().as_ref()),
        }
    }

    /// Mutable access to raw bytes
    pub(crate) fn with_bytes_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        match self {
            Self::Btree(b) => f(b.write().as_mut()),
            Self::Overflow(b) => f(b.write().as_mut()),
            Self::Zero(b) => f(b.write().as_mut()),
        }
    }
}
