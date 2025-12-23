use crate::{
    impl_enum_from,
    storage::{
        Allocatable, Identifiable,
        core::{buffer::MemBlock, traits::Buffer},
        page::{BtreePage, OverflowPage, PageZero},
    },
    types::PageId,
};

use std::{
    io::{Error as IoError, ErrorKind},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};

#[derive(Debug)]
pub(crate) struct ReadLatch<P>(ArcRwLockReadGuard<RawRwLock, P>);

pub trait Latch<P>: Deref<Target = P> {}

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

impl<P> Latch<P> for ReadLatch<P> {}
impl<P> Latch<P> for WriteLatch<P> {}

pub(crate) struct Frame<P: Buffer> {
    id: P::IdType,
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

impl<M> From<MemBlock<M>> for Frame<MemBlock<M>>
where
    MemBlock<M>: Buffer,
{
    fn from(value: MemBlock<M>) -> Self {
        Self::new(value)
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
}

impl<P> Frame<P>
where
    P: Buffer + AsMut<[u8]>,
{
    /// Utilities to execute callbacks on the raw bytes of the frame.
    /// The  inner type must be AsRef<[u8]> for read access and AsMut<[u8]> for write access.
    pub(crate) fn with_bytes_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut latch = self.inner.write();
        f(latch.as_mut())
    }
}

impl<P> Frame<P>
where
    P: Buffer + AsRef<[u8]>,
{
    pub(crate) fn with_bytes<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        let latch = self.inner.read();
        f(latch.as_ref())
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

impl Identifiable for MemFrame {
    type IdType = PageId;

    fn id(&self) -> Self::IdType {
        self.page_number()
    }
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
    pub(crate) fn reinit_as<H: Allocatable + Identifiable<IdType = PageId>>(self) -> Self
    where
        MemBlock<H>: Buffer<IdType = PageId>,
        MemFrame: From<Frame<MemBlock<H>>>,
    {
        match self {
            Self::Zero(_) => panic!("Attempted to reinitialize page zero. This is not valid."),
            Self::Btree(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");

                let mut page = lock.into_inner().cast::<H>();
                let page_size = page.size();
                let id = page.id();
                // Reinitialize the header.
                *page.metadata_mut() = <H as Allocatable>::alloc(id, page_size);
                Self::from(Frame::from(page))
            }
            Self::Overflow(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");
                let mut page = lock.into_inner().cast::<H>();
                let page_size = page.size();
                let id = page.id();
                // Reinitialize the header.
                *page.metadata_mut() = <H as Allocatable>::alloc(id, page_size);
                Self::from(Frame::from(page))
            }
        }
    }

    /// The frame remains the same variant but memory is reinterpreted
    pub(crate) fn dealloc(self) -> Self {
        match self {
            Self::Zero(_) => panic!("Attempted to deallocate page zero. This is not valid."),
            Self::Btree(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot dealloc: other references to this page exist");
                let page = lock.into_inner();
                let overflow = page.dealloc();
                Self::from(Frame::new(overflow))
            }
            Self::Overflow(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot dealloc: other references to this page exist");
                let mut page = lock.into_inner();
                page.data_mut().fill(0);
                Self::from(Frame::new(page))
            }
        }
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
    pub(crate) fn is_btree(&self) -> bool {
        matches!(self, MemFrame::Btree(_))
    }

    #[inline]
    pub(crate) fn is_overflow(&self) -> bool {
        matches!(self, MemFrame::Overflow(_))
    }

    #[inline]
    pub(crate) fn is_zero(&self) -> bool {
        matches!(self, MemFrame::Zero(_))
    }

    #[inline]
    pub(crate) fn mark_dirty(&self) {
        match self {
            MemFrame::Btree(f) => f.mark_dirty(),
            MemFrame::Overflow(f) => f.mark_dirty(),
            MemFrame::Zero(f) => f.mark_dirty(),
        }
    }

    /// Execute a callback on the raw bytes of the inner frame.
    ///
    /// Be aware that this function grabs the write lock inside of itself.
    pub(crate) fn with_bytes_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        match self {
            MemFrame::Btree(fm) => fm.with_bytes_mut(f),
            MemFrame::Overflow(fm) => fm.with_bytes_mut(f),
            MemFrame::Zero(fm) => fm.with_bytes_mut(f),
        }
    }

    /// Execute a callback on the raw bytes of the inner frame.
    ///
    /// Be aware that this function grabs the read lock inside  of itself.
    pub(crate) fn with_bytes<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        match self {
            MemFrame::Btree(fm) => fm.with_bytes(f),
            MemFrame::Overflow(fm) => fm.with_bytes(f),
            MemFrame::Zero(fm) => fm.with_bytes(f),
        }
    }
}

// Autogenerates From<$inner> for each variant.
impl_enum_from! {
    enum MemFrame {
        Btree => BtreeFrame,
        Overflow => OverflowFrame,
        Zero => PageZeroFrame,
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for ReadLatch<BtreePage> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Btree(f) => Ok(ReadLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Expected btreepage frame. Page id: {}", value.id()),
            )),
        }
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for WriteLatch<BtreePage> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        value.mark_dirty();
        match value {
            MemFrame::Btree(f) => Ok(WriteLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Expected btreepage frame. Page id: {}", value.id()),
            )),
        }
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for ReadLatch<OverflowPage> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Overflow(f) => Ok(ReadLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected overflow frame",
            )),
        }
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for WriteLatch<OverflowPage> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        value.mark_dirty();
        match value {
            MemFrame::Overflow(f) => Ok(WriteLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected overflow frame",
            )),
        }
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for ReadLatch<PageZero> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Zero(f) => Ok(ReadLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected page zero frame",
            )),
        }
    }
}

/// Convert from memframe to read and write latch.
impl TryFrom<&MemFrame> for WriteLatch<PageZero> {
    type Error = IoError;
    fn try_from(value: &MemFrame) -> Result<Self, Self::Error> {
        value.mark_dirty();
        match value {
            MemFrame::Zero(f) => Ok(WriteLatch::new(&f.inner)),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Expected page zero frame",
            )),
        }
    }
}

/// Traits for types that can be latched (locked).
pub(crate) trait AsReadLatch<P> {
    fn read(&self) -> Result<ReadLatch<P>, IoError>;
}

pub(crate) trait AsWriteLatch<P> {
    fn write(&self) -> Result<WriteLatch<P>, IoError>;
}

impl<T, P> AsReadLatch<P> for T
where
    for<'a> ReadLatch<P>: TryFrom<&'a T, Error = IoError>,
{
    fn read(&self) -> Result<ReadLatch<P>, IoError> {
        ReadLatch::try_from(self)
    }
}

impl<T, P> AsWriteLatch<P> for T
where
    for<'a> WriteLatch<P>: TryFrom<&'a T, Error = IoError>,
{
    fn write(&self) -> Result<WriteLatch<P>, IoError> {
        WriteLatch::try_from(self)
    }
}
