use crate::{
    as_slice, repr_enum,
    storage::{
        PageOps,
        buffer::MemBlock,
        cell::Slot,
        latches::{Latch, PageLatch},
        page::{BtreePage, OverflowPage, OverflowPageHeader, PageZero},
    },
    types::PageId,
};

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind},
    ptr,
    sync::{Arc, atomic::AtomicBool, atomic::Ordering},
};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Position(PageId, Slot);

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, " Position: page {}, slot {}", self.page(), self.slot())
    }
}

impl Position {
    pub(crate) fn new(page_id: PageId, slot: Slot) -> Self {
        Self(page_id, slot)
    }

    pub(crate) fn start_pos(page_id: PageId) -> Self {
        Self(page_id, Slot(0))
    }

    pub(crate) fn page(&self) -> PageId {
        self.0
    }

    pub(crate) fn slot(&self) -> Slot {
        self.1
    }
}
as_slice!(Position);

repr_enum!( pub(crate) enum FrameAccessMode: u8 {
    Read = 0x00,
    Write = 0x01,
    ReadWrite = 0x02,
});

#[derive(Debug)]
pub(crate) struct Frame<P: fmt::Debug> {
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avObjectId race conditions.
    pub inner: Arc<RwLock<P>>,
    // dirty flag
    is_dirty: Arc<AtomicBool>,
}

impl<P> Clone for Frame<P>
where
    P: fmt::Debug,
{
    fn clone(&self) -> Self {
        Frame {
            inner: Arc::clone(&self.inner),
            is_dirty: Arc::clone(&self.is_dirty),
        }
    }
}

impl<P> Frame<P>
where
    P: fmt::Debug,
{
    pub(crate) fn new(inner: P) -> Self {
        Self {
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
    P: Send + Sync + fmt::Debug,
{
    pub(crate) fn is_free(&self) -> bool {
        Arc::strong_count(&self.inner) <= 1
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_dirty(&mut self) {
        self.is_dirty.store(true, Ordering::SeqCst);
    }

    pub(crate) fn read(&self) -> Latch<P> {
        Latch::read(&self.inner)
    }

    pub(crate) fn upgradable(&self) -> Latch<P> {
        Latch::upgradable(&self.inner)
    }

    pub(crate) fn write(&self) -> Latch<P> {
        Latch::write(&self.inner)
    }
}

#[derive(Debug)]
pub struct FrameStack {
    latches: HashMap<PageId, PageLatch>,
    traversal: Vec<Position>,
}

impl FrameStack {
    pub(crate) fn new() -> Self {
        FrameStack {
            latches: HashMap::new(),
            traversal: Vec::new(),
        }
    }

    /// Acquires a latch and returns it
    pub(crate) fn acquire_unchecked(
        &mut self,
        key: PageId,
        value: MemFrame,
        access_mode: FrameAccessMode,
    ) {
        match access_mode {
            FrameAccessMode::Read => self.latches.insert(key, value.read()),
            FrameAccessMode::Write => self.latches.insert(key, value.write()),
            FrameAccessMode::ReadWrite => self.latches.insert(key, value.upgradable()),
        };
    }

    /// Acquire a latch storing it accordingly
    pub(crate) fn acquire(&mut self, key: PageId, value: MemFrame, access_mode: FrameAccessMode) {
        if let Some(existing_latch) = self.latches.get(&key) {
            match access_mode {
                FrameAccessMode::Write => {
                    if existing_latch.is_upgradable() {
                        let write_latch = self.latches.remove(&key).unwrap().upgrade();
                        self.latches.insert(key, write_latch);
                    } else if existing_latch.is_read() {
                        panic!(
                            "Attempted to acquire a write latch on a node that was borrowed for read. Must use upgradable latches for this use case."
                        )
                    };
                }
                FrameAccessMode::Read => {
                    if !existing_latch.is_read() {
                        let read_latch = self.latches.remove(&key).unwrap().downgrade();
                        self.latches.insert(key, read_latch);
                    }
                }
                _ => {}
            }
        } else {
            self.acquire_unchecked(key, value, access_mode);
        }
    }

    pub(crate) fn release(&mut self, key: PageId) {
        self.latches.remove(&key);
    }

    pub(crate) fn visit(&mut self, key: Position) {
        self.traversal.push(key);
    }

    pub(crate) fn get(&self, key: PageId) -> Option<&PageLatch> {
        self.latches.get(&key)
    }

    pub(crate) fn get_mut(&mut self, key: PageId) -> Option<&mut PageLatch> {
        self.latches.get_mut(&key)
    }

    pub(crate) fn pop(&mut self) -> Option<Position> {
        self.traversal.pop()
    }

    pub(crate) fn last(&self) -> Option<&Position> {
        self.traversal.last()
    }

    pub(crate) fn clear(&mut self) {
        self.latches.clear();
        self.traversal.clear();
    }
}

pub(crate) type OverflowFrame = Frame<OverflowPage>;
pub(crate) type BtreeFrame = Frame<BtreePage>;
pub(crate) type PageZeroFrame = Frame<PageZero>;

/// Dynamic dispatch for frames in the cache.
#[derive(Debug, Clone)]
pub(crate) enum MemFrame {
    Btree(BtreeFrame),
    Overflow(OverflowFrame),
    Zero(PageZeroFrame),
}

impl MemFrame {
    pub(crate) fn page_number(&self) -> PageId {
        match self {
            Self::Btree(p) => p.read().page_number(),
            Self::Zero(p) => p.read().page_number(),
            Self::Overflow(p) => p.read().page_number(),
        }
    }

    /// Reinitializes this frame as another type.
    /// Uses Arc in order to validate that we are the only reference to the page, so this is safe.
    pub(crate) fn reinit_as<H>(self) -> Self
    where
        MemFrame: From<MemBlock<H>>,
    {
        match self {
            Self::Zero(_) => panic!("Attempted to reinitialize page zero. This is not valid."),
            Self::Btree(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");
                let page = lock.into_inner();
                let new = page.cast::<H>();
                Self::from(new)
            }
            Self::Overflow(arc) => {
                let lock = Arc::try_unwrap(arc.inner)
                    .expect("Cannot reinit: other references to this page exist");
                let page = lock.into_inner();
                let new = page.cast::<H>();
                Self::from(new)
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

/// Checked conversions
impl TryFrom<MemFrame> for BtreeFrame {
    type Error = IoError;

    fn try_from(value: MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Btree(p) => Ok(p),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as btree page",
            )),
        }
    }
}

impl TryFrom<MemFrame> for OverflowFrame {
    type Error = IoError;

    fn try_from(value: MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Overflow(p) => Ok(p),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as overflow page",
            )),
        }
    }
}

impl TryFrom<MemFrame> for PageZeroFrame {
    type Error = IoError;

    fn try_from(value: MemFrame) -> Result<Self, Self::Error> {
        match value {
            MemFrame::Zero(p) => Ok(p),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as page zero",
            )),
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
    pub(crate) fn mark_dirty(&mut self) {
        match self {
            MemFrame::Btree(f) => f.mark_dirty(),
            MemFrame::Overflow(f) => f.mark_dirty(),
            MemFrame::Zero(f) => f.mark_dirty(),
        }
    }

    #[inline]
    pub(crate) fn read(&self) -> PageLatch {
        match self {
            MemFrame::Btree(f) => PageLatch::Btree(f.read()),
            MemFrame::Overflow(f) => PageLatch::Overflow(f.read()),
            MemFrame::Zero(f) => PageLatch::Zero(f.read()),
        }
    }

    #[inline]
    pub(crate) fn upgradable(&self) -> PageLatch {
        match self {
            MemFrame::Btree(f) => PageLatch::Btree(f.upgradable()),
            MemFrame::Overflow(f) => PageLatch::Overflow(f.upgradable()),
            MemFrame::Zero(f) => PageLatch::Zero(f.upgradable()),
        }
    }

    #[inline]
    pub(crate) fn write(&self) -> PageLatch {
        match self {
            MemFrame::Btree(f) => PageLatch::Btree(f.write()),
            MemFrame::Overflow(f) => PageLatch::Overflow(f.write()),
            MemFrame::Zero(f) => PageLatch::Zero(f.write()),
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
