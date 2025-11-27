use crate::{
    as_slice, repr_enum,
    storage::{cell::Slot, latches::Latch, page::MemPage},
    types::PageId,
};

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool, atomic::Ordering},
};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) struct Position(PageId, Slot);

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

pub(crate) struct MemFrame<P> {
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avObjectId race conditions.
    pub inner: Arc<RwLock<P>>,
    // dirty flag
    is_dirty: Arc<AtomicBool>,
}

impl<P: Send + Sync + std::fmt::Debug> std::fmt::Debug for MemFrame<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.read().fmt(f)
    }
}

impl<P> Clone for MemFrame<P> {
    fn clone(&self) -> Self {
        MemFrame {
            inner: Arc::clone(&self.inner),
            is_dirty: Arc::clone(&self.is_dirty),
        }
    }
}

impl<P> MemFrame<P> {
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

impl<P> MemFrame<P>
where
    P: Send + Sync + std::fmt::Debug,
{
    pub(crate) fn into_inner(self) -> std::sync::Arc<RwLock<P>> {
        self.inner
    }

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

    pub(crate) fn write(&mut self) -> Latch<P> {
        self.mark_dirty();
        Latch::write(&self.inner)
    }
}

impl<P> MemFrame<P>
where
    P: Send + Sync + std::fmt::Debug,
{
    /// Callables that allow to execute a custom function on any type that implements [TryFrom<P>]
    /// [`try_with_variant`] allows to execute callbacks on shared references taken over frames acquiring and automatically releasing the latches.
    ///
    /// These are pretty handy when we want to grab the lock on a page, apply a transform and inmediately release it.
    ///
    /// Notes on functor and closures traits in case you are interested: https://doc.rust-lang.org/std/ops/trait.Fn.html
    ///
    /// Basically, anything that is [Fn] will also be [FnMut] and [FnOnce],
    /// and everything that is [FnMut] is also [FnOnce], the compiler can coerce the Function types in that order. However, [FnOnce] cannot be coerced into [FnMut] neither into [Fn].
    ///
    /// So [try_with_variant] will accept a [FnOnce], that is a functor that takes a shared reference over a Frame. While [try_with_variant_mut] will take a [FnMut] (a functor that takes an exclusive reference over a Frame).
    pub fn try_with_variant<V, F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&V) -> R,
        for<'a> &'a Latch<P>: TryInto<&'a V, Error = E>,
    {
        let guard = self.read();
        let variant: &V = (&guard).try_into()?;
        Ok(f(variant))
    }

    pub fn try_with_variant_mut<V, F, R, E>(&mut self, f: F) -> Result<R, E>
    where
        F: FnOnce(&mut V) -> R,
        for<'a> &'a mut Latch<P>: TryInto<&'a mut V, Error = E>,
    {
        let mut guard = self.write();
        let variant: &mut V = (&mut guard).try_into()?;
        Ok(f(variant))
    }
}

#[derive(Debug)]
pub struct FrameStack {
    latches: HashMap<PageId, Latch<MemPage>>,
    traversal: Vec<Position>,
}

impl FrameStack {
    pub fn new() -> Self {
        FrameStack {
            latches: HashMap::new(),
            traversal: Vec::new(),
        }
    }

    pub fn acquire(&mut self, key: PageId, value: MemFrame<MemPage>, access_mode: FrameAccessMode) {
        if let Some(existing_latch) = self.latches.get(&key) {
            match (existing_latch, access_mode) {
                (Latch::Upgradable(p), FrameAccessMode::Write) => {
                    let write_latch = self.latches.remove(&key).unwrap().upgrade();
                    self.latches.insert(key, write_latch);
                }
                (Latch::Write(p), FrameAccessMode::Read) => {
                    let read_latch = self.latches.remove(&key).unwrap().downgrade();
                    self.latches.insert(key, read_latch);
                }
                (Latch::Read(p), FrameAccessMode::Write) => {
                    panic!(
                        "Attempted to acquire a write latch on a node that was borrowed for read. Must use upgradable latches for this use case."
                    )
                }
                _ => {}
            }
        } else {
            self.acquire_unchecked(key, value, access_mode);
        }
    }

    pub fn acquire_unchecked(
        &mut self,
        key: PageId,
        mut value: MemFrame<MemPage>,
        access_mode: FrameAccessMode,
    ) {
        match access_mode {
            FrameAccessMode::Read => self.latches.insert(key, value.read()),
            FrameAccessMode::Write => self.latches.insert(key, value.write()),
            FrameAccessMode::ReadWrite => self.latches.insert(key, value.upgradable()),
        };
    }

    pub fn release(&mut self, key: PageId) {
        self.latches.remove(&key);
    }

    pub fn visit(&mut self, key: Position) {
        self.traversal.push(key);
    }

    pub fn get(&self, key: PageId) -> Option<&Latch<MemPage>> {
        self.latches.get(&key)
    }

    pub fn get_mut(&mut self, key: PageId) -> Option<&mut Latch<MemPage>> {
        self.latches.get_mut(&key)
    }

    pub fn pop(&mut self) -> Option<Position> {
        self.traversal.pop()
    }

    pub fn last(&self) -> Option<&Position> {
        self.traversal.last()
    }

    pub fn clear(&mut self) {
        self.latches.clear();
        self.traversal.clear();
    }
}
