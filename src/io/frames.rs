use crate::storage::latches::{ReadOnlyLatch, UpgradableLatch, WriteLatch};

use parking_lot::RwLock;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

pub(crate) struct MemFrame<P> {
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avoid race conditions.
    inner: Arc<RwLock<P>>,
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

    pub(crate) fn read(&self) -> ReadOnlyLatch<P> {
        ReadOnlyLatch::lock(&self.inner)
    }

    pub(crate) fn read_for_upgrade(&self) -> UpgradableLatch<P> {
        UpgradableLatch::lock(&self.inner)
    }

    pub(crate) fn write(&self) -> WriteLatch<P> {
        WriteLatch::lock(&self.inner)
    }
}
