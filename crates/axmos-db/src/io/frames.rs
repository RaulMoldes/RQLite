use crate::storage::latches::Latch;

use parking_lot::RwLock;
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

pub(crate) struct MemFrame<P> {
    // Better [`RwLock`] than [`Mutex`] here, as we want to allow multiple readers to the page, but a single writer at a time to avoid race conditions.
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
