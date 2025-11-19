use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock,
};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum Latch<P> {
    Read(ArcRwLockReadGuard<RawRwLock, P>),
    Upgradable(ArcRwLockUpgradableReadGuard<RawRwLock, P>),
    Write(ArcRwLockWriteGuard<RawRwLock, P>),
}

impl<P> Latch<P> {
    /// Acquire a read-only latch
    pub(crate) fn read(lock: &Arc<RwLock<P>>) -> Self {
        Self::Read(lock.read_arc())
    }

    /// Acquire an upgradable latch
    pub(crate) fn upgradable(lock: &Arc<RwLock<P>>) -> Self {
        Self::Upgradable(RwLock::upgradable_read_arc(lock))
    }

    /// Acquire a write latch
    pub(crate) fn write(lock: &Arc<RwLock<P>>) -> Self {
        Self::Write(lock.write_arc())
    }

    /// Try to upgrade from Upgradable to Write
    pub(crate) fn upgrade(self) -> Self {
        match self {
            Self::Upgradable(guard) => Self::Write(ArcRwLockUpgradableReadGuard::upgrade(guard)),
            other => other, // No-op if not upgradable
        }
    }

    /// Downgrade from Write to Upgradable
    pub(crate) fn downgrade_to_upgradable(self) -> Self {
        match self {
            Self::Write(guard) => {
                Self::Upgradable(ArcRwLockWriteGuard::downgrade_to_upgradable(guard))
            }
            other => other,
        }
    }

    /// Downgrade from Write to Read
    pub(crate) fn downgrade(self) -> Self {
        match self {
            Self::Write(guard) => Self::Read(ArcRwLockWriteGuard::downgrade(guard)),
            other => other,
        }
    }

    /// Returns true if latch is write
    pub(crate) fn is_write(&self) -> bool {
        matches!(self, Self::Write(_))
    }

    /// Returns true if latch is read
    pub(crate) fn is_read(&self) -> bool {
        matches!(self, Self::Read(_))
    }

    /// Returns true if latch is upgradable
    pub(crate) fn is_upgradable(&self) -> bool {
        matches!(self, Self::Upgradable(_))
    }
}

impl<P> Deref for Latch<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Read(g) => g,
            Self::Upgradable(g) => g,
            Self::Write(g) => g,
        }
    }
}

impl<P> DerefMut for Latch<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Write(g) => &mut *g,
            _ => panic!("Latch is not write-locked; cannot get mutable reference"),
        }
    }
}
