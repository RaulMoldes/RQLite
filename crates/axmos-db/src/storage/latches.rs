use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock,
};

use std::{
    any::Any,
    io::{Error as IoError, ErrorKind},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::storage::page::{BtreePage, OverflowPage, PageZero};

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

/// Dynamic dispatch for latches.
#[derive(Debug)]
pub(crate) enum PageLatch {
    Btree(Latch<BtreePage>),
    Overflow(Latch<OverflowPage>),
    Zero(Latch<PageZero>),
}

impl PageLatch {
    #[inline]
    pub(crate) fn upgrade(self) -> Self {
        match self {
            PageLatch::Btree(l) => PageLatch::Btree(l.upgrade()),
            PageLatch::Overflow(l) => PageLatch::Overflow(l.upgrade()),
            PageLatch::Zero(l) => PageLatch::Zero(l.upgrade()),
        }
    }

    #[inline]
    pub(crate) fn downgrade(self) -> Self {
        match self {
            PageLatch::Btree(l) => PageLatch::Btree(l.downgrade()),
            PageLatch::Overflow(l) => PageLatch::Overflow(l.downgrade()),
            PageLatch::Zero(l) => PageLatch::Zero(l.downgrade()),
        }
    }

    #[inline]
    pub(crate) fn downgrade_to_upgradable(self) -> Self {
        match self {
            PageLatch::Btree(l) => PageLatch::Btree(l.downgrade_to_upgradable()),
            PageLatch::Overflow(l) => PageLatch::Overflow(l.downgrade_to_upgradable()),
            PageLatch::Zero(l) => PageLatch::Zero(l.downgrade_to_upgradable()),
        }
    }

    #[inline]
    pub(crate) fn is_read(&self) -> bool {
        match self {
            PageLatch::Btree(l) => l.is_read(),
            PageLatch::Overflow(l) => l.is_read(),
            PageLatch::Zero(l) => l.is_read(),
        }
    }

    #[inline]
    pub(crate) fn is_write(&self) -> bool {
        match self {
            PageLatch::Btree(l) => l.is_write(),
            PageLatch::Overflow(l) => l.is_write(),
            PageLatch::Zero(l) => l.is_write(),
        }
    }

    #[inline]
    pub(crate) fn is_upgradable(&self) -> bool {
        match self {
            PageLatch::Btree(l) => l.is_upgradable(),
            PageLatch::Overflow(l) => l.is_upgradable(),
            PageLatch::Zero(l) => l.is_upgradable(),
        }
    }
}

impl Deref for PageLatch {
    type Target = dyn Any;

    fn deref(&self) -> &Self::Target {
        match self {
            PageLatch::Btree(l) => &**l,
            PageLatch::Overflow(l) => &**l,
            PageLatch::Zero(l) => &**l,
        }
    }
}

impl DerefMut for PageLatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            PageLatch::Btree(l) => &mut **l,
            PageLatch::Overflow(l) => &mut **l,
            PageLatch::Zero(l) => &mut **l,
        }
    }
}

impl<'a> TryFrom<&'a PageLatch> for &'a BtreePage {
    type Error = IoError;

    fn try_from(l: &'a PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Btree(l) => Ok(&**l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as BtreePage",
            )),
        }
    }
}

impl<'a> TryFrom<&'a PageLatch> for &'a OverflowPage {
    type Error = IoError;

    fn try_from(l: &'a PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Overflow(l) => Ok(&**l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as OverflowPage",
            )),
        }
    }
}

impl<'a> TryFrom<&'a PageLatch> for &'a PageZero {
    type Error = IoError;

    fn try_from(l: &'a PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Zero(l) => Ok(&**l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as PageZero",
            )),
        }
    }
}

impl<'a> TryFrom<&'a mut PageLatch> for &'a mut BtreePage {
    type Error = IoError;

    fn try_from(l: &'a mut PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Btree(l) => Ok(&mut **l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as BtreePage",
            )),
        }
    }
}

impl<'a> TryFrom<&'a mut PageLatch> for &'a mut OverflowPage {
    type Error = IoError;

    fn try_from(l: &'a mut PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Overflow(l) => Ok(&mut **l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as OverflowPage",
            )),
        }
    }
}

impl<'a> TryFrom<&'a mut PageLatch> for &'a mut PageZero {
    type Error = IoError;

    fn try_from(l: &'a mut PageLatch) -> Result<Self, Self::Error> {
        match l {
            PageLatch::Zero(l) => Ok(&mut **l),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                "Cannot get as PageZero",
            )),
        }
    }
}

impl AsRef<[u8]> for PageLatch {
    fn as_ref(&self) -> &[u8] {
        match self {
            PageLatch::Btree(l) => l.deref().as_ref(),
            PageLatch::Overflow(l) => l.deref().as_ref(),
            PageLatch::Zero(l) => l.deref().as_ref(),
        }
    }
}

impl AsMut<[u8]> for PageLatch {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            PageLatch::Btree(l) => l.deref_mut().as_mut(),
            PageLatch::Overflow(l) => l.deref_mut().as_mut(),
            PageLatch::Zero(l) => l.deref_mut().as_mut(),
        }
    }
}
