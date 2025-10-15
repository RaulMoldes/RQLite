use super::{RQLiteIndexPage, RQLiteTablePage};
use crate::OverflowPage;
use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock,
};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// I am using latches to control the multithreaded access to the index.
/// A [Latch] is to a database what a [Lock] is to the OS.
/// On the database world, you acquire locks on actual database objects.
/// Therefore the naming `atches` is just to distinguish from that.
///
/// The latch implementation I am using is from [`ParkingLot`]
/// I refer to this blogpost in case you are interested on it:
/// https://webkit.org/blog/6161/locking-in-webkit/
///
/// My system allows to have either multiple readers or a single writer at a time.
///
/// Locks are acquired at a page level. I am using [ArcRwLock] instead of just RwLock since it
/// has a static lifetime. As parking lot locks are low size, there should be no memory issues on that.
pub(crate) struct ReadOnlyLatch<P>(pub ArcRwLockReadGuard<RawRwLock, P>);

impl<P> ReadOnlyLatch<P> {
    pub(crate) fn lock(lock: &Arc<RwLock<P>>) -> Self {
        Self(lock.read_arc())
    }
}
pub(crate) struct WriteLatch<P>(pub ArcRwLockWriteGuard<RawRwLock, P>);

impl<P> WriteLatch<P> {
    pub(crate) fn lock(lock: &Arc<RwLock<P>>) -> Self {
        Self(lock.write_arc())
    }
    pub(crate) fn downgrade_to_upgradable(self) -> UpgradableLatch<P> {
        UpgradableLatch(ArcRwLockWriteGuard::downgrade_to_upgradable(self.0))
    }

    pub(crate) fn downgrade(self) -> ReadOnlyLatch<P> {
        ReadOnlyLatch(ArcRwLockWriteGuard::downgrade(self.0))
    }
}

impl<P> DerefMut for WriteLatch<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub(crate) struct UpgradableLatch<P>(pub ArcRwLockUpgradableReadGuard<RawRwLock, P>);

impl<P> UpgradableLatch<P> {
    pub(crate) fn lock(lock: &Arc<RwLock<P>>) -> Self {
        Self(RwLock::upgradable_read_arc(lock))
    }

    pub(crate) fn upgrade(self) -> WriteLatch<P> {
        WriteLatch(ArcRwLockUpgradableReadGuard::upgrade(self.0))
    }
}

impl<P> Deref for UpgradableLatch<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P> Deref for WriteLatch<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P> Deref for ReadOnlyLatch<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) type TablePageWrite = WriteLatch<RQLiteTablePage>;
pub(crate) type TablePageRead = ReadOnlyLatch<RQLiteTablePage>;
pub(crate) type TablePageUpgradable = UpgradableLatch<RQLiteTablePage>;

pub(crate) type IndexPageWrite = WriteLatch<RQLiteIndexPage>;
pub(crate) type IndexPageRead = ReadOnlyLatch<RQLiteIndexPage>;
pub(crate) type IndexPageUpgradable = UpgradableLatch<RQLiteIndexPage>;

pub(crate) type OvfPageWrite = WriteLatch<OverflowPage>;
pub(crate) type OvfPageRead = ReadOnlyLatch<OverflowPage>;
pub(crate) type OvfPageUpgradable = UpgradableLatch<OverflowPage>;
