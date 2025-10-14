use super::{RQLiteIndexPage, RQLiteTablePage};
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::{impl_header_ops_guard, OverflowPage};
use crate::{HeaderOps, PageType};
use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock,
};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub(crate) struct ReadOnlyLatch<P>(pub ArcRwLockReadGuard<RawRwLock, P>);

impl_header_ops_guard!(ReadOnlyLatch<P>);

impl<P> ReadOnlyLatch<P> {
    pub(crate) fn lock(lock: &Arc<RwLock<P>>) -> Self {
        Self(lock.read_arc())
    }
}
pub(crate) struct WriteLatch<P>(pub ArcRwLockWriteGuard<RawRwLock, P>);
impl_header_ops_guard!(WriteLatch<P>);

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
impl_header_ops_guard!(UpgradableLatch<P>);

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

impl From<TablePageWrite> for Vec<u8> {
    fn from(guard: TablePageWrite) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteTablePage::Interior(page) => page.write_to(&mut buffer),
            RQLiteTablePage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<IndexPageWrite> for Vec<u8> {
    fn from(guard: IndexPageWrite) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteIndexPage::Interior(page) => page.write_to(&mut buffer),
            RQLiteIndexPage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<OvfPageWrite> for Vec<u8> {
    fn from(guard: OvfPageWrite) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        guard.write_to(&mut buffer).unwrap();
        buffer
    }
}

impl From<TablePageRead> for Vec<u8> {
    fn from(guard: TablePageRead) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteTablePage::Interior(page) => page.write_to(&mut buffer),
            RQLiteTablePage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<IndexPageRead> for Vec<u8> {
    fn from(guard: IndexPageRead) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteIndexPage::Interior(page) => page.write_to(&mut buffer),
            RQLiteIndexPage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<OvfPageRead> for Vec<u8> {
    fn from(guard: OvfPageRead) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        guard.write_to(&mut buffer).unwrap();
        buffer
    }
}

impl From<TablePageUpgradable> for Vec<u8> {
    fn from(guard: TablePageUpgradable) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteTablePage::Interior(page) => page.write_to(&mut buffer),
            RQLiteTablePage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<IndexPageUpgradable> for Vec<u8> {
    fn from(guard: IndexPageUpgradable) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        match &*guard {
            RQLiteIndexPage::Interior(page) => page.write_to(&mut buffer),
            RQLiteIndexPage::Leaf(page) => page.write_to(&mut buffer),
        }
        .unwrap();
        buffer
    }
}

impl From<OvfPageUpgradable> for Vec<u8> {
    fn from(guard: OvfPageUpgradable) -> Self {
        let mut buffer = Vec::with_capacity(guard.page_size());
        guard.write_to(&mut buffer).unwrap();
        buffer
    }
}
