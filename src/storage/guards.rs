use super::{RQLiteIndexPage, RQLiteTablePage};
use crate::serialization::Serializable;
use crate::types::PageId;
use crate::{impl_header_ops_rguard, impl_header_ops_wguard, OverflowPage};
use crate::{HeaderOps, PageType};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock};
use std::ops::Deref;

pub(crate) struct TablePageRead(pub ArcRwLockReadGuard<RawRwLock, RQLiteTablePage>);
impl_header_ops_rguard!(TablePageRead);

impl Deref for TablePageRead {
    type Target = RQLiteTablePage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct TablePageWrite(pub ArcRwLockWriteGuard<RawRwLock, RQLiteTablePage>);
impl_header_ops_wguard!(TablePageWrite);

impl Deref for TablePageWrite {
    type Target = RQLiteTablePage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub(crate) struct IndexPageRead(pub ArcRwLockReadGuard<RawRwLock, RQLiteIndexPage>);
impl_header_ops_rguard!(IndexPageRead);

impl Deref for IndexPageRead {
    type Target = RQLiteIndexPage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub(crate) struct IndexPageWrite(pub ArcRwLockWriteGuard<RawRwLock, RQLiteIndexPage>);
impl_header_ops_wguard!(IndexPageWrite);

impl Deref for IndexPageWrite {
    type Target = RQLiteIndexPage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct OvfPageRead(pub ArcRwLockReadGuard<RawRwLock, OverflowPage>);
impl_header_ops_rguard!(OvfPageRead);

impl Deref for OvfPageRead {
    type Target = OverflowPage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub(crate) struct OvfPageWrite(pub ArcRwLockWriteGuard<RawRwLock, OverflowPage>);
impl_header_ops_wguard!(OvfPageWrite);

impl Deref for OvfPageWrite {
    type Target = OverflowPage;

    fn deref(&self) -> &Self::Target {
        &self.0
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
