use super::BTree;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{Frame, IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::btreepage::BTreePageOps;
use crate::storage::{Cell, InteriorPageOps, LeafPageOps, Overflowable};
use crate::types::PageId;
use crate::HeaderOps;

use parking_lot::{
    ArcRwLockReadGuard, ArcRwLockUpgradableReadGuard, ArcRwLockWriteGuard, RawRwLock,
};

/// BTree locking operations go here.
/// TODO: this should include a transaction manager that acquires exclusive locks incrementally and deletes them at commit time. However this is not possible still so our tree is not completely ACID.
pub(crate) trait LockingOps<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
    Vl: Cell<Key = K>,
    Vi: Cell<Key = K, Data = PageId>,
    P: Send
        + Sync
        + HeaderOps
        + BTreePageOps
        + InteriorPageOps<Vi, KeyType = K>
        + LeafPageOps<Vl, KeyType = K>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
{
    fn acquire_shared<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockUpgradableReadGuard<RawRwLock, P>, PageFrame<P>);

    fn acquire_exclusive<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockWriteGuard<RawRwLock, P>, PageFrame<P>);

    fn acquire_readonly<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockReadGuard<RawRwLock, P>, PageFrame<P>);

    fn upgrade_shared(
        &self,
        lock: ArcRwLockUpgradableReadGuard<RawRwLock, P>,
    ) -> ArcRwLockWriteGuard<RawRwLock, P> {
        ArcRwLockUpgradableReadGuard::upgrade(lock)
    }

    fn downgrade_exclusive(
        &self,
        lock: ArcRwLockWriteGuard<RawRwLock, P>,
    ) -> ArcRwLockUpgradableReadGuard<RawRwLock, P> {
        ArcRwLockWriteGuard::downgrade_to_upgradable(lock)
    }

    fn store_and_release<FI: FileOps, M: MemoryPool>(
        &self,
        p_frame: PageFrame<P>,
        p_lock: ArcRwLockWriteGuard<RawRwLock, P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()>;
}

impl<K, Vl, Vi, P> LockingOps<K, Vl, Vi, P> for BTree<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq,
    Vl: Cell<Key = K>,
    Vi: Cell<Key = K, Data = PageId>,
    P: Send
        + Sync
        + HeaderOps
        + BTreePageOps
        + InteriorPageOps<Vi, KeyType = K>
        + LeafPageOps<Vl, KeyType = K>
        + Overflowable<LeafContent = Vl>
        + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
{
    fn acquire_shared<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockUpgradableReadGuard<RawRwLock, P>, PageFrame<P>) {
        let frame = self
            .try_get_frame(id, pager)
            .expect("Frame not found! Likely the page id of this frame is poisoned.");
        (frame.read_for_upgrade(), frame)
    }

    fn acquire_exclusive<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockWriteGuard<RawRwLock, P>, PageFrame<P>) {
        let frame = self
            .try_get_frame(id, pager)
            .expect("Frame not found! Likely the page id of this frame is poisoned.");
        (frame.write(), frame)
    }

    fn acquire_readonly<FI: FileOps, M: MemoryPool>(
        &self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> (ArcRwLockReadGuard<RawRwLock, P>, PageFrame<P>) {
        let frame = self
            .try_get_frame(id, pager)
            .expect("Frame not found! Likely the page id of this frame is poisoned.");
        (frame.read(), frame)
    }

    // Store a frame on the page cache, marking it as dirty in consequence.
    // The page cache will eventually flush the content to disk using the pager.
    fn store_and_release<FI: FileOps, M: MemoryPool>(
        &self,
        p_frame: PageFrame<P>,
        p_lock: ArcRwLockWriteGuard<RawRwLock, P>,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        drop(p_lock);
        let io_frame = IOFrame::try_from(p_frame)?;
        pager.cache_page(&io_frame);
        Ok(())
    }
}
