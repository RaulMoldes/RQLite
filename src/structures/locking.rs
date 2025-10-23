use super::try_get_frame;
use crate::btree::allocate_page;
use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::io::frames::{Frame, IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::guards::*;
use crate::storage::{Cell, HeaderOps, InteriorPageOps};
use crate::types::PageId;
use crate::PageType;
use std::collections::HashMap;

/// Thread local traverse stack.
pub(crate) struct TraverseStack<P> {
    read_only_latches: HashMap<PageId, ReadOnlyLatch<P>>,
    write_latches: HashMap<PageId, WriteLatch<P>>,
    upgradable_latches: HashMap<PageId, UpgradableLatch<P>>,
    tracked_frames: HashMap<PageId, PageFrame<P>>,
    visited_pages: Vec<PageId>,
    _not_send: std::marker::PhantomData<*mut ()>,
}

impl<P> Default for TraverseStack<P> {
    fn default() -> Self {
        Self {
            read_only_latches: HashMap::new(),
            write_latches: HashMap::new(),
            upgradable_latches: HashMap::new(),
            tracked_frames: HashMap::new(),
            visited_pages: Vec::new(),
            _not_send: std::marker::PhantomData,
        }
    }
}

impl<P> TraverseStack<P>
where
    P: Send + Sync + HeaderOps + std::fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: From<PageFrame<P>>,
{
    pub(crate) fn track_rlatch<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) {
        if !self.write_latches.contains_key(&id)
            && !self.read_only_latches.contains_key(&id)
            && !self.upgradable_latches.contains_key(&id)
        {
            let frame = try_get_frame(id, pager).expect(
                "Pointer to page id might be dangling. Unable to get frame from the pager.",
            );
            let latch = frame.read();
            self.visited_pages.push(id);
            self.tracked_frames.insert(id, frame);
            self.read_only_latches.insert(id, latch);
        } else if self.write_latches.contains_key(&id) {
            self.full_downgrade_latch(id);
        };
    }

    pub(crate) fn track_wlatch<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) {
        if !self.read_only_latches.contains_key(&id)
            && !self.write_latches.contains_key(&id)
            && !self.upgradable_latches.contains_key(&id)
        {
            let frame = try_get_frame(id, pager).expect(
                "Pointer to page id might be dangling. Unable to get frame from the pager.",
            );
            let latch = frame.write();
            self.visited_pages.push(id);
            self.tracked_frames.insert(id, frame);
            self.write_latches.insert(id, latch);
        } else if self.upgradable_latches.contains_key(&id) {
            self.upgrade_latch(id);
        }
    }

    pub(crate) fn track_slatch<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: PageId,
        pager: &mut Pager<FI, M>,
    ) {
        if !self.write_latches.contains_key(&id)
            && !self.read_only_latches.contains_key(&id)
            && !self.upgradable_latches.contains_key(&id)
        {
            let frame = try_get_frame(id, pager).expect(
                "Pointer to page id might be dangling. Unable to get frame from the pager.",
            );
            let latch = frame.read_for_upgrade();
            self.visited_pages.push(id);
            self.tracked_frames.insert(id, frame);
            self.upgradable_latches.insert(id, latch);
        } else if !self.write_latches.contains_key(&id) {
            self.downgrade_latch(id);
        };
    }

    pub(crate) fn allocate_for_read<FI: FileOps, M: MemoryPool>(
        &mut self,
        page_type: PageType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<PageId> {
        let frame = allocate_page(page_type, pager)?;
        let latch = frame.read();
        let id = frame.id();
        self.visited_pages.push(id);
        self.tracked_frames.insert(id, frame);
        self.read_only_latches.insert(id, latch);
        Ok(id)
    }

    pub(crate) fn allocate_for_write<FI: FileOps, M: MemoryPool>(
        &mut self,
        page_type: PageType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<PageId> {
        let frame = allocate_page(page_type, pager)?;
        let id = frame.id();
        let latch = frame.write();
        self.visited_pages.push(id);
        self.tracked_frames.insert(id, frame);
        self.write_latches.insert(id, latch);
        Ok(id)
    }

    pub(crate) fn allocate_for_upgrade<FI: FileOps, M: MemoryPool>(
        &mut self,
        page_type: PageType,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<PageId> {
        let frame = allocate_page(page_type, pager)?;
        let latch = frame.read_for_upgrade();
        let id = frame.id();
        self.visited_pages.push(id);
        self.tracked_frames.insert(id, frame);
        self.upgradable_latches.insert(id, latch);
        Ok(id)
    }

    pub(crate) fn reverse(&mut self) {
        self.visited_pages.reverse();
    }

    pub(crate) fn is_node_interior(&self, page_id: &PageId) -> bool {
        if let Some(latch) = self.write_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexInterior)
                || matches!(latch.type_of(), PageType::TableInterior);
        } else if let Some(latch) = self.upgradable_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexInterior)
                || matches!(latch.type_of(), PageType::TableInterior);
        } else if let Some(latch) = self.read_only_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexInterior)
                || matches!(latch.type_of(), PageType::TableInterior);
        }
        false
    }

    pub(crate) fn is_node_leaf(&self, page_id: &PageId) -> bool {
        if let Some(latch) = self.write_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexLeaf)
                || matches!(latch.type_of(), PageType::TableLeaf);
        } else if let Some(latch) = self.upgradable_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexLeaf)
                || matches!(latch.type_of(), PageType::TableLeaf);
        } else if let Some(latch) = self.read_only_latches.get(page_id) {
            return matches!(latch.type_of(), PageType::IndexLeaf)
                || matches!(latch.type_of(), PageType::TableLeaf);
        }
        false
    }

    pub(crate) fn upgrade_latch(&mut self, id: PageId) {
        if let Some(s_latch) = self.upgradable_latches.remove(&id) {
            let w_latch = s_latch.upgrade();

            self.write_latches.insert(id, w_latch);
        };
    }

    pub(crate) fn downgrade_latch(&mut self, id: PageId) {
        if let Some(s_latch) = self.write_latches.remove(&id) {
            let w_latch = s_latch.downgrade_to_upgradable();
            self.upgradable_latches.insert(id, w_latch);
        };
    }

    pub(crate) fn full_downgrade_latch(&mut self, id: PageId) {
        if let Some(s_latch) = self.write_latches.remove(&id) {
            let w_latch = s_latch.downgrade();
            self.read_only_latches.insert(id, w_latch);
        };
    }

    pub(crate) fn release_latch<FI: FileOps, M: MemoryPool>(
        &mut self,
        id: &PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        if let Some(frame) = self.tracked_frames.remove(id) {
            if let Some(latch) = self.write_latches.remove(id) {
                drop(latch);
            } else if let Some(latch) = self.upgradable_latches.remove(id) {
                drop(latch);
            } else if let Some(latch) = self.read_only_latches.remove(id) {
                drop(latch);
            }
        };

        Ok(())
    }

    pub(crate) fn release_until<FI: FileOps, M: MemoryPool>(
        &mut self,
        stop_id: PageId,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        // Release pages in order until reach stop id.
        let mut released = Vec::new();

        for id in &self.visited_pages {
            if *id == stop_id {
                break;
            }
            released.push(*id);
        }

        // Remove from the visited vector.
        self.visited_pages.retain(|id| !released.contains(id));

        for id in released {
            if let Some(frame) = self.tracked_frames.remove(&id) {
                if let Some(latch) = self.write_latches.remove(&id) {
                    drop(latch);
                } else if let Some(latch) = self.upgradable_latches.remove(&id) {
                    drop(latch);
                } else if let Some(latch) = self.read_only_latches.remove(&id) {
                    drop(latch);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn cleanup<FI: FileOps, M: MemoryPool>(
        &mut self,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        for id in self.visited_pages.drain(..) {
            if let Some(frame) = self.tracked_frames.remove(&id) {
                if let Some(latch) = self.write_latches.remove(&id) {
                    drop(latch);
                } else if let Some(latch) = self.upgradable_latches.remove(&id) {
                    drop(latch);
                } else if let Some(latch) = self.read_only_latches.remove(&id) {
                    drop(latch);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn navigate<K: Ord + Clone + Copy + Eq + PartialEq, Vi: Cell<Key = K>>(
        &self,
        id: &PageId,
        key: K,
    ) -> Option<PageId>
    where
        P: InteriorPageOps<Vi>,
    {
        if self.is_node_interior(id) {
            if let Some(latch) = self.read_only_latches.get(id) {
                return Some(latch.find_child(&key));
            } else if let Some(latch) = self.upgradable_latches.get(id) {
                return Some(latch.find_child(&key));
            } else if let Some(latch) = self.write_latches.get(id) {
                return Some(latch.find_child(&key));
            };
        };
        None
    }

    pub(crate) fn last(&self) -> Option<&PageId> {
        self.visited_pages.last()
    }

    pub(crate) fn first(&self) -> Option<&PageId> {
        self.visited_pages.first()
    }

    pub(crate) fn read(&self, id: &PageId) -> &ReadOnlyLatch<P> {
        self.read_only_latches
            .get(id)
            .expect("Asked for an invalid pointer. Lock for page was not acquired")
    }

    pub(crate) fn write(&mut self, id: &PageId) -> &mut WriteLatch<P> {
        if let Some(frame) = self.tracked_frames.get_mut(id) {
            frame.mark_dirty();
        };

        self.write_latches
            .get_mut(id)
            .expect("Asked for an invalid pointer. Lock for page was not acquired")
    }

    pub(crate) fn read_upgradable(&self, id: &PageId) -> &UpgradableLatch<P> {
        self.upgradable_latches
            .get(id)
            .expect("Asked for an invalid pointer. Lock for page was not acquired")
    }

    /// Returns an iterator over the visited page IDs
    pub(crate) fn iter(&self) -> impl Iterator<Item = &PageId> {
        self.visited_pages.iter()
    }

    /// Returns a mutable iterator over the visited page IDs
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut PageId> {
        self.visited_pages.iter_mut()
    }

    /// Consumes the TraverseStack and returns an iterator over owned PageIds
    pub(crate) fn into_iter(self) -> impl Iterator<Item = PageId> {
        self.visited_pages.into_iter()
    }

    /// Returns an iterator over pairs of consecutive page IDs
    pub(crate) fn iter_pairs(&self) -> impl Iterator<Item = (&PageId, &PageId)> {
        self.visited_pages
            .windows(2)
            .map(|window| (&window[0], &window[1]))
    }

    /// Returns an iterator over chunks of 2 page IDs
    pub(crate) fn iter_chunks(&self) -> std::slice::Chunks<'_, PageId> {
        self.visited_pages.chunks(2)
    }

    /// Returns an iterator over pairs of consecutive page IDs
    pub(crate) fn iter_pairs_rev(&self) -> impl Iterator<Item = (&PageId, &PageId)> {
        self.visited_pages
            .windows(2)
            .rev()
            .map(|window| (&window[1], &window[0])) // Invert the pair too
    }

    /// Returns the number of nodes in the visited stack
    pub(crate) fn len(&self) -> usize {
        self.visited_pages.len()
    }
}

impl<'a, P> IntoIterator for &'a TraverseStack<P> {
    type Item = &'a PageId;
    type IntoIter = std::slice::Iter<'a, PageId>;

    fn into_iter(self) -> Self::IntoIter {
        self.visited_pages.iter()
    }
}

impl<'a, P> IntoIterator for &'a mut TraverseStack<P> {
    type Item = &'a mut PageId;
    type IntoIter = std::slice::IterMut<'a, PageId>;

    fn into_iter(self) -> Self::IntoIter {
        self.visited_pages.iter_mut()
    }
}

impl<P> IntoIterator for TraverseStack<P> {
    type Item = PageId;
    type IntoIter = std::vec::IntoIter<PageId>;

    fn into_iter(self) -> Self::IntoIter {
        self.visited_pages.into_iter()
    }
}
