use crate::{
    as_slice,
    io::frames::{MemFrame, ReadLatch, WriteLatch},
    repr_enum,
    storage::{cell::Slot, page::BtreePage},
    types::PageId,
};

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Display, Formatter, Result as FmtResult},
};

/// The position represents the visited positions during the BPlustree search algorithm.
/// A position is defined by a page id and a slot number. When we start from the root node, we choose the subsequent pages by iterating over the cells in the slot array (see bplustree.rs), whenever we find the optimal key to follow, we traverse down the tree. We use this tracked positions later in the rebalancing algorithm to know where we where positioned in the parent node in order to reload our siblings.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Position(PageId, Slot);

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, " Position: page {}, slot {}", self.page(), self.slot())
    }
}

impl Position {
    pub(crate) fn new(page_id: PageId, slot: Slot) -> Self {
        Self(page_id, slot)
    }

    /// Utility to create a position that starts at slot 0
    pub(crate) fn start_pos(page_id: PageId) -> Self {
        Self(page_id, 0)
    }

    pub(crate) fn set_page(&mut self, page_id: PageId) {
        self.0 = page_id;
    }

    pub(crate) fn set_slot(&mut self, slot: u16) {
        self.1 = slot;
    }

    pub(crate) fn page(&self) -> PageId {
        self.0
    }

    pub(crate) fn slot(&self) -> Slot {
        self.1
    }
}
as_slice!(Position);

repr_enum!( pub(crate) enum AccessMode: u8 {
    Read = 0x00,
    Write = 0x01,
});

#[derive(Debug)]
pub(crate) struct BtreeLatchTracker {
    read_latches: HashMap<PageId, ReadLatch<BtreePage>>,
    write_latches: HashMap<PageId, WriteLatch<BtreePage>>,
    access_mode: AccessMode,
}

pub(crate) type TraversalStack = Vec<Position>;

impl BtreeLatchTracker {
    pub(crate) fn new(access_mode: AccessMode) -> Self {
        BtreeLatchTracker {
            read_latches: HashMap::new(),
            write_latches: HashMap::new(),
            access_mode,
        }
    }

    /// Acquires a latch for the given access mode.
    pub(crate) fn acquire(&mut self, value: MemFrame) {
        let key = value.page_number();

        // For correctness purposes, I am not using upgradable latches since they would overcomplicate everything and this is difficult enough.
        // Upgradable latches would allow a thread to read a leaf page and inmediately modify it (possibly causing a rebalance), while other siblings are accessing it. This is one of the issues with using Index - Organized storage.
        match self.access_mode {
            AccessMode::Read => {
                // TODO: It would be ideal not to unwrap here.
                if let Entry::Vacant(e) = self.read_latches.entry(key) {
                    let latch = value.try_read::<BtreePage>().unwrap();
                    e.insert(latch);
                }
            }

            AccessMode::Write => {
                if let Entry::Vacant(e) = self.write_latches.entry(key) {
                    let latch = value.try_write::<BtreePage>().unwrap();
                    e.insert(latch);
                }
            }
        }
    }

    // Get the current access mode of this accessor.
    pub(crate) fn current_access_mode(&self) -> AccessMode {
        self.access_mode
    }

    pub(crate) fn release(&mut self, key: PageId) {
        self.read_latches.remove(&key);
        self.write_latches.remove(&key);
    }

    pub(crate) fn get(&self, key: &PageId) -> Option<&ReadLatch<BtreePage>> {
        self.read_latches.get(&key)
    }

    pub(crate) fn get_mut(&mut self, key: &PageId) -> Option<&mut WriteLatch<BtreePage>> {
        self.write_latches.get_mut(&key)
    }

    pub(crate) fn clear(&mut self) {
        self.read_latches.clear();
        self.write_latches.clear();
    }
}

/// The accessor data structure is the main data structure that can access pages in the btree.
/// Keeps track of the visited pages on its FrameStack.
///
/// The lock acquisition strategy is the following:
///
/// There are two possible cases:
///
/// - Read operations (scans): During scans, the access to the full tree is read-only. Therefore we can safely release the lock on the parent page once after we have acquired the lock on the target child. This is called lock coupling.
///
/// - Write operations (inserts /deletes /updates): The problem here is that we might have to rebalance the tree after the insert is done, therefore we need to make sure that no one else is visiting the tree at the same time.
///
/// Then, what we do is that we acquire the latch on a page, and we DO NOT RELEASE it until we can be completely sure that we do not need it any more (we have check that rebalancing the tree is not need it). At this point, we can release all locks starting from the root towards the leaves so that waiters can start accessing the tree data structure.
///
/// Of course the accessor is neither Send nor Sync, as it holds the lock tracking data structure.
#[derive(Debug)]
pub(crate) struct Accessor {
    tracker: BtreeLatchTracker,
    traversal: TraversalStack,
}

impl !Send for Accessor {}
impl !Sync for Accessor {}

impl Accessor {
    pub(crate) fn new(access_mode: AccessMode) -> Self {
        Self {
            tracker: BtreeLatchTracker::new(access_mode),
            traversal: Vec::new(),
        }
    }

    pub(crate) fn access_mode(&self) -> AccessMode {
        self.tracker.access_mode
    }

    /// See [LatchTracker::acquire]
    pub(crate) fn acquire_latch(&mut self, value: MemFrame) {
        self.tracker.acquire(value);

        // If we are reading, we can safely release the latch on the parent.
        if let Some(parent_node) = self.traversal.last()
            && matches!(self.access_mode(), AccessMode::Read)
        {
            self.release_latch(parent_node.page());
        };
    }

    /// See [LatchTracker::release]
    pub(crate) fn release_latch(&mut self, key: PageId) {
        self.tracker.release(key);
    }

    /// Peek the last visited position without consuming it.
    #[inline]
    pub(crate) fn last(&self) -> Option<&Position> {
        self.traversal.last()
    }

    /// Pop from the stack.
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<Position> {
        self.traversal.pop()
    }

    /// Push a position to the stack
    #[inline]
    pub(crate) fn push(&mut self, position: Position) {
        self.traversal.push(position)
    }

    #[inline]
    pub(crate) fn get(&self, key: &PageId) -> Option<&ReadLatch<BtreePage>> {
        self.tracker.get(key)
    }

    #[inline]
    pub(crate) fn get_mut(&mut self, key: &PageId) -> Option<&mut WriteLatch<BtreePage>> {
        self.tracker.get_mut(key)
    }

    /// Cleanup
    pub(crate) fn clear(&mut self) {
        self.tracker.clear();
        self.traversal.clear();
    }
}
