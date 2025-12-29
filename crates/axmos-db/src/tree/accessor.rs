use crate::{
    multithreading::frames::{Latch, MemFrame, ReadLatch, WriteLatch},
    storage::page::BtreePage,
    storage::{Identifiable, core::traits::Buffer},
};

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Display, Formatter, Result as FmtResult},
    hash::Hash,
    io::{self, Error as IoError},
    ops::DerefMut,
};

/// The position represents the visited positions during the BPlustree search algorithm.
/// A position is defined by a page id and a slot number. When we start from the root node, we choose the subsequent pages by iterating over the cells in the slot array (see bplustree.rs), whenever we find the optimal key to follow, we traverse down the tree. We use this tracked positions later in the rebalancing algorithm to know where we where positioned in the parent node in order to reload our siblings.
#[repr(C, packed)]
#[derive(Debug, PartialEq, PartialOrd)]
pub struct Position<P: Buffer<IdType: Eq + Hash + Clone + Copy>>(P::IdType, usize);

pub type BtreePagePosition = Position<BtreePage>;

impl<P> Display for Position<P>
where
    P: Buffer<IdType: Eq + Hash + Clone + Copy + Display>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            " Position: at entry {}, slot {}",
            self.entry(),
            self.slot()
        )
    }
}

impl<P> Clone for Position<P>
where
    P: Buffer<IdType: Eq + Hash + Clone + Copy>,
{
    fn clone(&self) -> Self {
        Self(self.0, self.1)
    }
}

impl<P> Copy for Position<P> where P: Buffer<IdType: Eq + Hash + Clone + Copy> {}

impl<P> Position<P>
where
    P: Buffer<IdType: Eq + Hash + Clone + Copy>,
{
    pub(crate) fn new(entry: P::IdType, slot: usize) -> Self {
        Self(entry, slot)
    }

    /// Utility to create a position that starts at slot 0
    pub(crate) fn start_pos(entry: P::IdType) -> Self {
        Self(entry, 0)
    }

    pub(crate) fn set_entry(&mut self, entry: P::IdType) {
        self.0 = entry;
    }

    pub(crate) fn set_slot(&mut self, slot: usize) {
        self.1 = slot;
    }

    pub(crate) fn entry(&self) -> P::IdType {
        self.0
    }

    pub(crate) fn slot(&self) -> usize {
        self.1
    }
}

pub(crate) trait Accessor<P: Buffer<IdType: Eq + Hash>> {
    type LatchType: Latch<P>;

    fn try_acquire<F: Identifiable<IdType = P::IdType>>(&mut self, value: F) -> io::Result<()>
    where
        for<'a> Self::LatchType: TryFrom<&'a F, Error = IoError>;

    fn release(&mut self, key: P::IdType);

    fn clear(&mut self);
}

pub(crate) trait WriteAccessor<P: Buffer<IdType: Eq + Hash>>:
    Accessor<P, LatchType: DerefMut<Target = P>>
{
    fn get_mut(&mut self, key: P::IdType) -> Option<&mut Self::LatchType>;
}

pub(crate) trait ReadAccessor<P: Buffer<IdType: Eq + Hash>>: Accessor<P> {
    fn get(&self, key: P::IdType) -> Option<&Self::LatchType>;
}

pub(crate) struct AccessTracker<P: Buffer, L: Latch<P>> {
    latches: HashMap<<P as Identifiable>::IdType, L>,
}

impl<P, L> Default for AccessTracker<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    fn default() -> Self {
        Self {
            latches: HashMap::new(),
        }
    }
}

impl<P, L> AccessTracker<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<P, L> Accessor<P> for AccessTracker<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    type LatchType = L;

    /// Acquires a latch for the given access mode.
    fn try_acquire<F: Identifiable<IdType = P::IdType>>(&mut self, value: F) -> io::Result<()>
    where
        for<'a> L: TryFrom<&'a F, Error = IoError>,
    {
        let key = value.id();

        if let Entry::Vacant(e) = self.latches.entry(key) {
            let latch = (&value).try_into()?;
            e.insert(latch);
        }
        Ok(())
    }

    fn release(&mut self, key: P::IdType) {
        self.latches.remove(&key);
    }

    fn clear(&mut self) {
        self.latches.clear();
    }
}

pub(crate) type TraversalStack<P> = Vec<Position<P>>;

impl<P> ReadAccessor<P> for AccessTracker<P, ReadLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get(&self, key: P::IdType) -> Option<&Self::LatchType> {
        self.latches.get(&key)
    }
}

impl<P> WriteAccessor<P> for AccessTracker<P, WriteLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get_mut(&mut self, key: P::IdType) -> Option<&mut Self::LatchType> {
        self.latches.get_mut(&key)
    }
}

impl<P> ReadAccessor<P> for AccessTracker<P, WriteLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get(&self, key: P::IdType) -> Option<&Self::LatchType> {
        self.latches.get(&key)
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

pub(crate) struct BtreeAccessor<P, L: Latch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    tracker: AccessTracker<P, L>,
    traversal: TraversalStack<P>,
}

impl<P, L> !Send for BtreeAccessor<P, L> {}
impl<P, L> !Sync for BtreeAccessor<P, L> {}

type MutableTreeAccessor<P> = BtreeAccessor<P, WriteLatch<P>>;
type ReadTreeAccessor<P> = BtreeAccessor<P, ReadLatch<P>>;

impl<P, L> Default for BtreeAccessor<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    fn default() -> Self {
        Self {
            tracker: AccessTracker::new(),
            traversal: Vec::new(),
        }
    }
}

// The tracker cannot be cloned.
impl<P, L> Clone for BtreeAccessor<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    fn clone(&self) -> Self {
        Self {
            tracker: AccessTracker::new(),
            traversal: self.traversal.clone(),
        }
    }
}

pub(crate) trait PositionStack<P: Buffer<IdType: Eq + Hash>> {
    /// Push a position onto the traversal stack
    fn push_position(&mut self, position: Position<P>);

    /// Pop a position from the traversal stack
    fn pop_position(&mut self) -> Option<Position<P>>;

    /// Peek at the top of the traversal stack
    fn peek_position(&self) -> Option<&Position<P>>;

    /// Get the current traversal depth
    fn depth(&self) -> usize;

    /// Clear the whole traversal.
    fn clear_traversal(&mut self);
}

impl<P, L> BtreeAccessor<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Get immutable reference to traversal stack
    pub(crate) fn traversal(&self) -> &TraversalStack<P> {
        &self.traversal
    }

    /// Get mutable reference to traversal stack
    pub(crate) fn traversal_mut(&mut self) -> &mut TraversalStack<P> {
        &mut self.traversal
    }
}

impl<P, L> PositionStack<P> for BtreeAccessor<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    fn clear_traversal(&mut self) {
        self.traversal_mut().clear()
    }

    fn depth(&self) -> usize {
        self.traversal().len()
    }

    fn peek_position(&self) -> Option<&Position<P>> {
        self.traversal().last()
    }

    fn pop_position(&mut self) -> Option<Position<P>> {
        self.traversal_mut().pop()
    }

    fn push_position(&mut self, position: Position<P>) {
        self.traversal_mut().push(position)
    }
}

/// Accessor implementation for btree accessor.
impl<P, L> Accessor<P> for BtreeAccessor<P, L>
where
    P: Buffer<IdType: Eq + Hash>,
    L: Latch<P>,
{
    type LatchType = L;

    fn try_acquire<F: Identifiable<IdType = P::IdType>>(&mut self, value: F) -> io::Result<()>
    where
        for<'a> L: TryFrom<&'a F, Error = IoError>,
    {
        self.tracker.try_acquire(value)
    }

    fn release(&mut self, key: P::IdType) {
        self.tracker.release(key);
    }

    fn clear(&mut self) {
        self.tracker.clear();
        self.traversal.clear();
    }
}

impl<P> ReadAccessor<P> for BtreeAccessor<P, ReadLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get(&self, key: P::IdType) -> Option<&Self::LatchType> {
        self.tracker.get(key)
    }
}

impl<P> WriteAccessor<P> for BtreeAccessor<P, WriteLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get_mut(&mut self, key: P::IdType) -> Option<&mut Self::LatchType> {
        self.tracker.get_mut(key)
    }
}

impl<P> ReadAccessor<P> for BtreeAccessor<P, WriteLatch<P>>
where
    P: Buffer<IdType: Eq + Hash>,
{
    fn get(&self, key: P::IdType) -> Option<&Self::LatchType> {
        self.tracker.get(key)
    }
}

impl<P> MutableTreeAccessor<P>
where
    P: Buffer<IdType: Eq + Hash>,
{
    /// Try to acquire a write latch and track the position
    pub(crate) fn try_acquire_at<F>(&mut self, value: F, position: Position<P>) -> io::Result<()>
    where
        F: Identifiable<IdType = P::IdType>,
        for<'a> WriteLatch<P>: TryFrom<&'a F, Error = IoError>,
    {
        self.try_acquire(value)?;
        self.push_position(position);
        Ok(())
    }

    /// Release all latches from root towards the current position
    /// This is used when we determine rebalancing is not needed
    pub(crate) fn release_ancestors(&mut self) {
        // Release all but the last position (current page)
        while self.traversal.len() > 1 {
            if let Some(pos) = self.traversal.first() {
                self.tracker.release(pos.entry());
            }
        }
    }

    /// Release a specific latch and remove from traversal if present
    pub(crate) fn release_page(&mut self, entry_id: P::IdType)
    where
        P::IdType: PartialEq,
    {
        self.tracker.release(entry_id);
        self.traversal.retain(|pos| pos.entry() != entry_id);
    }
}

impl<P> ReadTreeAccessor<P>
where
    P: Buffer<IdType: Eq + Hash>,
{
    /// Try to acquire a read latch with lock coupling
    /// Releases the previous latch after acquiring the new one
    pub(crate) fn try_acquire_coupled<F>(&mut self, value: F) -> io::Result<()>
    where
        F: Identifiable<IdType = P::IdType>,
        for<'a> ReadLatch<P>: TryFrom<&'a F, Error = IoError>,
    {
        let new_key = value.id();

        // Acquire the new latch first
        self.try_acquire(value)?;

        // Release the previous latch (lock coupling)
        if let Some(prev_pos) = self.pop_position() {
            self.tracker.release(prev_pos.entry());
        }

        Ok(())
    }

    /// Try to acquire a read latch and track position with lock coupling
    pub(crate) fn try_acquire_coupled_at<F>(
        &mut self,
        value: F,
        position: Position<P>,
    ) -> io::Result<()>
    where
        F: Identifiable<IdType = P::IdType>,
        for<'a> ReadLatch<P>: TryFrom<&'a F, Error = IoError>,
    {
        self.try_acquire_coupled(value)?;
        self.push_position(position);
        Ok(())
    }
}

pub type BtreeReadAccessor = ReadTreeAccessor<BtreePage>;
pub type BtreeWriteAccessor = MutableTreeAccessor<BtreePage>;

pub(crate) trait TreeWriter = WriteAccessor<BtreePage, LatchType: for<'a> TryFrom<&'a MemFrame, Error = IoError>>
    + ReadAccessor<BtreePage, LatchType: for<'a> TryFrom<&'a MemFrame, Error = IoError>>
    + PositionStack<BtreePage>;

pub(crate) trait TreeReader = ReadAccessor<BtreePage, LatchType: for<'a> TryFrom<&'a MemFrame, Error = IoError>>
    + PositionStack<BtreePage>;
