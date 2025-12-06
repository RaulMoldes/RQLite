use crate::{
    CELL_ALIGNMENT,
    io::frames::{FrameAccessMode, Position},
    storage::{
        cell::{CELL_HEADER_SIZE, Cell, Slot},
        page::{BTREE_PAGE_HEADER_SIZE, BtreePage, OverflowPage},
    },
    transactions::worker::{ThreadWorker, Worker},
    types::{PAGE_ZERO, PageId, VarInt},
};

use super::comparator::Comparator;
use std::{
    cell::{Ref, RefMut},
    cmp::{Ordering, Reverse, min},
    collections::{BTreeMap, BinaryHeap, HashSet, VecDeque},
    io::{self, Error as IoError, ErrorKind},
    marker::PhantomData,
    mem, usize,
};

#[derive(Debug)]
pub(crate) enum SearchResult {
    Found(Position),    // Key found -> return its position
    NotFound(Position), // Key not found, return the last visited page id.
}

#[derive(Debug)]
pub(crate) enum SlotSearchResult {
    FoundInplace(Position),
    FoundBetween(Position),
    NotFound,
}

#[derive(Debug)]
pub(crate) enum NodeStatus {
    Underflow,
    Overflow,
    Balanced,
}

#[derive(Debug)]
pub enum Payload<'b> {
    Boxed(Box<[u8]>),
    Reference(&'b [u8]),
}

impl<'b> AsMut<[u8]> for Payload<'b> {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Boxed(a) => a.as_mut(),
            Self::Reference(a) => panic!("Cannot mutate a read only payload"),
        }
    }
}

impl From<Payload<'_>> for Box<[u8]> {
    fn from(value: Payload<'_>) -> Self {
        match value {
            Payload::Boxed(b) => b,
            Payload::Reference(b) => b.to_vec().into_boxed_slice(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct Child {
    pointer: PageId,
    slot: Slot,
}

impl Child {
    fn new(pointer: PageId, slot: Slot) -> Self {
        Self { pointer, slot }
    }
}

impl<'b> AsRef<[u8]> for Payload<'b> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Reference(reference) => reference,
            Self::Boxed(boxed) => boxed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BPlusTree<Cmp>
where
    Cmp: Comparator,
{
    pub(crate) root: PageId,
    pub(crate) worker: Worker,
    pub(crate) min_keys: usize,
    pub(crate) num_siblings_per_side: usize,
    comparator: Cmp,
}

impl<Cmp> BPlusTree<Cmp>
where
    Cmp: Comparator,
{
    pub(crate) fn new(
        worker: Worker,
        min_keys: usize,
        num_siblings_per_side: usize,
        comparator: Cmp,
    ) -> io::Result<Self> {
        let root = worker.borrow_mut().alloc_page::<BtreePage>()?;
        Ok(Self::from_existent(
            root,
            worker,
            min_keys,
            num_siblings_per_side,
            comparator,
        ))
    }

    pub(crate) fn from_existent(
        root: PageId,
        worker: Worker,
        min_keys: usize,
        num_siblings_per_side: usize,
        comparator: Cmp,
    ) -> Self {
        debug_assert!(min_keys >= 3, "Invalid argument. Minimum allowed keys is 3");
        Self {
            root,
            worker,
            min_keys,
            num_siblings_per_side,
            comparator,
        }
    }

    pub fn root_pos(&self) -> Position {
        Position::start_pos(self.root)
    }

    pub fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        self.comparator.compare(lhs, rhs)
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    pub fn get_root(&self) -> PageId {
        self.root
    }

    pub fn worker(&self) -> Ref<'_, ThreadWorker> {
        self.worker.borrow()
    }

    pub fn worker_mut(&self) -> RefMut<'_, ThreadWorker> {
        self.worker.borrow_mut()
    }

    pub fn clear_worker_stack(&self) {
        self.worker_mut().clear_stack();
    }

    /// Helper that acquires overflow latches, reassembles payload, and returns it.
    /// Handles the full lifecycle of overflow page access.
    fn get_cell_payload(&self, cell: &Cell) -> io::Result<Payload<'_>> {
        if cell.metadata().is_overflow() {
            let overflow_pages =
                self.acquire_overflow_chain(cell.overflow_page(), usize::MAX, 0)?;
            let payload = self.reassemble_payload(cell, &overflow_pages);

            // Release overflow latches
            for ovf_page in overflow_pages {
                self.worker_mut().release_latch(ovf_page);
            }

            Ok(payload)
        } else {
            Ok(Payload::Boxed(cell.used().to_vec().into_boxed_slice()))
        }
    }

    /// Acquires latches on overflow pages, but only as many as needed to get required_size bytes.
    /// Can be used to acquire a full overflow chain if required_size is USIZE::MAX and initial bytes is 0.
    fn acquire_overflow_chain(
        &self,
        start: PageId,
        required_size: usize,
        initial_bytes: usize,
    ) -> io::Result<Vec<PageId>> {
        let mut pages = Vec::new();
        let mut current = start;
        let mut accumulated = initial_bytes.saturating_sub(mem::size_of::<PageId>());

        while current.is_valid() && accumulated < required_size {
            self.worker_mut()
                .acquire::<OverflowPage>(current, FrameAccessMode::Read)?;

            let (next, num_bytes) = self
                .worker()
                .read_page::<OverflowPage, _, _>(current, |ovf| {
                    (ovf.metadata().next, ovf.metadata().num_bytes as usize)
                })?;

            pages.push(current);
            accumulated += num_bytes;
            current = next;
        }

        Ok(pages)
    }

    // Retrieves the content of a cell as a boxed payload from a search result.
    // Will return OK(None) if the cell is not found.
    // Propagates the error if it fails accessing the cell content.
    pub fn get_payload(&self, result: SearchResult) -> io::Result<Option<Payload<'_>>> {
        match result {
            SearchResult::NotFound(_) => Ok(None),
            SearchResult::Found(position) => {
                let page_id = position.page();
                let slot = position.slot();
                let cell = self
                    .worker()
                    .read_page::<BtreePage, _, _>(page_id, |p| p.cell(slot).clone())?;
                Ok(Some(self.get_cell_payload(&cell)?))
            }
        }
    }

    /// Traverses the full tree from the root to the leaf in order to find the position of the key it is asked for.
    pub fn search_from_root(
        &self,
        entry: &[u8],
        access_mode: FrameAccessMode,
    ) -> io::Result<SearchResult> {
        self.search(&self.root_pos(), entry, access_mode)
    }

    /// Searches for a specific key starting at a given position.
    /// For a public API is better to use the [`search_from_root`] as most searches may start at the root node.
    /// Might make it private when I make sure there are no calls to this function in other parts of the system.
    pub fn search(
        &self,
        start: &Position,
        entry: &[u8],
        access_mode: FrameAccessMode,
    ) -> io::Result<SearchResult> {
        let start_page = start.page();
        self.worker_mut()
            .acquire::<BtreePage>(start_page, access_mode)?;

        let last = self.worker().last().cloned();
        // As we are reading, we can safely release the latch on the parent.
        if let Some(parent_node) = last
            && matches!(access_mode, FrameAccessMode::Read)
        {
            self.worker_mut().release_latch(parent_node.page());
        };

        let is_leaf = self
            .worker()
            .read_page::<BtreePage, _, _>(start_page, |p| p.is_leaf())?;

        if is_leaf {
            return self.binary_search_key(start_page, entry);
        };

        // We are on an interior node, therefore we have to visit it in order to keep track of ot in the traversal stack.
        let child = self.find_child(start_page, entry)?;
        match child {
            SearchResult::NotFound(position) | SearchResult::Found(position) => {
                self.worker_mut()
                    .visit(Position::new(start_page, position.slot()))?;
                self.search(&position, entry, access_mode)
            }
        }
    }

    /// Finds the most suitable child to store a particular entry at a given page.
    fn find_child(&self, page_id: PageId, search_key: &[u8]) -> io::Result<SearchResult> {
        let (num_slots, right_child) = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |p| (p.num_slots(), p.metadata().right_child))?;

        let mut result = SearchResult::NotFound(Position::new(right_child, Slot(num_slots)));

        for i in 0..num_slots {
            let is_overflow =
                self.worker()
                    .try_read_page::<BtreePage, _, _>(page_id, |btreepage| {
                        let cell = btreepage.cell(Slot(i));

                        // Moved the check upwards to avoid cloning when it is not necessary.
                        if !cell.metadata().is_overflow()
                            && self.comparator.compare(search_key, cell.used().as_ref())?
                                == Ordering::Less
                        {
                            result = SearchResult::Found(Position::new(
                                cell.metadata().left_child(),
                                Slot(i),
                            ));
                            return Ok(None); // None indicates we should stop the search.
                        };

                        Ok(Some(cell.metadata().is_overflow())) // True is a flag to indicate to clone the cell.
                    })?;

            // If there is some cell returned this means we have found an overflow cell or we have not reach the end.
            if let Some(flag) = is_overflow {
                // If the cell is an overflow cell, then we have to reassemble.
                // Compute the initial and required size and use it to allocate as many overflow pages as needed. Then we can [`reassemble_payload`] with all the preacquired locks.
                // Acquiring one lock at a time caused issues here so I swapped to this approach of pre-reserving access to all the locks.
                if flag {
                    // If the cell is overflow, then we have to clone, there is no way around.
                    let cell =
                        self.worker()
                            .read_page::<BtreePage, _, _>(page_id, |btreepage| {
                                let cell = btreepage.cell(Slot(i));
                                cell.clone()
                            })?;

                    let required_size = if (cell.used().len() - mem::size_of::<PageId>())
                        >= mem::size_of::<VarInt>()
                        || self.comparator.is_fixed_size()
                    {
                        self.comparator.key_size(cell.used())?
                    } else {
                        usize::MAX
                    };

                    let initial_bytes = cell.used().len() - mem::size_of::<PageId>();
                    let start = cell.overflow_page();
                    let overflow_pages =
                        self.acquire_overflow_chain(start, required_size, initial_bytes)?;
                    let payload = self.reassemble_payload(&cell, &overflow_pages);
                    if self.comparator.compare(search_key, payload.as_ref())? == Ordering::Less {
                        result = SearchResult::Found(Position::new(
                            cell.metadata().left_child(),
                            Slot(i),
                        ));
                        break;
                    };
                }
            } else {
                break;
            };
        }

        Ok(result)
    }

    /// Finds the most suitable slot for a particular entry on a given page.
    /// The slot search result can be either:
    /// [`SlotSearchResult::FoundInplace`]: The slot was found to be at a specific position in the page slot array (duplicate key or exact match).
    /// [`SlotSearchResult::FoundBetween`]: The slot was found to be between two existing slots in the page slot array (best case).
    /// [`SlotSearchResult::NotFound`]: The slot was not found in the slot array, then the key should be inserted at the end of the page or an error will be thrown (depends on the case).
    fn find_slot(&self, page_id: PageId, search_key: &[u8]) -> io::Result<SlotSearchResult> {
        let num_slots = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |p| p.num_slots())?;

        if num_slots == 0 {
            return Ok(SlotSearchResult::NotFound);
        }

        let mut result = SlotSearchResult::NotFound;
        for i in 0..num_slots {
            // First we check if the cell is an overflow cell, stopping the search if not
            let is_overflow =
                self.worker()
                    .try_read_page::<BtreePage, _, _>(page_id, |btreepage| {
                        let cell = btreepage.cell(Slot(i));

                        // Moved the check upwards to avoid cloning when it is not necessary.
                        if !cell.metadata().is_overflow() {
                            match self.comparator.compare(search_key, cell.used())? {
                                Ordering::Less => {
                                    result = SlotSearchResult::FoundBetween(Position::new(
                                        page_id,
                                        Slot(i),
                                    ));
                                    return Ok(None);
                                }
                                Ordering::Equal => {
                                    result = SlotSearchResult::FoundInplace(Position::new(
                                        page_id,
                                        Slot(i),
                                    ));
                                    return Ok(None);
                                }
                                _ => {}
                            }
                        };

                        Ok(Some(cell.metadata().is_overflow())) // True is a flag to indicate to clone the cell.
                    })?;

            // Reassemble the payload if needed only.
            if let Some(flag) = is_overflow {
                if flag {
                    // If the cell is overflow, then we have to clone, there is no way around.
                    let cell =
                        self.worker()
                            .read_page::<BtreePage, _, _>(page_id, |btreepage| {
                                let cell = btreepage.cell(Slot(i));
                                cell.clone()
                            })?;

                    let content: Payload = if cell.metadata().is_overflow() {
                        let required_size = if (cell.used().len() - std::mem::size_of::<PageId>())
                            >= std::mem::size_of::<VarInt>()
                            || self.comparator.is_fixed_size()
                        {
                            self.comparator.key_size(cell.used())?
                        } else {
                            usize::MAX
                        };
                        let initial_bytes = cell.used().len() - mem::size_of::<PageId>();
                        let start = cell.overflow_page();
                        let overflow_pages =
                            self.acquire_overflow_chain(start, required_size, initial_bytes)?;
                        self.reassemble_payload(&cell, &overflow_pages)
                    } else {
                        Payload::Reference(cell.used())
                    };

                    match self.comparator.compare(search_key, content.as_ref())? {
                        Ordering::Less => {
                            result =
                                SlotSearchResult::FoundBetween(Position::new(page_id, Slot(i)));
                            break;
                        }
                        Ordering::Equal => {
                            result =
                                SlotSearchResult::FoundInplace(Position::new(page_id, Slot(i)));
                            break;
                        }
                        _ => {}
                    }
                }
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Binary searches over the slots of a Leaf Page in the btree.
    ///
    /// # Returns:
    ///
    ///  - [SearchResult::Found] if it finds the key it is looking for. The position contains the actual found position.
    ///
    /// - [SearchResult::NotFound] if it does not find the key it is looking for.  The position will include the last slot index in the page.
    fn binary_search_key(&self, page_id: PageId, search_key: &[u8]) -> io::Result<SearchResult> {
        let mut slot_count: Slot = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |p| p.num_slots())?
            .into();

        // store the last slot in a register
        let last_slot = slot_count;
        let mut left = Slot(0);
        let mut right = slot_count;

        while left < right {
            let mid = left + slot_count / Slot(2);
            // First we check if the cell is an overflow cell, stopping the search if not
            let is_overflow =
                self.worker()
                    .try_read_page::<BtreePage, _, _>(page_id, |btreepage| {
                        let cell = btreepage.cell(mid);

                        // Moved the check upwards to avoid cloning when it is not necessary.
                        if !cell.metadata().is_overflow() {
                            // The key content is always the first bytes of the cell
                            match self.comparator.compare(search_key, cell.used())? {
                                Ordering::Equal => {
                                    return Ok(None);
                                }
                                Ordering::Greater => left = mid + 1usize,
                                Ordering::Less => right = mid,
                            };
                            slot_count = right - left;
                        };

                        Ok(Some(cell.metadata().is_overflow())) // True is a flag to indicate to clone the cell.
                    })?;

            // Asks the comparator if we need more bytes in order to find the correct path.
            if let Some(flag) = is_overflow {
                if flag {
                    // If the cell is overflow, then we have to clone, there is no way around.
                    let cell =
                        self.worker()
                            .read_page::<BtreePage, _, _>(page_id, |btreepage| {
                                let cell = btreepage.cell(mid);
                                cell.clone()
                            })?;
                    let required_size = if (cell.used().len() - std::mem::size_of::<PageId>())
                        >= std::mem::size_of::<VarInt>()
                        || self.comparator.is_fixed_size()
                    {
                        self.comparator.key_size(cell.used())?
                    } else {
                        usize::MAX
                    };

                    let initial_bytes = cell.used().len() - mem::size_of::<PageId>();
                    let start = cell.overflow_page();
                    let overflow_pages =
                        self.acquire_overflow_chain(start, required_size, initial_bytes)?;
                    let content = self.reassemble_payload(&cell, &overflow_pages);
                    if mid == Slot(0) {
                        println!("SEARCHING FOR cero ");
                    };
                    // The key content is always the first bytes of the cell
                    match self.comparator.compare(search_key, content.as_ref())? {
                        Ordering::Equal => {
                            return Ok(SearchResult::Found(Position::new(page_id, mid)));
                        }
                        Ordering::Greater => left = mid + 1usize,
                        Ordering::Less => right = mid,
                    };

                    slot_count = right - left;
                };
            } else {
                // NO flag. Found the result
                return Ok(SearchResult::Found(Position::new(page_id, mid)));
            }
        }
        Ok(SearchResult::NotFound(Position::new(page_id, last_slot)))
    }

    /// Inserts a key at the corresponding position in the Btree.
    /// Returns an [io::Error] if the key already exists.
    pub fn insert(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let start_pos = Position::start_pos(page_id);
        let search_result = self.search(&start_pos, data, FrameAccessMode::Write)?;

        match search_result {
            SearchResult::Found(pos) => Err(IoError::new(
                ErrorKind::AlreadyExists,
                format!("The key already exists  at position {pos}",),
            )),

            SearchResult::NotFound(pos) => {
                let page_id = pos.page();
                self.insert_cell(page_id, data)
            }
        }?;

        // Cleanup traversal here.
        self.worker_mut().clear_stack();

        Ok(())
    }

    /// Inserts a cell at the corresponding slot on a given page.
    /// Uses the [find_slot] function in order to find the best fit slot for the cell given its key.
    /// Assumes the page is a leaf page.
    /// This method must not be called on interior nodes.
    fn insert_cell(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // Find the corresponding slot in the leaf page.
        let slot_result = self.find_slot(page_id, data)?;

        let free_space = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |btreepage| {
                min(
                    btreepage.max_allowed_payload_size(),
                    btreepage.metadata().free_space as u16,
                )
            })? as usize;

        // TODO: FIGURE OUT HOW TO MOVE THIS LOGIC TO THE PAGER.
        let cell = self.build_cell(free_space, data)?;

        match slot_result {
            // Slot not found so we just append the cell at the end of the page.
            SlotSearchResult::NotFound => {
                self.worker_mut()
                    .write_page::<BtreePage, _, _>(page_id, |btreepage| {
                        let index = btreepage.max_slot_index();
                        btreepage.insert(index, cell);
                    })?
            }
            // Best fit slot is found between two existing slots.
            SlotSearchResult::FoundBetween(pos) => {
                let slot = pos.slot();
                self.worker_mut()
                    .write_page::<BtreePage, _, _>(page_id, |btreepage| {
                        btreepage.insert(slot, cell);
                    })?;
            }
            _ => {}
        };

        // Check the current page status and call balance if required.
        let status = self.check_node_status(page_id)?;
        if !matches!(status, NodeStatus::Balanced) {
            self.balance(page_id)?;
        };

        Ok(())
    }

    /// Updates the cell content on a given leaf page at the given [Position].
    /// Assumes the position pointer is valid and the page is a leaf page.
    fn update_cell(&mut self, pos: Position, data: &[u8]) -> io::Result<()> {
        let page_id = pos.page();
        let slot = pos.slot();
        let free_space = self
            .worker_mut()
            .read_page::<BtreePage, _, _>(page_id, |btreepage| {
                btreepage.max_allowed_payload_size()
            })? as usize;

        let cell = self.build_cell(free_space, data)?;

        // Replace the contents of the page.
        let old_cell = self
            .worker_mut()
            .write_page::<BtreePage, _, _>(page_id, |btreepage| {
                let index = btreepage.max_slot_index();
                btreepage.replace(slot, cell)
            })?;

        // Deallocate the cell
        self.free_cell(old_cell)?;

        let status = self.check_node_status(page_id)?;

        // Balance if needed
        if !matches!(status, NodeStatus::Balanced) {
            self.balance(page_id)?;
        };

        Ok(())
    }

    /// Inserts a key at the corresponding position in the Btree.
    /// Updates its contents if it already exists.
    pub fn upsert(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let start_pos = Position::start_pos(page_id);
        let search_result = self.search(&start_pos, data, FrameAccessMode::Write)?;

        match search_result {
            SearchResult::Found(pos) => self.update_cell(pos, data)?, // This logic is similar to [self.insert]
            SearchResult::NotFound(pos) => {
                let page_id = pos.page();
                self.insert_cell(page_id, data)?;
            }
        };

        // Cleanup traversal here.
        self.worker_mut().clear_stack();
        Ok(())
    }

    /// Updates a key at the corresponding position in the Btree.
    /// Returns an [IoError] if the key is not found.
    pub fn update(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let start_pos = Position::start_pos(page_id);
        let search_result = self.search(&start_pos, data, FrameAccessMode::Write)?;

        match search_result {
            SearchResult::Found(pos) => self.update_cell(pos, data),
            SearchResult::NotFound(_) => {
                Err(IoError::new(ErrorKind::NotFound, "The key does not exist."))
            }
        }?;

        // Cleanup traversal here.
        self.worker_mut().clear_stack();
        Ok(())
    }

    /// Removes the entry corresponding to the given key if it exists.
    /// Returns an [IoError] if the key is not found.
    pub fn remove(&mut self, page_id: PageId, key: &[u8]) -> io::Result<()> {
        let start_pos = Position::start_pos(page_id);
        let search = self.search(&start_pos, key, FrameAccessMode::Write)?;

        match search {
            SearchResult::NotFound(_) => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found on page. Cannot remove",
            )),
            SearchResult::Found(pos) => {
                let page_id = pos.page();
                let slot = pos.slot();
                let cell = self
                    .worker_mut()
                    .write_page::<BtreePage, _, _>(page_id, |btreepage| btreepage.remove(slot))?;
                self.free_cell(cell)?;

                let status = self.check_node_status(page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };
                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.worker_mut().clear_stack();
        Ok(())
    }

    /// Deallocates a cell, the current worker will be responsible for deallocating all overflow pages.
    fn free_cell(&self, cell: Cell) -> io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while overflow_page.is_valid() {
            self.worker_mut()
                .acquire::<OverflowPage>(overflow_page, FrameAccessMode::Read)?;

            let next = self
                .worker()
                .read_page::<OverflowPage, _, _>(overflow_page, |overflow| {
                    overflow.metadata().next
                })?;

            self.worker_mut()
                .dealloc_page::<OverflowPage>(overflow_page)?;

            overflow_page = next;
        }

        Ok(())
    }

    fn build_cell(&self, remaining_space: usize, payload: &[u8]) -> io::Result<Cell> {
        let page_size = self.worker().get_page_size();

        let v = remaining_space.saturating_sub(Slot::SIZE);

        // Store payload in chunks, link overflow pages and return the cell.
        let mut max_payload_size_in_page: usize = if v <= CELL_ALIGNMENT as usize {
            0
        } else {
            ((v - 1) / CELL_ALIGNMENT as usize) * CELL_ALIGNMENT as usize
        };

        max_payload_size_in_page = max_payload_size_in_page
            .saturating_sub(std::mem::size_of::<PageId>())
            .saturating_sub(CELL_HEADER_SIZE);

        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.min_keys),
            max_payload_size_in_page,
        ); // Clamp to the remaining space

        if payload.len() <= max_payload_size {
            return Ok(Cell::new(payload));
        }

        let mut overflow_page_number = self.worker_mut().alloc_page::<OverflowPage>()?;

        let cell = Cell::new_overflow(&payload[..max_payload_size], overflow_page_number);

        let mut stored_bytes = max_payload_size;

        // Note that here we cannot release the latch on an overflow page until all the chain is written. This might waste a lot of memory  as it will keep overflow pages pinned. It is a tradeoff between correctness and efficiency, and for a database, we prefer to play the correctness side of the story.
        loop {
            self.worker_mut()
                .acquire::<OverflowPage>(overflow_page_number, FrameAccessMode::Write)?;

            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            self.worker_mut().write_page::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.data_mut()[..overflow_bytes]
                        .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().num_bytes = overflow_bytes as _;
                    overflow_page.metadata_mut().next = PAGE_ZERO;
                    stored_bytes += overflow_bytes;
                },
            )?;
            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = self.worker_mut().alloc_page::<OverflowPage>()?;

            self.worker_mut().write_page::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.metadata_mut().next = next_overflow_page;
                },
            )?;

            overflow_page_number = next_overflow_page;
        }
        Ok(cell)
    }

    /// Balance shallower algorithm inspired by SQLite (balance shallower)
    /// https://sqlite.org/btreemodule.html
    /// The balance shallower algorithm decreases the height of the tree when the root node has only one child.
    /// [root] -----|
    ///         [right child] ----|
    ///                          [right grandchild]
    ///
    /// 1. Drains the right child cells.
    /// 2. Pushes them to the root node.
    /// 3. The right child gets deallocated.
    /// 4. The right grandchild becomes the new right-child.
    fn balance_shallower(&mut self) -> io::Result<()> {
        let (is_underflow, is_leaf) = self
            .worker()
            .read_page::<BtreePage, _, _>(self.root, |p| (p.is_empty(), p.is_leaf()))?;

        if !is_underflow || is_leaf {
            return Ok(());
        };

        let child_page = self
            .worker()
            .read_page::<BtreePage, _, _>(self.root, |p| p.metadata().right_child)?;

        // TODO: REVIEW IF THIS CAN FAIL DUE TO THE CHILD PAGE LATCH NOT BEING ACQUIRED BY THE CURRENT TRANSACTION
        let grand_child = self
            .worker()
            .read_page::<BtreePage, _, _>(child_page, |p| p.metadata().right_child)?;

        let cells = self
            .worker_mut()
            .write_page::<BtreePage, _, _>(child_page, |p| p.drain(..).collect::<Vec<_>>())?;

        // Release the child latch and deallocate the page
        self.worker_mut().release_latch(child_page);
        self.worker_mut().dealloc_page::<BtreePage>(child_page)?;

        // Refill the old root.
        self.worker_mut()
            .write_page::<BtreePage, _, _>(self.root, |node| {
                cells.into_iter().for_each(|cell| node.push(cell));
                node.metadata_mut().right_child = grand_child;
            })?;
        Ok(())
    }

    /// Splits a mutable slice of cells into two parts, assuming they are sorted by key.
    /// Useful for cell redistribution.
    fn split_cells<'cells>(
        &mut self,
        cells: &'cells mut [Cell],
    ) -> io::Result<(&'cells [Cell], &'cells [Cell])> {
        // Assumes cells are sorted
        let sizes: Vec<u16> = cells.iter().map(|cell| cell.total_size()).collect();
        let total_size: u16 = sizes.iter().sum();
        let half_size = total_size.div_ceil(2);

        // Look for the point where the accumulated result crosses the mid point.
        let mut acc = 0;
        let split_index = sizes
            .iter()
            .position(|&s| {
                acc += s;
                acc >= half_size
            })
            .unwrap_or(cells.len().div_ceil(2));

        // Divide the vector
        Ok(cells.split_at(split_index))
    }

    /// Balance deeper algorithm inspired by SQLite [balance deeper].
    /// Balance deeper is the opposite of balance shallower in the sense that it will allocate two new nodes when the root becomes full, increasing the depth of the tree by one.
    /// The cell in the middle gets propagated upwards.
    /// We also need to fix the pointers in the frontiers between the two new nodes to maintain the leaf node's linked list.
    fn balance_deeper(&mut self) -> io::Result<()> {
        let new_left = self.worker_mut().alloc_page::<BtreePage>()?;
        self.worker_mut()
            .acquire::<BtreePage>(new_left, FrameAccessMode::Write)?;
        let new_right = self.worker_mut().alloc_page::<BtreePage>()?;
        self.worker_mut()
            .acquire::<BtreePage>(new_right, FrameAccessMode::Write)?;

        let old_right_child = self
            .worker()
            .read_page::<BtreePage, _, _>(self.root, |node| node.metadata().right_child)?;

        let was_leaf = !old_right_child.is_valid();
        let page_size = self.worker().get_page_size();

        let mut cells = self
            .worker_mut()
            .write_page::<BtreePage, _, _>(self.root, |node| {
                node.metadata_mut().free_space_ptr = (page_size - BTREE_PAGE_HEADER_SIZE) as u32;
                node.drain(..).collect::<Vec<_>>()
            })?;

        let (left_cells, right_cells) = self.split_cells(&mut cells)?;

        let mut propagated_cell = if was_leaf {
            right_cells.first().unwrap().clone()
        } else {
            left_cells.last().unwrap().clone()
        };
        propagated_cell.metadata_mut().left_child = new_left;

        // Now we move part of the cells to one node and the other part to the other node.
        self.worker_mut()
            .write_page::<BtreePage, _, _>(self.root, |node| {
                node.metadata_mut().right_child = new_right;
                node.push(propagated_cell);
            })?;

        self.worker_mut()
            .write_page::<BtreePage, _, _>(new_left, |node| {
                if !was_leaf {
                    if let Some((last, rest)) = left_cells.split_last() {
                        rest.iter().for_each(|cell| node.push(cell.clone()));
                        node.metadata_mut().right_child = last.metadata().left_child();
                    };
                } else {
                    left_cells.iter().for_each(|cell| node.push(cell.clone()));
                };

                node.metadata_mut().next_sibling = new_right;
                node.metadata_mut().previous_sibling = PAGE_ZERO;
            })?;

        self.worker_mut()
            .write_page::<BtreePage, _, _>(new_right, |node| {
                right_cells.iter().for_each(|cell| node.push(cell.clone()));
                node.metadata_mut().right_child = old_right_child;
                node.metadata_mut().next_sibling = PAGE_ZERO;
                node.metadata_mut().previous_sibling = new_left;
            })?;

        // Fix the frontier links.
        // The right child of the left page must be linked with the left most child of the right page to maintain proper ordering.
        let left_right_most = self
            .worker()
            .read_page::<BtreePage, _, _>(new_left, |node| node.metadata().right_child)?;

        let right_left_most = self
            .worker()
            .read_page::<BtreePage, _, _>(new_right, |node| {
                node.cell(Slot(0)).metadata().left_child()
            })?;

        if left_right_most.is_valid() {
            self.worker_mut()
                .acquire::<BtreePage>(left_right_most, FrameAccessMode::Write)?;
            self.worker_mut()
                .write_page::<BtreePage, _, _>(left_right_most, |p| {
                    p.metadata_mut().next_sibling = right_left_most;
                })?;
        };

        if right_left_most.is_valid() {
            self.worker_mut()
                .acquire::<BtreePage>(right_left_most, FrameAccessMode::Write)?;
            self.worker_mut()
                .write_page::<BtreePage, _, _>(right_left_most, |node| {
                    node.metadata_mut().previous_sibling = left_right_most;
                })?;
        };

        Ok(())
    }

    /// Full balance algorithm.
    /// The description of this algorithm can be found on original SQLite docs: https://sqlite.org/btreemodule.html
    /// The main idea is that you have to take as many pages as specified, recompute a balanced distribution, and redistribute the cells accordingly.
    /// Afterwards, propagate the pointers as required.
    fn balance(&mut self, page_id: PageId) -> io::Result<()> {
        let is_root = self.is_root(page_id);

        let is_leaf = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |p| p.is_leaf())?;

        let status = self.check_node_status(page_id)?;

        // Nothing to do, the node is balanced.
        if matches!(status, NodeStatus::Balanced) {
            return Ok(());
        };

        // Root underflow.
        if is_root && matches!(status, NodeStatus::Underflow) {
            return self.balance_shallower();
        };

        // Root overflow.
        if is_root && matches!(status, NodeStatus::Overflow) {
            return self.balance_deeper();
        };

        let parent_position = self.worker_mut().pop().ok_or(IoError::new(
            ErrorKind::NotFound,
            "Parent not found on traversal stack!",
        ))?;

        let parent_page = parent_position.page();

        // Internal/Leaf node Overflow/Underflow.
        self.balance_siblings(page_id, &parent_position)?;

        // No need to fuck anything else up.
        if matches!(self.check_node_status(page_id)?, NodeStatus::Balanced) {
            return Ok(());
        };

        // Obtain the parent and its siblings (uncles) of the current node.
        let (parent_prev, last_parent_slot, parent_next) = self
            .worker()
            .read_page::<BtreePage, _, _>(parent_page, |node| {
                (
                    node.metadata().previous_sibling,
                    node.max_slot_index(),
                    node.metadata().next_sibling,
                )
            })?;

        // Obtain the right most child of the parent's left sibling (right most cousin)
        let parent_prev_last = if parent_prev.is_valid() {
            self.worker_mut()
                .acquire::<BtreePage>(parent_prev, FrameAccessMode::Write)?;
            self.worker_mut()
                .write_page::<BtreePage, _, _>(parent_prev, |p| p.metadata().right_child)?
        } else {
            PAGE_ZERO
        };

        // Obtain the first child of the parent's next sibling (left most cousin)
        let parent_next_first = if parent_next.is_valid() {
            self.worker_mut()
                .acquire::<BtreePage>(parent_next, FrameAccessMode::Write)?;
            self.worker()
                .read_page::<BtreePage, _, _>(parent_next, |p| {
                    p.cell(Slot(0)).metadata().left_child()
                })?
        } else {
            PAGE_ZERO
        };

        // Load siblings
        let mut siblings =
            self.load_siblings(page_id, &parent_position, self.num_siblings_per_side)?;

        // Load the frontiers in case it is necessary.
        let last_non_allocated_sibling_slot = siblings.iter().last().unwrap().slot;
        let first_non_allocated_sibling_slot = siblings.front().unwrap().slot;

        // Load the left frontier from the parent
        let left_frontier = if first_non_allocated_sibling_slot > Slot(0) {
            let child_slot = first_non_allocated_sibling_slot - 1usize;
            self.load_child(parent_page, child_slot)
        } else {
            None
        };

        // Load the right frontier from the parent.
        let right_frontier = if last_non_allocated_sibling_slot < last_parent_slot {
            let child_slot = last_non_allocated_sibling_slot + 1usize;
            self.load_child(parent_page, child_slot)
        } else {
            None
        };

        // Read the page size from the pager.
        let page_size = self.worker().get_page_size();
        let mut cells = std::collections::VecDeque::new();

        // As we remove cells from the parent, slots shrink towards the left.
        // Therefore we need to always remove the first slot in order to clean up the parent node.
        let slot_to_remove = siblings[0].slot;

        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            // As always, when visiting sibling nodes we need to remember to acquire their corresponding latches, as they might have not been visited before.
            self.worker_mut()
                .acquire::<BtreePage>(sibling.pointer, FrameAccessMode::Write)?;

            // Extract this node's cells and return the right child.
            let right_child =
                self.worker_mut()
                    .write_page::<BtreePage, _, _>(sibling.pointer, |node| {
                        // Extract the cells and reset the free space pointer on this node.
                        cells.extend(node.drain(..));
                        // This is not needed probably, added it for consistency.
                        node.metadata_mut().free_space_ptr =
                            page_size.saturating_sub(BTREE_PAGE_HEADER_SIZE) as u32;
                        // Return the right child because we might need to create an additonal cell for it
                        node.metadata().right_child
                    })?;

            // If we are not the last sibling we can safely link with the next in the chain.
            // If we are the last sibling we use the right frontier.
            // If there is no right frontier we are the parent's right most and there is no [next].
            let next_sibling = if i < siblings.len() - 1 {
                siblings[i + 1].pointer
            } else if let Some(frontier) = right_frontier {
                frontier.pointer
            } else {
                PAGE_ZERO
            };

            // If the right child is different from [ZERO] and the netx sibling too,
            // extract the first cell of the first child on our next sibling and allocate a new cell.
            // This always happens on interior nodes rebalancing, as right_child is not set on leaf nodes.
            if right_child.is_valid() && next_sibling.is_valid() {
                self.worker_mut()
                    .acquire::<BtreePage>(next_sibling, FrameAccessMode::Write)?;

                // Obtain the first child
                let first_child_next = self
                    .worker()
                    .read_page::<BtreePage, _, _>(next_sibling, |node| {
                        node.cell(Slot(0)).metadata().left_child()
                    })?;

                self.worker_mut()
                    .acquire::<BtreePage>(first_child_next, FrameAccessMode::Write)?;

                // Obtain the first cell of the first child of our sibling.
                let mut first_cell_child = self
                    .worker()
                    .read_page::<BtreePage, _, _>(first_child_next, |node| {
                        node.cell(Slot(0)).clone()
                    })?;

                // Make the copied cell point to our right child and push it to the chain.
                first_cell_child.metadata_mut().left_child = right_child;
                cells.push_back(first_cell_child);
            };

            // Remove our entry from the parent node.
            self.worker_mut()
                .write_page::<BtreePage, _, _>(parent_page, |node| {
                    // Bounds checking just for safety.
                    if slot_to_remove.0 < node.num_slots() {
                        node.remove(slot_to_remove);
                    };
                })?;
        }

        // Obtain the maximum data that we can fit on this page.
        let usable_space = BtreePage::overflow_threshold(page_size) as u16;

        // These two arrays track the redistribution computation of the rebalancing algorithm.
        let mut total_size_in_each_page = vec![0];
        let mut number_of_cells_per_page = vec![0];

        for cell in &cells {
            let bucket = number_of_cells_per_page.len() - 1;

            // If we can add the cell, we push it to the current bucket.
            if total_size_in_each_page[bucket] + cell.storage_size() <= usable_space {
                number_of_cells_per_page[bucket] += 1;
                total_size_in_each_page[bucket] += cell.storage_size();
            } else {
                // If not, we create a new bucket and append this cell to it.
                number_of_cells_per_page.push(1);
                total_size_in_each_page.push(cell.storage_size());
            };
        }

        // Account for underflow towards the right.
        // At least two pages are needed in order to be able to account for that underflow.
        if number_of_cells_per_page.len() >= 2 {
            // The divider cell will be the first cell of the last page.
            let mut divider_cell = cells.len() - number_of_cells_per_page.last().unwrap() - 1;

            // Iterate backwards, moving cells towards the right.
            for i in (1..=(total_size_in_each_page.len() - 1)).rev() {
                while total_size_in_each_page[i] < BtreePage::underflow_threshold(page_size) as u16
                {
                    number_of_cells_per_page[i] += 1;
                    total_size_in_each_page[i] += &cells[divider_cell].storage_size();

                    number_of_cells_per_page[i - 1] -= 1;
                    total_size_in_each_page[i - 1] -= &cells[divider_cell - 1].storage_size();
                    divider_cell -= 1;
                }
            }

            // Second page has more data than the first one, make a little
            // adjustment to keep it left biased.
            if total_size_in_each_page[0] < BtreePage::underflow_threshold(page_size) as u16 {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
            };
        };

        // Take the last right child of the siblings chain.
        // For interior pages this is required in order to unfuck the last pointer.
        let old_right_child = self
            .worker()
            .read_page::<BtreePage, _, _>(siblings.iter().last().unwrap().pointer, |node| {
                node.metadata().right_child
            })?;

        // Allocate missing pages.
        while siblings.len() < number_of_cells_per_page.len() {
            let parent_index = siblings.iter().last().unwrap().slot + 1usize;
            let new_page = self.worker_mut().alloc_page::<BtreePage>()?;

            self.worker_mut()
                .acquire::<BtreePage>(new_page, FrameAccessMode::Write)?;
            self.worker()
                .read_page::<BtreePage, _, _>(new_page, |node| {
                    debug_assert!(
                        node.metadata().num_slots == 0,
                        "Recently allocated page: {new_page} is not empty"
                    );
                })?;
            siblings.push_back(Child::new(new_page, parent_index));
        }

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            let sibling = siblings.pop_back().unwrap();
            let slot = sibling.slot;
            let page = sibling.pointer;

            self.worker().read_page::<BtreePage, _, _>(page, |node| {
                debug_assert!(
                    node.metadata().num_slots == 0,
                    "About to deallocated page: {page} is not empty"
                );
            })?;

            self.worker_mut().release_latch(page);
            self.worker_mut().dealloc_page::<BtreePage>(page)?;
        }

        // Put pages in ascending order to favor sequential IO where possible.
        BinaryHeap::from_iter(siblings.iter().map(|s| Reverse(s.pointer)))
            .iter()
            .enumerate()
            .for_each(|(i, Reverse(page))| siblings[i].pointer = *page);

        // Fix the last child pointer.
        let last_sibling = siblings[siblings.len() - 1];
        self.worker_mut()
            .write_page::<BtreePage, _, _>(last_sibling.pointer, |node| {
                node.metadata_mut().right_child = old_right_child;
            })?;

        // If there is no right frontier, it means the last sibling in the chain is also the parent's right most.
        if right_frontier.is_none() {
            // Fix pointers in the parent in case we have allocated new pages.
            self.worker_mut()
                .write_page::<BtreePage, _, _>(parent_page, |node| {
                    node.metadata_mut().right_child = last_sibling.pointer;
                })?;
        };

        // Begin redistribution.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let propagated = self.worker_mut().try_write_page::<BtreePage, _, _>(
                siblings[i].pointer,
                |node| {
                    // Push all the cells to the child.
                    let num_cells = if is_leaf {
                        *n
                    } else {
                        *n - 1 // For interior nodes we reserve the last cell, which is the one we are going to propagate to the parent.
                    };

                    for _ in 0..num_cells {
                        node.push(cells.pop_front().unwrap());
                    }

                    // Maintain the linked list of nodes in the same level.
                    if i > 0 {
                        node.metadata_mut().previous_sibling = siblings[i - 1].pointer;
                    } else if let Some(frontier) = left_frontier {
                        node.metadata_mut().previous_sibling = frontier.pointer;
                    } else {
                        node.metadata_mut().previous_sibling = parent_prev_last;
                    };

                    if i < (siblings.len() - 1) {
                        node.metadata_mut().next_sibling = siblings[i + 1].pointer;
                    } else if let Some(frontier) = right_frontier {
                        node.metadata_mut().next_sibling = frontier.pointer;
                    } else {
                        node.metadata_mut().next_sibling = parent_next_first;
                    };

                    // For leaf nodes, we always propagate the first cell of the next node.
                    if i < siblings.len() - 1 && is_leaf {
                        let mut divider = cells.front().unwrap().clone();
                        node.metadata_mut().right_child = divider.metadata().left_child;
                        divider.metadata_mut().left_child = siblings[i].pointer;
                        return Ok(Some(divider));

                    // For interior nodes there are two possible cases.
                    // CASE A: We are not the right most of the parent.
                    // On that case we simply propagate our last cell and also use it as our right most.
                    } else if !is_leaf && (i < siblings.len() - 1 || right_frontier.is_some()) {
                        let mut divider = cells.pop_front().unwrap();
                        node.metadata_mut().right_child = divider.metadata().left_child;
                        divider.metadata_mut().left_child = siblings[i].pointer;
                        return Ok(Some(divider));

                    // CASE B: We are the right most of the parent,
                    // Then there is no cell to propagate upwards. We simply set our right child
                    } else if !is_leaf {
                        node.push(cells.pop_front().unwrap());
                    };

                    Ok(None)
                },
            )?;

            // Fix the pointer in the parent
            if let Some(divider) = propagated {
                self.worker_mut()
                    .write_page::<BtreePage, _, _>(parent_page, |node| {
                        node.insert(siblings[i].slot, divider);
                    })?;
            };
        }

        // Fix the frontier links.
        // Right frontier
        let frontier_divider = if let Some(frontier) = right_frontier {
            self.worker_mut()
                .acquire::<BtreePage>(frontier.pointer, FrameAccessMode::Write)?;
            self.worker_mut()
                .try_write_page::<BtreePage, _, _>(frontier.pointer, |node| {
                    // Maintain linked with the last sibling in the chain
                    node.metadata_mut().previous_sibling = last_sibling.pointer;
                    debug_assert_ne!(
                        node.metadata().next_sibling,
                        siblings[0].pointer,
                        "Found infinite loop in leaf pages linked list"
                    );
                    // For leaf nodes, we propagate the first child as a divider.
                    // For interior nodes this is not necessary as we use the last child of the previous sibling instead.
                    if is_leaf {
                        Ok(Some(node.cell(Slot(0)).clone()))
                    } else {
                        Ok(None)
                    }
                })?
        } else {
            None
        };

        // Left frontier.
        if let Some(frontier) = left_frontier {
            self.worker_mut()
                .acquire::<BtreePage>(frontier.pointer, FrameAccessMode::Write)?;
            self.worker_mut()
                .write_page::<BtreePage, _, _>(frontier.pointer, |node| {
                    // Maintain linked with the last sibling in the chain
                    node.metadata_mut().next_sibling = siblings[0].pointer;

                    debug_assert_ne!(
                        node.metadata().previous_sibling,
                        last_sibling.pointer,
                        "Found infinite loop in leaf pages linked list"
                    );
                })?;
        };

        //  Push the right frontier divider to the parent.
        if let Some(mut divider) = frontier_divider {
            self.worker_mut()
                .write_page::<BtreePage, _, _>(parent_page, |node| {
                    divider.metadata_mut().left_child = last_sibling.pointer;
                    node.insert(last_sibling.slot, divider);
                })?;
        };

        // Done, propagate upwards.
        self.balance(parent_page)?;

        Ok(())
    }

    fn load_child(&self, page: PageId, slot: Slot) -> Option<Child> {
        self.worker()
            .read_page::<BtreePage, _, _>(page, |node| {
                let last_slot = node.max_slot_index();
                if slot >= Slot(0) && slot <= last_slot {
                    Some(Child::new(node.child(slot), slot))
                } else {
                    None
                }
            })
            .ok()?
    }

    fn load_siblings(
        &mut self,
        page: PageId,
        parent_position: &Position,
        num_siblings_per_side: usize,
    ) -> io::Result<VecDeque<Child>> {
        let slot = parent_position.slot();
        let parent = parent_position.page();
        let last_child = self
            .worker()
            .read_page::<BtreePage, _, _>(parent, |node| node.max_slot_index())?;

        let mut loaded: HashSet<PageId> = HashSet::new();

        let mut siblings = VecDeque::new();
        siblings.push_back(Child::new(page, slot));
        loaded.insert(page);
        let mut next = slot + 1usize;
        let mut added_count = 0;

        while next <= last_child && added_count < num_siblings_per_side {
            if let Some(child) = self.load_child(parent, next) {
                if !loaded.contains(&child.pointer) {
                    siblings.push_back(child);
                    loaded.insert(child.pointer);
                } else {
                    break;
                };
            } else {
                break;
            };
            next += 1usize;
            added_count += 1;
        }

        added_count = 0;
        next = Slot(slot.0.saturating_sub(1));

        while next >= Slot(0) && added_count < num_siblings_per_side {
            if let Some(child) = self.load_child(parent, next) {
                if !loaded.contains(&child.pointer) {
                    siblings.push_front(child);
                    loaded.insert(child.pointer);
                } else {
                    break;
                };
            } else {
                break;
            };
            next = Slot(next.0.saturating_sub(1));
            added_count += 1;
        }

        Ok(siblings)
    }

    fn check_node_status(&self, page_id: PageId) -> io::Result<NodeStatus> {
        let is_root = self.is_root(page_id);

        let (is_underflow, is_overflow) =
            self.worker()
                .read_page::<BtreePage, _, _>(page_id, |node| {
                    (
                        (node.has_underflown() && !is_root) || (is_root && node.is_empty()),
                        node.has_overflown(),
                    )
                })?;

        if is_overflow {
            return Ok(NodeStatus::Overflow);
        } else if is_underflow {
            return Ok(NodeStatus::Underflow);
        }
        Ok(NodeStatus::Balanced)
    }

    // This should be only called on leaf nodes. on interior nodes it is a brainfuck to also be fixing pointers to right most children every time we borrow a single cell.
    // See SQLite 3.0 balance siblings algorithm: https://sqlite.org/btreemodule.html
    fn balance_siblings(&mut self, page_id: PageId, parent_pos: &Position) -> io::Result<()> {
        let is_leaf = self
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |node| node.is_leaf())?;

        if !is_leaf {
            return Ok(());
        };

        let status = self.check_node_status(page_id)?;

        let mut siblings = self.load_siblings(page_id, parent_pos, 1)?; // One sibling per side

        if siblings.is_empty() || siblings.len() == 1 {
            return Ok(());
        };

        match status {
            NodeStatus::Balanced => Ok(()),
            NodeStatus::Overflow => {
                // Okey, we are on overflow state and our left sibling is valid. Then we can ask him if it has space for us.
                if let Some(sibling_left) = siblings.pop_front()
                    && sibling_left.pointer != page_id
                {
                    self.worker_mut()
                        .acquire::<BtreePage>(sibling_left.pointer, FrameAccessMode::Write)?;
                    // The load siblings function can return invalid pointers. we need to check for the validity ourselves.
                    let first_cell_size = self
                        .worker()
                        .read_page::<BtreePage, _, _>(page_id, |node| {
                            node.cell(Slot(0)).storage_size()
                        })?;

                    // Check if we can fit the cell there
                    let has_space = self
                        .worker()
                        .read_page::<BtreePage, _, _>(sibling_left.pointer, |node| {
                            node.has_space_for(first_cell_size as usize)
                        })?;

                    // Fantastic! our friend has space for us.
                    // Lets push our cell there.
                    if has_space {
                        let removed_cell = self
                            .worker_mut()
                            .write_page::<BtreePage, _, _>(page_id, |node| node.remove(Slot(0)))?;

                        self.worker_mut().write_page::<BtreePage, _, _>(
                            sibling_left.pointer,
                            |node| {
                                node.push(removed_cell);
                            },
                        )?;

                        // Haha, but now our parent might have fucked up.
                        // We need to unfuck him.
                        // In order to unfuck the parent, we replace the entry that pointed to left with our new max key
                        let separator_index = sibling_left.slot;
                        self.fix_single_pointer(
                            parent_pos.page(),
                            sibling_left.pointer,
                            page_id,
                            separator_index,
                        )?;
                    };
                };

                let status = self.check_node_status(page_id)?;

                if let Some(sibling_right) = siblings.pop_back()
                    && sibling_right.pointer != page_id
                    && matches!(status, NodeStatus::Overflow)
                {
                    self.worker_mut()
                        .acquire::<BtreePage>(sibling_right.pointer, FrameAccessMode::Write)?;

                    let last_cell_size =
                        self.worker()
                            .read_page::<BtreePage, _, _>(page_id, |node| {
                                let last_index = node.max_slot_index() - 1usize;
                                node.cell(last_index).storage_size()
                            })?;

                    // Check if we can fit the cell there
                    let has_space = self
                        .worker()
                        .read_page::<BtreePage, _, _>(sibling_right.pointer, |node| {
                            node.has_space_for(last_cell_size as usize)
                        })?;

                    // Fantastic! our friend has space for us.
                    // Lets push our cell there.
                    if has_space {
                        let removed_cell = self
                            .worker_mut()
                            .write_page::<BtreePage, _, _>(page_id, |node| {
                                node.remove(node.max_slot_index() - 1usize)
                            })?;

                        self.worker_mut().write_page::<BtreePage, _, _>(
                            sibling_right.pointer,
                            |node| {
                                // It must go to the first slot
                                node.insert(Slot(0), removed_cell);
                            },
                        )?;

                        // In this case is the same
                        let separator_index = sibling_right.slot - 1usize;
                        self.fix_single_pointer(
                            parent_pos.page(),
                            page_id,
                            sibling_right.pointer,
                            separator_index,
                        )?;
                    };
                };
                Ok(())
            }
            NodeStatus::Underflow => {
                // Okey, we are on underflow state and our right sibling is valid. Then we can ask him if it has cells for us.
                if let Some(sibling_right) = siblings.pop_back()
                    && sibling_right.pointer != page_id
                {
                    self.worker_mut()
                        .acquire::<BtreePage>(sibling_right.pointer, FrameAccessMode::Write)?;
                    let can_borrow = self.worker().read_page::<BtreePage, _, _>(
                        sibling_right.pointer,
                        |node| {
                            let first_cell_size = node.cell(Slot(0)).storage_size() as usize;
                            node.can_release_space(first_cell_size)
                        },
                    )?;

                    // Fantastic! our friend has data for us.
                    // Lets steal the cell.
                    if can_borrow {
                        let removed_cell = self
                            .worker_mut()
                            .write_page::<BtreePage, _, _>(sibling_right.pointer, |node| {
                                node.remove(Slot(0))
                            })?;

                        self.worker_mut()
                            .write_page::<BtreePage, _, _>(page_id, |node| {
                                node.push(removed_cell);
                            })?;

                        // In this case is the same
                        let separator_index = sibling_right.slot - 1usize;
                        self.fix_single_pointer(
                            parent_pos.page(),
                            page_id,
                            sibling_right.pointer,
                            separator_index,
                        )?;
                    };
                };
                if let Some(sibling_left) = siblings.pop_front() {
                    let status = self.check_node_status(page_id)?;
                    if sibling_left.pointer != page_id && matches!(status, NodeStatus::Overflow) {
                        self.worker_mut()
                            .acquire::<BtreePage>(sibling_left.pointer, FrameAccessMode::Write)?;
                        let can_borrow = self.worker().read_page::<BtreePage, _, _>(
                            sibling_left.pointer,
                            |node| {
                                let last_cell_size =
                                    node.cell(node.max_slot_index()).storage_size() as usize;
                                node.can_release_space(last_cell_size)
                            },
                        )?;

                        // Fantastic! our friend has data for us.
                        // Lets steal the cell.
                        if can_borrow {
                            let removed_cell = self
                                .worker_mut()
                                .write_page::<BtreePage, _, _>(sibling_left.pointer, |node| {
                                    node.remove(node.max_slot_index())
                                })?;

                            self.worker_mut()
                                .write_page::<BtreePage, _, _>(page_id, |node| {
                                    node.insert(Slot(0), removed_cell);
                                })?;

                            let separator_index = sibling_left.slot;
                            self.fix_single_pointer(
                                parent_pos.page(),
                                sibling_left.pointer,
                                page_id,
                                separator_index,
                            )?;
                        };
                    };
                };

                Ok(())
            }
        }

        // This algorithm is more efficient than reasseambling everyting in the tree. However, it has the disadvantage that it can fuck up the pointers in the parent nodes.
        // We are responsible from unfucking them.
    }

    fn fix_single_pointer(
        &mut self,
        parent: PageId,
        left_child: PageId,
        right_child: PageId,
        separator_idx: Slot,
    ) -> io::Result<()> {
        let mut separator = self
            .worker()
            .read_page::<BtreePage, _, _>(right_child, |node| node.cell(Slot(0)).clone())?;

        separator.metadata_mut().left_child = left_child;

        self.worker_mut()
            .write_page::<BtreePage, _, _>(parent, |node| {
                if node.num_slots() == separator_idx {
                    node.metadata_mut().right_child = separator.metadata().left_child();
                } else {
                    node.replace(separator_idx, separator);
                };
            })
    }

    /// Reads a node into memory
    fn read_into_mem(&self, root: PageId, buf: &mut Vec<(PageId, BtreePage)>) -> io::Result<()> {
        // Must acquire the page in read mode in order to read it
        self.worker_mut()
            .acquire::<BtreePage>(root, FrameAccessMode::Read)?;
        let children = self.worker().read_page::<BtreePage, _, _>(root, |node| {
            buf.push((root, node.clone()));
            node.iter_children().collect::<Vec<_>>()
        })?;

        for page in children {
            self.read_into_mem(page, buf)?;
        }

        Ok(())
    }

    /// Utility to print a bplustree as json.
    pub fn json(&self) -> io::Result<String> {
        let mut nodes = Vec::new();

        self.read_into_mem(self.root, &mut nodes)?;

        nodes.sort_by(|(page_num1, _), (page_num2, _)| page_num1.cmp(page_num2));

        let mut string = String::from('[');

        string.push_str(&self.node_json(nodes[0].0, &nodes[0].1)?);

        for (page_num, node) in &nodes[1..] {
            string.push(',');
            string.push_str(&self.node_json(*page_num, node)?);
        }

        string.push(']');

        Ok(string)
    }

    /// Prints the content of a single page as a json string.
    fn node_json(&self, number: PageId, page: &BtreePage) -> io::Result<String> {
        let mut string = format!("{{\"page\":\"{number}\",\"entries\":[");

        if !page.is_empty() {
            let key = &page.cell(Slot(0)).used();
            string.push_str(&format!("{key:?}"));

            for i in 1..page.num_slots() {
                string.push(',');
                string.push_str(&format!("{:?}", &page.cell(Slot(i)).used()));
            }
        }

        string.push_str("],\"children\":[");

        if page.metadata().right_child.is_valid() {
            string.push_str(&format!("\"{}\"", page.child(Slot(0))));

            for child in page.iter_children().skip(1) {
                string.push_str(&format!(",\"{child}\""));
            }
        }

        string.push(']');
        string.push('}');

        Ok(string)
    }

    /// Reassembles payload when overflow page latches are already held.
    /// Does not acquire any new latches.
    fn reassemble_payload(&self, cell: &Cell, overflow_pages: &[PageId]) -> Payload<'_> {
        let mut payload = Vec::from(&cell.used()[..cell.len() - std::mem::size_of::<PageId>()]);

        for &ovf_page in overflow_pages {
            self.worker()
                .read_page::<OverflowPage, _, _>(ovf_page, |ovfpage| {
                    payload.extend_from_slice(ovfpage.payload());
                })
                .expect("Overflow page should be accessible with latch held");
        }

        Payload::Boxed(payload.into_boxed_slice())
    }

    /// Executes a callback on the cell at the given position, returning a reference-based payload when possible.
    /// Only clones data for overflow cells that need reassembly.
    pub fn with_cell_at<F, R>(&self, pos: Position, f: F) -> io::Result<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let page_id = pos.page();
        let slot = pos.slot();

        self.worker_mut()
            .acquire::<BtreePage>(page_id, FrameAccessMode::Read)?;

        // First, check if it's an overflow cell and get the overflow page if needed
        let (is_overflow, overflow_start) =
            self.worker()
                .read_page::<BtreePage, _, _>(page_id, |btree| {
                    let cell = btree.cell(slot);
                    if cell.metadata().is_overflow() {
                        (true, cell.overflow_page())
                    } else {
                        (false, PAGE_ZERO)
                    }
                })?;

        let result = if is_overflow {
            // Acquire all overflow pages first
            let overflow_pages = self.acquire_overflow_chain(overflow_start, usize::MAX, 0)?;

            // Now reassemble with all latches held
            let payload = self
                .worker()
                .read_page::<BtreePage, _, _>(page_id, |btree| {
                    let cell = btree.cell(slot);
                    self.reassemble_payload(&cell, &overflow_pages)
                })?;

            let result = f(payload.as_ref());

            // Release overflow latches
            for ovf_page in overflow_pages {
                self.worker_mut().release_latch(ovf_page);
            }

            result
        } else {
            self.worker()
                .read_page::<BtreePage, _, _>(page_id, |btree| {
                    let cell = btree.cell(slot);
                    f(cell.used())
                })?
        };

        // We cannot release the latch here!
        // self.worker_mut().release_latch(page_id);
        Ok(result)
    }

    /// Batch process multiple positions, grouping by page for efficiency.
    /// Calls the callback for each position with a reference to the cell data.
    pub fn with_cells_at<F, R>(&self, positions: &[Position], mut f: F) -> io::Result<Vec<R>>
    where
        F: FnMut(Position, &[u8]) -> R,
    {
        let mut results = Vec::with_capacity(positions.len());

        // Group positions by page for better locality
        let mut grouped: BTreeMap<PageId, Vec<(usize, Slot)>> = BTreeMap::new();

        for (idx, pos) in positions.iter().enumerate() {
            grouped
                .entry(pos.page())
                .or_default()
                .push((idx, pos.slot()));
        }

        // Temporary storage for results in original order
        let mut indexed_results: Vec<(usize, R)> = Vec::with_capacity(positions.len());

        for (page_id, slots) in grouped {
            self.worker_mut()
                .acquire::<BtreePage>(page_id, FrameAccessMode::Read)?;

            // First pass: identify overflow cells and collect their chains
            let cell_info: Vec<(usize, Slot, bool, PageId)> = self
                .worker()
                .read_page::<BtreePage, _, _>(page_id, |btree| {
                    slots
                        .iter()
                        .map(|(idx, slot)| {
                            let cell = btree.cell(*slot);
                            let is_overflow = cell.metadata().is_overflow();
                            let ovf_start = if is_overflow {
                                cell.overflow_page()
                            } else {
                                PAGE_ZERO
                            };
                            (*idx, *slot, is_overflow, ovf_start)
                        })
                        .collect()
                })?;

            // Process each cell
            for (original_idx, slot, is_overflow, ovf_start) in cell_info {
                let pos = Position::new(page_id, slot);

                let result = if is_overflow {
                    // Acquire overflow chain latches
                    let overflow_pages = self.acquire_overflow_chain(ovf_start, usize::MAX, 0)?;

                    // Reassemble with latches held
                    let payload = self
                        .worker()
                        .read_page::<BtreePage, _, _>(page_id, |btree| {
                            let cell = btree.cell(slot);
                            self.reassemble_payload(&cell, &overflow_pages)
                        })?;

                    let result = f(pos, payload.as_ref());

                    // Release overflow latches
                    for ovf_page in overflow_pages {
                        self.worker_mut().release_latch(ovf_page);
                    }

                    result
                } else {
                    self.worker()
                        .read_page::<BtreePage, _, _>(page_id, |btree| {
                            let cell = btree.cell(slot);
                            f(pos, cell.used())
                        })?
                };

                indexed_results.push((original_idx, result));
            }

            //  self.worker_mut().release_latch(page_id);
        }

        // Sort by original index and extract results
        indexed_results.sort_by_key(|(idx, _)| *idx);
        results.extend(indexed_results.into_iter().map(|(_, r)| r));

        Ok(results)
    }

    /// Computes the tree height by traversing from root to leaf.
    pub fn height(&self) -> io::Result<usize> {
        let mut height = 1usize;
        let mut current = self.root;
        loop {
            self.worker_mut()
                .acquire::<BtreePage>(current, FrameAccessMode::Read)?;

            let (is_leaf, first_child) =
                self.worker()
                    .read_page::<BtreePage, _, _>(current, |btreepage| {
                        (
                            btreepage.is_leaf(),
                            btreepage.cell(Slot(0)).metadata().left_child(),
                        )
                    })?;

            self.worker_mut().release_latch(current);

            if is_leaf {
                return Ok(height);
            }
            height += 1;
            current = first_child;
        }
    }

    /// Creates an iterator over cell slots from the leftmost position in the tree.
    pub fn iter_positions(&mut self) -> io::Result<BPlusTreePositionIterator<'_, Cmp>> {
        let mut current = self.root;

        loop {
            self.worker_mut()
                .acquire::<BtreePage>(current, FrameAccessMode::Read)?;

            let (is_leaf, first_child) =
                self.worker()
                    .read_page::<BtreePage, _, _>(current, |btreepage| {
                        let num_slots = btreepage.num_slots();
                        let first_child = if num_slots > 0 {
                            btreepage.cell(Slot(0)).metadata().left_child()
                        } else {
                            PAGE_ZERO
                        };
                        (btreepage.is_leaf(), first_child)
                    })?;

            self.worker_mut().release_latch(current);

            if is_leaf {
                // For empty trees, return an iterator that will immediately return None
                return BPlusTreePositionIterator::from_position(
                    self,
                    current,
                    0,
                    IterDirection::Forward,
                );
            }

            // If interior node has no children, tree is malformed but handle gracefully
            if !first_child.is_valid() {
                return BPlusTreePositionIterator::from_position(
                    self,
                    current,
                    0,
                    IterDirection::Forward,
                );
            }

            current = first_child;
        }
    }

    /// Iterates the tree backward, starting from the right most slot and visiting leaf pages in reverse order.
    pub fn iter_positions_rev(&mut self) -> io::Result<BPlusTreePositionIterator<'_, Cmp>> {
        let mut current = self.root;

        loop {
            self.worker_mut()
                .acquire::<BtreePage>(current, FrameAccessMode::Read)?;

            let (is_leaf, num_slots, last_child) =
                self.worker()
                    .read_page::<BtreePage, _, _>(current, |btree_page| {
                        (
                            btree_page.is_leaf(),
                            btree_page.num_slots(),
                            btree_page.metadata().right_child,
                        )
                    })?;

            self.worker_mut().release_latch(current);

            if is_leaf {
                // Start at last slot
                let start_slot = if num_slots > 0 {
                    (num_slots - 1) as i16
                } else {
                    -1 // Will immediately return None in backward iteration
                };

                return BPlusTreePositionIterator::from_position(
                    self,
                    current,
                    start_slot,
                    IterDirection::Backward,
                );
            }

            if !last_child.is_valid() {
                // Start at last slot
                let start_slot = if num_slots > 0 {
                    (num_slots - 1) as i16
                } else {
                    -1 // Will immediately return None in backward iteration
                };
                // Malformed interior node, handle gracefully
                return BPlusTreePositionIterator::from_position(
                    self,
                    current,
                    start_slot,
                    IterDirection::Backward,
                );
            }

            current = last_child;
        }
    }

    /// Creates an iterator over cell slots from the provided position in the tree.
    pub fn iter_positions_from(
        &mut self,
        key: &[u8],
        direction: IterDirection,
    ) -> io::Result<BPlusTreePositionIterator<'_, Cmp>> {
        let start_pos = Position::start_pos(self.root);
        let position = self.search(&start_pos, key, FrameAccessMode::Read)?;
        let pos = match position {
            SearchResult::Found(pos) | SearchResult::NotFound(pos) => pos,
        };
        let slot: u16 = pos.slot().into();
        BPlusTreePositionIterator::from_position(self, pos.page(), slot as i16, direction)
    }

    /// Deallocates the entire tree using BFS traversal.
    /// More efficient than position iterator since we only need page IDs.
    /// The intuition is the following:
    /// Starts from the root, accumulates all unique children and overflow pages and deallocates them in order.
    pub fn dealloc(&mut self) -> io::Result<()> {
        let mut queue: VecDeque<PageId> = VecDeque::new();
        let mut visited: HashSet<PageId> = HashSet::new();
        let mut overflow_chains: HashSet<PageId> = HashSet::new(); // Use HashSet to deduplicate.
        // During the balancing algorithm, some cells get copied to interior nodes.
        // When this cells have overflow pages linked to them, we do not copy the entire overflow chain, but just the first cell and create a pointer alias on the copied cell. The problem is that this caused duplicate overflow pages to appear in the list of pages to deallocate, generating an infinite deallocation loop.
        // I fixed it out using a [Set] instead of a [Vec] here.

        queue.push_back(self.root);

        while let Some(page_id) = queue.pop_front() {
            if !page_id.is_valid() || visited.contains(&page_id) {
                continue;
            }
            visited.insert(page_id);

            self.worker_mut()
                .acquire::<BtreePage>(page_id, FrameAccessMode::Read)?;

            let (children, overflows) =
                self.worker()
                    .read_page::<BtreePage, _, _>(page_id, |btree| {
                        let children: Vec<PageId> = btree.iter_children().collect();

                        let overflows: Vec<PageId> = (0..btree.num_slots())
                            .filter_map(|i| {
                                let cell = btree.cell(Slot(i));
                                if cell.metadata().is_overflow() {
                                    Some(cell.overflow_page())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        (children, overflows)
                    })?;

            self.worker_mut().release_latch(page_id);

            for child in children {
                if child.is_valid() && !visited.contains(&child) {
                    queue.push_back(child);
                }
            }

            // Insert into HashSet automatically deduplicates
            for ovf in overflows {
                overflow_chains.insert(ovf);
            }
        }

        // Deallocate overflow chains (now deduplicated)
        for overflow_start in overflow_chains {
            self.dealloc_overflow_chain(overflow_start)?;
        }

        // Deallocate btree pages
        for page_id in visited {
            self.worker_mut().dealloc_page::<BtreePage>(page_id)?;
        }

        Ok(())
    }

    /// Deallocates an entire overflow chain starting from the given page.
    fn dealloc_overflow_chain(&self, start: PageId) -> io::Result<()> {
        let mut current = start;

        while current.is_valid() {
            self.worker_mut()
                .acquire::<OverflowPage>(current, FrameAccessMode::Read)?;

            let next = self
                .worker()
                .read_page::<OverflowPage, _, _>(current, |overflow| overflow.metadata().next)?;

            self.worker_mut().release_latch(current);
            self.worker_mut().dealloc_page::<OverflowPage>(current)?;

            current = next;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IterDirection {
    Forward,
    Backward,
}

/// Iterator that yields cell positions instead of entire [`Payloads<'_>`]
/// My first idea was iterate over full payload objects, however, this comes with the downside of having to create a copy of each cell we visit. This is pretty awful specially with overflow cells that occupy a lot of space.
/// Positions are always small (10 bytes data structures) and therefore the time required to iterate remaings constant. Later, if required, we provide the [with_cells_at] and [with_cell_at] functions to execute custom payloads at specific positions in the btree.
pub struct BPlusTreePositionIterator<'a, Cmp>
where
    Cmp: Comparator,
{
    tree: &'a BPlusTree<Cmp>,
    current_page: Option<PageId>,
    current_slot: i16,
    direction: IterDirection,
    _phantom: PhantomData<Cmp>,
}

impl<'a, Cmp> BPlusTreePositionIterator<'a, Cmp>
where
    Cmp: Comparator,
{
    pub fn from_position(
        tree: &'a BPlusTree<Cmp>,
        page: PageId,
        slot: i16,
        direction: IterDirection,
    ) -> io::Result<Self> {
        tree.worker_mut()
            .acquire::<BtreePage>(page, FrameAccessMode::Read)?;

        Ok(Self {
            tree,
            current_page: Some(page),
            current_slot: slot,
            direction,
            _phantom: PhantomData,
        })
    }

    fn adv(&mut self) -> io::Result<bool> {
        if let Some(current) = self.current_page {
            let next_page = self
                .tree
                .worker()
                .read_page::<BtreePage, _, _>(current, |btree_page| {
                    btree_page.metadata().next_sibling
                })?;

            if next_page.is_valid() {
                self.tree
                    .worker_mut()
                    .acquire::<BtreePage>(next_page, FrameAccessMode::Read)?;
                self.tree.worker_mut().release_latch(current);
                self.current_page = Some(next_page);
                self.current_slot = 0;
                return Ok(true);
            }

            self.tree.worker_mut().release_latch(current);
        }

        self.current_page = None;
        Ok(false)
    }

    fn rev(&mut self) -> io::Result<bool> {
        if let Some(current) = self.current_page {
            let prev_page = self
                .tree
                .worker()
                .read_page::<BtreePage, _, _>(current, |btree_page| {
                    btree_page.metadata().previous_sibling
                })?;

            if prev_page.is_valid() {
                self.tree
                    .worker_mut()
                    .acquire::<BtreePage>(prev_page, FrameAccessMode::Read)?;
                self.current_slot = self
                    .tree
                    .worker()
                    .read_page::<BtreePage, _, _>(prev_page, |btree| {
                        btree.num_slots() as i16 - 1
                    })?;

                self.tree.worker_mut().release_latch(current);
                self.current_page = Some(prev_page);
                return Ok(true);
            }

            self.tree.worker_mut().release_latch(current);
        }

        self.current_page = None;
        Ok(false)
    }
}

impl<'a, Cmp> Iterator for BPlusTreePositionIterator<'a, Cmp>
where
    Cmp: Comparator,
{
    type Item = io::Result<Position>;

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.current_page?;

        match self.direction {
            IterDirection::Forward => {
                let num_slots = match self
                    .tree
                    .worker()
                    .read_page::<BtreePage, _, _>(page, |btree| btree.num_slots() as i16)
                {
                    Ok(n) => n,
                    Err(e) => return Some(Err(e)),
                };

                if self.current_slot < num_slots {
                    let pos = Position::new(page, Slot(self.current_slot as u16));
                    self.current_slot += 1;
                    Some(Ok(pos))
                } else {
                    match self.adv() {
                        Ok(true) => self.next(),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }
            IterDirection::Backward => {
                if self.current_slot >= 0 {
                    let pos = Position::new(page, Slot(self.current_slot as u16));
                    self.current_slot -= 1;
                    Some(Ok(pos))
                } else {
                    match self.rev() {
                        Ok(true) => self.next(),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }
        }
    }
}

impl<'a, Cmp> Drop for BPlusTreePositionIterator<'a, Cmp>
where
    Cmp: Comparator,
{
    fn drop(&mut self) {
        if let Some(page) = self.current_page {
            self.tree.worker_mut().release_latch(page);
        }
    }
}

impl<'a, Cmp> DoubleEndedIterator for BPlusTreePositionIterator<'a, Cmp>
where
    Cmp: Comparator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let original_direction = self.direction;
        self.direction = match original_direction {
            IterDirection::Forward => IterDirection::Backward,
            IterDirection::Backward => IterDirection::Forward,
        };

        let result = self.next();
        self.direction = original_direction;
        result
    }
}
