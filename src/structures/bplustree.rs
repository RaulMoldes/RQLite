use crate::io::frames::MemFrame;
use crate::io::pager::SharedPager;
use crate::storage::cell::{CELL_HEADER_SIZE, SLOT_SIZE};
use crate::storage::page::{
    BtreePage, BtreePageHeader, MemPage, OverflowPage, OverflowPageHeader, BTREE_PAGE_HEADER_SIZE,
};
use crate::storage::{
    cell::{Cell, Slot},
    latches::Latch,
};
use crate::CELL_ALIGNMENT;

use std::cmp::min;
use std::cmp::Ordering;

use crate::types::{PageId, PAGE_ZERO};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};

type Position = (PageId, Slot);

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
enum Payload<'b> {
    Boxed(Box<[u8]>),
    Reference(&'b [u8]),
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

#[repr(u8)]
enum SiblingLoadMode {
    Closest,
    All,
}
#[derive(Debug)]
pub(crate) struct BPlusTree<K>
where
    K: Ord + for<'a> TryFrom<&'a [u8], Error = std::io::Error> + AsRef<[u8]>,
{
    pub(crate) root: PageId,
    pub(crate) shared_pager: SharedPager,
    pub(crate) min_keys: usize,
    pub(crate) num_siblings_per_side: usize,
    __comparator: std::marker::PhantomData<K>,
}

#[derive(Debug)]
struct LatchStackFrame {
    latches: HashMap<PageId, Latch<MemPage>>,
    traversal: Vec<Position>,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeAccessMode {
    Read,
    Write,
    ReadWrite,
}

thread_local! {
    static LATCH_STACK: RefCell<LatchStackFrame> =
        RefCell::new(LatchStackFrame { latches: HashMap::new(), traversal: Vec::new() });
}

impl LatchStackFrame {
    fn acquire(&mut self, key: PageId, value: MemFrame<MemPage>, access_mode: NodeAccessMode) {
        if let Some(existing_latch) = self.latches.get(&key) {
            match (existing_latch, access_mode) {
                (Latch::Upgradable(p), NodeAccessMode::Write) => {
                    let write_latch = self.latches.remove(&key).unwrap().upgrade();
                    self.latches.insert(key, write_latch);
                }
                (Latch::Write(p), NodeAccessMode::Read) => {
                    let read_latch = self.latches.remove(&key).unwrap().downgrade();
                    self.latches.insert(key, read_latch);
                }
                (Latch::Read(p), NodeAccessMode::Write) => {
                    panic!("Attempted to acquire a write latch on a node that was borrowed for read. Must use upgradable latches for this use case.")
                }
                _ => {}
            }
        } else {
            self.acquire_unchecked(key, value, access_mode);
        }
    }

    fn acquire_unchecked(
        &mut self,
        key: PageId,
        mut value: MemFrame<MemPage>,
        access_mode: NodeAccessMode,
    ) {
        match access_mode {
            NodeAccessMode::Read => self.latches.insert(key, value.read()),
            NodeAccessMode::Write => self.latches.insert(key, value.write()),
            NodeAccessMode::ReadWrite => self.latches.insert(key, value.upgradable()),
        };
    }

    fn release(&mut self, key: PageId) {
        self.latches.remove(&key);
    }

    fn visit(&mut self, key: Position) {
        self.traversal.push(key);
    }

    fn get(&self, key: PageId) -> Option<&Latch<MemPage>> {
        self.latches.get(&key)
    }

    fn get_mut(&mut self, key: PageId) -> Option<&mut Latch<MemPage>> {
        self.latches.get_mut(&key)
    }

    fn pop(&mut self) -> Option<Position> {
        self.traversal.pop()
    }

    fn last(&self) -> Option<&Position> {
        self.traversal.last()
    }

    fn clear(&mut self) {
        self.latches.clear();
        self.traversal.clear();
    }
}

#[repr(u8)]
pub(crate) enum BalancingMode {
    Closest,
    All,
}
impl<K> BPlusTree<K>
where
    K: Ord + std::fmt::Display + for<'a> TryFrom<&'a [u8], Error = std::io::Error> + AsRef<[u8]>,
{
    pub(crate) fn new(pager: SharedPager, min_keys: usize, num_siblings_per_side: usize) -> Self {
        let root = pager
            .write()
            .alloc_page::<BtreePage, BtreePageHeader>()
            .unwrap();

        Self {
            shared_pager: pager,
            root,
            min_keys,
            num_siblings_per_side,
            __comparator: std::marker::PhantomData,
        }
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

    pub fn clear_stack(&self) {
        LATCH_STACK.try_with(|l| l.borrow_mut().clear()).unwrap();
    }

    fn release_latch(&self, page_id: PageId) {
        LATCH_STACK
            .try_with(|l| l.borrow_mut().release(page_id))
            .unwrap();
    }

    fn get_last_visited(&self) -> Option<Position> {
        LATCH_STACK
            .try_with(|l| l.try_borrow().ok().and_then(|stack| stack.last().copied()))
            .unwrap_or(None)
    }

    fn pop_last_visited(&self) -> Option<Position> {
        LATCH_STACK
            .try_with(|l| l.try_borrow_mut().ok().and_then(|mut stack| stack.pop()))
            .unwrap_or(None)
    }

    pub fn get_content_from_result(&self, result: SearchResult) -> Option<Vec<u8>> {
        match result {
            SearchResult::NotFound(_) => None,
            SearchResult::Found((page_id, slot)) => {
                let cell = self
                    .with_latched_page(page_id, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        Ok(node.cell(slot).clone())
                    })
                    .unwrap();

                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(&cell).unwrap()
                } else {
                    Payload::Reference(cell.used())
                };
                Some(content.as_ref().to_vec())
            }
        }
    }

    fn visit_node(&self, position: Position) -> std::io::Result<()> {
        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
                        stack.visit(position);
                    })
                    .is_err()
                {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Unable to access latch stack mutably",
                    );
                };
            })
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Unable to access latch stack mutably",
                )
            })?;
        Ok(())
    }

    fn acquire_latch(&self, page_id: PageId, access_mode: NodeAccessMode) -> std::io::Result<()> {
        let latch_page = self.shared_pager.write().read_page::<BtreePage>(page_id)?;

        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
                        stack.acquire(page_id, latch_page, access_mode);
                    })
                    .is_err()
                {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Unable to access latch stack mutably",
                    );
                };
            })
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Unable to access latch stack mutably",
                )
            })?;
        Ok(())
    }

    fn with_latched_page<T>(
        &self,
        page_id: PageId,
        f: impl FnOnce(&Latch<MemPage>) -> Result<T, std::io::Error>,
    ) -> std::io::Result<T> {
        LATCH_STACK
            .try_with(|l| -> Result<T, std::io::Error> {
                let stack = l.try_borrow().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Unable to access latch stack inmutably",
                    )
                })?;

                let page = stack.get(page_id).ok_or(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Attempted to access an unacquired latch!",
                ))?;

                debug_assert!(
                    page.page_number() == page_id,
                    "Memory corruption. Asked for page {}, but got {}",
                    page_id,
                    page.page_number()
                );

                f(page)
            })
            .map_err(|_| std::io::Error::other("Latch stack is unavailable"))?
    }

    fn with_latched_page_mut<T>(
        &self,
        page_id: PageId,
        f: impl FnOnce(&mut Latch<MemPage>) -> Result<T, std::io::Error>,
    ) -> std::io::Result<T> {
        LATCH_STACK
            .try_with(|l| -> Result<T, std::io::Error> {
                let mut stack = l.try_borrow_mut().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Unable to access latch stack inmutably",
                    )
                })?;

                let page = stack.get_mut(page_id).ok_or(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Attempted to access an unacquired latch!",
                ))?;

                // Running into bugs here.
                debug_assert!(
                    page.page_number() == page_id,
                    "Memory corruption. Asked for page {}, but got {}",
                    page_id,
                    page.page_number()
                );

                f(page)
            })
            .map_err(|_| std::io::Error::other("Latch stack is unavailable"))?
    }

    pub fn search(
        &mut self,
        start: &Position,
        entry: &K,
        access_mode: NodeAccessMode,
    ) -> std::io::Result<SearchResult> {
        self.acquire_latch(start.0, access_mode)?;

        // As we are reading, we can safely release the latch on the parent.
        if let Some(parent_node) = self.get_last_visited() {
            if matches!(access_mode, NodeAccessMode::Read) {
                self.release_latch(parent_node.0);
            };
        };

        let is_leaf = self.with_latched_page(start.0, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();
            Ok(btreepage.is_leaf())
        })?;

        if is_leaf {
            return self.binary_search_key(start.0, entry);
        };

        // We are on an interior node, therefore we have to visit it in order to keep track of ot in the traversal stack.
        let child = self.find_child(start.0, entry)?;
        match child {
            SearchResult::NotFound((last_page, last_slot)) => {
                self.visit_node((start.0, last_slot))?;
                self.search(&(last_page, last_slot), entry, access_mode)
            }
            SearchResult::Found((child, slot)) => {
                self.visit_node((start.0, slot))?;
                self.search(&(child, slot), entry, access_mode)
            }
        }
    }

    fn find_child(&self, page_id: PageId, search_key: &K) -> std::io::Result<SearchResult> {
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();

            let mut result = SearchResult::NotFound((
                btreepage.metadata().right_child,
                btreepage.max_slot_index(),
            ));

            for (i, cell) in btreepage.iter_cells().enumerate() {
                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(cell)?
                } else {
                    Payload::Reference(cell.used())
                };

                let payload_key = K::try_from(content.as_ref())?;
                if search_key.cmp(&payload_key) == Ordering::Less {
                    result = SearchResult::Found((cell.metadata().left_child(), Slot(i as u16)));
                    break;
                };
            }

            Ok(result)
        })
    }

    fn find_slot(&self, page_id: PageId, search_key: &K) -> std::io::Result<SlotSearchResult> {
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();

            if btreepage.num_slots() == 0 {
                return Ok(SlotSearchResult::NotFound);
            };

            let mut result = SlotSearchResult::NotFound;
            for (i, cell) in btreepage.iter_cells().enumerate() {
                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(cell)?
                } else {
                    Payload::Reference(cell.used())
                };

                let payload_key = K::try_from(content.as_ref())?;
                match &search_key.cmp(&payload_key) {
                    Ordering::Less => {
                        result = SlotSearchResult::FoundBetween((page_id, Slot(i as u16)));
                        break;
                    }
                    Ordering::Equal => {
                        result = SlotSearchResult::FoundInplace((page_id, Slot(i as u16)));
                        break;
                    }
                    _ => {}
                }
            }

            Ok(result)
        })
    }

    fn reassemble_payload(&self, cell: &Cell) -> std::io::Result<Payload> {
        let mut overflow_page = cell.overflow_page();
        let mut payload = Vec::from(&cell.used()[..cell.len() - std::mem::size_of::<PageId>()]);

        while overflow_page.is_valid() {
            let frame = self
                .shared_pager
                .write()
                .read_page::<OverflowPage>(overflow_page)?;

            frame.try_with_variant::<OverflowPage, _, _, _>(|ovfpage|{
                payload.extend_from_slice(ovfpage.payload());
                            let next = ovfpage.metadata().next;
                            debug_assert_ne!(
                                        next, overflow_page,
                                        "overflow page that points to itself causes infinite loop on reassemble_payload(): {:?}",
                            ovfpage.metadata(),
                            );
                             overflow_page = next;

            }).unwrap();
        }

        Ok(Payload::Boxed(payload.into_boxed_slice()))
    }

    fn binary_search_key(
        &mut self,
        page_id: PageId,
        search_key: &K,
    ) -> std::io::Result<SearchResult> {
        // Now get the btree page.
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();
            let mut slot_count = Slot(btreepage.num_slots());
            let mut left = Slot(0);
            let mut right = slot_count;

            while left < right {
                let mid = left + slot_count / Slot(2);
                let cell = btreepage.cell(mid);

                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(cell)?
                } else {
                    Payload::Reference(cell.used())
                };

                // The key content is always the first bytes of the cell
                let payload_key = K::try_from(content.as_ref())?;
                match payload_key.cmp(search_key) {
                    Ordering::Equal => return Ok(SearchResult::Found((page_id, mid))),
                    Ordering::Greater => right = mid,
                    Ordering::Less => left = mid + 1usize,
                };

                slot_count = right - left;
            }
            Ok(SearchResult::NotFound((
                page_id,
                btreepage.max_slot_index(),
            )))
        })
    }

    pub fn insert(&mut self, page_id: PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let start_pos = (page_id, Slot(0));
        let search_result = self.search(&start_pos, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("The key {search_key} already exists on page {page_id} at position {slot}",),
            )),

            SearchResult::NotFound((page_id, slot)) => {
                let slot_result = self.find_slot(page_id, &search_key)?;

                let free_space = self.with_latched_page(page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(min(
                        btreepage.max_allowed_payload_size(),
                        btreepage.metadata().free_space as u16,
                    ))
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                match slot_result {
                    SlotSearchResult::NotFound => self.with_latched_page_mut(page_id, |p| {
                        let btreepage: &mut BtreePage = p.try_into().unwrap();
                        let index = btreepage.max_slot_index();
                        btreepage.insert(index, cell);
                        Ok(())
                    }),
                    SlotSearchResult::FoundBetween((_, index)) => {
                        self.with_latched_page_mut(page_id, |p| {
                            let btreepage: &mut BtreePage = p.try_into().unwrap();
                            btreepage.insert(index, cell);
                            Ok(())
                        })
                    }

                    _ => Ok(()),
                }?;

                let status = self.check_node_status(page_id)?;
                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };

                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.clear_stack();

        Ok(())
    }

    pub fn upsert(&mut self, page_id: PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let start_pos = (page_id, Slot(0));
        let search_result = self.search(&start_pos, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => {
                let free_space = self.with_latched_page(page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                let old_cell = self.with_latched_page_mut(page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let index = btreepage.max_slot_index();

                    let cell = btreepage.replace(slot, cell);
                    Ok(cell)
                })?;

                self.free_cell(old_cell)?;

                let status = self.check_node_status(page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };
            }
            SearchResult::NotFound((page_id, slot)) => {
                let slot_result = self.find_slot(page_id, &search_key)?;
                let free_space = self.with_latched_page(page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                match slot_result {
                    SlotSearchResult::NotFound => self.with_latched_page_mut(page_id, |p| {
                        let btreepage: &mut BtreePage = p.try_into().unwrap();
                        let index = btreepage.max_slot_index();

                        btreepage.insert(index, cell);

                        Ok(())
                    }),

                    SlotSearchResult::FoundBetween((_, index)) => {
                        self.with_latched_page_mut(page_id, |p| {
                            let btreepage: &mut BtreePage = p.try_into().unwrap();
                            btreepage.insert(index, cell);

                            Ok(())
                        })
                    }

                    _ => Ok(()),
                }?;

                let status = self.check_node_status(page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };
            }
        };

        // Cleanup traversal here.
        self.clear_stack();
        Ok(())
    }

    pub fn update(&mut self, page_id: PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let start_pos = (page_id, Slot(0));
        let search_result = self.search(&start_pos, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => {
                let free_space = self.with_latched_page(page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                let old_cell = self.with_latched_page_mut(page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let index = btreepage.max_slot_index();

                    let cell = btreepage.replace(slot, cell);
                    Ok(cell)
                })?;

                self.free_cell(old_cell)?;

                let status = self.check_node_status(page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };
            }
            SearchResult::NotFound(_) => {} // No OP if not found.
        };

        // Cleanup traversal here.
        self.clear_stack();
        Ok(())
    }

    /// Removes the entry corresponding to the given key if it exists.
    pub fn remove(&mut self, page_id: PageId, key: &K) -> std::io::Result<()> {
        let start_pos = (page_id, Slot(0));
        let search = self.search(&start_pos, key, NodeAccessMode::Write)?;

        match search {
            SearchResult::NotFound(_) => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found on page. Cannot remove",
            )),
            SearchResult::Found((page_id, slot)) => {
                let cell = self.with_latched_page_mut(page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let cell = btreepage.remove(slot);
                    Ok(cell)
                })?;
                self.free_cell(cell)?;

                let status = self.check_node_status(page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(page_id)?;
                };
                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.clear_stack();
        Ok(())
    }

    fn free_cell(&self, cell: Cell) -> std::io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while overflow_page.is_valid() {
            self.acquire_latch(overflow_page, NodeAccessMode::Read)?;

            let next = self.with_latched_page(overflow_page, |p| {
                let overflow: &OverflowPage = p.try_into().unwrap();
                Ok(overflow.metadata().next)
            })?;

            self.shared_pager
                .write()
                .dealloc_page::<OverflowPage>(overflow_page)?;

            overflow_page = next;
        }

        Ok(())
    }

    fn build_cell(&self, remaining_space: usize, payload: &[u8]) -> std::io::Result<Cell> {
        let page_size = self.shared_pager.read().page_size() as usize;

        let v = remaining_space
            .saturating_sub(SLOT_SIZE);

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
            max_payload_size_in_page
        ); // Clamp to the remaining space

        if payload.len() <= max_payload_size {
            return Ok(Cell::new(payload));
        }



        let mut overflow_page_number = self
            .shared_pager
            .write()
            .alloc_page::<OverflowPage, OverflowPageHeader>()?;

        let cell = Cell::new_overflow(&payload[..max_payload_size], overflow_page_number);

        let mut stored_bytes = max_payload_size;

        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            let mut frame = self
                .shared_pager
                .write()
                .read_page::<OverflowPage>(overflow_page_number)?;

            frame
                .try_with_variant_mut::<OverflowPage, _, _, _>(|overflow_page| {
                    overflow_page.data_mut()[..overflow_bytes]
                        .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().num_bytes = overflow_bytes as _;
                    stored_bytes += overflow_bytes;
                })
                .unwrap();

            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = self
                .shared_pager
                .write()
                .alloc_page::<OverflowPage, OverflowPageHeader>()?;

            frame
                .try_with_variant_mut::<OverflowPage, _, _, _>(|overflow_page| {
                    overflow_page.metadata_mut().next = next_overflow_page;
                })
                .unwrap();

            overflow_page_number = next_overflow_page;
        }
        Ok(cell)
    }

    fn balance_shallower(&mut self) -> std::io::Result<()> {
        let (is_underflow, is_leaf) = self.with_latched_page(self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok((node.is_empty(), node.is_leaf()))
        })?;

        if !is_underflow || is_leaf {
            return Ok(());
        };

        let child_page = self.with_latched_page(self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        let grand_child = self.with_latched_page(child_page, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        let cells = self.with_latched_page_mut(child_page, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            Ok(node.drain(..).collect::<Vec<_>>())
        })?;

        self.release_latch(child_page);
        self.shared_pager
            .write()
            .dealloc_page::<BtreePage>(child_page)?;

        // Refill the old root.
        self.with_latched_page_mut(self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            cells.into_iter().for_each(|cell| node.push(cell));
            node.metadata_mut().right_child = grand_child;
            Ok(())
        })?;
        Ok(())
    }

    fn split_cells<'a>(
        &mut self,
        cells: &'a mut [Cell],
    ) -> std::io::Result<(&'a [Cell], &'a [Cell])> {
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

    fn balance_deeper(&mut self) -> std::io::Result<()> {
        let new_left = self
            .shared_pager
            .write()
            .alloc_page::<BtreePage, BtreePageHeader>()?;
        self.acquire_latch(new_left, NodeAccessMode::Write)?;
        let new_right = self
            .shared_pager
            .write()
            .alloc_page::<BtreePage, BtreePageHeader>()?;
        self.acquire_latch(new_right, NodeAccessMode::Write)?;

        let old_right_child = self.with_latched_page(self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        let was_leaf = !old_right_child.is_valid();
        let page_size = self.shared_pager.read().page_size();

        // TODO: this could be optimized further if we found a way to only take as many cells as necessary instead of taking them all.
        let mut cells = self.with_latched_page_mut(self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().free_space_ptr = page_size - BTREE_PAGE_HEADER_SIZE as u32;
            Ok(node.drain(..).collect::<Vec<_>>())
        })?;

        let (left_cells, right_cells) = self.split_cells(&mut cells)?;

        let propagated_key = if was_leaf {
            K::try_from(right_cells.first().unwrap().used())?
        } else {
            K::try_from(left_cells.last().unwrap().used())?
        };

        let free_space = self.with_latched_page(self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().free_space as usize)
        })?;

        let mut cell = self.build_cell(free_space, propagated_key.as_ref())?;
        cell.metadata_mut().left_child = new_left;

        // Now we move part of the cells to one node and the other part to the other node.
        self.with_latched_page_mut(self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = new_right;
            node.push(cell);
            Ok(())
        })?;

        self.with_latched_page_mut(new_left, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();

            if !was_leaf {
                if let Some((last, rest)) = left_cells.split_last() {
                    rest.iter().for_each(|cell| node.push(cell.clone()));
                    node.metadata_mut().right_child = last.metadata().left_child();
                };
            } else {
                left_cells.iter().for_each(|cell| node.push(cell.clone()));
            };

            node.metadata_mut().next_sibling = new_right;
            Ok(())
        })?;

        self.with_latched_page_mut(new_right, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            right_cells.iter().for_each(|cell| node.push(cell.clone()));
            node.metadata_mut().right_child = old_right_child;
            node.metadata_mut().previous_sibling = new_left;
            Ok(())
        })?;

        Ok(())
    }

    fn balance(&mut self, page_id: PageId) -> std::io::Result<()> {
        let is_root = self.is_root(page_id);

        let is_leaf = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.is_leaf())
        })?;

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

        let parent_position = self.pop_last_visited().ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Parent not found on traversal stack!",
        ))?;

        let parent_page = parent_position.0;

        // Internal/Leaf node Overflow/Underflow.
        self.balance_siblings(page_id, &parent_position)?;

        // No need to fuck anything else up.
        if matches!(self.check_node_status(page_id)?, NodeStatus::Balanced) {
            return Ok(());
        };

        let last_parent_slot = self.with_latched_page(parent_page, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.max_slot_index())
        })?;

        let mut siblings =
            self.load_siblings(page_id, &parent_position, self.num_siblings_per_side)?;

        // Load the frontiers in case it is necessary.
        let last_non_allocated_sibling_slot = siblings.iter().last().unwrap().slot;
        let first_non_allocated_sibling_slot = siblings.front().unwrap().slot;

        let left_frontier = if first_non_allocated_sibling_slot > Slot(0) {
            let child_slot = first_non_allocated_sibling_slot - 1usize;
            self.load_child(parent_page, child_slot)
        } else {
            None
        };

        let right_frontier = if last_non_allocated_sibling_slot < last_parent_slot {
            let child_slot = last_non_allocated_sibling_slot + 1usize;
            self.load_child(parent_page, child_slot)
        } else {
            None
        };

        let page_size = self.shared_pager.read().page_size();
        let mut cells = std::collections::VecDeque::new();
        let slot_to_remove = siblings[0].slot;
        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            self.acquire_latch(sibling.pointer, NodeAccessMode::Write)?;

            let right_child = self.with_latched_page_mut(sibling.pointer, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                cells.extend(node.drain(..));
                node.metadata_mut().free_space_ptr =
                    page_size.saturating_sub(BTREE_PAGE_HEADER_SIZE as u32);
                Ok(node.metadata().right_child)
            })?;

            let next_sibling = if i < siblings.len() - 1 {
                siblings[i + 1].pointer
            } else if let Some(frontier) = right_frontier {
                frontier.pointer
            } else {
                PAGE_ZERO
            };

            if right_child.is_valid() && next_sibling.is_valid() {
                self.acquire_latch(next_sibling, NodeAccessMode::Write)?;

                let first_child_next = self.with_latched_page(next_sibling, |p| {
                    let node: &BtreePage = p.try_into().unwrap();
                    Ok(node.cell(Slot(0)).metadata().left_child())
                })?;

                self.acquire_latch(first_child_next, NodeAccessMode::Write)?;

                let mut first_cell_child = self.with_latched_page(first_child_next, |p| {
                    let node: &BtreePage = p.try_into().unwrap();
                    Ok(node.cell(Slot(0)).clone())
                })?;

                first_cell_child.metadata_mut().left_child = right_child;
                cells.push_back(first_cell_child);
            };

            self.with_latched_page_mut(parent_page, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();

                if slot_to_remove.0 < node.num_slots() {
                    node.remove(slot_to_remove);
                };
                Ok(())
            })?;
        }

        let usable_space = BtreePage::overflow_threshold(page_size as usize) as u16;

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
                while total_size_in_each_page[i]
                    < BtreePage::underflow_threshold(page_size as usize) as u16
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
            if total_size_in_each_page[0]
                < BtreePage::underflow_threshold(page_size as usize) as u16
            {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
            };
        };

        // Take the last right child of the siblings chain.
        // For interior pages this is required in order to unfuck the last pointer.
        let old_right_child =
            self.with_latched_page(siblings.iter().last().unwrap().pointer, |p| {
                let node: &BtreePage = p.try_into().unwrap();
                Ok(node.metadata().right_child)
            })?;

        // Allocate missing pages.
        while siblings.len() < number_of_cells_per_page.len() {
            let parent_index = siblings.iter().last().unwrap().slot + 1usize;
            let new_page = self
                .shared_pager
                .write()
                .alloc_page::<BtreePage, BtreePageHeader>()?;

            self.acquire_latch(new_page, NodeAccessMode::Write)?;
            self.with_latched_page(new_page, |p| {
                let node: &BtreePage = p.try_into().unwrap();
                debug_assert!(
                    node.metadata().num_slots == 0,
                    "Recently allocated page: {new_page} is not empty"
                );

                Ok(())
            })?;
            siblings.push_back(Child::new(new_page, parent_index));
        }

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            let sibling = siblings.pop_back().unwrap();
            let slot = sibling.slot;
            let page = sibling.pointer;

            self.with_latched_page(page, |p| {
                let node: &BtreePage = p.try_into().unwrap();
                debug_assert!(
                    node.metadata().num_slots == 0,
                    "About to deallocated page: {page} is not empty"
                );
                Ok(())
            })?;

            self.release_latch(page);
            self.shared_pager.write().dealloc_page::<BtreePage>(page)?;
        }

        // Put pages in ascending order to favor sequential IO where possible.
        std::collections::BinaryHeap::from_iter(
            siblings.iter().map(|s| std::cmp::Reverse(s.pointer)),
        )
        .iter()
        .enumerate()
        .for_each(|(i, std::cmp::Reverse(page))| siblings[i].pointer = *page);
        // Fix the last child pointer.
        let last_sibling = siblings[siblings.len() - 1];

        self.with_latched_page_mut(last_sibling.pointer, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = old_right_child;
            Ok(())
        })?;

        if right_frontier.is_none() {
            // Fix pointers in the parent in case we have allocated new pages.
            self.with_latched_page_mut(parent_page, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();

                node.metadata_mut().right_child = last_sibling.pointer;

                Ok(())
            })?;
        };

        // Begin redistribution.
        // HAY QUE MODIFICAR ESTE BUCLE PARA CONSIDERAR EL CASO EN QUE SE PUEDE RESTAR 1 CUANDO UN NODO INTERIOR SE SPLITEA, ya que le roba una celdilla a su hermano para ponerla como su right child.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let propagated = self.with_latched_page_mut(siblings[i].pointer, |p| {
                // Push all the cells to the child.
                let node: &mut BtreePage = p.try_into().unwrap();

                let num_cells = if is_leaf {
                    *n
                } else {
                    *n - 1 // For interior nodes we reserve the last cell.
                };

                for _ in 0..num_cells {
                    node.push(cells.pop_front().unwrap());
                }

                if i > 0 {
                    node.metadata_mut().previous_sibling = siblings[i - 1].pointer;
                } else if let Some(frontier) = left_frontier {
                    node.metadata_mut().previous_sibling = frontier.pointer;
                };

                if i < (siblings.len() - 1) {
                    node.metadata_mut().next_sibling = siblings[i + 1].pointer;
                } else if let Some(frontier) = right_frontier {
                    node.metadata_mut().next_sibling = frontier.pointer;
                };

                // Always propagate the first cell of the next page.
                if i < siblings.len() - 1 && is_leaf {
                    let mut divider = cells.front().unwrap().clone();
                    node.metadata_mut().right_child = divider.metadata().left_child;
                    divider.metadata_mut().left_child = siblings[i].pointer;
                    return Ok(Some(divider));
                } else if !is_leaf && (i < siblings.len() - 1 || right_frontier.is_some()) {
                    let mut divider = cells.pop_front().unwrap();
                    node.metadata_mut().right_child = divider.metadata().left_child;
                    divider.metadata_mut().left_child = siblings[i].pointer;
                    return Ok(Some(divider));
                } else if !is_leaf {
                    node.push(cells.pop_front().unwrap());
                };

                Ok(None)
            })?;

            // Fix the pointer in the parent and propagate.
            if let Some(divider) = propagated {
                self.with_latched_page_mut(parent_page, |p| {
                    let node: &mut BtreePage = p.try_into().unwrap();
                    node.insert(siblings[i].slot, divider);
                    Ok(())
                })?;
            };
        }

        // Fix the frontier links.
        let frontier_divider = if let Some(frontier) = right_frontier {
            self.acquire_latch(frontier.pointer, NodeAccessMode::Write)?;
            self.with_latched_page_mut(frontier.pointer, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                node.metadata_mut().previous_sibling = last_sibling.pointer;
                if is_leaf {
                    Ok(Some(node.cell(Slot(0)).clone()))
                } else {
                    Ok(None)
                }
            })?
        } else {
            None
        };

        if let Some(frontier) = left_frontier {
            self.acquire_latch(frontier.pointer, NodeAccessMode::Write)?;
            self.with_latched_page_mut(frontier.pointer, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                node.metadata_mut().next_sibling = siblings[0].pointer;
                Ok(())
            })?;
        };

        if let Some(mut divider) = frontier_divider {
            self.with_latched_page_mut(parent_page, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                divider.metadata_mut().left_child = last_sibling.pointer;
                node.insert(last_sibling.slot, divider);
                Ok(())
            })?;
        };

        // Done, propagate upwards.
        self.balance(parent_page)?;

        Ok(())
    }

    fn load_child(&self, page: PageId, slot: Slot) -> Option<Child> {
        self.with_latched_page(page, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            let last_slot = node.max_slot_index();
            if slot >= Slot(0) && slot <= last_slot {
                Ok(Some(Child::new(node.child(slot), slot)))
            } else {
                Ok(None)
            }
        })
        .unwrap()
    }

    fn load_siblings(
        &mut self,
        page: PageId,
        parent_position: &Position,
        num_siblings_per_side: usize,
    ) -> std::io::Result<VecDeque<Child>> {
        let slot = parent_position.1;
        let parent = parent_position.0;
        let last_child = self.with_latched_page(parent, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.max_slot_index())
        })?;

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
            next += 1;
            added_count += 1;
        }

        added_count = 0;
        next = slot.saturating_sub(1);

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
            next = next.saturating_sub(1);
            added_count += 1;
        }

        Ok(siblings)
    }

    fn check_node_status(&self, page_id: PageId) -> std::io::Result<NodeStatus> {
        let is_root = self.is_root(page_id);

        let (is_underflow, is_overflow) = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok((
                (node.has_underflown() && !is_root) || (is_root && node.is_empty()),
                node.has_overflown(),
            ))
        })?;

        if is_overflow {
            return Ok(NodeStatus::Overflow);
        } else if is_underflow {
            return Ok(NodeStatus::Underflow);
        }
        Ok(NodeStatus::Balanced)
    }

    // This should be only called on leaf nodes. on interior nodes it is a brainfuck to also be fixing pointers to right most children every time we borrow a single cell.
    fn balance_siblings(&mut self, page_id: PageId, parent_pos: &Position) -> std::io::Result<()> {
        let is_leaf = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.is_leaf())
        })?;

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
                if let Some(sibling_left) = siblings.pop_front() {
                    if sibling_left.pointer != page_id {
                        self.acquire_latch(sibling_left.pointer, NodeAccessMode::Write)?;
                        // The load siblings function can return invalid pointers. we need to check for the validity ourselves.
                        let first_cell_size = self.with_latched_page(page_id, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            Ok(node.cell(Slot(0)).storage_size())
                        })?;

                        // Check if we can fit the cell there
                        let has_space = self.with_latched_page(sibling_left.pointer, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            Ok(node.has_space_for(first_cell_size as usize))
                        })?;

                        // Fantastic! our friend has space for us.
                        // Lets push our cell there.
                        if has_space {
                            let removed_cell = self.with_latched_page_mut(page_id, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                Ok(node.remove(Slot(0)))
                            })?;

                            self.with_latched_page_mut(sibling_left.pointer, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                node.push(removed_cell);
                                Ok(())
                            })?;

                            // Haha, but now our parent might have fucked up.
                            // We need to unfuck him.
                            // In order to unfuck the parent, we replace the entry that pointed to left with our new max key
                            let separator_index = sibling_left.slot;
                            self.fix_single_pointer(
                                parent_pos.0,
                                sibling_left.pointer,
                                page_id,
                                separator_index,
                            )?;
                        };
                    };
                };

                let status = self.check_node_status(page_id)?;

                if let Some(sibling_right) = siblings.pop_back() {
                    if sibling_right.pointer != page_id && matches!(status, NodeStatus::Overflow) {
                        self.acquire_latch(sibling_right.pointer, NodeAccessMode::Write)?;
                        let last_cell_size = self.with_latched_page(page_id, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            let last_index = node.max_slot_index() - 1usize;

                            Ok(node.cell(last_index).storage_size())
                        })?;

                        // Check if we can fit the cell there
                        let has_space = self.with_latched_page(sibling_right.pointer, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            Ok(node.has_space_for(last_cell_size as usize))
                        })?;

                        // Fantastic! our friend has space for us.
                        // Lets push our cell there.
                        if has_space {
                            let removed_cell = self.with_latched_page_mut(page_id, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                Ok(node.remove(node.max_slot_index() - 1usize))
                            })?;

                            self.with_latched_page_mut(sibling_right.pointer, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                // It must go to the first slot
                                node.insert(Slot(0), removed_cell);
                                Ok(())
                            })?;

                            // In this case is the same
                            let separator_index = sibling_right.slot - 1usize;
                            self.fix_single_pointer(
                                parent_pos.0,
                                page_id,
                                sibling_right.pointer,
                                separator_index,
                            )?;
                        };
                    };
                };
                Ok(())
            }
            NodeStatus::Underflow => {
                // Okey, we are on underflow state and our right sibling is valid. Then we can ask him if it has cells for us.
                if let Some(sibling_right) = siblings.pop_back() {
                    if sibling_right.pointer != page_id {
                        self.acquire_latch(sibling_right.pointer, NodeAccessMode::Write)?;
                        let can_borrow = self.with_latched_page(sibling_right.pointer, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            let first_cell_size = node.cell(Slot(0)).storage_size() as usize;
                            Ok(node.can_release_space(first_cell_size))
                        })?;

                        // Fantastic! our friend has data for us.
                        // Lets steal the cell.
                        if can_borrow {
                            let removed_cell =
                                self.with_latched_page_mut(sibling_right.pointer, |p| {
                                    let node: &mut BtreePage = p.try_into().unwrap();
                                    Ok(node.remove(Slot(0)))
                                })?;

                            self.with_latched_page_mut(page_id, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                node.push(removed_cell);
                                Ok(())
                            })?;

                            // In this case is the same
                            let separator_index = sibling_right.slot - 1usize;
                            self.fix_single_pointer(
                                parent_pos.0,
                                page_id,
                                sibling_right.pointer,
                                separator_index,
                            )?;
                        };
                    };
                };
                if let Some(sibling_left) = siblings.pop_front() {
                    let status = self.check_node_status(page_id)?;
                    if sibling_left.pointer != page_id && matches!(status, NodeStatus::Overflow) {
                        self.acquire_latch(sibling_left.pointer, NodeAccessMode::Write)?;
                        let can_borrow = self.with_latched_page(sibling_left.pointer, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            let last_cell_size =
                                node.cell(node.max_slot_index()).storage_size() as usize;
                            Ok(node.can_release_space(last_cell_size))
                        })?;

                        // Fantastic! our friend has data for us.
                        // Lets steal the cell.
                        if can_borrow {
                            let removed_cell =
                                self.with_latched_page_mut(sibling_left.pointer, |p| {
                                    let node: &mut BtreePage = p.try_into().unwrap();
                                    Ok(node.remove(node.max_slot_index()))
                                })?;

                            self.with_latched_page_mut(page_id, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                node.insert(Slot(0), removed_cell);
                                Ok(())
                            })?;

                            let separator_index = sibling_left.slot;
                            self.fix_single_pointer(
                                parent_pos.0,
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
    ) -> std::io::Result<()> {
        let mut separator = self.with_latched_page(right_child, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.cell(Slot(0)).clone())
        })?;
        separator.metadata_mut().left_child = left_child;

        self.with_latched_page_mut(parent, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            if node.num_slots() == separator_idx {
                node.metadata_mut().right_child = separator.metadata().left_child();
            } else {
                node.replace(separator_idx, separator);
            };
            Ok(())
        })
    }

    fn read_into_mem(
        &mut self,
        root: PageId,
        buf: &mut Vec<(PageId, BtreePage)>,
    ) -> std::io::Result<()> {
        let root_frame = self.shared_pager.write().read_page::<BtreePage>(root)?;

        let children = root_frame
            .try_with_variant::<BtreePage, _, _, _>(|fr| {
                buf.push((root, fr.clone()));
                fr.iter_children().collect::<Vec<_>>()
            })
            .unwrap();

        for page in children {
            self.read_into_mem(page, buf).unwrap();
        }

        Ok(())
    }

    pub fn json(&mut self) -> std::io::Result<String> {
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

    fn node_json(&mut self, number: PageId, page: &BtreePage) -> std::io::Result<String> {
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
}
