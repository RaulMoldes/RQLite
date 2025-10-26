//mod btree;

//#[cfg(test)]
//mod tests;

use crate::io::frames::MemFrame;
use crate::io::pager::{access_pager, access_pager_mut, SharedPager};
use std::cmp::min;
use std::cmp::Ordering;

use crate::storage::page::{BtreePage, MemPage, OverflowPage};
use crate::storage::{
    cell::{Cell, Slot},
    latches::Latch,
};

use crate::types::{ PageId, SizedType};
use std::cell::RefCell;
use std::collections::{HashMap};
use std::sync::Arc;

type Position = (PageId, Slot);

pub(crate) enum SearchResult {
    Found(Position),  // Key found -> return its position
    NotFound(PageId), // Key not found, return the last visited page id.
}

pub(crate) enum SlotSearchResult {
    FoundInplace(Position),
    FoundBetween(Position),
    NotFound,
}

pub(crate) enum NodeStatus {
    Underflow,
    Overflow,
    Balanced,
}

enum Payload<'b> {
    Boxed(Box<[u8]>),
    Reference(&'b [u8]),
}


#[derive(Copy, Clone, PartialEq, Eq)]
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

#[derive(Debug)]
pub(crate) struct BPlusTree<K>
where
    K: Ord + for<'a> TryFrom<&'a [u8], Error = std::io::Error> + AsMut<[u8]> + AsRef<[u8]>,
{
    pub(crate) root: PageId,
    pub(crate) shared_pager: SharedPager,
    pub(crate) min_keys: usize,
    pub(crate) num_siblings_per_side: usize,
    __comparator: std::marker::PhantomData<K>,
}

struct LatchStackFrame {
    latches: HashMap<PageId, Latch<MemPage>>,
    traversal: Vec<PageId>,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeAccessMode {
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
        value: MemFrame<MemPage>,
        access_mode: NodeAccessMode,
    ) {
        match access_mode {
            NodeAccessMode::Read => self.latches.insert(key, value.read()),
            NodeAccessMode::Write => self.latches.insert(key, value.write()),
            NodeAccessMode::ReadWrite => self.latches.insert(key, value.upgradable()),
        };
    }

    fn release(&mut self, key: &PageId) {
        self.latches.remove(key);
    }

    fn visit(&mut self, key: PageId) {
        self.traversal.push(key);
    }

    fn get(&self, key: &PageId) -> Option<&Latch<MemPage>> {
        self.latches.get(key)
    }

    fn get_mut(&mut self, key: &PageId) -> Option<&mut Latch<MemPage>> {
        self.latches.get_mut(key)
    }

    fn last(&mut self) -> Option<PageId> {
        self.traversal.pop()
    }

    fn clear(&mut self) {
        self.latches.clear();
        self.traversal.clear();
    }
}

impl<K> BPlusTree<K>
where
    K: Ord
        + SizedType
        + std::fmt::Display
        + for<'a> TryFrom<&'a [u8], Error = std::io::Error>
        + AsRef<[u8]>
        + AsMut<[u8]>,
{
    pub(crate) fn new(
        pager: SharedPager,
        root: PageId,
        min_keys: usize,
        num_siblings_per_side: usize,
    ) -> Self {
        Self {
            shared_pager: Arc::clone(&pager),
            root,
            min_keys,
            num_siblings_per_side,
            __comparator: std::marker::PhantomData,
        }
    }

    fn is_root(&self, id: &PageId) -> bool {
        &self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    fn get_last_visited(&self) -> Option<PageId> {
        LATCH_STACK
            .try_with(|l| l.try_borrow_mut().ok().and_then(|mut stack| stack.last()))
            .unwrap_or(None)
    }

    fn visit_node(&self, page_id: &PageId, access_mode: NodeAccessMode) -> std::io::Result<()> {
        let latch_page =
            access_pager_mut(&self.shared_pager, |p| p.read_page::<BtreePage>(page_id))?;

        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
                        stack.acquire(*page_id, latch_page, access_mode);
                        stack.visit(*page_id);
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
        page_id: &PageId,
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

                f(page)
            })
            .map_err(|_| std::io::Error::other("Latch stack is unavailable"))?
    }

    fn with_latched_page_mut<T>(
        &self,
        page_id: &PageId,
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

                f(page)
            })
            .map_err(|_| std::io::Error::other("Latch stack is unavailable"))?
    }

    fn search(
        &self,
        page_id: &PageId,
        entry: &K,
        access_mode: NodeAccessMode,
    ) -> std::io::Result<SearchResult> {
        self.visit_node(page_id, access_mode)?;

        let is_leaf = self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();
            Ok(btreepage.is_leaf())
        })?;

        if is_leaf {
            return self.binary_search_key(page_id, entry);
        };

        let child = self.find_child(page_id, entry)?;

        match child {
            SearchResult::NotFound(last_page) => self.search(&last_page, entry, access_mode),
            SearchResult::Found((last_page, _)) => self.search(&last_page, entry, access_mode),
        }
    }

    fn find_child(&self, page_id: &PageId, search_key: &K) -> std::io::Result<SearchResult> {
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();

            let mut result = SearchResult::NotFound(btreepage.metadata().right_child);

            for cell in btreepage.iter_cells() {
                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(cell)?
                } else {
                    Payload::Reference(cell.used())
                };

                let payload_key = K::try_from(content.as_ref())?;
                match &payload_key.cmp(search_key) {
                    Ordering::Less => {
                        result = SearchResult::NotFound(cell.metadata().left_child());
                        break;
                    }
                    Ordering::Equal => {
                        result = SearchResult::Found((cell.metadata().left_child(), Slot(0)));
                        break;
                    }
                    _ => {}
                }
            }

            Ok(result)
        })
    }

    fn find_slot(&self, page_id: &PageId, search_key: &K) -> std::io::Result<SlotSearchResult> {
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();

            let mut result = SlotSearchResult::NotFound;
            for (i, cell) in btreepage.iter_cells().enumerate() {
                let content: Payload = if cell.metadata().is_overflow() {
                    self.reassemble_payload(cell)?
                } else {
                    Payload::Reference(cell.used())
                };

                let payload_key = K::try_from(content.as_ref())?;
                match &payload_key.cmp(search_key) {
                    Ordering::Less => {
                        result = SlotSearchResult::FoundBetween((*page_id, Slot(i as u16)));
                        break;
                    }
                    Ordering::Equal => {
                        result = SlotSearchResult::FoundInplace((*page_id, Slot(i as u16)));
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
            self.visit_node(&overflow_page, NodeAccessMode::ReadWrite)?;

            let next = self.with_latched_page(&overflow_page, |p2|{
                            let ovfpage: &OverflowPage = p2.try_into().unwrap();
                            payload.extend_from_slice(ovfpage.payload());
                            let next = ovfpage.metadata().next;
                            debug_assert_ne!(
                                        next, overflow_page,
                                        "overflow page that points to itself causes infinite loop on reassemble_payload(): {:?}",
                            ovfpage.metadata(),
                            );

                            Ok(next)
                        })?;

            overflow_page = next;
        }

        Ok(Payload::Boxed(payload.into_boxed_slice()))
    }

    fn binary_search_child(
        &self,
        page_id: &PageId,
        search_child: &PageId,
    ) -> std::io::Result<SearchResult> {
        // Now get the btree page.
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();
            let mut slot_count = Slot(btreepage.num_slots());
            let mut left = Slot(0);
            let mut right = slot_count;

            while left < right {
                let mid = left + slot_count / Slot(2);
                let child = btreepage.cell(mid).metadata().left_child();

                match child.cmp(search_child) {
                    Ordering::Equal => return Ok(SearchResult::Found((*page_id, mid))),
                    Ordering::Greater => right = mid,
                    Ordering::Less => left = mid + 1usize,
                };

                slot_count = right - left;
            }
            Ok(SearchResult::NotFound(*page_id))
        })
    }

    fn binary_search_key(&self, page_id: &PageId, search_key: &K) -> std::io::Result<SearchResult> {
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
                    Ordering::Equal => return Ok(SearchResult::Found((*page_id, mid))),
                    Ordering::Greater => right = mid,
                    Ordering::Less => left = mid + 1usize,
                };

                slot_count = right - left;
            }
            Ok(SearchResult::NotFound(*page_id))
        })
    }

    fn insert(&mut self, page_id: &PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let search_result = self.search(page_id, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("The key {search_key} already exists on page {page_id} at position {slot}",),
            )),

            SearchResult::NotFound(page_id) => {
                let slot_result = self.find_slot(&page_id, &search_key)?;
                let free_space = self.with_latched_page(&page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                match slot_result {
                    SlotSearchResult::NotFound => self.with_latched_page_mut(&page_id, |p| {
                        let btreepage: &mut BtreePage = p.try_into().unwrap();
                        let index = btreepage.max_slot_index();
                        btreepage.insert(index, cell);
                        Ok(())
                    }),
                    SlotSearchResult::FoundBetween((_, index)) => {
                        self.with_latched_page_mut(&page_id, |p| {
                            let btreepage: &mut BtreePage = p.try_into().unwrap();
                            btreepage.insert(index, cell);
                            Ok(())
                        })
                    }

                    _ => unreachable!(),
                }
            }
        }
    }

    fn upsert(&mut self, page_id: &PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let search_result = self.search(page_id, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => {
                let free_space = self.with_latched_page(&page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                let old_cell = self.with_latched_page_mut(&page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let index = btreepage.max_slot_index();

                    let cell = btreepage.replace(slot, cell);
                    Ok(cell)
                })?;

                self.free_cell(old_cell)
            }
            SearchResult::NotFound(page_id) => {
                let slot_result = self.find_slot(&page_id, &search_key)?;
                let free_space = self.with_latched_page(&page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                match slot_result {
                    SlotSearchResult::NotFound => self.with_latched_page_mut(&page_id, |p| {
                        let btreepage: &mut BtreePage = p.try_into().unwrap();
                        let index = btreepage.max_slot_index();

                        btreepage.insert(index, cell);
                        Ok(())
                    }),

                    SlotSearchResult::FoundBetween((_, index)) => {
                        self.with_latched_page_mut(&page_id, |p| {
                            let btreepage: &mut BtreePage = p.try_into().unwrap();
                            btreepage.insert(index, cell);
                            Ok(())
                        })
                    }

                    _ => unreachable!(),
                }
            }
        }
    }

    fn update(&mut self, page_id: &PageId, data: &[u8]) -> std::io::Result<()> {
        let search_key = K::try_from(data)?;
        let search_result = self.search(page_id, &search_key, NodeAccessMode::Write)?;

        match search_result {
            SearchResult::Found((page_id, slot)) => {
                let free_space = self.with_latched_page(&page_id, |p| {
                    let btreepage: &BtreePage = p.try_into().unwrap();
                    Ok(btreepage.max_allowed_payload_size())
                })? as usize;

                let cell = self.build_cell(free_space, data)?;

                let old_cell = self.with_latched_page_mut(&page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let index = btreepage.max_slot_index();

                    let cell = btreepage.replace(slot, cell);
                    Ok(cell)
                })?;

                self.free_cell(old_cell)
            }
            SearchResult::NotFound(page_id) => Ok(()), // No OP if not found.
        }
    }

    /// Removes the entry corresponding to the given key if it exists.
    pub fn remove(&mut self, page_id: &PageId, key: &K) -> std::io::Result<()> {
        let search = self.search(page_id, key, NodeAccessMode::Write)?;

        match search {
            SearchResult::NotFound(_) => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found on page. Cannot remove",
            )),
            SearchResult::Found((page_id, slot)) => {
                let cell = self.with_latched_page_mut(&page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let cell = btreepage.remove(slot);
                    Ok(cell)
                })?;

                self.free_cell(cell)
            }
        }
    }

    fn free_cell(&self, cell: Cell) -> std::io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while overflow_page.is_valid() {
            self.visit_node(&overflow_page, NodeAccessMode::Read)?;

            let next = self.with_latched_page(&overflow_page, |p| {
                let overflow: &OverflowPage = p.try_into().unwrap();
                Ok(overflow.metadata().next)
            })?;

            access_pager_mut(&self.shared_pager, |s| {
                s.dealloc_page::<OverflowPage>(overflow_page)
            })?;

            overflow_page = next;
        }

        Ok(())
    }

    fn build_cell(&self, remaining_space: usize, payload: &[u8]) -> std::io::Result<Cell> {
        let page_size = access_pager(&self.shared_pager, |pager| Ok(pager.page_size() as usize))?;

        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.min_keys),
            remaining_space,
        ); // Clamp to the remaining space

        if payload.len() <= max_payload_size {
            return Ok(Cell::new(payload));
        }

        // Store payload in chunks, link overflow pages and return the cell.
        let first_cell_payload_size = max_payload_size - std::mem::size_of::<PageId>();
        let mut overflow_page_number = access_pager_mut(&self.shared_pager, |pager| {
            pager.alloc_page::<OverflowPage>()
        })?;

        let cell = Cell::new_overflow(&payload[..first_cell_payload_size], overflow_page_number);

        let mut stored_bytes = first_cell_payload_size;

        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(page_size as usize) as usize,
                payload[stored_bytes..].len(),
            );

            self.visit_node(&overflow_page_number, NodeAccessMode::Write)?;

            self.with_latched_page_mut(&overflow_page_number, |p| {
                let overflow_page: &mut OverflowPage = p.try_into().unwrap();
                overflow_page.data_mut()[..overflow_bytes]
                    .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                overflow_page.metadata_mut().num_bytes = overflow_bytes as _;
                stored_bytes += overflow_bytes;
                Ok(())
            })?;

            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = access_pager_mut(&self.shared_pager, |pager| {
                pager.alloc_page::<OverflowPage>()
            })?;

            self.with_latched_page_mut(&overflow_page_number, |p| {
                let overflow_page: &mut OverflowPage = p.try_into().unwrap();
                overflow_page.metadata_mut().next = next_overflow_page;
                Ok(())
            })?;

            overflow_page_number = next_overflow_page;
        }

        Ok(cell)
    }

    fn balance_shallower(&mut self) -> std::io::Result<()> {
        let (is_underflow, is_leaf) = self.with_latched_page(&self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok((node.is_empty(), node.is_leaf()))
        })?;

        if !is_underflow || is_leaf {
            return Ok(());
        };

        let child_page = self.with_latched_page(&self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        let grand_child = self.with_latched_page(&child_page, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        let cells = self.with_latched_page_mut(&child_page, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            Ok(node.drain(..).collect::<Vec<_>>())
        })?;

        access_pager_mut(&self.shared_pager, |pager| {
            pager.dealloc_page::<BtreePage>(child_page)
        })?;

        // Refill the old root.
        self.with_latched_page_mut(&self.root, |p| {
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
        let new_left = access_pager_mut(&self.shared_pager, |p| p.alloc_page::<BtreePage>())?;
        let new_right = access_pager_mut(&self.shared_pager, |p| p.alloc_page::<BtreePage>())?;

        // TODO: this could be optimized further if we found a way to only take as many cells as necessary instead of taking them all.
        let mut cells = self.with_latched_page_mut(&self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            Ok(node.drain(..).collect::<Vec<_>>())
        })?;

        let (left_cells, right_cells) = self.split_cells(&mut cells)?;
        let propagated_key = K::try_from(right_cells.first().unwrap().used())?;

        let free_space = self.with_latched_page(&self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().free_space as usize)
        })?;

        let cell = self.build_cell(free_space, propagated_key.as_ref())?;

        // Now we move part of the cells to one node and the other part to the other node.
        self.with_latched_page_mut(&self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = new_right;
            node.push(cell);
            Ok(())
        })?;

        self.with_latched_page_mut(&new_left, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            left_cells
                .iter()
                .for_each(|cell| node.push(cell.clone()));
            node.metadata_mut().next_sibling = new_right;
            Ok(())
        })?;

        self.with_latched_page_mut(&new_right, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            right_cells
                .iter()
                .for_each(|cell| node.push(cell.clone()));
            node.metadata_mut().previous_sibling = new_left;
            Ok(())
        })?;

        Ok(())
    }

    fn balance(&mut self, page_id: &PageId) -> std::io::Result<()> {
        let is_root = self.is_root(page_id);

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

        let parent_page = self.get_last_visited().ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Parent not found on traversal stack!",
        ))?;

        // Internal/Leaf node Overflow/Underflow.
        let needs_unfuck = self.balance_siblings(page_id, &parent_page)?;

        // No need to fuck anything else up.
        if matches!(self.check_node_status(page_id)?, NodeStatus::Balanced) {
            if needs_unfuck {
                self.unfuck_pointers(&parent_page)?;
            }
            return Ok(());
        };

        let mut siblings = self.load_siblings(page_id, &parent_page, self.num_siblings_per_side)?;

        let mut cells = std::collections::VecDeque::new();
        let divider_idx = siblings[0].slot;

        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            self.with_latched_page_mut(&sibling.pointer, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                cells.extend(node.drain(..));
                Ok(())
            })?;

            if i < siblings.len() - 1 {
                let right_child = self.with_latched_page(&sibling.pointer, |p| {
                    let node: &BtreePage = p.try_into().unwrap();
                    Ok(node.metadata().right_child)
                })?;

                self.with_latched_page_mut(&parent_page, |p| {
                    let node: &mut BtreePage = p.try_into().unwrap();
                    let mut divider = node.remove(divider_idx);
                    divider.metadata_mut().left_child = right_child;
                    cells.push_back(divider);
                    Ok(())
                })?;
            }
        }

        let usable_space = access_pager(&self.shared_pager, |sp| {
            Ok(BtreePage::usable_space(sp.page_size() as usize))
        })? as u16;

        let mut total_size_in_each_page = vec![0];
        let mut number_of_cells_per_page = vec![0];

        // Precompute left biased distribution.
        for cell in &cells {
            let i = number_of_cells_per_page.len() - 1;
            if total_size_in_each_page[i] + cell.storage_size() <= usable_space {
                number_of_cells_per_page[i] += 1;
                total_size_in_each_page[i] += cell.storage_size();
            } else {
                number_of_cells_per_page.push(0);
                total_size_in_each_page.push(0);
            }
        }

        // Account for underflow towards the right.
        if number_of_cells_per_page.len() >= 2 {
            let mut divider_cell = cells.len() - number_of_cells_per_page.last().unwrap() - 1;

            for i in (1..=(total_size_in_each_page.len() - 1)).rev() {
                while total_size_in_each_page[i] < usable_space / 2 {
                    number_of_cells_per_page[i] += 1;
                    total_size_in_each_page[i] += &cells[divider_cell].storage_size();

                    number_of_cells_per_page[i - 1] -= 1;
                    total_size_in_each_page[i - 1] -= &cells[divider_cell - 1].storage_size();
                    divider_cell -= 1;
                }
            }

            // Second page has more data than the first one, make a little
            // adjustment to keep it left biased.
            if total_size_in_each_page[0] < usable_space / 2 {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
            }
        }

        let old_right_child = self.with_latched_page(&siblings.last().unwrap().pointer, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        // Allocate missing pages.
        while siblings.len() < number_of_cells_per_page.len() {
            let new_page =
                access_pager_mut(&self.shared_pager, |pager| pager.alloc_page::<BtreePage>())?;

            let parent_index = siblings.last().unwrap().slot + 1usize;
            siblings.push(Child::new(new_page, parent_index));
        }

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            access_pager_mut(&self.shared_pager, |pager| {
                pager.dealloc_page::<BtreePage>(siblings.pop().unwrap().pointer)
            })?;
        }

        // Put pages in ascending order to favor sequential IO where possible.
        std::collections::BinaryHeap::from_iter(
            siblings.iter().map(|s| std::cmp::Reverse(s.pointer)),
        )
        .iter()
        .enumerate()
        .for_each(|(i, std::cmp::Reverse(page))| siblings[i].pointer = *page);

        // Fix children pointers.
        let last_sibling = siblings[siblings.len() - 1];

        self.with_latched_page_mut(&last_sibling.pointer, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = old_right_child;
            Ok(())
        })?;

        self.with_latched_page_mut(&parent_page, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            if divider_idx == node.num_slots() {
                node.metadata_mut().right_child = last_sibling.pointer;
            } else {
                node.cell_mut(divider_idx).metadata_mut().left_child = last_sibling.pointer;
            };
            Ok(())
        })?;

        // Begin redistribution.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let propagated = self.with_latched_page_mut(&siblings[i].pointer, |p| {
                let node: &mut BtreePage = p.try_into().unwrap();
                for _ in 0..*n {
                    node.push(cells.pop_front().unwrap());
                }

                if i < siblings.len() - 1 {
                    let mut divider = cells.front().unwrap().clone();
                    node.metadata_mut().right_child = divider.metadata().left_child;
                    divider.metadata_mut().left_child = siblings[i].pointer;
                    return Ok(Some(divider));
                }

                Ok(None)
            })?;

            if let Some(divider) = propagated {
                self.with_latched_page_mut(&parent_page, |p| {
                    let node: &mut BtreePage = p.try_into().unwrap();
                    node.insert(siblings[i].slot, divider);

                    Ok(())
                })?;
            };
        }
        // Done, propagate upwards.
        self.balance(&parent_page)?;

        Ok(())
    }

    fn load_child(&self, page: &PageId, slot: Slot) -> Option<Child> {
        self.with_latched_page(page, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            let last_slot = node.max_slot_index();
            if slot >= Slot(0) && slot <= last_slot {
                Ok(Some(Child::new(
                    node.cell(slot).metadata().left_child(),
                    slot,
                )))
            } else {
                Ok(None)
            }
        })
        .unwrap()
    }

    fn load_siblings(
        &mut self,
        page: &PageId,
        parent: &PageId,
        mut num_siblings_per_side: usize,
    ) -> std::io::Result<Vec<Child>> {
        let search_result = self.binary_search_child(parent, page)?;

        let last_child = self.with_latched_page(parent, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.max_slot_index())
        })?;

        match search_result {
            SearchResult::Found((_, slot)) => {
                if slot == 0 || slot == last_child {
                    num_siblings_per_side *= 2;
                };

                let left = (1..=num_siblings_per_side)
                    .map(|i| slot.saturating_sub(i as u16))
                    .take_while(|&s| s > Slot(0))
                    .filter_map(|slot_child| self.load_child(page, slot_child));

                let center = std::iter::once(Child::new(*page, slot));

                let right = (1..=num_siblings_per_side)
                    .map(|i| slot.clamped_add(i as u16, last_child.into()))
                    .take_while(|&slot_child| slot_child <= last_child)
                    .filter_map(|slot_child| self.load_child(page, slot_child));

                let siblings = left.chain(center).chain(right).collect();
                Ok(siblings)
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Node not found in parent!",
            )),
        }
    }

    fn check_node_status(&self, page_id: &PageId) -> std::io::Result<NodeStatus> {
        let (is_underflow, is_overflow) = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok((node.has_underflown(), node.has_overflown()))
        })?;

        if is_overflow {
            return Ok(NodeStatus::Overflow);
        } else if is_underflow {
            return Ok(NodeStatus::Underflow);
        }
        Ok(NodeStatus::Balanced)
    }

    // This should be only called on leaf nodes. on interior nodes it is a brainfuck to also be fixing pointers to right most children every time we borrow a single cell.
    fn balance_siblings(&mut self, page_id: &PageId, parent: &PageId) -> std::io::Result<bool> {
        let is_leaf = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.is_leaf())
        })?;

        if !is_leaf {
            return Ok(false);
        };

        let status = self.check_node_status(page_id)?;

        let siblings = self.load_siblings(page_id, parent, 1)?; // One sibling per side

        if siblings.is_empty() || siblings.len() == 1 {
            return Ok(false);
        };

        let sibling_left = siblings.first().unwrap();
        let sibling_right = siblings.last().unwrap();

        match status {
            NodeStatus::Balanced => Ok(false),
            NodeStatus::Overflow => {
                let mut needs_unfuck = false;

                // Okey, we are on overflow state and our left sibling is valid. Then we can ask him if it has space for us.
                if sibling_left.pointer != *page_id {
                    // The load siblings function can return invalid pointers. we need to check for the validity ourselves.
                    let first_cell_size = self.with_latched_page(page_id, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        Ok(node.cell(Slot(0)).storage_size())
                    })?;

                    // Check if we can fit the cell there
                    let has_space = self.with_latched_page(&sibling_left.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let capacity = node
                            .capacity()
                            .saturating_mul(2)
                            .div_ceil(3)
                            .saturating_sub(node.metadata().free_space as usize);
                        Ok(first_cell_size as usize <= capacity)
                    })?;

                    // Fantastic! our friend has space for us.
                    // Lets push our cell there.
                    if has_space {
                        let removed_cell = self.with_latched_page_mut(page_id, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            Ok(node.remove(Slot(0)))
                        })?;

                        self.with_latched_page_mut(&sibling_left.pointer, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            node.push(removed_cell);
                            Ok(())
                        })?;

                        needs_unfuck = true
                    };
                    // Haha, but now our parent might have fucked up.
                    // We need to unfuck him.
                };

                let status = self.check_node_status(page_id)?;
                if sibling_right.pointer != *page_id && matches!(status, NodeStatus::Overflow) {
                    let last_cell_size = self.with_latched_page(page_id, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let last_index = node.max_slot_index();
                        Ok(node.cell(last_index).storage_size())
                    })?;

                    // Check if we can fit the cell there
                    let has_space = self.with_latched_page(&sibling_right.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let capacity = node
                            .capacity()
                            .saturating_mul(2)
                            .div_ceil(3)
                            .saturating_sub(node.metadata().free_space as usize);

                        Ok(last_cell_size as usize <= capacity)
                    })?;

                    // Fantastic! our friend has space for us.
                    // Lets push our cell there.
                    if has_space {
                        let removed_cell = self.with_latched_page_mut(page_id, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            Ok(node.remove(node.max_slot_index()))
                        })?;

                        self.with_latched_page_mut(&sibling_right.pointer, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            // It must go to the first slot
                            node.insert(Slot(0), removed_cell);
                            Ok(())
                        })?;

                        needs_unfuck = true;
                    };
                };

                Ok(needs_unfuck)
            }
            NodeStatus::Underflow => {
                let mut needs_unfuck = false;

                // Okey, we are on underflow state and our right sibling is valid. Then we can ask him if it has cells for us.
                if sibling_right.pointer != *page_id {
                    let can_borrow = self.with_latched_page(&sibling_right.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let first_cell_size = node.cell(Slot(0)).storage_size() as usize;
                        Ok((node.metadata().free_space as usize + first_cell_size)
                            >= node.capacity().saturating_mul(1).div_ceil(3))
                    })?;

                    // Fantastic! our friend has data for us.
                    // Lets steal the cell.
                    if can_borrow {
                        let removed_cell =
                            self.with_latched_page_mut(&sibling_right.pointer, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                Ok(node.remove(Slot(0)))
                            })?;

                        self.with_latched_page_mut(page_id, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            node.push(removed_cell);
                            Ok(())
                        })?;

                        needs_unfuck = true;
                    };
                };

                let status = self.check_node_status(page_id)?;
                if sibling_left.pointer != *page_id && matches!(status, NodeStatus::Overflow) {
                    let can_borrow = self.with_latched_page(&sibling_left.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let last_cell_size =
                            node.cell(node.max_slot_index()).storage_size() as usize;
                        Ok((node.metadata().free_space as usize + last_cell_size)
                            >= node.capacity().saturating_mul(1).div_ceil(3))
                    })?;

                    // Fantastic! our friend has data for us.
                    // Lets steal the cell.
                    if can_borrow {
                        let removed_cell =
                            self.with_latched_page_mut(&sibling_left.pointer, |p| {
                                let node: &mut BtreePage = p.try_into().unwrap();
                                Ok(node.remove(node.max_slot_index()))
                            })?;

                        self.with_latched_page_mut(page_id, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            node.insert(Slot(0), removed_cell);
                            Ok(())
                        })?;

                        needs_unfuck = true;
                    };
                };

                Ok(needs_unfuck)
            }
        }

        // This algorithm is more efficient than reasseambling everyting in the tree. However, it has the disadvantage that it can fuck up the pointers in the parent nodes.
        // We are responsible from unfucking them.
    }

    fn unfuck_pointers(&self, page_id: &PageId) -> std::io::Result<()> {
        let children = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            let children: Vec<PageId> = node.iter_children().collect();
            Ok(children)
        })?;

        if children.is_empty() {
            return Ok(()); // SAFETY GUARD in case we call this function on a leaf node
        };

        let mut new_pointers: Vec<(K, PageId, Slot)> = Vec::with_capacity(children.len());
        for (i, child) in children.iter().enumerate() {
            let new_key = self.with_latched_page(child, |p| {
                let node: &BtreePage = p.try_into().unwrap();
                K::try_from(node.cell(node.max_slot_index()).used())
            })?;

            new_pointers.push((new_key, *child, Slot(i as u16)));
        }

        // Now we can unfuck this node.
        self.with_latched_page_mut(page_id, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();

            for (key, pointer, slot) in new_pointers {
                let old_cell_size = node.cell(slot).storage_size() as u32;
                let remaining_space = node.metadata().free_space + old_cell_size;
                let mut new_cell = self.build_cell(remaining_space as usize, key.as_ref())?;
                new_cell.metadata_mut().left_child = pointer;
                node.replace(slot, new_cell);
            }
            Ok(())
        })?;
        Ok(())
    }
}
