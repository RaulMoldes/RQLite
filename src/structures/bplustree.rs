//mod btree;

//#[cfg(test)]
//mod tests;

use crate::io::frames::MemFrame;
use crate::io::pager::SharedPager;
use crate::storage::page::{BtreePage, MemPage, OverflowPage};
use crate::storage::{
    cell::{Cell, Slot},
    latches::Latch,
};
use std::cmp::min;
use std::cmp::Ordering;

use crate::types::PageId;
use std::cell::RefCell;
use std::collections::HashMap;

type Position = (PageId, Slot);

#[derive(Debug)]
pub(crate) enum SearchResult {
    Found(Position),  // Key found -> return its position
    NotFound(PageId), // Key not found, return the last visited page id.
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
        mut value: MemFrame<MemPage>,
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

    fn pop(&mut self) -> Option<PageId> {
        self.traversal.pop()
    }

    fn last(&self) -> Option<&PageId> {
        self.traversal.last()
    }

    fn clear(&mut self) {
        self.latches.clear();
        self.traversal.clear();
    }
}

impl<K> BPlusTree<K>
where
    K: Ord + std::fmt::Display + for<'a> TryFrom<&'a [u8], Error = std::io::Error> + AsRef<[u8]>,
{
    pub(crate) fn new(pager: SharedPager, min_keys: usize, num_siblings_per_side: usize) -> Self {
        let root = pager.write().alloc_page::<BtreePage>().unwrap();

        Self {
            shared_pager: pager,
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

    fn get_root(&self) -> PageId {
        self.root
    }

    fn clear_stack(&self) {
        LATCH_STACK.try_with(|l| l.borrow_mut().clear()).unwrap();
    }

    fn release_latch(&self, page_id: &PageId) {
        LATCH_STACK
            .try_with(|l| l.borrow_mut().release(page_id))
            .unwrap();
    }

    fn get_last_visited(&self) -> Option<PageId> {
        LATCH_STACK
            .try_with(|l| l.try_borrow().ok().and_then(|stack| stack.last().copied()))
            .unwrap_or(None)
    }

    fn pop_last_visited(&self) -> Option<PageId> {
        LATCH_STACK
            .try_with(|l| l.try_borrow_mut().ok().and_then(|mut stack| stack.pop()))
            .unwrap_or(None)
    }

    fn get_cell_from_result(&self, result: SearchResult) -> Option<Cell> {
        match result {
            SearchResult::NotFound(_) => None,
            SearchResult::Found((page_id, slot)) => self
                .with_latched_page(&page_id, |p| {
                    let node: &BtreePage = p.try_into().unwrap();
                    Ok(node.cell(slot).clone())
                })
                .ok(),
        }
    }

    fn visit_node(&self, page_id: &PageId) -> std::io::Result<()> {
        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
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

    fn acquire_latch(&self, page_id: &PageId, access_mode: NodeAccessMode) -> std::io::Result<()> {
        let latch_page = self.shared_pager.write().read_page::<BtreePage>(page_id)?;

        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
                        stack.acquire(*page_id, latch_page, access_mode);
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
        &mut self,
        page_id: &PageId,
        entry: &K,
        access_mode: NodeAccessMode,
    ) -> std::io::Result<SearchResult> {
        self.acquire_latch(page_id, access_mode)?;

        // As we are reading, we can safely release the latch on the parent.
        if let Some(parent_node) = self.get_last_visited() {
            if matches!(access_mode, NodeAccessMode::Read) {
                self.release_latch(&parent_node);
            };
        };

        let is_leaf = self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();
            Ok(btreepage.is_leaf())
        })?;

        if is_leaf {
            return self.binary_search_key(page_id, entry);
        };

        // We are on an interior node, therefore we have to visit it in order to keep track of ot in the traversal stack.
        self.visit_node(page_id)?;
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
                if search_key.cmp(&payload_key) == Ordering::Less {
                    result = SearchResult::Found((cell.metadata().left_child(), Slot(0)));
                    break;
                };
            }

            Ok(result)
        })
    }

    fn find_slot(&self, page_id: &PageId, search_key: &K) -> std::io::Result<SlotSearchResult> {
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
            let frame = self
                .shared_pager
                .write()
                .read_page::<OverflowPage>(&overflow_page)?;

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

    fn binary_search_child(
        &self,
        page_id: &PageId,
        search_child: &PageId,
    ) -> std::io::Result<SearchResult> {
        // Now get the btree page.
        self.with_latched_page(page_id, |p| {
            let btreepage: &BtreePage = p.try_into().unwrap();

            if btreepage.metadata().right_child == *search_child {
                return Ok(SearchResult::Found((*page_id, btreepage.max_slot_index())));
            };

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

    fn binary_search_key(
        &mut self,
        page_id: &PageId,
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
                    Ok(min(
                        btreepage.max_allowed_payload_size(),
                        btreepage.metadata().free_space as u16,
                    ))
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

                    _ => Ok(()),
                }?;

                let status = self.check_node_status(&page_id)?;
                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(&page_id)?;
                };

                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.clear_stack();

        Ok(())
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

                self.free_cell(old_cell)?;

                let status = self.check_node_status(&page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(&page_id)?;
                };
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

                    _ => Ok(()),
                }?;

                let status = self.check_node_status(&page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(&page_id)?;
                };
            }
        };

        // Cleanup traversal here.
        self.clear_stack();
        Ok(())
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

                self.free_cell(old_cell)?;

                let status = self.check_node_status(&page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(&page_id)?;
                };
            }
            SearchResult::NotFound(page_id) => {} // No OP if not found.
        };

        // Cleanup traversal here.
        self.clear_stack();
        Ok(())
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
                self.free_cell(cell)?;

                let status = self.check_node_status(&page_id)?;

                if !matches!(status, NodeStatus::Balanced) {
                    self.balance(&page_id)?;
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
            self.acquire_latch(&overflow_page, NodeAccessMode::Read)?;

            let next = self.with_latched_page(&overflow_page, |p| {
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

        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.min_keys),
            remaining_space,
        ); // Clamp to the remaining space

        if payload.len() <= max_payload_size {
            return Ok(Cell::new(payload));
        }

        // Store payload in chunks, link overflow pages and return the cell.
        let first_cell_payload_size = max_payload_size - std::mem::size_of::<PageId>();
        let mut overflow_page_number = self.shared_pager.write().alloc_page::<OverflowPage>()?;

        let cell = Cell::new_overflow(&payload[..first_cell_payload_size], overflow_page_number);

        let mut stored_bytes = first_cell_payload_size;

        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            if stored_bytes >= payload.len() {
                break;
            }

            let mut frame = self
                .shared_pager
                .write()
                .read_page::<OverflowPage>(&overflow_page_number)?;

            frame
                .try_with_variant_mut::<OverflowPage, _, _, _>(|overflow_page| {
                    overflow_page.data_mut()[..overflow_bytes]
                        .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().num_bytes = overflow_bytes as _;
                    stored_bytes += overflow_bytes;
                })
                .unwrap();

            let next_overflow_page = self.shared_pager.write().alloc_page::<OverflowPage>()?;

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

        self.shared_pager
            .write()
            .dealloc_page::<BtreePage>(child_page)?;

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
        let new_left = self.shared_pager.write().alloc_page::<BtreePage>()?;
        self.acquire_latch(&new_left, NodeAccessMode::Write)?;
        let new_right = self.shared_pager.write().alloc_page::<BtreePage>()?;
        self.acquire_latch(&new_right, NodeAccessMode::Write)?;

        let old_right_child = self.with_latched_page(&self.root, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

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

        let mut cell = self.build_cell(free_space, propagated_key.as_ref())?;
        cell.metadata_mut().left_child = new_left;

        // Now we move part of the cells to one node and the other part to the other node.
        self.with_latched_page_mut(&self.root, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = new_right;
            node.push(cell);
            Ok(())
        })?;

        self.with_latched_page_mut(&new_left, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            left_cells.iter().for_each(|cell| node.push(cell.clone()));

            if let Some(first_cell) = right_cells.first() {
                if old_right_child.is_valid() {

                    let new_right_child = first_cell.metadata().left_child();
                    node.metadata_mut().right_child = new_right_child;
                };
            };

            node.metadata_mut().next_sibling = new_right;
            Ok(())
        })?;

        self.with_latched_page_mut(&new_right, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            right_cells.iter().for_each(|cell| node.push(cell.clone()));
            node.metadata_mut().right_child = old_right_child;
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

        let parent_page = self.pop_last_visited().ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Parent not found on traversal stack!",
        ))?;

        // Internal/Leaf node Overflow/Underflow.
        self.balance_siblings(page_id, &parent_page)?;

        // No need to fuck anything else up.
        if matches!(self.check_node_status(page_id)?, NodeStatus::Balanced) {
            return Ok(());
        };

        let mut siblings = self.load_siblings(page_id, &parent_page, self.num_siblings_per_side)?;

        let mut cells = std::collections::VecDeque::new();

        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            if sibling.pointer != *page_id {
                self.acquire_latch(&sibling.pointer, NodeAccessMode::Write)?;
            };
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

                if right_child.is_valid() {

                    let divider_idx = siblings[i].slot;
                    self.with_latched_page_mut(&parent_page, |p| {
                        let node: &mut BtreePage = p.try_into().unwrap();

                        if divider_idx.0 < node.num_slots() {
                            let mut divider = node.remove(divider_idx);
                            divider.metadata_mut().left_child = right_child;
                            cells.push_back(divider);
                        };
                        Ok(())
                    })?;
                };
            }
        }

        let page_size = self.shared_pager.read().page_size() as usize;
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
        let old_right_child = self.with_latched_page(&siblings.last().unwrap().pointer, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.metadata().right_child)
        })?;

        // Allocate missing pages.
        while siblings.len() < number_of_cells_per_page.len() {
            let new_page = self.shared_pager.write().alloc_page::<BtreePage>()?;
            let parent_index = siblings.last().unwrap().slot + 1usize;
            self.acquire_latch(&new_page, NodeAccessMode::Write)?;
            siblings.push(Child::new(new_page, parent_index));
        };

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            let page = siblings.pop().unwrap().pointer;
            self.release_latch(&page);
            self.shared_pager.write().dealloc_page::<BtreePage>(page)?;
        };

        // Put pages in ascending order to favor sequential IO where possible.
        std::collections::BinaryHeap::from_iter(
            siblings.iter().map(|s| std::cmp::Reverse(s.pointer)),
        )
        .iter()
        .enumerate()
        .for_each(|(i, std::cmp::Reverse(page))| siblings[i].pointer = *page);

        // Fix the last child pointer.
        let last_sibling = siblings[siblings.len() - 1];

        self.with_latched_page_mut(&last_sibling.pointer, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = old_right_child;
            Ok(())
        })?;

        // Fix pointers in the parent in case we have allocated new pages.
        self.with_latched_page_mut(&parent_page, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.metadata_mut().right_child = last_sibling.pointer;
            Ok(())
        })?;

        // Begin redistribution.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let propagated = self.with_latched_page_mut(&siblings[i].pointer, |p| {
                // Push all the cells to the child.
                let node: &mut BtreePage = p.try_into().unwrap();
                for _ in 0..*n {
                    node.push(cells.pop_front().unwrap());
                }

                if i < siblings.len() - 1 {
                    let mut divider = cells.front().unwrap().clone();
                    node.metadata_mut().right_child = divider.metadata().left_child;
                    divider.metadata_mut().left_child = siblings[i].pointer;
                    return Ok(Some(divider));
                };

                Ok(None)
            })?;

            // Fix the pointer in the parent and propagate.
            if let Some(divider) = propagated {
                self.with_latched_page_mut(&parent_page, |p| {
                    let node: &mut BtreePage = p.try_into().unwrap();

                    if node.num_slots() > siblings[i].slot.0 {
                        node.replace(siblings[i].slot, divider);
                    } else {
                        node.insert(siblings[i].slot, divider);
                    };
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
                Ok(Some(Child::new(node.child(slot), slot)))
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
        num_siblings_per_side: usize,
    ) -> std::io::Result<Vec<Child>> {
        let search_result = self.binary_search_child(parent, page)?;

        let last_child = self.with_latched_page(parent, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.max_slot_index())
        })?;

        match search_result {
            SearchResult::Found((_, slot)) => {
                if num_siblings_per_side == 1 {
                    if slot == 0 && slot == last_child {
                        return Ok(vec![Child::new(*page, slot)]);
                    };

                    let mut children = Vec::new();

                    if slot > Slot(0) {
                        let left_page = self.with_latched_page(page, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            Ok(node.metadata().previous_sibling)
                        })?;

                        if let Some(child) = self.load_child(parent, slot - 1usize) {
                            children.push(child);
                        };
                    };

                    children.push(Child::new(*page, slot));

                    if slot < last_child {
                        let right_page = self.with_latched_page(page, |p| {
                            let node: &BtreePage = p.try_into().unwrap();
                            Ok(node.metadata().next_sibling)
                        })?;

                        if let Some(child) = self.load_child(parent, slot + 1usize) {
                            children.push(child);
                        };
                    };

                    return Ok(children);
                };

                // Precompute the child distribution.
                let (siblings_left, siblings_right) = if num_siblings_per_side as u16 > slot.0 {
                    let total = num_siblings_per_side * 2;
                    let num_left = slot.0 as usize;
                    let num_right = total - slot.0 as usize;
                    (num_left, num_right)
                } else if (num_siblings_per_side as u16 + slot.0) > last_child.0 {
                    let total = num_siblings_per_side * 2;
                    let num_right = (last_child.0 - slot.0) as usize;
                    let num_left = total - num_right;
                    (num_left, num_right)
                } else {
                    (num_siblings_per_side, num_siblings_per_side)
                };

                let first_slot = slot.saturating_sub(num_siblings_per_side as u16).0;
                let last_slot = min(slot.0 + num_siblings_per_side as u16, last_child.into());

                let mut siblings = Vec::with_capacity((last_slot - first_slot + 1) as usize);

                for s in first_slot..=last_slot {
                    if let Some(child) = self.load_child(parent, Slot(s)) {
                        siblings.push(child);
                    }
                }

                Ok(siblings)
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Node not found in parent!",
            )),
        }
    }

    fn check_node_status(&self, page_id: &PageId) -> std::io::Result<NodeStatus> {
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
    fn balance_siblings(&mut self, page_id: &PageId, parent: &PageId) -> std::io::Result<()> {
        let is_leaf = self.with_latched_page(page_id, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.is_leaf())
        })?;

        if !is_leaf {
            return Ok(());
        };

        let status = self.check_node_status(page_id)?;

        let siblings = self.load_siblings(page_id, parent, 1)?; // One sibling per side

        if siblings.is_empty() || siblings.len() == 1 {
            return Ok(());
        };

        let sibling_left = siblings.first().unwrap();

        let sibling_right = siblings.last().unwrap();

        match status {
            NodeStatus::Balanced => Ok(()),
            NodeStatus::Overflow => {
                // Okey, we are on overflow state and our left sibling is valid. Then we can ask him if it has space for us.
                if sibling_left.pointer != *page_id {
                    self.acquire_latch(&sibling_left.pointer, NodeAccessMode::Write)?;
                    // The load siblings function can return invalid pointers. we need to check for the validity ourselves.
                    let first_cell_size = self.with_latched_page(page_id, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        Ok(node.cell(Slot(0)).storage_size())
                    })?;

                    // Check if we can fit the cell there
                    let has_space = self.with_latched_page(&sibling_left.pointer, |p| {
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

                        self.with_latched_page_mut(&sibling_left.pointer, |p| {
                            let node: &mut BtreePage = p.try_into().unwrap();
                            node.push(removed_cell);
                            Ok(())
                        })?;

                        // Haha, but now our parent might have fucked up.
                        // We need to unfuck him.
                        // In order to unfuck the parent, we replace the entry that pointed to left with our new max key
                        let separator_index = sibling_left.slot;
                        self.fix_single_pointer(
                            parent,
                            &sibling_left.pointer,
                            page_id,
                            separator_index,
                        )?;
                    };
                };

                let status = self.check_node_status(page_id)?;
                if sibling_right.pointer != *page_id && matches!(status, NodeStatus::Overflow) {
                    self.acquire_latch(&sibling_right.pointer, NodeAccessMode::Write)?;
                    let last_cell_size = self.with_latched_page(page_id, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let last_index = node.max_slot_index();
                        Ok(node.cell(last_index).storage_size())
                    })?;

                    // Check if we can fit the cell there
                    let has_space = self.with_latched_page(&sibling_right.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        Ok(node.has_space_for(last_cell_size as usize))
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

                        // In this case is the same
                        let separator_index = sibling_right.slot - 1usize;
                        self.fix_single_pointer(
                            parent,
                            page_id,
                            &sibling_right.pointer,
                            separator_index,
                        )?;
                    };
                };
                Ok(())
            }
            NodeStatus::Underflow => {
                // Okey, we are on underflow state and our right sibling is valid. Then we can ask him if it has cells for us.
                if sibling_right.pointer != *page_id {
                    self.acquire_latch(&sibling_right.pointer, NodeAccessMode::Write)?;
                    let can_borrow = self.with_latched_page(&sibling_right.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let first_cell_size = node.cell(Slot(0)).storage_size() as usize;
                        Ok(node.can_release_space(first_cell_size))
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

                        // In this case is the same
                        let separator_index = sibling_right.slot - 1usize;
                        self.fix_single_pointer(
                            parent,
                            page_id,
                            &sibling_right.pointer,
                            separator_index,
                        )?;
                    };
                };

                let status = self.check_node_status(page_id)?;
                if sibling_left.pointer != *page_id && matches!(status, NodeStatus::Overflow) {
                    self.acquire_latch(&sibling_left.pointer, NodeAccessMode::Write)?;
                    let can_borrow = self.with_latched_page(&sibling_left.pointer, |p| {
                        let node: &BtreePage = p.try_into().unwrap();
                        let last_cell_size =
                            node.cell(node.max_slot_index()).storage_size() as usize;
                        Ok(node.can_release_space(last_cell_size))
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

                        let separator_index = sibling_left.slot;
                        self.fix_single_pointer(
                            parent,
                            &sibling_left.pointer,
                            page_id,
                            separator_index,
                        )?;
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
        parent: &PageId,
        left_child: &PageId,
        right_child: &PageId,
        separator_idx: Slot,
    ) -> std::io::Result<()> {
        let mut separator = self.with_latched_page(right_child, |p| {
            let node: &BtreePage = p.try_into().unwrap();
            Ok(node.cell(Slot(0)).clone())
        })?;
        separator.metadata_mut().left_child = *left_child;

        self.with_latched_page_mut(parent, |p| {
            let node: &mut BtreePage = p.try_into().unwrap();
            node.replace(separator_idx, separator);
            Ok(())
        })
    }

    fn read_into_mem(
        &mut self,
        root: PageId,
        buf: &mut Vec<(PageId, BtreePage)>,
    ) -> std::io::Result<()> {

        let root_frame = self.shared_pager.write().read_page::<BtreePage>(&root)?;

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
        let mut string = format!("{{\"page\":{number},\"entries\":[");

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
            string.push_str(&format!("{}", page.child(Slot(0))));

            for child in page.iter_children().skip(1) {
                string.push_str(&format!(",{child}"));
            }
        }

        string.push(']');
        string.push('}');

        Ok(string)
    }
}

#[cfg(test)]
mod bplus_tree_tests {
    use super::*;
    use crate::io::pager::Pager;
    use crate::*;
    use serial_test::serial;
    use std::io;
    use tempfile::tempdir;

    // Test key type
    #[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
    struct TestKey(i32);

    impl AsRef<[u8]> for TestKey {
        fn as_ref(&self) -> &[u8] {
            unsafe {
                std::slice::from_raw_parts(
                    &self.0 as *const i32 as *const u8,
                    std::mem::size_of::<i32>(),
                )
            }
        }
    }

    impl std::fmt::Display for TestKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestKey: {}", self.0)
        }
    }

    impl AsMut<[u8]> for TestKey {
        fn as_mut(&mut self) -> &mut [u8] {
            unsafe {
                std::slice::from_raw_parts_mut(
                    &mut self.0 as *mut i32 as *mut u8,
                    std::mem::size_of::<i32>(),
                )
            }
        }
    }

    impl TryFrom<&[u8]> for TestKey {
        type Error = std::io::Error;

        fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
            if bytes.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid length for TestKey: expected 4 bytes",
                ));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&bytes[..4]);
            Ok(TestKey(i32::from_ne_bytes(arr)))
        }
    }

    fn create_test_btree(page_size: u32, capacity: usize, min_keys: usize) -> io::Result<BPlusTree<TestKey>> {
        let dir = tempdir()?;
        let path = dir.path().join("test_btree.db");

        let config = RQLiteConfig {
            page_size,
            cache_size: Some(capacity as u16),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        Ok(BPlusTree::new(SharedPager::from(pager), min_keys, 2))
    }

    #[test]
    fn validate_test_key() {
        let key = TestKey(9);
        let bytes = key.as_ref();
        let key_result = TestKey::try_from(bytes);
        assert!(key_result.is_ok());
        assert_eq!(
            key_result.unwrap(),
            key,
            "Deserialization failed without padding"
        );

        let mut padded = Vec::from(bytes);
        padded.extend_from_slice(&[0x00, 0xFF, 0xAA]); // Extra bytes

        // Validate that with padding we can compare the results effectibvely too.
        let padded_result = TestKey::try_from(padded.as_ref());
        assert!(
            padded_result.is_ok(),
            "Deserialization failed when adding padding"
        );
        assert_eq!(padded_result.unwrap(), key);
    }

    /// Searches on an empty bplustree.
    /// The search should not fail but also should not return any results.
    #[test]
    #[serial]
    fn test_search_empty_tree() -> io::Result<()> {
        let mut tree = create_test_btree(4096, 1, 2)?;
        let root = tree.get_root();
        let key = TestKey(42);

        let result = tree.search(&root, &key, NodeAccessMode::Read)?;
        assert!(matches!(result, SearchResult::NotFound(_)));

        Ok(())
    }

    /// Inserts a key on the tree and searches for it.
    /// Validates that the retrieved key matches the inserted data.
    #[test]
    #[serial]
    fn test_single_key() -> io::Result<()> {
        let mut btree = create_test_btree(4096, 1, 2)?;

        let root = btree.get_root();

        // Insert a key-value pair
        let key = TestKey(42);

        btree.insert(&root, key.as_ref())?;

        // Retrieve it back
        let retrieved = btree.search(&root, &key, NodeAccessMode::Read)?;
        assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
        let cell = btree.get_cell_from_result(retrieved);
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().used(), key.as_ref());

        Ok(())
    }

    #[test]
    #[serial]
    fn test_split_root() -> io::Result<()> {
        let mut tree = create_test_btree(4096, 3, 2)?;
        let root = tree.get_root();

        // Insert enough keys to force root to split
        for i in 0..50 {
            let key = TestKey(i * 10);
            tree.insert(&root, key.as_ref())?;
        }
        println!("{}", tree.json()?);

        for i in 0..50 {
            let key = TestKey(i * 10);

            let retrieved = tree.search(&root, &key, NodeAccessMode::Read)?;
            // Search does not release the latch on the node so we have to do it ourselves.
            let cell = tree.get_cell_from_result(retrieved);
            tree.clear_stack();
            assert!(cell.is_some());
            assert_eq!(cell.unwrap().used(), key.as_ref());
        }

        println!("{}", tree.json()?);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_balance_multiple_inserts() -> io::Result<()> {
        let mut tree = create_test_btree(4096, 10000, 3)?;
        let root = tree.get_root();
        const N: i32 = 900000;
        // Insert enough keys to force [balance_siblings] to be called
        for i in 0..N {
            let key = TestKey(i);
            tree.insert(&root, key.as_ref())?;
        };

        for i in 0..N {
            let key = TestKey(i);
            let retrieved = tree.search(&root, &key, NodeAccessMode::Read)?;
            // Search does not release the latch on the node so we have to do it ourselves.
            let cell = tree.get_cell_from_result(retrieved);
            tree.clear_stack();
            assert!(cell.is_some());
            assert_eq!(cell.unwrap().used(), key.as_ref());
        };

        Ok(())
    }
}
