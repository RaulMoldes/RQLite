//mod btree;

//#[cfg(test)]
//mod tests;

use crate::io::frames::MemFrame;
use crate::io::pager::{access_pager, access_pager_mut, SharedPager};
use crate::TextEncoding;
use std::cmp::min;
use std::cmp::Ordering;

use crate::storage::page::{BtreePage, MemPage, OverflowPage};
use crate::storage::{
    cell::{Cell, Slot},
    latches::Latch,
};

use crate::types::{DataType, PageId, SizedType, PAGE_ZERO};
use std::cell::RefCell;
use std::collections::HashMap;
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

enum Payload<'b> {
    Boxed(Box<[u8]>),
    Reference(&'b [u8]),
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
    pub(crate) fn new(pager: SharedPager, root: PageId, min_keys: usize) -> Self {
        Self {
            shared_pager: Arc::clone(&pager),
            root,
            min_keys,
            __comparator: std::marker::PhantomData,
        }
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    fn visit_node(&self, page_id: &PageId, access_mode: NodeAccessMode) -> std::io::Result<()> {
        let latch_page =
            access_pager_mut(&self.shared_pager, |p| p.read_page::<BtreePage>(page_id))?;

        LATCH_STACK
            .try_with(|l| {
                if l.try_borrow_mut()
                    .map(|mut stack| {
                        // Lock coupling strategy is demonstrated here.
                        stack.acquire(*page_id, latch_page, access_mode);
                        if let Some(last) = stack.last() {
                            if last != *page_id {
                                stack.release(&last);
                            };
                        };
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
            return self.binary_search_leaf(page_id, entry);
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

    fn binary_search_leaf(
        &self,
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
                    Ordering::Less => left = mid + 1,
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
            SearchResult::NotFound(page_id) => Ok(()) // No OP if not found.
        }
    }




    /// Removes the entry corresponding to the given key if it exists.
    pub fn remove(&mut self, page_id: &PageId, key: &K) -> std::io::Result<Cell> {
        let search = self.search(page_id, key, NodeAccessMode::Write)?;

        match search {
            SearchResult::NotFound(_) => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found on page. Cannot remove"
            )),
            SearchResult::Found((page_id, slot)) => {
                self.with_latched_page_mut(&page_id, |p| {
                    let btreepage: &mut BtreePage = p.try_into().unwrap();
                    let cell = btreepage.remove(slot);
                    Ok(cell)
                })
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
}
