//! Disk BTree data structure implementation.
use std::{
    cmp::{min, Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    io::{self, Read, Seek, Write, Error, ErrorKind},
    mem,
};

use crate::{io::pager::Pager, storage::page::{BtreePage, MemPage}, types::{DataType, PageId, PAGE_ZERO}};
use crate::io::pager::{SharedPager, access_pager, access_pager_mut};
use crate::storage::latches::Latch;
use std::sync::Arc;
use crate::storage::{
    cell::{Cell, Slot},
    page::{OverflowPage, Page},
};

pub(crate) enum NodeSearchResult {
    Found(Slot), // Key found -> return its slot
    NotFound(Slot) // Key not found -> return the most likely child
}


impl NodeSearchResult {
    fn get_result(&self) -> Slot {
        match self {
            NodeSearchResult::Found(s) => *s,
            NodeSearchResult::NotFound(s) => *s
        }
    }
}

/// The result of a search in the [`BTree`] structure.
pub(crate) struct SearchResult {

    pub page: PageId,
    pub node: NodeSearchResult,
}

/// The result of a remove operation on the BTree.
struct RemovalResult {
    /// The removed cell.
    cell: Cell,
    /// Page number of the leaf node where a substitute was found.
    leaf_node: PageId,
    /// If the removal ocurred on an internal node, then this is its page number.
    internal_node: Option<PageId>,
}



enum SearchType {
    /// Maximum key in a leaf node.
    Max,
    /// Minimum key in a leaf node.
    Min,
}


pub enum Payload<'p>{
    Reassembled(Vec<u8>),
    CellReference(&'p [u8])

}


// The generic arg in the BTree is the [DataType] of the key.
pub(crate) struct BTree<K: Ord> {
    /// Root page.
    root: PageId,

    /// Shared pager instance.
    pager: SharedPager,
    /// Number of siblings to examine at each side when balancing a node.
    balance_siblings_per_side: usize,

    /// Forces pages to store at least this number of [`Cell`] instances.
    minimum_keys: usize,

    key_type: std::marker::PhantomData<K>
}

/// Default value for [`BTree::balance_siblings_per_side`].
pub(crate) const DEFAULT_BALANCE_SIBLINGS_PER_SIDE: usize = 1;

/// Default value for [`BTree::minimum_keys`].
pub(crate) const DEFAULT_MINIMUM_KEYS: usize = 4;

impl<K> BTree<K>
where K: Ord
{
    pub fn new(pager: SharedPager, root: PageId) -> Self {
        Self {
            pager: Arc::clone(&pager),
            root,
            balance_siblings_per_side: DEFAULT_BALANCE_SIBLINGS_PER_SIDE,
            minimum_keys: DEFAULT_MINIMUM_KEYS,
            key_type: std::marker::PhantomData
        }
    }
}

impl<K> BTree<K>
where K: Ord,
{
    /// Returns the value corresponding to the key.
    ///
    /// See [`Self::search`] for details.
    pub fn get(&mut self, entry: &[u8]) -> io::Result<Option<Payload>> {
        let search = self.search(self.root, entry, &mut Vec::new())?;

        match search.node {
            NodeSearchResult::NotFound(_slot) => Ok(None),
            NodeSearchResult::Found(slot ) => Ok(Some(reassemble_payload(Arc::clone(&self.pager), search.page, slot)?)),
        }
    }


    pub fn get_page_latch(&self, page_id: PageId) -> Option<Latch<MemPage>> {
        if let Ok(page) = self.pager.borrow_mut().read_page::<BtreePage>(page_id){
            Some(page.read())
        } else {
            None
        }
    }

    pub fn get_page_latch_mut(&mut self, page_id: PageId) -> Option<Latch<MemPage>> {
        if let Ok(page) = self.pager.borrow_mut().read_page::<BtreePage>(page_id){
            Some(page.write())
        } else {
            None
        }
    }


    pub fn get_page(&self, page_id: PageId) -> Option<MemPage> {
        if let Ok(page) = self.pager.borrow_mut().read_page::<BtreePage>(page_id){
            Some(page)
        } else {
            None
        }
    }

    /// BTree Search Algorithm
    pub fn search(
        &self,
        page_id: PageId,
        entry: &[u8],
        parents: &mut Vec<PageId>,
    ) -> io::Result<SearchResult> {
        // Search key in this node.
        let node_search = self.binary_search(page_id, entry).unwrap();


        // TODO: Implement some kind of lock coupling strategy like with the cursor.
        let page_latch = self.get_page_latch(page_id).unwrap();
        let page: &BtreePage = (&page_latch).try_into().unwrap();

        // We found the key or we're already at the bottom, stop recursion.
        if matches!(node_search, NodeSearchResult::Found(_)) || page.is_leaf() {
            return Ok(SearchResult { page: page.page_number(), node: node_search});
        }

        // No luck, keep recursing downwards.
        parents.push(page.page_number());
        let next_node = page.child(node_search.get_result());

        self.search(next_node, entry, parents)
    }

    /// Binary search with support for overflow data.
    ///
    /// Returns an [`Ok`] result containing the index where `entry` was found or
    /// an [`Err`] result containing the index where `entry` should have been
    /// found if present.
    fn binary_search(&self, page_id: PageId, entry: &[u8]) -> io::Result<NodeSearchResult> {

        let mut size = {
            let page = self.get_page_latch(page_id).unwrap();
            let btree: &BtreePage = (&page).try_into().unwrap();
            Slot(btree.num_slots())
        };

        let left = Slot(0);
        let right = size;

        while left < right {

            let mid = left + size / Slot(2);

            let page = self.get_page_latch(page_id).unwrap();
            let btree: &BtreePage = (&page).try_into().unwrap();
            let cell = btree.cell(mid);
            let payload = if cell.metadata().is_overflow() {

                match reassemble_payload(Arc::clone(&self.pager), page_id, mid)? {
                    Payload::Reassembled(buf) => buf, // To do: figure out ho to do this without cloning
                    _ => unreachable!(),
                }

            } else {
                cell.used().to_vec()
            };


            // The key content is always the first bytes of the cell

            // TODO: RAUL -> FIGURE OUT HOW TO DO THE COMPARISON STUFF
            // PROBABLY I WILL NEED A COMPARATOR TO DO IT
           /* match self.comparator.bytes_cmp(payload, entry) {
                Ordering::Equal => return Ok(Ok(mid)),
                Ordering::Greater => right = mid,
                Ordering::Less => left = mid + 1,
            }*/

            size = right - left;
        }

        Ok(NodeSearchResult::NotFound(left))
    }

    /// Inserts a new entry into the tree or updates the value if the key
    pub fn insert(&mut self, entry: Vec<u8>) -> io::Result<()> {
        let mut parents = Vec::new();
        let search = self.search(self.root, &entry, &mut parents)?;

        let mut new_cell = self.alloc_cell(entry)?;
        let mut latch = self.get_page_latch_mut(search.page).unwrap();
        let page: &mut BtreePage = (&mut latch).try_into().unwrap();

        match search.node {
            // Key found, swap value.
            NodeSearchResult::Found(index) => {
                new_cell.metadata_mut().left_child = page.cell(index).metadata().left_child();
                let (old_cell, overflow_payload) = page.replace(index, *new_cell);

                free_cell(Arc::clone(&self.pager), old_cell)?;
                overflow_payload
            }
            // Key not found, insert new entry.
            NodeSearchResult::NotFound(index) => page.insert(index, *new_cell),
        };
        // RAUL -> TODO: MAYBE HERE WE SHOULD HANDLE THE POSSIBLE OVERFLOW THAT COULD HAVE OCCURRED
        self.balance(search.page, &mut parents)
    }

    /// Same as [`Self::insert`] but doesn't update the key if it already
    /// exists.
    ///
    /// It returns its location as an error instead.
    pub fn try_insert(&mut self, entry: Vec<u8>) -> io::Result<()> {
       let mut parents = Vec::new();
        let search = self.search(self.root, &entry, &mut parents)?;

        let new_cell = self.alloc_cell(entry)?;
        let mut latch = self.get_page_latch_mut(search.page).unwrap();
        let page: &mut BtreePage = (&mut latch).try_into().unwrap();

        match search.node {
            // Key found, swap value.
            NodeSearchResult::Found(index) => return Err(io::Error::new(ErrorKind::AlreadyExists, "The key already exists in the tree. Cannot insert.")),
            // Key not found, insert new entry.
            NodeSearchResult::NotFound(index) => page.insert(index, *new_cell),
        };
        // RAUL -> TODO: MAYBE HERE WE SHOULD HANDLE THE POSSIBLE OVERFLOW THAT COULD HAVE OCCURRED
        self.balance(search.page, &mut parents)
    }

    /// Removes the entry corresponding to the given key if it exists.
    pub fn remove(&mut self, entry: &[u8]) -> io::Result<Option<Cell>> {
        let mut parents = Vec::new();
        let Some(RemovalResult
             {
            cell,
            leaf_node,
            internal_node,
        }) = self.remove_entry(entry, &mut parents)?
        else {
            return Ok(None);
        };

        self.balance(leaf_node, &mut parents)?;

        // Since data is variable size it could happen that we peek a large
        // substitute from a leaf node to replace a small key in an internal
        // node, leaving the leaf node balanced and the internal node overflow.
        if let Some(node) = internal_node {
            // This algorithm is O(n) but the height of the tree grows
            // logarithmically so there shoudn't be that many elements to search
            // here.
            if let Some(index) = parents.iter().position(|n| n == &node) {
                parents.drain(index..);
                self.balance(node, &mut parents)?;
            }
        }

        Ok(Some(cell))
    }




    /// Finds the node where `key` is located and removes it from the page.
    ///
    /// The removes [`Cell`] is replaced with either its predecessor or
    /// successor in the case of internal nodes. [`Self::balance`] must be
    /// called on the leaf node after this operation, and possibly on the
    /// internal node if the leaf was balanced. See [`Self::remove`] for more
    /// details.
    fn remove_entry(
        &mut self,
        entry: &[u8],
        parents: &mut Vec<PageId>,
    ) -> io::Result<Option<RemovalResult>> {
        let search = self.search(self.root, entry, parents)?;
        let mut latch = self.get_page_latch_mut(search.page).unwrap();
        let page: &mut BtreePage = (&mut latch).try_into().unwrap();

        // Can't remove entry, key not found.
        if matches!(search.node, NodeSearchResult::NotFound(_)) {
            return Ok(None);
        }

        let index = search.node.get_result();

        // Leaf node is the simplest case, remove key and pop off the stack.
        if page.is_leaf() {

                let cell = page.remove(index);

                return Ok(Some(RemovalResult {
                    cell,
                    leaf_node: search.page,
                    internal_node: None,
                }));



        }

        // Root or internal nodes require additional work. We need to find a
        // suitable substitute for the key in one of the leaf nodes. We'll
        // pick either the predecessor (max key in the left subtree) or the
        // successor (min key in the right subtree) of the key we want to
        // delete.
        let left_child = page.child(index);
        let right_child = page.child(index + 1);
        parents.push(search.page);


        let (leaf_node, key_idx) =
            if self.pager.get(left_child)?.len() >= self.pager.get(right_child)?.len() {
                self.search_max_key(left_child, parents)?
            } else {
                self.search_min_key(right_child, parents)?
            };

        let mut substitute = self.pager.try_borrow_mut()?.read_page::<BtreePage>(leaf_node)?.remove(key_idx);

        let node = self.pager.try_borrow_mut()?.read_page::<BtreePage>(search.page)?;

        substitute.header.left_child = node.child(index);
        let cell = node.replace(index, substitute);

        Ok(Some(RemovalResult {
            cell,
            leaf_node,
            internal_node: Some(search.page),
        }))
    }

    /// Traverses the tree all the way down to the leaf nodes, following the
    /// path specified by [`LeafKeySearch`].
    ///
    /// [`LeafKeySearch::Max`] will always choose the last child for recursion,
    /// while [`LeafKeySearch::Min`] will always choose the first child. This
    /// function is used to find successors or predecessors of keys in internal
    /// nodes.
    fn search_leaf_key(
        &mut self,
        page: PageId,
        parents: &mut Vec<PageId>,
        leaf_key_search: SearchType,
    ) -> io::Result<(PageId, u16)> {
        let node = self.pager.get(page)?;

        let (key_idx, child_idx) = match leaf_key_search {
            SearchType::Min => (0, 0),
            SearchType::Max => (node.len().saturating_sub(1), node.len()),
        };

        if node.is_leaf() {
            return Ok((page, key_idx));
        }

        parents.push(page);
        let child = node.child(child_idx);

        self.search_leaf_key(child, parents, leaf_key_search)
    }

    /// Returns the page number and slot index in [`Page`] where the greatest
    /// key of the given subtree is located.
    fn search_max_key(
        &mut self,
        page: PageId,
        parents: &mut Vec<PageId>,
    ) -> io::Result<(PageId, u16)> {
        self.search_leaf_key(page, parents, SearchType::Max)
    }

    /// Returns the page number and slot index in [`Page`] where the smallest
    /// key of the given subtree is located.
    fn search_min_key(
        &mut self,
        page: PageId,
        parents: &mut Vec<PageId>,
    ) -> io::Result<(PageId, u16)> {
        self.search_leaf_key(page, parents, SearchType::Min)
    }

    /// Returns the greatest entry in this tree.
    pub fn max(&mut self) -> io::Result<Option<Payload>> {
        let (page, slot) = self.search_max_key(self.root, &mut Vec::new())?;

        if self.pager.get(page)?.is_empty() {
            return Ok(None);
        }

        reassemble_payload(self.pager, page, slot).map(Some)
    }


    fn balance(&mut self, mut page: PageId, parents: &mut Vec<PageId>) -> io::Result<()> {
        let node = self.pager.get(page)?;

        // Started from the bottom now we're here :)
        let is_root = parents.is_empty();

        // The root is a special case because it can be fully empty.
        let is_underflow = node.is_empty() || !is_root && node.is_underflow();

        // Nothing to do, the node is balanced.
        if !node.is_overflow() && !is_underflow {
            return Ok(());
        }

        // Root underflow.
        if is_root && is_underflow {
            // Root doesn't have children, can't do anything.
            if node.is_leaf() {
                return Ok(());
            }

            let child_page = node.header().right_child;
            let necessary_space = self.pager.get(child_page)?.used_bytes();

            // Account for page zero having less space than the rest of pages.
            // If page zero can't consume its children then we'll leave it
            // empty temporarily until it can do so safely.
            if self.pager.get(page)?.free_space() < necessary_space {
                return Ok(());
            }

            let child = self.pager.try_borrow_mut()?.read_page::<BtreePage>(child_page)?;
            let grandchild = child.header().right_child;
            let cells = child.drain(..).collect::<Vec<_>>();

            self.pager.free_page(child_page)?;

            let root = self.pager.try_borrow_mut()?.read_page::<BtreePage>(page)?;
            cells.into_iter().for_each(|cell| root.push(cell));
            root.header_mut().right_child = grandchild;

            return Ok(());
        }

        // Root overflow.
        if is_root && node.is_overflow() {
            let new_page = self.pager.alloc_page::<Page>()?;

            let root = self.pager.try_borrow_mut()?.read_page::<BtreePage>(page)?;
            let grandchild = mem::replace(&mut root.header_mut().right_child, new_page);
            let cells = root.drain(..).collect::<Vec<_>>();

            let new_child = self.pager.try_borrow_mut()?.read_page::<BtreePage>(new_page)?;
            cells.into_iter().for_each(|cell| new_child.push(cell));
            new_child.header_mut().right_child = grandchild;

            parents.push(page);
            page = new_page;
        }

        // Internal/Leaf node Overflow/Underlow.
        let parent_page = parents.remove(parents.len() - 1);
        let mut siblings = self.load_siblings(page, parent_page)?;

        // Run into some nasty bug because of this and it was hard to spot so...
        debug_assert_eq!(
            HashSet::<PageId>::from_iter(siblings.iter().map(|s| s.page)).len(),
            siblings.len(),
            "siblings array contains duplicated pages: {siblings:?}"
        );

        let mut cells = VecDeque::new();
        let divider_idx = siblings[0].index;

        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            cells.extend(self.pager.try_borrow_mut()?.read_page::<BtreePage>(sibling.page)?.drain(..));
            if i < siblings.len() - 1 {
                let mut divider = self.pager.try_borrow_mut()?.read_page::<BtreePage>(parent_page)?.remove(divider_idx);
                divider.header.left_child = self.pager.get(sibling.page)?.header().right_child;
                cells.push_back(divider);
            }
        }

        let usable_space = Page::usable_space(self.pager.page_size);

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
            let mut div_cell = cells.len() - number_of_cells_per_page.last().unwrap() - 1;

            for i in (1..=(total_size_in_each_page.len() - 1)).rev() {
                while total_size_in_each_page[i] < usable_space / 2 {
                    number_of_cells_per_page[i] += 1;
                    total_size_in_each_page[i] += &cells[div_cell].storage_size();

                    number_of_cells_per_page[i - 1] -= 1;
                    total_size_in_each_page[i - 1] -= &cells[div_cell - 1].storage_size();
                    div_cell -= 1;
                }
            }

            // Second page has more data than the first one, make a little
            // adjustment to keep it left biased.
            if total_size_in_each_page[0] < usable_space / 2 {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
            }
        }

        let old_right_child = self
            .pager
            .get(siblings.last().unwrap().page)?
            .header()
            .right_child;

        // Allocate missing pages.
        while siblings.len() < number_of_cells_per_page.len() {
            let new_page = self.pager.alloc_page::<Page>()?;
            let parent_index = siblings.last().unwrap().index + 1;
            siblings.push(Sibling::new(new_page, parent_index));
        }

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            self.pager.free_page(siblings.pop().unwrap().page)?;
        }

        // Put pages in ascending order to favor sequential IO where possible.
        BinaryHeap::from_iter(siblings.iter().map(|s| Reverse(s.page)))
            .iter()
            .enumerate()
            .for_each(|(i, Reverse(page))| siblings[i].page = *page);

        // Fix children pointers.
        let last_sibling = siblings[siblings.len() - 1];
        self.pager
            .get_mut(last_sibling.page)?
            .header_mut()
            .right_child = old_right_child;

        let parent_node = self.pager.try_borrow_mut()?.read_page::<BtreePage>(parent_page)?;
        if divider_idx == parent_node.len() {
            parent_node.header_mut().right_child = last_sibling.page;
        } else {
            parent_node.cell_mut(divider_idx).header.left_child = last_sibling.page;
        }

        // Begin redistribution.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let page = self.pager.try_borrow_mut()?.read_page::<BtreePage>(siblings[i].page)?;
            for _ in 0..*n {
                page.push(cells.pop_front().unwrap());
            }

            if i < siblings.len() - 1 {
                let mut divider = cells.pop_front().unwrap();
                page.header_mut().right_child = divider.header.left_child;
                divider.header.left_child = siblings[i].page;
                self.pager
                    .get_mut(parent_page)?
                    .insert(siblings[i].index, divider);
            }
        }

        // Done, propagate upwards.
        self.balance(parent_page, parents)?;

        Ok(())
    }

    fn load_siblings(
        &mut self,
        page: PageId,
        parent_page: PageId,
    ) -> io::Result<Vec<Sibling>> {
        let mut num_siblings_per_side = self.balance_siblings_per_side as u16;

        let parent = self.pager.get(parent_page)?;

        // TODO: Store this somewhere somehow, probably in the "parents" Vec that
        // we keep passing around. It's not a big deal anyway except for large
        // pages with many cells.
        let index = parent.iter_children().position(|p| p == page).unwrap() as u16;

        if index == 0 || index == parent.len() {
            num_siblings_per_side *= 2;
        };

        let left_siblings = index.saturating_sub(num_siblings_per_side)..index;
        let right_siblings = (index + 1)..min(index + num_siblings_per_side + 1, parent.len() + 1);

        let read_sibling = |index| Sibling::new(parent.child(index), index);

        Ok(left_siblings
            .map(read_sibling)
            .chain(std::iter::once(Sibling::new(page, index)))
            .chain(right_siblings.map(read_sibling))
            .collect())
    }

    /// Allocates a cell that can fit the entire given `payload`.
    ///
    /// Overflow pages are used if necessary. See [`OverflowPage`] for details.
    fn alloc_cell(&mut self, payload: Vec<u8>) -> io::Result<Box<Cell>> {
        let max_payload_size =
            Page::ideal_max_payload_size(self.pager.page_size, self.minimum_keys) as usize;

        // No overflow needed.
        if payload.len() <= max_payload_size {
            return Ok(Cell::new(payload));
        }

        // Store payload in chunks, link overflow pages and return the cell.
        let first_cell_payload_size = max_payload_size - mem::size_of::<PageId>();
        let mut overflow_page_number = self.pager.alloc_page::<OverflowPage>()?;

        // TODO: We're making a copy of the vec and the Cell::new makes another one...
        let cell = Cell::new_overflow(
            Vec::from(&payload[..first_cell_payload_size]),
            overflow_page_number,
        );

        let mut stored_bytes = first_cell_payload_size;

        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(self.pager.page_size) as usize,
                payload[stored_bytes..].len(),
            );

            let overflow_page = self
                .pager
                .get_mut_as::<OverflowPage>(overflow_page_number)?;

            overflow_page.content_mut()[..overflow_bytes]
                .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);

            overflow_page.header_mut().num_bytes = overflow_bytes as _;

            stored_bytes += overflow_bytes;

            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = self.pager.alloc_page::<OverflowPage>()?;

            self.pager
                .get_mut_as::<OverflowPage>(overflow_page_number)?
                .header_mut()
                .next = next_overflow_page;

            overflow_page_number = next_overflow_page;
        }

        Ok(cell)
    }


}

/// Frees the pages occupied by the given `cell`.
///
/// Pretty much a no-op if the cell is not "overflow".
///
/// This function and [`reassemble_payload`] are declared outside of the
/// [`BTree`] struct because they are reused in other modules like
/// [`crate::vm::statement`] or [`crate::vm::plan`].
pub(crate) fn free_cell(
    pager: SharedPager,
    cell: Cell,
) -> io::Result<()> {
    if !cell.metadata().is_overflow() {
        return Ok(());
    }

    let mut overflow_page = cell.overflow_page();

    while overflow_page != PAGE_ZERO {
        let pager_mut = pager.borrow_mut();
        let unused_page = pager_mut.read_page::<OverflowPage>(overflow_page)?;

        let next   = {
            let latch: OverflowPage = unused_page.read().try_into()?;
            latch.metadata().next

        };

        pager_mut.dealloc_page(overflow_page)?;
        overflow_page = next;
    }

    Ok(())
}

/// Joins a split payload back into one contiguous memory region.
///
/// If the cell at the given slot is not "overflow" then this simply returns a
/// reference to its content.
pub(crate) fn reassemble_payload(
    pager: SharedPager,
    page: PageId,
    slot: Slot,
) -> io::Result<Payload<'static>> {
    let cell = pager.get(page)?.cell(slot);
    let is_overflow = cell.header.is_overflow;

    if !is_overflow {
        return Ok(Payload::CellReference(&cell.content));
    }

    let mut overflow_page = cell.overflow_page();


    // Read the overflow page.
    let mut payload = Vec::from(&cell.content[..cell.content.len() - mem::size_of::<PageId>()]);

    while overflow_page != PAGE_ZERO {
        let page = pager.read_page::<OverflowPage>(overflow_page)?;
        payload.extend_from_slice(page.payload());

        let next = page.header().next;

        debug_assert_ne!(
            next, overflow_page,
            "overflow page that points to itself causes infinite loop on reassemble_payload(): {:?}",
            page.header(),
        );
        overflow_page = next;
    }

    Ok(Payload::Reassembled(payload))
}





#[derive(Debug)]
#[repr(u8)]
enum TravelMode {
    Read,
    Write,
    Mixed
}

/// BTree cursor.
#[derive(Debug)]
pub(crate) struct BtreeCursor {
    /// The page that the cursor points at currently.
    page: PageId,
    /// Current slot in [`Self::page`].
    slot: Slot,
    /// Stack of parents. See the documentation of [`Self::try_next`].
    descent: Vec<PageId>,
    /// Shared Pager for latch acquisition.
    pager: Option<SharedPager>,
    /// Stack of latches
    latches: HashMap<PageId, Latch<MemPage>>,
    /// `true` if there are no more elements to return.
    done: bool,
    /// The mode in which this cursor operates
    mode: TravelMode
}



type Position = (PageId, Slot);


impl BtreeCursor {
    /// New uninitialized cursor located at `(page, slot)`.
    pub fn new_uninit(position: Position, mode: TravelMode) -> Self {
        Self {
            page: position.0,
            slot: position.1,
            descent: vec![],
            latches: HashMap::new(),
            pager: None,
            mode,
            done: false,
        }
    }



    fn is_init(&self) -> bool {
        self.pager.is_some()
    }


    pub fn new_init(position: Position, descent: Vec<PageId>, pager: SharedPager, mode: TravelMode) -> Self {


        let pager = Arc::clone(pager);
        let mut latches = HashMap::new();

        if let Some(page) = access_pager_mut(pager, |p: &Pager |
            p.read_page::<BtreePage>(position.0)
        ){
            latches.insert(position.0, page.upgradable())?;
        };

        Self {
            page: position.0,
            slot: position.1,
            descent,
            pager: Some(pager),
            latches,
            mode,
            done: false,
        }
    }


    fn acquire_latch(&mut self, page: PageId) -> bool{

        if let Some(frame) = access_pager_mut(self.pager, |p: &Pager |
            p.read_page::<BtreePage>(page)
        ){

            match self.mode {
                TravelMode::Mixed => self.latches.insert(page, frame.upgradable()),
                TravelMode::Read => self.latches.insert(page, frame.read()),
                TravelMode::Write => self.latches.insert(page, frame.write()),
            };

            true
        }

        false
    }

    fn release_latch(&mut self, latch_id: PageId) {
        self.latches.remove(&latch_id);
    }


    pub fn initialize(&mut self, pager: SharedPager) -> bool {
        !if self.is_init(){
            self.pager = Some(pager);
        };
        self.acquire_latch(self.page)
    }

    fn move_to_leftmost(
        &mut self,
        pager: SharedPager,
    ) -> io::Result<()> {

        if self.initialize(pager) {

            let mut node = self.latches.get(&self.page).unwrap();

            while !node.is_leaf() {

                self.descent.push(self.page);
                self.page = node.child(0);
                // Lock coupling strategy mainly consists on acquiring the lock on the children and inmediately after release it on the parent.
                node = if self.acquire_latch(self.page) {
                    self.latches.get(&self.page).unwrap();
                } else {
                    return Err(Error::new(ErrorKind::Interrupted, "Unable to acquire latch on current node"));
                };
                let parent = self.descent[self.descent.len() - 1];
                self.release_latch(parent);
            }

            self.slot = 0;
            Ok(())
        }

        Err(Error::new(ErrorKind::Interrupted,
            "Unable to initialize Cursor!"))
    }


    pub fn try_next(
        &mut self,
        pager: SharedPager,
    ) -> io::Result<Option<Position>> {
        if self.done {
            return Ok(None);
        }

        // Lazy initialization
        if !self.is_init() {
            self.move_to_leftmost(pager)?;
        };

        let node = self.latches.get(self.page).unwrap();

        // The only page that can be empty is the root. The BTree will not allow
        // the rest of pages to stay empty.
        if node.is_empty() && node.is_leaf() {
            self.done = true;
            return Ok(None);
        }

        // We return the "current" position and prepare the next one on every call.
        let position = Ok(Some((self.page, self.slot)));

        // We are currently returning keys from a leaf node and we're not done
        // yet, so simply move to the next key (or cell in this case).
        if node.is_leaf() && self.slot + 1 < node.len() {
            self.slot += 1;
            return position;
        }

        // The last position we returned was pointing at an internal node and
        // the node has more children. Move to the bottom of the next subtree to
        // maintain order.
        if !node.is_leaf() && self.slot < node.len() {
            self.descent.push(self.page);
            self.page = node.child(self.slot + 1);
            self.move_to_leftmost(pager)?;

            return position;
        }

        // Now we know for sure we have to move upwards because we are done with
        // the current subtree. We'll go back the same path we came from until
        // we find the next branch that we have to take.
        let mut found_branch = false;

        while !self.descent.is_empty() && !found_branch {
            let parent_page = self.descent.pop().unwrap();
            let parent = self.latches.get(&parent_page).unwrap();
            // TODO: We can get rid of this O(n) by storing the index in the stack.
            let index = parent.iter_children().position(|c| c == self.page).unwrap() as u16;
            // Follow lock coupling strategy.
            node = if self.acquire_latch(parent_page) {
                self.latches.get(&parent_page).unwrap();
            } else {
                return Err(Error::new(ErrorKind::Interrupted, "Unable to acquire latch on current node"));
            };
            self.release_latch(self.page);
            self.page = parent_page;



            if index < parent.len() {
                self.slot = index;
                found_branch = true;
            }
        }

        // We went all the way back to the root and didn't find any branch. We
        // are done.
        if self.descent.is_empty() && !found_branch {
            self.done = true;
        }

        position
    }

    /// Returns the next position in the BTree.
    pub fn next(
        &mut self,
        pager: SharedPager,
    ) -> Option<io::Result<Position>> {
        self.try_next(pager).transpose()
    }
}
