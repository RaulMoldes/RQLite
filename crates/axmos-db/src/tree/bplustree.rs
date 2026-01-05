use crate::{
    io::pager::SharedPager,
    multithreading::coordinator::Snapshot,
    schema::base::Schema,
    storage::{
        BtreeMetadata, BtreeOps, Identifiable,
        cell::OwnedCell,
        page::{BtreePage, OverflowPage},
        tuple::{Row, Tuple, TupleBuilder, TupleError, TupleReader, TupleRef},
    },
    tree::accessor::{
        Accessor, BtreePagePosition, BtreeReadAccessor, BtreeWriteAccessor, TreeReader, TreeWriter,
    },
    types::{PAGE_ZERO, PageId},
};

use super::{
    accessor::Position,
    cell_ops::{CellBuilder, CellComparator, CellDeallocator, Reassembler},
};

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, HashSet, VecDeque},
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult, Write},
    fs,
    io::{self, Error as IoError, ErrorKind},
    path::Path,
    usize,
};

const MINIMUM_KEYS_PER_PAGE: usize = 3;

pub(crate) enum SearchResult {
    Found(BtreePagePosition),
    NotFound(BtreePagePosition),
}

#[derive(Debug)]
#[repr(u8)]
pub(crate) enum NodeStatus {
    Underflow,
    Overflow,
    Balanced,
}

#[derive(Debug)]
pub enum BtreeError {
    Io(IoError),
    BtreePageNotFound(PageId),
    BtreeUnintialized,
    TraversalEmpty,
    BtreeEmpty,
    InvalidIterator((PageId, usize)),
    InvalidPointer(String),
    DuplicatedKey(String),
    NonExistentKey,
    TupleReadError(TupleError),
    Other(String),
}

impl Display for BtreeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Io(err) => write!(f, "Io error {err}"),
            Self::TraversalEmpty => f.write_str("traversal stack is empty!"),
            Self::BtreePageNotFound(id) => write!(f, "btree page not found: {id}"),
            Self::InvalidPointer(str) => write!(f, "invalid tree structure: {str}"),
            Self::BtreeUnintialized => f.write_str("btree not initialized"),
            Self::NonExistentKey => write!(f, "key  does not exist"),
            Self::DuplicatedKey(str) => write!(f, "key {str} is duplicated"),
            Self::TupleReadError(err) => write!(f, "Tuple read error {err}"),
            Self::InvalidIterator(pos) => write!(
                f,
                "Btree iterator received an invalid position to iterate over: ({}, {})",
                pos.0, pos.1
            ),
            Self::BtreeEmpty => f.write_str("Btree is empty!"),
            Self::Other(st) => f.write_str(st),
        }
    }
}

pub type BtreeResult<T> = Result<T, BtreeError>;

impl Error for BtreeError {}

impl From<IoError> for BtreeError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<TupleError> for BtreeError {
    fn from(value: TupleError) -> Self {
        Self::TupleReadError(value)
    }
}

pub(crate) struct Btree<Acc>
where
    Acc: TreeReader,
{
    pub(crate) root: PageId,
    pub(crate) pager: SharedPager,
    pub(crate) min_keys: usize,
    pub(crate) num_siblings_per_side: usize,
    accessor: Option<Acc>, // Lazy initialization for the accessor.
}

impl<Acc> Btree<Acc>
where
    Acc: TreeReader,
{
    /// Creates a [Btree] data structure from an existing root page.
    ///
    /// The root page must have already been allocated in advance.
    ///
    /// A runtime check is applied to validate that the value of the [MIN_KEYS] parameter is valid.
    ///
    /// This is because if we allow to have less than three keys on a single page, when we split this page we wont be able to get any good.
    pub(crate) fn new(
        root: PageId,
        pager: SharedPager,
        min_keys: usize,
        num_siblings_per_side: usize,
    ) -> Self {
        debug_assert!(
            min_keys >= MINIMUM_KEYS_PER_PAGE,
            "Invalid argument. Minimum allowed keys is {MINIMUM_KEYS_PER_PAGE}"
        );
        Self {
            root,
            pager,
            min_keys,
            num_siblings_per_side,

            accessor: None,
        }
    }

    pub(crate) fn with_accessor(mut self, accessor: Acc) -> Self {
        self.accessor = Some(accessor);
        self
    }

    pub(crate) fn root_pos(&self) -> BtreePagePosition {
        Position::start_pos(self.root)
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.accessor.is_some()
    }

    fn is_root(&self, id: PageId) -> bool {
        self.root == id
    }

    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root
    }

    pub(crate) fn get_root(&self) -> PageId {
        self.root
    }

    pub(crate) fn get_pager(&self) -> &SharedPager {
        &self.pager
    }

    /// Get a shared reference to the accessor.
    pub(crate) fn accessor(&mut self) -> BtreeResult<&Acc> {
        self.accessor.as_ref().ok_or(BtreeError::BtreeUnintialized)
    }

    /// Get a mutable reference to the accessor.
    pub(crate) fn accessor_mut(&mut self) -> BtreeResult<&mut Acc> {
        self.accessor.as_mut().ok_or(BtreeError::BtreeUnintialized)
    }

    pub(crate) fn cloned_shared(&self) -> Btree<BtreeReadAccessor> {
        Btree::new(
            self.get_root(),
            self.get_pager().clone(),
            self.min_keys,
            self.num_siblings_per_side,
        )
        .with_accessor(BtreeReadAccessor::new())
    }

    pub(crate) fn cloned_mut(&self) -> Btree<BtreeWriteAccessor> {
        Btree::new(
            self.get_root(),
            self.get_pager().clone(),
            self.min_keys,
            self.num_siblings_per_side,
        )
        .with_accessor(BtreeWriteAccessor::new())
    }
}

impl<Acc> Btree<Acc>
where
    Acc: TreeReader,
{
    /// Acquires latches on btreepages with the given accessor
    pub(crate) fn acquire_with_accessor(&mut self, id: PageId) -> BtreeResult<()> {
        // Page zero is a special page.
        if id == 0 {
            return Ok(());
        };

        if self
            .accessor
            .as_ref()
            .ok_or(BtreeError::BtreeUnintialized)?
            .contains(id)
        {
            return Ok(());
        }

        let frame = self.pager.write().read_page::<BtreePage>(id)?;
        self.accessor
            .as_mut()
            .ok_or(BtreeError::BtreeUnintialized)?
            .try_acquire(frame)?;
        Ok(())
    }

    /// Gets a shared reference to a page from the stack.
    pub(crate) fn get_page(&mut self, id: PageId) -> BtreeResult<&BtreePage> {
        self.acquire_with_accessor(id)?;
        self.accessor()?
            .get(id)
            .map(|latch| &**latch)
            .ok_or_else(|| BtreeError::BtreePageNotFound(id))
    }

    pub(crate) fn is_empty(&mut self) -> BtreeResult<bool> {
        let root = self.get_root();
        Ok(self.get_page(root)?.num_slots() == 0)
    }

    /// Traverses the whole tree from the root to the leaf nodes.
    ///
    /// Assumes the entry contains the serialized list of keys.
    pub(crate) fn search(&mut self, entry: &[u8], schema: &Schema) -> BtreeResult<SearchResult> {
        self.page_search(self.get_root(), entry, 0, schema)
    }

    /// Traverses the whole tree from the root to the leaf nodes.
    ///
    /// Assumes the entry contains the serialized list of keys.
    pub(crate) fn search_tuple(
        &mut self,
        entry: &Tuple,
        schema: &Schema,
    ) -> BtreeResult<SearchResult> {
        let target_cursor = Tuple::keys_offset(schema.num_values());
        self.page_search(
            self.get_root(),
            entry.effective_data(),
            target_cursor,
            schema,
        )
    }

    /// Searches for a specific key on a page (mutably borrowing)
    pub(crate) fn page_search(
        &mut self,
        page_id: PageId, // Last tracked position
        entry: &[u8],
        target_cursor: usize,
        schema: &Schema,
    ) -> BtreeResult<SearchResult> {
        // Get the start page and allocate a new accessor to start the search.

        let is_leaf = self.get_page(page_id)?.is_leaf();

        if is_leaf {
            // If the page is a leaf we have reached the end of the tree, therefore we perform binary search inside the page.
            return self.binary_search_key(page_id, entry, target_cursor, schema);
        }

        // We are on an interior node, therefore we look for the most suitable child to continue traversing downwards.

        let child = self.find_child(page_id, entry, target_cursor, schema)?;

        match child {
            SearchResult::NotFound(position) | SearchResult::Found(position) => {
                // Track the position.
                let accessor = self.accessor_mut()?;
                accessor.push_position(Position::new(page_id, position.slot()));
                self.page_search(position.entry(), entry, target_cursor, schema)
            }
        }
    }

    /// Binary searches over the slots of a Leaf Page in the btree.
    ///
    /// # Returns:
    ///
    ///  - [SearchResult::Found] if it finds the key it is looking for. The position contains the actual found position.
    ///
    /// - [SearchResult::NotFound] if it does not find the key it is looking for.  The position will include the last slot index in the page.
    fn binary_search_page(
        page: &BtreePage,
        comparator: CellComparator,
        search_key: &[u8],
        target_cursor: usize,
    ) -> BtreeResult<SearchResult> {
        let mut slot_count = page.num_slots();
        let last_slot = slot_count;
        let mut left = 0;
        let mut right = slot_count;

        while left < right {
            let mid = left + slot_count / 2;

            let cell = page.cell(mid);

            match comparator.compare_cell_payload(search_key, cell, target_cursor)? {
                Ordering::Equal => {
                    return Ok(SearchResult::Found(Position::new(page.id(), mid)));
                }
                Ordering::Greater => left = mid + 1,
                Ordering::Less => right = mid,
            }

            slot_count = right - left;
        }

        Ok(SearchResult::NotFound(Position::new(page.id(), last_slot)))
    }

    /// Finds the most suitable child to store a particular entry at a given page.
    fn find_child_on_page(
        page: &BtreePage,
        comparator: CellComparator,
        search_key: &[u8],
        target_cursor: usize,
    ) -> BtreeResult<SearchResult> {
        let num_slots = page.num_slots();
        let right_child = page.right_child().unwrap_or(PAGE_ZERO);

        let mut result = SearchResult::NotFound(Position::new(right_child, num_slots));

        for i in 0..num_slots {
            let cell = page.cell(i);
            // Once we find a suitable child, stop the search.
            if matches!(
                comparator.compare_cell_payload(search_key, cell, target_cursor)?,
                Ordering::Less
            ) {
                // Note that left child must ever be None for interior pages, so as no one
                result = SearchResult::Found(Position::new(
                    cell.metadata().left_child().unwrap_or(PAGE_ZERO),
                    i,
                ));
                break;
            };
        }

        Ok(result)
    }

    /// Look for the most suitable child on a page given its page_id (shared access)
    fn find_child(
        &mut self,
        page_id: PageId,
        search_key: &[u8],
        target_cursor: usize,
        schema: &Schema,
    ) -> BtreeResult<SearchResult> {
        let comparator = CellComparator::new(schema, self.pager.clone());
        let page = self.get_page(page_id)?;
        Self::find_child_on_page(page, comparator, search_key, target_cursor)
    }

    /// Binary searches over the slots of a Leaf Page in the btree (inmutable access version).
    fn binary_search_key(
        &mut self,
        page_id: PageId,
        search_key: &[u8],
        target_cursor: usize,
        schema: &Schema,
    ) -> BtreeResult<SearchResult> {
        let comparator = CellComparator::new(schema, self.pager.clone());
        let page = self.get_page(page_id)?;
        Self::binary_search_page(page, comparator, search_key, target_cursor)
    }

    /// Finds the most suitable slot to insert a particular entry on a given page.
    fn find_slot(
        &mut self,
        page_id: PageId,
        search_key: &[u8],
        target_cursor: usize,
        schema: &Schema,
    ) -> BtreeResult<SearchResult> {
        let comparator = CellComparator::new(schema, self.pager.clone());
        let page = self.get_page(page_id)?;
        let num_slots = page.num_slots();

        if num_slots == 0 {
            return Ok(SearchResult::NotFound(Position::start_pos(page_id)));
        }

        let mut result = SearchResult::NotFound(Position::new(page_id, num_slots));
        // let mut existing = false;
        for i in 0..num_slots {
            let cell = page.cell(i);

            match comparator.compare_cell_payload(search_key, cell, target_cursor)? {
                Ordering::Less => {
                    result = SearchResult::Found(Position::new(page_id, i));
                    break;
                }
                Ordering::Equal => {
                    result = SearchResult::Found(Position::new(page_id, i));
                    //  existing = true;

                    break;
                }
                _ => {}
            }
        }

        Ok(result)
    }

    /// Obtains the right most position in the tree.
    fn get_right_most(&mut self) -> BtreeResult<BtreePagePosition> {
        let mut current = self.get_root();
        loop {
            let right_child = self.get_page(current)?.right_child();
            let num_slots = self.get_page(current)?.num_slots();
            self.accessor_mut()?.release(current);

            // If right child is not set it means we have reached up a leaf node.
            if let Some(child) = right_child {
                current = child;
            } else {
                return Ok(Position::new(current, num_slots - 1));
            };
        }
    }

    /// Obtains the right most position in the tree.
    fn get_left_most(&mut self) -> BtreeResult<BtreePagePosition> {
        if self.is_empty()? {
            return Err(BtreeError::BtreeEmpty);
        };

        let mut current = self.get_root();

        loop {
            let left_child = self.get_page(current)?.cell(0).left_child();

            let is_leaf = self.get_page(current)?.is_leaf();
            self.accessor_mut()?.release(current);

            // If left child is not set it means we have reached up a leaf node.
            if let Some(child) = left_child {
                current = child;
            } else {
                return Ok(Position::new(current, 0));
            };
        }
    }

    /// Utility to get a row at a specific position in the tree.
    pub(crate) fn get_row_at(
        &mut self,
        pos: BtreePagePosition,
        schema: &Schema,
        snapshot: &Snapshot,
    ) -> BtreeResult<Option<Row>> {
        self.with_cell_at(pos, |bytes| {
            let reader = TupleReader::from_schema(&schema);

            if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                let tuple = TupleRef::new(bytes, layout);
                let row = tuple.to_row_with(&schema)?;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        })?
    }

    /// Utility to get a row at a specific position in the tree.
    pub(crate) fn get_tuple_at(
        &mut self,
        pos: BtreePagePosition,
        schema: &Schema,
        snapshot: &Snapshot,
    ) -> BtreeResult<Option<Tuple>> {
        self.with_cell_at(pos, |bytes| {
            let reader = TupleReader::from_schema(&schema);

            if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                let tuple = Tuple::from_slice_unchecked(bytes)?;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        })?
    }



    /// Utility to get a tuple at a specific position in the tree without a visibility check
    pub(crate) fn get_tuple_at_unchecked(
        &mut self,
        pos: BtreePagePosition,
        schema: &Schema,
    ) -> BtreeResult<Tuple> {
        self.with_cell_at(pos, |bytes| {


                let tuple = Tuple::from_slice_unchecked(bytes)?;
                Ok(tuple)

        })?
    }

    /// Executes a callback on the cell at the given position.
    /// For overflow cells, reassembles the full payload first.
    /// For normal cells, passes the payload directly to avoid copying.
    pub(crate) fn with_cell_at<F, R>(&mut self, pos: BtreePagePosition, f: F) -> BtreeResult<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let page_id = pos.entry();
        let slot = pos.slot();
        let mut reassembler = Reassembler::new(self.get_pager().clone());
        let page = self.get_page(page_id)?;
        let cell = page.cell(slot);

        let result = if cell.metadata().is_overflow() {
            reassembler.reassemble(cell)?;
            f(reassembler.into_boxed_slice().as_ref())
        } else {
            f(cell.effective_data())
        };

        Ok(result)
    }

    /// Batch process multiple positions, grouping by page for efficiency.
    pub(crate) fn with_cells_at<F, R>(
        &mut self,
        positions: &[BtreePagePosition],
        mut f: F,
    ) -> BtreeResult<Vec<R>>
    where
        F: FnMut(BtreePagePosition, &[u8]) -> R,
    {
        let mut grouped: BTreeMap<PageId, Vec<(usize, usize)>> = BTreeMap::new();

        for (idx, pos) in positions.iter().enumerate() {
            grouped
                .entry(pos.entry())
                .or_default()
                .push((idx, pos.slot()));
        }

        let mut indexed_results: Vec<(usize, R)> = Vec::with_capacity(positions.len());

        for (page_id, slots) in grouped {
            let mut reassembler = Reassembler::new(self.get_pager().clone());
            let page = self.get_page(page_id)?;

            for (original_idx, slot) in slots {
                let pos = BtreePagePosition::new(page_id, slot);
                let cell = page.cell(slot);

                let result = if cell.metadata().is_overflow() {
                    reassembler.reassemble(cell)?;
                    f(pos, reassembler.clone().into_boxed_slice().as_ref())
                } else {
                    f(pos, cell.effective_data())
                };

                indexed_results.push((original_idx, result));
            }
        }

        indexed_results.sort_by_key(|(idx, _)| *idx);
        Ok(indexed_results.into_iter().map(|(_, r)| r).collect())
    }

    /// Computes the tree height by traversing from root to leaf.
    pub(crate) fn height(&mut self) -> BtreeResult<usize> {
        if self.is_empty()? {
            return Ok(0);
        };

        let mut height = 1usize;
        let mut current = self.get_root();
        loop {
            let right_child = self.get_page(current)?.right_child();
            self.accessor_mut()?.release(current);

            // If right child is not set it means we have reached up a leaf node.
            if let Some(child) = right_child {
                height += 1;
                current = child;
            } else {
                return Ok(height);
            };
        }
    }

    pub(crate) fn iter_forward(&mut self) -> BtreeResult<BtreePositionalIterator>
    where
        Acc: Default,
    {
        if self.is_empty()? {
            return Err(BtreeError::BtreeEmpty);
        };

        let left_most_position = self.get_left_most()?;
        BtreePositionalIterator::from_position(
            self.cloned_shared(),
            left_most_position.entry(),
            left_most_position.slot() as isize,
            IterDirection::Forward,
        )
    }

    pub(crate) fn into_iter_backward(&mut self) -> BtreeResult<BtreePositionalIterator>
    where
        Acc: Default,
    {
        if self.is_empty()? {
            return Err(BtreeError::BtreeEmpty);
        };

        let right_most_position = self.get_right_most()?;
        BtreePositionalIterator::from_position(
            self.cloned_shared(),
            right_most_position.entry(),
            right_most_position.slot() as isize,
            IterDirection::Forward,
        )
    }

    /// Prints the string to stdout
    pub(crate) fn to_string(&self, schema: &Schema, snapshot: &Snapshot) -> BtreeResult<String>
    where
        Acc: Default,
    {
        let printer = BtreePrinter::new(self.cloned_shared(), schema, snapshot);
        printer.as_json()
    }

    /// Prints the string to a file.
    pub(crate) fn export_to(
        &self,
        path: impl AsRef<Path>,
        schema: &Schema,
        snapshot: &Snapshot,
    ) -> BtreeResult<()>
    where
        Acc: Default,
    {
        let path = path.as_ref();

        // Verify that the parent dir exists.
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            return Err(BtreeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("directory {:?} does not exist", parent),
            )));
        }

        fs::write(path, self.to_string(schema, snapshot)?)?;

        Ok(())
    }
}

enum BorrowDirection {
    RightToLeft,
    LeftToRight,
}

/// Mutable / Write only accessor.
impl<Acc> Btree<Acc>
where
    Acc: TreeWriter,
{
    /// Gets a mutable reference from a page
    pub(crate) fn get_page_mut(&mut self, id: PageId) -> BtreeResult<&mut BtreePage> {
        self.acquire_with_accessor(id)?;
        self.accessor_mut()?
            .get_mut(id)
            .map(|latch| &mut **latch)
            .ok_or_else(|| BtreeError::BtreePageNotFound(id))
    }

    /// Inserts a key at the corresponding position in the Btree.
    /// Returns an [BtreeError] if the key already exists.
    pub(crate) fn insert(
        &mut self,
        page_id: PageId,
        data: Tuple,
        schema: &Schema,
    ) -> BtreeResult<()> {
        let target_cursor = Tuple::keys_offset(schema.num_values());
        let search_result = self.page_search(
            self.get_root(),
            &data.effective_data(),
            target_cursor,
            schema,
        )?;

        match search_result {
            SearchResult::Found(pos) => Err(IoError::new(
                ErrorKind::AlreadyExists,
                format!("The key already exists  at position {pos}",),
            )),

            SearchResult::NotFound(pos) => {
                let page_id = pos.entry();
                self.insert_cell(page_id, data, target_cursor, schema)?;
                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.accessor_mut()?.clear();

        Ok(())
    }

    /// Inserts a cell at the corresponding slot on a given page.
    /// Uses the [find_slot] function in order to find the best fit slot for the cell given its key.
    /// Assumes the page is a leaf page.
    /// This method must not be called on interior nodes.
    pub(crate) fn insert_cell(
        &mut self,
        page_id: PageId,
        data: Tuple,
        target_cursor: usize,
        schema: &Schema,
    ) -> BtreeResult<()> {
        let max_cell_storage_size = {
            let page = self.get_page(page_id)?;
            std::cmp::min(page.max_allowed_payload_size(), page.free_space() as u16) as usize
        };

        let builder = CellBuilder::new(max_cell_storage_size, self.min_keys, self.pager.clone());

        let key = &data.effective_data();
        // Find the corresponding slot in the leaf page.
        let slot_result = self.find_slot(page_id, key, target_cursor, schema)?;

        let cell = builder.build_cell(data, schema)?;
        let page = self.get_page_mut(page_id)?;
        match slot_result {
            // Slot not found so we just append the cell at the end of the page.
            SearchResult::NotFound(_) => {
                page.push(cell)?;
            }
            // Best fit slot is found between two existing slots.
            SearchResult::Found(pos) => {
                let slot = pos.slot();
                page.insert(slot, cell)?;
            }
        };

        // Check the current page status and call balance if required.
        self.balance(page_id)?;
        Ok(())
    }

    /// Updates the cell content on a given leaf page at the given [Position].
    /// Assumes the position pointer is valid and the page is a leaf page.
    fn update_cell(
        &mut self,
        pos: BtreePagePosition,
        data: Tuple,
        schema: &Schema,
    ) -> BtreeResult<()> {
        let page_id = pos.entry();
        let slot = pos.slot();

        let max_cell_storage_size = {
            let page = self.get_page_mut(page_id)?;
            std::cmp::min(page.max_allowed_payload_size(), page.free_space() as u16) as usize
        };

        let builder = CellBuilder::new(max_cell_storage_size, self.min_keys, self.pager.clone());
        let cell = builder.build_cell(data, schema)?;
        let page = self.get_page_mut(page_id)?;
        let old_cell = page.replace(slot, cell)?;

        let deallocator = CellDeallocator::new(self.pager.clone());
        // Deallocate the cell
        deallocator.deallocate_cell(old_cell)?;

        self.balance(page_id)?;

        Ok(())
    }

    /// Inserts a key at the corresponding position in the Btree.
    /// Updates its contents if it already exists.
    pub(crate) fn upsert(
        &mut self,
        page_id: PageId,
        data: Tuple,
        schema: &Schema,
    ) -> BtreeResult<()> {
        let target_cursor = Tuple::keys_offset(schema.num_values());
        let search_result = self.page_search(
            self.get_root(),
            &data.effective_data(),
            target_cursor,
            schema,
        )?;

        match search_result {
            SearchResult::Found(pos) => self.update_cell(pos, data, schema)?, // This logic is similar to [self.insert]
            SearchResult::NotFound(pos) => {
                let page_id = pos.entry();
                self.insert_cell(page_id, data, target_cursor, schema)?;
            }
        };

        // Cleanup traversal here.
        self.accessor_mut()?.clear();
        Ok(())
    }

    /// Updates a key at the corresponding position in the Btree.
    /// Returns an [IoError] if the key is not found.
    pub(crate) fn update(
        &mut self,
        page_id: PageId,
        data: Tuple,
        schema: &Schema,
    ) -> BtreeResult<()> {
        let target_cursor = Tuple::keys_offset(schema.num_values());
        let search_result = self.page_search(
            self.get_root(),
            &data.effective_data(),
            target_cursor,
            schema,
        )?;

        match search_result {
            SearchResult::Found(pos) => self.update_cell(pos, data, schema),
            SearchResult::NotFound(_) => Err(BtreeError::NonExistentKey),
        }?;

        // Cleanup traversal here.
        self.accessor_mut()?.clear();
        Ok(())
    }

    /// Removes the entry corresponding to the given key if it exists.
    /// Returns an [BtreeError] if the key is not found.
    pub(crate) fn remove(
        &mut self,
        page_id: PageId,
        key: &[u8],
        schema: &Schema,
    ) -> BtreeResult<()> {
        let start_pos: BtreePagePosition = Position::start_pos(page_id);
        let search = self.search(key, schema)?;

        match search {
            SearchResult::NotFound(_) => Err(BtreeError::NonExistentKey),
            SearchResult::Found(pos) => {
                let page_id = pos.entry();
                let slot = pos.slot();
                let page = self.get_page_mut(page_id)?;
                let cell = page.remove(slot)?;

                let deallocator = CellDeallocator::new(self.pager.clone());
                deallocator.deallocate_cell(cell)?;

                self.balance(page_id)?;

                Ok(())
            }
        }?;

        // Cleanup traversal here.
        self.accessor_mut()?.clear();
        Ok(())
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
    fn balance_shallower(&mut self) -> BtreeResult<()> {
        let is_empty = self.get_page_mut(self.root)?.is_empty();
        let is_leaf = self.get_page_mut(self.root)?.is_leaf();

        // Check if the root has actually underflown.
        if !is_empty || is_leaf {
            return Ok(());
        };

        // Safe to unwrap since we already checked that we are not at a leaf node.
        let right_child_id = self.get_page_mut(self.root)?.right_child().unwrap();
        let child_page = self.get_page_mut(right_child_id)?;

        // Get the right child of the child (can be None).
        let optional_grand_child_id = child_page.right_child();
        let cells = child_page.drain(..).collect::<Vec<OwnedCell>>();

        // Release the child's latch and deallocate.
        self.accessor_mut()?.release(right_child_id);
        self.pager
            .write()
            .dealloc_page::<BtreePage>(right_child_id)?;

        let root_page = self.get_page_mut(self.root)?;
        // Refill the old root (we have checked at the beginning that it was empty)
        for cell in cells {
            root_page.push(cell)?;
        }

        // Re-set the root to point to the old grand child , a we have removed the node in the middle.
        root_page.set_right_child(optional_grand_child_id);

        Ok(())
    }

    /// Splits a mutable slice of cells into two parts, assuming they are sorted by key.
    /// Useful for cell redistribution.
    fn split_cells(mut cells: Vec<OwnedCell>) -> (Vec<OwnedCell>, Vec<OwnedCell>) {
        // Assumes cells are sorted
        let sizes: Vec<usize> = cells.iter().map(|cell| cell.total_size()).collect();
        let total_size: usize = sizes.iter().sum();
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
        let right = cells.split_off(split_index);
        let left = cells;

        (left, right)
    }

    /// Balance deeper algorithm inspired by SQLite [balance deeper].
    /// Balance deeper is the opposite of balance shallower in the sense that it will allocate two new nodes when the root becomes full, increasing the depth of the tree by one.
    /// The cell in the middle gets propagated upwards.
    /// We also need to fix the pointers in the frontiers between the two new nodes to maintain the leaf node's linked list.
    fn balance_deeper(&mut self) -> BtreeResult<()> {
        let page_size = self.pager.write().page_size();
        // Allocate new left and right childs
        let new_left = self.pager.write().allocate_page::<BtreePage>()?;
        let new_right = self.pager.write().allocate_page::<BtreePage>()?;

        // Obtain root page metadata

        let optional_old_right_id = self.get_page_mut(self.root)?.right_child();
        let was_leaf = self.get_page_mut(self.root)?.is_leaf();

        // drain the root node.
        let cells = self
            .get_page_mut(self.root)?
            .drain(..)
            .collect::<Vec<OwnedCell>>();

        // Split the cell's array.
        let (mut left_cells, right_cells) = Self::split_cells(cells);

        // Choose the appropiate cell to propagate based on the type of page.
        // We need to copy  because we also need to insert it on the new leaves (this is a Bplustree, so the data must reside on the leaf pages.)
        let mut propagated_cell = if was_leaf {
            right_cells.first().unwrap().clone()
        } else {
            left_cells.last().unwrap().clone()
        };
        propagated_cell.set_left_child(Some(new_left));

        {
            let current_root_page = self.get_page_mut(self.root)?;
            // Now we move part of the cells to one node and the other part to the other node.
            current_root_page.set_right_child(Some(new_right));
            current_root_page.push(propagated_cell)?;
        }

        // In case we were splitting an interior node we need to account that the last cell must be used as the right child of the new allocated pages.

        let (cells_for_left, right_child) = if was_leaf {
            (left_cells, None)
        } else if let Some(last) = left_cells.pop() {
            (left_cells, last.left_child())
        } else {
            (left_cells, None)
        };

        {
            let left_page = self.get_page_mut(new_left)?;

            // Push the owned cells to the left page.
            for cell in cells_for_left {
                left_page.push(cell)?;
            }

            left_page.set_right_child(right_child);
            left_page.set_next_sibling(Some(new_right));
            left_page.set_prev_sibling(None);
        }

        {
            let right_page = self.get_page_mut(new_right)?;
            // Now do the same with the other page.
            for cell in right_cells {
                right_page.push(cell)?;
            }

            right_page.set_prev_sibling(Some(new_left));
            right_page.set_next_sibling(None);
            right_page.set_right_child(optional_old_right_id);
        }

        // Fix the frontier links.
        // The right child of the left page must be linked with the left most child of the right page to maintain proper ordering.
        let left_right_most = self.get_page_mut(new_left)?.right_child();
        let right_left_most = self.get_page_mut(new_right)?.cell(0).left_child();

        if let Some(page_id) = left_right_most {
            let page = self.get_page_mut(page_id)?;
            page.set_next_sibling(right_left_most);
        };

        if let Some(page_id) = right_left_most {
            let page = self.get_page_mut(page_id)?;
            page.set_prev_sibling(left_right_most);
        };

        Ok(())
    }

    // Computes the best cell distribution given a list of cells.
    //
    // The cell distribution computation is part of the rebalance algorithm.
    //
    // It is done once before the actual redistribution in order to know in advance where do we have to put cells on.
    // This way cells are moved only once in memory
    fn compute_best_cell_distribution(
        cells: &VecDeque<OwnedCell>,
        page_size: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        // Obtain the maximum data that we can fit on this page.
        let usable_space = BtreePage::overflow_threshold(page_size);

        // These two arrays track the redistribution computation of the rebalancing algorithm.
        let mut total_size_in_each_page = vec![0];
        let mut number_of_cells_per_page = vec![0];

        // We keep pushing cells to the bucket until we exhaust it.
        // Once done, we create a new bucket an advance the cursor.
        for cell in cells {
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
                while total_size_in_each_page[i] < BtreePage::underflow_threshold(page_size) {
                    number_of_cells_per_page[i] += 1;
                    total_size_in_each_page[i] += &cells[divider_cell].storage_size();

                    number_of_cells_per_page[i - 1] -= 1;
                    total_size_in_each_page[i - 1] -= &cells[divider_cell - 1].storage_size();
                    divider_cell -= 1;
                }
            }

            // Second page has more data than the first one, make a little
            // adjustment to keep it left biased.
            if total_size_in_each_page[0] < BtreePage::underflow_threshold(page_size) {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
            };
        };

        (total_size_in_each_page, number_of_cells_per_page)
    }

    /// Full balance algorithm.
    /// The description of this algorithm can be found on original SQLite docs: https://sqlite.org/btreemodule.html
    /// The main idea is that you have to take as many pages as specified, recompute a balanced distribution, and redistribute the cells accordingly.
    /// Afterwards, propagate the pointers as required.
    fn balance(&mut self, page_id: PageId) -> BtreeResult<()> {
        let is_root = self.is_root(page_id);
        // Read the page size from the pager.
        let page_size = self.pager.write().page_size();
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

        // Pop the last visited position from the stack.
        // During balance, we propagate upwards, therefore we use the stack in the opposite way as we do when we traverse down, popping as we call it on each node and using the popped position to identify where we are at in the tree.
        let parent_position = self
            .accessor_mut()?
            .pop_position()
            .ok_or(BtreeError::TraversalEmpty)?;

        // Internal/Leaf node Overflow/Underflow.
        self.balance_siblings(page_id, &parent_position)?;

        // No need to fuck anything else up.
        if matches!(self.check_node_status(page_id)?, NodeStatus::Balanced) {
            return Ok(());
        };

        let parent_page_id = parent_position.entry();
        let (parent_prev, last_parent_slot, parent_next) = {
            let parent_page = self.get_page_mut(parent_page_id)?;

            // Get metadata about the parent
            let parent_prev = parent_page.prev_sibling();
            let last_parent_slot = parent_page.num_slots();
            let parent_next = parent_page.next_sibling();
            (parent_prev, last_parent_slot, parent_next)
        };

        // Check if we are about to rebalance a leaf node (we will need it later).
        let is_leaf = self.get_page_mut(page_id)?.is_leaf();

        // Obtain the right most child of the parent's left sibling (right most cousin)
        let parent_prev_sibling_last = if let Some(prev_id) = parent_prev {
            let parent_child_page = self.get_page_mut(prev_id)?;
            parent_child_page
                .right_child()
                .map(|id| BtreePagePosition::new(id, parent_child_page.num_slots()))
        } else {
            None
        };

        // Obtain the first child of the parent's next sibling (left most cousin)
        let parent_next_sibling_first = if let Some(next_id) = parent_next {
            let parent_child_page = self.get_page_mut(next_id)?;
            parent_child_page
                .cell(0)
                .left_child()
                .map(|id| BtreePagePosition::new(id, 0))
        } else {
            None
        };

        // Load siblings
        let mut siblings =
            self.load_siblings(page_id, &parent_position, self.num_siblings_per_side)?;

        // Load the frontiers in case it is necessary.
        let last_loaded_sibling_slot = siblings.iter().last().unwrap().slot();
        let first_loaded_sibling_slot = siblings.front().unwrap().slot();

        // Load the left and right frontiers from the parent page
        let (left_frontier_in_parent, right_frontier_in_parent) = {
            let parent_page = self.get_page(parent_page_id)?;

            // If the first loaded sibling is > 0, this means there are other siblings in the parent at the left of the chain that we have not loaded. We load the last one in order to re-link with the rest of the siblings later.
            let left = (first_loaded_sibling_slot > 0)
                .then(|| first_loaded_sibling_slot - 1)
                .and_then(|child_slot| {
                    parent_page
                        .child(child_slot)
                        .map(|id| BtreePagePosition::new(id, child_slot))
                });

            // If the last loaded sibling is < last_parent_slot, this means there are other siblings in the parent at the left of the chain that we have not loaded. We load the last one in order to re-link with the rest of the siblings later.
            let right = (last_loaded_sibling_slot < last_parent_slot)
                .then(|| last_loaded_sibling_slot + 1)
                .and_then(|child_slot| {
                    parent_page
                        .child(child_slot)
                        .map(|id| BtreePagePosition::new(id, child_slot))
                });
            (left, right)
        };

        let mut cells = VecDeque::new();

        // As we remove cells from the parent, slots shrink towards the left.
        // Therefore we need to always remove the first slot in order to clean up the parent node.
        let slot_to_remove = siblings[0].slot();

        // Make copies of cells in order.
        for (i, sibling) in siblings.iter().enumerate() {
            // As always, when visiting sibling nodes we need to remember to acquire their corresponding latches, as they might have not been visited before.
            let sibling_page_id = sibling.entry();

            let page = self.get_page_mut(sibling_page_id)?;
            cells.extend(page.drain(..));

            // Extract this node's cells and return the right child.
            let right_child = page.right_child();

            // If we are not the last sibling we can safely link with the next in the chain.
            // If we are the last sibling we use the right frontier.
            // If there is no right frontier we are the parent's right most and there is no [next].
            let next_sibling = siblings
                .get(i + 1)
                .map(|s| s.entry())
                .or(right_frontier_in_parent.map(|f| f.entry()));

            // If the right child is different from None and the next sibling too,
            // extract the first cell of the first child on our next sibling and allocate a new cell.
            // This always happens on interior nodes rebalancing, as right_child is not set on leaf nodes.
            if let Some(right_most_child_id) = right_child
                && let Some(next_id) = next_sibling
            {
                let next_page = self.get_page_mut(next_id)?;

                // Obtain the first child
                if let Some(first_child_id) = next_page.child(0) {
                    let child_page = self.get_page_mut(first_child_id)?;
                    let mut cell = child_page.owned_cell(0);

                    // Make the copied cell point to our right child and push it to the chain.
                    cell.set_left_child(Some(right_most_child_id));
                    cells.push_back(cell);
                };
            };

            let parent_page = self.get_page_mut(parent_page_id)?;
            // Remove our entry from the parent node.
            if slot_to_remove < parent_page.num_slots() {
                parent_page.remove(slot_to_remove)?;
            };
        }

        // Take the last right child of the siblings chain.
        // For interior pages this is required in order to unfuck the last pointer.
        let old_right_child = siblings.iter().last().and_then(|last| {
            let page_id = last.entry();
            let page = self.get_page_mut(page_id).ok()?;
            page.right_child()
        });

        let (total_size_in_each_page, number_of_cells_per_page) =
            Self::compute_best_cell_distribution(&cells, page_size);

        // Allocate missing pages
        while siblings.len() < number_of_cells_per_page.len() {
            // New allocated pages are added at the end.
            let new_parent_index = siblings.iter().last().unwrap().slot() + 1;
            let new_page = self.pager.write().allocate_page::<BtreePage>()?;
            siblings.push_back(Position::new(new_page, new_parent_index));
        }

        // Free unused pages.
        while number_of_cells_per_page.len() < siblings.len() {
            let deallocated_sibling = siblings.pop_back().unwrap();
            let deallocated_page_id = deallocated_sibling.entry();
            self.accessor_mut()?.release(deallocated_page_id);
            self.pager
                .write()
                .dealloc_page::<BtreePage>(deallocated_page_id)?;
        }

        // Put pages in ascending order to favor sequential IO where possible.
        BinaryHeap::from_iter(siblings.iter().map(|s| Reverse(s.entry())))
            .iter()
            .enumerate()
            .for_each(|(i, Reverse(page))| siblings[i].set_entry(*page));

        // Fix the last child pointer.
        let last_sibling_id = siblings[siblings.len() - 1].entry();
        let last_sibling_page = self.get_page_mut(last_sibling_id)?;
        last_sibling_page.set_right_child(old_right_child);

        // If there is no right frontier at this point, it means the last sibling in the chain is also the parent's right most.
        if right_frontier_in_parent.is_none() {
            let parent_page = self.get_page_mut(parent_page_id)?;
            // Fix pointers in the parent in case we have allocated new pages.
            parent_page.set_right_child(Some(last_sibling_id));
        };

        // Obtain the global frontiers considering that if we get out of the bounds of the parent node we need to grab the frontiers from its siblings.
        let global_left_frontier = left_frontier_in_parent.or(parent_prev_sibling_last);
        let global_right_frontier = right_frontier_in_parent.or(parent_next_sibling_first);

        // Begin redistribution.
        for (i, n) in number_of_cells_per_page.iter().enumerate() {
            let current_iter_id = siblings[i].entry();
            let current_iter_slot = siblings[i].slot();

            let current_iter_page = self.get_page_mut(current_iter_id)?;

            // Push all the cells to the child.
            let num_cells = if is_leaf {
                *n
            } else {
                *n - 1 // For interior nodes we reserve the last cell. Which we will propagate to the parent. In leaf nodes we cannot do this since there is where data resides.
            };

            for _ in 0..num_cells {
                current_iter_page.push(cells.pop_front().unwrap())?;
            }

            // Get the previous sibling based on the value of [i]
            let prev_sibling = if i > 0 {
                Some(siblings[i - 1].entry())
            } else {
                global_left_frontier.map(|p| p.entry())
            };

            // Set the previous sibling
            current_iter_page.set_prev_sibling(prev_sibling);

            // Set the next sibling
            let next_sibling = if i + 1 < siblings.len() {
                Some(siblings[i + 1].entry())
            } else {
                global_right_frontier.map(|p| p.entry())
            };

            current_iter_page.set_next_sibling(next_sibling);

            // For leaf nodes, we always propagate the first cell of the next node.
            //
            // Note that the last node's  next is the right frontier, which gets propagated afterwards.
            if i < siblings.len() - 1 && is_leaf {
                // Simply create a copy of the next node's front cell, make it point towards ourselves and propagate.
                let mut divider = cells.front().unwrap().clone();
                current_iter_page.set_right_child(divider.left_child());
                divider.set_left_child(Some(current_iter_id));
                let parent_page = self.get_page_mut(parent_page_id)?;
                parent_page.insert(current_iter_slot, divider)?;

            // For interior nodes there are two possible cases.
            // CASE A: We are not the right most of the parent.
            // On that case we simply propagate our last cell and also use it as our right most.
            } else if !is_leaf && (i < siblings.len() - 1 || right_frontier_in_parent.is_some()) {
                let mut divider = cells.pop_front().unwrap();
                current_iter_page.set_right_child(divider.left_child());
                divider.set_left_child(Some(current_iter_id));
                let parent_page = self.get_page_mut(parent_page_id)?;
                parent_page.insert(current_iter_slot, divider)?;

            // CASE B: We are the right most of the parent,
            // Then there is no cell to propagate upwards, so we push an additional cell to ourselves.
            } else if !is_leaf {
                current_iter_page.push(cells.pop_front().unwrap())?;
            };
        }

        // Fix the right frontier in the parent.
        // For leaf nodes, we propagate the first child as a divider, as we still have not done it in the redistribute section (see upwards).
        //
        // For interior nodes this is not necessary as we use the last child of the previous sibling instead.
        if is_leaf && let Some(frontier) = right_frontier_in_parent {
            let frontier_id = frontier.entry();
            let page = self.get_page_mut(frontier_id)?;
            let last_sibling = siblings[siblings.len() - 1];
            let last_sibling_id = last_sibling.entry();
            let last_sibling_slot = last_sibling.slot();

            let mut divider_cell = page.owned_cell(0);
            divider_cell.set_left_child(Some(last_sibling_id));
            let parent_page = self.get_page_mut(parent_page_id)?;
            parent_page.insert(last_sibling_slot, divider_cell)?;
        }

        // Fix the global frontier links.
        // Right frontier
        if let Some(frontier) = global_right_frontier {
            let frontier_id = frontier.entry();
            let page = self.get_page_mut(frontier_id)?;
            let last_sibling = siblings[siblings.len() - 1];
            let last_sibling_id = last_sibling.entry();
            let last_sibling_slot = last_sibling.slot();

            // Maintain it linked with the last sibling in the chain
            page.set_prev_sibling(Some(last_sibling_id));
        }

        // Left frontier.
        if let Some(frontier) = global_left_frontier {
            let frontier_id = frontier.entry();
            let page = self.get_page_mut(frontier_id)?;
            let next_sibling = siblings.front().map(|p| p.entry());
            page.set_next_sibling(next_sibling);
        };

        // Done, propagate upwards.
        self.balance(parent_page_id)?;

        Ok(())
    }

    /// Clears the pointers on every page that will be modified on rebalance.
    pub(crate) fn clear_ptrs_on_page(&mut self, page_id: PageId) -> BtreeResult<()> {
        let page_mut = self.get_page_mut(page_id)?;
        page_mut.set_prev_sibling(None);
        page_mut.set_next_sibling(None);
        Ok(())
    }

    /// Loads the closest [num_siblings_per_side] positions at each side of the node with page [page_id].
    /// located at the [parent_position] on its parent.
    fn load_siblings(
        &mut self,
        page: PageId,
        parent_position: &BtreePagePosition,
        num_siblings_per_side: usize,
    ) -> BtreeResult<VecDeque<BtreePagePosition>> {
        let slot = parent_position.slot();
        let parent_id = parent_position.entry();
        let parent_page = self.get_page_mut(parent_id)?;

        let total_cells_in_parent = parent_page.num_slots();

        // Keep track of loaded pages to avoid duplicates.
        let mut loaded: HashSet<PageId> = HashSet::new();
        let mut siblings = VecDeque::new();

        // Push ourselves into the queue.
        siblings.push_back(Position::new(page, slot));
        loaded.insert(page);

        let mut next = slot + 1;
        let mut added_count = 0;

        // Load positions at the right side until we exhaust the parent
        while next <= total_cells_in_parent && added_count < num_siblings_per_side {
            if let Some(child) = parent_page.child(next).map(|id| Position::new(id, next)) {
                if !loaded.contains(&child.entry()) {
                    siblings.push_back(child);
                    loaded.insert(child.entry());
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

        // As we use saturating sub, when we reach slot = 0 we risk loading the same position multiple times, that is why we keep track of loaded pages in order to avoid duplicates.
        next = slot.saturating_sub(1);

        // Load slots at the left side.
        while added_count < num_siblings_per_side {
            if let Some(child) = parent_page.child(next).map(|id| Position::new(id, next)) {
                if !loaded.contains(&child.entry()) {
                    siblings.push_front(child);
                    loaded.insert(child.entry());
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

    fn check_node_status(&mut self, page_id: PageId) -> BtreeResult<NodeStatus> {
        let is_root = self.is_root(page_id);
        let page = self.get_page_mut(page_id)?;

        if (page.has_underflown() && !is_root) || (is_root && page.is_empty()) {
            Ok(NodeStatus::Underflow)
        } else if page.has_overflown() {
            Ok(NodeStatus::Overflow)
        } else {
            Ok(NodeStatus::Balanced)
        }
    }

    /// Grabs the cell one page to another and fix the pointer in the parent.
    /// Does nothing if the target page does not have enough space to fit the borrowed cell.
    fn take_cell_checked(
        &mut self,
        source_page_id: PageId,
        target_page_id: PageId,
        parent_id: PageId,
        borrowed_slot: usize,
        target_slot: usize,
        separator_slot: usize,
        direction: BorrowDirection,
    ) -> BtreeResult<()> {
        // Size occupied by the cell that would go to the target node.
        let cell_size = self
            .get_page_mut(source_page_id)?
            .cell(borrowed_slot)
            .storage_size();

        let can_borrow = self
            .get_page_mut(source_page_id)?
            .can_release_space(cell_size);

        let has_space = self
            .get_page_mut(target_page_id)?
            .has_space_for(cell_size as usize);

        // Cannot release
        if !has_space || !can_borrow {
            return Ok(());
        };

        // Fantastic! our friend has space for us.
        // Lets push our cell there.
        let removed_cell = self.get_page_mut(source_page_id)?.remove(borrowed_slot)?;
        let left = self.get_page_mut(target_page_id)?;
        left.insert(target_slot, removed_cell)?;

        // Haha, but now our parent might have fucked up.
        // We need to unfuck him.
        // In order to unfuck the parent, we replace the entry that pointed to left with the right node's first key
        let divider = match direction {
            BorrowDirection::RightToLeft => {
                let mut separator = self.get_page_mut(source_page_id)?.owned_cell(0);
                separator.set_left_child(Some(target_page_id));
                separator
            }
            BorrowDirection::LeftToRight => {
                let mut separator = self.get_page_mut(target_page_id)?.owned_cell(0);
                separator.set_left_child(Some(source_page_id));
                separator
            }
        };

        let parent_page = self.get_page_mut(parent_id)?;
        if parent_page.num_slots() == separator_slot {
            parent_page.set_right_child(divider.left_child());
        } else {
            parent_page.replace(separator_slot, divider)?;
        };

        Ok(())
    }

    // This should be only called on leaf nodes. on interior nodes it is a brainfuck to also be fixing pointers to right most children every time we borrow a single cell.
    // See SQLite 3.0 balance siblings algorithm: https://sqlite.org/btreemodule.html
    fn balance_siblings(
        &mut self,
        page_id: PageId,
        parent_pos: &BtreePagePosition,
    ) -> BtreeResult<()> {
        let is_leaf = self.get_page_mut(page_id)?.is_leaf();

        if !is_leaf {
            return Ok(());
        };

        let status = self.check_node_status(page_id)?;

        let mut siblings = self.load_siblings(page_id, parent_pos, 1)?; // One sibling per side

        if siblings.is_empty() || siblings.len() == 1 {
            return Ok(());
        };

        let parent_id = parent_pos.entry();
        match status {
            NodeStatus::Balanced => Ok(()),
            NodeStatus::Overflow => {
                let source_page_id = page_id;
                // Okey, we are on overflow state and our left sibling is valid. Then we can ask him if it has space for us.
                if let Some(sibling_left) = siblings.pop_front()
                    && sibling_left.entry() != source_page_id
                {
                    let target_page_id = sibling_left.entry();
                    let separator_slot = sibling_left.slot();

                    let target_slot = self.get_page_mut(target_page_id)?.num_slots();

                    self.take_cell_checked(
                        source_page_id,
                        target_page_id,
                        parent_id,
                        0,
                        target_slot,
                        separator_slot,
                        BorrowDirection::RightToLeft,
                    )?;
                };

                let status = self.check_node_status(page_id)?;

                if let Some(sibling_right) = siblings.pop_back()
                    && sibling_right.entry() != source_page_id
                    && matches!(status, NodeStatus::Overflow)
                {
                    let borrowed_slot = self.get_page_mut(source_page_id)?.num_slots() - 1;

                    let target_page_id = sibling_right.entry();
                    let separator_slot = sibling_right.slot() - 1;

                    self.take_cell_checked(
                        source_page_id,
                        target_page_id,
                        parent_id,
                        borrowed_slot,
                        0,
                        separator_slot,
                        BorrowDirection::LeftToRight,
                    )?;
                };
                Ok(())
            }
            NodeStatus::Underflow => {
                let target_page_id = page_id;

                // Okey, we are on underflow state and our right sibling is valid. Then we can ask him if it has cells for us.
                if let Some(sibling_right) = siblings.pop_back()
                    && sibling_right.entry() != target_page_id
                {
                    let source_page_id = sibling_right.entry();
                    let separator_slot = sibling_right.slot() - 1;

                    let target_slot = self.get_page_mut(target_page_id)?.num_slots();

                    self.take_cell_checked(
                        source_page_id,
                        target_page_id,
                        parent_id,
                        0,
                        target_slot,
                        separator_slot,
                        BorrowDirection::RightToLeft,
                    )?;
                };

                // Borrow from the left side
                if let Some(sibling_left) = siblings.pop_front()
                    && sibling_left.entry() != page_id
                    && matches!(
                        self.check_node_status(target_page_id)?,
                        NodeStatus::Underflow
                    )
                {
                    let source_page_id = sibling_left.entry();
                    let borrowed_slot = self.get_page_mut(source_page_id)?.num_slots() - 1;
                    let separator_slot = sibling_left.slot();
                    self.take_cell_checked(
                        source_page_id,
                        target_page_id,
                        parent_id,
                        borrowed_slot,
                        0,
                        separator_slot,
                        BorrowDirection::LeftToRight,
                    )?;
                };

                Ok(())
            }
        }

        // This algorithm is more efficient than reasseambling everyting in the tree. However, it has the disadvantage that it can fuck up the pointers in the parent nodes.
        // We are responsible from unfucking them.
    }

    /// Deallocates the entire tree using BFS traversal.
    /// More efficient than position iterator since we only need page IDs.
    /// The intuition is the following:
    /// Starts from the root, accumulates all unique children and overflow pages and deallocates them in order.
    pub(crate) fn dealloc(&mut self) -> BtreeResult<()> {
        let mut queue: VecDeque<PageId> = VecDeque::new();
        let mut visited: HashSet<PageId> = HashSet::new();
        let mut overflow_chains: HashSet<PageId> = HashSet::new(); // Use HashSet to deduplicate.
        // During the balancing algorithm, some cells get copied to interior nodes.
        // When this cells have overflow pages linked to them, we do not copy the entire overflow chain, but just the first cell and create a pointer alias on the copied cell. The problem is that this caused duplicate overflow pages to appear in the list of pages to deallocate, generating an infinite deallocation loop.
        // I fixed it out using a [Set] instead of a [Vec] here.

        queue.push_back(self.get_root());

        while let Some(page_id) = queue.pop_front() {
            if visited.contains(&page_id) {
                continue;
            }
            visited.insert(page_id);
            let page = self.get_page_mut(page_id)?;

            // Collect child pages.
            let children: Vec<PageId> = page.iter_children().collect();

            // Collect all overflow chains.
            let overflows: Vec<PageId> = (0..page.num_slots())
                .filter_map(|i| page.cell(i).overflow_page())
                .collect();

            self.accessor_mut()?.release(page_id);

            for child in children {
                if !visited.contains(&child) {
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
            self.get_pager()
                .write()
                .dealloc_page::<BtreePage>(page_id)?;
        }

        Ok(())
    }

    /// Deallocates an entire overflow chain starting from the given page.
    fn dealloc_overflow_chain(&self, start: PageId) -> io::Result<()> {
        let mut current = Some(start);

        while let Some(current_page) = current {
            let next = self
                .get_pager()
                .write()
                .with_page::<OverflowPage, _, _>(current_page, |ovf| ovf.next())?;

            self.get_pager()
                .write()
                .dealloc_page::<OverflowPage>(current_page)?;
            current = next;
        }

        Ok(())
    }
}

pub(crate) struct BtreePrinter<'a> {
    tree: Btree<BtreeReadAccessor>,
    schema: &'a Schema,
    snapshot: &'a Snapshot,
}

impl<'a> BtreePrinter<'a> {
    pub(crate) fn new(
        mut tree: Btree<BtreeReadAccessor>,
        schema: &'a Schema,
        snapshot: &'a Snapshot,
    ) -> Self {
        if !tree.is_initialized() {
            tree = tree.with_accessor(BtreeReadAccessor::default());
        }
        Self {
            tree,
            schema,
            snapshot,
        }
    }

    /// Utility to print a bplustree as json.
    pub(crate) fn as_json(mut self) -> BtreeResult<String> {
        let mut output = String::from('[');
        let mut first = true;

        self.write_subtree(&mut output, self.tree.get_root(), &mut first)?;

        output.push(']');
        Ok(output)
    }

    /// Writes a node and its children recursively to the output string.
    fn write_subtree(
        &mut self,
        output: &mut String,
        page_id: PageId,
        first: &mut bool,
    ) -> BtreeResult<()> {
        let children: Vec<PageId> = self.tree.get_page(page_id)?.iter_children().collect();

        // Write this node
        if !*first {
            output.push(',');
        }
        *first = false;
        self.write_node_json(output, page_id)?;

        // Recurse into children (page already dropped after this scope)
        for child_id in children {
            self.write_subtree(output, child_id, first)?;
        }

        Ok(())
    }

    /// Writes the content of a single page as json directly to output.
    fn write_node_json(&mut self, output: &mut String, page_id: PageId) -> BtreeResult<()> {
        let page = self.tree.get_page(page_id)?;

        write!(output, "{{\"page\":\"{page_id}\",\"entries\":[").unwrap();

        let reader = TupleReader::from_schema(self.schema);
        let mut first_entry = true;

        for i in 0..page.num_slots() {
            let cell = page.cell(i);
            if let Some(layout) = reader.parse_for_snapshot(cell.effective_data(), self.snapshot)? {
                if !first_entry {
                    output.push(',');
                }
                first_entry = false;

                let tuple = TupleRef::new(cell.effective_data(), layout);
                write!(output, "{}", tuple.display_with(self.schema, self.snapshot)).unwrap();
            }
        }

        output.push_str("],\"children\":[");

        if page.right_child().is_some() {
            for (i, child) in page.iter_children().enumerate() {
                if i > 0 {
                    output.push(',');
                }
                write!(output, "\"{child}\"").unwrap();
            }
        }

        output.push_str("]}");
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum IterDirection {
    Forward,
    Backward,
}

/// Iterator that yields cell positions instead of entire [`Payloads<'_>`]
/// My first idea was iterate over full payload objects, however, this comes with the downside of having to create a copy of each cell we visit. This is pretty awful specially with overflow cells that occupy a lot of space.
/// Positions are always small (10 bytes data structures) and therefore the time required to iterate remaings constant. Later, if required, we provide the [with_cells_at] and [with_cell_at] functions to execute custom payloads at specific positions in the btree.
pub(crate) struct BtreePositionalIterator {
    tree: Btree<BtreeReadAccessor>,
    current_page: Option<PageId>,
    current_slot: isize,
    direction: IterDirection,
}

impl BtreePositionalIterator {
    // build an iterator from a tree at a given position and for the given direction.
    pub(crate) fn from_position(
        mut tree: Btree<BtreeReadAccessor>,
        page: PageId,
        slot: isize,
        direction: IterDirection,
    ) -> BtreeResult<Self> {
        if !tree.is_initialized() {
            tree = tree.with_accessor(BtreeReadAccessor::new());
        };

        assert!(tree.accessor.is_some());

        if tree.is_empty()? {
            return Err(BtreeError::BtreeEmpty);
        };

        let mut iterator = Self {
            tree,
            current_page: Some(page),
            current_slot: slot,
            direction,
        };

        iterator.validate(BtreePagePosition::new(page, slot as usize))?;
        Ok(iterator)
    }

    fn validate(&mut self, pos: BtreePagePosition) -> BtreeResult<()> {
        let page = self.tree.get_page(pos.entry())?;
        if !page.is_leaf() || pos.slot() > page.num_slots() {
            return Err(BtreeError::InvalidIterator((pos.entry(), pos.slot())));
        }
        Ok(())
    }

    /// Advance pages in normal order.
    fn adv(&mut self) -> BtreeResult<bool> {
        if let Some(current) = self.current_page {
            self.current_page = self.tree.get_page(current)?.next_sibling();
            self.tree.accessor_mut()?.release(current);
            self.current_slot = 0;

            if let Some(next) = self.current_page {
                return Ok(true);
            };
        };
        Ok(false)
    }

    /// advance pages in reverse order.
    fn rev(&mut self) -> BtreeResult<bool> {
        if let Some(current) = self.current_page {
            self.current_page = self.tree.get_page(current)?.prev_sibling();
            if let Some(prev) = self.current_page {
                self.current_slot = (self.tree.get_page(prev)?.num_slots() - 1) as isize;
                self.tree.accessor_mut()?.release(current);
                return Ok(true);
            };

            self.tree.accessor_mut()?.release(current);
        }

        Ok(false)
    }

    pub(crate) fn get_tree(&self) -> Btree<BtreeReadAccessor> {
        self.tree.cloned_shared()
    }

    pub(crate) fn get_tree_mut(&self) -> Btree<BtreeWriteAccessor> {
        self.tree.cloned_mut()
    }
}

impl Iterator for BtreePositionalIterator {
    type Item = BtreeResult<BtreePagePosition>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_id = self.current_page?;

        match self.direction {
            IterDirection::Forward => {
                let num_slots = match self.tree.get_page(page_id) {
                    Ok(page) => page.num_slots() as isize,
                    Err(e) => return Some(Err(e)),
                };

                if self.current_slot < num_slots {
                    let pos = BtreePagePosition::new(page_id, self.current_slot as usize);
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
                    let pos = Position::new(page_id, self.current_slot as usize);
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

impl Drop for BtreePositionalIterator {
    fn drop(&mut self) {
        if let Some(page) = self.current_page {
            self.tree.accessor_mut().unwrap().release(page);
        }
    }
}

impl DoubleEndedIterator for BtreePositionalIterator {
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
