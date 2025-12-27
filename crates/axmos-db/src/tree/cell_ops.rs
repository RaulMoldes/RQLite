use crate::{
    CELL_ALIGNMENT,
    core::RuntimeSized,
    io::pager::SharedPager,
    storage::{
        cell::{CELL_HEADER_SIZE, CellRef, OwnedCell, Slot},
        core::{buffer::Payload, traits::BtreeOps},
        page::{BtreePage, OverflowPage},
    },
    tree::comparators::Comparator,
    types::{PageId, core::TypeClass},
};

use std::{
    cmp::{Ordering, min},
    io, mem,
    ops::Deref,
    usize,
};

/// The cell builder is a type of accessor that is only used to build cells.
/// As such, it can bypass the accessor and use the pager methods directly.
/// For overflow pages the concerns that exist on normal pages does not apply, since overflow pages re automatically locked once you lock the first page in the overflow chain.
pub(crate) struct CellBuilder {
    /// amount of space available in the first page to store the first cell.
    /// Must be known in advance when creating the builder
    max_cell_storage_size: usize,

    /// Minimum number of cells that any btreepage can have
    minimum_keys_per_page: usize,

    /// Shared access to the pager.
    pager: SharedPager,
}

pub(crate) struct CellBytesComparator<Cmp: Comparator> {
    comparator: Cmp,
    pager: SharedPager,
}

impl<Cmp> CellBytesComparator<Cmp>
where
    Cmp: Comparator,
{
    pub(crate) fn new(comparator: Cmp, pager: SharedPager) -> Self {
        Self { comparator, pager }
    }

    pub(crate) fn compare_cell_payload<'b>(
        &self,
        target: KeyBytes<'_>,
        cell: CellRef<'b>,
    ) -> io::Result<Ordering> {
        let ordering = if cell.metadata().is_overflow() {
            // If the cell is overflow we need to reassemble.
            let key_start = cell.key_start();
            let key_size = cell.key_size();
            let mut reassembler =
                Reassembler::with_capacity_and_max_size(self.pager.clone(), key_size);
            reassembler.reassemble(cell)?;

            self.comparator.compare(target, reassembler.key_bytes())?
        } else {
            // Otherwise we can just compare the raw payload without copying or moving data.
            self.comparator.compare(target, cell.key_bytes())?
        };

        Ok(ordering)
    }
}

/// Struct intended to reassemble cell payloads.
#[derive(Clone)]
pub(crate) struct Reassembler {
    pager: SharedPager,
    max_size: usize,
    key_start: usize,
    key_size: usize,
    reassembled: Vec<u8>,
}

impl Reassembler {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self {
            pager,
            key_start: 0,
            key_size: 0, // Unset at the beginning
            max_size: usize::MAX,
            reassembled: Vec::new(),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            key_start: 0,
            key_size: 0,
            max_size: usize::MAX,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity_and_max_size(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            key_start: 0,
            key_size: 0,
            max_size: cap,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Reassembles the full payload from a cell and its overflow chain
    pub(crate) fn reassemble<'b>(&mut self, cell: CellRef<'b>) -> io::Result<()> {
        // Clear the reassembled vec to be able to reuse the same reasssembler accross slots.
        self.reassembled.clear();

        self.key_start = cell.key_start();
        self.key_size = cell.key_size();

        if cell.metadata().is_overflow() {
            // First part is in the cell (minus the overflow page pointer)
            let first_part_len = cell.len() - mem::size_of::<PageId>();
            self.reassembled
                .extend_from_slice(&cell.effective_data()[..first_part_len]);

            // Rest is in overflow pages
            let mut current_page = cell.overflow_page();
            while let Some(page_id) = current_page
                && self.max_size >= self.reassembled.len()
            {
                self.pager
                    .write()
                    .with_page::<OverflowPage, _, _>(page_id, |overflow| {
                        current_page = overflow.metadata().next;
                        self.reassembled
                            .extend_from_slice(&overflow.effective_data());
                    })?;
            }
        } else {
            self.reassembled.extend_from_slice(cell.effective_data());
        }

        Ok(())
    }

    /// Returns the [KeyBytes] of the reassembled payload
    pub(crate) fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.reassembled[self.key_start..self.key_start + self.key_size])
    }

    pub(crate) fn into_boxed_slice(self) -> Box<[u8]> {
        self.reassembled.into_boxed_slice()
    }
}

pub(crate) struct CellDeallocator {
    pager: SharedPager,
}

impl CellDeallocator {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self { pager }
    }

    /// Deallocates a cell.
    /// Iterates over the full overflow chain and uses its reference to the pager in order to deallocate all of them.
    pub(crate) fn deallocate_cell(&self, cell: OwnedCell) -> io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while let Some(page_id) = overflow_page {
            let next = self
                .pager
                .write()
                .with_page::<OverflowPage, _, _>(page_id, |overflow| overflow.metadata().next)?;

            self.pager.write().dealloc_page::<OverflowPage>(page_id)?;

            overflow_page = next;
        }

        Ok(())
    }
}

impl CellBuilder {
    pub fn new(
        max_cell_storage_size: usize,
        minimum_keys_per_page: usize,
        pager: SharedPager,
    ) -> Self {
        Self {
            max_cell_storage_size,
            minimum_keys_per_page,
            pager,
        }
    }

    #[inline]
    /// Utility to align down a number to the closets multiple of alignment
    fn align_down(value: usize, alignment: usize) -> usize {
        if value <= alignment {
            0
        } else {
            (value.saturating_sub(1) / alignment) * alignment
        }
    }

    /// Computes the total available space in the first page to store the cell.
    ///
    /// The total available space computation depends if the cell is or not an overflow cell.
    ///
    /// If the cell is going to be stored normally, the amount of space we need for the payload is just the cell aligned payload size + cell header size + slot size.
    ///
    /// However, if we need to create overflowpages, we also need to reserve 4 bytes at the end of the page in order to store the pointer to the overflow page.
    fn compute_available_space_in_first_page(&self, page_size: usize) -> usize {
        // Available size in the page for the actual cell (including header data)
        let available_cell_size = self
            .max_cell_storage_size
            .saturating_sub(mem::size_of::<Slot>());

        // Align down to CELL_ALIGNMENT since we need to account for the header.
        let mut max_payload_size_in_page: usize =
            Self::align_down(available_cell_size, CELL_ALIGNMENT as usize);

        // Account for the header and the bytes reserved for the overflow page.
        max_payload_size_in_page = max_payload_size_in_page
            .saturating_sub(mem::size_of::<PageId>())
            .saturating_sub(CELL_HEADER_SIZE);

        // Clamp to max payload size in the page.
        // [BtreePage::ideal_maz_payload_size] should be already aligned to [CELL_ALIGNMENT]
        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.minimum_keys_per_page),
            max_payload_size_in_page,
        ); // Clamp

        max_payload_size
    }

    /// Builds a cell from any type that implements From<Box<[u8]>> and From <&'a [u8]>>
    pub fn build_cell<'b, T>(&self, data: T) -> io::Result<OwnedCell>
    where
        T: Buildable,
    {
        let page_size = self.pager.write().page_size();
        let max_payload_size: usize = self.compute_available_space_in_first_page(page_size);
        let total_size = data.built_size();

        // If the cell fits in a single page simply return the built cell.
        if total_size <= max_payload_size {
            return Ok(data.into());
        };

        // At this point we have to split the payload.
        let key_start = data.key_start();
        let key_size = data.key_size();
        let payload_boxed: Payload = data.into();
        let payload = payload_boxed.effective_data();

        let mut overflow_page_number = self.pager.write().allocate_page::<OverflowPage>()?;

        let cell = OwnedCell::new_overflow_with_key_bounds(
            &payload[..max_payload_size],
            overflow_page_number,
            key_start,
            key_size,
        );

        let mut stored_bytes = max_payload_size;

        // Note that here we cannot release the latch on an overflow page until all the chain is written. This might waste a lot of memory  as it will keep overflow pages pinned. It is a tradeoff between correctness and efficiency, and for a database, we prefer to play the correctness side of the story.
        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            self.pager.write().with_page_mut::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.push_bytes(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().next = None;
                    stored_bytes += overflow_bytes;
                },
            )?;

            // Overflow chain built. Stop here.
            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = self.pager.write().allocate_page::<OverflowPage>()?;

            self.pager.write().with_page_mut::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.metadata_mut().next = Some(next_overflow_page);
                },
            )?;

            overflow_page_number = next_overflow_page;
        }

        Ok(cell)
    }
}

/// Custom trait to add information about the serialized size of the data before converting into a cell. Allows to know the size of the payload in advance.
pub trait Buildable: Into<OwnedCell> + Into<Payload> + for<'a> AsKeyBytes<'a> {
    fn built_size(&self) -> usize;
}

/// On cells payload, keys are stored at the beginning always.
/// This type is a custom type to ensure that we access them from the btree
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct KeyBytes<'a>(&'a [u8]);

impl<'a> KeyBytes<'a> {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn as_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        str::from_utf8(self.0)
    }
}

impl<'a> From<&'a [u8]> for KeyBytes<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}

impl<'a> AsRef<[u8]> for KeyBytes<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

// Type to obtain the key bytes for the implementor type
pub(crate) trait AsKeyBytes<'a> {
    /// Reference to the keys of the target object
    fn key_bytes(&'a self) -> KeyBytes<'a>;

    /// Offset where the keys start in the payload
    fn key_start(&self) -> usize;

    /// Size of the keys section in bytes.
    fn key_size(&self) -> usize;
}

/// All types in the typesystem implement [AsKeyBytes] (used in tests)
impl<'a, T> AsKeyBytes<'a> for T
where
    T: AsRef<[u8]> + TypeClass + RuntimeSized,
{
    fn key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes(self.as_ref())
    }

    fn key_start(&self) -> usize {
        0
    }

    fn key_size(&self) -> usize {
        self.runtime_size()
    }
}

impl<'a> Deref for KeyBytes<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
