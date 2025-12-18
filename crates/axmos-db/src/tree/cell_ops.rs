use crate::{
    CELL_ALIGNMENT,
    io::pager::SharedPager,
    storage::{
        cell::{CELL_HEADER_SIZE, CellRef, OwnedCell, Slot},
        core::traits::{BtreeBuffer, Buffer},
        page::{BtreePage, OverflowPage},
    },
    tree::comparators::Comparator,
    types::PageId,
};

use std::{cmp::min, io, mem, usize};

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
        target: &[u8],
        cell: CellRef<'b>,
    ) -> io::Result<std::cmp::Ordering> {
        let ordering = if cell.metadata().is_overflow() {
            // If the cell is overflow we need to reassemble.
            let key_size = self.comparator.key_size(cell.payload())?;
            let mut reassembler =
                Reassembler::with_capacity_and_max_size(self.pager.clone(), key_size);
            reassembler.reassemble(cell)?;
            let bytes = reassembler.into_boxed_slice();
            self.comparator.compare(target, bytes.as_ref())?
        } else {
            // Otherwise we can just compare the raw payload without copying or moving data.
            self.comparator.compare(target, cell.payload())?
        };

        Ok(ordering)
    }
}

/// Struct intended to reassemble cell payloads.
#[derive(Clone)]
pub(crate) struct Reassembler {
    pager: SharedPager,
    max_size: usize,
    reassembled: Vec<u8>,
}

impl Reassembler {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self {
            pager,
            max_size: usize::MAX,
            reassembled: Vec::new(),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            max_size: usize::MAX,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity_and_max_size(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            max_size: cap,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Reassembles the full payload from a cell and its overflow chain
    pub(crate) fn reassemble<'b>(&mut self, cell: CellRef<'b>) -> io::Result<()> {
        // Clear the reassembled vec to be able to reuse the same reasssembler accross slots.
        self.reassembled.clear();

        if cell.metadata().is_overflow() {
            // First part is in the cell (minus the overflow page pointer)
            let first_part_len = cell.len() - mem::size_of::<PageId>();
            self.reassembled
                .extend_from_slice(&cell.payload()[..first_part_len]);

            // Rest is in overflow pages
            let mut current_page = cell.overflow_page();
            while let Some(page_id) = current_page
                && self.max_size >= self.reassembled.len()
            {
                let (next, data) = self
                    .pager
                    .write()
                    .with_page::<OverflowPage, _, _>(page_id, |overflow| {
                        (overflow.metadata().next, overflow.payload().to_vec())
                    })?;

                self.reassembled.extend_from_slice(&data);

                current_page = next;
            }
        } else {
            self.reassembled.extend_from_slice(cell.payload());
        }

        Ok(())
    }

    pub(crate) fn into_boxed_slice(self) -> Box<[u8]> {
        self.reassembled.into_boxed_slice()
    }
}

/// The cell reader is a struct that can create items of any type T from any cell payload as long as the item T implements From<&[u8]> + From<Box<[u8]>>
pub(crate) struct CellReader {
    pager: SharedPager,
}

impl CellReader {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self { pager }
    }

    /// Attempts to get the T from the cell ref, if it can't do it creates a new [Reassembler] to reassemble the overflow chain.
    pub(crate) fn get<T>(&self, cell: CellRef<'_>) -> io::Result<T>
    where
        T: Deserializable,
    {
        if !cell.is_overflow() {
            // TODO: I do not understand why i need this copy. Must figure it out.
            let payload = cell.payload();
            let data = T::from_borrowed(payload);
            Ok(data)
        } else {
            let mut reassembler = Reassembler::new(self.pager.clone());
            reassembler.reassemble(cell)?;
            Ok(T::from_owned(reassembler.into_boxed_slice()))
        }
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
        T: Serializable,
    {
        let page_size = self.pager.write().page_size()?;
        let max_payload_size: usize = self.compute_available_space_in_first_page(page_size);
        let total_size = data.serialized_size();

        // If the cell fits in a single page simply return the built cell.
        if total_size <= max_payload_size {
            return Ok(data.into());
        };

        // At this point we have to split the payload.
        let payload: Box<[u8]> = data.into();

        let mut overflow_page_number = self.pager.write().allocate_page::<OverflowPage>()?;

        let cell = OwnedCell::new_overflow(&payload[..max_payload_size], overflow_page_number);

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
                    overflow_page.data_mut()[..overflow_bytes]
                        .copy_from_slice(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().num_bytes = overflow_bytes as _;
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
pub trait Serializable: Into<OwnedCell> + Into<Box<[u8]>> {
    fn serialized_size(&self) -> usize;
}

pub(crate) trait Deserializable: Sized {
    fn from_borrowed(bytes: &[u8]) -> Self;
    fn from_owned(bytes: Box<[u8]>) -> Self;
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
    fn as_key_bytes(&'a self) -> KeyBytes<'a>;
}
