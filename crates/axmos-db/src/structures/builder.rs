use crate::{
    CELL_ALIGNMENT,
    io::frames::FrameAccessMode,
    storage::{
        cell::{CELL_HEADER_SIZE, OwnedCell, Slot},
        page::{BtreePage, OverflowPage},
    },
    transactions::accessor::{PageAccessor, RcPageAccessor},
    types::{PAGE_ZERO, PageId},
};

use std::{
    cell::{Ref, RefMut},
    cmp::min,
    io, mem, usize,
};

#[derive(Debug)]
pub(crate) struct CellBuilder {
    max_cell_size: usize,
    minimum_keys_per_page: usize,
    accessor: RcPageAccessor,
}

impl CellBuilder {
    pub fn for_deallocation(minimum_keys_per_page: usize, accessor: RcPageAccessor) -> Self {
        Self {
            max_cell_size: 0,
            minimum_keys_per_page,
            accessor,
        }
    }

    pub fn for_allocation(
        id: PageId,
        minimum_keys_per_page: usize,
        accessor: RcPageAccessor,
    ) -> io::Result<Self> {
        let free_space = accessor
            .borrow()
            .read_page::<BtreePage, _, _>(id, |btreepage| {
                min(
                    btreepage.max_allowed_payload_size(),
                    btreepage.metadata().free_space as u16,
                )
            })? as usize;

        Ok(Self {
            max_cell_size: free_space,
            minimum_keys_per_page,
            accessor,
        })
    }

    fn accessor(&self) -> Ref<'_, PageAccessor> {
        self.accessor.borrow()
    }

    fn accessor_mut(&self) -> RefMut<'_, PageAccessor> {
        self.accessor.borrow_mut()
    }

    /// Deallocates a cell, the current RcPageAccessor will be responsible for deallocating all overflow pages.
    pub fn free_cell(&self, cell: OwnedCell) -> io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while overflow_page.is_valid() {
            self.accessor_mut()
                .acquire::<OverflowPage>(overflow_page, FrameAccessMode::Read)?;

            let next = self
                .accessor()
                .read_page::<OverflowPage, _, _>(overflow_page, |overflow| {
                    overflow.metadata().next
                })?;

            self.accessor_mut()
                .dealloc_page::<OverflowPage>(overflow_page)?;

            overflow_page = next;
        }

        Ok(())
    }

    #[inline]
    fn align_down(value: usize, alignment: usize) -> usize {
        if value <= alignment {
            0
        } else {
            (value.saturating_sub(1) / alignment) * alignment
        }
    }

    fn compute_available_space(&self) -> usize {
        let page_size = self.accessor().get_page_size();

        let available_payload_size = self.max_cell_size.saturating_sub(Slot::SIZE);

        // Store payload in chunks, link overflow pages and return the cell.
        let mut max_payload_size_in_page: usize =
            Self::align_down(available_payload_size, CELL_ALIGNMENT as usize);

        max_payload_size_in_page = max_payload_size_in_page
            .saturating_sub(mem::size_of::<PageId>())
            .saturating_sub(CELL_HEADER_SIZE);

        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.minimum_keys_per_page),
            max_payload_size_in_page,
        ); // Clamp

        max_payload_size
    }

    pub fn build_cell<T>(&self, data: T) -> io::Result<OwnedCell>
    where
        T: IntoCell,
    {
        let max_payload_size: usize = self.compute_available_space();
        let total_size = data.serialized_size();


        // If the cell fits in a single page simply return the built cell.
        if total_size <= max_payload_size {
            return Ok(data.into());
        };

        // At this point we have to split the payload.
        let payload : Box<[u8]> = data.into();
        let page_size = self.accessor().get_page_size();
        let mut overflow_page_number = self.accessor_mut().alloc_page::<OverflowPage>()?;

        let cell = OwnedCell::new_overflow(&payload[..max_payload_size], overflow_page_number);

        let mut stored_bytes = max_payload_size;

        // Note that here we cannot release the latch on an overflow page until all the chain is written. This might waste a lot of memory  as it will keep overflow pages pinned. It is a tradeoff between correctness and efficiency, and for a database, we prefer to play the correctness side of the story.
        loop {
            self.accessor_mut()
                .acquire::<OverflowPage>(overflow_page_number, FrameAccessMode::Write)?;

            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            self.accessor_mut().write_page::<OverflowPage, _, _>(
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

            let next_overflow_page = self.accessor_mut().alloc_page::<OverflowPage>()?;

            self.accessor_mut().write_page::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.metadata_mut().next = next_overflow_page;
                },
            )?;

            overflow_page_number = next_overflow_page;
        }
        Ok(cell)
    }
}

/// Custom trait to add information about the serialized size of the data before converting into a cell. Allows to know the size of the payload in advance.
pub trait IntoCell: Into<OwnedCell> + Into<Box<[u8]>> {
    fn serialized_size(&self) -> usize;

    fn key_bytes(&self) -> KeyBytes<'_>;
}

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

// Any type within the crate that implements [`AsRef<[u8]>`] will automatically implement [`AsKeyBytes`]
pub trait AsKeyBytes<'a> {
    fn as_key_bytes(&'a self) -> KeyBytes<'a>;
}

impl<'a, T> AsKeyBytes<'a> for T
where
    T: AsRef<[u8]> + ?Sized,
{
    fn as_key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes::from(self.as_ref())
    }
}
