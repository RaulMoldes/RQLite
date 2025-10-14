use crate::page_header::PAGE_HEADER_SIZE;
use crate::serialization::Serializable;

use super::PageType;
use crate::types::PageId;
use crate::{HeaderOps, PageHeader};
use std::io::{self, Read, Write};

#[derive(Clone, Debug)]
pub(crate) struct FreePage {
    pub(crate) header: PageHeader,
    pub(crate) data: Vec<u8>,
}

impl HeaderOps for FreePage {
    fn cell_count(&self) -> usize {
        self.header.cell_count()
    }

    fn free_space_start(&self) -> usize {
        self.header.free_space_start()
    }

    fn set_next_overflow(&mut self, overflowpage: PageId) {
        self.header.set_next_overflow(overflowpage);
    }

    fn content_start(&self) -> usize {
        self.header.content_start()
    }

    fn free_space(&self) -> usize {
        self.header.free_space()
    }

    fn id(&self) -> crate::types::PageId {
        self.header.id()
    }

    fn is_overflow(&self) -> bool {
        self.header.is_overflow()
    }

    fn page_size(&self) -> usize {
        self.header.page_size()
    }

    fn type_of(&self) -> super::PageType {
        self.header.type_of()
    }

    fn get_next_overflow(&self) -> Option<PageId> {
        self.header.get_next_overflow()
    }

    fn set_type(&mut self, page_type: super::PageType) {
        self.header.set_type(page_type);
    }
}

impl Serializable for FreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let header = PageHeader::read_from(reader)?;

        // Free pages are assumed to be empty.
        let data_size = header.page_size() - PAGE_HEADER_SIZE;
        let buffer = vec![0u8; data_size];
        Ok(FreePage {
            header,
            data: buffer,
        })
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        self.data.write_to(writer)?;
        Ok(())
    }
}

impl FreePage {
    pub fn create(page_id: PageId, page_size: u32, right_most_page: Option<PageId>) -> Self {
        let header = PageHeader::new(page_id, page_size, PageType::Free, right_most_page);
        Self {
            header,
            data: Vec::new(),
        }
    }
}
