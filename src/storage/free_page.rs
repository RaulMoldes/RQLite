use crate::page_header::PAGE_HEADER_SIZE;
use crate::serialization::Serializable;

use super::PageType;
use crate::types::PageId;
use crate::{impl_header_ops, HeaderOps, PageHeader};
use std::io::{self, Read, Write};

#[derive(Clone, Debug)]
pub(crate) struct FreePage {
    pub(crate) header: PageHeader,
    pub(crate) data: Vec<u8>,
}

impl_header_ops!(FreePage);

impl Serializable for FreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let header = PageHeader::read_from(reader)?;

        // Free pages are assumed to be empty.
        let data_size = header.page_size() - PAGE_HEADER_SIZE;
        let buffer = vec![0u8; data_size as usize];
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
