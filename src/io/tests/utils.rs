use crate::serialization::Serializable;
use crate::types::PageId;
use std::marker::{Send, Sync};

#[derive(Default, Debug)]
pub struct TestPage {
    id: PageId,
}

impl TestPage {
    pub fn new(id: PageId) -> Self {
        Self { id }
    }
}

impl Serializable for TestPage {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let id = PageId::read_from(reader)?;
        Ok(TestPage { id })
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        self.id.write_to(writer)?;
        Ok(())
    }
}

unsafe impl Send for TestPage {}
unsafe impl Sync for TestPage {}

impl crate::HeaderOps for TestPage {
    fn cell_count(&self) -> u16 {
        0u16
    }

    fn free_space_start(&self) -> u16 {
        0u16
    }

    fn set_next_overflow(&mut self, _overflowpage: PageId) {}

    fn content_start(&self) -> u16 {
        0u16
    }

    fn free_space(&self) -> u16 {
        0u16
    }

    fn id(&self) -> PageId {
        self.id
    }

    fn is_overflow(&self) -> bool {
        false
    }

    fn page_size(&self) -> u16 {
        0u16
    }

    fn type_of(&self) -> crate::PageType {
        crate::PageType::Free
    }

    fn get_next_overflow(&self) -> Option<PageId> {
        None
    }

    fn set_type(&mut self, _page_type: crate::storage::PageType) {}

    fn set_cell_count(&mut self, _count: u16) {}

    fn set_content_start_ptr(&mut self, _content_start: u16) {}

    fn set_free_space_ptr(&mut self, _ptr: u16) {}
}
