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
    fn cell_count(&self) -> usize {
        0usize
    }

    fn free_space_start(&self) -> usize {
        0usize
    }

    fn set_next_overflow(&mut self, overflowpage: PageId) {}

    fn content_start(&self) -> usize {
        0usize
    }

    fn free_space(&self) -> usize {
        0usize
    }

    fn id(&self) -> PageId {
        self.id
    }

    fn is_overflow(&self) -> bool {
        false
    }

    fn page_size(&self) -> usize {
        0usize
    }

    fn type_of(&self) -> crate::PageType {
        crate::PageType::Free
    }

    fn get_next_overflow(&self) -> Option<PageId> {
        None
    }

    fn set_type(&mut self, page_type: crate::storage::PageType) {}
}
