use crate::page_header::PAGE_HEADER_SIZE;
use crate::serialization::Serializable;
use crate::types::varlena::VarlenaType;
use crate::types::{Key, PageId, RQLiteType, Splittable};
use crate::{HeaderOps, PageHeader};
use std::io::Cursor;
use std::io::{self, Read, Write};

/// Trait that all Overflowable items can implement.
/// Should handle proper content splitting, overflow page creation and linking.
/// Table Leaf Pages, Index Interior Pages and IndexLeafPages are overflowable, but Table Interior Pages are not.
pub(crate) trait Overflowable {
    /// Type of content that can be inserted in this overflowable type.
    type Content: Serializable;
    type LeafContent: Serializable;
    type InteriorContent: Serializable;

    /// Attempt to insert a chunk of [Content] into the Overflowable item. Ifnot possible,
    /// will split it and handle the creation of the overflow chain.
    /// If the content does not fit in a single overflow page, will return the remaining to ensure the caller can create multiple overflow pages on demand.
    fn try_insert_with_overflow(
        &mut self,
        content: Self::Content,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        Ok(None)
    }

    fn try_insert_with_overflow_leaf(
        &mut self,
        content: Self::LeafContent,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        Ok(None)
    }

    fn try_insert_with_overflow_interior(
        &mut self,
        content: Self::InteriorContent,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct OverflowPage {
    pub(crate) header: PageHeader,
    pub(crate) data: VarlenaType,
}

impl OverflowPage {
    pub(crate) fn new(header: PageHeader, data: VarlenaType) -> Self {
        OverflowPage { header, data }
    }
}

impl HeaderOps for OverflowPage {
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

impl Serializable for OverflowPage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let header = PageHeader::read_from(reader)?;
        // Calculate how many bytes we should read for the overflow data
        let data_size = header.page_size() - PAGE_HEADER_SIZE;

        let mut buffer = vec![0u8; data_size];
        reader.read_exact(&mut buffer)?;

        // Now parse the VarlenaType from the fixed-size buffer
        let mut cursor = Cursor::new(&buffer);
        let data = VarlenaType::read_from(&mut cursor)?;

        Ok(OverflowPage { header, data })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        self.data.write_to(writer)?;
        let data_written = self.data.size_of();

        if data_written < self.page_size() {
            let padding = vec![0u8; self.page_size() - data_written];
            writer.write_all(&padding)?;
        }

        Ok(())
    }
}

impl OverflowPage {
    pub fn create(
        page_id: PageId,
        page_size: u32,
        page_type: crate::page_header::PageType,
        right_most_page: Option<PageId>,
    ) -> Self {
        let header = PageHeader::new(page_id, page_size, page_type, right_most_page);
        Self {
            header,
            data: VarlenaType::from_raw_bytes(&[], None),
        }
    }
}

impl Overflowable for OverflowPage {
    type Content = VarlenaType;
    type LeafContent = VarlenaType;
    type InteriorContent = VarlenaType;

    /// Attempt to insert a VarlenaType in this Overflow Page.
    /// If not possible, will create additional pages, connecting them properly in an overflow chain.
    fn try_insert_with_overflow(
        &mut self,
        mut content: Self::Content,
        max_payload_factor: f32,
    ) -> std::io::Result<Option<(OverflowPage, VarlenaType)>> {
        if content.size_of() < self.max_cell_size(max_payload_factor) {
            self.data = content;
            self.header.free_space_ptr -= self.data.size_of() as u16;
            return Ok(None);
        }

        let available = self.available_space(max_payload_factor);
        let remaining = content.split_at(available);
        self.data = content;
        let new = PageId::new_key();
        self.set_next_overflow(new);
        let new_page = OverflowPage::create(new, self.page_size() as u32, self.type_of(), None);

        Ok(Some((new_page, remaining)))
    }
}

unsafe impl Send for OverflowPage {}
unsafe impl Sync for OverflowPage {}
