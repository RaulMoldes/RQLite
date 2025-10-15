use crate::impl_arithmetic_ops;
use crate::types::Key;
use crate::{RQLiteType, Serializable};
use std::io::{self, Read, Write};
use std::sync::atomic::AtomicU32;

static __GLOBAL_PAGE_COUNT: AtomicU32 = AtomicU32::new(0);

/// Type alias for the page id. ¿¿Are we going to have more than (2^32) pages ?? Probably not.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Copy, Default)]
pub struct PageId(u32);

impl PageId {
    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }
}

impl Key for PageId {
    fn new_key() -> Self {
        __gen_new_page()
    }
}

impl From<PageId> for u32 {
    fn from(value: PageId) -> Self {
        value.0
    }
}

impl From<u32> for PageId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

fn __gen_new_page() -> PageId {
    PageId(__GLOBAL_PAGE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

impl Serializable for PageId {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        Ok(PageId(u32::from_be_bytes(buffer)))
    }

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.0.to_be_bytes())?;
        Ok(())
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageId: ({})", self.0)
    }
}

impl RQLiteType for PageId {
    fn size_of(&self) -> u16 {
        4
    }

    fn _type_of(&self) -> crate::types::RQLiteTypeMarker {
        crate::types::RQLiteTypeMarker::Key
    }
}

impl PartialOrd for PageId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PageId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl_arithmetic_ops!(PageId);
