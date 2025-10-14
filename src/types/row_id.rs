use crate::impl_arithmetic_ops;
use crate::serialization::Serializable;
use crate::types::Key;
use crate::RQLiteType;
use std::cmp::Ordering;
use std::fmt::Display;
use std::io::{Read, Write};
use std::sync::atomic::AtomicU32;

static __GLOBAL_ROW_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Hash, PartialEq, Eq, Copy)]
pub struct RowId(u32);

impl Key for RowId {
    fn new_key() -> Self {
        __gen_new_row()
    }
}

fn __gen_new_row() -> RowId {
    RowId(__GLOBAL_ROW_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

impl RQLiteType for RowId {
    fn size_of(&self) -> usize {
        4
    }

    fn _type_of(&self) -> crate::types::RQLiteTypeMarker {
        crate::types::RQLiteTypeMarker::Key
    }
}

impl From<u32> for RowId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<RowId> for u32 {
    fn from(value: RowId) -> Self {
        value.0
    }
}

impl Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowId: ({})", self.0)
    }
}

impl Serializable for RowId {
    fn read_from<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let row_id = u32::from_be_bytes(buffer);
        Ok(RowId(row_id))
    }

    fn write_to<W: Write>(self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.0.to_be_bytes())?;
        Ok(())
    }
}

impl PartialOrd for RowId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
impl_arithmetic_ops!(RowId);
