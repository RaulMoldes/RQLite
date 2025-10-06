use crate::serialization::Serializable;
use crate::types::{RQLiteType, RQLiteTypeMarker};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Byte(pub u8);

pub const TRUE: Byte = Byte(0x01);
pub const FALSE: Byte = Byte(0x00);

impl RQLiteType for Byte {
    fn _type_of(&self) -> RQLiteTypeMarker {
        RQLiteTypeMarker::Byte
    }

    fn size_of(&self) -> usize {
        1
    }
}

impl Display for Byte {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Byte: {}", self.0)
    }
}

impl Serializable for Byte {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut bytes = [0u8; 1];
        reader.read_exact(&mut bytes)?;
        Ok(Byte(bytes[0]))
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&[self.0])?;
        Ok(())
    }
}
