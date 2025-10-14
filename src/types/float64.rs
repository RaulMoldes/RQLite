use crate::serialization::Serializable;
use crate::types::{RQLiteType, RQLiteTypeMarker};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt::Display;

#[derive(Debug, Clone, Copy)]
pub struct Float64(pub f64);

impl RQLiteType for Float64 {
    fn _type_of(&self) -> RQLiteTypeMarker {
        RQLiteTypeMarker::Float64
    }

    fn size_of(&self) -> usize {
        8
    }
}

impl Display for Float64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Float64: {}", self.0)?;
        Ok(())
    }
}

impl Serializable for Float64 {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        let value = f64::from_be_bytes(bytes);
        Ok(Float64(value))
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.0.to_be_bytes();
        writer.write_all(&bytes)?;

        Ok(())
    }
}

impl Ord for Float64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Float64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Float64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Float64 {}
