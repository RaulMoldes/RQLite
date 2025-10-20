use crate::serialization::Serializable;
/// Varchar type implementation.
use crate::types::{DataType, VarlenaType};
use crate::TextEncoding;
use std::cmp::Ordering;
use std::cmp::{Ord, PartialOrd};

pub struct Varchar(VarlenaType);

impl Varchar {
    fn new(size: u16, encoding: TextEncoding) -> Self {
        Self(VarlenaType::with_capacity(size, Some(encoding)))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn write(&mut self, value: &str) -> Result<(), String> {
        if self.len() == 0 {
            self.0.extend_from_str(value)?;
        };
        Ok(())
    }
}

impl DataType for Varchar {
    fn _type_of(&self) -> super::DataTypeMarker {
        super::DataTypeMarker::Varchar
    }

    fn size_of(&self) -> u16 {
        self.0.size_of()
    }
}

impl Serializable for Varchar {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let inner = VarlenaType::read_from(reader)?;
        Ok(Self(inner))
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        self.0.write_to(writer)?;
        Ok(())
    }
}

impl std::fmt::Display for Varchar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialOrd for Varchar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Varchar {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for Varchar {}

impl Ord for Varchar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
