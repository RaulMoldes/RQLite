use std::io::{self, Read, Write};

#[cfg(test)]
pub mod tests;

pub(crate) trait Serializable {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized;

    fn write_to<W: Write>(self, writer: &mut W) -> io::Result<()>;
}
