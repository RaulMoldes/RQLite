use std::io::{self, Read, Write};

#[cfg(test)]
pub mod tests;

/// Custom trait for serializing and deserializing data to and from byte streams.
/// This trait is used to read and write data in a binary format.
/// It is implemented for various types, including B-Tree page headers and cells.
pub(crate) trait Serializable {
    /// Reads a value from a byte stream.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized;
    /// Writes a value to a byte stream.
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;
}
