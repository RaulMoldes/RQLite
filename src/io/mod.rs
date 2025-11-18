//! # IO Module
//! This module provides the io layer for the database, including buffer management,
//! disk management, and paging. It is responsible for managing how data is stored and retrieved
//! from disk, as well as caching frequently accessed data in memory to improve performance.
pub mod cache;
pub mod disk;
pub mod frames;

//pub mod journal;
pub mod pager;
mod wal;

use crate::types::{DataType, DataTypeKind, VarInt, reinterpret_cast, varint::MAX_VARINT_LEN};
use std::alloc::{Layout, alloc_zeroed};
use std::io::{Read, Seek, SeekFrom, Write};

#[cfg(test)]
mod tests;

pub(crate) trait AsBytes {
    fn write_to<W: Write>(&self, buffer: &mut W) -> std::io::Result<()>;
    fn read_from<R: Read + Seek>(bytes: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;
}

pub fn read_string_unchecked<R: Read>(reader: &mut R) -> std::io::Result<String> {
    let varint = VarInt::read_buf(reader)?;
    let (len, offset) = VarInt::from_encoded_bytes(&varint)?;
    let len_usize: usize = len.try_into()?;
    let mut str_buf = vec![0u8; len_usize];
    reader.read_exact(&mut str_buf)?;
    Ok(unsafe { String::from_utf8_unchecked(str_buf) })
}

pub fn write_string_unchecked<W: Write>(writer: &mut W, s: &str) -> std::io::Result<()> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut vbuf = [0u8; MAX_VARINT_LEN];
    let len_varint = VarInt::encode(len as i64, &mut vbuf);
    writer.write_all(len_varint)?;
    writer.write_all(bytes)?;

    Ok(())
}

pub fn read_variable_length<R: Read + Seek>(buf: &mut R) -> std::io::Result<Box<[u8]>> {
    let varint = VarInt::read_buf(buf)?;
    let (len, offset) = VarInt::from_encoded_bytes(varint.as_ref())?;
    let len_usize: usize = len.try_into().unwrap();
    let mut val_buffer = vec![0u8; len_usize];
    buf.read_exact(&mut val_buffer)?;
    Ok(val_buffer.into_boxed_slice())
}

pub fn write_variable_length<W: Write>(buf: &mut W, s: &[u8]) -> std::io::Result<()> {
    let mut vbuf = [0u8; MAX_VARINT_LEN];
    let len = VarInt::encode(s.len() as i64, &mut vbuf);
    buf.write_all(len)?;
    buf.write_all(s)?;
    Ok(())
}

pub fn read_type_from_buf<R: Read + Seek>(
    dtype: DataTypeKind,
    buf: &mut R,
) -> std::io::Result<DataType> {
    if dtype.is_fixed_size() {
        let mut temp_buf = vec![0u8; dtype.size().unwrap()];
        buf.read_exact(&mut temp_buf)?;
        Ok(reinterpret_cast(dtype, &temp_buf)?.0.to_owned())
    } else {
        let varint = VarInt::read_buf(buf)?;
        let (len, offset) = VarInt::from_encoded_bytes(&varint)?;
        let len_usize: usize = len.try_into().unwrap();
        let mut data_buf = vec![0u8; len_usize];
        buf.read_exact(&mut data_buf)?;

        let mut full_buf = Vec::with_capacity(offset + len_usize);
        full_buf.extend_from_slice(&varint[..offset]);
        full_buf.extend_from_slice(&data_buf);

        Ok(reinterpret_cast(dtype, &full_buf)?.0.to_owned())
    }
}



/// Fixed-size aligned mem buffer with vec-like semantics
#[derive(Debug)]
struct MemBuffer {
    cursor: std::io::Cursor<Box<[u8]>>,
    layout: Layout,
}

impl MemBuffer {
    fn alloc(size: usize, alignment: usize) -> std::io::Result<Self> {
        let aligned_size = size.next_multiple_of(alignment);

        let layout = Layout::from_size_align(aligned_size, alignment)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let inner = unsafe {
            let ptr = alloc_zeroed(layout);
            debug_assert!(
                (ptr as usize).is_multiple_of(alignment),
                "Invalid allocation, layout.from_size_align did not return an aligned buffer"
            );
            if ptr.is_null() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to allocate aligned buffer",
                ));
            }
            let slice = std::slice::from_raw_parts_mut(ptr, aligned_size);
            Box::from_raw(slice)
        };

        let mut cursor = std::io::Cursor::new(inner);
        cursor.seek(SeekFrom::Start(0))?;
        Ok(Self { cursor, layout })
    }

    // This is a workaraound because Rust Vec moves the data around when you [extend from slice], but here what i want to do is to extend it without moving the data.
    // TODO. I cannot belive there is no other way to do this stuff in rust.
    fn extend_from_slice(&mut self, slice: &[u8]) -> std::io::Result<()> {
        let alignment = self.mem_alignment();
        let old_pos = self.position();
        let required_length = self.position() + slice.len();
        let mut new_vec = Self::alloc(required_length, alignment)?;
        new_vec[..self.position()].copy_from_slice(&self.as_ref()[..self.position()]);
        new_vec[self.position()..self.position() + slice.len()].copy_from_slice(slice);
        new_vec.set_position(required_length as u64);
        *self = new_vec;
        Ok(())
    }

    fn empty() -> Self {
        Self{ cursor: std::io::Cursor::new(Box::new([])),
            layout: Layout::from_size_align(0, 1).unwrap()
        }
    }

    fn set_position(&mut self, pos: u64) {
        self.cursor.set_position(pos);
    }

    fn len(&self) -> usize {
        self.cursor.get_ref().len()
    }

    fn position(&self) -> usize {
        self.cursor.position() as usize
    }

    fn is_empty(&self) -> bool {
        self.cursor.get_ref().is_empty()
    }

    fn as_ptr(&self) -> *const u8 {
        self.cursor.get_ref().as_ptr()
    }

    fn mem_alignment(&self) -> usize {
        self.layout.align()
    }
}

impl Write for MemBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.cursor.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.cursor.flush()
    }
}

impl Read for MemBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Seek for MemBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl AsRef<[u8]> for MemBuffer {
    fn as_ref(&self) -> &[u8] {
        self.cursor.get_ref().as_ref()
    }
}

impl AsMut<[u8]> for MemBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.cursor.get_mut().as_mut()
    }
}

impl std::ops::Deref for MemBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::ops::DerefMut for MemBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}
