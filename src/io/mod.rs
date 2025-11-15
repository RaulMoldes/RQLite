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
use std::io::{Read, Seek, Write};
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
