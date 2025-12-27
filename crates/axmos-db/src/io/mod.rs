//! # IO Module
//! This module provides the io layer for the database, including buffer management,
//! disk management, and paging. It is responsible for managing how data is stored and retrieved
//! from disk, as well as caching frequently accessed data in memory to improve performance.
pub mod cache;
pub mod disk;
pub mod logger;

pub mod pager;
pub mod wal;

use crate::types::varint::{MAX_VARINT_LEN, VarInt};

//use crate::types::{DataType, DataTypeKind, VarInt, reinterpret_cast, varint::MAX_VARINT_LEN};
use std::io::{Read, Seek, Write};

#[cfg(test)]
mod tests;
