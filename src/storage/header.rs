//! # Header Module
//!
//! This module defines the structure and functionality for handling the SQLite database header.
//! It includes methods for reading and writing the header, as well as validating the page size.
//! The header is a 100-byte structure that contains the metadata about the SQLite database file.
//! Link to SQLite documentation: https://www.sqlite.org/fileformat.html#fileformat_header
//!
//! To achieve maximum portability, we always serialize and deserialize everything using big endian format.
use crate::serialization::Serializable;
use crate::{
    IncrementalVaccum, ReadWriteVersion, TextEncoding, HEADER_SIZE, LEAF_PAYLOAD_FRACTION,
    MAX_PAGE_SIZE, MAX_PAYLOAD_FRACTION, MIN_PAGE_SIZE, MIN_PAYLOAD_FRACTION, PAGE_SIZE,
    RQLITE_HEADER_STRING,
};
use std::fmt;
use std::io::{self, Read, Write};

/// Represents the RQLite database header.
/// Reference: https://www.sqlite.org/fileformat.html
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Header {
    /// Database page size. Must be a power of two between 512 and 65536
    pub(crate) page_size: u32,
    /// File format write version. 1 for legacy; 2 for WAL.
    pub(crate) write_version: ReadWriteVersion,
    /// File format read version. 1 for legacy; 2 for WAL.
    pub(crate) read_version: ReadWriteVersion,
    /// Nº of bytes reserved at the end of each page. Usually is zero.
    pub(crate) reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    pub(crate) max_payload_fraction: u8,
    /// Minimum embedded payload fraction. Must be 32.
    pub(crate) min_payload_fraction: u8,
    /// Leaf payload fraction. Must be 32.
    pub(crate) leaf_payload_fraction: u8,
    /// File change counter.
    pub(crate) change_counter: u32,
    /// Total number of pages in the database.
    pub(crate) database_size: u32,
    /// Page number of the first freelist trunk page.
    pub(crate) first_freelist_trunk_page: u32,
    /// Number of pages in the free list trunk.
    pub(crate) freelist_pages: u32,
    /// Schema cookie
    pub(crate) schema_cookie: u32,
    /// Schema format number.
    /// This is used to determine if the schema has changed. Useful for backward and forward compatibility.
    pub(crate) schema_format_number: u32,
    /// Buffer size for the default cache (number of pages).
    pub(crate) default_cache_size: u32,
    /// Page number of the largest root B-tree.
    pub(crate) largest_root_btree_page: u32,
    /// Encoding of the text in the database.
    /// 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be.
    pub(crate) text_encoding: TextEncoding,
    /// User version of the database.
    pub(crate) user_version: u32,
    /// Incremental vacuum mode.
    /// Non zero for incremental vaccum, zero for disabled.
    pub(crate) incremental_vacuum_mode: IncrementalVaccum,
    /// Application ID.
    pub(crate) application_id: u32,
    /// Reserved for future expansion.
    pub(crate) reserved: [u8; 20],
    /// RQlite version valid for.
    /// This is used to determine if the database file is compatible with the current version of RQLite.
    pub(crate) version_valid_for: u32,
    /// Current SQLite version number.
    /// This is the SQLIte version number that created the database file.
    pub(crate) rqlite_version_number: u32,
}

impl Default for Header {
    /// Creates a new header with default values.
    fn default() -> Self {
        Header {
            page_size: PAGE_SIZE,
            write_version: ReadWriteVersion::Wal,
            read_version: ReadWriteVersion::Wal,
            reserved_space: 0,
            max_payload_fraction: MAX_PAYLOAD_FRACTION,
            min_payload_fraction: MIN_PAYLOAD_FRACTION,
            leaf_payload_fraction: LEAF_PAYLOAD_FRACTION,
            change_counter: 0,
            database_size: 0,
            first_freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format_number: 0,
            default_cache_size: 0,
            largest_root_btree_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            rqlite_version_number: 0,
        }
    }
}

impl Header {
    /// Creates a new header with a specific page size
    pub fn create(page_size: u32) -> Self {
        Header {
            page_size: page_size
                .next_multiple_of(2)
                .clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE),
            write_version: ReadWriteVersion::Wal,
            read_version: ReadWriteVersion::Wal,
            reserved_space: 0,
            max_payload_fraction: MAX_PAYLOAD_FRACTION,
            min_payload_fraction: MIN_PAYLOAD_FRACTION,
            leaf_payload_fraction: LEAF_PAYLOAD_FRACTION,
            change_counter: 0,
            database_size: 0,
            first_freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format_number: 0,
            default_cache_size: 0,
            largest_root_btree_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            rqlite_version_number: 0,
        }
    }
}

impl Serializable for Header {
    /// Writes the Header to a writer using
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Magic string (bytes 0-15)
        writer.write_all(RQLITE_HEADER_STRING)?;
        // Page size (bytes 16-17)
        writer.write_all(&self.page_size.to_be_bytes()[2..4])?;
        // Write version (byte 18)
        writer.write_all(&[self.write_version as u8])?;
        // Read version (byte 19)
        writer.write_all(&[self.read_version as u8])?;
        // Reserved space (byte 20)
        writer.write_all(&[self.reserved_space])?;
        // Max payload fraction (byte 21)
        writer.write_all(&[self.max_payload_fraction])?;
        // Min payload fraction (byte 22)
        writer.write_all(&[self.min_payload_fraction])?;
        // Leaf payload fraction (byte 23)
        writer.write_all(&[self.leaf_payload_fraction])?;
        // Change counter (bytes 24-27)
        writer.write_all(&self.change_counter.to_be_bytes())?;
        // Database size (bytes 28-31)
        writer.write_all(&self.database_size.to_be_bytes())?;
        // First freelist trunk page (bytes 32-35)
        writer.write_all(&self.first_freelist_trunk_page.to_be_bytes())?;
        // Freelist pages (bytes 36-39)
        writer.write_all(&self.freelist_pages.to_be_bytes())?;
        // Schema cookie (bytes 40-43)
        writer.write_all(&self.schema_cookie.to_be_bytes())?;
        // Schema format number (bytes 44-47)
        writer.write_all(&self.schema_format_number.to_be_bytes())?;
        // Default cache size (bytes 48-51)
        writer.write_all(&self.default_cache_size.to_be_bytes())?;
        // Largest root btree page (bytes 52-55)
        writer.write_all(&self.largest_root_btree_page.to_be_bytes())?;
        // Text encoding (bytes 56-59)
        writer.write_all(&(self.text_encoding as u32).to_be_bytes())?;
        // User version (bytes 60-63)
        writer.write_all(&self.user_version.to_be_bytes())?;
        // Incremental vacuum mode (bytes 64-67)
        writer.write_all(&(self.incremental_vacuum_mode as u32).to_be_bytes())?;
        // Application ID (bytes 68-71)
        writer.write_all(&self.application_id.to_be_bytes())?;
        // Reserved (bytes 72-91)
        writer.write_all(&self.reserved)?;
        // Version valid for (bytes 92-95)
        writer.write_all(&self.version_valid_for.to_be_bytes())?;
        // RQLite version number (bytes 96-99)
        writer.write_all(&self.rqlite_version_number.to_be_bytes())?;
        Ok(())
    }

    /// Read the database header from a [`Read`] buffer.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buffer)?;

        // Validate the magic string
        if &buffer[0..16] != RQLITE_HEADER_STRING {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid RQLite header magic string",
            ));
        }

        // Page size (bytes 16-17)
        let page_size = u16::from_be_bytes([buffer[16], buffer[17]]) as u32;

        // Write version (byte 18)
        let write_version = match buffer[18] {
            1 => ReadWriteVersion::Legacy,
            2 => ReadWriteVersion::Wal,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid write version",
                ))
            }
        };

        // Read version (byte 19)
        let read_version = match buffer[19] {
            1 => ReadWriteVersion::Legacy,
            2 => ReadWriteVersion::Wal,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid read version",
                ))
            }
        };

        // Reserved space (byte 20)
        let reserved_space = buffer[20];

        // Payload fractions (bytes 21-23)
        let max_payload_fraction = buffer[21];
        let min_payload_fraction = buffer[22];
        let leaf_payload_fraction = buffer[23];

        // Change counter (bytes 24-27)
        let change_counter = u32::from_be_bytes([buffer[24], buffer[25], buffer[26], buffer[27]]);

        // Database size (bytes 28-31)
        let database_size = u32::from_be_bytes([buffer[28], buffer[29], buffer[30], buffer[31]]);

        // First freelist trunk page (bytes 32-35)
        let first_freelist_trunk_page =
            u32::from_be_bytes([buffer[32], buffer[33], buffer[34], buffer[35]]);

        // Freelist pages (bytes 36-39)
        let freelist_pages = u32::from_be_bytes([buffer[36], buffer[37], buffer[38], buffer[39]]);

        // Schema cookie (bytes 40-43)
        let schema_cookie = u32::from_be_bytes([buffer[40], buffer[41], buffer[42], buffer[43]]);

        // Schema format number (bytes 44-47)
        let schema_format_number =
            u32::from_be_bytes([buffer[44], buffer[45], buffer[46], buffer[47]]);

        // Default cache size (bytes 48-51)
        let default_cache_size =
            u32::from_be_bytes([buffer[48], buffer[49], buffer[50], buffer[51]]);

        // Largest root btree page (bytes 52-55)
        let largest_root_btree_page =
            u32::from_be_bytes([buffer[52], buffer[53], buffer[54], buffer[55]]);

        // Text encoding (bytes 56-59)
        let text_encoding =
            match u32::from_be_bytes([buffer[56], buffer[57], buffer[58], buffer[59]]) {
                1 => TextEncoding::Utf8,
                2 => TextEncoding::Utf16le,
                3 => TextEncoding::Utf16be,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid text encoding",
                    ))
                }
            };

        // User version (bytes 60-63)
        let user_version = u32::from_be_bytes([buffer[60], buffer[61], buffer[62], buffer[63]]);

        // Incremental vacuum mode (bytes 64-67)
        let incremental_vacuum_mode =
            match u32::from_be_bytes([buffer[64], buffer[65], buffer[66], buffer[67]]) {
                0 => IncrementalVaccum::Disabled,
                1 => IncrementalVaccum::Enabled,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid incremental vacuum mode",
                    ))
                }
            };

        // Application ID (bytes 68-71)
        let application_id = u32::from_be_bytes([buffer[68], buffer[69], buffer[70], buffer[71]]);

        // Reserved (bytes 72-91)
        let mut reserved = [0u8; 20];
        reserved.copy_from_slice(&buffer[72..92]);

        // Version valid for (bytes 92-95)
        let version_valid_for =
            u32::from_be_bytes([buffer[92], buffer[93], buffer[94], buffer[95]]);

        // RQLite version number (bytes 96-99)
        let rqlite_version_number =
            u32::from_be_bytes([buffer[96], buffer[97], buffer[98], buffer[99]]);

        Ok(Header {
            page_size,
            write_version,
            read_version,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
            leaf_payload_fraction,
            change_counter,
            database_size,
            first_freelist_trunk_page,
            freelist_pages,
            schema_cookie,
            schema_format_number,
            default_cache_size,
            largest_root_btree_page,
            text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            reserved,
            version_valid_for,
            rqlite_version_number,
        })
    }
}

// Implementation of Display trait for Header
impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SQLite Database Header:")?;
        writeln!(f, "  Page Size: {} bytes", self.page_size)?;
        writeln!(f, "  Write version: {}", self.write_version)?;
        writeln!(f, "  Read version: {}", self.read_version)?;
        writeln!(f, "  Reserved Space: {} bytes", self.reserved_space)?;
        writeln!(f, "  Change Counter: {}", self.change_counter)?;
        writeln!(f, "  Database Size: {} pages", self.database_size)?;
        writeln!(f, "  Schema Format Number: {}", self.schema_format_number)?;
        writeln!(f, "  Text encoding: {}", self.text_encoding)?;
        writeln!(
            f,
            "  Incremental vaccum mode: {}",
            self.incremental_vacuum_mode
        )?;
        writeln!(f, "  User Version: {}", self.user_version)?;
        writeln!(f, "  Application ID: {:#x}", self.application_id)?;
        writeln!(f, "  SQLite Version: {}", self.rqlite_version_number)?;
        Ok(())
    }
}
