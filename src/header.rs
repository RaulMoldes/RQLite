//! # Header Module
//!
//! This module defines the structure and functionality for handling the SQLite database header.
//! It includes methods for reading and writing the header, as well as validating the page size.
//! The header is a 100-byte structure that contains the metadata about the SQLite database file.
//! Link to SQLite documentation: https://www.sqlite.org/fileformat.html#fileformat_header
//!
//! HEADER STRUCTURE
//! page_size: u32, // Size of a page in bytes
//! write_version: u8, // Write version of the database
//! read_version: u8, // Read version of the database
//! reserved_space: u8, // Reserved space at the end of each page
//! max_payload_fraction: u8, // Maximum payload fraction for fractional pages
//! min_payload_fraction: u8, // Minimum payload fraction for fractional pages
//! leaf_payload_fraction: u8, // Leaf payload fraction
//! change_counter: u32, // Change counter for the database. Useful for handling multiple accessors.
//! database_size: u32, // Size of the database in pages
//! first_freelist_trunk_page: u32, // First page of the free list trunk
//! freelist_pages: u32, // Total number of pages in the free list
//! schema_cookie: u32, // Schema cookie for the database
//! schema_format_number: u32, // Format number of the schema
//! default_cache_size: u32, // Default cache size
//! largest_root_btree_page: u32, // Largest root B-tree page
//! text_encoding: u32, // Text encoding of the database
//! user_version: u32, // User version of the database
//! incremental_vacuum_mode: u32, // Incremental vacuum mode
//! application_id: u32, // Application ID
//! reserved: [u8; 20], // Reserved for future expansion
//! version_valid_for: u32, // Version valid for
//! sqlite_version_number: u32, // SQLite version number
//!
//!
//! Currently I am serializing the header in big-endian format. This can be improved with a more dynamic approach if I am able to detect the endianness of the system..
//
use std::fmt;
use std::io::{self, Read, Write};

/// Size of the SQLite header in bytes.
pub const HEADER_SIZE: usize = 100;
/// Magic string that identifies the RQLite file format.
pub const SQLITE_HEADER_STRING: &[u8; 16] = b"SQLite format 3\0";

/// Represents the SQLite database header.
#[derive(Debug, Clone)]
pub struct Header {
    pub page_size: u32, // Always a power of 2 between 512 and 65536
    pub write_version: u8,
    pub read_version: u8,
    /// Nº of bytes reserved at the end of each page..
    pub reserved_space: u8,

    // Payload fractions
    pub max_payload_fraction: u8,
    pub min_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    /// History of changes to the database.
    /// Useful for handling multiple accessors.
    pub change_counter: u32,
    /// Total number of pages in the database.
    /// This is the number of pages in the database file.
    pub database_size: u32,
    /// Pointer to the first page of the free list trunk.
    pub first_freelist_trunk_page: u32,
    /// Number of pages in the free list trunk.
    pub freelist_pages: u32,
    /// Cookie del esquema.
    pub schema_cookie: u32,
    /// Schema format number.
    /// This is used to determine if the schema has changed. Useful for backward and forward compatibility.
    pub schema_format_number: u32,
    /// Buffer size for the default cache (number of pages).
    pub default_cache_size: u32,
    /// Page number of the largest root B-tree.
    pub largest_root_btree_page: u32,
    /// Encoding of the text in the database.
    /// 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be.
    pub text_encoding: u32,
    /// User version of the database.
    pub user_version: u32,
    /// Incremental vacuum mode.
    pub incremental_vacuum_mode: u32,
    /// Application ID.
    pub application_id: u32,
    /// Reserved for future expansion.
    pub reserved: [u8; 20],
    /// SQlite version valid for.
    /// This is used to determine if the database file is compatible with the current version of SQLite.
    pub version_valid_for: u32,
    /// Current SQLite version number.
    /// This is the SQLIte version number that created the database file.
    pub sqlite_version_number: u32,
}

impl Default for Header {
    /// Creates a new header with default values.
    ///
    fn default() -> Self {
        Header {
            page_size: 4096,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            max_payload_fraction: 64,
            min_payload_fraction: 32,
            leaf_payload_fraction: 32,
            change_counter: 0,
            database_size: 0,
            first_freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format_number: 4,
            default_cache_size: 0,
            largest_root_btree_page: 0,
            text_encoding: 1, // 1 = UTF-8
            user_version: 0,
            incremental_vacuum_mode: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            sqlite_version_number: 0,
        }
    }
}

impl Header {
    /// Creates a new header with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates a new header with a specified page size.
    ///
    /// Parameters:
    /// * `page_size` - Size of the page in bytes. Must be a power of 2 between 512 and 65536.
    ///
    /// Returns an `io::Result` with the header.
    pub fn with_page_size(page_size: u32) -> io::Result<Self> {
        if !is_valid_page_size(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Invalid PAGE SIZE: {}. Should be a power of 2 between 512 and 65536",
                    page_size
                ),
            ));
        }

        let mut header = Self::new();
        header.page_size = page_size;
        Ok(header)
    }

    /// Reads a header from a source. The source must implement the `Read` trait.
    ///
    /// Parameters:
    /// * `reader` - Source that implements `Read`.
    ///
    /// Returns an `io::Result` with the header.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buffer)?;

        // Verify the header string
        // The first 16 bytes should be the SQLite header string
        // If not, return an error
        // This can happen if the file is corrupted or not a SQLite file
        if &buffer[0..16] != SQLITE_HEADER_STRING {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Database corrupted: Invalid header string",
            ));
        }

        // Read the page size
        // The page size is stored in bytes 16 and 17
        // If the page size is 1, it means 65536 bytes
        // Otherwise, convert the bytes to a u16 and then to a u32
        // The page size is stored in big-endian format
        let page_size = match u16::from_be_bytes([buffer[16], buffer[17]]) {
            1 => 65536, // Special case for 65536
            size => u32::from(size),
        };

        if !is_valid_page_size(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page size: {}", page_size),
            ));
        }

        Ok(Header {
            page_size,
            write_version: buffer[18],
            read_version: buffer[19],
            reserved_space: buffer[20],
            max_payload_fraction: buffer[21],
            min_payload_fraction: buffer[22],
            leaf_payload_fraction: buffer[23],
            change_counter: u32::from_be_bytes([buffer[24], buffer[25], buffer[26], buffer[27]]),
            database_size: u32::from_be_bytes([buffer[28], buffer[29], buffer[30], buffer[31]]),
            first_freelist_trunk_page: u32::from_be_bytes([
                buffer[32], buffer[33], buffer[34], buffer[35],
            ]),
            freelist_pages: u32::from_be_bytes([buffer[36], buffer[37], buffer[38], buffer[39]]),
            schema_cookie: u32::from_be_bytes([buffer[40], buffer[41], buffer[42], buffer[43]]),
            schema_format_number: u32::from_be_bytes([
                buffer[44], buffer[45], buffer[46], buffer[47],
            ]),
            default_cache_size: u32::from_be_bytes([
                buffer[48], buffer[49], buffer[50], buffer[51],
            ]),
            largest_root_btree_page: u32::from_be_bytes([
                buffer[52], buffer[53], buffer[54], buffer[55],
            ]),
            text_encoding: u32::from_be_bytes([buffer[56], buffer[57], buffer[58], buffer[59]]),
            user_version: u32::from_be_bytes([buffer[60], buffer[61], buffer[62], buffer[63]]),
            incremental_vacuum_mode: u32::from_be_bytes([
                buffer[64], buffer[65], buffer[66], buffer[67],
            ]),
            application_id: u32::from_be_bytes([buffer[68], buffer[69], buffer[70], buffer[71]]),
            reserved: {
                let mut reserved = [0u8; 20];
                reserved.copy_from_slice(&buffer[72..92]);
                reserved
            },
            version_valid_for: u32::from_be_bytes([buffer[92], buffer[93], buffer[94], buffer[95]]),
            sqlite_version_number: u32::from_be_bytes([
                buffer[96], buffer[97], buffer[98], buffer[99],
            ]),
        })
    }

    /// Writes the header to a destination. The destination must implement the `Write` trait.
    ///
    /// Parameters:
    /// * `writer` - Destination that implements `Write`.
    ///
    /// Returns an `io::Result` indicating success or failure.
    ///
    /// # Errors
    /// This function will return an error if the write operation fails.
    /// # Panics
    /// This function will panic if the header size is not equal to `HEADER_SIZE`.
    /// # Safety
    /// This function is safe to call as long as the header is valid and the writer is valid.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = [0u8; HEADER_SIZE];

        // First 16 bytes should be the SQLite header string
        buffer[0..16].copy_from_slice(SQLITE_HEADER_STRING);

        // Write the page size
        let page_size_bytes = if self.page_size == 65536 {
            [0, 1] // Special case for 65536 in big-endian format
        } else {
            ((self.page_size) as u16).to_be_bytes()
        };
        buffer[16..18].copy_from_slice(&page_size_bytes);

        // Write the rest of the header
        buffer[18] = self.write_version;
        buffer[19] = self.read_version;
        buffer[20] = self.reserved_space;
        buffer[21] = self.max_payload_fraction;
        buffer[22] = self.min_payload_fraction;
        buffer[23] = self.leaf_payload_fraction;
        buffer[24..28].copy_from_slice(&self.change_counter.to_be_bytes());
        buffer[28..32].copy_from_slice(&self.database_size.to_be_bytes());
        buffer[32..36].copy_from_slice(&self.first_freelist_trunk_page.to_be_bytes());
        buffer[36..40].copy_from_slice(&self.freelist_pages.to_be_bytes());
        buffer[40..44].copy_from_slice(&self.schema_cookie.to_be_bytes());
        buffer[44..48].copy_from_slice(&self.schema_format_number.to_be_bytes());
        buffer[48..52].copy_from_slice(&self.default_cache_size.to_be_bytes());
        buffer[52..56].copy_from_slice(&self.largest_root_btree_page.to_be_bytes());
        buffer[56..60].copy_from_slice(&self.text_encoding.to_be_bytes());
        buffer[60..64].copy_from_slice(&self.user_version.to_be_bytes());
        buffer[64..68].copy_from_slice(&self.incremental_vacuum_mode.to_be_bytes());
        buffer[68..72].copy_from_slice(&self.application_id.to_be_bytes());
        buffer[72..92].copy_from_slice(&self.reserved);
        buffer[92..96].copy_from_slice(&self.version_valid_for.to_be_bytes());
        buffer[96..100].copy_from_slice(&self.sqlite_version_number.to_be_bytes());

        writer.write_all(&buffer)
    }

    /// Increments the change counter.
    /// This is useful for handling multiple accessors to the database.
    /// The change counter is used to determine if the database has changed since it was last accessed.
    /// This is useful for handling multiple accessors to the database.
    /// wrapping_add is used to avoid panicing on overflow.
    pub fn increment_change_counter(&mut self) {
        self.change_counter = self.change_counter.wrapping_add(1);
    }
}

// Implementation of Display trait for Header
impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SQLite Database Header:")?;
        writeln!(f, "  Page Size: {} bytes", self.page_size)?;
        writeln!(f, "  Write Version: {}", self.write_version)?;
        writeln!(f, "  Read Version: {}", self.read_version)?;
        writeln!(f, "  Reserved Space: {} bytes", self.reserved_space)?;
        writeln!(f, "  Change Counter: {}", self.change_counter)?;
        writeln!(f, "  Database Size: {} pages", self.database_size)?;
        writeln!(f, "  Schema Format Number: {}", self.schema_format_number)?;
        writeln!(
            f,
            "  Text Encoding: {}",
            text_encoding_to_string(self.text_encoding)
        )?;
        writeln!(f, "  User Version: {}", self.user_version)?;
        writeln!(f, "  Application ID: {:#x}", self.application_id)?;
        writeln!(f, "  SQLite Version: {}", self.sqlite_version_number)
    }
}

/// Utility function to check if a page size is valid.
/// A valid page size is a power of 2 between 512 and 65536.
///
/// # Parameters:
/// * `size` - Size of the page in bytes.
/// # Returns true if the size is valid, false otherwise.
/// # Errors
/// This function will return an error if the size is not a power of 2 or if it is out of range.
/// # Panics
/// This function will panic if the size is not a power of 2 or if it is out of range.
/// # Safety
/// This function is safe to call as long as the size is valid.
fn is_valid_page_size(size: u32) -> bool {
    if !(512..=65536).contains(&size) {
        return false;
    }

    // Verifies if the size is a power of 2
    // A number is a power of 2 if it has only one bit set in its binary representation
    (size & (size - 1)) == 0
}

/// Utility function to convert the text encoding to a string.
/// # Parameters:
/// * `encoding` - Encoding number. Valid values are 1 (UTF-8), 2 (UTF-16le), and 3 (UTF-16be).
/// # Returns a string representation of the encoding.
/// # Errors
/// This function will return an error if the encoding is not recognized.
/// # Panics
/// This function will panic if the encoding is not recognized.
/// # Safety
/// This function is safe to call as long as the encoding is valid.
fn text_encoding_to_string(encoding: u32) -> String {
    match encoding {
        1 => "UTF-8".to_string(),
        2 => "UTF-16le".to_string(),
        3 => "UTF-16be".to_string(),
        _ => format!("Unknown ({})", encoding),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_default() {
        let header = Header::default();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.write_version, 1);
        assert_eq!(header.read_version, 1);
        assert_eq!(header.schema_format_number, 4);
    }

    #[test]
    fn test_with_page_size_valid() {
        let header = Header::with_page_size(8192).unwrap();
        assert_eq!(header.page_size, 8192);
    }

    #[test]
    fn test_with_page_size_invalid() {
        let result = Header::with_page_size(1000);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_serialization() {
        let header = Header::default();
        let mut buffer = Vec::new();

        // Write the header to the buffer
        header.write_to(&mut buffer).unwrap();

        // Read the header back from the buffer
        let mut cursor = Cursor::new(buffer);
        let read_header = Header::read_from(&mut cursor).unwrap();

        // Verify that the read header matches the original header
        assert_eq!(header.page_size, read_header.page_size);
        assert_eq!(header.write_version, read_header.write_version);
        assert_eq!(header.read_version, read_header.read_version);
        assert_eq!(
            header.schema_format_number,
            read_header.schema_format_number
        );
    }

    #[test]
    fn test_is_valid_page_size() {
        // Valid sizes
        assert!(is_valid_page_size(512));
        assert!(is_valid_page_size(1024));
        assert!(is_valid_page_size(4096));
        assert!(is_valid_page_size(8192));
        assert!(is_valid_page_size(16384));
        assert!(is_valid_page_size(32768));
        assert!(is_valid_page_size(65536));

        // Invalid sizes
        assert!(!is_valid_page_size(511)); // Less than minimum
        assert!(!is_valid_page_size(513)); // Not a power of 2
        assert!(!is_valid_page_size(1000)); // Not a power of 2
        assert!(!is_valid_page_size(65537)); // Greater than maximum
    }
}
