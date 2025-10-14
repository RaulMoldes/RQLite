use std::fmt;

/// Size of the SQLite header in bytes.
pub(crate) const HEADER_SIZE: usize = 110;
/// Magic string that identifies the RQLite file format.
pub(crate) const RQLITE_HEADER_STRING: &[u8; 16] = b"RQLite format 3\0";
/// Default max embedded payload fraction.C
pub(crate) const MAX_PAYLOAD_FRACTION: f32 = 0.95;
/// Default min embedded payload fraction.
pub(crate) const MIN_PAYLOAD_FRACTION: f32 = 0.1;
pub(crate) const MIN_PAGE_SIZE: u32 = 512;

pub(crate) const MAX_PAGE_SIZE: u32 = 65536;
/// Default leaf payload fraction.
pub(crate) const LEAF_PAYLOAD_FRACTION: f32 = 0.95;
/// Default cache size of the database (num pages)
pub(crate) const MAX_CACHE_SIZE: u32 = 10000;
/// Default page size of the database
pub(crate) const PAGE_SIZE: u32 = 4 * 1024; // 4KB

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ReadWriteVersion {
    Legacy = 1,
    Wal = 2,
}

impl fmt::Display for ReadWriteVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadWriteVersion::Legacy => writeln!(f, "Legacy"),
            ReadWriteVersion::Wal => writeln!(f, "WAL"),
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum TextEncoding {
    Utf8 = 1,
    Utf16le = 2,
    Utf16be = 3,
}

impl fmt::Display for TextEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TextEncoding::Utf8 => writeln!(f, "utf-8"),
            TextEncoding::Utf16le => writeln!(f, "utf-16 (little endian)"),
            TextEncoding::Utf16be => writeln!(f, "utf-16 (big endian)"),
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum IncrementalVaccum {
    Enabled = 1,
    Disabled = 0,
}

impl fmt::Display for IncrementalVaccum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IncrementalVaccum::Enabled => writeln!(f, "enabled"),
            IncrementalVaccum::Disabled => writeln!(f, "disabled"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RQLiteConfig {
    pub(crate) incremental_vacuum_mode: IncrementalVaccum,
    pub(crate) read_write_version: ReadWriteVersion,
    pub(crate) text_encoding: TextEncoding,
    pub(crate) cache_size: Option<u32>,
    pub(crate) page_size: u32,
}

impl Default for RQLiteConfig {
    fn default() -> Self {
        Self {
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
            cache_size: None,
            page_size: PAGE_SIZE,
        }
    }
}

impl RQLiteConfig {
    fn new(
        page_size: u32,
        read_write_version: ReadWriteVersion,
        text_encoding: TextEncoding,
        incremental_vacuum_mode: IncrementalVaccum,
    ) -> Self {
        let page_size = page_size
            .next_power_of_two()
            .clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        Self {
            incremental_vacuum_mode,
            read_write_version,
            text_encoding,
            cache_size: None,
            page_size,
        }
    }

    fn with_cache_size(
        page_size: u32,
        cache_size: u32,
        read_write_version: ReadWriteVersion,
        text_encoding: TextEncoding,
        incremental_vacuum_mode: IncrementalVaccum,
    ) -> Self {
        let cache_size = cache_size.next_power_of_two().max(MAX_CACHE_SIZE);
        let mut config = Self::new(
            page_size,
            read_write_version,
            text_encoding,
            incremental_vacuum_mode,
        );
        config.cache_size = Some(cache_size);
        config
    }
}
