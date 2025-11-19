/// Magic string that identifies the AxmosDB file format.
pub(crate) const AXMO: u32 = 0x41584D4F;
/// Maximum page size is 64 KiB.
pub(crate) const MIN_PAGE_SIZE: u32 = 4096;
pub(crate) const MAX_PAGE_SIZE: u32 = 65536;
/// Default cache size of the database (num pages)
pub(crate) const DEFAULT_CACHE_SIZE: u16 = 10000;
/// Default page size of the database
pub(crate) const DEFAULT_PAGE_SIZE: u32 = MIN_PAGE_SIZE; // 4KB

/// Cells are aligned to 64 bits for memory efficiency.
pub(crate) const CELL_ALIGNMENT: u8 = 64;

/// [`O_DIRECT`] flag in Linux systems requires that buffers are aligned to at least the logical block size of the block device which is typically 4096 bytes.
/// TODO: There should be a way to query this value to the system dynamically.
/// Must read: https://stackoverflow.com/questions/53902811/why-direct-i-o-requires-alignments
pub(crate) const PAGE_ALIGNMENT: u32 = MIN_PAGE_SIZE;
crate::repr_enum!(pub(crate) enum ReadWriteVersion: u8 {
    Legacy = 1,
    Wal = 2,
});

crate::repr_enum!(pub enum TextEncoding: u32 {
    Utf8 = 1,
    Utf16le = 2,
    Utf16be = 3,
});
crate::repr_enum!(pub(crate) enum IncrementalVaccum: u32 {
    Enabled = 1,
    Disabled = 0,
});

#[derive(Debug, Clone, Copy)]
pub(crate) struct AxmosDBConfig {
    pub(crate) incremental_vacuum_mode: IncrementalVaccum,
    pub(crate) read_write_version: ReadWriteVersion,
    pub(crate) text_encoding: TextEncoding,
    pub(crate) cache_size: Option<u16>,
    pub(crate) page_size: u32,
}

impl Default for AxmosDBConfig {
    fn default() -> Self {
        Self {
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
            cache_size: None,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

impl AxmosDBConfig {
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
        cache_size: u16,
        read_write_version: ReadWriteVersion,
        text_encoding: TextEncoding,
        incremental_vacuum_mode: IncrementalVaccum,
    ) -> Self {
        let cache_size = cache_size.next_power_of_two().max(DEFAULT_CACHE_SIZE);
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
