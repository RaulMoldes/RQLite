use crate::{
    AxmosDBConfig, DEFAULT_CACHE_SIZE, PAGE_ALIGNMENT,
    io::{
        cache::PageCache,
        disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
        frames::MemFrame,
        wal::WriteAheadLog,
    },
    make_shared,
    storage::{
        core::{
            buffer::MemBlock,
            traits::{Buffer, BufferOps},
        },
        latches::PageLatch,
        page::{OverflowPage, PageZero, PageZeroHeader},
        wal::OwnedRecord,
    },
    types::{PAGE_ZERO, PageId},
};

use std::{
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Implementation of a pager.
#[derive(Debug)]
pub struct Pager {
    file: DBFile,
    wal: WriteAheadLog, // TODO: Implement recoverability from the [WAL].
    cache: PageCache,
    page_zero: Option<MemFrame>, // Page zero is always in memory
}

impl Write for Pager {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Read for Pager {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for Pager {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
impl FileOperations for Pager {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = DBFile::create(&path)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::create(wal_path)?;
        let cache = PageCache::with_capacity(DEFAULT_CACHE_SIZE as usize);

        Ok(Self {
            file,
            cache,
            wal,
            page_zero: None,
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        let file = DBFile::open(&path)?;
        let block_size = FileSystem::block_size(&path)?;
        let cache = PageCache::with_capacity(DEFAULT_CACHE_SIZE);
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        let wal = WriteAheadLog::open(wal_path)?;

        let mut pager = Self {
            file,
            cache,
            wal,
            page_zero: None,
        };

        let mut page_zero = pager.load_page_zero(block_size)?;
        let actual_block_size = page_zero.metadata().page_size;

        // If the target page size happens to be greater than the filesystem block size we might have to reload.
        if actual_block_size > block_size as u32 {
            page_zero = pager.load_page_zero(actual_block_size as usize)?;
        };

        pager.page_zero = Some(MemFrame::from(page_zero));

        // TODO. MUST WRITE THE RECOVER FUNCTION TO REPLAY THE WAL HERE.
        Ok(pager)
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        DBFile::remove(&path)?;
        let dir = path.as_ref().parent().expect("Not a directory");
        let wal_path = dir.join("axmos.log");
        WriteAheadLog::remove(&wal_path)?;
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.wal.sync_all()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.truncate()?;
        self.cache.clear();
        self.wal.truncate()?;
        Ok(())
    }
}

impl Pager {
    /// Compute the offset at which a page is placed given the database header size and the page id.
    /// [`PAGEZERO`] contains the header.
    ///
    /// Then all pages are placed sequentially, and as all pages are equal size, one can get the offset by computing the page id * size.
    ///
    /// So the layout of the database file is the following:
    ///
    /// [PAGE ZERO          ] Offset 0
    /// [PAGE 1             ] Offset 1 * Page size
    /// [PAGE 2             ] Offset 2 * Page size
    /// [PAGE 3             ] Offset 3 * Page size
    /// [....               ]
    fn page_offset(page_id: PageId, page_size: u64) -> u64 {
        u64::from(page_id) * page_size
    }

    pub(crate) fn from_config(config: AxmosDBConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        let mut pager = Pager::create(path)?;
        let page_zero = pager.alloc_page_zero(config)?;
        pager.page_zero = Some(MemFrame::from(page_zero));
        Ok(pager)
    }

    /// Allocates page zero from the provided configuration
    fn alloc_page_zero(&mut self, config: AxmosDBConfig) -> io::Result<PageZero> {
        self.cache.set_capacity(config.cache_size as usize);

        let mut page_zero: PageZero = PageZero::from_config(config);
        page_zero.metadata_mut().page_size = config.page_size;
        page_zero.metadata_mut().min_keys = config.min_keys_per_page;
        page_zero.metadata_mut().cache_size = config.cache_size;
        page_zero.metadata_mut().num_siblings_per_side = config.num_siblings_per_side;

        self.write_block(PAGE_ZERO, page_zero.as_ref(), config.page_size as usize)?;

        Ok(page_zero)
    }

    /// Push a record to the write ahead log (implementation is not complete)
    pub(crate) fn push_to_log(&mut self, record: OwnedRecord) -> io::Result<()> {
        self.wal.push(record)
    }

    /// Load block zero from disk
    fn load_page_zero(&mut self, block_size: usize) -> io::Result<PageZero> {
        self.load_block::<PageZeroHeader>(0, block_size)
    }

    /// Load a block from disk at the given offset and of the given size.
    fn load_block<M>(&mut self, offset: u64, block_size: usize) -> io::Result<MemBlock<M>> {
        let mut block: MemBlock<M> = MemBlock::new(block_size);
        self.read_block(PAGE_ZERO, block.as_mut(), block_size)?;
        Ok(block)
    }

    /// Apply a read-only callback to any page type
    pub(crate) fn with_page<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<MemBlock<P::Header>> + From<P>,
        F: FnOnce(&P) -> R,
        for<'a> &'a P: TryFrom<&'a PageLatch, Error = IoError>,
    {
        let frame = self.read_page::<P>(id)?;
        let latch = frame.read();
        let page: &P = (&latch).try_into()?;
        Ok(f(page))
    }

    /// Apply a mutable callback to any page type
    pub(crate) fn with_page_mut<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<MemBlock<P::Header>> + From<P>,
        F: FnOnce(&mut P) -> R,
        for<'a> &'a mut P: TryFrom<&'a mut PageLatch, Error = IoError>,
    {
        let mut frame = self.read_page::<P>(id)?;
        frame.mark_dirty();
        let mut latch = frame.write();
        let page: &mut P = (&mut latch).try_into()?;

        Ok(f(page))
    }

    /// Apply a fallible read-only callback to any page type
    pub(crate) fn try_with_page<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<MemBlock<P::Header>> + From<P>,
        F: FnOnce(&P) -> io::Result<R>,
        for<'a> &'a P: TryFrom<&'a PageLatch, Error = IoError>,
    {
        let frame = self.read_page::<P>(id)?;
        let latch = frame.read();
        let page: &P = (&latch).try_into()?;
        f(page)
    }

    /// Apply a fallible mutable callback to any page type
    pub(crate) fn try_with_page_mut<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<MemBlock<P::Header>> + From<P>,
        F: FnOnce(&mut P) -> io::Result<R>,
        for<'a> &'a mut P: TryFrom<&'a mut PageLatch, Error = IoError>,
    {
        let mut frame = self.read_page::<P>(id)?;
        frame.mark_dirty();
        let mut latch = frame.write();
        let page: &mut P = (&mut latch).try_into()?;
        f(page)
    }

    /// Check if the pager is intialized
    pub(crate) fn is_initialized(&self) -> bool {
        self.page_zero.is_some()
    }

    /// Attempt to get page zero as a frame. Panics if we are not initialized.
    fn page_zero_frame(&self) -> io::Result<&MemFrame> {
        self.page_zero
            .as_ref()
            .ok_or(IoError::new(ErrorKind::Other, "pager not initialized"))
    }

    /// Attempt to get page zero as a frame. Panics if we are not initialized.
    fn page_zero_frame_mut(&mut self) -> io::Result<&mut MemFrame> {
        self.page_zero
            .as_mut()
            .ok_or(IoError::new(ErrorKind::Other, "pager not initialized"))
    }

    // Utility to execute a callback on page zero.
    fn with_page_zero<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&PageZero) -> R,
    {
        let frame = self.page_zero_frame()?;
        let latch = frame.read();
        let page: &PageZero = (&latch).try_into()?;
        Ok(f(page))
    }

    // Utility to execute a mutable callback on page zero.
    fn with_page_zero_mut<F, R>(&mut self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut PageZero) -> R,
    {
        let frame = self.page_zero_frame_mut()?;
        frame.mark_dirty();
        let mut latch = frame.write();
        let page: &mut PageZero = (&mut latch).try_into()?;
        Ok(f(page))
    }

    fn page_size(&self) -> io::Result<usize> {
        self.with_page_zero(|pz| pz.metadata().page_size as usize)
    }

    fn min_keys_per_page(&self) -> io::Result<usize> {
        self.with_page_zero(|pz| pz.metadata().min_keys as usize)
    }

    fn first_free_page(&self) -> io::Result<Option<PageId>> {
        self.with_page_zero(|pz| pz.metadata().first_free_page)
    }

    fn last_free_page(&self) -> io::Result<Option<PageId>> {
        self.with_page_zero(|pz| pz.metadata().last_free_page)
    }

    fn try_set_first_free_page(&mut self, value: Option<PageId>) -> io::Result<()> {
        self.with_page_zero_mut(|pz| pz.metadata_mut().first_free_page = value)?;
        Ok(())
    }

    fn try_set_last_free_page(&mut self, value: Option<PageId>) -> io::Result<()> {
        self.with_page_zero_mut(|pz| pz.metadata_mut().last_free_page = value)?;
        Ok(())
    }

    /// Validates that the provided value is aligned.
    fn validate_alignment_of(value: usize, align: usize) -> io::Result<()> {
        // Offset in the file must be aligned
        if !(value).is_multiple_of(align) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("invalid value {}: must be {}-byte aligned", value, align),
            ));
        }
        Ok(())
    }

    /// Write a block directly to the underlying disk using DirectIO.
    /// The caller must ensure that the size of the provided buffer is aligned for [O_DIRECT].
    fn write_block(
        &mut self,
        start_page_number: PageId,
        content: &[u8],
        block_size: usize,
    ) -> io::Result<()> {
        let offset = Self::page_offset(start_page_number, block_size as u64);

        Self::validate_alignment_of(offset as usize, PAGE_ALIGNMENT as usize)?;
        // Content pointer must also be aligned.
        Self::validate_alignment_of(content.as_ptr() as usize, PAGE_ALIGNMENT as usize)?;
        Self::validate_alignment_of(content.len() as usize, PAGE_ALIGNMENT as usize)?;

        // Seek in the file to the targete offset and write the content
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(content)?;

        Ok(())
    }

    /// Read a block back from the disk into an output buffer.
    fn read_block(
        &mut self,
        start_page_number: PageId,
        buffer: &mut [u8],
        block_size: usize,
    ) -> io::Result<()> {
        Self::validate_alignment_of(buffer.as_ptr() as usize, PAGE_ALIGNMENT as usize)?;
        Self::validate_alignment_of(buffer.len() as usize, PAGE_ALIGNMENT as usize)?;
        let offset = Self::page_offset(start_page_number, block_size as u64);
        Self::validate_alignment_of(offset as usize, PAGE_ALIGNMENT as usize)?;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    /// Allocate a new page with the given <H> header.
    pub(crate) fn allocate_page<P>(&mut self) -> io::Result<PageId>
    where
        MemFrame: From<P> + From<MemBlock<P::Header>>,
        P: BufferOps,
    {
        let (id, mut mem_page) = if let Some(page_id) = self.first_free_page()? {
            let next = self.with_page::<OverflowPage, _, _>(page_id, |overflow| overflow.next())?;

            self.try_set_first_free_page(next)?;

            if let Some(last_id) = self.last_free_page()?
                && last_id == page_id
            {
                self.try_set_last_free_page(None)?;
            }
            let page = self.read_page::<P>(page_id)?;
            (page_id, page.reinit_as::<P::Header>())
        } else {
            // TODO: Think how to best do this part
            let page = P::alloc(self.page_size()? as u32);
            let frame = MemFrame::from(page);
            let id = frame.page_number();
            (id, frame)
        };
        mem_page.mark_dirty();
        let id = mem_page.page_number();
        self.cache_frame(mem_page)?;

        Ok(id)
    }

    /// Adds a frame to the cache, possibly evicting another one from it and writing its contents to disk thereby.
    ///
    /// As we are doing write ahead logging instead of shadow paging, we can safely perform NO-STEAL, NO FORCE behaviour.
    /// - **NO-STEAL:** Dirty pages are not written to disk immediately upon modification unless eviction occurs.
    /// - **NO-FORCE:** Writes to disk do not happen automatically at transaction commit; only when evicted.
    fn cache_frame(&mut self, frame: MemFrame) -> io::Result<PageId> {
        let id = frame.page_number();
        if let Some(evicted) = self.cache.insert(frame)? {
            let evicted_id = evicted.page_number();

            if evicted.is_dirty() {
                let page_size = self.page_size()?;
                self.write_block(evicted_id, evicted.write().as_mut(), page_size)?;
            };
        };
        Ok(id)
    }

    /// Reads a page from the cache, if it is not found there it goes to the disk to get its contents
    ///
    /// The marker [H] indicates the type of page that is going to be read.
    ///
    /// After reading it, if any page was evicted, goes to the disk to write its contents.
    ///
    /// See [Pager::write_block]
    pub(crate) fn read_page<P>(&mut self, id: PageId) -> io::Result<MemFrame>
    where
        P: Buffer,
        MemFrame: From<MemBlock<P::Header>> + From<P>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            let frame = self
                .page_zero
                .as_ref()
                .ok_or(IoError::new(ErrorKind::Other, "Pager not initialized!"))?
                .clone();
            return Ok(frame);
        };

        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };

        let page_size = self.page_size()?;

        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer: MemBlock<P::Header> = MemBlock::new(page_size);

        self.read_block(id, buffer.as_mut(), page_size)?;
        let page = buffer.cast::<P::Header>();

        let mem_page = MemFrame::from(page);
        self.cache_frame(mem_page)?;

        Ok(self.cache.get(&id).unwrap())
    }

    pub(crate) fn dealloc_page<P>(&mut self, id: PageId) -> io::Result<()>
    where
        P: Buffer,
        MemFrame: From<MemBlock<<P as Buffer>::Header>> + From<P>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot deallocate PageZero!",
            ));
        };

        let first_free_page = self.first_free_page()?;

        // Set the first free page in the header
        if first_free_page.is_none() {
            self.try_set_first_free_page(Some(id))?;
        };

        if let Some(last_free_id) = self.last_free_page()? {
            // Read the free page and set the next page.

            self.with_page_mut::<OverflowPage, _, _>(last_free_id, |overflow| {
                overflow.metadata_mut().next = Some(last_free_id);
            })?;
        };

        let mem_page = self.read_page::<P>(id)?.dealloc();
        self.try_set_first_free_page(Some(id))?;
        let page_size = self.page_size()?;
        // Probably not necessary
        self.write_block(id, mem_page.write().as_mut(), page_size)?;

        Ok(())
    }
}

make_shared! {SharedPager, Pager}
