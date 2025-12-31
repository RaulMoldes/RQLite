use crate::{
    DBConfig, DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, DEFAULT_CACHE_SIZE,
    MAGIC, PAGE_ALIGNMENT, TransactionId, UInt64,
    core::SerializableType,
    io::{
        cache::PageCache,
        disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
        logger::Operation,
        wal::{AnalysisResult, WriteAheadLog},
    },
    make_shared,
    multithreading::frames::{AsReadLatch, AsWriteLatch, Frame, MemFrame},
    runtime::{RuntimeError, RuntimeResult, context::TransactionContext},
    schema::catalog::CatalogTrait,
    storage::{
        core::{buffer::MemBlock, traits::Buffer},
        page::{OverflowPage, PageZero, PageZeroHeader},
        tuple::{Tuple, TupleError},
    },
    tree::{
        accessor::{BtreeReadAccessor, BtreeWriteAccessor, TreeReader, TreeWriter},
        bplustree::{Btree, SearchResult},
    },
    types::{Lsn, PAGE_ZERO, PageId},
};

use std::{
    io::{self, Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Implementation of a pager.
pub struct Pager {
    file: DBFile,
    wal: WriteAheadLog,
    cache: PageCache,
    db_header: Option<PageZeroHeader>, // Page zero header is always in memory
}

impl<'a> FileOperations for Pager {
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
            db_header: None,
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
            db_header: None,
        };

        let page_zero = pager.load_page_zero(block_size)?;
        pager.db_header = Some(*page_zero.metadata());

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

pub struct WalRecuperator {
    ctx: TransactionContext<BtreeWriteAccessor>,
}

impl WalRecuperator {
    pub(crate) fn new_with_context(ctx: TransactionContext<BtreeWriteAccessor>) -> Self {
        Self { ctx }
    }

    /// Runs the recovery
    pub(crate) fn run_recovery(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        // let analysis = wal.run_analysis()?;
        self.run_undo(&analysis)?;
        self.run_redo(&analysis)?;
        //  wal.truncate()?;
        Ok(())
    }

    /// Run all the undo.
    pub(crate) fn run_undo(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        for redo_transaction in analysis.needs_undo.iter() {
            // Reapply all the operations of this transaction
            for lsn in analysis.try_iter_lsn(redo_transaction).ok_or(IoError::new(
                ErrorKind::NotSeekable,
                "transaction not found in th write ahead analysis",
            ))? {
                if let Some(delete_operation) = analysis.delete_ops.get(&lsn) {
                    let table_id = delete_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    let data = Tuple::from_slice_unchecked(delete_operation.undo())?;

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    btree.update(table.root(), data, schema)?;
                }
                if let Some(update_operation) = analysis.update_ops.get(&lsn) {
                    let table_id = update_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    let data = Tuple::from_slice_unchecked(update_operation.undo())?;

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    btree.update(table.root(), data, schema)?;
                }

                if let Some(insert_operation) = analysis.insert_ops.get(&lsn) {
                    let table_id = insert_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    let row_id = insert_operation
                        .row_id()
                        .map(|r| UInt64::from(r))
                        .ok_or(RuntimeError::InvalidState)?
                        .serialize()?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    let Ok(SearchResult::Found(position)) = btree.search(&row_id, schema) else {
                        return Err(RuntimeError::InvalidState);
                    };

                    let tuple = btree.with_cell_at(position, |bytes| {
                        let mut tuple = Tuple::from_slice_unchecked(bytes)?;
                        tuple.delete(self.ctx.tid())?;
              //          tuple.vacuum_with_schema(self.ctx.snapshot().xmin(), schema)?;

                        Ok::<Tuple, TupleError>(tuple)
                    })??;

                    btree.update(table.root(), tuple, schema)?;
                }
            }
        }

        Ok(())
    }

    /// Run all the redo.
    pub(crate) fn run_redo(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        for redo_transaction in analysis.needs_redo.iter() {
            // Reapply all the operations of this transaction
            for lsn in analysis.try_iter_lsn(redo_transaction).ok_or(IoError::new(
                ErrorKind::NotSeekable,
                "transaction not found in th write ahead analysis",
            ))? {
                if let Some(delete_operation) = analysis.delete_ops.get(&lsn) {
                    let table_id = delete_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    let row_id = delete_operation
                        .row_id()
                        .map(|r| UInt64::from(r))
                        .ok_or(RuntimeError::InvalidState)?
                        .serialize()?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    let Ok(SearchResult::Found(position)) = btree.search(&row_id, schema) else {
                        return Err(RuntimeError::InvalidState);
                    };

                    let tuple = btree.with_cell_at(position, |bytes| {
                        let mut tuple = Tuple::from_slice_unchecked(bytes)?;
                        tuple.delete(self.ctx.tid())?;
               //         tuple.vacuum_with_schema(self.ctx.snapshot().xmin(), schema)?;

                        Ok::<Tuple, TupleError>(tuple)
                    })??;

                    btree.update(table.root(), tuple, schema)?;
                }

                if let Some(update_operation) = analysis.update_ops.get(&lsn) {
                    let table_id = update_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    let data = Tuple::from_slice_unchecked(update_operation.redo())?;

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    btree.update(table.root(), data, schema)?;
                }

                if let Some(insert_operation) = analysis.insert_ops.get(&lsn) {
                    let table_id = insert_operation
                        .object_id()
                        .ok_or(RuntimeError::InvalidState)?;

                    // Get builder and snapshot
                    let builder = self.ctx.tree_builder();
                    let snapshot = self.ctx.snapshot();

                    // Locate the table
                    let table = self
                        .ctx
                        .catalog()
                        .get_relation(table_id, &builder, &snapshot)?;

                    let schema = table.schema();

                    let data = Tuple::from_slice_unchecked(insert_operation.redo())?;

                    // Locate the btree
                    let mut btree = builder.build_tree_mut(table.root());
                    btree.upsert(table.root(), data, schema)?;
                }
            }
        }

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

    pub(crate) fn from_config(config: DBConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        let mut pager = Pager::create(path)?;
        let page_zero = pager.alloc_page_zero(config)?;
        pager.db_header = Some(*page_zero.metadata());
        Ok(pager)
    }

    /// Allocates page zero from the provided configuration
    fn alloc_page_zero(&mut self, config: DBConfig) -> io::Result<PageZero> {
        self.cache.set_capacity(config.cache_size as usize);

        let mut page_zero: PageZero = PageZero::from_config(config);
        page_zero.metadata_mut().page_size = config.page_size as u32;
        page_zero.metadata_mut().min_keys = config.min_keys_per_page as u8;
        page_zero.metadata_mut().cache_size = config.cache_size as u16;
        page_zero.metadata_mut().num_siblings_per_side = config.num_siblings_per_side as u8;

        self.write_block(PAGE_ZERO, page_zero.as_ref(), config.page_size as usize)?;

        Ok(page_zero)
    }

    /// Syncs the header to the disk.
    fn sync_header(&mut self) -> io::Result<()> {
        let page_size = self.page_size();
        let mut zero_from_disk = self.load_page_zero(page_size)?;
        *zero_from_disk.metadata_mut() = *self.header_unchecked();
        self.write_block(PAGE_ZERO, zero_from_disk.as_ref(), page_size)?;
        Ok(())
    }

    /// Push a record to the write ahead log (implementation is not complete)
    pub(crate) fn push_to_log<O: Operation>(
        &mut self,
        operation: O,
        tid: TransactionId,
        prev_lsn: Option<Lsn>,
    ) -> io::Result<Lsn> {
        let lsn = self.wal.last_lsn().map(|l| l + 1).unwrap_or(0);
        let record = operation.into_record(lsn, tid, prev_lsn);
        self.wal.push(record)?;
        Ok(lsn)
    }

    /// Load block zero from disk
    fn load_page_zero(&mut self, block_size: usize) -> io::Result<PageZero> {
        let mut block: MemBlock<PageZeroHeader> = MemBlock::new(block_size);
        self.read_block(PAGE_ZERO, block.as_mut(), block_size)?;
        Ok(block)
    }

    /// Apply a read-only callback to any page type
    pub(crate) fn with_page<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<Frame<P>> + From<Frame<MemBlock<P::Header>>> + AsReadLatch<P>,
        F: FnOnce(&P) -> R,
    {
        let frame = self.read_page::<P>(id)?;
        let latch = frame.read()?;
        Ok(f(&latch))
    }

    /// Apply a mutable callback to any page type
    pub(crate) fn with_page_mut<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<Frame<P>> + From<Frame<MemBlock<P::Header>>> + AsWriteLatch<P>,
        F: FnOnce(&mut P) -> R,
    {
        let frame = self.read_page::<P>(id)?;
        let mut latch = frame.write()?;
        Ok(f(&mut latch))
    }

    /// Apply a fallible read-only callback to any page type
    pub(crate) fn try_with_page<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<Frame<MemBlock<P::Header>>> + From<Frame<P>> + AsReadLatch<P>,
        F: FnOnce(&P) -> io::Result<R>,
    {
        let frame = self.read_page::<P>(id)?;
        let latch = frame.read()?;
        f(&latch)
    }

    /// Apply a fallible mutable callback to any page type
    pub(crate) fn try_with_page_mut<P, F, R>(&mut self, id: PageId, f: F) -> io::Result<R>
    where
        P: Buffer,
        MemFrame: From<Frame<MemBlock<P::Header>>> + From<Frame<P>> + AsWriteLatch<P>,
        F: FnOnce(&mut P) -> io::Result<R>,
    {
        let frame = self.read_page::<P>(id)?;
        frame.mark_dirty();
        let mut latch = frame.write()?;
        f(&mut latch)
    }

    /// Check if the pager is intialized
    pub(crate) fn is_initialized(&self) -> bool {
        self.db_header.is_some()
    }

    pub(crate) fn header_unchecked(&self) -> &PageZeroHeader {
        self.db_header.as_ref().expect("Pager not initialized")
    }

    pub(crate) fn header_unchecked_mut(&mut self) -> &mut PageZeroHeader {
        self.db_header.as_mut().expect("Pager not initialized")
    }

    /// Gets the page size from the header.
    pub fn page_size(&self) -> usize {
        self.header_unchecked().page_size as usize
    }

    pub fn min_keys_per_page(&self) -> usize {
        self.header_unchecked().min_keys as usize
    }

    pub fn num_siblings_per_side(&self) -> usize {
        self.header_unchecked().num_siblings_per_side as usize
    }

    fn first_free_page(&self) -> Option<PageId> {
        self.header_unchecked().first_free_page
    }

    fn last_free_page(&self) -> Option<PageId> {
        self.header_unchecked().last_free_page
    }

    fn get_next_page(&mut self) -> PageId {
        let header_mut = self.header_unchecked_mut();
        let next = header_mut.total_pages;
        header_mut.total_pages += 1;
        next
    }

    fn set_first_free_page(&mut self, value: Option<PageId>) {
        self.header_unchecked_mut().first_free_page = value;
    }

    fn set_last_free_page(&mut self, value: Option<PageId>) {
        self.header_unchecked_mut().last_free_page = value;
    }

    fn validate_header(&self) -> io::Result<()> {
        if self.header_unchecked().magic != MAGIC {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Invalid magic string found on header {self.db_header.magic}",
            ));
        }
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
        P: Buffer<IdType = PageId>,
        MemFrame: From<Frame<P>> + From<Frame<MemBlock<P::Header>>>,
    {
        let mem_page = if let Some(page_id) = self.first_free_page() {
            let next = self.with_page::<OverflowPage, _, _>(page_id, |overflow| overflow.next())?;

            self.set_first_free_page(next);

            // If we have reinit the last free page, the list of deallocated pages is empty, which means we can unset it.
            if let Some(last_id) = self.last_free_page()
                && last_id == page_id
            {
                self.set_last_free_page(None);
            }

            // We need to make sure that we hold the only reference to the page in order to be able to dealloc or reinit the page. Take a look at [MemFrame::dealloc] if you want to figure out why.
            // To do so, the page must not be on the cache.
            // The easiest way to do it is to simply load the page into the cache first (in case it was on disk, the [Pager::ensure_cached] method takes care of that, and later remove it)
            self.ensure_cached::<P>(page_id)?; // Load the page to the cache.
            let page = self.cache.remove(page_id).ok_or(IoError::new(
                ErrorKind::NotFound,
                "Page not found on the cache",
            ))?;
            page.reinit_as::<P::Header>()
        } else {
            let id = self.get_next_page();
            let page = P::alloc(id, self.page_size() as usize);

            let frame = MemFrame::from(Frame::new(page));
            frame
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
                let page_size = self.page_size();
                evicted.with_bytes_mut(|bytes| self.write_block(evicted_id, &bytes, page_size))?;
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
        MemFrame: From<Frame<P>> + From<Frame<MemBlock<P::Header>>>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "cannot read page zero. Page zero is reserved for internal use only.",
            ));
        };

        // First we try to go to the cache.
        if let Some(page) = self.cache.get(&id) {
            return Ok(page);
        };

        let page_size = self.page_size();

        // That was a cache miss.
        // we need to go to the disk to find the page.
        let mut buffer: MemBlock<P::Header> = MemBlock::new(page_size);
        self.read_block(id, buffer.as_mut(), page_size)?;

        let mem_page = MemFrame::from(Frame::new(buffer));
        self.cache_frame(mem_page)?;

        Ok(self.cache.get(&id).unwrap())
    }

    /// Loads a page into the cache without pinning it.
    /// See [Pager::read_page]
    fn ensure_cached<P>(&mut self, id: PageId) -> io::Result<()>
    where
        P: Buffer,
        MemFrame: From<Frame<P>> + From<Frame<MemBlock<P::Header>>>,
    {
        let _ = self.read_page::<P>(id)?;
        Ok(())
    }

    /// Deallocates an existing page.
    pub(crate) fn dealloc_page<P>(&mut self, id: PageId) -> io::Result<()>
    where
        P: Buffer + AsMut<[u8]>,
        MemFrame: From<Frame<MemBlock<P::Header>>> + From<Frame<P>> + AsWriteLatch<P>,
    {
        // Someone asked for page zero, which we have on our local private cache so we return it.
        if id == PAGE_ZERO {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot deallocate PageZero!",
            ));
        };

        let first_free_page = self.first_free_page();

        // Set the first free page in the header
        if first_free_page.is_none() {
            self.set_first_free_page(Some(id));
        };

        if let Some(last_free_id) = self.last_free_page() {
            // Read the free page and set the next page with the id of the page we have just deallocated.
            self.with_page_mut::<OverflowPage, _, _>(last_free_id, |overflow| {
                overflow.metadata_mut().next = Some(id);
            })?;
        };

        // The new last free page must be the one we have deallocated.
        self.set_last_free_page(Some(id));

        self.ensure_cached::<P>(id)?;

        // We need to ensure that the frame is free
        if let Some(mem_page) = self.cache.remove(id) {
            let page_size = self.page_size();

            // [MemPage::dealloc] consumes itself and creates a new MemPage with an overflow header.
            let deallocated_page = mem_page.dealloc();

            // Here the write needs to write as an overflow page , regardless of the value of [P]
            deallocated_page.with_bytes(|bytes| self.write_block(id, bytes, page_size))?;
            self.cache_frame(deallocated_page)?;
        };

        Ok(())
    }

    pub fn run_analysis(&mut self) -> io::Result<AnalysisResult> {
        self.wal.run_analysis()
    }

    pub fn truncate_wal(&mut self) -> io::Result<()> {
        self.wal.truncate()
    }

    pub fn flush_wal(&mut self) -> io::Result<()> {
        self.wal.flush()
    }
}

make_shared! {SharedPager, Pager}

impl Write for Pager {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let block_size = self.page_size();
        self.wal.flush()?;
        let pages = self.cache.clear();
        for page in pages {
            let page_number = page.page_number();
            if page.is_dirty() {
                page.with_bytes_mut(|bytes| self.write_block(page_number, &bytes, block_size))?;
            };
        }

        self.sync_header()?;

        self.file.flush()?;
        Ok(())
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

#[derive(Clone)]
pub struct BtreeBuilder {
    pager: Option<SharedPager>,
    min_keys: usize,
    num_siblings_per_side: usize,
}

impl Default for BtreeBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE)
    }
}

impl BtreeBuilder {
    pub fn new(min_keys: usize, num_siblings_per_side: usize) -> Self {
        Self {
            pager: None,
            min_keys,
            num_siblings_per_side,
        }
    }

    pub fn with_pager(mut self, pager: SharedPager) -> Self {
        self.pager = Some(pager);
        self
    }

    pub(crate) fn build_tree_with_accessor<Acc: TreeReader>(
        &self,
        root: PageId,
        accessor: Acc,
    ) -> Btree<Acc> {
        let pager = self.pager.as_ref().expect("Pager not intialized").clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side).with_accessor(accessor)
    }

    pub(crate) fn build_tree_mut_with_accessor<Acc: TreeWriter>(
        &self,
        root: PageId,
        accessor: Acc,
    ) -> Btree<Acc> {
        let pager = self.pager.as_ref().expect("Pager not intialized").clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side).with_accessor(accessor)
    }

    pub(crate) fn build_tree(&self, root: PageId) -> Btree<BtreeReadAccessor> {
        let pager = self.pager.as_ref().expect("Pager not intialized").clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side)
            .with_accessor(BtreeReadAccessor::new())
    }

    pub(crate) fn build_tree_mut(&self, root: PageId) -> Btree<BtreeWriteAccessor> {
        let pager = self.pager.as_ref().expect("Pager not intialized").clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side)
            .with_accessor(BtreeWriteAccessor::new())
    }
}
