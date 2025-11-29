use crate::{
    DataType,
    database::{SharedCatalog, errors::TransactionError, schema::Relation},
    io::{
        frames::{FrameAccessMode, FrameStack, Position},
        pager::SharedPager,
        wal::{Abort, Begin, Commit, Delete, End, Insert, LogRecordBuilder, LogRecordType, Update},
    },
    storage::{
        buffer::BufferWithMetadata,
        latches::Latch,
        page::{MemPage, Page},
        tuple::{OwnedTuple, Tuple},
    },
    transactions::{LockRequest, LogicalId, TransactionManager},
    types::{ObjectId, PageId, TransactionId},
};

use std::{
    cell::{Ref, RefCell, RefMut},
    io::{self, Error as IoError, ErrorKind as IoErrorKind},
    rc::Rc,
    sync::mpsc::Sender,
    thread::{self, ThreadId},
};

// TODO: FOR NOW, EACH TRANSACTION WILL MANAGE A SINGLE THREAD. IN THE FUTURE , WE SHOULD FIGURE OUT HOW TO DO THIS PROPERLY WITH THREAD POOLS
#[derive(Debug)]
pub struct ThreadWorker {
    /// Thread id.
    thread_id: ThreadId,

    /// Shared access to the pager
    pager: SharedPager,

    /// Private stack for transactions
    stack: FrameStack,
}

#[derive(Debug)]
pub struct Worker(Rc<RefCell<ThreadWorker>>);

impl Worker {
    pub fn new(pager: SharedPager) -> Self {
        Self(Rc::new(RefCell::new(ThreadWorker::new(pager))))
    }

    pub fn borrow(&self) -> Ref<'_, ThreadWorker> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> RefMut<'_, ThreadWorker> {
        self.0.borrow_mut()
    }
}

impl Clone for Worker {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl ThreadWorker {
    pub fn new(pager: SharedPager) -> Self {
        Self {
            thread_id: thread::current_id(),
            pager,
            stack: FrameStack::new(),
        }
    }

    pub fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    fn stack_mut(&mut self) -> &mut FrameStack {
        &mut self.stack
    }

    fn stack_ref(&self) -> &FrameStack {
        &self.stack
    }

    pub fn get_page_size(&self) -> usize {
        self.pager.read().page_size() as usize
    }

    pub fn clear_stack(&mut self) {
        self.stack_mut().clear()
    }

    pub fn release_latch(&mut self, page_id: PageId) {
        self.stack_mut().release(page_id);
    }

    pub fn last(&self) -> Option<&Position> {
        self.stack_ref().last()
    }

    pub fn pop(&mut self) -> Option<Position> {
        self.stack_mut().pop()
    }

    // Track the current position in the stack
    pub fn visit(&mut self, position: Position) -> std::io::Result<()> {
        self.stack_mut().visit(position);
        Ok(())
    }

    // Acquire a lock on the stack for a page using the given [AccessMode]
    pub fn acquire<P: Page>(
        &mut self,
        page_id: PageId,
        access_mode: FrameAccessMode,
    ) -> std::io::Result<()>
    where
        MemPage: From<P>,
    {
        let latch_page = self.pager.write().read_page::<P>(page_id)?;
        self.stack_mut().acquire(page_id, latch_page, access_mode);
        Ok(())
    }

    pub fn alloc_page<P: Page>(&mut self) -> std::io::Result<PageId>
    where
        MemPage: From<P>,
        BufferWithMetadata<P::Header>: Into<MemPage>,
    {
        let page = self.pager.write().alloc_frame()?;
        self.pager.write().cache_frame(page)
    }

    pub fn dealloc_page<P: Page>(&mut self, id: PageId) -> std::io::Result<()>
    where
        MemPage: From<P>,
    {
        let cloned = self.pager.write().read_page::<P>(id)?.deep_copy();
        debug_assert!(
            cloned.page_number() == id,
            "MEMORY CORRUPTION. ID IN CACHE DOES NOT MATCH PHYSICAL ID"
        );
        self.pager.write().dealloc_page(id)?;

        Ok(())
    }

    // Allows to execute a mutating callback on the requested page.
    // The page latch must have been already acquired.
    pub fn write_page<P, F, R>(&mut self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&mut P) -> R,
        for<'a> &'a mut Latch<MemPage>: TryInto<&'a mut P, Error = IoError>,
        MemPage: From<P>,
    {
        if let Some(guard) = self.stack_mut().get_mut(id) {
            let page_mut: &mut P = guard.try_into()?;
            // Execute the callback
            let result = callback(page_mut);
            return Ok(result);
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Allows to execute a read only callback on the given page.
    pub fn read_page<P, F, R>(&self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&P) -> R,
        for<'a> &'a Latch<MemPage>: TryInto<&'a P, Error = IoError>,
        MemPage: From<P>,
    {
        if let Some(guard) = self.stack_ref().get(id) {
            let page: &P = guard.try_into()?;
            // Execute the callback
            let result = callback(page);
            return Ok(result);
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Same as [`write_page`] but the callback must return io::Result, propagating the error o the caller.
    // The page latch must have been already acquired.
    pub fn try_write_page<P, F, R>(&mut self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&mut P) -> io::Result<R>,
        for<'a> &'a mut Latch<MemPage>: TryInto<&'a mut P, Error = IoError>,
        MemPage: From<P>,
    {
        if let Some(guard) = self.stack_mut().get_mut(id) {
            let page_mut: &mut P = guard.try_into()?;
            // Execute the callback
            let result = callback(page_mut);

            return result;
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Same as [`read_page`] but the callback must return io::Result, propagating the error o the caller.
    // Allows to execute a read only callback on the given page.
    pub fn try_read_page<P, F, R>(&self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&P) -> io::Result<R>,
        for<'a> &'a Latch<MemPage>: TryInto<&'a P, Error = IoError>,
        MemPage: From<P>,
    {
        if let Some(guard) = self.stack_ref().get(id) {
            let page: &P = guard.try_into()?;
            // Execute the callback
            let result = callback(page);
            return result;
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }
}

pub struct WorkerPool {
    transaction_id: TransactionId,
    /// Workers assigned to this pool
    workers: Vec<Worker>,
    /// Shared access to the pager
    pager: SharedPager,
    /// lock sender to access the lock manager:
    lock_requestor: Sender<LockRequest>,
    /// Shared transaction manager
    manager: TransactionManager,
    /// Shared catalog.
    catalog: SharedCatalog,
    /// Log builder to build records for the write ahead log
    builder: LogRecordBuilder,
}

impl WorkerPool {
    pub fn new(
        pager: SharedPager,
        lock_requestor: Sender<LockRequest>,
        manager: TransactionManager,
        catalog: SharedCatalog,
    ) -> Self {
        let tx_id = TransactionId::new();
        Self::for_transaction(tx_id, pager, lock_requestor, manager, catalog)
    }

    pub fn for_transaction(
        tx_id: TransactionId,
        pager: SharedPager,
        lock_requestor: Sender<LockRequest>,
        manager: TransactionManager,
        catalog: SharedCatalog,
    ) -> Self {
        let builder = LogRecordBuilder::for_transaction(tx_id);
        Self {
            transaction_id: tx_id,
            workers: Vec::new(),
            builder,
            pager,
            lock_requestor,
            manager,
            catalog,
        }
    }

    fn builder(&self) -> LogRecordBuilder {
        self.builder
    }

    pub fn begin(&self) -> io::Result<()> {
        let operation = Begin;
        let rec = self.builder().build_rec(LogRecordType::Begin, operation);
        self.pager.write().push_to_log(rec)
    }

    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub fn commit(&self) -> io::Result<()> {
        let operation = Commit;
        let rec = self.builder().build_rec(LogRecordType::Commit, operation);
        self.pager.write().push_to_log(rec)
    }

    pub fn rollback(&self) -> io::Result<()> {
        let operation = Abort;
        let rec = self.builder().build_rec(LogRecordType::Abort, operation);
        self.pager.write().push_to_log(rec)
    }

    pub fn end(&self) -> std::io::Result<()> {
        let operation = End;
        let rec = self.builder().build_rec(LogRecordType::End, operation);
        self.pager.write().push_to_log(rec)
    }

    fn get_or_create_worker(&self) -> Worker {
        if let Some(worker) = self.workers.first() {
            worker.clone()
        } else {
            Worker::new(self.pager.clone())
        }
    }

    pub fn insert_one(&self, table: ObjectId, values: &[DataType]) -> Result<(), TransactionError> {
        let worker = self.get_or_create_worker();
        let mut relation = self.catalog.get_relation_unchecked(table, worker.clone())?;

        let mut btree = self.catalog.table_btree(relation.root(), worker.clone())?;

        let next_row = if let Relation::TableRel(tab) = &mut relation {
            let next = tab.get_next();
            tab.add_row(); // Increment the row count
            next
        } else {
            return Err(TransactionError::Other(
                "Transaction attempted to access an invalid object".to_string(),
            ));
        };

        let schema = relation.schema();
        let logical_id = LogicalId::new(table, next_row);

        // Lock until the tuple handle is valid.
        self.manager.get_tuple_handle(
            self.transaction_id,
            logical_id,
            self.lock_requestor.clone(),
        )?;

        let tuple = Tuple::new(values, schema, self.transaction_id)?;
        let bytes: OwnedTuple = tuple.into();
        let op = Insert::new(table, bytes.clone());
        let record = self.builder().build_rec(LogRecordType::Insert, op);

        self.pager.write().push_to_log(record)?;
        btree.insert(relation.root(), bytes.as_ref())?;
        // Create btree, search for the tuple, write a new version.
        btree.clear_worker_stack();

        Ok(())
    }

    pub fn update_one(
        &self,
        logical_id: LogicalId,
        values: &[(usize, DataType)],
    ) -> Result<(), TransactionError> {
        let worker = self.get_or_create_worker();
        let table = logical_id.table();
        let relation = self.catalog.get_relation_unchecked(table, worker.clone())?;

        let mut btree = self.catalog.table_btree(relation.root(), worker.clone())?;

        let schema = relation.schema();

        // Lock until the tuple handle is valid.
        self.manager.get_tuple_handle(
            self.transaction_id,
            logical_id,
            self.lock_requestor.clone(),
        )?;

        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Read)?;

        // Try to read the tuple from the payload.
        let mut tuple: Tuple = if let Some(payload) = btree.get_payload(current_position)? {
            Tuple::try_from((payload.as_ref(), schema))?
        } else {
            return Err(TransactionError::Other("Tuple not found".to_string()));
        };

        let old: OwnedTuple = tuple.clone().into();

        tuple.add_version(values, self.transaction_id)?;
        let new: OwnedTuple = tuple.into();
        let op = Update::new(table, old, new.clone());
        let record = self.builder().build_rec(LogRecordType::Update, op);

        self.pager.write().push_to_log(record)?;
        btree.update(relation.root(), new.as_ref())?;
        // Create btree, search for the tuple, write a new version.
        btree.clear_worker_stack();

        Ok(())
    }

    pub fn delete_one(&self, logical_id: LogicalId) -> Result<(), TransactionError> {
        let worker = self.get_or_create_worker();
        let table = logical_id.table();
        let relation = self.catalog.get_relation_unchecked(table, worker.clone())?;

        let mut btree = self.catalog.table_btree(relation.root(), worker.clone())?;

        let schema = relation.schema();

        // Lock until the tuple handle is valid.
        self.manager.get_tuple_handle(
            self.transaction_id,
            logical_id,
            self.lock_requestor.clone(),
        )?;

        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Read)?;

        // Try to read the tuple from the payload.
        let mut tuple: Tuple = if let Some(payload) = btree.get_payload(current_position)? {
            Tuple::try_from((payload.as_ref(), schema))?
        } else {
            return Err(TransactionError::Other("Tuple not found".to_string()));
        };

        let old: OwnedTuple = tuple.clone().into();
        tuple.delete(self.transaction_id)?;
        let new: OwnedTuple = tuple.into();

        let op = Delete::new(table, old);
        let record = self.builder().build_rec(LogRecordType::Delete, op);

        self.pager.write().push_to_log(record)?;
        btree.update(relation.root(), new.as_ref())?;
        // Create btree, search for the tuple, write a new version.
        btree.clear_worker_stack();

        Ok(())
    }
}
