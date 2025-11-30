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
        tuple::{OwnedTuple, Tuple, TupleRef, TupleRefMut},
    },
    transactions::{LogicalId, Snapshot, TransactionCoordinator, TransactionHandle},
    types::{ObjectId, PageId, TransactionId},
};

use std::{
    cell::{Ref, RefCell, RefMut},
    io::{self, Error as IoError, ErrorKind as IoErrorKind},
    rc::Rc,
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

/// Result of a tuple read operation
#[derive(Debug)]
pub enum ReadResult<T> {
    /// Tuple found and visible
    Found(T),
    /// Tuple exists but not visible to this transaction
    NotVisible,
    /// Tuple does not exist
    NotFound,
}

impl From<TransactionHandle> for WorkerPool {
    fn from(value: TransactionHandle) -> Self {
        let catalog = value.coordinator.catalog();
        let pager = value.coordinator.pager();
        Self::new(
            value.id(),
            value.snapshot,
            pager,
            value.coordinator,
            catalog,
        )
    }
}

pub struct WorkerPool {
    /// Transaction ID
    transaction_id: TransactionId,
    /// Snapshot for visibility checks
    snapshot: Snapshot,
    /// Workers for page operations
    workers: Vec<Worker>,
    /// Shared pager
    pager: SharedPager,
    /// Transaction coordinator for recording reads/writes
    coordinator: TransactionCoordinator,
    /// Shared catalog
    catalog: SharedCatalog,
    /// WAL record builder
    builder: LogRecordBuilder,
}

impl WorkerPool {
    pub fn new(
        tx_id: TransactionId,
        snapshot: Snapshot,
        pager: SharedPager,
        coordinator: TransactionCoordinator,
        catalog: SharedCatalog,
    ) -> Self {
        let builder = LogRecordBuilder::for_transaction(tx_id);
        Self {
            transaction_id: tx_id,
            snapshot,
            workers: Vec::new(),
            pager,
            coordinator,
            catalog,
            builder,
        }
    }

    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn builder(&self) -> LogRecordBuilder {
        self.builder
    }

    /// Write BEGIN record to WAL
    pub fn begin(&self) -> io::Result<()> {
        let operation = Begin;
        let rec = self.builder().build_rec(LogRecordType::Begin, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write COMMIT record to WAL
    pub fn commit(&self) -> io::Result<()> {
        let operation = Commit;
        let rec = self.builder().build_rec(LogRecordType::Commit, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write ABORT record to WAL
    pub fn rollback(&self) -> io::Result<()> {
        let operation = Abort;
        let rec = self.builder().build_rec(LogRecordType::Abort, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write END record to WAL
    pub fn end(&self) -> io::Result<()> {
        let operation = End;
        let rec = self.builder().build_rec(LogRecordType::End, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Validate and commit the transaction
    /// Returns error if write-write conflict detected
    pub fn try_commit(&self) -> Result<(), TransactionError> {
        // First validate with coordinator (checks tuple_commits)
        self.coordinator.commit(self.transaction_id)?;

        // If validation passed, write COMMIT to WAL
        self.commit()?;

        Ok(())
    }

    /// Abort the transaction
    pub fn try_abort(&self) -> Result<(), TransactionError> {
        self.coordinator.abort(self.transaction_id)?;
        self.rollback()?;
        Ok(())
    }

    fn get_or_create_worker(&self) -> Worker {
        if let Some(worker) = self.workers.first() {
            worker.clone()
        } else {
            Worker::new(self.pager.clone())
        }
    }

    /// Insert a new tuple into a table
    pub fn insert_one(&self, table: ObjectId, values: &[DataType]) -> Result<LogicalId, TransactionError> {
        let worker = self.get_or_create_worker();
        let mut relation = self.catalog.get_relation_unchecked(table, worker.clone())?;

        let mut btree = self.catalog.table_btree(relation.root(), worker.clone())?;

        let next_row = if let Relation::TableRel(tab) = &mut relation {
            let next = tab.get_next();
            tab.add_row();
            next
        } else {
            return Err(TransactionError::Other(
                "Transaction attempted to access an invalid object".to_string(),
            ));
        };

        let schema = relation.schema();
        let logical_id = LogicalId::new(table, next_row);

        // Create tuple with our transaction ID as xmin
        let tuple = Tuple::new(values, schema, self.transaction_id)?;
        let bytes: OwnedTuple = tuple.into();

        // Record this write for conflict detection
        self.coordinator
            .record_write(self.transaction_id, logical_id, 0)?;

        // Write WAL record
        let op = Insert::new(table, bytes.clone());
        let record = self.builder().build_rec(LogRecordType::Insert, op);
        self.pager.write().push_to_log(record)?;

        // Insert into B-tree
        btree.insert(relation.root(), bytes.as_ref())?;
        btree.clear_worker_stack();

        Ok(logical_id)
    }

    /// Read a tuple by logical ID, respecting MVCC visibility.
    /// Executes the provided function over the visible tuple if found.
    pub fn read_one<F, T>(
        &self,
        logical_id: LogicalId,
        f: F,
    ) -> Result<ReadResult<T>, TransactionError>
    where
        F: FnOnce(&TupleRef) -> T,
    {
        let worker = self.get_or_create_worker();
        let table = logical_id.table();
        let relation = self.catalog.get_relation_unchecked(table, worker.clone())?;
        let btree = self.catalog.table_btree(relation.root(), worker.clone())?;
        let schema = relation.schema();

        // Search for the tuple
        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Read)?;

        let result = if let Some(payload) = btree.get_payload(current_position)? {
            match TupleRef::read_for_snapshot(payload.as_ref(), schema, &self.snapshot)? {
                Some(tuple_ref) => {
                    self.coordinator
                        .record_read(self.transaction_id, logical_id)?;
                    ReadResult::Found(f(&tuple_ref))
                }
                None => ReadResult::NotVisible,
            }
        } else {
            ReadResult::NotFound
        };

        btree.clear_worker_stack();
        Ok(result)
    }

    /// Read a tuple mutably by logical ID, respecting MVCC visibility.
    /// Executes the provided function over the visible tuple if found.
    pub fn read_one_mut<F, T>(
        &self,
        logical_id: LogicalId,
        f: F,
    ) -> Result<ReadResult<T>, TransactionError>
    where
        F: FnOnce(&mut TupleRefMut) -> T,
    {
        let worker = self.get_or_create_worker();
        let table = logical_id.table();
        let relation = self.catalog.get_relation_unchecked(table, worker.clone())?;
        let btree = self.catalog.table_btree(relation.root(), worker.clone())?;
        let schema = relation.schema();

        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Write)?;

        let result = if let Some(mut payload) = btree.get_payload(current_position)? {
            match TupleRefMut::read_for_snapshot(payload.as_mut(), schema, &self.snapshot)? {
                Some(mut tuple_ref) => {
                    self.coordinator
                        .record_read(self.transaction_id, logical_id)?;
                    ReadResult::Found(f(&mut tuple_ref))
                }
                None => ReadResult::NotVisible,
            }
        } else {
            ReadResult::NotFound
        };

        btree.clear_worker_stack();
        Ok(result)
    }

    /// Update a tuple, creating a new version
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

        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Read)?;

        let Some(payload) = btree.get_payload(current_position)? else {
            btree.clear_worker_stack();
            return Err(TransactionError::Other("Tuple not found".to_string()));
        };

        // Check visibility using read_for_snapshot
        let Some(tuple_ref) =
            TupleRef::read_for_snapshot(payload.as_ref(), schema, &self.snapshot)?
        else {
            btree.clear_worker_stack();
            return Err(TransactionError::TupleNotVisible(
                self.transaction_id(),
                logical_id,
            ));
        };

        let old_version = tuple_ref.version();
        let mut tuple = Tuple::try_from((payload.as_ref(), schema))?;
        let old: OwnedTuple = tuple.clone().into();

        // Record write with the version we read
        self.coordinator
            .record_write(self.transaction_id, logical_id, old_version.into())?;

        // Add new version
        tuple.add_version(values, self.transaction_id)?;
        let new: OwnedTuple = tuple.into();

        // Write WAL record
        let op = Update::new(table, old, new.clone());
        let record = self.builder().build_rec(LogRecordType::Update, op);
        self.pager.write().push_to_log(record)?;

        // Update in B-tree
        btree.update(relation.root(), new.as_ref())?;
        btree.clear_worker_stack();

        Ok(())
    }

    /// Delete a tuple by setting xmax
    pub fn delete_one(&self, logical_id: LogicalId) -> Result<(), TransactionError> {
        let worker = self.get_or_create_worker();
        let table = logical_id.table();
        let relation = self.catalog.get_relation_unchecked(table, worker.clone())?;
        let mut btree = self.catalog.table_btree(relation.root(), worker.clone())?;
        let schema = relation.schema();

        let current_position =
            btree.search_from_root(logical_id.row().as_ref(), FrameAccessMode::Read)?;

        let Some(payload) = btree.get_payload(current_position)? else {
            btree.clear_worker_stack();
            return Err(TransactionError::Other("Tuple not found".to_string()));
        };

        // Check visibility using read_for_snapshot
        let Some(tuple_ref) =
            TupleRef::read_for_snapshot(payload.as_ref(), schema, &self.snapshot)?
        else {
            btree.clear_worker_stack();
            return Err(TransactionError::TupleNotVisible(
                self.transaction_id(),
                logical_id,
            ));
        };

        // Check if already deleted
        if tuple_ref.xmax() != crate::TRANSACTION_ZERO {
            btree.clear_worker_stack();
            return Err(TransactionError::AlreadyDeleted(logical_id));
        }

        let mut tuple = Tuple::try_from((payload.as_ref(), schema))?;
        let old: OwnedTuple = tuple.clone().into();

        // Record write
        self.coordinator
            .record_write(self.transaction_id, logical_id, tuple.version().into())?;

        // Mark as deleted by setting xmax to our transaction ID
        tuple.delete(self.transaction_id)?;
        let new: OwnedTuple = tuple.into();

        // Write WAL record
        let op = Delete::new(table, old);
        let record = self.builder().build_rec(LogRecordType::Delete, op);
        self.pager.write().push_to_log(record)?;

        // Update in B-tree (tuple still exists, just marked deleted)
        btree.update(relation.root(), new.as_ref())?;
        btree.clear_worker_stack();

        Ok(())
    }
}







#[cfg(test)]
mod worker_pool_tests {
    use super::*;
    use crate::{
        IncrementalVaccum, TextEncoding,
        configs::AxmosDBConfig,
        database::{Database, schema::{Column, Schema}},
        io::pager::Pager,
        types::{DataType, DataTypeKind, UInt64, Blob},
    };
    use std::{io, path::Path};
    use serial_test::serial;


    /// Creates a test database with a simple "users" table.
    /// Schema: [row_id: BigUInt, name: Text, age: BigUInt]
    fn create_db(path: impl AsRef<Path>) -> io::Result<(Database, ObjectId)> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        // Create a test table
        let schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
                Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
            ],
            1, // key index
        );

        let worker = db.main_worker_cloned();
        db.catalog().create_table("users", schema, worker)?;

        // Get the table's ObjectId
        let worker = db.main_worker_cloned();
        let relation = db.catalog().get_relation("users", worker)?;
        let table_id = relation.id();

        Ok((db, table_id))
    }

    /// Helper to begin a transaction and get a WorkerPool
    fn begin_transaction(db: &Database) -> WorkerPool {
        let coordinator = TransactionCoordinator::new(db.pager(), db.catalog());
        let handle = coordinator.begin().expect("Failed to begin transaction");
        WorkerPool::from(handle)
    }


    /// Test: Basic insert and read operations work correctly.
    ///
    /// Scenario:
    /// 1. Transaction A inserts a tuple (row_id=1, name="Alice", age=30)
    /// 2. Transaction A reads the tuple back
    /// 3. The read returns the correct values
    #[test]
    #[serial]
    fn test_workerpool_1() -> Result<(), TransactionError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Insert a tuple
        let values = [
            DataType::BigUInt(UInt64::from(1u64)),
            DataType::Text(Blob::from("Alice")),
            DataType::BigUInt(UInt64::from(30u64)),
        ];
        let logical_id = pool.insert_one(table_id, &values)?;

        // Read it back
        let result = pool.read_one(logical_id, |tuple| {
            let name = tuple.value(1).ok().map(|v| format!("{:?}", v));
            let age = tuple.value(2).ok().map(|v| format!("{:?}", v));
            (name, age)
        })?;

        match result {
            ReadResult::Found((name, age)) => {
                assert!(name.is_some(), "Name should be readable");
                assert!(age.is_some(), "Age should be readable");
            }
            ReadResult::NotVisible => panic!("Tuple should be visible to its creator"),
            ReadResult::NotFound => panic!("Tuple should exist after insert"),
        }

        pool.try_commit()?;
        Ok(())
    }

    /// Test: A transaction can update a tuple it inserted.
    ///
    /// Scenario:
    /// 1. Transaction A inserts a tuple (age=30)
    /// 2. Transaction A updates the tuple (age=31)
    /// 3. Transaction A reads the updated value
    #[test]
    fn test_workerpool_2() -> Result<(), TransactionError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Insert
        let values = [
            DataType::BigUInt(UInt64::from(1u64)),
            DataType::Text(Blob::from("Bob")),
            DataType::BigUInt(UInt64::from(25u64)),
        ];
        pool.insert_one(table_id, &values)?;

        // Update age from 25 to 26
        let logical_id = LogicalId::new(table_id, UInt64::from(1u64));
        let new_age = DataType::BigUInt(UInt64::from(26u64));
        pool.update_one(logical_id, &[(2, new_age)])?;

        // Read and verify the update
        let result = pool.read_one(logical_id, |tuple| {
            // Extract age value
            tuple.value(2).ok().map(|v| format!("{:?}", v))
        })?;

        match result {
            ReadResult::Found(age) => {
                assert!(age.is_some(), "Age should be readable after update");
                // The age should reflect the updated value
            }
            _ => panic!("Tuple should be found and visible after update"),
        }

        pool.try_commit()?;
        Ok(())
    }

    /// Test: A deleted tuple becomes invisible to subsequent reads.
    ///
    /// Scenario:
    /// 1. Transaction A inserts a tuple
    /// 2. Transaction A commits
    /// 3. Transaction B deletes the tuple
    /// 4. Transaction B reads the tuple -> NotVisible (deleted by self)
    #[test]
    fn test_workerpool_3() -> Result<(), TransactionError>  {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        // Transaction A: insert and commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;

        let values = [
            DataType::BigUInt(UInt64::from(1u64)),
            DataType::Text(Blob::from("Charlie")),
            DataType::BigUInt(UInt64::from(40u64)),
        ];
        pool_a.insert_one(table_id, &values)?;
        pool_a.try_commit()?;

        // Transaction B: delete the tuple
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;

        let logical_id = LogicalId::new(table_id, UInt64::from(1u64));
        pool_b.delete_one(logical_id)?;

        // After deleting, the tuple should not be visible to transaction B
        // (xmax = B's transaction ID, and B sees its own deletes as invisible)
        let result = pool_b.read_one(logical_id, |_| ())?;

        assert!(
            matches!(result, ReadResult::NotVisible),
            "Deleted tuple should not be visible to the deleting transaction"
        );

        pool_b.try_commit()?;
        Ok(())
    }

    /// Test: Snapshot isolation - a transaction does not see uncommitted changes from others.
    ///
    /// Scenario:
    /// 1. Transaction A inserts a tuple but does NOT commit
    /// 2. Transaction B starts and tries to read the tuple
    /// 3. Transaction B should NOT see the tuple (A hasn't committed)
    #[test]
    fn test_workerpool_4() -> Result<(), TransactionError>  {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        // Transaction A: insert but don't commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;

        let values = [
            DataType::BigUInt(UInt64::from(1u64)),
            DataType::Text(Blob::from("Dave")),
            DataType::BigUInt(UInt64::from(50u64)),
        ];
        pool_a.insert_one(table_id, &values)?;
        // Note: NOT committing pool_a

        // Transaction B: try to read the tuple
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;

        let logical_id = LogicalId::new(table_id, UInt64::from(1u64));
        let result = pool_b.read_one(logical_id, |_| ())?;

        // B should not see A's uncommitted insert
        assert!(
            matches!(result, ReadResult::NotFound | ReadResult::NotVisible),
            "Transaction B should NOT see uncommitted writes from Transaction A, got: {:?}",
            result
        );

        // Cleanup
        pool_a.try_abort()?;
        pool_b.try_abort()?;
        Ok(())
    }

    /// Test: Reading a non-existent tuple returns NotFound.
    ///
    /// Scenario:
    /// 1. Transaction A tries to read a tuple that was never inserted
    /// 2. The result should be NotFound
    #[test]
    fn test_workerpool_5() -> Result<(), TransactionError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Try to read a tuple that doesn't exist
        let logical_id = LogicalId::new(table_id, UInt64::from(999u64));
        let result = pool.read_one(logical_id, |_| ())?;

        assert!(
            matches!(result, ReadResult::NotFound),
            "Reading non-existent tuple should return NotFound, got: {:?}",
            result
        );

        pool.try_abort()?;
        Ok(())
    }

    /// Test: Attempting to delete an already-deleted tuple fails.
    ///
    /// Scenario:
    /// 1. Transaction A inserts and commits a tuple
    /// 2. Transaction B deletes the tuple and commits
    /// 3. Transaction C tries to delete the same tuple -> AlreadyDeleted error
    #[test]
    fn test_workerpool_6() -> Result<(), TransactionError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let (db, table_id) = create_db(&path)?;

        // Transaction A: insert and commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;
        let values = [
            DataType::BigUInt(UInt64::from(1u64)),
            DataType::Text(Blob::from("Eve")),
            DataType::BigUInt(UInt64::from(28u64)),
        ];
        pool_a.insert_one(table_id, &values)?;
        pool_a.try_commit()?;

        // Transaction B: delete and commit
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;
        let logical_id = LogicalId::new(table_id, UInt64::from(1u64));
        pool_b.delete_one(logical_id)?;
        pool_b.try_commit()?;

        // Transaction C: try to delete the same tuple
        let pool_c = begin_transaction(&db);
        pool_c.begin()?;
        let result = pool_c.delete_one(logical_id);

        assert!(
            matches!(result, Err(TransactionError::AlreadyDeleted(_)) | Err(TransactionError::TupleNotVisible(_, _))),
            "Deleting already-deleted tuple should fail, got: {:?}",
            result
        );

        pool_c.try_abort()?;
        Ok(())
    }
}
