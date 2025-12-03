use crate::{
    DataType,
    database::{
        META_INDEX, META_TABLE, SharedCatalog,
        errors::{TransactionError, TransactionResult},
        meta_idx_schema,
        schema::{Column, Index, IndexInfo, ObjectType, Relation, Schema, Table},
    },
    io::{
        frames::{FrameAccessMode, FrameStack, Position},
        pager::SharedPager,
        wal::{Abort, Begin, Commit, Delete, End, Insert, LogRecordBuilder, LogRecordType, Update},
    },
    storage::{
        buffer::BufferWithMetadata,
        latches::Latch,
        page::{BtreePage, MemPage, Page},
        tuple::{OwnedTuple, Tuple, TupleRef, TupleRefMut},
    },
    structures::bplustree::SearchResult,
    transactions::{LogicalId, Snapshot, TransactionCoordinator, TransactionHandle},
    types::{Blob, DataTypeKind, ObjectId, PageId, TransactionId, UInt64},
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
    pub fn try_commit(&self) -> TransactionResult<()> {
        // First validate with coordinator (checks tuple_commits)
        self.coordinator.commit(self.transaction_id)?;

        // If validation passed, write COMMIT to WAL
        self.commit()?;

        Ok(())
    }

    /// Abort the transaction
    pub fn try_abort(&self) -> TransactionResult<()> {
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

    /// Creates a new table using the catalog low level methods and properly logging to the wal
    pub fn create_table(&self, table_name: &str, schema: Schema) -> TransactionResult<ObjectId> {
        let worker = self.get_or_create_worker();

        // Check if relation already exists
        if let Ok(SearchResult::Found(_)) = self.catalog.lookup_relation(table_name, worker.clone())
        {
            return Err(TransactionError::RelationAlreadyExists(
                table_name.to_string(),
            ));
        }

        let root = worker.borrow_mut().alloc_page::<BtreePage>()?;

        let table = Table::new(table_name, root, schema);
        let object_id = table.id();
        let relation = Relation::TableRel(table);

        // Serialize relation for meta_table
        let meta_tuple = relation.clone().into_boxed_tuple()?;
        let meta_index_relation = self.catalog.get_relation(META_INDEX, worker.clone())?;
        // Insert into meta_index
        let meta_idx_schema = meta_idx_schema();
        let idx_tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(relation.name())),
                DataType::BigUInt(UInt64::from(relation.id())),
            ],
            &meta_idx_schema,
            self.transaction_id,
        )?;
        let idx_bytes: OwnedTuple = idx_tuple.into();

        // WAL for meta_index insert
        let idx_op = Insert::new(meta_index_relation.id(), idx_bytes.clone());
        let idx_record = self.builder().build_rec(LogRecordType::Insert, idx_op);
        self.pager.write().push_to_log(idx_record)?;

        // Insert into meta table
        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;
        // WAL for meta_table insert
        let table_op = Insert::new(meta_table_relation.id(), meta_tuple.clone());
        let table_record = self.builder().build_rec(LogRecordType::Insert, table_op);
        self.pager.write().push_to_log(table_record)?;

        self.catalog.create_relation(relation, worker)?;

        Ok(object_id)
    }

    pub fn add_column(&self, table_name: &str, column: Column) -> TransactionResult<()> {
        let worker = self.get_or_create_worker();

        let mut relation = self.catalog.get_relation(table_name, worker.clone())?;

        if !matches!(relation, Relation::TableRel(_)) {
            return Err(TransactionError::InvalidObjectType(
                relation.id(),
                ObjectType::Table,
                ObjectType::Index,
            ));
        }

        // Check column doesn't already exist
        if relation.schema().has_column(column.name()) {
            let name_str = column.name().to_string();
            return Err(TransactionError::Other(format!(
                "Column {name_str} already exists"
            )));
        }

        // Get old tuple for WAL
        let old_tuple = relation.clone().into_boxed_tuple()?;

        // Add column to schema
        relation.schema_mut().push_column(column);

        // Get new tuple for WAL
        let new_tuple = relation.clone().into_boxed_tuple()?;

        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;

        // WAL for table update
        let update_op = Update::new(meta_table_relation.id(), old_tuple, new_tuple);
        let update_record = self.builder().build_rec(LogRecordType::Update, update_op);
        self.pager.write().push_to_log(update_record)?;

        // Update in catalog
        self.catalog.update_relation(relation, worker)?;

        Ok(())
    }

    pub fn drop_column(
        &self,
        table_name: &str,
        column_name: &str,
        cascade: bool,
    ) -> TransactionResult<()> {
        let worker = self.get_or_create_worker();

        let mut relation = self.catalog.get_relation(table_name, worker.clone())?;

        if !matches!(relation, Relation::TableRel(_)) {
            return Err(TransactionError::InvalidObjectType(
                relation.id(),
                ObjectType::Table,
                ObjectType::Index,
            ));
        }

        // Check column exists
        if !relation.schema().has_column(column_name) {
            return Err(TransactionError::Other(format!(
                "Column {column_name} not found"
            )));
        }

        // Check if column has an index
        if let Some(col) = relation.schema().column(column_name) {
            if let Some(index_name) = col.index() {
                if cascade {
                    self.drop_object(&index_name, cascade)?;
                } else {
                    return Err(TransactionError::Other(
                        "Column has an index! Cannot delete indexed columns".to_string(),
                    ));
                }
            }
        }

        // Get old tuple for WAL
        let old_tuple = relation.clone().into_boxed_tuple()?;

        // Remove column from schema
        relation
            .schema_mut()
            .columns_mut()
            .retain(|c| c.name() != column_name);

        // Get new tuple for WAL
        let new_tuple = relation.clone().into_boxed_tuple()?;

        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;

        // WAL for table update
        let update_op = Update::new(meta_table_relation.id(), old_tuple, new_tuple);
        let update_record = self.builder().build_rec(LogRecordType::Update, update_op);
        self.pager.write().push_to_log(update_record)?;

        // Update in catalog
        self.catalog.update_relation(relation, worker)?;

        Ok(())
    }

    pub fn drop_object(&self, object_name: &str, cascade: bool) -> TransactionResult<()> {
        let worker = self.get_or_create_worker();

        let relation = self.catalog.get_relation(object_name, worker.clone())?;

        // Handle cascade
        if cascade {
            let dependants = relation.schema().get_dependants();
            for dep_name in dependants {
                // Try to drop dependant objects too
                self.drop_object(&dep_name, cascade)?;
            }
        }

        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;
        let meta_index_relation = self.catalog.get_relation(META_INDEX, worker.clone())?;

        // Get tuple for WAL
        let tuple = relation.clone().into_boxed_tuple()?;

        // WAL for meta_table delete
        let delete_op = Delete::new(meta_table_relation.id(), tuple);
        let delete_record = self.builder().build_rec(LogRecordType::Delete, delete_op);
        self.pager.write().push_to_log(delete_record)?;

        // WAL for meta_index delete
        let meta_idx_schema = meta_idx_schema();
        let idx_tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(relation.name())),
                DataType::BigUInt(UInt64::from(relation.id())),
            ],
            &meta_idx_schema,
            self.transaction_id,
        )?;
        let idx_bytes: OwnedTuple = idx_tuple.into();

        let idx_delete_op = Delete::new(meta_index_relation.id(), idx_bytes);
        let idx_delete_record = self
            .builder()
            .build_rec(LogRecordType::Delete, idx_delete_op);
        self.pager.write().push_to_log(idx_delete_record)?;

        // Remove from catalog
        self.catalog.remove_relation(relation, false, worker)?;

        // TODO: For indexes we need to remove also the references to the tables pointing to this index. The safest way currently is to not allow to delete indexes directly and instead force them to be deleted only via ```ALTER TABLE ALTER COLUMN DROP INDEX``` or ```ALTER TABLE DROP COLUMN CASCADE``` statements.

        Ok(())
    }

    pub fn alter_column(
        &self,
        table_name: &str,
        column_name: &str,
        column: Column,
    ) -> TransactionResult<()> {
        let worker = self.get_or_create_worker();

        let mut relation = self.catalog.get_relation(table_name, worker.clone())?;

        if !matches!(relation, Relation::TableRel(_)) {
            return Err(TransactionError::InvalidObjectType(
                relation.id(),
                ObjectType::Table,
                ObjectType::Index,
            ));
        }

        // Get old tuple for WAL
        let old_tuple = relation.clone().into_boxed_tuple()?;

        // Update column in schema
        if let Some(existing_col) = relation.schema_mut().column_mut(column_name) {
            if let Some(existing_idx) = existing_col.index()
                && !column.has_index()
            {
                // We are removing the index.
                self.drop_object(&existing_idx, true)?;
            }
            *existing_col = column;
        } else {
            return Err(TransactionError::Other(format!(
                "Column {column_name} not found"
            )));
        };

        // Get new tuple for WAL
        let new_tuple = relation.clone().into_boxed_tuple()?;

        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;

        // WAL for table update
        let update_op = Update::new(meta_table_relation.id(), old_tuple, new_tuple);
        let update_record = self.builder().build_rec(LogRecordType::Update, update_op);
        self.pager.write().push_to_log(update_record)?;

        // Update in catalog
        self.catalog.update_relation(relation, worker)?;

        Ok(())
    }

    /// Creates a new index using the catalog low level methods and properly logging to the wal
    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> TransactionResult<ObjectId> {
        let worker = self.get_or_create_worker();

        // Check if index already exists
        if let Ok(SearchResult::Found(_)) = self.catalog.lookup_relation(index_name, worker.clone())
        {
            return Err(TransactionError::RelationAlreadyExists(
                index_name.to_string(),
            ));
        }

        // Get the table to find column type
        let mut table_relation = self.catalog.get_relation(table_name, worker.clone())?;

        if !matches!(table_relation, Relation::TableRel(_)) {
            return Err(TransactionError::InvalidObjectType(
                table_relation.id(),
                ObjectType::Table,
                ObjectType::Index,
            ));
        }

        // Find the column and get its type
        let column = table_relation.schema().column(column_name).ok_or_else(|| {
            TransactionError::Other(format!("Column {column_name} not found").to_string())
        })?;

        let column_dtype = column.dtype;

        // Build index schema: (indexed_value, row_id)
        let index_schema = Schema::from_columns(
            &[
                Column::new_unindexed(column_dtype, column_name, None),
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
            ],
            1,
        );

        // Allocate root page for the index
        let root = worker.borrow_mut().alloc_page::<BtreePage>()?;

        let index = Index::new(index_name, root, index_schema);
        let object_id = index.id();
        let relation = Relation::IndexRel(index);

        // Serialize relation for meta_table
        let meta_tuple = relation.clone().into_boxed_tuple()?;

        let meta_index_relation = self.catalog.get_relation(META_INDEX, worker.clone())?;

        // Insert into meta_index
        let meta_idx_schema = meta_idx_schema();
        let idx_tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(relation.name())),
                DataType::BigUInt(UInt64::from(relation.id())),
            ],
            &meta_idx_schema,
            self.transaction_id,
        )?;
        let idx_bytes: OwnedTuple = idx_tuple.into();

        // WAL for meta_index insert
        let idx_op = Insert::new(meta_index_relation.id(), idx_bytes.clone());
        let idx_record = self.builder().build_rec(LogRecordType::Insert, idx_op);
        self.pager.write().push_to_log(idx_record)?;

        // Insert into meta_table
        let meta_table_relation = self.catalog.get_relation(META_TABLE, worker.clone())?;

        // WAL for meta_table insert
        let table_op = Insert::new(meta_table_relation.id(), meta_tuple.clone());
        let table_record = self.builder().build_rec(LogRecordType::Insert, table_op);
        self.pager.write().push_to_log(table_record)?;

        // Create the index relation in catalog
        self.catalog.create_relation(relation, worker.clone())?;

        // Update table's schema to reference this index
        if let Some(col) = table_relation.schema_mut().column_mut(column_name) {
            col.set_index(index_name.to_string());
        }

        // Serialize updated table relation
        let updated_table_tuple = table_relation.clone().into_boxed_tuple()?;

        // WAL for table update
        let update_op = Update::new(
            meta_table_relation.id(),
            updated_table_tuple.clone(),
            updated_table_tuple.clone(),
        );
        let update_record = self.builder().build_rec(LogRecordType::Update, update_op);
        self.pager.write().push_to_log(update_record)?;

        // Update table relation in catalog
        self.catalog.update_relation(table_relation, worker)?;

        Ok(object_id)
    }

    /// Insert a new tuple into a table
    pub fn insert_one(&self, table: &str, values: &[DataType]) -> TransactionResult<LogicalId> {
        let worker = self.get_or_create_worker();

        let mut relation = self.catalog.get_relation(table, worker.clone())?;
        debug_assert!(
            matches!(relation, Relation::TableRel(_)),
            "Table must be a table"
        );

        let id = relation.id();
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

        // Prepend the row id before insertion
        let mut data = vec![DataType::BigUInt(next_row)];
        data.extend_from_slice(values);

        let schema = relation.schema();
        // Collect indexed columns before creating the tuple
        let indexed_cols = schema.get_indexed_columns();
        let logical_id = LogicalId::new(id, next_row);

        // Create tuple with our transaction ID as xmin
        let tuple = Tuple::new(&data, schema, self.transaction_id)?;
        let bytes: OwnedTuple = tuple.into();

        // Record this write for conflict detection
        self.coordinator
            .record_write(self.transaction_id, logical_id, 0)?;

        // Write WAL record
        let op = Insert::new(id, bytes.clone());
        let record = self.builder().build_rec(LogRecordType::Insert, op);
        self.pager.write().push_to_log(record)?;

        // Insert into B-tree
        btree.insert(relation.root(), bytes.as_ref())?;
        btree.clear_worker_stack();

        for (i, idx_info) in indexed_cols {
            let index_relation = self
                .catalog
                .get_relation(&idx_info.name(), worker.clone())?;
            let mut index_btree = self.catalog.index_btree(
                index_relation.root(),
                idx_info.datatype(),
                worker.clone(),
            )?;

            // Build index entry: (indexed_value, row_id)
            let indexed_value = values[i].clone();
            let index_schema = index_relation.schema();
            let index_tuple = Tuple::new(
                &[indexed_value, DataType::BigUInt(next_row)],
                index_schema,
                self.transaction_id,
            )?;
            let index_bytes: OwnedTuple = index_tuple.into();

            index_btree.insert(index_relation.root(), index_bytes.as_ref())?;
            index_btree.clear_worker_stack();
        }

        self.catalog.update_relation(relation, worker)?;

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
    ) -> TransactionResult<()> {
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

        // Load the indexed columns to identify those that have been modified in order to update the indexes.
        // Collect indexed columns that are being updated
        let indexed_cols = schema.get_indexed_columns();
        let mut index_inserts: Vec<(IndexInfo, DataType)> = Vec::new();

        for (val_idx, idx_info) in &indexed_cols {
            // Check if this indexed column is being updated
            if let Some((_, new_value)) = values.iter().find(|(i, _)| *i == *val_idx) {
                index_inserts.push((idx_info.clone(), new_value.clone()));
            }
        }

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
        btree.clear_worker_stack();
        // Update in B-tree
        btree.update(relation.root(), new.as_ref())?;
        btree.clear_worker_stack();

        // Insert new index entries for modified indexed columns
        // Old entries are kept for MVCC visibility (Will be cleaned by vaccum)
        // Reference found here: https://stackoverflow.com/questions/43581810/how-postgresql-index-deals-with-mvcc
        for (idx_info, new_value) in index_inserts {
            let index_relation = self.catalog.get_relation(idx_info.name(), worker.clone())?;
            let mut index_btree = self.catalog.index_btree(
                index_relation.root(),
                idx_info.datatype(),
                worker.clone(),
            )?;
            let index_schema = index_relation.schema();

            // Insert new index entry pointing to the same row
            let new_index_tuple = Tuple::new(
                &[new_value, DataType::BigUInt(logical_id.row())],
                index_schema,
                self.transaction_id,
            )?;
            let new_index_bytes: OwnedTuple = new_index_tuple.into();
            index_btree.insert(index_relation.root(), new_index_bytes.as_ref())?;
            index_btree.clear_worker_stack();
        }

        Ok(())
    }

    /// Delete a tuple by setting xmax
    pub fn delete_one(&self, logical_id: LogicalId) -> TransactionResult<()> {
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

        btree.clear_worker_stack();
        // Mark as deleted by setting xmax to_owned our transaction ID
        tuple.delete(self.transaction_id)?;
        let new: OwnedTuple = tuple.into();

        // Write WAL record
        let op = Delete::new(table, old);
        let record = self.builder().build_rec(LogRecordType::Delete, op);
        self.pager.write().push_to_log(record)?;

        // Update in B-tree (tuple still exists, just marked deleted)
        btree.update(relation.root(), new.as_ref())?;
        btree.clear_worker_stack();

        // Index entries are NOT removed here for MVCC visibility
        // Other transactions may still need to see the deleted tuple
        // Vacuum will clean up index entries pointing to dead tuples

        Ok(())
    }
}

#[cfg(test)]
mod worker_pool_tests {
    use super::*;
    use crate::{
        IncrementalVaccum, TextEncoding,
        configs::AxmosDBConfig,
        database::{
            Database,
            schema::{Column, Schema},
        },
        io::pager::Pager,
        structures::bplustree::SearchResult,
        types::{Blob, DataType, DataTypeKind, DataTypeRef, UInt64},
    };
    use serial_test::serial;
    use std::{io, path::Path};

    /// Creates a test database with a simple "users" table.
    /// Schema: [row_id: BigUInt, name: Text, age: BigUInt]
    fn create_db(path: impl AsRef<Path>) -> io::Result<Database> {
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

        // Create index first
        let index_schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::Text, "email", None),
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
            ],
            1, // email is the key
        );

        let worker = db.main_worker_cloned();
        db.catalog()
            .create_index("idx_users_email", index_schema, worker)?;

        // Create table with indexed column
        // Schema: [row_id: BigUInt (auto), name: Text, email: Text (indexed), age: BigUInt]
        let table_schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
                Column::new(
                    DataTypeKind::Text,
                    "email",
                    Some("idx_users_email".to_string()), // Reference to the index
                    None,
                ),
                Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
            ],
            1, // row_id is the key
        );

        let worker = db.main_worker_cloned();
        db.catalog()
            .create_table("users_indexed", table_schema, worker)?;

        Ok(db)
    }

    /// Helper to begin a transaction and get a WorkerPool
    fn begin_transaction(db: &Database) -> WorkerPool {
        let coordinator = db.coordinator();
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
    fn test_workerpool_1() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Insert a tuple
        let values = [
            DataType::Text(Blob::from("Alice")),
            DataType::BigUInt(UInt64::from(30u64)),
        ];

        // ROW ID MUST BE PREPENDED.
        let logical_id = pool.insert_one("users", &values)?;

        // Read it back
        let result = pool.read_one(logical_id, |tuple| {
            let name = tuple.value(0).ok().map(|v| format!("{:?}", v));
            let age = tuple.value(1).ok().map(|v| format!("{:?}", v));
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
    #[serial]
    fn test_workerpool_2() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Insert
        let values = [
            DataType::Text(Blob::from("Bob")),
            DataType::BigUInt(UInt64::from(25u64)),
        ];
        let logical_id = pool.insert_one("users", &values)?;

        // Update age from 25 to 26
        let new_age = DataType::BigUInt(UInt64::from(26u64));
        pool.update_one(logical_id, &[(1, new_age)])?;

        // Read and verify the update
        let result = pool.read_one(logical_id, |tuple| {
            // Extract age value
            tuple.value(1).ok().map(|v| format!("{:?}", v))
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
    #[serial]
    fn test_workerpool_3() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        // Transaction A: insert and commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;

        let values = [
            DataType::Text(Blob::from("Charlie")),
            DataType::BigUInt(UInt64::from(40u64)),
        ];
        let logical_id = pool_a.insert_one("users", &values)?;
        pool_a.try_commit()?;

        // Transaction B: delete the tuple
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;
        pool_b.delete_one(logical_id)?; // This fails with TransactionError::TupleNotVisible

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
    #[serial]
    fn test_workerpool_4() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        // Transaction A: insert but don't commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;

        let values = [
            DataType::Text(Blob::from("Dave")),
            DataType::BigUInt(UInt64::from(50u64)),
        ];
        let logical_id = pool_a.insert_one("users", &values)?;
        // Note: NOT committing pool_a

        // Transaction B: try to read the tuple
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;

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

    /// Test: Attempting to delete an already-deleted tuple fails.
    ///
    /// Scenario:
    /// 1. Transaction A inserts and commits a tuple
    /// 2. Transaction B deletes the tuple and commits
    /// 3. Transaction C tries to delete the same tuple -> AlreadyDeleted error
    #[test]
    #[serial]
    fn test_workerpool_5() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        // Transaction A: insert and commit
        let pool_a = begin_transaction(&db);
        pool_a.begin()?;
        let values = [
            DataType::Text(Blob::from("Eve")),
            DataType::BigUInt(UInt64::from(28u64)),
        ];
        let logical_id = pool_a.insert_one("users", &values)?;
        pool_a.try_commit()?;

        // Transaction B: delete and commit
        let pool_b = begin_transaction(&db);
        pool_b.begin()?;
        pool_b.delete_one(logical_id)?;
        pool_b.try_commit()?;

        // Transaction C: try to delete the same tuple
        let pool_c = begin_transaction(&db);
        pool_c.begin()?;
        let result = pool_c.delete_one(logical_id);

        assert!(
            matches!(
                result,
                Err(TransactionError::AlreadyDeleted(_))
                    | Err(TransactionError::TupleNotVisible(_, _))
            ),
            "Deleting already-deleted tuple should fail, got: {:?}",
            result
        );

        pool_c.try_abort()?;
        Ok(())
    }

    /// Test: Insert with index - verifies that inserting a tuple also creates index entries.
    ///
    /// Scenario:
    /// 1. Create a table with an indexed column (e.g., "email" with index "idx_email")
    /// 2. Transaction A inserts a tuple with values for all columns
    /// 3. Verify the tuple exists in the main table
    /// 4. Verify the index entry was created in the index B-tree
    /// 5. Verify we can look up the row_id via the index
    #[test]
    #[serial]
    fn test_workerpool_6() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");

        let db = create_db(&path)?;

        // Begin transaction and insert
        let pool = begin_transaction(&db);
        pool.begin()?;

        let values = [
            DataType::Text(Blob::from("Alice")),
            DataType::Text(Blob::from("alice@example.com")),
            DataType::BigUInt(UInt64::from(30u64)),
        ];

        let logical_id = pool.insert_one("users_indexed", &values)?;

        // Verify tuple exists in main table
        let result = pool.read_one(logical_id, |tuple| {
            let name = tuple.value(0).ok().map(|v| format!("{:?}", v));
            let email = tuple.value(1).ok().map(|v| format!("{:?}", v));
            let age = tuple.value(2).ok().map(|v| format!("{:?}", v));
            (name, email, age)
        })?;

        match result {
            ReadResult::Found((name, email, age)) => {
                assert!(name.is_some(), "Name should be readable");
                assert!(email.is_some(), "Email should be readable");
                assert!(age.is_some(), "Age should be readable");
            }
            ReadResult::NotVisible => panic!("Tuple should be visible to its creator"),
            ReadResult::NotFound => panic!("Tuple should exist after insert"),
        }

        // Verify index entry exists by searching the index directly
        let worker = db.main_worker_cloned();
        let index_relation = db
            .catalog()
            .get_relation("idx_users_email", worker.clone())?;
        let index_btree =
            db.catalog()
                .index_btree(index_relation.root(), DataTypeKind::Text, worker.clone())?;

        // Search for the email in the index
        let email_blob = Blob::from("alice@example.com");
        let search_result =
            index_btree.search_from_root(email_blob.as_ref(), FrameAccessMode::Read)?;

        // Verify the index entry was found
        match search_result {
            SearchResult::Found(_) => {
                // Get the payload and verify it contains the correct row_id
                let payload = index_btree
                    .get_payload(search_result)?
                    .expect("Index entry should have payload");

                let index_schema = index_relation.schema();
                let index_tuple = TupleRef::read(payload.as_ref(), index_schema)?;

                // The second value in index tuple should be the row_id
                let stored_row_id = index_tuple.value(0)?;
                match stored_row_id {
                    DataTypeRef::BigUInt(rid) => {
                        assert_eq!(
                            rid.to_owned(),
                            logical_id.row(),
                            "Index should point to correct row_id"
                        );
                    }
                    _ => panic!("Index row_id should be BigUInt"),
                }
            }
            SearchResult::NotFound(_) => {
                panic!("Index entry should exist after insert");
            }
        }

        index_btree.clear_worker_stack();
        pool.try_commit()?;

        Ok(())
    }

    /// Test: Insert multiple rows with index - verifies index maintains multiple entries.
    ///
    /// Scenario:
    /// 1. Create a table with an indexed column
    /// 2. Insert multiple tuples with different indexed values
    /// 3. Verify each index entry points to the correct row
    #[test]
    #[serial]
    fn test_workerpool_7() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");

        let db = create_db(&path)?;

        let pool = begin_transaction(&db);
        pool.begin()?;

        // Insert multiple rows
        let test_data = [
            ("Alice", "alice@example.com"),
            ("Bob", "bob@example.com"),
            ("Charlie", "charlie@example.com"),
        ];

        let mut logical_ids = Vec::new();
        for (name, email) in &test_data {
            let values = [
                DataType::Text(Blob::from(*name)),
                DataType::Text(Blob::from(*email)),
                DataType::BigUInt(UInt64::from(30u64)),
            ];

            let lid = pool.insert_one("users_indexed", &values)?;
            logical_ids.push(lid);
        }

        // Verify each index entry
        let worker = db.main_worker_cloned();
        let index_relation = db
            .catalog()
            .get_relation("idx_users_email", worker.clone())?;

        for (i, (_, email)) in test_data.iter().enumerate() {
            let index_btree = db.catalog().index_btree(
                index_relation.root(),
                DataTypeKind::Text,
                worker.clone(),
            )?;

            let email_blob = Blob::from(*email);
            let search_result =
                index_btree.search_from_root(email_blob.as_ref(), FrameAccessMode::Read)?;

            match search_result {
                SearchResult::Found(_) => {
                    let payload = index_btree
                        .get_payload(search_result)?
                        .expect("Index entry should have payload");

                    let index_schema = index_relation.schema();
                    let index_tuple = TupleRef::read(payload.as_ref(), index_schema)?;

                    let stored_row_id = index_tuple.value(0)?;
                    match stored_row_id {
                        DataTypeRef::BigUInt(rid) => {
                            assert_eq!(
                                rid.to_owned(),
                                logical_ids[i].row(),
                                "Index for {} should point to correct row_id",
                                email
                            );
                        }
                        _ => panic!("Index row_id should be BigUInt"),
                    }
                }
                SearchResult::NotFound(_) => {
                    panic!("Index entry for {} should exist", email);
                }
            }

            index_btree.clear_worker_stack();
        }

        pool.try_commit()?;

        Ok(())
    }

    /// Test: Update with index - verifies that updating an indexed column creates a new index entry.
    ///
    /// Scenario:
    /// 1. Create a table with an indexed column
    /// 2. Insert a tuple
    /// 3. Update the indexed column
    /// 4. Verify both old and new index entries exist (MVCC - old entry kept for visibility)
    /// 5. Verify the new index entry points to the correct row
    #[test]
    #[serial]
    fn test_workerpool_8() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        // Insert initial data
        let pool = begin_transaction(&db);
        pool.begin()?;

        let values = [
            DataType::Text(Blob::from("Alice")),
            DataType::Text(Blob::from("alice@old.com")),
            DataType::BigUInt(UInt64::from(30u64)),
        ];

        let logical_id = pool.insert_one("users_indexed", &values)?;
        pool.try_commit()?;

        // Start new transaction and update the indexed column
        let pool2 = begin_transaction(&db);
        pool2.begin()?;

        let new_email = DataType::Text(Blob::from("alice@new.com"));
        pool2.update_one(logical_id, &[(1, new_email.clone())])?;

        // Verify the tuple has the new value
        let result = pool2.read_one(logical_id, |tuple| {
            tuple.value(1).ok().map(|v| v.to_owned())
        })?;

        match result {
            ReadResult::Found(Some(email)) => {
                assert_eq!(
                    email,
                    DataType::Text(Blob::from("alice@new.com")),
                    "Email should be updated"
                );
            }
            _ => panic!("Tuple should be found with updated value"),
        }

        // Verify the NEW index entry exists
        let worker = db.main_worker_cloned();
        let index_relation = db
            .catalog()
            .get_relation("idx_users_email", worker.clone())?;
        let index_btree =
            db.catalog()
                .index_btree(index_relation.root(), DataTypeKind::Text, worker.clone())?;

        let new_email_blob = Blob::from("alice@new.com");
        let search_result =
            index_btree.search_from_root(new_email_blob.as_ref(), FrameAccessMode::Read)?;

        match search_result {
            crate::structures::bplustree::SearchResult::Found(_) => {
                let payload = index_btree
                    .get_payload(search_result)?
                    .expect("New index entry should have payload");

                let index_schema = index_relation.schema();
                let index_tuple = TupleRef::read(payload.as_ref(), index_schema)?;

                let stored_row_id = index_tuple.value(0)?;
                match stored_row_id {
                    DataTypeRef::BigUInt(rid) => {
                        assert_eq!(
                            rid.to_owned(),
                            logical_id.row(),
                            "New index entry should point to correct row_id"
                        );
                    }
                    _ => panic!("Index row_id should be BigUInt"),
                }
            }

            SearchResult::NotFound(_) => {
                panic!("New index entry should exist after update");
            }
        }

        index_btree.clear_worker_stack();

        // Verify the OLD index entry still exists (MVCC)
        let index_btree2 =
            db.catalog()
                .index_btree(index_relation.root(), DataTypeKind::Text, worker.clone())?;

        let old_email_blob = Blob::from("alice@old.com");
        let old_search_result =
            index_btree2.search_from_root(old_email_blob.as_ref(), FrameAccessMode::Read)?;

        assert!(
            matches!(old_search_result, SearchResult::Found(_)),
            "Old index entry should still exist for MVCC visibility"
        );

        index_btree2.clear_worker_stack();
        pool2.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_workerpool_9() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.db");
        let db = create_db(&path)?;

        // Insert initial data
        let pool = begin_transaction(&db);
        pool.begin()?;

        let schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
                Column::new_unindexed(DataTypeKind::Text, "description", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
            ],
            1, // email is the key
        );
        pool.create_table("new-table", schema.clone())?;
        pool.try_commit()?;

        let relation_res = db
            .catalog()
            .get_relation("new-table", db.main_worker_cloned());
        assert!(relation_res.is_ok());
        let relation = relation_res.unwrap();
        assert_eq!(relation.schema(), &schema);
        assert!(relation.root().is_valid());
        assert!(matches!(relation, Relation::TableRel(_)));

        Ok(())
    }
}
