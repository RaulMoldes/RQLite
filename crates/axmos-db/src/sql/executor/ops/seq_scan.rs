// src/sql/executor/operators/seq_scan.rs
use std::io::{self, Error as IoError, ErrorKind};

use crate::{
    database::schema::Schema,
    sql::{
        binder::ast::BoundExpression,
        executor::eval::ExpressionEvaluator,
        executor::{ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::SeqScanOp,
    },
    storage::tuple::TupleRef,
    structures::{bplustree::BPlusTreePositionIterator, comparator::NumericComparator},
};

/// Sequential scan executor (iterates over all visible tuples in a table).
///
/// This executor performs a full table scan using the B+tree leaf page linked list.
/// It respects MVCC visibility rules through the transaction's snapshot.
pub(crate) struct SeqScanExecutor {
    /// Physical operator definition
    op: SeqScanOp,
    /// Execution context with worker, catalog, and snapshot
    ctx: ExecutionContext,
    /// Output schema (may be projected)
    output_schema: Schema,
    /// Current scan state
    state: ScanState,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Internal state machine for the scan
enum ScanState {
    /// Not yet initialized
    Uninitialized,
    /// Actively scanning a table.
    Scanning(BPlusTreePositionIterator<NumericComparator>),
    /// Scan completed or closed
    Closed,
}

impl SeqScanExecutor {
    pub(crate) fn new(op: SeqScanOp, ctx: ExecutionContext) -> Self {
        let output_schema = op.schema.clone();
        Self {
            op,
            ctx,
            output_schema,

            state: ScanState::Uninitialized,
            stats: ExecutionStats::default(),
        }
    }

    fn set_state(&mut self, new: ScanState) {
        self.state = new;
    }

    /// Convert a TupleRef to a Row, applying projection if specified
    fn tuple_to_row(&self, bytes: &[u8]) -> io::Result<Option<Row>> {
        let maybe_tuple =
            TupleRef::read_for_snapshot(&bytes, &self.op.schema, self.ctx.snapshot())?;

        if maybe_tuple.is_none() {
            return Ok(None);
        };

        let tuple = maybe_tuple.unwrap();
        let mut row = Vec::new();

        if let Some(ref columns) = self.op.columns {
            // Project specific columns
            for &col_idx in columns {
                let value = tuple.value(col_idx).map_err(|e| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!("Failed to read column {}: {}", col_idx, e),
                    )
                })?;
                row.push(value.to_owned());
            }
        } else {
            // Return all columns
            let num_keys = self.op.schema.num_keys as usize;
            for i in 0..num_keys {
                let value = tuple.key(i).map_err(|e| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!("Failed to read key {}: {}", i, e),
                    )
                })?;
                row.push(value.to_owned());
            }

            let num_values = self.op.schema.values().len();
            for i in 0..num_values {
                let value = tuple.value(i).map_err(|e| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!("Failed to read value {}: {}", i, e),
                    )
                })?;
                row.push(value.to_owned());
            }
        }

        // Apply predicate filter if present
        if !self.evaluate_predicate(&row)? {
            return Ok(None); // Filtered out
        }

        Ok(Some(row))
    }

    /// Evaluate a filter predicate against a row
    /// Evaluate the predicate against a row.
    /// Returns true if the row passes the filter (or if there's no predicate).
    fn evaluate_predicate(&self, row: &Row) -> io::Result<bool> {
        match &self.op.predicate {
            None => Ok(true),
            Some(pred) => {
                let evaluator = ExpressionEvaluator::new(row, self.schema());
                evaluator.evaluate_as_bool(pred.clone())
            }
        }
    }

    /// Get current execution statistics
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for SeqScanExecutor {
    fn open(&mut self) -> io::Result<()> {
        // Get table root page from catalog
        let relation = self
            .ctx
            .catalog()
            .get_relation_unchecked(self.op.table_id, self.ctx.worker().clone())?;

        let root_page = relation.root();
        let mut table = self.ctx.table(root_page)?;
        let iterator = table.iter_positions()?;
        self.set_state(ScanState::Scanning(iterator));

        Ok(())
    }

    fn next(&mut self) -> io::Result<Option<Row>> {
        loop {
            match &mut self.state {
                ScanState::Uninitialized => {
                    return Err(IoError::new(
                        ErrorKind::Other,
                        "Executor not opened. Call open() first.",
                    ));
                }

                ScanState::Closed => return Ok(None),

                ScanState::Scanning(cursor) => {
                    // Try to advance the cursor
                    let next_pos = match cursor.next() {
                        Some(res) => res?, // propagate io::Error
                        None => {
                            // end of scan → close and return None
                            self.state = ScanState::Closed;
                            return Ok(None);
                        }
                    };

                    self.stats.rows_scanned += 1;

                    // Access tuple at this position
                    let tree = cursor.get_tree();
                    let maybe_row = tree
                        .with_cell_at(next_pos, |bytes| self.tuple_to_row(bytes))?
                        .and_then(|result| Ok(result))?;

                    // Row filtered out
                    if maybe_row.is_none() {
                        continue;
                    }

                    // Row is produced
                    self.stats.rows_produced += 1;
                    return Ok(maybe_row);
                }
            }
        }
    }

    fn close(&mut self) -> io::Result<()> {
        self.set_state(ScanState::Closed);

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}

#[cfg(test)]
mod seq_scan_tests {
    use super::*;
    use crate::{
        AxmosDBConfig, IncrementalVaccum, TextEncoding,
        database::{Database, errors::TransactionResult, schema::Column},
        io::pager::{Pager, SharedPager},
        sql::planner::physical::SeqScanOp,
        transactions::worker::WorkerPool,
        types::{Blob, DataType, DataTypeKind, ObjectId, UInt64},
    };
    use serial_test::serial;
    use std::path::Path;

    fn create_test_db(path: impl AsRef<Path>) -> io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        // Create users table with schema:
        // [row_id: BigUInt (key), name: Text, age: BigUInt]
        let schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
                Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
            ],
            1, // row_id is the key
        );

        let worker = db.main_worker_cloned();
        db.catalog().create_table("users", schema, worker)?;

        Ok(db)
    }

    fn get_users_table_id(db: &Database) -> io::Result<ObjectId> {
        let relation = db
            .catalog()
            .get_relation("users", db.main_worker_cloned())?;
        Ok(relation.id())
    }

    fn get_users_schema(db: &Database) -> io::Result<Schema> {
        let relation = db
            .catalog()
            .get_relation("users", db.main_worker_cloned())?;
        Ok(relation.schema().clone())
    }

    fn insert_test_users(pool: &WorkerPool, count: usize) -> TransactionResult<()> {
        let names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"];
        let ages = [25u64, 30, 35, 28, 22, 40];

        for i in 0..count {
            let name = names[i % names.len()];
            let age = ages[i % ages.len()];

            pool.insert_one(
                "users",
                &[
                    DataType::Text(Blob::from(name)),
                    DataType::BigUInt(UInt64::from(age)),
                ],
            )?;
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_empty_table() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Begin transaction
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        // Get execution context
        let ctx = pool.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        // Create SeqScan operator
        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        // Execute scan
        executor.open()?;

        let row = executor.next()?;
        assert!(row.is_none(), "Empty table should return no rows");

        executor.close()?;
        pool.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_single_row() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Insert one row
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        pool.insert_one(
            "users",
            &[
                DataType::Text(Blob::from("Alice")),
                DataType::BigUInt(UInt64::from(30u64)),
            ],
        )?;

        pool.try_commit()?;

        // Now scan with a new transaction
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        // Should get one row
        let row1 = executor.next()?;
        assert!(row1.is_some(), "Should have one row");

        let row = row1.unwrap();
        assert_eq!(row.len(), 2); // name, age
        assert_eq!(row[0], DataType::Text(Blob::from("Alice")));
        assert_eq!(row[1], DataType::BigUInt(UInt64::from(30u64)));

        // No more rows
        let row2 = executor.next()?;
        assert!(row2.is_none(), "Should have no more rows");

        executor.close()?;
        pool2.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_multiple_rows() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Insert multiple rows
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        insert_test_users(&pool, 5)?;
        pool.try_commit()?;

        // Scan
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let mut count = 0;
        while let Some(row) = executor.next()? {
            count += 1;
            assert_eq!(row.len(), 2);
            // Verify we get Text and BigUInt types
            assert!(matches!(row[0], DataType::Text(_)));
            assert!(matches!(row[1], DataType::BigUInt(_)));
        }

        assert_eq!(count, 5, "Should have 5 rows");
        assert_eq!(executor.stats().rows_produced, 5);
        assert_eq!(executor.stats().rows_scanned, 5);

        executor.close()?;
        pool2.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_mvcc_visibility_uncommitted() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Transaction 1: Insert but DON'T commit
        let handle1 = db.coordinator().begin()?;
        let pool1 = WorkerPool::from(handle1);
        pool1.begin()?;

        pool1.insert_one(
            "users",
            &[
                DataType::Text(Blob::from("Ghost")),
                DataType::BigUInt(UInt64::from(99u64)),
            ],
        )?;
        // Note: NOT committing pool1

        // Transaction 2: Should NOT see uncommitted row
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let row = executor.next()?;
        assert!(
            row.is_none(),
            "Should not see uncommitted row from other transaction"
        );

        executor.close()?;

        // Cleanup
        pool1.try_abort()?;
        pool2.try_abort()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_mvcc_visibility_committed() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Transaction 1: Insert and commit
        let handle1 = db.coordinator().begin()?;
        let pool1 = WorkerPool::from(handle1);
        pool1.begin()?;

        pool1.insert_one(
            "users",
            &[
                DataType::Text(Blob::from("Visible")),
                DataType::BigUInt(UInt64::from(42u64)),
            ],
        )?;

        pool1.try_commit()?;

        // Transaction 2: Should see committed row
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let row = executor.next()?;
        assert!(row.is_some(), "Should see committed row");

        let row = row.unwrap();
        assert_eq!(row[0], DataType::Text(Blob::from("Visible")));

        executor.close()?;
        pool2.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_mvcc_deleted_rows() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Transaction 1: Insert and commit
        let handle1 = db.coordinator().begin()?;
        let pool1 = WorkerPool::from(handle1);
        pool1.begin()?;

        let logical_id = pool1.insert_one(
            "users",
            &[
                DataType::Text(Blob::from("ToDelete")),
                DataType::BigUInt(UInt64::from(50u64)),
            ],
        )?;

        pool1.try_commit()?;

        // Transaction 2: Delete and commit
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        pool2.delete_one(logical_id)?;
        pool2.try_commit()?;

        // Transaction 3: Should NOT see deleted row
        let handle3 = db.coordinator().begin()?;
        let pool3 = WorkerPool::from(handle3);
        pool3.begin()?;

        let ctx = pool3.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let row = executor.next()?;
        assert!(row.is_none(), "Should not see deleted row");

        executor.close()?;
        pool3.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_same_transaction_sees_own_inserts() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Single transaction: insert then scan
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        pool.insert_one(
            "users",
            &[
                DataType::Text(Blob::from("SameTransaction")),
                DataType::BigUInt(UInt64::from(100u64)),
            ],
        )?;

        // Scan within same transaction
        let ctx = pool.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let row = executor.next()?;
        assert!(
            row.is_some(),
            "Should see own uncommitted insert within same transaction"
        );

        let row = row.unwrap();
        assert_eq!(row[0], DataType::Text(Blob::from("SameTransaction")));

        executor.close()?;
        pool.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_many_rows_pagination() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Insert many rows to potentially span multiple pages
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        let num_rows = 100;
        for i in 0..num_rows {
            pool.insert_one(
                "users",
                &[
                    DataType::Text(Blob::from(format!("User{}", i).as_str())),
                    DataType::BigUInt(UInt64::from(i as u64)),
                ],
            )?;
        }

        pool.try_commit()?;

        // Scan all rows
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        let mut count = 0;
        while let Some(_row) = executor.next()? {
            count += 1;
        }

        assert_eq!(count, num_rows, "Should scan all {} rows", num_rows);

        executor.close()?;
        pool2.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_executor_not_opened_error() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        let ctx = pool.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        // Try to call next() without open()
        let result = executor.next();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Executor not opened")
        );

        pool.try_abort()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_close_idempotent() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        let ctx = pool.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        // Close multiple times should be safe
        executor.close()?;
        executor.close()?;
        executor.close()?;

        // After close, next should return None
        let row = executor.next()?;
        assert!(row.is_none());

        pool.try_commit()?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_seq_scan_stats_tracking() -> TransactionResult<()> {
        let dir = tempfile::tempdir()?;
        let db = create_test_db(dir.path().join("test.db"))?;

        // Insert rows
        let handle = db.coordinator().begin()?;
        let pool = WorkerPool::from(handle);
        pool.begin()?;

        insert_test_users(&pool, 10)?;
        pool.try_commit()?;

        // Scan and check stats
        let handle2 = db.coordinator().begin()?;
        let pool2 = WorkerPool::from(handle2);
        pool2.begin()?;

        let ctx = pool2.execution_context();
        let table_id = get_users_table_id(&db)?;
        let schema = get_users_schema(&db)?;

        let op = SeqScanOp::new(table_id, schema);
        let mut executor = SeqScanExecutor::new(op, ctx);

        executor.open()?;

        while executor.next()?.is_some() {}

        let stats = executor.stats();
        assert_eq!(stats.rows_scanned, 10);
        assert_eq!(stats.rows_produced, 10);

        executor.close()?;
        pool2.try_commit()?;

        Ok(())
    }
}
