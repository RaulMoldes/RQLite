#![feature(allocator_api)]
#![feature(pointer_is_aligned_to)]
#![feature(set_ptr_value)]
#![feature(debug_closure_helpers)]
#![feature(trait_alias)]
#![allow(dead_code)]
#![allow(unused_assignments)]
#![allow(unused_variables)]
#![feature(slice_ptr_get)]
#![feature(concat_bytes)]
#![feature(str_from_raw_parts)]
#![feature(negative_impls)]
#![feature(current_thread_id)]
#![feature(box_vec_non_null)]
pub mod common;
pub mod io;
mod macros;
mod multithreading;
pub mod runtime;
pub mod schema;
pub mod sql;
mod storage;
pub mod tcp;
mod tree;
pub mod types;

pub use common::*;
/// Jemalloc apparently has better alignment guarantees than rust's standard allocator.
/// Rust's global system allocator does not seem to guarantee that allocations are aligned.
/// Therefore we prefer to use [Jemalloc], to ensure allocations are aligned.
///
///  Docs on Jemalloc: https://manpages.debian.org/jessie/libjemalloc-dev/jemalloc.3.en.html.
///
/// More on alignment guarantees: https://github.com/jemalloc/jemalloc/issues/1533
/// We also provide an API with a custom [Direct-IO] allocator (see [io::disk::linux] for details), but Jemalloc has performed better in benchmarks.
#[cfg(not(miri))]
use jemallocator::Jemalloc;
pub use types::*;
pub(crate) use types::*;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
pub use common::*;

use crate::{
    DBConfig,
    io::{
        disk::FileOperations,
        pager::{BtreeBuilder, Pager, SharedPager, WalRecuperator},
    },
    multithreading::{
        coordinator::TransactionCoordinator,
        runner::{BoxError, TaskError, TaskRunner},
    },
    runtime::{
        QueryError, QueryPreparator, QueryResult, QueryRunner, QueryRunnerBuilder,
        context::TransactionContext,
    },
    schema::catalog::{Catalog, CatalogTrait, SharedCatalog},
    storage::page::BtreePage,
    tree::accessor::BtreeWriteAccessor,
    types::{ObjectId, PageId, TransactionId},
};

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::{Error as IoError, Write},
    path::{Path, PathBuf},
};

/// Database-level errors.
#[derive(Debug)]
pub enum DatabaseError {
    /// I/O error during database operations.
    Io(IoError),
    /// Query execution error.
    Query(QueryError),
    /// Task execution error.
    Task(TaskError),
    /// Database already exists (for create).
    AlreadyExists(PathBuf),
    /// Database not found (for open).
    NotFound(PathBuf),
    /// Recovery failed.
    RecoveryFailed(String),
    /// Other error.
    Other(String),
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Query(e) => write!(f, "Query error: {}", e),
            Self::Task(e) => write!(f, "Task error: {}", e),
            Self::AlreadyExists(p) => write!(f, "Database already exists: {}", p.display()),
            Self::NotFound(p) => write!(f, "Database not found: {}", p.display()),
            Self::RecoveryFailed(msg) => write!(f, "Recovery failed: {}", msg),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for DatabaseError {}

impl From<IoError> for DatabaseError {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}

impl From<QueryError> for DatabaseError {
    fn from(e: QueryError) -> Self {
        Self::Query(e)
    }
}

impl From<TaskError> for DatabaseError {
    fn from(e: TaskError) -> Self {
        Self::Task(e)
    }
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// Converts any error into a BoxError for use in task closures.
fn box_err<E: Error + Send + 'static>(e: E) -> BoxError {
    Box::new(e)
}

/// Main database instance.
pub struct Database {
    /// Path to the database file.
    path: PathBuf,
    /// Shared pager for page I/O.
    pager: SharedPager,
    /// Shared catalog for schema metadata.
    catalog: SharedCatalog,
    /// Transaction coordinator for MVCC.
    coordinator: TransactionCoordinator,
    /// Task runner for parallel query execution.
    task_runner: TaskRunner,
    /// Database configuration.
    config: DBConfig,
}

impl Database {
    /// Creates a new database at the specified path.
    ///
    /// This will create the database file and WAL. Fails if the database
    /// already exists.
    pub fn create(path: impl AsRef<Path>, config: DBConfig) -> DatabaseResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Check if database already exists
        if path.exists() {
            return Err(DatabaseError::AlreadyExists(path));
        }

        // Create pager with configuration
        let pager = Pager::from_config(config, &path)?;
        let pager = SharedPager::from(pager);

        // Allocate meta table and meta index pages
        let (meta_table_page, meta_index_page) = {
            let mut p = pager.write();
            let meta_table = p.allocate_page::<BtreePage>()?;
            let meta_index = p.allocate_page::<BtreePage>()?;
            (meta_table, meta_index)
        };

        // Create catalog
        let catalog = Catalog::new(meta_table_page, meta_index_page);
        let catalog = SharedCatalog::from(catalog);

        // Create transaction coordinator
        let coordinator = TransactionCoordinator::new(pager.clone());

        // Create task runner
        let task_runner = TaskRunner::new(config.pool_size, pager.clone(), coordinator.clone());

        Ok(Self {
            path,
            pager,
            catalog,
            coordinator,
            task_runner,
            config,
        })
    }

    /// Opens an existing database at the specified path.
    ///
    /// This will open the database file and WAL, then run recovery if needed.
    pub fn open(path: impl AsRef<Path>, config: DBConfig) -> DatabaseResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Check if database exists
        if !path.exists() {
            return Err(DatabaseError::NotFound(path));
        }

        // Open pager
        let pager = Pager::open(&path)?;
        let pager = SharedPager::from(pager);

        // Read persisted state from page zero header
        let (last_created_transaction, last_object, last_committed) = {
            let p = pager.read();
            let header = p.header_unchecked();
            (
                header.last_created_transaction,
                header.last_stored_object,
                header.last_committed_transaction,
            )
        };

        // Create catalog with last stored object
        let catalog = Catalog::new(1, 2).with_last_stored_object(last_object);
        let catalog = SharedCatalog::from(catalog);

        // Create transaction coordinator with last transaction
        let coordinator = TransactionCoordinator::new(pager.clone())
            .with_last_created_transaction(last_created_transaction)
            .with_last_committed_transaction(last_committed);

        // Create task runner
        let task_runner = TaskRunner::new(config.pool_size, pager.clone(), coordinator.clone());

        let db = Self {
            path,
            pager,
            catalog,
            coordinator,
            task_runner,
            config,
        };

        // Run recovery
        db.run_recovery()?;

        Ok(db)
    }

    /// Opens or creates a database at the specified path.
    ///
    /// If the database exists, opens it. Otherwise, creates a new one.
    pub fn open_or_create(path: impl AsRef<Path>, config: DBConfig) -> DatabaseResult<Self> {
        let path = path.as_ref();
        if path.exists() {
            Self::open(path, config)
        } else {
            Self::create(path, config)
        }
    }

    /// Runs WAL recovery to restore database to consistent state.
    fn run_recovery(&self) -> DatabaseResult<()> {
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        self.task_runner.run(move |ctx| {
            // Begin a recovery transaction
            let handle = coordinator.begin().map_err(box_err)?;

            // Create write context for recovery
            let tree_builder = {
                let p = pager.read();
                BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                    .with_pager(pager.clone())
            };

            let tx_ctx = TransactionContext::new(
                BtreeWriteAccessor::new(),
                pager.clone(),
                catalog.clone(),
                handle.clone(),
            )
            .map_err(box_err)?;

            let mut recuperator = WalRecuperator::new_with_context(tx_ctx);

            // Run analysis INSIDE the closure using the cloned pager
            let analysis = pager.write().run_analysis().map_err(box_err)?;
            println!("Analysis result: {:?}", analysis);
            // Run recovery through recuperator
            recuperator.run_recovery(&analysis).map_err(box_err)?;

            // Truncate WAL
            pager.write().truncate_wal().map_err(box_err)?;

            // Commit recovery transaction
            handle.commit().map_err(box_err)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Executes a SQL query and returns the result.
    pub fn execute(&self, sql: &str) -> DatabaseResult<QueryResult> {
        let sql = sql.to_string();
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        let result = self.task_runner.run_with_result(move |_ctx| {
            // Begin transaction
            let handle = coordinator.begin().map_err(box_err)?;

            // Create tree builder
            let tree_builder = {
                let p = pager.read();
                BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                    .with_pager(pager.clone())
            };

            let preparator =
                QueryPreparator::new(catalog.clone(), pager.clone(), tree_builder, handle.clone());

            let handle = preparator.build_and_run(&sql, false).map_err(box_err)?;
            handle.commit().map_err(box_err)?;
            Ok(handle.into())
        })?;

        Ok(result)
    }

    /// Executes multiple SQL statements in a single transaction.
    pub fn execute_batch(&self, statements: &[&str]) -> DatabaseResult<Vec<QueryResult>> {
        let statements: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        let results = self.task_runner.run_with_result(move |_ctx| {
            // Begin transaction
            let global_handle = coordinator.begin().map_err(box_err)?;

            // Create tree builder
            let tree_builder = {
                let p = pager.read();
                BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                    .with_pager(pager.clone())
            };

            let preparator = QueryPreparator::new(
                catalog.clone(),
                pager.clone(),
                tree_builder,
                global_handle.clone(),
            );

            // Execute all statements
            let mut handles = Vec::with_capacity(statements.len());

            for sql in statements {
                let result = preparator.build_and_run(&sql, false).map_err(box_err)?;
                handles.push(result);
            }

            if let Some(last_handle) = handles.last() {
                last_handle.commit().map_err(box_err)?;
            }
            let query_results: Vec<QueryResult> = handles.into_iter().map(|c| c.into()).collect();
            Ok(query_results)
        })?;

        Ok(results)
    }

    /// Flushes all pending writes to disk.
    pub fn flush(&self) -> DatabaseResult<()> {
        self.pager.write().flush()?;
        Ok(())
    }

    /// Synchronizes all data to disk.
    pub fn sync(&self) -> DatabaseResult<()> {
        self.flush()?;
        self.update_header()?;
        self.pager.read().sync_all()?;
        Ok(())
    }

    /// Updates the page zero header with current state.
    fn update_header(&self) -> DatabaseResult<()> {
        // Get current last object ID from catalog
        let last_object = self.catalog.read().get_next_object_id();
        let last_committed_transaction = self.coordinator.get_last_committed();
        let last_created_transaction = self.coordinator.get_next_transaction();

        // Update header in pager
        {
            let mut pager = self.pager.write();
            let header = pager.header_unchecked_mut();
            header.last_stored_object = last_object;
            header.last_created_transaction = last_created_transaction;
            header.last_committed_transaction = last_committed_transaction;
        }

        Ok(())
    }

    /// Returns the database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the database configuration.
    pub fn config(&self) -> &DBConfig {
        &self.config
    }

    /// Returns a reference to the shared pager.
    pub fn pager(&self) -> &SharedPager {
        &self.pager
    }

    /// Returns a reference to the shared catalog.
    pub fn catalog(&self) -> &SharedCatalog {
        &self.catalog
    }

    /// Returns a reference to the transaction coordinator.
    pub fn coordinator(&self) -> &TransactionCoordinator {
        &self.coordinator
    }

    /// Runs ANALYZE to update table statistics.
    pub fn analyze(&self, sample_rate: f64, max_sample_rows: usize) -> DatabaseResult<()> {
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        self.task_runner.run(move |_ctx| {
            let handle = coordinator.begin().map_err(box_err)?;
            let snapshot = handle.snapshot();

            let tree_builder = {
                let p = pager.read();
                BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                    .with_pager(pager.clone())
            };

            catalog
                .write()
                .analyze(&tree_builder, &snapshot, sample_rate, max_sample_rows)
                .map_err(box_err)?;

            handle.commit().map_err(box_err)?;
            Ok(())
        })?;

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best-effort flush on drop
        let _ = self.flush();
    }
}

#[cfg(test)]
mod database_tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_db_path() -> (TempDir, PathBuf) {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_create_database() {
        let (_dir, path) = temp_db_path();
        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");
        assert!(path.exists());
        drop(db);
    }

    #[test]
    fn test_create_existing_fails() {
        let (_dir, path) = temp_db_path();
        let _db = Database::create(&path, DBConfig::default()).expect("Failed to create database");

        let result = Database::create(&path, DBConfig::default());
        assert!(matches!(result, Err(DatabaseError::AlreadyExists(_))));
    }

    #[test]
    fn test_open_nonexistent_fails() {
        let (_dir, path) = temp_db_path();
        let result = Database::open(&path, DBConfig::default());
        assert!(matches!(result, Err(DatabaseError::NotFound(_))));
    }

    #[test]
    fn test_open_or_create() {
        let (_dir, path) = temp_db_path();

        // Should create
        let db1 = Database::open_or_create(&path, DBConfig::default())
            .expect("Failed to create database");
        drop(db1);

        // Should open
        let db2 =
            Database::open_or_create(&path, DBConfig::default()).expect("Failed to open database");
        drop(db2);
    }

    #[test]
    fn test_database_analyze() {
        let (_dir, path) = temp_db_path();

        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");

        // Create a table
        db.execute("CREATE TABLE users (id BIGINT, name TEXT, age INT)")
            .expect("Failed to create table");

        // Insert some data
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();

        // Run analyze
        let result = db.analyze(1.0, 10000);
        assert!(result.is_ok(), "Analyze failed: {:?}", result.err());
    }

    #[test]
    fn test_database_analyze_empty() {
        let (_dir, path) = temp_db_path();

        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");

        // Analyze on empty database should succeed
        let result = db.analyze(1.0, 10000);
        assert!(
            result.is_ok(),
            "Analyze on empty db failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_database_analyze_with_sampling() {
        let (_dir, path) = temp_db_path();

        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");

        // Create table and insert data
        db.execute("CREATE TABLE items (id BIGINT, value INT)")
            .unwrap();

        for i in 0..100 {
            db.execute(&format!("INSERT INTO items VALUES ({}, {})", i, i * 10))
                .unwrap();
        }

        // Analyze with 10% sampling
        let result = db.analyze(0.1, 1000);
        assert!(
            result.is_ok(),
            "Analyze with sampling failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_recovery_after_clean_shutdown() {
        let (_dir, path) = temp_db_path();

        // Create database and insert data
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE users (id BIGINT, name TEXT, age INT)")
                .expect("Failed to create table");

            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
                .unwrap();

            db.flush().expect("Failed to flush");
        }

        // Reopen database - recovery should run
        {
            let db = Database::open(&path, DBConfig::default())
                .expect("Failed to open database after shutdown");

            // Verify data persisted
            let result = db.execute("SELECT COUNT(*) FROM users").unwrap();
            let rows = result.into_rows().expect("Expected rows result");
            assert_eq!(rows.len(), 1, "Expected 1 row from COUNT(*)");
        }
    }

    #[test]
    fn test_recovery_preserves_inserts() {
        let (_dir, path) = temp_db_path();
        let count = 50;

        // Create database with data
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE items (id BIGINT, value INT)")
                .expect("Failed to create table");

            for i in 0..count {
                db.execute(&format!("INSERT INTO items VALUES ({}, {})", i, i * 10))
                    .unwrap();
            }

            db.flush().expect("Failed to flush");
        }

        // Reopen and verify
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM items").unwrap();
            let rows = result.into_rows().expect("Expected rows result");

            assert_eq!(rows.len(), 1);
            let value = rows[0][0].as_big_int().unwrap();
            assert_eq!(value.value(), count);
        }
    }

    #[test]
    fn test_recovery_preserves_updates() {
        let (_dir, path) = temp_db_path();

        // Create database, insert, then update
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE users (id BIGINT, name TEXT, age INT)")
                .expect("Failed to create table");

            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
                .unwrap();
            db.execute("UPDATE users SET age = 31 WHERE id = 1")
                .unwrap();

            let result = db.execute("SELECT age FROM users WHERE id = 1").unwrap();
            let rows = result.into_rows().expect("Expected rows result");

            assert_eq!(rows.len(), 1, "Expected 1 row before flush");

            db.flush().expect("Failed to flush");
        }

        // Reopen and verify update persisted
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT * FROM users").unwrap();
            let rows = result.into_rows().expect("Expected rows result");
            assert_eq!(rows.len(), 1, "Expected 1 row");
        }
    }

    #[test]
    fn test_recovery_preserves_deletes() {
        let (_dir, path) = temp_db_path();

        // Create database, insert, then delete
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE users (id BIGINT, name TEXT, age INT)")
                .expect("Failed to create table");

            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
                .unwrap();
            db.execute("DELETE FROM users WHERE id = 2").unwrap();

            db.flush().expect("Failed to flush");
        }

        // Reopen and verify delete persisted
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM users").unwrap();
            let rows = result.into_rows().expect("Expected rows result");
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_multiple_tables() {
        let (_dir, path) = temp_db_path();

        // Create multiple tables with data
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE users (id BIGINT, name TEXT)")
                .expect("Failed to create users table");
            db.execute("CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DOUBLE)")
                .expect("Failed to create orders table");

            db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();

            db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

            db.execute("INSERT INTO orders VALUES (100, 1, 99.99)")
                .unwrap();

            db.execute("INSERT INTO orders VALUES (101, 1, 149.99)")
                .unwrap();

            db.execute("INSERT INTO orders VALUES (102, 2, 49.99)")
                .unwrap();

            db.flush().expect("Failed to flush");
        }

        // Reopen and verify both tables
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let users_result = db.execute("SELECT COUNT(*) FROM users").unwrap();
            let users_rows = users_result.into_rows().expect("Expected rows");
            assert_eq!(users_rows.len(), 1);

            let orders_result = db.execute("SELECT COUNT(*) FROM orders").unwrap();
            let orders_rows = orders_result.into_rows().expect("Expected rows");
            assert_eq!(orders_rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_after_multiple_sessions() {
        let (_dir, path) = temp_db_path();

        // Session 1: Create table and insert initial data
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE counter (id BIGINT, value INT)")
                .expect("Failed to create table");

            db.execute("INSERT INTO counter VALUES (1, 100)").unwrap();

            db.flush().expect("Failed to flush");
        }

        // Session 2: Update data
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            db.execute("UPDATE counter SET value = 200 WHERE id = 1")
                .unwrap();
            db.execute("INSERT INTO counter VALUES (2, 300)").unwrap();

            db.flush().expect("Failed to flush");
        }

        // Session 3: More updates
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            db.execute("UPDATE counter SET value = 250 WHERE id = 1")
                .unwrap();
            db.execute("DELETE FROM counter WHERE id = 2").unwrap();

            db.flush().expect("Failed to flush");
        }

        // Session 4: Verify final state
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM counter").unwrap();
            let rows = result.into_rows().expect("Expected rows");
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_large_transaction() {
        let (_dir, path) = temp_db_path();

        // Create database with large batch insert
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE large_table (id BIGINT, data TEXT)")
                .expect("Failed to create table");

            // Insert many rows to test recovery with larger WAL
            for i in 0..200 {
                db.execute(&format!(
                    "INSERT INTO large_table VALUES ({}, 'data_item_{}')",
                    i, i
                ))
                .unwrap();
            }

            db.flush().expect("Failed to flush");
        }

        // Reopen and verify
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM large_table").unwrap();
            let rows = result.into_rows().expect("Expected rows");
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_batch_operations() {
        let (_dir, path) = temp_db_path();

        // Use batch execution
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute_batch(&[
                "CREATE TABLE batch_test (id BIGINT, name TEXT)",
                "INSERT INTO batch_test VALUES (1, 'first')",
                "INSERT INTO batch_test VALUES (2, 'second')",
                "INSERT INTO batch_test VALUES (3, 'third')",
                "UPDATE batch_test SET name = 'updated_first' WHERE id = 1",
                "DELETE FROM batch_test WHERE id = 2",
            ])
            .expect("Batch execution failed");

            db.flush().expect("Failed to flush");
        }

        // Verify batch operations persisted
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM batch_test").unwrap();
            let rows = result.into_rows().expect("Expected rows");
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_empty_wal() {
        let (_dir, path) = temp_db_path();

        // Create database without any DML operations
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            // Only DDL, no data
            db.execute("CREATE TABLE empty_table (id BIGINT)")
                .expect("Failed to create table");

            db.flush().expect("Failed to flush");
        }

        // Recovery with minimal WAL should succeed
        {
            let db = Database::open(&path, DBConfig::default())
                .expect("Failed to open database with empty WAL");

            // Table should exist
            let result = db.execute("SELECT COUNT(*) FROM empty_table").unwrap();
            let rows = result.into_rows().expect("Expected rows");
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn test_recovery_maintains_transaction_isolation() {
        let (_dir, path) = temp_db_path();

        // Create database with committed transactions
        {
            let db =
                Database::create(&path, DBConfig::default()).expect("Failed to create database");

            db.execute("CREATE TABLE isolation_test (id BIGINT, value INT)")
                .expect("Failed to create table");

            // Transaction 1
            db.execute("INSERT INTO isolation_test VALUES (1, 100)")
                .unwrap();

            // Transaction 2
            db.execute("INSERT INTO isolation_test VALUES (2, 200)")
                .unwrap();

            // Transaction 3: update
            db.execute("UPDATE isolation_test SET value = 150 WHERE id = 1")
                .unwrap();

            db.flush().expect("Failed to flush");
        }

        // Verify all committed transactions recovered
        {
            let db = Database::open(&path, DBConfig::default()).expect("Failed to open database");

            let result = db.execute("SELECT COUNT(*) FROM isolation_test").unwrap();
            let rows = result.into_rows().expect("Expected rows");
            assert_eq!(rows.len(), 1);
        }
    }
}
