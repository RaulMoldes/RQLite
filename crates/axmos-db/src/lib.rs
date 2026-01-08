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

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::{
    DBConfig,
    io::{
        disk::FileOperations,
        logger::Begin,
        pager::{Pager, SharedPager, WalRecuperator},
    },
    multithreading::{
        coordinator::{TransactionCoordinator, TransactionError},
        runner::{BoxError, SharedTaskRunner, TaskError},
    },
    runtime::{
        MultiQueryRunner, QueryError, QueryResult, QueryRunner, RuntimeError,
        context::{TransactionContext, TransactionLogger},
    },
    schema::catalog::{Catalog, CatalogTrait, SharedCatalog, VacuumStats},
    storage::page::BtreePage,
    tcp::session::Session,
};

use std::{
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
    /// Runtime errror
    Runtime(RuntimeError),
    /// Transaction error
    TransactionManagement(TransactionError),
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
            Self::TransactionManagement(err) => write!(f, "transaction management error {err}"),
            Self::Runtime(err) => write!(f, "runtime error {err}"),
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

impl From<TransactionError> for DatabaseError {
    fn from(e: TransactionError) -> Self {
        Self::TransactionManagement(e)
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

impl From<RuntimeError> for DatabaseError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
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
    task_runner: SharedTaskRunner,
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
        let task_runner =
            SharedTaskRunner::new(config.pool_size, pager.clone(), coordinator.clone());

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
        let task_runner =
            SharedTaskRunner::new(config.pool_size, pager.clone(), coordinator.clone());

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

    pub(crate) fn begin_transaction(
        coordinator: TransactionCoordinator,
        pager: SharedPager,
        catalog: SharedCatalog,
    ) -> DatabaseResult<(TransactionContext, TransactionLogger)> {
        let handle = coordinator.begin()?;
        let tid = handle.id();
        println!("Se crea transaccion con id {}", tid);
        let first_lsn = pager.write().push_to_log(Begin, tid, None)?;
        let ctx = TransactionContext::new(pager.clone(), catalog.clone(), handle)?;
        let logger = TransactionLogger::new(tid, pager.clone(), first_lsn);
        Ok((ctx, logger))
    }

    /// Creates a new session for this database.
    ///
    /// Each client/connection should have its own session.
    /// Sessions can have independent transactions with BEGIN/COMMIT/ROLLBACK.
    pub fn session(&self) -> DatabaseResult<Session> {
        let (ctx, logger) = Self::begin_transaction(
            self.coordinator.clone(),
            self.pager.clone(),
            self.catalog.clone(),
        )?;

        Ok(Session::new(ctx, logger, self.task_runner.clone()))
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
        let (tx_ctx, logger) = Self::begin_transaction(
            self.coordinator.clone(),
            self.pager.clone(),
            self.catalog.clone(),
        )?;
        let child = tx_ctx.create_child()?;
        let pager = self.pager.clone();
        // Begin a recovery transaction
        self.task_runner.run(move |ctx| {
            let mut recuperator = WalRecuperator::new(child, logger.clone());

            // Run analysis INSIDE the closure using the cloned pager
            let analysis = pager.write().run_analysis().map_err(box_err)?;

            // Run recovery through recuperator
            recuperator.run_recovery(&analysis).map_err(box_err)?;

            // Truncate WAL
            pager.write().truncate_wal().map_err(box_err)?;

            // Commit recovery transaction
            tx_ctx.commit_transaction().map_err(box_err)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Executes a SQL query and returns the result.
    pub fn execute(&self, sql: &str) -> DatabaseResult<QueryResult> {
        let sql = sql.to_string();
        let (tx_ctx, logger) = Self::begin_transaction(
            self.coordinator.clone(),
            self.pager.clone(),
            self.catalog.clone(),
        )?;

        let child = tx_ctx.create_child()?;
        let result = self.task_runner.run_with_result(move |_ctx| {
            let runner = QueryRunner::new(child, logger.clone());
            let result_guard = runner.prepare_and_run(&sql).map_err(box_err)?;

            logger.log_commit().map_err(box_err)?;
            tx_ctx.commit_transaction().map_err(box_err)?;
            logger.log_end().map_err(box_err)?;

            Ok(result_guard)
        })?;

        Ok(result)
    }

    /// Executes multiple SQL statements in a single transaction.
    pub fn execute_batch(&self, statements: &[&str]) -> DatabaseResult<Vec<QueryResult>> {
        let statements: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
        // Begin transaction
        let (tx_ctx, logger) = Self::begin_transaction(
            self.coordinator.clone(),
            self.pager.clone(),
            self.catalog.clone(),
        )?;

        let child = tx_ctx.create_child()?;
        let results = self.task_runner.run_with_result(move |_ctx| {
            // Use BatchQueryRunner for atomic execution
            let runner = MultiQueryRunner::new(child, logger.clone());

            let results = runner.execute_all(&statements).map_err(box_err)?;

            logger.log_commit().map_err(box_err)?;
            tx_ctx.commit_transaction().map_err(box_err)?;
            logger.log_end().map_err(box_err)?;

            Ok(results)
        })?;

        Ok(results)
    }

    /// Flushes all pending writes to disk.
    pub fn flush(&self) -> DatabaseResult<()> {
        self.update_header()?;
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
            let (tx_ctx, logger) =
                Self::begin_transaction(coordinator.clone(), pager.clone(), catalog.clone())
                    .map_err(box_err)?;

            let tree_builder = tx_ctx.tree_builder();
            let snapshot = tx_ctx.snapshot();

            catalog
                .write()
                .analyze(&tree_builder, &snapshot, sample_rate, max_sample_rows)
                .map_err(box_err)?;

            tx_ctx.commit_transaction().map_err(box_err)?;
            Ok(())
        })?;

        Ok(())
    }

    /// Runs VACUUM to clean up old MVCC versions and transaction metadata.
    ///
    /// This operation:
    /// 1. Aborts all active transactions (ensures no concurrent modifications)
    /// 2. Determines the oldest transaction ID still needed
    /// 3. Iterates through all tuples and removes old delta versions
    /// 4. Cleans up transaction metadata from the coordinator
    pub fn vacuum(&self) -> DatabaseResult<VacuumStats> {
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        let stats = self.task_runner.run_with_result(move |_ctx| {
            // Abort all active transactions
            let aborted_txs = coordinator.abort_all();
            let oldest_active_xid = coordinator.get_last_committed();
            if !aborted_txs.is_empty() {
                println!("VACUUM: Aborted {} active transactions", aborted_txs.len());
            }

            // Create a vacuum transaction
            let (tx_ctx, logger) =
                Self::begin_transaction(coordinator.clone(), pager.clone(), catalog.clone())
                    .map_err(box_err)?;

            let tree_builder = tx_ctx.tree_builder();
            let vacuum_snapshot = tx_ctx.snapshot();

            let mut stats = catalog
                .write()
                .vacuum(&tree_builder, &vacuum_snapshot, oldest_active_xid)
                .map_err(box_err)?;

            // Clean up transaction coordinator
            stats.transactions_cleaned = coordinator.vacuum_transactions();

            // Commit vacuum transaction
            tx_ctx.commit_transaction().map_err(box_err)?;

            // Flush everything to disk
            pager.write().flush().map_err(box_err)?;

            Ok(stats)
        })?;

        Ok(stats)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best-effort flush on drop
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests;
