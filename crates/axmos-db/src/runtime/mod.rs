use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
};

use crate::{
    SerializationError, TypeSystemError,
    io::pager::{BtreeBuilder, SharedPager},
    multithreading::coordinator::{Snapshot, TransactionError, TransactionHandle},
    runtime::{
        builder::{BoxedExecutor, MutableExecutorBuilder, ReadOnlyExecutorBuilder},
        context::TransactionContext,
        ddl::{DdlError, DdlExecutor, DdlOutcome},
        eval::EvaluationError,
    },
    schema::catalog::{CatalogError, SharedCatalog},
    sql::{
        binder::{
            binder::{Binder, BinderError},
            bounds::BoundStatement,
        },
        parser::{Parser, ParserError, ast::Statement},
        planner::{CascadesOptimizer, PhysicalPlan, PlannerError},
    },
    storage::tuple::{Row, TupleError},
    tree::{
        accessor::{BtreeReadAccessor, BtreeWriteAccessor},
        bplustree::BtreeError,
    },
};

mod builder;
pub mod context;
pub mod ddl;
pub mod dml;
mod eval;
mod ops;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum RuntimeError {
    InvalidExpression(EvaluationError),
    Io(IoError),
    ColumnNotFound(usize),
    TransactionalError(TransactionError),
    Catalog(CatalogError),
    Btree(BtreeError),
    TypeError(TypeSystemError),
    TupleError(TupleError),
    CannotUpdateKeyColumn,
    InvalidState,
    Serialization(SerializationError),
    Other(String),
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Other(str) => write!(f, "internal error: {str}"),
            Self::InvalidExpression(err) => write!(f, "error while evaluating expression: {err}"),
            Self::Io(err) => write!(f, "io error {err}"),
            Self::Catalog(err) => write!(f, "catalog error {err}"),
            Self::TransactionalError(err) => write!(f, "transaction management error {err}"),
            Self::Btree(err) => write!(f, "btree error: {err}"),
            Self::TypeError(err) => write!(f, "type error {err}"),
            Self::TupleError(err) => write!(f, "tuple error {err}"),
            Self::Serialization(err) => write!(f, "serialization error {err}"),
            Self::InvalidState => write!(f, "invalid execution state"),
            Self::ColumnNotFound(idx) => write!(f, "column not found in schema {idx}"),
            Self::CannotUpdateKeyColumn => f.write_str("attempted to update a key column"),
        }
    }
}

impl From<SerializationError> for RuntimeError {
    fn from(value: SerializationError) -> Self {
        Self::Serialization(value)
    }
}

impl From<BtreeError> for RuntimeError {
    fn from(value: BtreeError) -> Self {
        Self::Btree(value)
    }
}

impl From<IoError> for RuntimeError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<CatalogError> for RuntimeError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}
impl From<TransactionError> for RuntimeError {
    fn from(value: TransactionError) -> Self {
        Self::TransactionalError(value)
    }
}

impl From<TypeSystemError> for RuntimeError {
    fn from(value: TypeSystemError) -> Self {
        Self::TypeError(value)
    }
}

impl From<EvaluationError> for RuntimeError {
    fn from(value: EvaluationError) -> Self {
        Self::InvalidExpression(value)
    }
}

impl From<TupleError> for RuntimeError {
    fn from(value: TupleError) -> Self {
        Self::TupleError(value)
    }
}

impl Error for RuntimeError {}
pub type RuntimeResult<T> = Result<T, RuntimeError>;

pub(crate) trait ClosedExecutor {
    type Running: RunningExecutor;
    /// Initialize the operator and its children
    fn open(self) -> RuntimeResult<Self::Running>;
}

pub(crate) trait RunningExecutor {
    type Closed: ClosedExecutor;
    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> RuntimeResult<Option<Row>>;

    /// Clean up resources
    fn close(self) -> RuntimeResult<Self::Closed>;
}

/// Execution statistics for profiling
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}

/// Result of query execution.
#[derive(Debug)]
pub enum QueryResult {
    /// DDL operation completed
    Ddl(DdlOutcome),
    /// Query returned rows
    Rows(Vec<Row>),
    /// DML operation affected N rows
    RowsAffected(u64),
}

impl QueryResult {
    /// Returns the rows if this is a Rows result.
    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            Self::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    /// Returns the affected count if this is a RowsAffected result.
    pub fn rows_affected(&self) -> Option<u64> {
        match self {
            Self::RowsAffected(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the DDL outcome if this is a Ddl result.
    pub fn into_ddl(self) -> Option<DdlOutcome> {
        match self {
            Self::Ddl(outcome) => Some(outcome),
            _ => None,
        }
    }
}

/// Errors during query execution.
#[derive(Debug)]
pub enum QueryError {
    Parse(ParserError),
    Binder(BinderError),
    Ddl(DdlError),
    Planner(PlannerError),
    Runtime(RuntimeError),
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Parse(e) => write!(f, "parse error: {}", e),
            Self::Binder(e) => write!(f, "binder error: {}", e),
            Self::Ddl(e) => write!(f, "DDL error: {}", e),
            Self::Planner(e) => write!(f, "planner error: {}", e),
            Self::Runtime(e) => write!(f, "runtime error: {}", e),
        }
    }
}

impl Error for QueryError {}

impl From<ParserError> for QueryError {
    fn from(e: ParserError) -> Self {
        Self::Parse(e)
    }
}

impl From<BinderError> for QueryError {
    fn from(e: BinderError) -> Self {
        Self::Binder(e)
    }
}

impl From<DdlError> for QueryError {
    fn from(e: DdlError) -> Self {
        Self::Ddl(e)
    }
}

impl From<PlannerError> for QueryError {
    fn from(e: PlannerError) -> Self {
        Self::Planner(e)
    }
}

impl From<RuntimeError> for QueryError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}

pub type QueryRunnerResult = Result<QueryResult, QueryError>;

/// Determines if a bound statement requires write access.
fn is_dml(stmt: &BoundStatement) -> bool {
    matches!(
        stmt,
        BoundStatement::Insert(_) | BoundStatement::Update(_) | BoundStatement::Delete(_)
    )
}

/// Determines if a bound statement is a DDL statement.
fn is_ddl(stmt: &BoundStatement) -> bool {
    matches!(
        stmt,
        BoundStatement::CreateTable(_)
            | BoundStatement::CreateIndex(_)
            | BoundStatement::AlterTable(_)
            | BoundStatement::DropTable(_)
            | BoundStatement::Transaction(_)
    )
}

pub struct QueryRunner {
    catalog: SharedCatalog,
    pager: SharedPager,
    tree_builder: BtreeBuilder,
    handle: TransactionHandle,
}

impl QueryRunner {
    /// Creates a new query runner.
    pub fn new(
        catalog: SharedCatalog,
        pager: SharedPager,
        tree_builder: BtreeBuilder,
        handle: TransactionHandle,
    ) -> Self {
        Self {
            catalog,
            pager,
            tree_builder,
            handle,
        }
    }

    /// Returns the snapshot for this transaction.
    pub fn snapshot(&self) -> Snapshot {
        self.handle.snapshot()
    }

    /// Executes a SQL query string.
    pub fn execute(&mut self, sql: &str) -> QueryRunnerResult {
        // Phase 1: Parse
        let stmt = self.parse(sql)?;

        // Phase 2: Bind
        let bound = self.bind(&stmt)?;

        // Phase 3: Route to appropriate executor
        if is_ddl(&bound) {
            self.execute_ddl(&bound)
        } else if is_dml(&bound) {
            self.execute_dml(&bound)
        } else {
            self.execute_query(&bound)
        }
    }

    /// Parses SQL into an AST.
    fn parse(&self, sql: &str) -> Result<Statement, QueryError> {
        let mut parser = Parser::new(sql);
        Ok(parser.parse()?)
    }

    /// Binds an AST to produce a bound statement.
    fn bind(&self, stmt: &Statement) -> Result<BoundStatement, QueryError> {
        let mut binder = Binder::new(
            self.catalog.clone(),
            self.tree_builder.clone(),
            self.snapshot(),
        );
        Ok(binder.bind(stmt)?)
    }

    /// Executes a DDL statement directly against the catalog.
    fn execute_ddl(&mut self, stmt: &BoundStatement) -> QueryRunnerResult {
        let snapshot = self.snapshot();
        let mut executor = DdlExecutor::new(
            &mut self.catalog,
            &self.tree_builder,
            &snapshot,
            self.pager.clone(),
        );

        let outcome = executor.execute(stmt)?;
        Ok(QueryResult::Ddl(outcome))
    }

    /// Executes a read-only query (SELECT, WITH).
    fn execute_query(&self, stmt: &BoundStatement) -> QueryRunnerResult {
        // Create read-only context
        let ctx = TransactionContext::new(
            BtreeReadAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        // Optimize
        let plan = self.optimize(stmt)?;

        // Build and execute with read-only builder
        let builder = ReadOnlyExecutorBuilder::new(ctx);
        let mut executor = builder.build(&plan)?;

        // Collect results
        let rows = self.collect_rows(&mut executor)?;
        Ok(QueryResult::Rows(rows))
    }

    /// Executes a DML statement (INSERT, UPDATE, DELETE).
    fn execute_dml(&self, stmt: &BoundStatement) -> QueryRunnerResult {
        // Create write context
        let ctx = TransactionContext::new(
            BtreeWriteAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        // Optimize
        let plan = self.optimize(stmt)?;

        // Build and execute with mutable builder
        let builder = MutableExecutorBuilder::new(ctx);
        let mut executor = builder.build(&plan)?;

        // Collect results and extract affected count
        let rows = self.collect_rows(&mut executor)?;
        let affected = extract_affected_count(&rows);
        Ok(QueryResult::RowsAffected(affected))
    }

    /// Optimizes a bound statement into a physical plan.
    fn optimize(&self, stmt: &BoundStatement) -> Result<PhysicalPlan, QueryError> {
        let mut optimizer = CascadesOptimizer::with_defaults(
            self.catalog.clone(),
            self.tree_builder.clone(),
            self.snapshot(),
        );
        Ok(optimizer.optimize(stmt)?)
    }

    /// Collects all rows from an executor.
    fn collect_rows(&self, executor: &mut BoxedExecutor) -> RuntimeResult<Vec<Row>> {
        let mut rows = Vec::new();
        while let Some(row) = executor.next()? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Commits the transaction.
    pub fn commit(&self) -> Result<(), RuntimeError> {
        self.handle
            .commit()
            .map_err(|e| RuntimeError::Other(e.to_string()))
    }

    /// Aborts the transaction.
    pub fn abort(&self) -> Result<(), RuntimeError> {
        self.handle
            .abort()
            .map_err(|e| RuntimeError::Other(e.to_string()))
    }

    /// Returns a reference to the transaction handle.
    pub fn handle(&self) -> &TransactionHandle {
        &self.handle
    }
}

fn extract_affected_count(rows: &[Row]) -> u64 {
    rows.first()
        .and_then(|r| r.iter().next())
        .and_then(|v| v.as_big_u_int())
        .map(|v| v.value())
        .unwrap_or(0)
}

/// Builder for QueryRunner to simplify construction.
pub struct QueryRunnerBuilder {
    catalog: Option<SharedCatalog>,
    pager: Option<SharedPager>,
    tree_builder: Option<BtreeBuilder>,
    handle: Option<TransactionHandle>,
}

impl Default for QueryRunnerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryRunnerBuilder {
    pub fn new() -> Self {
        Self {
            catalog: None,
            pager: None,
            tree_builder: None,
            handle: None,
        }
    }

    pub fn catalog(mut self, catalog: SharedCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub fn pager(mut self, pager: SharedPager) -> Self {
        self.pager = Some(pager);
        self
    }

    pub fn tree_builder(mut self, builder: BtreeBuilder) -> Self {
        self.tree_builder = Some(builder);
        self
    }

    pub fn handle(mut self, handle: TransactionHandle) -> Self {
        self.handle = Some(handle);
        self
    }

    pub fn build(self) -> Result<QueryRunner, &'static str> {
        Ok(QueryRunner {
            catalog: self.catalog.ok_or("catalog is required")?,
            pager: self.pager.ok_or("pager is required")?,
            tree_builder: self.tree_builder.ok_or("tree_builder is required")?,
            handle: self.handle.ok_or("handle is required")?,
        })
    }
}
