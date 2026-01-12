use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
};

use crate::{
    SerializationError, TypeSystemError,
    io::pager::BtreeBuilder,
    multithreading::coordinator::{Snapshot, TransactionError},
    runtime::{
        builder::{BoxedExecutor, ExecutorBuilder},
        context::{ThreadContext, TransactionLogger},
        ddl::{DdlExecutor, DdlResult},
        eval::EvaluationError,
        validator::ValidationError,
    },
    schema::{
        DatabaseItem, Schema,
        base::SchemaError,
        catalog::{CatalogError, SharedCatalog},
    },
    sql::{
        binder::{
            binder::{Binder, BinderError},
            bounds::BoundStatement,
        },
        parser::{Parser, ParserError, ast::Statement},
        planner::{CascadesOptimizer, PhysicalPlan, PlannerError},
    },
    storage::tuple::{Row, TupleError},
    tree::bplustree::BtreeError,
};

pub mod builder;
pub mod context;
pub mod ddl;
pub mod dml;
pub mod eval;
mod ops;
pub mod validator;

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
    ValidationError(ValidationError),
    CannotUpdateKeyColumn,

    /// Cursor not initialized.
    CursorUninitialized,
    /// Schema error
    Schema(SchemaError),

    /// Object already exists
    AlreadyExists(DatabaseItem),
    /// Object not found
    NotFound(DatabaseItem),
    Serialization(SerializationError),

    /// An invalid statement was passed to an executor.
    InvalidStatement,

    Other(String),
}

impl From<TransactionError> for QueryError {
    fn from(value: TransactionError) -> Self {
        Self::Runtime(RuntimeError::TransactionalError(value))
    }
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
            Self::InvalidStatement => write!(f, "invalid statement received"),
            Self::ColumnNotFound(idx) => write!(f, "column not found in schema {idx}"),
            Self::CannotUpdateKeyColumn => f.write_str("attempted to update a key column"),
            Self::AlreadyExists(ae) => write!(f, "{} already exists", ae),
            Self::NotFound(nf) => write!(f, "{} not found", nf),
            Self::Schema(e) => write!(f, "schema error: {}", e),
            Self::CursorUninitialized => f.write_str("uninitialized btree cursor"),
            Self::ValidationError(err) => write!(f, "constraint validation error {err}"),
        }
    }
}

impl From<SerializationError> for RuntimeError {
    fn from(value: SerializationError) -> Self {
        Self::Serialization(value)
    }
}

impl From<ValidationError> for RuntimeError {
    fn from(value: ValidationError) -> Self {
        Self::ValidationError(value)
    }
}

impl From<SchemaError> for RuntimeError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
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

pub(crate) trait Executor {
    fn open(&mut self) -> RuntimeResult<()>;

    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> RuntimeResult<Option<Row>>;

    /// Clean up resources
    fn close(&mut self) -> RuntimeResult<()>;
}

impl Executor for Box<dyn Executor> {
    fn open(&mut self) -> RuntimeResult<()> {
        (**self).open()
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        (**self).next()
    }

    fn close(&mut self) -> RuntimeResult<()> {
        (**self).close()
    }
}

/// Execution statistics for profiling
#[derive(Debug, Default, Clone)]
pub(crate) struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}

/// Result of query execution.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// DDL operation completed
    Ddl(DdlResult),
    /// Query returned rows
    Rows(RowsResult),
    /// DML operation affected N rows
    RowsAffected(u64),
}

#[derive(Debug, Clone)]
pub struct RowsResult {
    data: Vec<Row>,
    out_schema: Schema,
}

impl RowsResult {
    pub fn new(rows: Vec<Row>, schema: Schema) -> Self {
        Self {
            data: rows,
            out_schema: schema,
        }
    }
    pub fn first(&self) -> Option<&Row> {
        self.data.first()
    }

    pub fn last(&self) -> Option<&Row> {
        self.data.last()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn num_columns(&self) -> usize {
        self.out_schema.num_columns()
    }

    pub fn column(&self, index: usize) -> Option<&str> {
        self.out_schema.column(index).map(|c| c.name.as_str())
    }

    pub fn iterrows(&self) -> std::slice::Iter<'_, Row> {
        self.data.iter()
    }
}

impl QueryResult {
    /// Returns the rows if this is a Rows result.
    pub fn into_rows(self) -> Option<RowsResult> {
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
    pub fn into_ddl(self) -> Option<DdlResult> {
        match self {
            Self::Ddl(outcome) => Some(outcome),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum QueryPreparationError {
    Parse(ParserError),
    Binder(BinderError),
}

impl Display for QueryPreparationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Parse(e) => write!(f, "parse error: {}", e),
            Self::Binder(e) => write!(f, "binder error: {}", e),
        }
    }
}

impl std::error::Error for QueryPreparationError {}
pub type QueryPreparationResult<T> = Result<T, QueryPreparationError>;

/// Errors during query execution.
#[derive(Debug)]
pub enum QueryError {
    Prep(QueryPreparationError),
    Planner(PlannerError),
    Runtime(RuntimeError),
    InvalidStatementType,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Prep(e) => write!(f, "preparation error {e}"),
            Self::Planner(e) => write!(f, "planner error: {}", e),
            Self::Runtime(e) => write!(f, "runtime error: {}", e),
            Self::InvalidStatementType => f.write_str("invalid statement type"),
        }
    }
}

impl Error for QueryError {}

impl From<ParserError> for QueryPreparationError {
    fn from(e: ParserError) -> Self {
        Self::Parse(e)
    }
}

impl From<BinderError> for QueryPreparationError {
    fn from(e: BinderError) -> Self {
        Self::Binder(e)
    }
}

impl From<QueryPreparationError> for QueryError {
    fn from(value: QueryPreparationError) -> Self {
        Self::Prep(value)
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

pub type QueryRunnerResult<T> = Result<T, QueryError>;

fn is_query(stmt: &BoundStatement) -> bool {
    matches!(stmt, BoundStatement::Select(_))
}

/// Determines if a bound statement requires write access.
pub(crate) fn is_dml(stmt: &BoundStatement) -> bool {
    matches!(
        stmt,
        BoundStatement::Insert(_) | BoundStatement::Update(_) | BoundStatement::Delete(_)
    )
}

/// Determines if a bound statement is a DDL statement.
pub(crate) fn is_ddl(stmt: &BoundStatement) -> bool {
    matches!(
        stmt,
        BoundStatement::CreateTable(_)
            | BoundStatement::CreateIndex(_)
            | BoundStatement::AlterTable(_)
            | BoundStatement::DropTable(_)
            | BoundStatement::Transaction(_)
    )
}

pub(crate) fn parse(sql: &str) -> QueryPreparationResult<Statement> {
    let mut parser = Parser::new(sql);
    Ok(parser.parse()?)
}

pub(crate) fn bind(
    stmt: &Statement,
    catalog: &SharedCatalog,
    tree_builder: &BtreeBuilder,
    snapshot: &Snapshot,
) -> QueryPreparationResult<BoundStatement> {
    let mut binder = Binder::new(catalog.clone(), tree_builder.clone(), snapshot);
    Ok(binder.bind(stmt)?)
}

pub(crate) fn optimize(
    stmt: &BoundStatement,
    catalog: &SharedCatalog,
    tree_builder: &BtreeBuilder,
    snapshot: &Snapshot,
) -> Result<PhysicalPlan, QueryError> {
    let mut optimizer =
        CascadesOptimizer::with_defaults(catalog.clone(), tree_builder.clone(), &snapshot);
    Ok(optimizer.optimize(stmt)?)
}

pub(crate) fn collect_rows(executor: &mut BoxedExecutor) -> RuntimeResult<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(row) = executor.next()? {
        rows.push(row);
    }
    Ok(rows)
}

pub(crate) fn extract_affected_count(rows: &[Row]) -> u64 {
    rows.first()
        .and_then(|r| r.iter().next())
        .and_then(|v| v.as_big_u_int())
        .map(|v| v.value())
        .unwrap_or(0)
}

pub(crate) struct QueryRunner {
    ctx: ThreadContext,
    logger: TransactionLogger,
}

impl QueryRunner {
    pub fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self { ctx, logger }
    }

    /// Prepare an sql statement
    pub fn prepare(&self, sql: &str) -> QueryRunnerResult<BoundStatement> {
        let ast = parse(sql)?;
        let builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let bound = bind(&ast, &self.ctx.catalog(), &builder, &snapshot)?;
        Ok(bound)
    }

    pub fn is_ddl(stmt: &BoundStatement) -> bool {
        is_ddl(stmt)
    }

    pub fn is_dml(stmt: &BoundStatement) -> bool {
        is_dml(stmt)
    }

    fn snapshot(&self) -> &Snapshot {
        self.ctx.snapshot()
    }

    pub(crate) fn prepare_and_run(&self, sql: &str) -> QueryRunnerResult<QueryResult> {
        let bound = self.prepare(sql)?;
        self.execute_statement(bound)
    }

    fn execute_statement(&self, stmt: BoundStatement) -> QueryRunnerResult<QueryResult> {
        if is_ddl(&stmt) {
            let mut executor = DdlExecutor::new(self.ctx.clone(), self.logger.clone());
            let outcome = executor.execute(&stmt)?;
            return Ok(QueryResult::Ddl(outcome));
        }
        let is_dml = is_dml(&stmt);
        let plan = optimize(
            &stmt,
            &self.ctx.catalog(),
            &self.ctx.tree_builder(),
            self.ctx.snapshot(),
        )?;

        let builder = ExecutorBuilder::new(self.ctx.clone(), self.logger.clone());
        let mut executor = builder.build(&plan)?;
        let rows = collect_rows(&mut executor)?;

        if is_dml {
            let affected = extract_affected_count(&rows);

            Ok(QueryResult::RowsAffected(affected))
        } else {
            Ok(QueryResult::Rows(RowsResult::new(
                rows,
                plan.output_schema().clone(),
            )))
        }
    }
}

/// Executes multiple statements in a single transaction with atomic commit/rollback.
pub(crate) struct MultiQueryRunner {
    ctx: ThreadContext,
    logger: TransactionLogger,
}

impl MultiQueryRunner {
    pub fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self { ctx, logger }
    }

    /// Execute all statements atomically. If any fails, all are rolled back.
    pub fn execute_all(self, statements: &[String]) -> QueryRunnerResult<Vec<QueryResult>> {
        let mut results = Vec::with_capacity(statements.len());

        // Execute all statements sequentially, collecting results or failing fast

        for sql in statements {
            let runner = QueryRunner::new(self.ctx.clone(), self.logger.clone());
            results.push(runner.prepare_and_run(sql)?);
        }

        Ok(results)
    }
}
