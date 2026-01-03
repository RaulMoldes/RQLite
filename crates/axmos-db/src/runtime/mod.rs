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
    schema::{
        Schema,
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
pub(crate) struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}

/// Result of query execution.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// DDL operation completed
    Ddl(DdlOutcome),
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
    pub fn into_ddl(self) -> Option<DdlOutcome> {
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
    Ddl(DdlError),
    Planner(PlannerError),
    Runtime(RuntimeError),
    InvalidStatementType,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Prep(e) => write!(f, "preparation error {e}"),
            Self::Ddl(e) => write!(f, "DDL error: {}", e),
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

pub type QueryRunnerResult<T> = Result<T, QueryError>;

fn is_query(stmt: &BoundStatement) -> bool {
    matches!(stmt, BoundStatement::Select(_))
}

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

fn parse(sql: &str) -> QueryPreparationResult<Statement> {
    let mut parser = Parser::new(sql);
    Ok(parser.parse()?)
}

fn bind(
    stmt: &Statement,
    catalog: &SharedCatalog,
    tree_builder: &BtreeBuilder,
    snapshot: Snapshot,
) -> QueryPreparationResult<BoundStatement> {
    let mut binder = Binder::new(catalog.clone(), tree_builder.clone(), snapshot);
    Ok(binder.bind(stmt)?)
}

fn optimize(
    stmt: &BoundStatement,
    catalog: &SharedCatalog,
    tree_builder: &BtreeBuilder,
    snapshot: Snapshot,
) -> Result<PhysicalPlan, QueryError> {
    let mut optimizer =
        CascadesOptimizer::with_defaults(catalog.clone(), tree_builder.clone(), snapshot);
    Ok(optimizer.optimize(stmt)?)
}

fn collect_rows(executor: &mut BoxedExecutor) -> RuntimeResult<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(row) = executor.next()? {
        rows.push(row);
    }
    Ok(rows)
}

fn extract_affected_count(rows: &[Row]) -> u64 {
    rows.first()
        .and_then(|r| r.iter().next())
        .and_then(|v| v.as_big_u_int())
        .map(|v| v.value())
        .unwrap_or(0)
}

pub(crate) struct QueryRunnerBuilder {
    catalog: SharedCatalog,
    pager: SharedPager,
    tree_builder: BtreeBuilder,
    handle: TransactionHandle,
}

pub(crate) struct QueryPreparator {
    catalog: SharedCatalog,
    pager: SharedPager,
    tree_builder: BtreeBuilder,
    handle: TransactionHandle,
}

impl QueryPreparator {
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

    /// Prepare an sql statement
    pub fn prepare(&self, sql: &str) -> QueryRunnerResult<BoundStatement> {
        let ast = parse(sql)?;
        let bound = bind(
            &ast,
            &self.catalog,
            &self.tree_builder,
            self.handle.snapshot(),
        )?;
        Ok(bound)
    }

    pub fn is_ddl(stmt: &BoundStatement) -> bool {
        is_ddl(stmt)
    }

    pub fn is_dml(stmt: &BoundStatement) -> bool {
        is_dml(stmt)
    }

    fn snapshot(&self) -> Snapshot {
        self.handle.snapshot()
    }

    pub fn read(&self, autocommit: bool) -> QueryRunnerResult<StatementRunner> {
        let ctx = TransactionContext::new(
            BtreeReadAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        Ok(StatementRunner::Query(ReadQueryRunner { ctx, autocommit }))
    }

    pub fn write(&self, autocommit: bool) -> QueryRunnerResult<StatementRunner> {
        let ctx = TransactionContext::new(
            BtreeWriteAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        Ok(StatementRunner::Dml(WriteQueryRunner { ctx, autocommit }))
    }

    pub fn ddl(&self, autocommit: bool) -> QueryRunnerResult<StatementRunner> {
        let ctx = TransactionContext::new(
            BtreeWriteAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        Ok(StatementRunner::Ddl(DdlQueryRunner { ctx, autocommit }))
    }

    pub fn build(&self, sql: &str, autocommit: bool) -> QueryRunnerResult<StatementRunner> {
        let bound = self.prepare(sql)?;
        if Self::is_ddl(&bound) {
            return self.ddl(autocommit);
        } else if Self::is_dml(&bound) {
            return self.write(autocommit);
        } else {
            return self.read(autocommit);
        }
    }

    pub(crate) fn build_and_run(
        &self,
        sql: &str,
        autocommit: bool,
    ) -> QueryRunnerResult<ResultGuard> {
        let bound = self.prepare(sql)?;
        if Self::is_ddl(&bound) {
            let built = self.ddl(autocommit)?;
            built.execute(bound)
        } else if Self::is_dml(&bound) {
            let built = self.write(autocommit)?;
            built.execute(bound)
        } else {
            let built = self.read(autocommit)?;
            built.execute(bound)
        }
    }
}

pub struct ResultGuard {
    handle: CommitHandle,
    result: QueryResult,
}

impl ResultGuard {
    pub(crate) fn write(ctx: TransactionContext<BtreeWriteAccessor>, result: QueryResult) -> Self {
        Self {
            handle: CommitHandle::Write { ctx: Some(ctx) },
            result,
        }
    }

    pub(crate) fn read(ctx: TransactionContext<BtreeReadAccessor>, result: QueryResult) -> Self {
        Self {
            handle: CommitHandle::Read { ctx: Some(ctx) },
            result,
        }
    }

    pub(crate) fn take_handle(&mut self) {
        self.handle.invalidate();
    }

    pub(crate) fn commit(&mut self) -> QueryRunnerResult<()> {
        self.handle.commit()
    }

    pub(crate) fn abort(&mut self) -> QueryRunnerResult<()> {
        self.handle.abort()
    }
}

impl From<ResultGuard> for QueryResult {
    fn from(value: ResultGuard) -> Self {
        value.result
    }
}
pub(crate) enum CommitHandle {
    Write {
        ctx: Option<TransactionContext<BtreeWriteAccessor>>,
    },
    Read {
        ctx: Option<TransactionContext<BtreeReadAccessor>>,
    },
}

impl CommitHandle {
    pub(crate) fn invalidate(&mut self) {
        match self {
            Self::Read { ctx, .. } => {
                let _ = ctx.take();
            }
            Self::Write { ctx, .. } => {
                let _ = ctx.take();
            }
        }
    }

    pub fn commit(&mut self) -> QueryRunnerResult<()> {
        match self {
            Self::Read { ctx, .. } => {
                if let Some(ctx) = ctx.take() {
                    ctx.pre_commit()?;
                    ctx.handle()
                        .commit()
                        .map_err(|_| RuntimeError::InvalidState)?;
                    ctx.end()?;
                }
                Ok(())
            }
            Self::Write { ctx, .. } => {
                if let Some(ctx) = ctx.take() {
                    ctx.pre_commit()?;
                    ctx.handle()
                        .commit()
                        .map_err(|_| RuntimeError::InvalidState)?;
                    ctx.end()?;
                }
                Ok(())
            }
        }
    }

    pub fn abort(&mut self) -> QueryRunnerResult<()> {
        match self {
            Self::Read { ctx, .. } => {
                if let Some(ctx) = ctx.take() {
                    ctx.pre_abort()?;
                    ctx.handle()
                        .commit()
                        .map_err(|_| RuntimeError::InvalidState)?;
                    ctx.end()?;
                }
                Ok(())
            }
            Self::Write { ctx, .. } => {
                if let Some(ctx) = ctx.take() {
                    ctx.pre_abort()?;
                    ctx.handle()
                        .abort()
                        .map_err(|_| RuntimeError::InvalidState)?;
                    ctx.end()?;
                }
                Ok(())
            }
        }
    }
}

impl Drop for CommitHandle {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}
pub(crate) trait QueryRunner {
    /// Executes a prepared statement
    fn execute(self, stmt: BoundStatement) -> QueryRunnerResult<ResultGuard>;
}

pub(crate) struct WriteQueryRunner {
    ctx: TransactionContext<BtreeWriteAccessor>,
    autocommit: bool,
}

pub(crate) enum StatementRunner {
    Ddl(DdlQueryRunner),
    Dml(WriteQueryRunner),
    Query(ReadQueryRunner),
}

impl QueryRunner for StatementRunner {
    fn execute(self, stmt: BoundStatement) -> QueryRunnerResult<ResultGuard> {
        match self {
            Self::Ddl(d) => d.execute(stmt),
            Self::Dml(d) => d.execute(stmt),
            Self::Query(q) => q.execute(stmt),
        }
    }
}

impl StatementRunner {
    fn get_write_context(&self) -> Option<&TransactionContext<BtreeWriteAccessor>> {
        match self {
            Self::Ddl(d) => Some(&d.ctx),
            Self::Dml(d) => Some(&d.ctx),
            Self::Query(q) => None,
        }
    }

    fn get_read_context(&self) -> Option<&TransactionContext<BtreeReadAccessor>> {
        match self {
            Self::Ddl(d) => None,
            Self::Dml(d) => None,
            Self::Query(q) => Some(&q.ctx),
        }
    }
}

impl QueryRunner for WriteQueryRunner {
    fn execute(self, stmt: BoundStatement) -> QueryRunnerResult<ResultGuard> {
        let plan = optimize(
            &stmt,
            &self.ctx.catalog(),
            &self.ctx.tree_builder(),
            self.ctx.snapshot(),
        )?;

        let builder = MutableExecutorBuilder::new(self.ctx.clone());
        let mut executor = builder.build(&plan)?;

        let rows = collect_rows(&mut executor)?;
        let affected = extract_affected_count(&rows);

        if self.autocommit {
            self.ctx.pre_commit()?;
            self.ctx
                .handle()
                .commit()
                .map_err(|_| RuntimeError::InvalidState)?;
            self.ctx.end()?;
        }

        Ok(ResultGuard::write(
            self.ctx,
            QueryResult::Rows(RowsResult::new(rows, plan.output_schema().clone())),
        ))
    }
}

pub(crate) struct ReadQueryRunner {
    ctx: TransactionContext<BtreeReadAccessor>,
    autocommit: bool,
}

impl QueryRunner for ReadQueryRunner {
    fn execute(self, stmt: BoundStatement) -> QueryRunnerResult<ResultGuard> {
        let plan: PhysicalPlan = optimize(
            &stmt,
            &self.ctx.catalog(),
            &self.ctx.tree_builder(),
            self.ctx.snapshot(),
        )?;

        let builder = ReadOnlyExecutorBuilder::new(self.ctx.clone());
        let mut executor = builder.build(&plan)?;

        let rows = collect_rows(&mut executor)?;

        if self.autocommit {
            self.ctx.pre_commit()?;
            self.ctx
                .handle()
                .commit()
                .map_err(|_| RuntimeError::InvalidState)?;
            self.ctx.end()?;
        }

        Ok(ResultGuard::read(
            self.ctx,
            QueryResult::Rows(RowsResult::new(rows, plan.output_schema().clone())),
        ))
    }
}

pub(crate) struct DdlQueryRunner {
    ctx: TransactionContext<BtreeWriteAccessor>,
    autocommit: bool,
}

impl QueryRunner for DdlQueryRunner {
    fn execute(self, stmt: BoundStatement) -> QueryRunnerResult<ResultGuard> {
        let mut executor = DdlExecutor::new(self.ctx.clone());

        let outcome = executor.execute(&stmt)?;

        if self.autocommit {
            self.ctx.pre_commit()?;
            self.ctx
                .handle()
                .commit()
                .map_err(|_| RuntimeError::InvalidState)?;
            self.ctx.end()?;
        }

        Ok(ResultGuard::write(self.ctx, QueryResult::Ddl(outcome)))
    }
}

/// Executes multiple statements in a single transaction with atomic commit/rollback.
pub(crate) struct BatchQueryRunner {
    catalog: SharedCatalog,
    pager: SharedPager,
    tree_builder: BtreeBuilder,
    handle: TransactionHandle,
}

impl BatchQueryRunner {
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

    /// Execute all statements atomically. If any fails, all are rolled back.
    pub fn execute_all(self, statements: &[String]) -> QueryRunnerResult<Vec<QueryResult>> {
        // Create a single write context for the entire batch
        let ctx = TransactionContext::new(
            BtreeWriteAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            self.handle.clone(),
        )?;

        let mut results = Vec::with_capacity(statements.len());

        // Execute all statements, collecting results or failing fast
        let execution_result: QueryRunnerResult<()> = (|| {
            for sql in statements {
                let result = self.execute_single(&ctx, sql)?;
                results.push(result);
            }
            Ok(())
        })();

        // Commit or abort based on execution result
        match execution_result {
            Ok(()) => {
                ctx.pre_commit()?;
                self.handle
                    .commit()
                    .map_err(|_| RuntimeError::InvalidState)?;
                ctx.end()?;
                Ok(results)
            }
            Err(e) => {
                // Abort the transaction
                let _ = self.handle.abort();
                Err(e)
            }
        }
    }

    fn execute_single(
        &self,
        ctx: &TransactionContext<BtreeWriteAccessor>,
        sql: &str,
    ) -> QueryRunnerResult<QueryResult> {
        // Parse and bind
        let ast = parse(sql)?;
        let bound = bind(
            &ast,
            &self.catalog,
            &self.tree_builder,
            self.handle.snapshot(),
        )?;

        // Execute based on statement type
        if is_ddl(&bound) {
            let mut executor = DdlExecutor::new(ctx.clone());
            let outcome = executor.execute(&bound)?;
            Ok(QueryResult::Ddl(outcome))
        } else if is_dml(&bound) {
            let plan = optimize(
                &bound,
                &self.catalog,
                &self.tree_builder,
                self.handle.snapshot(),
            )?;
            let builder = MutableExecutorBuilder::new(ctx.clone());
            let mut executor = builder.build(&plan)?;
            let rows = collect_rows(&mut executor)?;
            Ok(QueryResult::Rows(RowsResult::new(
                rows,
                plan.output_schema().clone(),
            )))
        } else {
            // SELECT queries use read accessor but share the transaction
            let plan = optimize(
                &bound,
                &self.catalog,
                &self.tree_builder,
                self.handle.snapshot(),
            )?;
            let read_ctx = TransactionContext::new(
                BtreeReadAccessor::new(),
                self.pager.clone(),
                self.catalog.clone(),
                self.handle.clone(),
            )?;
            let builder = ReadOnlyExecutorBuilder::new(read_ctx);
            let mut executor = builder.build(&plan)?;
            let rows = collect_rows(&mut executor)?;
            Ok(QueryResult::Rows(RowsResult::new(
                rows,
                plan.output_schema().clone(),
            )))
        }
    }
}
