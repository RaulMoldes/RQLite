//! Database session with transaction state management.

use crate::{
    box_err,
    io::pager::{BtreeBuilder, SharedPager},
    multithreading::{
        coordinator::{TransactionCoordinator, TransactionError, TransactionHandle},
        runner::SharedTaskRunner,
    },
    runtime::{
        QueryError, QueryResult,ContextBuilder, execute_statement,  QueryRunnerResult,  RuntimeError, bind,  context::TransactionContext, parse, ddl::DdlOutcome
    },
    schema::catalog::SharedCatalog,
    sql::{
        binder::bounds::{BoundStatement, },
        parser::ast::{Statement, TransactionStatement},
    },
    tree::accessor::{BtreeReadAccessor,  TreeReader},
};

/// A database session with its own transaction state.
///
/// Each client/connection should have its own Session instance.
/// This allows multiple concurrent transactions without blocking
#[derive(Clone)]
pub struct SessionContext {
    pager: SharedPager,
    catalog: SharedCatalog,
    coordinator: TransactionCoordinator,
    /// Active transaction handle
    active_handle: Option<TransactionHandle>,
}



impl SessionContext {
    pub(crate) fn new(
        pager: SharedPager,
        catalog: SharedCatalog,
        coordinator: TransactionCoordinator,

    ) -> Self {
        Self {
            pager,
            catalog,
            coordinator,

            active_handle: None,
        }
    }

    /// Check if session is in an explicit transaction
    pub fn in_transaction(&self) -> bool {
        self.active_handle.is_some()
    }


    fn begin(&mut self) -> QueryRunnerResult<QueryResult> {
        if self.active_handle.is_some() {
            return Err(QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionAlreadyStarted,
            )));
        }

        let handle = self.coordinator.begin()?;
        self.active_handle = Some(handle);

        Ok(QueryResult::Ddl(DdlOutcome::TransactionStarted))
    }

    fn commit(&mut self) -> QueryRunnerResult<QueryResult> {
        let handle = self.active_handle.take().ok_or_else(|| {
            QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionNotStarted,
            ))
        })?;
         let ctx = TransactionContext::new(
            BtreeReadAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            handle,
        )?;
        ctx.commit_transaction()?;
        Ok(QueryResult::Ddl(DdlOutcome::TransactionCommitted))
    }

    fn rollback(&mut self) -> QueryRunnerResult<QueryResult> {
        let handle = self.active_handle.take().ok_or_else(|| {
            QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionNotStarted,
            ))
        })?;

        let ctx = TransactionContext::new(
            BtreeReadAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            handle,
        )?;
        ctx.abort_transaction()?;

        Ok(QueryResult::Ddl(DdlOutcome::TransactionRolledBack))
    }
}

pub struct Session {
    ctx: SessionContext,
    task_runner: SharedTaskRunner,
}

impl Session {
    pub(crate) fn new(
        pager: SharedPager,
        catalog: SharedCatalog,
        coordinator: TransactionCoordinator,
        task_runner: SharedTaskRunner,
    ) -> Self {
        Self { ctx: SessionContext::new(pager, catalog, coordinator), task_runner }
    }

    /// Check if session is in an explicit transaction
    pub fn in_transaction(&self) -> bool {
        self.ctx.in_transaction()
    }

    /// Execute a SQL statement
    pub fn execute(&mut self, sql: &str) -> QueryRunnerResult<QueryResult> {
        let sql = sql.to_string();
        let pager = self.ctx.pager.clone();
        let catalog = self.ctx.catalog.clone();
        let coordinator = self.ctx.coordinator.clone();

        // Parse first to detect transaction control
        let ast = parse(&sql)?;

        if !self.in_transaction()
            && !matches!(ast, Statement::Transaction(TransactionStatement::Begin))
        {
            return Err(QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionNotStarted,
            )));
        };

        if self.in_transaction()
            && matches!(ast, Statement::Transaction(TransactionStatement::Begin))
        {
            return Err(QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionAlreadyStarted,
            )));
        };

        if let Statement::Transaction(tx_stmt) = ast {
            match tx_stmt {
                TransactionStatement::Begin => {
                    return self.ctx.begin();
                }
                TransactionStatement::Commit => {
                    return self.ctx.commit();
                }
                TransactionStatement::Rollback => {
                    return self.ctx.rollback();
                }
            }
        }

        // At this point it should be safe to unwrap the handle.
        let handle = self
            .ctx.active_handle
            .as_ref()
            .ok_or(TransactionError::TransactionNotStarted)?;

        // Bind needs a snapshot

        let min_keys = pager.read().min_keys_per_page();
        let num_siblings = pager.read().num_siblings_per_side();

        let tree_builder = BtreeBuilder::new(min_keys, num_siblings).with_pager(pager.clone());


        let bound = bind(&ast, &catalog, &tree_builder, handle.snapshot())?;
        self.execute_async(bound)

    }

    fn execute_async(
        &self,
        bound: BoundStatement,
    ) -> QueryRunnerResult<QueryResult> {

        let ctx = self.ctx.clone();
        self.task_runner
            .run_with_result(move |_| {
                let result = execute_statement(&ctx, &bound).map_err(box_err)?;
                Ok(result)
            })
            .map_err(|e| QueryError::Runtime(RuntimeError::Other(e.to_string())))
    }

}


impl ContextBuilder for SessionContext {

    fn build_ctx<Acc: TreeReader + Clone>(
        &self,
        acc: Acc,
    ) -> QueryRunnerResult<TransactionContext<Acc>> {
        if !self.in_transaction() {
            return Err(QueryError::Runtime(RuntimeError::TransactionalError(
                TransactionError::TransactionNotStarted,
            )));
        }
        let ctx = TransactionContext::new(
            acc,
            self.pager.clone(),
            self.catalog.clone(),
            self.active_handle.as_ref().unwrap().clone(),
        )?;
        Ok(ctx)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Rollback any uncommitted transaction
        if let Some(handle) = self.ctx.active_handle.take() {
            let _ = handle.abort();
        }
    }
}
