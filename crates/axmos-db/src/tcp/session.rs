//! Database session with transaction state management.

use crate::{
    box_err,
    multithreading::runner::SharedTaskRunner,
    runtime::{
        QueryError, QueryResult, QueryRunner, QueryRunnerResult, RuntimeError,
        context::{TransactionContext, TransactionLogger},
    },
};
pub struct Session {
    ctx: TransactionContext,
    logger: TransactionLogger,
    task_runner: SharedTaskRunner,
}

impl Session {
    pub(crate) fn new(
        ctx: TransactionContext,
        logger: TransactionLogger,
        task_runner: SharedTaskRunner,
    ) -> Self {
        Self {
            ctx,
            logger,
            task_runner,
        }
    }

    pub fn commit_transaction(&mut self) -> QueryRunnerResult<()> {
        self.logger.log_commit()?;
        self.ctx.commit_transaction()?;
        self.logger.log_end()?;
        Ok(())
    }

    pub fn abort_transaction(&mut self) -> QueryRunnerResult<()> {
        self.logger.log_abort()?;
        self.ctx.abort_transaction()?;
        self.logger.log_end()?;
        Ok(())
    }

    /// Execute a SQL statement
    pub fn execute(&mut self, sql: &str) -> QueryRunnerResult<QueryResult> {
        self.execute_async(sql.to_string())
    }

    fn execute_async(&self, sql: String) -> QueryRunnerResult<QueryResult> {
        let logger = self.logger.clone();
        // Build a temporary context for this thread
        // Cloning the handle creates an invalidated copy of itself that can fo everything but committing  or aborting.
        let child_ctx = self.ctx.create_child()?;

        self.task_runner
            .run_with_result(move |_| {
                let runner = QueryRunner::new(child_ctx.clone(), logger.clone());
                let result = runner.prepare_and_run(&sql, false).map_err(box_err)?;

                Ok(result.into_result())
            })
            .map_err(|e| QueryError::Runtime(RuntimeError::Other(e.to_string())))
    }
}

unsafe impl Send for Session {}
unsafe impl Sync for Session {}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.abort_transaction();
    }
}
