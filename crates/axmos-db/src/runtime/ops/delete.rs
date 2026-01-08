use crate::{
    ObjectId,
    runtime::{
        ExecutionStats, Executor, RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        dml::{DmlExecutor, extract_row_id},
    },
    sql::planner::physical::PhysDeleteOp,
    storage::tuple::Row,
    types::{DataType, UInt64},
};

/// Delete operator.
pub(crate) struct Delete<Child>
where
    Child: Executor,
{
    table_id: ObjectId,
    ctx: ThreadContext,
    logger: TransactionLogger,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

impl<Child> Delete<Child>
where
    Child: Executor,
{
    pub(crate) fn new(
        op: &PhysDeleteOp,
        ctx: ThreadContext,
        logger: TransactionLogger,
        child: Child,
        stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            table_id: op.table_id,
            ctx,
            child,
            logger,
            stats: ExecutionStats::default(),
            returned_result: false,
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child> Executor for Delete<Child>
where
    Child: Executor,
{
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let row_id = extract_row_id(&row)?;
            let mut dml = DmlExecutor::new(self.ctx.clone(), self.logger.clone());
            let result = dml.delete(self.table_id, row_id)?;

            if result.deleted {
                self.stats.rows_produced += 1;
            }
        }

        self.returned_result = true;

        // Return count of deleted rows
        Ok(Some(Row::new(Box::new([DataType::BigUInt(UInt64::from(
            self.stats.rows_produced,
        ))]))))
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
