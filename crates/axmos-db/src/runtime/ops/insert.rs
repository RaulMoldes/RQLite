use crate::{
    ObjectId,
    runtime::{
        ExecutionStats, Executor, RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        dml::DmlExecutor,
    },
    sql::planner::physical::PhysInsertOp,
    storage::tuple::Row,
    types::{DataType, UInt64},
};

/// Running state for the Insert operator.
pub(crate) struct Insert<Child>
where
    Child: Executor,
{
    table_id: ObjectId,
    columns: Box<[usize]>,
    ctx: ThreadContext,
    logger: TransactionLogger,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

impl<Child> Insert<Child>
where
    Child: Executor,
{
    pub(crate) fn new(
        op: &PhysInsertOp,
        ctx: ThreadContext,
        logger: TransactionLogger,
        child: Child,
    ) -> Self {
        Self {
            table_id: op.table_id,
            columns: op.columns.clone().into_boxed_slice(),
            ctx,
            logger,
            child,
            returned_result: false,
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child> Executor for Insert<Child>
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

            let mut dml = DmlExecutor::new(self.ctx.clone(), self.logger.clone());
            dml.insert(self.table_id, &self.columns, &row)?;

            self.stats.rows_produced += 1;
        }

        self.returned_result = true;

        // Return count of inserted rows
        Ok(Some(Row::new(Box::new([DataType::BigUInt(UInt64::from(
            self.stats.rows_produced,
        ))]))))
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
