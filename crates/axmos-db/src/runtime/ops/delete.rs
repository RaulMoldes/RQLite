//! Delete operator implementation.
//!
//! Deletes rows from a table by consuming rows from its child operator
//! (which provides rows to delete) and delegating the actual deletion
//! to the DML executor.
use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult,
        context::TransactionContext,
        dml::{DmlExecutor, extract_row_id},
    },
    sql::planner::physical::PhysDeleteOp,
    storage::tuple::Row,
    tree::accessor::TreeWriter,
    types::{DataType, UInt64},
};

/// Running state for the Delete operator.
pub(crate) struct OpenDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    op: PhysDeleteOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

/// Closed state for the Delete operator.
pub(crate) struct ClosedDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    op: PhysDeleteOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
}

impl<Acc, Child> ClosedDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    pub(crate) fn new(
        op: PhysDeleteOp,
        ctx: TransactionContext<Acc>,
        child: Child,
        stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            op,
            ctx,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Acc, Child> ClosedExecutor for ClosedDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    type Running = OpenDelete<Acc, Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        Ok(OpenDelete {
            op: self.op,
            ctx: self.ctx,
            child: self.child.open()?,
            stats: self.stats,
            returned_result: false,
        })
    }
}

impl<Acc, Child> OpenDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Acc, Child> RunningExecutor for OpenDelete<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    type Closed = ClosedDelete<Acc, Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let row_id = extract_row_id(&row)?;
            let mut dml = DmlExecutor::new(self.ctx.clone());
            let result = dml.delete(self.op.table_id, row_id)?;

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

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedDelete::new(
            self.op,
            self.ctx,
            self.child.close()?,
            Some(self.stats),
        ))
    }
}
