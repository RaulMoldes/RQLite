//! Insert operator implementation.
//!
//! Inserts rows into a table by consuming rows from its child operator
//! and delegating the actual insertion to the DML executor.

use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult,
        context::TransactionContext, dml::DmlExecutor,
    },
    sql::planner::physical::PhysInsertOp,
    storage::tuple::Row,
    tree::accessor::TreeWriter,
    types::{DataType, UInt64},
};

/// Running state for the Insert operator.
pub(crate) struct OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    op: PhysInsertOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

/// Closed state for the Insert operator.
pub(crate) struct ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    op: PhysInsertOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
}

impl<Acc, Child> ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    pub(crate) fn new(
        op: PhysInsertOp,
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

impl<Acc, Child> ClosedExecutor for ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    type Running = OpenInsert<Acc, Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        Ok(OpenInsert {
            op: self.op,
            ctx: self.ctx,
            child: self.child.open()?,
            stats: self.stats,
            returned_result: false,
        })
    }
}

impl<Acc, Child> OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Acc, Child> RunningExecutor for OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    type Closed = ClosedInsert<Acc, Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let mut dml = DmlExecutor::new(&mut self.ctx);
            dml.insert(self.op.table_id, &self.op.columns, &row)?;

            self.stats.rows_produced += 1;
        }

        self.returned_result = true;

        // Return count of inserted rows
        Ok(Some(Row::new(Box::new([DataType::BigUInt(UInt64::from(
            self.stats.rows_produced,
        ))]))))
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedInsert::new(
            self.op,
            self.ctx,
            self.child.close()?,
            Some(self.stats),
        ))
    }
}
