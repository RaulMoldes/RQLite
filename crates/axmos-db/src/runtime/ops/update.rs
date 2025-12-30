//! Update operator implementation.
//!
//! Updates rows in a table by consuming rows from its child operator
//! (which provides rows to update) and delegating the actual update
//! to the DML executor.

use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult,
        context::TransactionContext,
        dml::{DmlExecutor, evaluate_assignments, extract_row_id},
    },
    schema::catalog::CatalogTrait,
    sql::planner::physical::PhysUpdateOp,
    storage::tuple::Row,
    tree::accessor::TreeWriter,
    types::{DataType, UInt64},
};

/// Running state for the Update operator.
pub(crate) struct OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    op: PhysUpdateOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

/// Closed state for the Update operator.
pub(crate) struct ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    op: PhysUpdateOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
}

impl<Acc, Child> ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    pub(crate) fn new(
        op: PhysUpdateOp,
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

impl<Acc, Child> ClosedExecutor for ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    type Running = OpenUpdate<Acc, Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        Ok(OpenUpdate {
            op: self.op,
            ctx: self.ctx,
            child: self.child.open()?,
            stats: self.stats,
            returned_result: false,
        })
    }
}

impl<Acc, Child> OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Evaluates assignments and performs the update for a single row.
    fn update_row(&mut self, row: &Row) -> RuntimeResult<bool> {
        // Get the schema to evaluate assignments
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(self.op.table_id, &tree_builder, &snapshot)?;
        let schema = relation.schema().clone();

        // Extract row ID and evaluate assignments
        let row_id = extract_row_id(row)?;
        let assignments = evaluate_assignments(self.op.assignments.iter(), row, &schema)?;

        // Delegate to DML executor
        let mut dml = DmlExecutor::new(&mut self.ctx);
        let result = dml.update(self.op.table_id, row_id, &assignments)?;

        Ok(result.updated)
    }
}

impl<Acc, Child> RunningExecutor for OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    type Closed = ClosedUpdate<Acc, Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            if self.update_row(&row)? {
                self.stats.rows_produced += 1;
            }
        }

        self.returned_result = true;

        // Return count of updated rows
        Ok(Some(Row::new(Box::new([DataType::BigUInt(UInt64::from(
            self.stats.rows_produced,
        ))]))))
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedUpdate::new(
            self.op,
            self.ctx,
            self.child.close()?,
            Some(self.stats),
        ))
    }
}
