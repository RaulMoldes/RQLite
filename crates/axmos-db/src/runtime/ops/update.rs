use crate::{
    ObjectId,
    runtime::{
        ExecutionStats, Executor, RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        dml::{DmlExecutor, evaluate_assignments, extract_row_id},
    },
    schema::{Schema, catalog::CatalogTrait},
    sql::{binder::bounds::BoundExpression, planner::physical::PhysUpdateOp},
    storage::tuple::Row,
    types::{DataType, UInt64},
};

/// Running state for the Update operator.
pub(crate) struct Update<Child>
where
    Child: Executor,
{
    ctx: ThreadContext,
    logger: TransactionLogger,
    table_id: ObjectId,
    table_schema: Schema,
    assignments: Box<[(usize, BoundExpression)]>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

impl<Child> Update<Child>
where
    Child: Executor,
{
    pub(crate) fn new(
        op: &PhysUpdateOp,
        ctx: ThreadContext,
        logger: TransactionLogger,
        child: Child,
    ) -> Self {
        Self {
            table_id: op.table_id,
            table_schema: op.table_schema.clone(),
            assignments: op.assignments.clone().into_boxed_slice(),
            ctx,
            logger,
            child,
            stats: ExecutionStats::default(),
            returned_result: false,
        }
    }

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
                .get_relation(self.table_id, &tree_builder, &snapshot)?;
        let schema = relation.schema().clone();

        // Extract row ID and evaluate assignments
        let row_id = extract_row_id(row)?;
        let assignments = evaluate_assignments(self.assignments.iter(), row, &schema)?;

        // Delegate to DML executor
        let mut dml = DmlExecutor::new(self.ctx.clone(), self.logger.clone());
        let result = dml.update(self.table_id, row_id, assignments)?;

        Ok(result.updated)
    }
}

impl<Child> Executor for Update<Child>
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

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
