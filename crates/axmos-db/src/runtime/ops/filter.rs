// src/runtime/ops/filter.rs
use crate::{
    runtime::eval::ExpressionEvaluator,
    runtime::{ExecutionStats, Executor, RuntimeResult},
    schema::Schema,
    sql::{binder::bounds::BoundExpression, planner::physical::PhysFilterOp},
    storage::tuple::Row,
};

pub(crate) struct Filter<Child: Executor> {
    schema: Schema,
    predicate: BoundExpression,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: Executor> Filter<Child> {
    pub(crate) fn new(op: &PhysFilterOp, child: Child) -> Self {
        Self {
            schema: op.schema.clone(),
            predicate: op.predicate.clone(),
            child,
            stats: ExecutionStats::default(),
        }
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn predicate(&self) -> &BoundExpression {
        &self.predicate
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: Executor> Executor for Filter<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(row) => {
                    self.stats.rows_scanned += 1;

                    let evaluator = ExpressionEvaluator::new(&row, self.schema());

                    if evaluator.evaluate_as_bool(self.predicate())? {
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                }
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
