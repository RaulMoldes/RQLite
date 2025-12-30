// src/runtime/ops/filter.rs

use crate::{
    runtime::eval::ExpressionEvaluator,
    runtime::{ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult},
    schema::Schema,
    sql::{binder::bounds::BoundExpression, planner::physical::PhysFilterOp},
    storage::tuple::Row,
};

pub(crate) struct OpenFilter<Child: RunningExecutor> {
    predicate: BoundExpression,
    schema: Schema,
    child: Child,
    stats: ExecutionStats,
}

pub(crate) struct ClosedFilter<Child: ClosedExecutor> {
    op: PhysFilterOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedFilter<Child> {
    pub(crate) fn new(op: PhysFilterOp, child: Child, stats: Option<ExecutionStats>) -> Self {
        Self {
            op,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: ClosedExecutor> ClosedExecutor for ClosedFilter<Child> {
    type Running = OpenFilter<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenFilter {
            predicate: self.op.predicate,
            schema: self.op.schema,
            child,
            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenFilter<Child> {
    type Closed = ClosedFilter<Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(row) => {
                    self.stats.rows_scanned += 1;

                    let evaluator = ExpressionEvaluator::new(&row, &self.schema);

                    if evaluator.evaluate_as_bool(&self.predicate)? {
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                }
            }
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        let child = self.child.close()?;
        Ok(ClosedFilter {
            op: PhysFilterOp {
                predicate: self.predicate,
                schema: self.schema,
            },
            child,
            stats: self.stats,
        })
    }
}
