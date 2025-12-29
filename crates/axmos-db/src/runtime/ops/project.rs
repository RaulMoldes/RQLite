// src/runtime/ops/project.rs

use crate::{
    runtime::eval::ExpressionEvaluator,
    runtime::{ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult},
    schema::Schema,
    sql::{planner::logical::ProjectExpr, planner::physical::PhysProjectOp},
    storage::tuple::Row,
    types::DataType,
};

pub(crate) struct OpenProject<Child: RunningExecutor> {
    expressions: Vec<ProjectExpr>,
    input_schema: Schema,
    output_schema: Schema,
    child: Child,
    stats: ExecutionStats,
}

pub(crate) struct ClosedProject<Child: ClosedExecutor> {
    op: PhysProjectOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedProject<Child> {
    pub(crate) fn new(op: PhysProjectOp, child: Child, stats: Option<ExecutionStats>) -> Self {
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

impl<Child: ClosedExecutor> ClosedExecutor for ClosedProject<Child> {
    type Running = OpenProject<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenProject {
            expressions: self.op.expressions,
            input_schema: self.op.input_schema,
            output_schema: self.op.output_schema,
            child,
            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenProject<Child> {
    type Closed = ClosedProject<Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        match self.child.next()? {
            None => Ok(None),
            Some(row) => {
                self.stats.rows_scanned += 1;

                let evaluator = ExpressionEvaluator::new(&row, &self.input_schema);
                let mut projected: Vec<DataType> = Vec::with_capacity(self.expressions.len());

                for proj_expr in &self.expressions {
                    let value = evaluator.evaluate(&proj_expr.expr)?;
                    projected.push(value);
                }

                self.stats.rows_produced += 1;
                Ok(Some(Row::new(projected.into_boxed_slice())))
            }
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        let child = self.child.close()?;
        Ok(ClosedProject {
            op: PhysProjectOp {
                expressions: self.expressions,
                input_schema: self.input_schema,
                output_schema: self.output_schema,
            },
            child,
            stats: self.stats,
        })
    }
}
