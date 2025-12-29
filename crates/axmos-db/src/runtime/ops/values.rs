// src/runtime/ops/values.rs

use crate::{
    runtime::eval::ExpressionEvaluator,
    runtime::{ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult},
    schema::Schema,
    sql::planner::physical::PhysValuesOp,
    storage::tuple::Row,
    types::DataType,
};

pub(crate) struct OpenValues {
    rows: std::vec::IntoIter<Vec<crate::sql::binder::bounds::BoundExpression>>,
    schema: Schema,
    stats: ExecutionStats,
}

pub(crate) struct ClosedValues {
    op: PhysValuesOp,
    stats: ExecutionStats,
}

impl ClosedValues {
    pub(crate) fn new(op: PhysValuesOp, stats: Option<ExecutionStats>) -> Self {
        Self {
            op,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl ClosedExecutor for ClosedValues {
    type Running = OpenValues;

    fn open(self) -> RuntimeResult<Self::Running> {
        Ok(OpenValues {
            rows: self.op.rows.into_iter(),
            schema: self.op.schema,
            stats: self.stats,
        })
    }
}

impl RunningExecutor for OpenValues {
    type Closed = ClosedValues;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        match self.rows.next() {
            None => Ok(None),
            Some(exprs) => {
                self.stats.rows_scanned += 1;

                // Values should not reference columns in the schema so we can safely evaluate each expression using an empty row
                let row = Row::default();
                let schema = Schema::default();
                let evaluator = ExpressionEvaluator::new(&row, &schema);

                let mut values: Vec<DataType> = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    let value = evaluator.evaluate(&expr)?;
                    values.push(value);
                }

                self.stats.rows_produced += 1;
                Ok(Some(Row::new(values.into_boxed_slice())))
            }
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedValues {
            op: PhysValuesOp {
                rows: Vec::new(),
                schema: self.schema,
            },
            stats: self.stats,
        })
    }
}
