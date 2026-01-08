// src/runtime/ops/values.rs
use crate::{
    runtime::eval::ExpressionEvaluator,
    runtime::{ExecutionStats, Executor, RuntimeResult},
    schema::Schema,
    sql::{binder::bounds::BoundExpression, planner::physical::PhysValuesOp},
    storage::tuple::Row,
    types::DataType,
};

use std::vec::IntoIter;

pub(crate) struct Values {
    rows: IntoIter<Vec<BoundExpression>>,
    schema: Schema,
    stats: ExecutionStats,
}

impl Values {
    pub(crate) fn new(op: &PhysValuesOp) -> Self {
        let schema = op.schema.clone();
        Self {
            rows: op.rows.clone().into_iter(),
            schema,
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for Values {
    fn open(&mut self) -> RuntimeResult<()> {
        Ok(())
    }

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
                    let value = evaluator.evaluate_as_single_value(&expr)?;
                    values.push(value);
                }

                self.stats.rows_produced += 1;
                Ok(Some(Row::new(values.into_boxed_slice())))
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
}
