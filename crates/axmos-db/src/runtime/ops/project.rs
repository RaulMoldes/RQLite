// src/runtime/ops/project.rs
use crate::{
    runtime::{ExecutionStats, Executor, RuntimeError, RuntimeResult, eval::ExpressionEvaluator},
    schema::Schema,
    sql::{binder::bounds::BoundExpression, planner::physical::PhysProjectOp},
    storage::tuple::Row,
    types::DataType,
};

pub(crate) struct Project<Child: Executor> {
    child: Child,
    input_schema: Schema,
    output_schema: Schema,
    exprs: Vec<BoundExpression>,
    stats: ExecutionStats,
}

impl<Child: Executor> Project<Child> {
    pub(crate) fn new(op: &PhysProjectOp, child: Child) -> Self {
        Self {
            exprs: op
                .expressions
                .iter()
                .map(|expr| expr.expr.clone())
                .collect(),
            input_schema: op.input_schema.clone(),
            output_schema: op.output_schema.clone(),
            child,
            stats: ExecutionStats::default(),
        }
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: Executor> Executor for Project<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        match self.child.next()? {
            None => Ok(None),
            Some(row) => {
                self.stats.rows_scanned += 1;

                let evaluator = ExpressionEvaluator::new(&row, &self.input_schema);
                let mut projected: Vec<DataType> = Vec::with_capacity(self.exprs.len());

                for (i, proj_expr) in self.exprs.iter().enumerate() {
                    let value = evaluator.evaluate_as_single_value(&proj_expr)?;
                    // Cast to the expected output type from the schema
                    let expected_type = self
                        .output_schema
                        .column(i)
                        .ok_or(RuntimeError::ColumnNotFound(i))?
                        .datatype();

                    let casted = value.try_cast(expected_type)?;
                    projected.push(casted);
                }

                self.stats.rows_produced += 1;
                Ok(Some(Row::new(projected.into_boxed_slice())))
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
