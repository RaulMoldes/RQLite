use crate::{
    database::{
        errors::{EvalResult, ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        binder::ast::BoundExpression,
        executor::{ExecutionState, ExecutionStats, Executor, Row, eval::ExpressionEvaluator},
    },
};

pub(crate) struct Project {
    child: Box<dyn Executor>,
    expressions: Vec<BoundExpression>,
    output_schema: Schema,
    stats: ExecutionStats,
    state: ExecutionState,
}

impl Project {
    pub(crate) fn new(
        child: Box<dyn Executor>,
        expressions: Vec<BoundExpression>,
        output_schema: Schema,
    ) -> Self {
        Self {
            child,
            expressions,
            output_schema,
            stats: ExecutionStats::default(),
            state: ExecutionState::Closed,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn project_row(&self, input_row: &Row) -> EvalResult<Row> {
        let input_schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(input_row, input_schema);
   
        let mut output_row = Row::with_capacity(self.expressions.len());
        for expr in &self.expressions {
            let value = evaluator.evaluate(expr.clone())?;
            output_row.push(value);
        }
        Ok(output_row)
    }
}

impl Executor for Project {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }
        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        match self.child.next()? {
            None => Ok(None),
            Some(input_row) => {
                self.stats.rows_scanned += 1;
                let output_row = self.project_row(&input_row)?;
                self.stats.rows_produced += 1;
                Ok(Some(output_row))
            }
        }
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }
        self.child.close()?;
        self.state = ExecutionState::Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
