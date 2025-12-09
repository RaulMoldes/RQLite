// src/sql/executor/ops/values.rs
//! Values executor that produces rows from literal expressions.

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        executor::{
            eval::ExpressionEvaluator, ExecutionState, ExecutionStats, Executor, Row,
        },
        planner::physical::PhysValuesOp,
    },
};

/// Values operator that produces rows from literal expressions.
///
/// This is used for INSERT ... VALUES (...), (...) statements
/// and standalone VALUES queries.
pub(crate) struct Values {
    /// Physical operator definition
    op: PhysValuesOp,
    /// Current row index
    current_row: usize,
    /// Current execution state
    state: ExecutionState,
    /// Execution statistics
    stats: ExecutionStats,
}

impl Values {
    pub(crate) fn new(op: PhysValuesOp) -> Self {
        Self {
            op,
            current_row: 0,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for Values {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.current_row = 0;
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

        // Check if we have more rows to produce
        if self.current_row >= self.op.rows.len() {
            return Ok(None);
        }

        // Get the current row's expressions
        let row_exprs = &self.op.rows[self.current_row];
        self.current_row += 1;
        self.stats.rows_scanned += 1;

        // Evaluate each expression to produce the row values
        // For VALUES, we use an empty row as context since expressions are literals
        let empty_row = Row::new();
        let evaluator = ExpressionEvaluator::new(&empty_row, &self.op.schema);

        let mut output_row = Row::with_capacity(row_exprs.len());
        for expr in row_exprs {
            let value = evaluator.evaluate(expr.clone())?;
            output_row.push(value);
        }

        self.stats.rows_produced += 1;
        Ok(Some(output_row))
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }

        self.state = ExecutionState::Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.schema
    }
}
