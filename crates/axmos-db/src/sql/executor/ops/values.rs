// src/sql/executor/ops/values.rs
use crate::{
    PAGE_ZERO,
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    io::frames::{FrameAccessMode, Position},
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, eval::ExpressionEvaluator},
        planner::physical::PhysValuesOp,
    },
    storage::cell::Slot,
};

pub(crate) struct Values {
    op: PhysValuesOp,
    current_row: usize,
    state: ExecutionState,
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

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for Values {
    fn open(&mut self, _access_mode: FrameAccessMode) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }
        self.current_row = 0;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<(Position, Row)>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        if self.current_row >= self.op.rows.len() {
            return Ok(None);
        }

        let row_exprs = &self.op.rows[self.current_row];
        self.current_row += 1;
        self.stats.rows_scanned += 1;

        let empty_row = Row::new();
        let evaluator = ExpressionEvaluator::new(&empty_row, &self.op.schema);
        let mut output_row = Row::with_capacity(row_exprs.len());
        for expr in row_exprs {
            let value = evaluator.evaluate(expr.clone())?;
            output_row.push(value);
        }

        self.stats.rows_produced += 1;
        // Values don't come from storage, use dummy position
        Ok(Some((Position::new(PAGE_ZERO, Slot(0)), output_row)))
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
