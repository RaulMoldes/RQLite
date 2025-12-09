// src/sql/executor/ops/filter.rs
use crate::{
    database::{
        errors::{EvalResult, ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    io::frames::{FrameAccessMode, Position},
    sql::{
        binder::ast::BoundExpression,
        executor::{
            eval::ExpressionEvaluator,
            {ExecutionState, ExecutionStats, Executor, Row},
        },
    },
};

pub(crate) struct Filter {
    child: Box<dyn Executor>,
    predicate: BoundExpression,
    stats: ExecutionStats,
    state: ExecutionState,
}

impl Filter {
    pub(crate) fn new(child: Box<dyn Executor>, predicate: BoundExpression) -> Self {
        Self {
            child,
            predicate,
            stats: ExecutionStats::default(),
            state: ExecutionState::Closed,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn evaluate_predicate(&self, row: &Row) -> EvalResult<bool> {
        let schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(row, schema);
        evaluator.evaluate_as_bool(self.predicate.clone())
    }
}

impl Executor for Filter {
    fn open(&mut self, access_mode: FrameAccessMode) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }
        self.child.open(access_mode)?;
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

        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some((position, row)) => {
                    self.stats.rows_scanned += 1;
                    if self.evaluate_predicate(&row)? {
                        self.stats.rows_produced += 1;
                        return Ok(Some((position, row)));
                    }
                }
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
        self.child.schema()
    }
}
