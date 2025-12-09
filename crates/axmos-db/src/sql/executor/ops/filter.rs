// src/sql/executor/ops/filter.rs
//! Filter executor that applies predicates to input rows.
use crate::{
    database::{
        errors::{EvalResult, ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        binder::ast::BoundExpression,
        executor::{
            eval::ExpressionEvaluator,
            {ExecutionState, ExecutionStats, Executor, Row},
        },
    },
};

/// Filter operator that evaluates a predicate on each input row.
/// Only rows where the predicate evaluates to true are passed through.
pub(crate) struct Filter {
    /// Child executor providing input rows
    child: Box<dyn Executor>,
    /// Filter predicate to evaluate
    predicate: BoundExpression,
    /// Execution statistics
    stats: ExecutionStats,
    /// Whether the executor is open
    state: ExecutionState,
}

impl Filter {
    /// Creates a new Filter executor.
    ///
    /// # Arguments
    /// * `child` - The input executor
    /// * `predicate` - The filter predicate (WHERE clause expression)
    pub(crate) fn new(child: Box<dyn Executor>, predicate: BoundExpression) -> Self {
        Self {
            child,
            predicate,
            stats: ExecutionStats::default(),
            state: ExecutionState::Closed,
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Evaluate the predicate against a row.
    fn evaluate_predicate(&self, row: &Row) -> EvalResult<bool> {
        let schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(row, schema);
        evaluator.evaluate_as_bool(self.predicate.clone())
    }
}

impl Executor for Filter {
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

        // Keep fetching from child until we find a matching row or exhaust input
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(row) => {
                    self.stats.rows_scanned += 1;

                    if self.evaluate_predicate(&row)? {
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                    // Row didn't pass predicate, continue to next
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
