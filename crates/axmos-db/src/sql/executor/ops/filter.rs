// src/sql/executor/ops/filter.rs
//! Filter executor that applies predicates to input rows.

use std::io::{self, Error as IoError, ErrorKind};

use crate::database::schema::Schema;
use crate::sql::binder::ast::BoundExpression;
use crate::sql::executor::eval::ExpressionEvaluator;
use crate::sql::executor::{ExecutionStats, Executor, Row};

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
    is_open: bool,
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
            is_open: false,
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Evaluate the predicate against a row.
    fn evaluate_predicate(&self, row: &Row) -> io::Result<bool> {
        let schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(row, schema);
        dbg!("EVALUATING PREDICATE");
        dbg!(row);
        dbg!(schema);
        evaluator.evaluate_as_bool(self.predicate.clone())
    }
}

impl Executor for Filter {
    fn open(&mut self) -> io::Result<()> {
        if self.is_open {
            return Err(IoError::new(ErrorKind::InvalidInput, "Filter already open"));
        }

        self.child.open()?;
        self.is_open = true;
        self.stats = ExecutionStats::default();

        Ok(())
    }

    fn next(&mut self) -> io::Result<Option<Row>> {
        if !self.is_open {
            return Err(IoError::new(ErrorKind::InvalidInput, "Filter not opened"));
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

    fn close(&mut self) -> io::Result<()> {
        if !self.is_open {
            return Ok(());
        }

        self.child.close()?;
        self.is_open = false;

        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}
