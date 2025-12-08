// src/sql/executor/ops/project.rs
//! Project executor that evaluates output expressions.

use std::io::{self, Error as IoError, ErrorKind};

use crate::database::schema::Schema;
use crate::sql::binder::ast::BoundExpression;
use crate::sql::executor::eval::ExpressionEvaluator;
use crate::sql::executor::{ExecutionStats, Executor, Row};

/// Project operator that computes output expressions for each input row.
pub(crate) struct Project {
    /// Child executor providing input rows
    child: Box<dyn Executor>,
    /// Expressions to evaluate for output columns
    expressions: Vec<BoundExpression>,
    /// Output schema (after projection)
    output_schema: Schema,
    /// Execution statistics
    stats: ExecutionStats,
    /// Whether the executor is open
    is_open: bool,
}

impl Project {
    /// Creates a new Project executor.
    ///
    /// # Arguments
    /// * `child` - The input executor
    /// * `expressions` - Expressions to evaluate for each output column
    /// * `output_schema` - The schema of the projected output
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
            is_open: false,
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Evaluate all projection expressions against a row.
    fn project_row(&self, input_row: &Row) -> io::Result<Row> {
        let input_schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(input_row, input_schema);

        let mut output_row = Vec::with_capacity(self.expressions.len());
        for expr in &self.expressions {
            let value = evaluator.evaluate(expr.clone())?;
            output_row.push(value);
        }

        Ok(output_row)
    }
}

impl Executor for Project {
    fn open(&mut self) -> io::Result<()> {
        if self.is_open {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Project already open",
            ));
        }

        self.child.open()?;
        self.is_open = true;
        self.stats = ExecutionStats::default();

        Ok(())
    }

    fn next(&mut self) -> io::Result<Option<Row>> {
        if !self.is_open {
            return Err(IoError::new(ErrorKind::InvalidInput, "Project not opened"));
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

    fn close(&mut self) -> io::Result<()> {
        if !self.is_open {
            return Ok(());
        }

        self.child.close()?;
        self.is_open = false;

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
