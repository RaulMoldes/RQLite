// src/runtime/ops/nested_loop_join.rs
use crate::{
    runtime::{ExecutionStats, Executor, RuntimeResult, eval::ExpressionEvaluator},
    schema::Schema,
    sql::{
        binder::bounds::BoundExpression, parser::ast::JoinType, planner::physical::NestedLoopJoinOp,
    },
    storage::tuple::Row,
    types::DataType,
};

/// Nested Loop Join executor.
/// For each row from the outer (left) child, scans all rows from the inner (right) child.
pub(crate) struct NestedLoopJoin<Left: Executor, Right: Executor> {
    join_type: JoinType,
    condition: Option<BoundExpression>,
    output_schema: Schema,

    left: Left,
    right: Right,
    /// Current row from the left (outer) side
    current_left_row: Option<Row>,
    /// Buffered rows from the right (inner) side - rescanned for each left row
    right_buffer: Vec<Row>,
    /// Current position in right buffer
    right_idx: usize,
    /// Whether we've buffered the right side
    right_buffered: bool,
    /// For LEFT JOIN: tracks if current left row matched any right row
    left_matched: bool,
    /// For RIGHT JOIN: tracks which right rows have been matched
    right_matched: Vec<bool>,
    /// For RIGHT/FULL JOIN: emitting unmatched right rows phase
    emitting_unmatched_right: bool,
    /// Index for emitting unmatched right rows
    unmatched_right_idx: usize,
    /// Execution stats
    stats: ExecutionStats,
}

impl<Left: Executor, Right: Executor> NestedLoopJoin<Left, Right> {
    pub(crate) fn new(op: &NestedLoopJoinOp, left: Left, right: Right) -> Self {
        Self {
            join_type: op.join_type,
            condition: op.condition.clone(),
            output_schema: op.output_schema.clone(),
            left,
            right,
            current_left_row: None,
            stats: ExecutionStats::default(),
            right_buffer: Vec::new(),
            right_idx: 0,
            right_buffered: false,
            left_matched: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_idx: 0,
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Buffer all rows from the right child
    fn buffer_right(&mut self) -> RuntimeResult<()> {
        if self.right_buffered {
            return Ok(());
        }

        while let Some(row) = self.right.next()? {
            self.right_buffer.push(row);
        }
        self.right.close()?;

        // Initialize right_matched for RIGHT/FULL joins
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.right_matched = vec![false; self.right_buffer.len()];
        }

        self.right_buffered = true;
        Ok(())
    }

    /// Combine left and right rows into output row
    fn combine_rows(&self, left: &Row, right: &Row) -> Row {
        let mut values: Vec<DataType> = Vec::with_capacity(left.len() + right.len());
        for i in 0..left.len() {
            values.push(left[i].clone());
        }
        for i in 0..right.len() {
            values.push(right[i].clone());
        }
        Row::new(values.into_boxed_slice())
    }

    /// Create a row with left values and NULLs for right side
    fn left_with_nulls(&self, left: &Row) -> Row {
        let right_cols = self.output_schema.num_columns() - left.len();
        let mut values: Vec<DataType> = Vec::with_capacity(left.len() + right_cols);
        for i in 0..left.len() {
            values.push(left[i].clone());
        }
        for _ in 0..right_cols {
            values.push(DataType::Null);
        }
        Row::new(values.into_boxed_slice())
    }

    /// Create a row with NULLs for left side and right values
    fn nulls_with_right(&self, right: &Row) -> Row {
        let left_cols = self.output_schema.num_columns() - right.len();
        let mut values: Vec<DataType> = Vec::with_capacity(left_cols + right.len());
        for _ in 0..left_cols {
            values.push(DataType::Null);
        }
        for i in 0..right.len() {
            values.push(right[i].clone());
        }
        Row::new(values.into_boxed_slice())
    }

    /// Evaluate join condition
    fn evaluate_condition(&self, combined: &Row) -> RuntimeResult<bool> {
        match &self.condition {
            None => Ok(true),
            Some(cond) => {
                let evaluator = ExpressionEvaluator::new(combined, &self.output_schema);
                let evaluated_bool = evaluator.evaluate_as_bool(cond)?;
                Ok(evaluated_bool)
            }
        }
    }
}

impl<Left: Executor, Right: Executor> Executor for NestedLoopJoin<Left, Right> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.left.open()?;
        self.right.open()?;

        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        // First, buffer the right side
        self.buffer_right()?;

        // Handle RIGHT/FULL JOIN: emit unmatched right rows at the end
        if self.emitting_unmatched_right {
            while self.unmatched_right_idx < self.right_buffer.len() {
                let idx = self.unmatched_right_idx;
                self.unmatched_right_idx += 1;

                if !self.right_matched[idx] {
                    let row = self.nulls_with_right(&self.right_buffer[idx]);
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
            }
            return Ok(None);
        }

        loop {
            // If no current left row, get one
            if self.current_left_row.is_none() {
                match self.left.next()? {
                    Some(row) => {
                        self.stats.rows_scanned += 1;
                        self.current_left_row = Some(row);
                        self.right_idx = 0;
                        self.left_matched = false;
                    }
                    None => {
                        self.left.close()?;

                        // Left exhausted
                        // For RIGHT/FULL JOIN, now emit unmatched right rows
                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            self.emitting_unmatched_right = true;
                            return self.next();
                        }
                        return Ok(None);
                    }
                }
            }

            let left_row = self.current_left_row.as_ref().unwrap();

            // Scan through right buffer
            while self.right_idx < self.right_buffer.len() {
                let right_row = &self.right_buffer[self.right_idx];
                self.right_idx += 1;

                let combined = self.combine_rows(left_row, right_row);

                if self.evaluate_condition(&combined)? {
                    self.left_matched = true;

                    // Mark right row as matched for RIGHT/FULL joins
                    if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                        self.right_matched[self.right_idx - 1] = true;
                    }

                    self.stats.rows_produced += 1;
                    return Ok(Some(combined));
                }
            }

            // Finished scanning right for current left row
            // For LEFT/FULL JOIN: emit left row with NULLs if no match
            if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                let row = self.left_with_nulls(left_row);
                self.current_left_row = None;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }

            // Move to next left row
            self.current_left_row = None;
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}
