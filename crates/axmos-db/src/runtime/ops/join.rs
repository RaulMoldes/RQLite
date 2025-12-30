// src/runtime/ops/nested_loop_join.rs
use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult, eval::ExpressionEvaluator,
    },
    sql::{parser::ast::JoinType, planner::physical::NestedLoopJoinOp},
    storage::tuple::Row,
    types::DataType,
};

/// Nested Loop Join executor.
/// For each row from the outer (left) child, scans all rows from the inner (right) child.
pub(crate) struct OpenNestedLoopJoin<Left: RunningExecutor, Right: RunningExecutor> {
    op: NestedLoopJoinOp,
    left: Option<Left>,
    right: Option<Right>,
    closed_left: Option<Left::Closed>,
    closed_right: Option<Right::Closed>,
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

pub(crate) struct ClosedNestedLoopJoin<Left: ClosedExecutor, Right: ClosedExecutor> {
    op: NestedLoopJoinOp,
    left: Left,
    right: Right,
    stats: ExecutionStats,
}

impl<Left: ClosedExecutor, Right: ClosedExecutor> ClosedNestedLoopJoin<Left, Right> {
    pub(crate) fn new(
        op: NestedLoopJoinOp,
        left: Left,
        right: Right,
        stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            op,
            left,
            right,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Left: ClosedExecutor, Right: ClosedExecutor> ClosedExecutor
    for ClosedNestedLoopJoin<Left, Right>
{
    type Running = OpenNestedLoopJoin<Left::Running, Right::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let left = self.left.open()?;
        let right = self.right.open()?;

        Ok(OpenNestedLoopJoin {
            op: self.op,
            left: Some(left),
            right: Some(right),
            closed_left: None,
            closed_right: None,
            current_left_row: None,
            right_buffer: Vec::new(),
            right_idx: 0,
            right_buffered: false,
            left_matched: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_idx: 0,
            stats: self.stats,
        })
    }
}

impl<Left: RunningExecutor, Right: RunningExecutor> OpenNestedLoopJoin<Left, Right> {
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Buffer all rows from the right child
    fn buffer_right(&mut self) -> RuntimeResult<()> {
        if self.right_buffered {
            return Ok(());
        }

        if let Some(mut right) = self.right.take() {
            while let Some(row) = right.next()? {
                self.right_buffer.push(row);
            }
            self.closed_right = Some(right.close()?);
        }

        // Initialize right_matched for RIGHT/FULL joins
        if matches!(self.op.join_type, JoinType::Right | JoinType::Full) {
            self.right_matched = vec![false; self.right_buffer.len()];
        }

        self.right_buffered = true;
        Ok(())
    }

    /// Get next row from left child
    fn next_left_row(&mut self) -> RuntimeResult<Option<Row>> {
        if let Some(ref mut left) = self.left {
            if let Some(row) = left.next()? {
                self.stats.rows_scanned += 1;
                return Ok(Some(row));
            }
            // Left exhausted, close it
            let left = self.left.take().unwrap();
            self.closed_left = Some(left.close()?);
        }
        Ok(None)
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
        let right_cols = self.op.output_schema.num_columns() - left.len();
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
        let left_cols = self.op.output_schema.num_columns() - right.len();
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
        match &self.op.condition {
            None => Ok(true),
            Some(cond) => {
                let evaluator = ExpressionEvaluator::new(combined, &self.op.output_schema);
                let evaluated_bool = evaluator.evaluate_as_bool(cond)?;
                Ok(evaluated_bool)
            }
        }
    }
}

impl<Left: RunningExecutor, Right: RunningExecutor> RunningExecutor
    for OpenNestedLoopJoin<Left, Right>
{
    type Closed = ClosedNestedLoopJoin<Left::Closed, Right::Closed>;

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
                match self.next_left_row()? {
                    Some(row) => {
                        self.current_left_row = Some(row);
                        self.right_idx = 0;
                        self.left_matched = false;
                    }
                    None => {
                        // Left exhausted
                        // For RIGHT/FULL JOIN, now emit unmatched right rows
                        if matches!(self.op.join_type, JoinType::Right | JoinType::Full) {
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
                    if matches!(self.op.join_type, JoinType::Right | JoinType::Full) {
                        self.right_matched[self.right_idx - 1] = true;
                    }

                    self.stats.rows_produced += 1;
                    return Ok(Some(combined));
                }
            }

            // Finished scanning right for current left row
            // For LEFT/FULL JOIN: emit left row with NULLs if no match
            if !self.left_matched && matches!(self.op.join_type, JoinType::Left | JoinType::Full) {
                let row = self.left_with_nulls(left_row);
                self.current_left_row = None;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }

            // Move to next left row
            self.current_left_row = None;
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        // Handle case where iterators weren't fully consumed
        let closed_left = match self.closed_left {
            Some(c) => c,
            None => self.left.expect("Left should exist").close()?,
        };
        let closed_right = match self.closed_right {
            Some(c) => c,
            None => self.right.expect("Right should exist").close()?,
        };

        Ok(ClosedNestedLoopJoin {
            op: self.op,
            left: closed_left,
            right: closed_right,
            stats: self.stats,
        })
    }
}
