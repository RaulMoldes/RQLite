// src/runtime/ops/sort.rs
use std::cmp::Ordering;

use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult, eval::ExpressionEvaluator,
    },
    sql::planner::logical::SortOp,
    storage::tuple::Row,
    types::DataType,
};

/// Sort executor that buffers all input rows and sorts them in memory.
pub(crate) struct OpenSort<Child: RunningExecutor> {
    op: SortOp,
    child: Option<Child>,
    closed_child: Option<Child::Closed>,
    /// Buffered rows after sorting
    buffer: Vec<Row>,
    /// Current position in buffer during iteration
    current_idx: usize,
    /// Execution stats
    stats: ExecutionStats,
}

pub(crate) struct ClosedSort<Child: ClosedExecutor> {
    op: SortOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedSort<Child> {
    pub(crate) fn new(op: SortOp, child: Child, stats: Option<ExecutionStats>) -> Self {
        Self {
            op,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: ClosedExecutor> ClosedExecutor for ClosedSort<Child> {
    type Running = OpenSort<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenSort {
            op: self.op,
            child: Some(child),
            closed_child: None,
            buffer: Vec::new(),
            current_idx: 0,
            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> OpenSort<Child> {
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract sort key values from a row.
    fn extract_sort_key(&self, row: &Row) -> RuntimeResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.op.schema);

        let mut key_values = Vec::with_capacity(self.op.order_by.len());
        for sort_expr in &self.op.order_by {
            let value = evaluator.evaluate(&sort_expr.expr)?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Compare two sort keys based on sort expressions.
    fn compare_keys(&self, a_keys: &[DataType], b_keys: &[DataType]) -> Ordering {
        for (i, sort_expr) in self.op.order_by.iter().enumerate() {
            let a_val = &a_keys[i];
            let b_val = &b_keys[i];

            // Handle NULL ordering
            let cmp = match (a_val, b_val) {
                (DataType::Null, DataType::Null) => Ordering::Equal,
                (DataType::Null, _) => {
                    if sort_expr.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, DataType::Null) => {
                    if sort_expr.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => a_val.partial_cmp(b_val).unwrap_or(Ordering::Equal),
            };

            if cmp != Ordering::Equal {
                return if sort_expr.asc { cmp } else { cmp.reverse() };
            }
        }

        Ordering::Equal
    }

    /// Consume all input rows, sort them, and store in buffer.
    fn build_sorted(&mut self) -> RuntimeResult<()> {
        if let Some(mut child) = self.child.take() {
            let mut rows_with_keys: Vec<(Row, Vec<DataType>)> = Vec::new();

            while let Some(row) = child.next()? {
                self.stats.rows_scanned += 1;
                let sort_key = self.extract_sort_key(&row)?;
                rows_with_keys.push((row, sort_key));
            }

            // Sort using the comparison function
            rows_with_keys.sort_by(|a, b| self.compare_keys(&a.1, &b.1));

            // Extract just the rows
            self.buffer = rows_with_keys.into_iter().map(|(row, _)| row).collect();
            self.closed_child = Some(child.close()?);
        }

        Ok(())
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenSort<Child> {
    type Closed = ClosedSort<Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        // Build sorted buffer on first call
        if self.child.is_some() {
            self.build_sorted()?;
        }

        // Return buffered rows one at a time
        if self.current_idx < self.buffer.len() {
            let row = self.buffer[self.current_idx].clone();
            self.current_idx += 1;
            self.stats.rows_produced += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedSort {
            op: self.op,
            child: self.closed_child.expect("Child should be closed"),
            stats: self.stats,
        })
    }
}
