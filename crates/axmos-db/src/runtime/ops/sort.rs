// src/runtime/ops/sort.rs
use std::cmp::Ordering;

use crate::{
    runtime::{ExecutionStats, Executor, RuntimeResult, eval::ExpressionEvaluator},
    schema::Schema,
    sql::{planner::logical::SortExpr, planner::logical::SortOp},
    storage::tuple::Row,
    types::DataType,
};

/// QuickSort executor that buffers all input rows and sorts them in memory.
pub(crate) struct QuickSort<Child: Executor> {
    /// Output schema
    output_schema: Schema,
    /// Expr
    sort_exprs: Vec<SortExpr>,
    /// Child operator
    child: Child,
    /// Buffered rows after sorting
    buffer: Vec<Row>,
    /// Current position in buffer during iteration
    current_idx: usize,
    /// Execution stats
    stats: ExecutionStats,
}

impl<Child: Executor> QuickSort<Child> {
    pub(crate) fn new(op: &SortOp, child: Child) -> Self {
        Self {
            output_schema: op.schema.clone(),
            sort_exprs: op.order_by.clone(),
            child,
            stats: ExecutionStats::default(),
            buffer: Vec::new(),
            current_idx: 0,
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract sort key values from a row.
    fn extract_sort_key(&self, row: &Row) -> RuntimeResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.output_schema);

        let mut key_values = Vec::with_capacity(self.sort_exprs.len());
        for sort_expr in &self.sort_exprs {
            let value = evaluator.evaluate_as_single_value(&sort_expr.expr)?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Compare two sort keys based on sort expressions.
    fn compare_keys(&self, a_keys: &[DataType], b_keys: &[DataType]) -> Ordering {
        for (i, sort_expr) in self.sort_exprs.iter().enumerate() {
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
        let mut rows_with_keys: Vec<(Row, Vec<DataType>)> = Vec::new();

        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;
            let sort_key = self.extract_sort_key(&row)?;
            rows_with_keys.push((row, sort_key));
        }
        // Sort using the comparison function
        rows_with_keys.sort_by(|a, b| self.compare_keys(&a.1, &b.1));

        // Extract just the rows
        self.buffer = rows_with_keys.into_iter().map(|(row, _)| row).collect();
        self.child.close()?;

        Ok(())
    }
}

impl<Child: Executor> Executor for QuickSort<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        self.build_sorted()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
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

    fn close(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
}
