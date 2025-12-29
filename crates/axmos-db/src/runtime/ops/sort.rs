use std::cmp::Ordering;

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, eval::ExpressionEvaluator},
        planner::physical::EnforcerSortOp,
    },
    types::DataType,
};

/// Sort executor that handles both in-memory sorting
pub(crate) struct QuickSort {
    op: EnforcerSortOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,

    /// Buffered rows for in-memory sort.
    buffer: Vec<Row>,

    /// Current position in buffer during output phase.
    current_idx: usize,

    /// Sort key cache: precomputed sort keys for each row.
    sort_keys: Vec<Vec<DataType>>,
}

impl QuickSort {
    pub(crate) fn new(op: EnforcerSortOp, child: Box<dyn Executor>) -> Self {
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),

            buffer: Vec::new(),
            current_idx: 0,

            sort_keys: Vec::new(),
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract sort key values from a row.
    fn extract_sort_key(&self, row: &Row) -> ExecutionResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.op.schema);

        let mut key_values = Vec::with_capacity(self.op.order_by.len());
        for sort_expr in &self.op.order_by {
            let value = evaluator.evaluate(sort_expr.expr.clone())?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Compare two rows based on sort expressions.
    fn compare_rows(&self, a_keys: &[DataType], b_keys: &[DataType]) -> Ordering {
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

    /// Consume all input and sort.
    fn build(&mut self) -> ExecutionResult<()> {
        let mut current_run: Vec<(Row, Vec<DataType>)> = Vec::new();

        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let sort_key = self.extract_sort_key(&row)?;
            current_run.push((row, sort_key));
        }

        // In-memory sort using quicksort
        self.quick_sort(current_run)?;

        Ok(())
    }

    /// Perform in-memory quicksort.
    fn quick_sort(&mut self, mut rows_with_keys: Vec<(Row, Vec<DataType>)>) -> ExecutionResult<()> {
        // Sort using the comparison function
        rows_with_keys.sort_by(|a, b| self.compare_rows(&a.1, &b.1));

        // Extract just the rows
        self.buffer = rows_with_keys.into_iter().map(|(row, _)| row).collect();
        self.current_idx = 0;

        Ok(())
    }
}

impl Executor for QuickSort {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.buffer.clear();
        self.current_idx = 0;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => {
                self.state = ExecutionState::Running;
                self.build()?;
            }
            _ => {}
        }

        // In-memory sort: return from buffer
        if self.current_idx < self.buffer.len() {
            let row = self.buffer[self.current_idx].clone();
            self.current_idx += 1;
            self.stats.rows_produced += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }
        self.child.close()?;
        self.state = ExecutionState::Closed;
        self.buffer.clear();

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.schema
    }
}
