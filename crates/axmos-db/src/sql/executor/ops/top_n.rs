// src/sql/executor/ops/top_n.rs
//! TopN executor for ORDER BY ... LIMIT queries.
//!
//! Uses a bounded max-heap to efficiently find the top N rows without
//! sorting the entire dataset. Time complexity: O(n log k) where n is
//! input size and k is the limit.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{

        executor::{
            eval::ExpressionEvaluator, ExecutionState, ExecutionStats, Executor, Row,
        },
        planner::{physical::TopNOp,
            logical::SortExpr
        },
    },
    types::DataType,
};

/// A row with its precomputed sort key for heap comparison.
struct HeapEntry {
    row: Row,
    sort_key: Vec<DataType>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for HeapEntry {}

/// Wrapper to implement custom ordering based on sort expressions.
/// Uses inverted ordering for max-heap behavior (we want to evict largest elements).
struct HeapEntryWrapper {
    entry: HeapEntry,
    order_by: Vec<SortExpr>,
}

impl PartialEq for HeapEntryWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.entry.sort_key == other.entry.sort_key
    }
}

impl Eq for HeapEntryWrapper {}

impl PartialOrd for HeapEntryWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntryWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        // Inverted comparison for max-heap behavior
        // We want the "largest" (according to sort order) element at the top
        // so we can efficiently evict it when heap exceeds limit
        for (i, sort_expr) in self.order_by.iter().enumerate() {
            let a_val = &self.entry.sort_key[i];
            let b_val = &other.entry.sort_key[i];

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
                // Note: inverted for max-heap
                return if sort_expr.asc {
                    cmp.reverse() // Largest (worst) at top
                } else {
                    cmp // Smallest (worst for DESC) at top
                };
            }
        }

        Ordering::Equal
    }
}

/// TopN executor using a bounded max-heap.
///
/// Algorithm:
/// 1. Maintain a max-heap of size `limit + offset`
/// 2. For each input row:
///    - If heap size < limit: push row
///    - Else if row is "better" than heap top: pop top and push row
/// 3. After all input consumed, pop and collect results
/// 4. Skip `offset` rows and return the rest in order
pub(crate) struct TopN {
    op: TopNOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,

    /// Bounded heap for finding top N elements.
    heap: BinaryHeap<HeapEntryWrapper>,

    /// Maximum heap size (limit + offset).
    heap_capacity: usize,

    /// Output buffer (sorted results).
    output: Vec<Row>,

    /// Current position in output buffer.
    current_idx: usize,

    /// Whether build phase is complete.
    build_complete: bool,
}

impl TopN {
    pub(crate) fn new(op: TopNOp, child: Box<dyn Executor>) -> Self {
        let heap_capacity = op.limit + op.offset.unwrap_or(0);
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            heap: BinaryHeap::with_capacity(heap_capacity + 1),
            heap_capacity,
            output: Vec::new(),
            current_idx: 0,
            build_complete: false,
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

    /// Check if a row should be added to the heap.
    fn should_add(&self, entry: &HeapEntryWrapper) -> bool {
        if self.heap.len() < self.heap_capacity {
            return true;
        }

        // Compare with the worst element in heap (at the top of max-heap)
        if let Some(worst) = self.heap.peek() {
            // entry < worst means entry is "better" (should be in result)
            entry < worst
        } else {
            true
        }
    }

    /// Build phase: consume input and maintain bounded heap.
    fn build(&mut self) -> ExecutionResult<()> {
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let sort_key = self.extract_sort_key(&row)?;
            let entry = HeapEntry { row, sort_key };
            let wrapper = HeapEntryWrapper {
                entry,
                order_by: self.op.order_by.clone(),
            };

            if self.should_add(&wrapper) {
                self.heap.push(wrapper);

                // Maintain heap size
                if self.heap.len() > self.heap_capacity {
                    self.heap.pop();
                }
            }
        }

        // Extract results in sorted order
        let mut results: Vec<HeapEntryWrapper> = self.heap.drain().collect();

        // Sort in correct order (heap gives us reverse order)
        results.sort_by(|a, b| {
            // Use normal comparison (not inverted)
            for (i, sort_expr) in a.order_by.iter().enumerate() {
                let a_val = &a.entry.sort_key[i];
                let b_val = &b.entry.sort_key[i];

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
        });

        // Skip offset rows and collect the rest
        let offset = self.op.offset.unwrap_or(0);
        self.output = results
            .into_iter()
            .skip(offset)
            .take(self.op.limit)
            .map(|w| w.entry.row)
            .collect();

        self.current_idx = 0;
        self.build_complete = true;

        Ok(())
    }
}

impl Executor for TopN {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.heap.clear();
        self.output.clear();
        self.current_idx = 0;
        self.build_complete = false;

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

        if self.current_idx < self.output.len() {
            let row = self.output[self.current_idx].clone();
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
        self.heap.clear();
        self.output.clear();
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.schema
    }
}
