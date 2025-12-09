// src/sql/executor/operators/seq_scan.rs
use crate::{
    database::{
        errors::{EvalResult, ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        executor::{
            ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext,
            eval::ExpressionEvaluator,
        },
        planner::physical::SeqScanOp,
    },
    storage::tuple::TupleRef,
    structures::{bplustree::BPlusTreePositionIterator, comparator::NumericComparator},
};

/// Sequential scan executor (iterates over all visible tuples in a table).
///
/// This executor performs a full table scan using the B+tree leaf page linked list.
/// It respects MVCC visibility rules through the transaction's snapshot.
pub(crate) struct SeqScan {
    /// Physical operator definition
    op: SeqScanOp,
    /// Execution context with worker, catalog, and snapshot
    ctx: ExecutionContext,
    /// Current scan state
    state: ExecutionState,
    /// Btree cursor (unset if not opened)
    cursor: Option<BPlusTreePositionIterator<NumericComparator>>,
    /// Execution statistics
    stats: ExecutionStats,
}

impl SeqScan {
    pub(crate) fn new(op: SeqScanOp, ctx: ExecutionContext) -> Self {
        Self {
            op,
            ctx,
            state: ExecutionState::Closed,
            cursor: None,
            stats: ExecutionStats::default(),
        }
    }

    /// Convert a TupleRef to a Row, applying projection if specified
    fn tuple_to_row(&self, bytes: &[u8]) -> ExecutionResult<Option<Row>> {
        let maybe_tuple =
            TupleRef::read_for_snapshot(&bytes, &self.op.table_schema, self.ctx.snapshot())?;

        if maybe_tuple.is_none() {
            return Ok(None);
        };

        let tuple = maybe_tuple.unwrap();
        let mut row = Row::new();

        if let Some(ref columns) = self.op.columns {
            // Project specific columns - indices are into table schema
            for &col_idx in columns {
                // Determine if this is a key or value column
                let num_keys = self.op.table_schema.num_keys as usize;
                let value = if col_idx < num_keys {
                    tuple.key(col_idx)?
                } else {
                    tuple.value(col_idx - num_keys)?
                };
                row.push(value.to_owned());
            }
        } else {
            // Return all columns
            let num_keys = self.op.table_schema.num_keys as usize;
            for i in 0..num_keys {
                let value = tuple.key(i)?;
                row.push(value.to_owned());
            }

            let num_values = self.op.table_schema.values().len();
            for i in 0..num_values {
                let value = tuple.value(i)?;
                row.push(value.to_owned());
            }
        }

        // Apply predicate filter if present
        // NOTE: Predicate column indices must already be remapped to output schema.
        // See ProjectionPushdown in [rules.rs] (optimizer module)
        if !self.evaluate_predicate(&row)? {
            return Ok(None);
        }

        Ok(Some(row))
    }

    /// Evaluate a filter predicate against a row
    /// Evaluate the predicate against a row.
    /// Returns true if the row passes the filter (or if there's no predicate).
    fn evaluate_predicate(&self, row: &Row) -> EvalResult<bool> {
        match &self.op.predicate {
            None => Ok(true),
            Some(pred) => {
                let evaluator = ExpressionEvaluator::new(row, &self.op.output_schema);
                evaluator.evaluate_as_bool(pred.clone())
            }
        }
    }

    /// Get current execution statistics
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for SeqScan {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }
        // Get table root page from catalog
        let relation = self
            .ctx
            .catalog()
            .get_relation_unchecked(self.op.table_id, self.ctx.worker().clone())?;

        let root_page = relation.root();
        let mut table = self.ctx.table(root_page)?;
        let iterator = table.iter_positions()?;
        self.cursor = Some(iterator);
        self.state = ExecutionState::Open;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                // Try to advance the cursor
                let next_pos = match cursor.next() {
                    Some(res) => res?, // propagate io::Error
                    None => {
                        // end of scan → close and return None
                        self.state = ExecutionState::Closed;
                        return Ok(None);
                    }
                };

                self.stats.rows_scanned += 1;

                // Access tuple at this position
                let tree = cursor.get_tree();
                let maybe_row = tree
                    .with_cell_at(next_pos, |bytes| self.tuple_to_row(bytes))?
                    .and_then(|result| Ok(result))?;

                // Row filtered out
                if maybe_row.is_none() {
                    continue;
                }

                // Row is produced
                self.stats.rows_produced += 1;
                return Ok(maybe_row);
            } else {
                // Cannot be open or running without an iterator.
                return Err(QueryExecutionError::InvalidState(self.state));
            }
        }
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }

        self.state = ExecutionState::Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.output_schema
    }
}
