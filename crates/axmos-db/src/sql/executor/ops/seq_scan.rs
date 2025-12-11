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

pub(crate) struct SeqScan {
    op: SeqScanOp,
    ctx: ExecutionContext,
    state: ExecutionState,
    cursor: Option<BPlusTreePositionIterator<NumericComparator>>,
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

    fn tuple_to_row(&self, bytes: &[u8]) -> ExecutionResult<Option<Row>> {
        let maybe_tuple =
            TupleRef::read_for_snapshot(&bytes, &self.op.table_schema, self.ctx.snapshot())?;

        if maybe_tuple.is_none() {
            return Ok(None);
        };

        let tuple = maybe_tuple.unwrap();
        let mut row = Row::new();

        if let Some(ref columns) = self.op.columns {
            for &col_idx in columns {
                let num_keys = self.op.table_schema.num_keys as usize;
                let value = if col_idx < num_keys {
                    tuple.key(col_idx)?
                } else {
                    tuple.value(col_idx - num_keys)?
                };
                row.push(value.to_owned());
            }
        } else {
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

        if !self.evaluate_predicate(&row)? {
            return Ok(None);
        }

        Ok(Some(row))
    }

    fn evaluate_predicate(&self, row: &Row) -> EvalResult<bool> {
        match &self.op.predicate {
            None => Ok(true),
            Some(pred) => {
                let evaluator = ExpressionEvaluator::new(row, &self.op.output_schema);
                evaluator.evaluate_as_bool(pred.clone())
            }
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for SeqScan {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        let relation = self
            .ctx
            .catalog()
            .get_relation_unchecked(self.op.table_id, self.ctx.accessor().clone())?;

        let root_page = relation.root();
        let mut table = self.ctx.table(root_page)?;

        let iterator = table.iter_positions()?;

        self.cursor = Some(iterator);
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

        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                let next_pos = match cursor.next() {
                    Some(res) => res?,
                    None => {
                        self.state = ExecutionState::Closed;
                        return Ok(None);
                    }
                };

                self.stats.rows_scanned += 1;

                let tree = cursor.get_tree();
                let maybe_row = tree
                    .with_cell_at(next_pos, |bytes| self.tuple_to_row(bytes))?
                    .and_then(|result| Ok(result))?;

                if maybe_row.is_none() {
                    continue;
                }

                self.stats.rows_produced += 1;
                return Ok(Some(maybe_row.unwrap()));
            } else {
                return Err(QueryExecutionError::InvalidState(self.state));
            }
        }
    }

    fn close(&mut self) -> ExecutionResult<()> {
        self.state = ExecutionState::Closed;
        if let Some(iterator) = self.cursor.as_mut() {
            iterator.get_tree().clear_accessor_stack();
        };

        self.cursor = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.output_schema
    }
}
