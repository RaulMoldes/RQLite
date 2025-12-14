use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    io::frames::FrameAccessMode,
    sql::{
        executor::{
            ExecutionState, ExecutionStats, Executor, Row,
            context::ExecutionContext,
            eval::ExpressionEvaluator,
            ops::{
                capture_tuple_values_unchecked, cast_to_column_type, get_row_id,
                rebuild_index_entry,
            },
        },
        planner::physical::PhysUpdateOp,
    },
    storage::tuple::{Tuple, TupleRef},
    structures::{
        bplustree::SearchResult,
        builder::{AsKeyBytes, IntoCell},
    },
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

pub(crate) struct Update {
    op: PhysUpdateOp,
    ctx: ExecutionContext,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,
    returned_result: bool,
}

impl Update {
    pub(crate) fn new(op: PhysUpdateOp, ctx: ExecutionContext, child: Box<dyn Executor>) -> Self {
        Self {
            op,
            ctx,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            returned_result: false,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn compute_updates(
        &self,
        row: &Row,
        schema: &Schema,
    ) -> ExecutionResult<Vec<(usize, DataType)>> {
        let schema_columns = schema.columns();
        let num_keys = schema.num_keys as usize;
        // Evaluate assignment expressions using the materialized row
        let child_schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(row, child_schema);
        let mut updates: Vec<(usize, DataType)> = Vec::new();
        for (col_idx, expr) in &self.op.assignments {
            let new_value = evaluator.evaluate(expr.clone())?;
            let expected_type = schema_columns[*col_idx].dtype;
            let casted_value = cast_to_column_type(new_value, expected_type)?;

            if *col_idx >= num_keys {
                let value_idx = *col_idx - num_keys;
                updates.push((value_idx, casted_value));
            } else {
                return Err(QueryExecutionError::Other(
                    "Cannot update key columns".to_string(),
                ));
            }
        }

        Ok(updates)
    }

    #[inline]
    fn index_affected(columns: &[(usize, DataTypeKind)], updates: &[(usize, DataType)]) -> bool {
        columns
            .iter()
            .any(|(col_idx, _)| updates.iter().any(|(upd_idx, _)| upd_idx == col_idx))
    }

    fn maintain_indexes(
        &self,
        schema: &Schema,                 // Updated table schema
        updates: Vec<(usize, DataType)>, // Indexes of the updated columns in the table schema.
        old_values: Vec<DataType>,       // List of old tuple values (in the table, not the index)
        new_values: Vec<DataType>,       // List of new tuple values (in the table, not the indexes)
        row_id: UInt64,                  // Updated row id.
    ) -> ExecutionResult<()> {
        let indexes = schema.get_indexes();
        let accessor = self.ctx.accessor();
        let catalog = self.ctx.catalog();
        let tx_id = self.ctx.transaction_id();
        for (index_name, columns) in indexes {
            if !Self::index_affected(&columns, &updates) {
                continue;
            }

            let index_relation = self
                .ctx
                .catalog()
                .get_relation(&index_name, accessor.clone())?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            let mut index_btree =
                catalog.index_btree(index_root, index_schema, accessor.clone())?;

            // Build old index key for search
            let old_index_entry =
                rebuild_index_entry(&old_values, &columns, index_schema, row_id, tx_id)?;

            // Search for old index entry, mark as deleted, update
            let search_result = index_btree
                .search_from_root(old_index_entry.key_bytes(), FrameAccessMode::Write)?;

            if let SearchResult::Found(pos) = search_result {
                let deleted_result: ExecutionResult<Tuple> =
                    index_btree.with_cell_at(pos, |bytes| {
                        let mut index_tuple = Tuple::try_from((bytes, index_schema))?;
                        index_tuple.delete(tx_id)?;
                        Ok(index_tuple)
                    })?;

                let deleted = deleted_result?;

                index_btree.clear_accessor_stack();
                index_btree.update(index_root, deleted)?;
            } else {
                index_btree.clear_accessor_stack();
            }

            // Insert new index entry
            let new_index_entry =
                rebuild_index_entry(&new_values, &columns, index_schema, row_id, tx_id)?;
            index_btree.insert(index_root, new_index_entry)?;
            index_btree.clear_accessor_stack();
        }

        Ok(())
    }

    fn update_row(&mut self, row: &Row) -> ExecutionResult<()> {
        let row_id = get_row_id(row)?;

        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor().clone();
        let tx_id = self.ctx.transaction_id();

        let logger = self.ctx.logger();

        let relation = catalog.get_relation_unchecked(self.op.table_id, accessor.clone())?;
        let schema = relation.schema();
        let root = relation.root();
        let snapshot = self.ctx.snapshot();

        let mut btree = catalog.table_btree(root, accessor.clone())?;
        let logical_id = LogicalId::new(self.op.table_id, row_id);
        let key_bytes = row_id.as_key_bytes();
        let search_result = btree.search_from_root(key_bytes, FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_accessor_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        let updates = self.compute_updates(row, schema)?;

        // Read current tuple, capture old values, apply updates
        let result: ExecutionResult<(Box<[u8]>, Tuple)> =
            btree.with_cell_at(position, |bytes| {
                if let Some(old_tuple) = TupleRef::read_for_snapshot(bytes, schema, snapshot)? {
                    let mut tuple = Tuple::try_from((bytes, schema))?;
                    let old_bytes = bytes.to_vec().into_boxed_slice();
                    tuple.add_version(&updates, tx_id)?;
                    return Ok((old_bytes, tuple));
                }
                return Err(QueryExecutionError::AlreadyDeleted(logical_id));
            })?;

        let (old, new) = result?;
        btree.clear_accessor_stack();

        let snapshot = self.ctx.snapshot();
        let old_values = capture_tuple_values_unchecked(old, schema, snapshot)?;
        let new_bytes = new.as_bytes();

        let new_values = capture_tuple_values_unchecked(new_bytes, schema, snapshot)?;

        // logger.log_update(self.op.table_id, old_payload, payload.clone())?;

        btree.update(root, new)?;

        // Maintain indexes
        let indexes = schema.get_indexes();

        // TODO: Review this part.

        Ok(())
    }
}

impl Executor for Update {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.returned_result = false;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        if !self.returned_result {
            // Process each row from Materialize (position is ignored)
            while let Some(row) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.update_row(&row)?;
                self.stats.rows_produced += 1;
            }

            self.returned_result = true;

            let mut result = Row::new();
            result.push(DataType::BigUInt(UInt64::from(self.stats.rows_produced)));
            return Ok(Some(result));
        }

        Ok(None)
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }
        self.child.close()?;
        self.state = ExecutionState::Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.table_schema
    }
}
