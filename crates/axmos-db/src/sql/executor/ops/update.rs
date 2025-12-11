use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    io::frames::FrameAccessMode,
    sql::{
        executor::{
            ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext,
            eval::ExpressionEvaluator,
        },
        planner::physical::PhysUpdateOp,
    },
    storage::tuple::{OwnedTuple, Tuple},
    structures::bplustree::SearchResult,
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
            let casted_value = Self::cast_to_column_type(new_value, expected_type)?;

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

    fn update_row(&mut self, row: &Row) -> ExecutionResult<()> {
        let row_id = match &row[0] {
            DataType::BigUInt(id) => *id,
            _ => {
                return Err(QueryExecutionError::Type(TypeError::TypeMismatch {
                    left: row[0].kind(),
                    right: DataTypeKind::BigUInt,
                }));
            }
        };

        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor().clone();
        let tx_id = self.ctx.transaction_id();

        let relation = catalog.get_relation_unchecked(self.op.table_id, accessor.clone())?;
        let schema = relation.schema();
        let root = relation.root();

        let mut btree = catalog.table_btree(root, accessor.clone())?;
        let logical_id = LogicalId::new(self.op.table_id, row_id);

        let search_result = btree.search_from_root(row_id.as_ref(), FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_accessor_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        let updates = self.compute_updates(row, schema)?;

        // Read current tuple, capture old values, apply updates
        let result: ExecutionResult<(OwnedTuple, Vec<DataType>, Vec<DataType>)> = btree
            .with_cell_at(position, |bytes| {
                let mut tuple = Tuple::try_from((bytes, schema))?;
                let old_vals: Vec<DataType> = tuple.values().to_vec();
                tuple.add_version(&updates, tx_id)?;
                let new_vals: Vec<DataType> = tuple.values().to_vec();
                let new_bytes: OwnedTuple = tuple.into();
                Ok((new_bytes, old_vals, new_vals))
            })?;
        let (payload, old_values, new_values) = result?;
        btree.clear_accessor_stack();
        btree.update(root, payload.as_ref())?;

        // Maintain indexes
        let indexes = schema.get_indexes();

        for (index_name, columns) in indexes {
            // Check if any column in this index was updated
            let index_affected = columns
                .iter()
                .any(|(col_idx, _)| updates.iter().any(|(upd_idx, _)| upd_idx == col_idx));

            if !index_affected {
                continue;
            }

            let index_relation = catalog.get_relation(&index_name, accessor.clone())?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            let mut index_btree =
                catalog.index_btree(index_root, index_schema, accessor.clone())?;

            // Build old index key for search
            let mut old_index_key: Vec<u8> = Vec::new();
            for (col_idx, _dtype) in &columns {
                old_index_key.extend_from_slice(old_values[*col_idx].as_ref());
            }
            old_index_key.extend_from_slice(row_id.as_ref());

            // Search for old index entry, mark as deleted, update
            let search_result =
                index_btree.search_from_root(&old_index_key, FrameAccessMode::Write)?;

            if let SearchResult::Found(pos) = search_result {
                let deleted_result: ExecutionResult<OwnedTuple> =
                    index_btree.with_cell_at(pos, |bytes| {
                        let mut index_tuple = Tuple::try_from((bytes, index_schema))?;
                        index_tuple.delete(tx_id)?;
                        let owned: OwnedTuple = index_tuple.into();
                        Ok(owned)
                    })?;

                let deleted_bytes = deleted_result?;

                index_btree.clear_accessor_stack();
                index_btree.update(index_root, deleted_bytes.as_ref())?;
            } else {
                index_btree.clear_accessor_stack();
            }

            // Insert new index entry
            let mut new_index_values: Vec<DataType> = Vec::with_capacity(columns.len() + 1);
            for (col_idx, _dtype) in &columns {
                new_index_values.push(new_values[*col_idx].clone());
            }
            new_index_values.push(DataType::BigUInt(row_id));

            let new_index_tuple = Tuple::new(&new_index_values, index_schema, tx_id)?;
            let new_index_bytes: OwnedTuple = new_index_tuple.into();

            index_btree.insert(index_root, new_index_bytes.as_ref())?;
            index_btree.clear_accessor_stack();
        }

        Ok(())
    }

    fn cast_to_column_type(
        value: DataType,
        expected_kind: DataTypeKind,
    ) -> ExecutionResult<DataType> {
        if value.kind() == expected_kind {
            return Ok(value);
        }
        value.try_cast(expected_kind).map_err(|_| {
            QueryExecutionError::Type(TypeError::CastError {
                from: value.kind(),
                to: expected_kind,
            })
        })
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
