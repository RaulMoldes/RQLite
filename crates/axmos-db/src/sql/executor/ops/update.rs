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

    /// Update a single row by searching for it by row_id (primary key)
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
        let worker = self.ctx.worker().clone();
        let tx_id = self.ctx.transaction_id();
        let snapshot = self.ctx.snapshot();

        let relation = catalog.get_relation_unchecked(self.op.table_id, worker.clone())?;
        let schema = relation.schema();
        let root = relation.root();

        let mut btree = catalog.table_btree(root, worker.clone())?;
        let logical_id = LogicalId::new(self.op.table_id, row_id);

        // Search by row_id key - tree structure may have changed since materialization
        let search_result = btree.search_from_root(row_id.as_ref(), FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_worker_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        let updates = self.compute_updates(row, schema)?;

        // Read current tuple at found position
        let payload_result: ExecutionResult<OwnedTuple> =
            btree.with_cell_at(position, |bytes| {
                let mut tuple = Tuple::try_from((bytes, schema))?;
                tuple.add_version(&updates, tx_id)?;
                let new_bytes: OwnedTuple = tuple.into();
                Ok(new_bytes)
            })?;

        let payload = payload_result?;
        btree.clear_worker_stack();

        // Update in B-tree
        btree.update(root, payload.as_ref())?;

        // Handle index updates
        let indexed_cols = schema.get_indexed_columns();
        for (col_idx, idx_info) in &indexed_cols {
            if let Some((_, new_value)) = updates.iter().find(|(idx, _)| idx == col_idx) {
                let index_relation = catalog.get_relation(idx_info.name(), worker.clone())?;
                let mut index_btree = catalog.index_btree(
                    index_relation.root(),
                    idx_info.datatype(),
                    worker.clone(),
                )?;

                let index_schema = index_relation.schema();
                let new_index_tuple = Tuple::new(
                    &[new_value.clone(), DataType::BigUInt(row_id)],
                    index_schema,
                    tx_id,
                )?;
                let new_index_bytes: OwnedTuple = new_index_tuple.into();
                index_btree.insert(index_relation.root(), new_index_bytes.as_ref())?;
                index_btree.clear_worker_stack();
            }
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
