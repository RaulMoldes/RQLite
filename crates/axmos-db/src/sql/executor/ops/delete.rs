use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    io::frames::FrameAccessMode,
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::PhysDeleteOp,
    },
    storage::tuple::{OwnedTuple, Tuple, TupleRef},
    structures::bplustree::SearchResult,
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

pub(crate) struct Delete {
    op: PhysDeleteOp,
    ctx: ExecutionContext,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,
    returned_result: bool,
}

impl Delete {
    pub(crate) fn new(op: PhysDeleteOp, ctx: ExecutionContext, child: Box<dyn Executor>) -> Self {
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

    pub(crate) fn get_row_id(row: &Row) -> ExecutionResult<UInt64> {
        let row_id = match &row[0] {
            DataType::BigUInt(id) => *id,
            _ => {
                return Err(QueryExecutionError::Type(TypeError::TypeMismatch {
                    left: row[0].kind(),
                    right: DataTypeKind::BigUInt,
                }));
            }
        };

        Ok(row_id)
    }

    /// Delete a single row by searching for it by row_id (primary key)
    fn delete_row(&mut self, row: &Row) -> ExecutionResult<()> {
        let row_id = Self::get_row_id(row)?;

        let catalog = self.ctx.catalog();
        let worker = self.ctx.worker().clone();
        let tx_id = self.ctx.transaction_id();
        let snapshot = self.ctx.snapshot();

        let relation = catalog.get_relation_unchecked(self.op.table_id, worker.clone())?;
        let schema = relation.schema();
        let root = relation.root();

        let mut btree = catalog.table_btree(root, worker.clone())?;
        let logical_id = LogicalId::new(self.op.table_id, row_id);

        let search_result = btree.search_from_root(row_id.as_ref(), FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_worker_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        // Read current tuple, get values for index maintenance, mark as deleted
        let values: Vec<DataType> = btree.with_cell_at(position, |bytes| {
            let Some(tuple_ref) = TupleRef::read_for_snapshot(bytes, schema, snapshot)? else {
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            };

            if tuple_ref.xmax().is_valid() {
                return Err(QueryExecutionError::AlreadyDeleted(logical_id));
            }

            // Collect values for index deletion
            let mut vals = Vec::with_capacity(schema.values().len());
            for i in 0..schema.values().len() {
                vals.push(tuple_ref.value(i)?.to_owned());
            }

            Ok(vals)
        })??;

        // Mark tuple as deleted and update
        let deleted_result: ExecutionResult<OwnedTuple> =
            btree.with_cell_at(position, |bytes| {
                let mut tuple = Tuple::try_from((bytes, schema))?;
                tuple.delete(tx_id)?;
                Ok(tuple.into())
            })?;

        let deleted_bytes = deleted_result?;

        btree.clear_worker_stack();
        btree.update(root, deleted_bytes.as_ref())?;

        // Maintain indexes - search, mark as deleted, update
        let indexes = schema.get_indexes();

        for (index_name, columns) in indexes {
            let index_relation = catalog.get_relation(&index_name, worker.clone())?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            let mut index_btree = catalog.index_btree(index_root, index_schema, worker.clone())?;

            // Build index key for search
            let mut index_key: Vec<u8> = Vec::new();
            for (col_idx, _dtype) in &columns {
                index_key.extend_from_slice(values[*col_idx].as_ref());
            }
            index_key.extend_from_slice(row_id.as_ref());

            // Search for index entry
            let search_result = index_btree.search_from_root(&index_key, FrameAccessMode::Write)?;

            if let SearchResult::Found(pos) = search_result {
                // Mark as deleted and update
                let deleted_index_bytes_result: ExecutionResult<OwnedTuple> = index_btree
                    .with_cell_at(pos, |bytes| {
                        let mut index_tuple = Tuple::try_from((bytes, index_schema))?;
                        index_tuple.delete(tx_id)?;
                        Ok(index_tuple.into())
                    })?;
                let deleted_index_bytes = deleted_index_bytes_result?;

                index_btree.clear_worker_stack();
                index_btree.update(index_root, deleted_index_bytes.as_ref())?;
            } else {
                index_btree.clear_worker_stack();
            }
        }

        Ok(())
    }
}

impl Executor for Delete {
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
            // Process each row
            while let Some(row) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.delete_row(&row)?;
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
