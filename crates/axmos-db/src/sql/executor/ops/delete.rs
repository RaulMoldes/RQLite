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
            ops::{capture_tuple_values_unchecked, get_row_id, rebuild_index_entry},
        },
        planner::physical::PhysDeleteOp,
    },
    storage::tuple::{Tuple, TupleRef},
    structures::{
        bplustree::SearchResult,
        builder::{AsKeyBytes, IntoCell},
    },
    transactions::LogicalId,
    types::{DataType, UInt64},
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

    /// Maintain indexes (search, mark as deleted, update).
    /// We do not remove the full index entry because it would fuck up other transactions.
    /// Instead, we mark the tuple at the target position in the index as deleted as we do with standard data updates.
    fn maintain_indexes(
        &self,
        values: Vec<DataType>,
        schema: &Schema,
        row_id: UInt64,
    ) -> ExecutionResult<()> {
        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor().clone();
        let tx_id = self.ctx.transaction_id();
        let snapshot = self.ctx.snapshot();
        let indexes = schema.get_indexes();

        for (index_name, columns) in indexes {
            let index_relation = catalog.get_relation(&index_name, accessor.clone())?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            let mut index_btree =
                catalog.index_btree(index_root, index_schema, accessor.clone())?;

            // Build index key for search
            let index_key = rebuild_index_entry(&values, &columns, index_schema, row_id, tx_id)?;

            // Search for index entry
            let search_result =
                index_btree.search_from_root(index_key.key_bytes(), FrameAccessMode::Write)?;

            if let SearchResult::Found(pos) = search_result {
                // Mark as deleted and update
                let deleted_index_result: ExecutionResult<Tuple> =
                    index_btree.with_cell_at(pos, |bytes| {
                        let mut index_tuple = Tuple::try_from((bytes, index_schema))?;
                        index_tuple.delete(tx_id)?;
                        Ok(index_tuple)
                    })?;
                let deleted_index = deleted_index_result?;

                index_btree.clear_accessor_stack();
                index_btree.update(index_root, deleted_index)?;
            } else {
                index_btree.clear_accessor_stack();
            }
        }

        Ok(())
    }

    /// Delete a single row by searching for it by row_id (primary key)
    fn delete_row(&mut self, row: &Row) -> ExecutionResult<()> {
        let row_id = get_row_id(row)?;
        let row_id_key = row_id.as_key_bytes();

        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor();
        let tx_id = self.ctx.transaction_id();
        let snapshot = self.ctx.snapshot();

        let relation = catalog.get_relation_unchecked(self.op.table_id, accessor.clone())?;
        let schema = relation.schema();
        let root = relation.root();

        let mut btree = catalog.table_btree(root, accessor.clone())?;
        let logical_id = LogicalId::new(self.op.table_id, row_id);

        let search_result = btree.search_from_root(row_id_key, FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_accessor_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        // Mark tuple as deleted and update
        let deleted_result: ExecutionResult<(Box<[u8]>, Tuple)> =
            btree.with_cell_at(position, |bytes| {
                if let Some(old_tuple) = TupleRef::read_for_snapshot(bytes, schema, snapshot)? {
                    let mut tuple = Tuple::try_from((bytes, schema))?;
                    tuple.delete(tx_id)?;
                    let old_data = bytes.to_vec().into_boxed_slice();
                    return Ok((old_data, tuple));
                }
                return Err(QueryExecutionError::AlreadyDeleted(logical_id));
            })?;

        let (old_bytes, deleted) = deleted_result?;

        let old_values = capture_tuple_values_unchecked(old_bytes, schema, snapshot)?;

        //  let logger = self.ctx.logger();
        //  logger.log_delete(self.op.table_id, old_bytes)?; (THIS MUST ACTUALLY BE LOGGED AS AN UPDATE).

        btree.clear_accessor_stack();
        btree.update(root, deleted)?;

        self.maintain_indexes(old_values, schema, row_id)?;

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
