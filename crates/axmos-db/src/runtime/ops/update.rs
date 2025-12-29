use crate::{
    core::SerializableType,
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeError, RuntimeResult,
        TypeSystemError, context::TransactionContext, eval::ExpressionEvaluator,
    },
    schema::{Schema, base::IndexHandle, catalog::CatalogTrait},
    sql::planner::physical::PhysUpdateOp,
    storage::tuple::{Row, Tuple, TupleBuilder, TupleReader},
    tree::{accessor::TreeWriter, bplustree::SearchResult},
    types::{DataType, RowId, UInt64},
};

pub(crate) struct OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    op: PhysUpdateOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

pub(crate) struct ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    op: PhysUpdateOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
}

impl<Acc, Child> ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    pub(crate) fn new(
        op: PhysUpdateOp,
        ctx: TransactionContext<Acc>,
        child: Child,
        stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            op,
            ctx,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

/// Build an index entry from the given handle
fn build_index_entry(
    input_values: &[DataType],
    index: &IndexHandle,
    index_schema: &crate::schema::Schema,
    row_id: RowId,
) -> RuntimeResult<Row> {
    let mut index_entry: Vec<DataType> = Vec::with_capacity(index_schema.num_keys() + 1);
    for col_idx in index.indexed_column_ids().iter() {
        let value = input_values
            .get(*col_idx)
            .ok_or(RuntimeError::ColumnNotFound(*col_idx))?;
        index_entry.push(value.clone());
    }
    index_entry.push(DataType::BigUInt(UInt64(row_id)));

    Ok(Row::new(index_entry.into_boxed_slice()))
}

impl<Acc, Child> OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    pub(crate) fn get_row_id_checked(row: &Row) -> RuntimeResult<&UInt64> {
        let row_id = row[0].as_big_u_int().ok_or(RuntimeError::TypeError(
            TypeSystemError::UnexpectedDataType(row[row.len() - 1].kind()),
        ))?;
        Ok(row_id)
    }

    /// Maintain indexes (search, mark as deleted, update).
    /// We do not remove the full index entry because it would fuck up other transactions.
    /// Instead, we mark the tuple at the target position in the index as deleted as we do with standard data updates.
    fn maintain_indexes(
        &mut self,
        values: &[DataType],
        schema: &Schema,
        row_id: RowId,
    ) -> RuntimeResult<()> {
        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor().clone();
        let tid = self.ctx.tid();
        let snapshot = self.ctx.snapshot();
        let indexes = schema.get_indexes();
        let builder = self.ctx.tree_builder();
        for index in indexes {
            let index_relation = catalog.get_relation(index.id(), &builder, &snapshot)?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            let mut index_btree = self.ctx.build_tree(index_root);

            // Build index key for search
            let index_row = build_index_entry(&values, &index, index_schema, row_id)?;
            let index_builder = TupleBuilder::from_schema(index_schema);
            let index_tuple = index_builder.build(index_row, tid)?;

            // Search for index entry
            let search_result = index_btree.search_tuple(&index_tuple, &index_schema)?;

            if let SearchResult::Found(pos) = search_result {
                // Mark as deleted and update
                let tuple_reader = TupleReader::from_schema(index_schema);
                let old_bytes = index_btree.with_cell_at(pos, |bytes| {
                    if let Some(tuple_reference) =
                        tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()?
                    {
                        let mut tuple = Tuple::from_slice_unchecked(bytes).ok()?;
                        tuple.delete(tid).ok()?;
                        Some(tuple)
                    } else {
                        None
                    }
                })?;

                let Some(tuple) = old_bytes else {
                    continue;
                };

                index_btree.update(index_root, tuple, &index_schema)?;
            }
        }

        self.ctx.accessor_mut().clear();

        Ok(())
    }

    fn evaluate_assignments(
        &self,
        row: &Row,
        schema: &Schema,
    ) -> RuntimeResult<Box<[(usize, DataType)]>> {
        let mut assigned = Vec::with_capacity(self.op.assignments.len());
        let evaluator = ExpressionEvaluator::new(row, schema);
        for (index, expr) in self.op.assignments.iter() {
            let evaluated = evaluator.evaluate(&expr)?;
            let expected_type = schema
                .column(*index)
                .ok_or(RuntimeError::ColumnNotFound(*index))?
                .datatype();
            let casted = evaluated.try_cast(expected_type)?;
            assigned.push((*index, casted));
        }
        Ok(assigned.into_boxed_slice())
    }

    /// Delete a single row by searching for it by row_id (primary key)
    fn update_row(&mut self, row: &Row) -> RuntimeResult<bool> {
        let row_id = Self::get_row_id_checked(row)?;
        let row_id_bytes = row_id.serialize()?;

        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor();
        let tid = self.ctx.tid();
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        let relation = catalog.get_relation(self.op.table_id, &tree_builder, &snapshot)?;
        let schema = relation.schema();
        let root = relation.root();

        let mut btree = self.ctx.build_tree(root);
        let search_result = btree.search(&row_id_bytes, &schema)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(false),
        };

        let tuple_reader = TupleReader::from_schema(schema);
        let old_bytes = btree.with_cell_at(position, |bytes| {
            if let Some(tuple_reference) = tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()? {
                Some(Tuple::from_slice_unchecked(bytes).ok()?)
            } else {
                None
            }
        })?;

        let Some(tuple) = old_bytes else {
            self.ctx.accessor_mut().clear();
            return Ok(false); // Tuple is not visible. Cannot delete or make changes to it.
        };
        let mut updated = tuple.clone();

        let old_row = btree.get_row_at(position, schema, &snapshot)?.unwrap();

        let assigned = self.evaluate_assignments(&old_row, schema)?;
        updated.add_version_with_schema(&assigned, tid, schema)?;

        self.ctx.log_update(
            self.op.table_id,
            row_id.value(),
            Box::from(&tuple),
            Box::from(&updated),
        )?;

        btree.update(root, updated, &schema)?;

        self.maintain_indexes(&old_row.into_inner(), schema, row_id.value())?;

        Ok(true)
    }
}

impl<Acc, Child> ClosedExecutor for ClosedUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    type Running = OpenUpdate<Acc, Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child_running = self.child.open()?;
        Ok(OpenUpdate {
            op: self.op,
            ctx: self.ctx,
            child: child_running,
            stats: self.stats,
            returned_result: false,
        })
    }
}

impl<Acc, Child> RunningExecutor for OpenUpdate<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    type Closed = ClosedUpdate<Acc, Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;
            if self.update_row(&row)? {
                self.stats.rows_produced += 1;
            };
        }

        self.returned_result = true;

        // Return count of inserted rows
        let result = Row::new(Box::new([DataType::BigUInt(UInt64::from(
            self.stats.rows_produced,
        ))]));

        Ok(Some(result))
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        let child_closed = self.child.close()?;
        Ok(ClosedUpdate::new(
            self.op,
            self.ctx,
            child_closed,
            Some(self.stats),
        ))
    }
}
