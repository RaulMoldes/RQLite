use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeError, RuntimeResult,
        context::TransactionContext,
    },
    schema::{base::IndexHandle, catalog::CatalogTrait},
    sql::planner::physical::PhysInsertOp,
    storage::tuple::{Row, TupleBuilder},
    tree::accessor::TreeWriter,
    types::{DataType, RowId, UInt64},
};

pub(crate) struct OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    op: PhysInsertOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
    returned_result: bool,
}

pub(crate) struct ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    op: PhysInsertOp,
    ctx: TransactionContext<Acc>,
    child: Child,
    stats: ExecutionStats,
}

impl<Acc, Child> ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    pub(crate) fn new(
        op: PhysInsertOp,
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

impl<Acc, Child> OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn insert_row(&mut self, row: &Row) -> RuntimeResult<RowId> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let tid = self.ctx.tid();

        // Get the relation
        let mut relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(self.op.table_id, &tree_builder, &snapshot)?;

        // Get next row id and increment
        let next_row_id = relation.next_row_id();
        relation.increment_row_id();

        let schema = relation.schema();
        let num_keys = schema.num_keys();
        let num_cols = schema.num_columns();

        // Build full row with default values
        let mut full_values: Vec<DataType> = vec![DataType::Null; num_cols];
        full_values[0] = DataType::BigUInt(next_row_id);

        // Map input columns to schema positions
        for (input_idx, &schema_col_idx) in self.op.columns.iter().enumerate() {
            if input_idx < row.len() && schema_col_idx < num_cols {
                let expected_type = schema
                    .column(schema_col_idx)
                    .ok_or(RuntimeError::ColumnNotFound(schema_col_idx))?
                    .datatype();
                let casted = row[input_idx].try_cast(expected_type)?;
                full_values[schema_col_idx] = casted;
            }
        }

        // Create the row and build the tuple
        let full_row = Row::new(full_values.clone().into_boxed_slice());
        let tuple_builder = TupleBuilder::from_schema(schema);
        let tuple = tuple_builder.build(full_row, tid)?;

        // Serialize tuple for WAL
        let tuple_bytes = Box::from(&tuple);

        // Log the insert operation
        let row_id = next_row_id.value();
        self.ctx.log_insert(self.op.table_id, row_id, tuple_bytes)?;

        // Insert into main table btree
        let root = relation.root();

        let mut btree = self.ctx.build_tree(root);
        btree.insert(root, tuple, &schema)?;
        btree.accessor_mut()?.clear();

        // Maintain secondary indexes
        let indexes = relation.get_indexes();
        for index in indexes {
            let index_relation =
                self.ctx
                    .catalog()
                    .read()
                    .get_relation(index.id(), &tree_builder, &snapshot)?;
            let index_schema = index_relation.schema();
            // Build index entry: [indexed_cols..., row_id]
            let index_row =
                build_index_entry(&full_values, &index, index_schema, next_row_id.value())?;

            let index_builder = TupleBuilder::from_schema(index_schema);
            let index_tuple = index_builder.build(index_row, tid)?;

            let index_root = index_relation.root();

            let mut index_btree = self.ctx.build_tree(index_root);
            index_btree.insert(index_root, index_tuple, &index_schema)?;
            index_btree.accessor_mut()?.clear();
        }

        // Update relation metadata in catalog
        self.ctx
            .catalog()
            .write()
            .update_relation(relation, &tree_builder, &snapshot)?;

        Ok(row_id)
    }
}

impl<Acc, Child> ClosedExecutor for ClosedInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: ClosedExecutor,
{
    type Running = OpenInsert<Acc, Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child_running = self.child.open()?;
        Ok(OpenInsert {
            op: self.op,
            ctx: self.ctx,
            child: child_running,
            stats: self.stats,
            returned_result: false,
        })
    }
}

impl<Acc, Child> RunningExecutor for OpenInsert<Acc, Child>
where
    Acc: TreeWriter + Clone + Default,
    Child: RunningExecutor,
{
    type Closed = ClosedInsert<Acc, Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.returned_result {
            return Ok(None);
        }

        // Process all rows from child
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;
            self.insert_row(&row)?;
            self.stats.rows_produced += 1;
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
        Ok(ClosedInsert::new(
            self.op,
            self.ctx,
            child_closed,
            Some(self.stats),
        ))
    }
}
