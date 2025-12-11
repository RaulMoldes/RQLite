use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::{ObjectType, Relation, Schema},
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::PhysInsertOp,
    },
    storage::tuple::{OwnedTuple, Tuple},
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

pub(crate) struct Insert {
    op: PhysInsertOp,
    ctx: ExecutionContext,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,
    returned_result: bool,
}

impl Insert {
    pub(crate) fn new(op: PhysInsertOp, ctx: ExecutionContext, child: Box<dyn Executor>) -> Self {
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

    fn insert_row(&mut self, row: &Row) -> ExecutionResult<LogicalId> {
        let catalog = self.ctx.catalog();
        let accessor = self.ctx.accessor().clone();
        let tx_id = self.ctx.transaction_id();

        let mut relation = catalog.get_relation_unchecked(self.op.table_id, accessor.clone())?;

        let next_row = if let Relation::TableRel(ref mut tab) = relation {
            let next = tab.get_next();
            tab.add_row();
            next
        } else {
            return Err(QueryExecutionError::InvalidObjectType(ObjectType::Table));
        };

        let schema = relation.schema();
        let schema_columns = schema.columns();

        let mut full_values: Vec<DataType> = vec![DataType::Null; schema_columns.len()];
        full_values[0] = DataType::BigUInt(next_row);

        for (input_idx, &schema_col_idx) in self.op.columns.iter().enumerate() {
            if input_idx < row.len() && schema_col_idx < schema_columns.len() {
                let expected_type = schema_columns[schema_col_idx].dtype;
                let value = row[input_idx].clone();
                let casted_value = Self::cast_to_column_type(value, expected_type)?;
                full_values[schema_col_idx] = casted_value;
            }
        }

        let logical_id = LogicalId::new(self.op.table_id, next_row);

        let tuple = Tuple::new(&full_values, schema, tx_id)?;
        let bytes: OwnedTuple = tuple.into();

        let mut btree = catalog.table_btree(relation.root(), accessor.clone())?;
        btree.insert(relation.root(), bytes.as_ref())?;
        btree.clear_accessor_stack();

        // Maintain indexes - insert entries for all composite/single indexes
        let indexes = schema.get_indexes();
        let num_keys = schema.num_keys as usize;

        for (index_name, columns) in indexes {
            let index_relation = catalog.get_relation(&index_name, accessor.clone())?;
            let index_schema = index_relation.schema();

            // Build index tuple: [indexed_col_1, indexed_col_2, ..., row_id]
            let mut index_values: Vec<DataType> = Vec::with_capacity(columns.len() + 1);
            for (col_idx, _dtype) in &columns {
                let full_idx = num_keys + col_idx;
                index_values.push(full_values[full_idx].clone());
            }
            index_values.push(DataType::BigUInt(next_row));

            let index_tuple = Tuple::new(&index_values, index_schema, tx_id)?;
            let index_bytes: OwnedTuple = index_tuple.into();

            let mut index_btree =
                catalog.index_btree(index_relation.root(), index_schema, accessor.clone())?;
            index_btree.insert(index_relation.root(), index_bytes.as_ref())?;
            index_btree.clear_accessor_stack();
        }

        catalog.update_relation(relation, accessor)?;

        Ok(logical_id)
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

impl Executor for Insert {
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
            while let Some(row) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.insert_row(&row)?;
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
