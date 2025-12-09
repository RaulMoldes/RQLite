// src/sql/executor/ops/insert.rs
//! Insert executor that inserts rows into a table.

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::{ObjectType, Schema},
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::PhysInsertOp,
    },
    storage::tuple::{OwnedTuple, Tuple},
    transactions::LogicalId,
    types::{DataType, DataTypeKind,  UInt64},
};

/// Insert operator that takes rows from a child executor and inserts them into a table.
pub(crate) struct Insert {
    /// Physical operator definition
    op: PhysInsertOp,
    /// Execution context
    ctx: ExecutionContext,
    /// Child executor providing rows to insert
    child: Box<dyn Executor>,
    /// Current execution state
    state: ExecutionState,
    /// Execution statistics
    stats: ExecutionStats,
    /// Number of rows inserted (returned as result)
    rows_affected: u64,
    /// Whether we've returned the result row
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
            rows_affected: 0,
            returned_result: false,
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Insert a single row into the table
    fn insert_row(&mut self, row: &Row) -> ExecutionResult<LogicalId> {
        let catalog = self.ctx.catalog();
        let worker = self.ctx.worker().clone();
        let tx_id = self.ctx.transaction_id();

        // Get the relation
        let mut relation = catalog.get_relation_unchecked(self.op.table_id, worker.clone())?;

        // Get next row ID
        let next_row = if let crate::database::schema::Relation::TableRel(ref mut tab) = relation {
            let next = tab.get_next();
            tab.add_row();
            next
        } else {
            return Err(QueryExecutionError::InvalidObjectType(ObjectType::Table));
        };

        let schema = relation.schema();
        let schema_columns = schema.columns();

        // Build the full row with row_id prepended
        // Schema is: [row_id, col1, col2, ...]
        // op.columns contains indices into the non-row_id columns (1-based in schema)
        let mut full_values: Vec<DataType> = vec![DataType::Null; schema_columns.len()];

        // Set row_id at position 0
        full_values[0] = DataType::BigUInt(next_row);

        // Map input columns to table columns with type casting
        for (input_idx, &schema_col_idx) in self.op.columns.iter().enumerate() {
            if input_idx < row.len() && schema_col_idx < schema_columns.len() {
                let expected_type = schema_columns[schema_col_idx].dtype;
                let value = row[input_idx].clone();
                let casted_value = Self::cast_to_column_type(value, expected_type)?;
                full_values[schema_col_idx] = casted_value;
            }
        }


        let logical_id = LogicalId::new(self.op.table_id, next_row);

        // Create tuple with transaction ID as xmin
        let tuple = Tuple::new(&full_values, schema, tx_id)?;
        let bytes: OwnedTuple = tuple.into();

        // Get btree and insert
        let mut btree = catalog.table_btree(relation.root(), worker.clone())?;
        btree.insert(relation.root(), bytes.as_ref())?;
        btree.clear_worker_stack();

        // Handle index updates
        let indexed_cols = schema.get_indexed_columns();
        for (col_idx, idx_info) in indexed_cols {
            if col_idx < full_values.len() {
                let index_relation = catalog.get_relation(idx_info.name(), worker.clone())?;
                let mut index_btree = catalog.index_btree(
                    index_relation.root(),
                    idx_info.datatype(),
                    worker.clone(),
                )?;

                let indexed_value = full_values[col_idx].clone();
                let index_schema = index_relation.schema();
                let index_tuple = Tuple::new(
                    &[indexed_value, DataType::BigUInt(next_row)],
                    index_schema,
                    tx_id,
                )?;
                let index_bytes: OwnedTuple = index_tuple.into();

                index_btree.insert(index_relation.root(), index_bytes.as_ref())?;
                index_btree.clear_worker_stack();
            }
        }

        // Update relation in catalog
        catalog.update_relation(relation, worker)?;

        Ok(logical_id)
    }


     /// Cast a value to the expected column type if needed
    fn cast_to_column_type(
        value: DataType,
        expected_kind: DataTypeKind,
    ) -> ExecutionResult<DataType> {
        // If already the correct type, return as-is
        if value.kind() == expected_kind {
            return Ok(value);
        }

        // Try to cast to the expected type
        value.try_cast(expected_kind).map_err(|e| {
            QueryExecutionError::Type(TypeError::CastError { from: value.kind(), to: expected_kind })
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
        self.rows_affected = 0;
        self.returned_result = false;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        // If we haven't processed all input rows yet, do so
        if !self.returned_result {
            // Process all rows from child
            while let Some(row) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.insert_row(&row)?;
                self.rows_affected += 1;
                self.stats.rows_produced += 1;
            }

            self.returned_result = true;

            // Return a row with the count of affected rows
            let mut result = Row::new();
            result.push(DataType::BigUInt(UInt64::from(self.rows_affected)));
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
