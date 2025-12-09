// src/sql/executor/ops/update.rs
//! Update executor that modifies existing rows in a table.

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    sql::{
        executor::{
            ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext,
            eval::ExpressionEvaluator,
        },
        planner::physical::PhysUpdateOp,
    },
    storage::tuple::{OwnedTuple, Tuple, TupleRef},
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

/// Update operator that modifies rows matching a predicate.
///
/// The child executor should provide rows to update (typically from a scan with filter).
/// Each row must include the row_id as the first column for identifying which tuple to update.
pub(crate) struct Update {
    /// Physical operator definition
    op: PhysUpdateOp,
    /// Execution context
    ctx: ExecutionContext,
    /// Child executor providing rows to update
    child: Box<dyn Executor>,
    /// Current execution state
    state: ExecutionState,
    /// Execution statistics
    stats: ExecutionStats,
    /// Number of rows updated
    rows_affected: u64,
    /// Whether we've returned the result row
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
            rows_affected: 0,
            returned_result: false,
        }
    }

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Update a single row
    fn update_row(&mut self, row: &Row) -> ExecutionResult<()> {
        // Extract row_id from the first column
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
        let schema_columns = schema.columns();
        let num_keys = schema.num_keys as usize;

        let mut btree = catalog.table_btree(relation.root(), worker.clone())?;

        let logical_id = LogicalId::new(self.op.table_id, row_id);

        // Search for the tuple
        let current_position =
            btree.search_from_root(row_id.as_ref(), crate::io::frames::FrameAccessMode::Read)?;

        let Some(payload) = btree.get_payload(current_position)? else {
            btree.clear_worker_stack();
            return Err(QueryExecutionError::TupleNotFound(logical_id));
        };

        // Check visibility
        let Some(tuple_ref) = TupleRef::read_for_snapshot(payload.as_ref(), schema, snapshot)?
        else {
            btree.clear_worker_stack();
            return Err(QueryExecutionError::TupleNotFound(logical_id));
        };

        // Evaluate assignment expressions to get new values
        let child_schema = self.child.schema();
        let evaluator = ExpressionEvaluator::new(row, child_schema);

         let mut updates: Vec<(usize, DataType)> = Vec::new();
        for (col_idx, expr) in &self.op.assignments {
            let new_value = evaluator.evaluate(expr.clone())?;

            // Cast to the expected column type
            let expected_type = schema_columns[*col_idx].dtype;
            let casted_value = Self::cast_to_column_type(new_value, expected_type)?;

            // Convert schema column index to value index for add_version
            // add_version expects indices into the values array, not the full schema
            if *col_idx >= num_keys {
                let value_idx = *col_idx - num_keys;
                updates.push((value_idx, casted_value));
            } else {
                // Updating a key column is not supported via add_version
                return Err(QueryExecutionError::Other(
                    "Cannot update key columns".to_string(),
                ));
            }
        }

        // Load current tuple and apply updates
        let mut tuple = Tuple::try_from((payload.as_ref(), schema))?;

        //btree.clear_worker_stack();

        // Add new version with updates
        tuple.add_version(&updates, tx_id)?;
        let new_bytes: OwnedTuple = tuple.into();

        // Update in B-tree
        btree.update(relation.root(), new_bytes.as_ref())?;
       // btree.clear_worker_stack();

        // Handle index updates for modified indexed columns
        let indexed_cols = schema.get_indexed_columns();
        for (col_idx, idx_info) in &indexed_cols {
            // Check if this column was updated
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

impl Executor for Update {
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

        if !self.returned_result {
            // Process all rows from child
            while let Some(row) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.update_row(&row)?;
                self.rows_affected += 1;
                self.stats.rows_produced += 1;
            }

            self.returned_result = true;

            // Return affected row count
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
