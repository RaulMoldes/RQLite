// src/sql/executor/ops/update.rs
use crate::{
    PAGE_ZERO,
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    io::frames::{FrameAccessMode, Position},
    sql::{
        executor::{
            ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext,
            eval::ExpressionEvaluator,
        },
        planner::physical::PhysUpdateOp,
    },
    storage::{
        cell::Slot,
        tuple::{OwnedTuple, Tuple, TupleRef},
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
    rows_affected: u64,
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

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Update a single row using the position from the scan
    fn update_row(&mut self, position: Position, row: &Row) -> ExecutionResult<()> {
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

        // Use position directly - no need to search from root
        let payload = btree.with_cell_at(position, |bytes| bytes.to_vec())?;

        let Some(_tuple_ref) = TupleRef::read_for_snapshot(&payload, schema, snapshot)? else {
            return Err(QueryExecutionError::TupleNotFound(logical_id));
        };

        // Evaluate assignment expressions
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

        let mut tuple = Tuple::try_from((payload.as_slice(), schema))?;
        tuple.add_version(&updates, tx_id)?;
        let new_bytes: OwnedTuple = tuple.into();

        // Update at known position
        btree.update_at(position, new_bytes.as_ref())?;

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
    fn open(&mut self, _access_mode: FrameAccessMode) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        // Open child with write access for DML
        self.child.open(FrameAccessMode::Write)?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.rows_affected = 0;
        self.returned_result = false;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<(Position, Row)>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        if !self.returned_result {
            while let Some((position, row)) = self.child.next()? {
                self.stats.rows_scanned += 1;
                self.update_row(position, &row)?;
                self.rows_affected += 1;
                self.stats.rows_produced += 1;
            }

            self.returned_result = true;

            let mut result = Row::new();
            result.push(DataType::BigUInt(UInt64::from(self.rows_affected)));
            return Ok(Some((Position::new(PAGE_ZERO, Slot(0)), result)));
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

    fn required_access_mode(&self) -> FrameAccessMode {
        FrameAccessMode::Write
    }
}
