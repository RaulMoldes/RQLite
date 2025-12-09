// src/sql/executor/ops/delete.rs
use crate::{
    PAGE_ZERO,
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    io::frames::{FrameAccessMode, Position},
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::PhysDeleteOp,
    },
    storage::cell::Slot,
    storage::tuple::{OwnedTuple, Tuple, TupleRef},
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

pub(crate) struct Delete {
    op: PhysDeleteOp,
    ctx: ExecutionContext,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,
    rows_affected: u64,
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
            rows_affected: 0,
            returned_result: false,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Delete a single row using the position from the scan
    fn delete_row(&mut self, position: Position, row: &Row) -> ExecutionResult<()> {
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
        let mut btree = catalog.table_btree(relation.root(), worker.clone())?;

        let logical_id = LogicalId::new(self.op.table_id, row_id);

        // Use position directly - no need to search from root
        let payload = btree.with_cell_at(position, |bytes| bytes.to_vec())?;

        let Some(tuple_ref) = TupleRef::read_for_snapshot(&payload, schema, snapshot)? else {
            return Err(QueryExecutionError::TupleNotFound(logical_id));
        };

        if tuple_ref.xmax() != crate::TRANSACTION_ZERO {
            return Err(QueryExecutionError::AlreadyDeleted(logical_id));
        }

        let mut tuple = Tuple::try_from((payload.as_slice(), schema))?;
        tuple.delete(tx_id)?;
        let new_bytes: OwnedTuple = tuple.into();

        // Update at known position
        btree.update_at(position, new_bytes.as_ref())?;

        Ok(())
    }
}

impl Executor for Delete {
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
                self.delete_row(position, &row)?;
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
