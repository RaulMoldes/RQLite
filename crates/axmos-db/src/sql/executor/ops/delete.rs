// src/sql/executor/ops/delete.rs
//! Delete executor that removes rows from a table.

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row, context::ExecutionContext},
        planner::physical::PhysDeleteOp,
    },
    storage::tuple::{OwnedTuple, Tuple, TupleRef},
    transactions::LogicalId,
    types::{DataType, DataTypeKind, UInt64},
};

/// Delete operator that marks rows as deleted.
///
/// The child executor should provide rows to delete (typically from a scan with filter).
/// Each row must include the row_id as the first column for identifying which tuple to delete.
pub(crate) struct Delete {
    /// Physical operator definition
    op: PhysDeleteOp,
    /// Execution context
    ctx: ExecutionContext,
    /// Child executor providing rows to delete
    child: Box<dyn Executor>,
    /// Current execution state
    state: ExecutionState,
    /// Execution statistics
    stats: ExecutionStats,
    /// Number of rows deleted
    rows_affected: u64,
    /// Whether we've returned the result row
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

    /// Get execution statistics
    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Delete a single row by setting xmax
    fn delete_row(&mut self, row: &Row) -> ExecutionResult<()> {
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

        // Check if already deleted
        if tuple_ref.xmax() != crate::TRANSACTION_ZERO {
            btree.clear_worker_stack();
            return Err(QueryExecutionError::AlreadyDeleted(logical_id));
        }

        // Load tuple and mark as deleted
        let mut tuple = Tuple::try_from((payload.as_ref(), schema))?;

        btree.clear_worker_stack();

        // Mark as deleted by setting xmax
        tuple.delete(tx_id)?;
        let new_bytes: OwnedTuple = tuple.into();

        // Update in B-tree (tuple still exists, just marked deleted)
        btree.update(relation.root(), new_bytes.as_ref())?;
        btree.clear_worker_stack();

        // Note: Index entries are NOT removed for MVCC visibility
        // Vacuum will clean up index entries pointing to dead tuples

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
                self.delete_row(&row)?;
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
