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

        // Search the tuple by row id.
        let search_result = btree.search_from_root(row_id.as_ref(), FrameAccessMode::Write)?;

        let position = match search_result {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => {
                btree.clear_worker_stack();
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            }
        };

        // Read current tuple
        if let Ok(mut tuple) = btree.with_cell_at(position, |bytes| {
            let Some(tuple_ref) = TupleRef::read_for_snapshot(bytes, schema, snapshot)? else {
                return Err(QueryExecutionError::TupleNotFound(logical_id));
            };

            // Check if already deleted
            // If the xmax is not valid, it means it points to [TRANSACTION ZERO] which is the transaction id null ptr.
            if tuple_ref.xmax().is_valid() {
                return Err(QueryExecutionError::AlreadyDeleted(logical_id));
            }

            Ok(Tuple::try_from((bytes, schema))?)
        })? {
            // Mark the tuple as deleted
            tuple.delete(tx_id)?;
            let new_bytes: OwnedTuple = tuple.into();
            btree.clear_worker_stack();
            // Update in B-tree
            btree.update(root, new_bytes.as_ref())?;
        } else {
            btree.clear_worker_stack();
            return Err(QueryExecutionError::TupleNotFound(logical_id));
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
