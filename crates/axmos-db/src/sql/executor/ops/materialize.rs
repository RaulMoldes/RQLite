use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row},
        planner::physical::PhysMaterializeOp,
    },
};

/// The Materialize operator buffers all input rows before producing output.
/// This breaks the pipeline and ensures the child scan completes before
/// any downstream DML operations modify the tree structure.
pub(crate) struct Materialize {
    op: PhysMaterializeOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,
    /// Buffered rows from child
    buffer: Vec<Row>,
    /// Current position in buffer during iteration
    current_idx: usize,
    /// Whether we've finished collecting from child
    materialized: bool,
}

impl Materialize {
    pub(crate) fn new(op: PhysMaterializeOp, child: Box<dyn Executor>) -> Self {
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            buffer: Vec::new(),
            current_idx: 0,
            materialized: false,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Collect all rows from child into buffer
    fn materialize_child(&mut self) -> ExecutionResult<()> {
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;
            self.buffer.push(row);
        }

        // Close child immediately after materializing
        self.child.close()?;
        self.materialized = true;

        Ok(())
    }
}

impl Executor for Materialize {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;

        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.buffer.clear();
        self.current_idx = 0;
        self.materialized = false;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => {
                self.state = ExecutionState::Running;
                // Materialize all rows on first call to next()
                self.materialize_child()?;
            }
            _ => {}
        }

        // Return buffered rows one at a time
        if self.current_idx < self.buffer.len() {
            let row = self.buffer[self.current_idx].clone();
            self.current_idx += 1;
            self.stats.rows_produced += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }

        // Child should already be closed, but be safe
        if !self.materialized {
            let _ = self.child.close();
        }

        self.state = ExecutionState::Closed;
        self.buffer.clear();

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.schema
    }
}
