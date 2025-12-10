use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        executor::{ExecutionState, ExecutionStats, Executor, Row},
        planner::physical::PhysLimitOp,
    },
};

/// Limit executor for LIMIT/OFFSET queries.
pub(crate) struct Limit {
    op: PhysLimitOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,

    /// Number of rows skipped so far (for offset).
    skipped: usize,

    /// Number of rows returned so far (for limit).
    returned: usize,
}

impl Limit {
    pub(crate) fn new(op: PhysLimitOp, child: Box<dyn Executor>) -> Self {
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            skipped: 0,
            returned: 0,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl Executor for Limit {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.skipped = 0;
        self.returned = 0;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => self.state = ExecutionState::Running,
            _ => {}
        }

        // Check if we've already returned enough rows
        if self.returned >= self.op.limit {
            return Ok(None);
        }

        let offset = self.op.offset.unwrap_or(0);

        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(row) => {
                    self.stats.rows_scanned += 1;

                    // Skip rows for offset
                    if self.skipped < offset {
                        self.skipped += 1;
                        continue;
                    }

                    // Return row if within limit
                    self.returned += 1;
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
            }
        }
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
        &self.op.schema
    }
}
