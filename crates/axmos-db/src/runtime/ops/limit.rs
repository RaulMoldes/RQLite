// Limit operator
use crate::{
    runtime::{ExecutionStats, Executor, RuntimeResult},
    schema::Schema,
    sql::planner::physical::PhysLimitOp,
    storage::tuple::Row,
};

pub(crate) struct Limit<Child: Executor> {
    child: Child,
    limit: usize,
    offset: usize,
    current: usize,
    skipped: usize,
    schema: Schema,
    stats: ExecutionStats,
}

impl<Child: Executor> Limit<Child> {
    pub(crate) fn new(op: &PhysLimitOp, child: Child) -> Self {
        Self {
            child,
            stats: ExecutionStats::default(),
            limit: op.limit,
            offset: op.offset.unwrap_or(0),
            schema: op.schema.clone(),
            current: 0,
            skipped: 0,
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<Child: Executor> Executor for Limit<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        // Skip offset rows
        while self.skipped < self.offset {
            match self.child.next()? {
                None => return Ok(None),
                Some(_) => {
                    self.skipped += 1;
                    self.stats.rows_scanned += 1;
                }
            }
        }

        // Return up to limit rows
        if self.current >= self.limit {
            return Ok(None);
        }

        match self.child.next()? {
            None => Ok(None),
            Some(row) => {
                self.current += 1;
                self.stats.rows_scanned += 1;
                self.stats.rows_produced += 1;
                Ok(Some(row))
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
