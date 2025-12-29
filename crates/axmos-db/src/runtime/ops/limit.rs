// Limit operator
use crate::{
    runtime::{ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult},
    schema::Schema,
    sql::planner::physical::PhysLimitOp,
    storage::tuple::Row,
};

pub(crate) struct OpenLimit<Child: RunningExecutor> {
    limit: usize,
    offset: usize,
    schema: Schema,
    child: Child,
    current: usize,
    skipped: usize,
    stats: ExecutionStats,
}

pub(crate) struct ClosedLimit<Child: ClosedExecutor> {
    op: PhysLimitOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedLimit<Child> {
    pub(crate) fn new(
        op: PhysLimitOp,
        child: Child,
        stats: Option<crate::runtime::ExecutionStats>,
    ) -> Self {
        Self {
            op,
            child,
            stats: stats.unwrap_or_default(),
        }
    }
}

impl<Child: ClosedExecutor> ClosedExecutor for ClosedLimit<Child> {
    type Running = OpenLimit<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenLimit {
            limit: self.op.limit,
            offset: self.op.offset.unwrap_or(0),
            schema: self.op.schema,
            child,
            current: 0,
            skipped: 0,
            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenLimit<Child> {
    type Closed = ClosedLimit<Child::Closed>;

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

    fn close(self) -> RuntimeResult<Self::Closed> {
        let child = self.child.close()?;
        Ok(ClosedLimit {
            op: PhysLimitOp {
                limit: self.limit,
                offset: Some(self.offset),
                schema: self.schema,
            },
            child,
            stats: self.stats,
        })
    }
}
