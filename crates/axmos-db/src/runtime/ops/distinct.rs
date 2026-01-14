use std::collections::HashSet;

use crate::{
    runtime::{ExecutionStats, Executor, RuntimeResult},
    schema::Schema,
    sql::planner::physical::HashDistinctOp,
    storage::tuple::Row,
    types::DataType,
};

pub(crate) struct HashDistinct<Child: Executor> {
    schema: Schema,
    child: Child,
    seen: HashSet<Vec<DataType>>,
    stats: ExecutionStats,
}

impl<Child: Executor> HashDistinct<Child> {
    pub(crate) fn new(op: &HashDistinctOp, child: Child) -> Self {
        Self {
            schema: op.schema.clone(),
            child,
            seen: HashSet::new(),
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn row_to_key(&self, row: &Row) -> Vec<DataType> {
        let mut key = Vec::with_capacity(row.len());
        for i in 0..row.len() {
            key.push(row[i].clone());
        }
        key
    }
}

impl<Child: Executor> Executor for HashDistinct<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(row) => {
                    self.stats.rows_scanned += 1;
                    let key = self.row_to_key(&row);

                    if self.seen.insert(key) {
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                }
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
