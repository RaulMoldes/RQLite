// src/sql/executor/mod.rs
pub mod build;
pub mod context;
pub mod ddl;
pub mod eval;
pub mod ops;

use crate::{
    database::{errors::ExecutionResult, schema::Schema},
    types::DataType,
    vector,
};

use std::fmt::{Display, Formatter, Result as FmtResult};

vector!(Row, DataType);

impl From<&[DataType]> for Row {
    fn from(value: &[DataType]) -> Self {
        Self(value.to_vec())
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        for (i, value) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, " | ")?;
            }
            write!(f, "{}", value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ExecutionState {
    Open,
    Closed,
    Running,
}

impl Display for ExecutionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Open => f.write_str("OPEN"),
            Self::Closed => f.write_str("CLOSED"),
            Self::Running => f.write_str("RUNNING"),
        }
    }
}

/// The Volcano/Iterator model trait
pub(crate) trait Executor {
    /// Initialize the operator and its children
    fn open(&mut self) -> ExecutionResult<()>;

    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> ExecutionResult<Option<Row>>;

    /// Clean up resources
    fn close(&mut self) -> ExecutionResult<()>;

    /// Get the output schema for this operator
    fn schema(&self) -> &Schema;
}

/// Execution statistics for profiling
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}
