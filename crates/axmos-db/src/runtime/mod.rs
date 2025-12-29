use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
};

mod builder;
mod context;
mod eval;
mod ops;

use crate::{
    SerializationError, TypeSystemError,
    multithreading::coordinator::TransactionError,
    runtime::eval::EvaluationError,
    schema::catalog::CatalogError,
    storage::tuple::{Row, TupleError},
    tree::bplustree::BtreeError,
};

#[derive(Debug)]
pub enum RuntimeError {
    InvalidExpression(EvaluationError),
    Io(IoError),
    ColumnNotFound(usize),
    TransactionalError(TransactionError),
    Catalog(CatalogError),
    Btree(BtreeError),
    TypeError(TypeSystemError),
    TupleError(TupleError),
    Serialization(SerializationError),
    Other(String),
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Other(str) => write!(f, "internal error: {str}"),
            Self::InvalidExpression(err) => write!(f, "error while evaluating expression: {err}"),
            Self::Io(err) => write!(f, "io error {err}"),
            Self::Catalog(err) => write!(f, "catalog error {err}"),
            Self::TransactionalError(err) => write!(f, "transaction management error {err}"),
            Self::Btree(err) => write!(f, "btree error: {err}"),
            Self::TypeError(err) => write!(f, "type error {err}"),
            Self::TupleError(err) => write!(f, "tuple error {err}"),
            Self::Serialization(err) => write!(f, "serialization error {err}"),
            Self::ColumnNotFound(idx) => write!(f, "column not found in schema {idx}"),
        }
    }
}

impl From<SerializationError> for RuntimeError {
    fn from(value: SerializationError) -> Self {
        Self::Serialization(value)
    }
}

impl From<BtreeError> for RuntimeError {
    fn from(value: BtreeError) -> Self {
        Self::Btree(value)
    }
}

impl From<IoError> for RuntimeError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<CatalogError> for RuntimeError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}
impl From<TransactionError> for RuntimeError {
    fn from(value: TransactionError) -> Self {
        Self::TransactionalError(value)
    }
}

impl From<TypeSystemError> for RuntimeError {
    fn from(value: TypeSystemError) -> Self {
        Self::TypeError(value)
    }
}

impl From<EvaluationError> for RuntimeError {
    fn from(value: EvaluationError) -> Self {
        Self::InvalidExpression(value)
    }
}

impl From<TupleError> for RuntimeError {
    fn from(value: TupleError) -> Self {
        Self::TupleError(value)
    }
}

impl Error for RuntimeError {}
pub type RuntimeResult<T> = Result<T, RuntimeError>;

pub(crate) trait ClosedExecutor {
    type Running: RunningExecutor;
    /// Initialize the operator and its children
    fn open(self) -> RuntimeResult<Self::Running>;
}

pub(crate) trait RunningExecutor {
    type Closed: ClosedExecutor;
    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> RuntimeResult<Option<Row>>;

    /// Clean up resources
    fn close(self) -> RuntimeResult<Self::Closed>;
}

/// Execution statistics for profiling
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}
