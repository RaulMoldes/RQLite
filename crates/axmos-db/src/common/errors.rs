use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind},
    num::{ParseFloatError, ParseIntError},
};

use crate::types::{DataTypeKind, LogicalId, ObjectId, ObjectType, TransactionId};

/// Thread pool errors
#[derive(Debug)]
pub enum ThreadPoolError {
    ShutdownTimeout,
    ThreadJoinError(String),
    PoolShutdown,
}

impl Display for ThreadPoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ShutdownTimeout => write!(f, "Thread pool shutdown timed out"),
            Self::ThreadJoinError(msg) => write!(f, "Failed to join thread: {}", msg),
            Self::PoolShutdown => write!(f, "Thread pool has been shut down"),
        }
    }
}

impl Error for ThreadPoolError {}

/// Task runner errors
#[derive(Debug)]
pub enum TaskError {
    Io(IoError),
    ThreadPool(ThreadPoolError),
    TaskFailed(String),
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::ThreadPool(e) => write!(f, "Thread pool error: {}", e),
            Self::TaskFailed(msg) => write!(f, "Task failed: {}", msg),
        }
    }
}

impl Error for TaskError {}

impl From<IoError> for TaskError {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}

impl From<ThreadPoolError> for TaskError {
    fn from(e: ThreadPoolError) -> Self {
        Self::ThreadPool(e)
    }
}

/// Numeric parsing errors
#[derive(Debug)]
pub enum ParsingError {
    Int(ParseIntError),
    Float(ParseFloatError),
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Int(err) => write!(f, "Error parsing INT: {}", err),
            Self::Float(err) => write!(f, "Error parsing FLOAT: {}", err),
        }
    }
}

impl Error for ParsingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Int(err) => Some(err),
            Self::Float(err) => Some(err),
        }
    }
}

impl From<ParseIntError> for ParsingError {
    fn from(value: ParseIntError) -> Self {
        Self::Int(value)
    }
}

impl From<ParseFloatError> for ParsingError {
    fn from(value: ParseFloatError) -> Self {
        Self::Float(value)
    }
}

/// Type system errors
#[derive(Debug)]
pub enum TypeError {
    UnexpectedDataType(DataTypeKind),
    ParseError(ParsingError),
    NullOperation,
    TypeMismatch {
        left: DataTypeKind,
        right: DataTypeKind,
    },
    CastError {
        from: DataTypeKind,
        to: DataTypeKind,
    },
    NotComparable(DataTypeKind),
    UnsupportedOperation {
        kind: DataTypeKind,
        operation: &'static str,
    },
    Other(String),
}

impl Display for TypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NullOperation => write!(f, "Cannot perform operation on Null"),
            Self::TypeMismatch { left, right } => write!(f, "Type mismatch: {} vs {}", left, right),
            Self::CastError { from, to } => write!(f, "Cannot cast {} to {}", from, to),
            Self::NotComparable(k) => write!(f, "Type {} is not comparable", k),
            Self::UnsupportedOperation { kind, operation } => {
                write!(f, "Type {} does not support {}", kind, operation)
            }
            Self::UnexpectedDataType(data) => write!(f, "Invalid data type: {}", data),
            Self::ParseError(err) => write!(f, "Parse error: {}", err),
            Self::Other(s) => write!(f, "Type error: {}", s),
        }
    }
}

impl Error for TypeError {}

impl From<ParsingError> for TypeError {
    fn from(value: ParsingError) -> Self {
        Self::ParseError(value)
    }
}

impl From<ParseFloatError> for TypeError {
    fn from(value: ParseFloatError) -> Self {
        Self::ParseError(ParsingError::Float(value))
    }
}

impl From<ParseIntError> for TypeError {
    fn from(value: ParseIntError) -> Self {
        Self::ParseError(ParsingError::Int(value))
    }
}

impl From<IoError> for TypeError {
    fn from(value: IoError) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<TaskError> for TypeError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<TypeError> for IoError {
    fn from(value: TypeError) -> Self {
        IoError::new(ErrorKind::InvalidData, value.to_string())
    }
}

/// Object already exists errors
#[derive(Debug, Clone, PartialEq)]
pub enum AlreadyExists {
    Table(String),
    Index(String),
    Constraint(String),
    Column(String, String),
}

impl Display for AlreadyExists {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Index(index) => write!(f, "Index '{}' already exists", index),
            Self::Table(table) => write!(f, "Table '{}' already exists", table),
            Self::Constraint(ct) => write!(f, "Constraint '{}' already exists", ct),
            Self::Column(table, column) => {
                write!(f, "Column '{}' already exists on table '{}'", column, table)
            }
        }
    }
}

/// Parser errors (Step 1: SQL text -> AST)
#[derive(Debug, PartialEq, Clone)]
pub enum ParserError {
    InvalidExpression(String),
    UnexpectedToken(String),
    UnexpectedEof,
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidExpression(s) => write!(f, "Invalid expression: {}", s),
            Self::UnexpectedToken(s) => write!(f, "Unexpected token: {}", s),
            Self::UnexpectedEof => write!(f, "Unexpected end of input"),
        }
    }
}

impl Error for ParserError {}

impl From<TaskError> for ParserError {
    fn from(value: TaskError) -> Self {
        Self::InvalidExpression(value.to_string())
    }
}

/// Analyzer errors (Step 2: AST validation)
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzerError {
    Parser(ParserError),
    ConstraintViolation(String, String),
    ColumnValueCountMismatch(usize),
    MissingColumns,
    NotFound(String),
    DuplicatedColumn(String),
    MultiplePrimaryKeys,
    AlreadyExists(AlreadyExists),
    ArithmeticOverflow(f64, DataTypeKind),
    InvalidFormat(String, String),
    InvalidExpression,
    AlreadyStarted,
    NotStarted,
    InvalidDataType(DataTypeKind),
    AliasNotProvided,
    DuplicatedConstraint(String),
    InvalidValue(DataTypeKind),
    Other(String),
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Parser(e) => write!(f, "{}", e),
            Self::ColumnValueCountMismatch(row) => {
                write!(f, "Column count doesn't match values on row {}", row)
            }
            Self::NotFound(obj) => write!(f, "Object not found: {}", obj),
            Self::MultiplePrimaryKeys => write!(f, "Only one primary key per table is allowed"),
            Self::MissingColumns => {
                write!(
                    f,
                    "Default values not supported, all columns must be specified"
                )
            }
            Self::DuplicatedColumn(col) => write!(f, "Column '{}' specified more than once", col),
            Self::AlreadyExists(e) => write!(f, "{}", e),
            Self::ArithmeticOverflow(num, dtype) => {
                write!(f, "Value {} out of range for type {}", num, dtype)
            }
            Self::ConstraintViolation(ct, col) => {
                write!(f, "Constraint '{}' violated for column '{}'", ct, col)
            }
            Self::InvalidExpression => write!(f, "Invalid expression"),
            Self::AlreadyStarted => write!(f, "Transaction already started on this session"),
            Self::NotStarted => write!(f, "No active transaction"),
            Self::InvalidDataType(dtype) => write!(f, "Invalid data type: {}", dtype),
            Self::InvalidValue(dtype) => write!(f, "Invalid value for type: {}", dtype),
            Self::InvalidFormat(s, expected) => {
                write!(f, "Invalid format '{}', expected: {}", s, expected)
            }
            Self::AliasNotProvided => write!(f, "Alias required for multi-table references"),
            Self::DuplicatedConstraint(s) => write!(f, "Duplicated constraint on column '{}'", s),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for AnalyzerError {}

impl From<ParserError> for AnalyzerError {
    fn from(value: ParserError) -> Self {
        Self::Parser(value)
    }
}

impl From<TaskError> for AnalyzerError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Binder errors (Step 3: Name resolution, type checking)
#[derive(Debug, Clone, PartialEq)]
pub enum BinderError {
    Analyzer(AnalyzerError),
    TableNotFound(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    TypeMismatch {
        expected: DataTypeKind,
        found: DataTypeKind,
        context: String,
    },
    InvalidExpression(String),
    AliasRequired(String),
    DuplicateAlias(String),
    CteNotFound(String),
    ColumnCountMismatch {
        expected: usize,
        found: usize,
    },
    Other(String),
}

impl Display for BinderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Analyzer(e) => write!(f, "{}", e),
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Self::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            Self::AmbiguousColumn(name) => write!(f, "Ambiguous column reference: {}", name),
            Self::ColumnCountMismatch { expected, found } => {
                write!(
                    f,
                    "Column count mismatch: expected {}, found {}",
                    expected, found
                )
            }
            Self::TypeMismatch {
                expected,
                found,
                context,
            } => {
                write!(
                    f,
                    "Type mismatch in {}: expected {:?}, found {:?}",
                    context, expected, found
                )
            }
            Self::InvalidExpression(msg) => write!(f, "Invalid expression: {}", msg),
            Self::AliasRequired(msg) => write!(f, "Alias required: {}", msg),
            Self::DuplicateAlias(alias) => write!(f, "Duplicate alias: {}", alias),
            Self::CteNotFound(name) => write!(f, "CTE not found: {}", name),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for BinderError {}

impl From<AnalyzerError> for BinderError {
    fn from(value: AnalyzerError) -> Self {
        Self::Analyzer(value)
    }
}

impl From<ParserError> for BinderError {
    fn from(value: ParserError) -> Self {
        Self::Analyzer(AnalyzerError::Parser(value))
    }
}

impl From<TaskError> for BinderError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Planner errors (Step 4: Logical plan generation)
#[derive(Debug)]
pub enum PlanError {
    Binder(BinderError),
    TableNotFound(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    TypeMismatch(DataTypeKind, DataTypeKind),
    InvalidExpr(String),
    Unsupported(String),
    InvalidPlan(String),
    Io(IoError),
    Other(String),
}

impl Display for PlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Binder(e) => write!(f, "{}", e),
            Self::TableNotFound(t) => write!(f, "Table not found: {}", t),
            Self::ColumnNotFound(c) => write!(f, "Column not found: {}", c),
            Self::AmbiguousColumn(c) => write!(f, "Ambiguous column: {}", c),
            Self::TypeMismatch(a, b) => write!(f, "Type mismatch: {:?} vs {:?}", a, b),
            Self::InvalidExpr(e) => write!(f, "Invalid expression: {}", e),
            Self::Unsupported(s) => write!(f, "Unsupported: {}", s),
            Self::InvalidPlan(s) => write!(f, "Invalid plan: {}", s),
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for PlanError {}

impl From<BinderError> for PlanError {
    fn from(value: BinderError) -> Self {
        Self::Binder(value)
    }
}

impl From<AnalyzerError> for PlanError {
    fn from(value: AnalyzerError) -> Self {
        Self::Binder(BinderError::Analyzer(value))
    }
}

impl From<ParserError> for PlanError {
    fn from(value: ParserError) -> Self {
        Self::Binder(BinderError::Analyzer(AnalyzerError::Parser(value)))
    }
}

impl From<IoError> for PlanError {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}

impl From<String> for PlanError {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<TaskError> for PlanError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// DDL errors (schema modifications)
#[derive(Debug)]
pub enum DdlError {
    AlreadyExists(AlreadyExists),
    NotFound(String),
    InvalidObjectType(ObjectType),
    ConstraintViolation(String),
    CatalogError(String),
    Io(IoError),
    Other(String),
}

impl Display for DdlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyExists(e) => write!(f, "{}", e),
            Self::NotFound(name) => write!(f, "Object not found: {}", name),
            Self::InvalidObjectType(t) => write!(f, "Invalid object type: {}", t),
            Self::ConstraintViolation(msg) => write!(f, "Constraint violation: {}", msg),
            Self::CatalogError(msg) => write!(f, "Catalog error: {}", msg),
            Self::Io(err) => write!(f, "IO error: {}", err),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for DdlError {}

impl From<IoError> for DdlError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<String> for DdlError {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<AlreadyExists> for DdlError {
    fn from(value: AlreadyExists) -> Self {
        Self::AlreadyExists(value)
    }
}

impl From<TaskError> for DdlError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Optimizer errors (Step 5: Physical plan optimization)
#[derive(Debug)]
pub enum OptimizerError {
    Plan(PlanError),
    Ddl(DdlError),
    NoPlanFound(String),
    Unsupported(String),
    InvalidState(String),
    CostOverflow,
    Timeout,
    Other(String),
}

impl Display for OptimizerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Plan(e) => write!(f, "{}", e),
            Self::Ddl(e) => write!(f, "{}", e),
            Self::NoPlanFound(msg) => write!(f, "No plan found: {}", msg),
            Self::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
            Self::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            Self::CostOverflow => write!(f, "Cost overflow"),
            Self::Timeout => write!(f, "Optimization timeout"),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for OptimizerError {}

impl From<PlanError> for OptimizerError {
    fn from(value: PlanError) -> Self {
        Self::Plan(value)
    }
}

impl From<BinderError> for OptimizerError {
    fn from(value: BinderError) -> Self {
        Self::Plan(PlanError::Binder(value))
    }
}

impl From<AnalyzerError> for OptimizerError {
    fn from(value: AnalyzerError) -> Self {
        Self::Plan(PlanError::Binder(BinderError::Analyzer(value)))
    }
}

impl From<ParserError> for OptimizerError {
    fn from(value: ParserError) -> Self {
        Self::Plan(PlanError::Binder(BinderError::Analyzer(
            AnalyzerError::Parser(value),
        )))
    }
}

impl From<DdlError> for OptimizerError {
    fn from(value: DdlError) -> Self {
        Self::Ddl(value)
    }
}

impl From<TaskError> for OptimizerError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Builder errors (physical operator construction)
#[derive(Debug)]
pub enum BuilderError {
    InvalidOperator(String),
    NumChildrenMismatch(usize, usize),
    Other(String),
}

impl Display for BuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidOperator(o) => write!(f, "Invalid operator: {}", o),
            Self::NumChildrenMismatch(found, expected) => {
                write!(
                    f,
                    "Children mismatch: found {}, expected {}",
                    found, expected
                )
            }
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for BuilderError {}

impl From<TaskError> for BuilderError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Evaluation errors (expression evaluation)
#[derive(Debug)]
pub enum EvaluationError {
    Type(TypeError),
    InvalidExpression(String),
    InvalidArgument(String),
    ColumnOutOfBounds(usize, usize),
    Other(String),
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Type(e) => write!(f, "{}", e),
            Self::InvalidExpression(expr) => write!(f, "Invalid expression: {}", expr),
            Self::ColumnOutOfBounds(i, total) => {
                write!(f, "Column {} out of bounds (row has {} columns)", i, total)
            }
            Self::InvalidArgument(func) => write!(f, "Invalid argument for {}", func),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for EvaluationError {}

impl From<TypeError> for EvaluationError {
    fn from(value: TypeError) -> Self {
        Self::Type(value)
    }
}

impl From<TaskError> for EvaluationError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Query execution errors (Step 6: Plan execution)
#[derive(Debug)]
pub enum QueryExecutionError {
    Optimizer(OptimizerError),
    Builder(BuilderError),
    Eval(EvaluationError),
    Type(TypeError),
    Io(IoError),
    AlreadyDeleted(LogicalId),
    InvalidObjectType(ObjectType),
    ObjectNotFound(ObjectId),
    TupleNotFound(LogicalId),
    AlreadyExists(AlreadyExists),
    Other(String),
}

impl Display for QueryExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Optimizer(e) => write!(f, "{}", e),
            Self::Builder(e) => write!(f, "{}", e),
            Self::Eval(e) => write!(f, "{}", e),
            Self::Type(e) => write!(f, "{}", e),
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::AlreadyDeleted(id) => write!(f, "Tuple {} already deleted", id),
            Self::TupleNotFound(id) => write!(f, "Tuple {} not found", id),
            Self::ObjectNotFound(id) => write!(f, "Object {} not found", id),
            Self::InvalidObjectType(t) => write!(f, "Invalid object type: {}", t),
            Self::AlreadyExists(e) => write!(f, "{}", e),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for QueryExecutionError {}

impl From<OptimizerError> for QueryExecutionError {
    fn from(value: OptimizerError) -> Self {
        Self::Optimizer(value)
    }
}

impl From<PlanError> for QueryExecutionError {
    fn from(value: PlanError) -> Self {
        Self::Optimizer(OptimizerError::Plan(value))
    }
}

impl From<BinderError> for QueryExecutionError {
    fn from(value: BinderError) -> Self {
        Self::Optimizer(OptimizerError::Plan(PlanError::Binder(value)))
    }
}

impl From<AnalyzerError> for QueryExecutionError {
    fn from(value: AnalyzerError) -> Self {
        Self::Optimizer(OptimizerError::Plan(PlanError::Binder(
            BinderError::Analyzer(value),
        )))
    }
}

impl From<ParserError> for QueryExecutionError {
    fn from(value: ParserError) -> Self {
        Self::Optimizer(OptimizerError::Plan(PlanError::Binder(
            BinderError::Analyzer(AnalyzerError::Parser(value)),
        )))
    }
}

impl From<BuilderError> for QueryExecutionError {
    fn from(value: BuilderError) -> Self {
        Self::Builder(value)
    }
}

impl From<EvaluationError> for QueryExecutionError {
    fn from(value: EvaluationError) -> Self {
        Self::Eval(value)
    }
}

impl From<TypeError> for QueryExecutionError {
    fn from(value: TypeError) -> Self {
        Self::Type(value)
    }
}

impl From<IoError> for QueryExecutionError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<TaskError> for QueryExecutionError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

/// Transaction errors (top-level)
#[derive(Debug)]
pub enum TransactionError {
    Sql(QueryExecutionError),
    Aborted(TransactionId),
    NotActive(TransactionId),
    WriteWriteConflict(TransactionId, LogicalId),
    NotFound(TransactionId),
    TupleNotVisible(TransactionId, LogicalId),
    Other(String),
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Sql(e) => write!(f, "{}", e),
            Self::Aborted(id) => write!(f, "Transaction {} aborted", id),
            Self::NotActive(id) => write!(f, "Transaction {} not active", id),
            Self::WriteWriteConflict(txid, tuple) => {
                write!(f, "Transaction {} conflict on tuple {}", txid, tuple)
            }
            Self::NotFound(id) => write!(f, "Transaction {} not found", id),
            Self::TupleNotVisible(id, logical) => {
                write!(f, "Tuple {} not visible to transaction {}", logical, id)
            }
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for TransactionError {}

impl From<QueryExecutionError> for TransactionError {
    fn from(value: QueryExecutionError) -> Self {
        Self::Sql(value)
    }
}

impl From<OptimizerError> for TransactionError {
    fn from(value: OptimizerError) -> Self {
        Self::Sql(QueryExecutionError::Optimizer(value))
    }
}

impl From<PlanError> for TransactionError {
    fn from(value: PlanError) -> Self {
        Self::Sql(QueryExecutionError::Optimizer(OptimizerError::Plan(value)))
    }
}

impl From<BinderError> for TransactionError {
    fn from(value: BinderError) -> Self {
        Self::Sql(QueryExecutionError::Optimizer(OptimizerError::Plan(
            PlanError::Binder(value),
        )))
    }
}

impl From<AnalyzerError> for TransactionError {
    fn from(value: AnalyzerError) -> Self {
        Self::Sql(QueryExecutionError::Optimizer(OptimizerError::Plan(
            PlanError::Binder(BinderError::Analyzer(value)),
        )))
    }
}

impl From<ParserError> for TransactionError {
    fn from(value: ParserError) -> Self {
        Self::Sql(QueryExecutionError::Optimizer(OptimizerError::Plan(
            PlanError::Binder(BinderError::Analyzer(AnalyzerError::Parser(value))),
        )))
    }
}

impl From<BuilderError> for TransactionError {
    fn from(value: BuilderError) -> Self {
        Self::Sql(QueryExecutionError::Builder(value))
    }
}

impl From<IoError> for TransactionError {
    fn from(value: IoError) -> Self {
        Self::Sql(QueryExecutionError::Io(value))
    }
}

impl From<String> for TransactionError {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<TaskError> for TransactionError {
    fn from(value: TaskError) -> Self {
        Self::Other(value.to_string())
    }
}

pub(crate) type ParseResult<T> = Result<T, ParserError>;
pub(crate) type AnalyzerResult<T> = Result<T, AnalyzerError>;
pub(crate) type BinderResult<T> = Result<T, BinderError>;
pub(crate) type PlanResult<T> = Result<T, PlanError>;
pub(crate) type OptimizerResult<T> = Result<T, OptimizerError>;
pub(crate) type DdlResult<T> = Result<T, DdlError>;
pub(crate) type BuilderResult<T> = Result<T, BuilderError>;
pub(crate) type EvalResult<T> = Result<T, EvaluationError>;
pub(crate) type TypeResult<T> = Result<T, TypeError>;
pub(crate) type ExecutionResult<T> = Result<T, QueryExecutionError>;
pub(crate) type TransactionResult<T> = Result<T, TransactionError>;
pub(crate) type ThreadPoolResult<T> = Result<T, ThreadPoolError>;
pub(crate) type TaskResult<T> = Result<T, TaskError>;

pub(crate) type BoxError = Box<dyn Error + Send + 'static>;

pub trait IntoBoxError<T> {
    fn box_err(self) -> Result<T, BoxError>;
}

impl<T, E: Error + Send + 'static> IntoBoxError<T> for Result<T, E> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| Box::new(e) as BoxError)
    }
}
