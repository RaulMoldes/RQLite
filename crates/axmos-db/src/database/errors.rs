use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind},
    num::{ParseFloatError, ParseIntError},
};

use crate::{
    ObjectId,
    database::schema::ObjectType,
    sql::{
        binder::ast::{BoundExpression, Function},
        executor::ExecutionState,
        lexer::Token,
        planner::physical::PhysicalOperator,
    },
    transactions::LogicalId,
    types::{DataTypeKind, TransactionId},
};

#[derive(Debug)]
pub(crate) enum QueryExecutionError {
    Io(IoError),
    Build(BuilderError),
    Eval(EvaluationError),
    Type(TypeError),
    AlreadyDeleted(LogicalId),
    InvalidObjectType(ObjectType),
    ObjectNotFound(ObjectId),
    TupleNotFound(LogicalId),
    AlreadyExists(AlreadyExists),
    InvalidState(ExecutionState),
    Other(String),
}

impl Display for QueryExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Type(err) => write!(f, "Type error: {err}"),
            Self::Eval(err) => write!(f, "Evaluation error {err}"),
            Self::Io(err) => write!(f, "Io error {err}"),
            Self::Build(err) => write!(f, "Builder error {err}"),
            Self::InvalidState(state) => write!(f, "Invalid execution state: {state}"),
            Self::AlreadyDeleted(logical) => {
                write!(f, "Tuple {logical} is already deleted")
            }
            Self::TupleNotFound(id) => write!(f, "Tuple with id {id} was anot found"),
            Self::ObjectNotFound(id) => write!(f, "Object with id {id} was anot found"),
            Self::InvalidObjectType(o) => write!(f, "Invalid object type {o}"),
            Self::AlreadyExists(str) => write!(f, "Object {str}, already exists"),
            Self::Other(str) => write!(f, "Runtime error {str}"),
        }
    }
}

impl From<IoError> for QueryExecutionError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl From<EvaluationError> for QueryExecutionError {
    fn from(value: EvaluationError) -> Self {
        Self::Eval(value)
    }
}

impl From<BuilderError> for QueryExecutionError {
    fn from(value: BuilderError) -> Self {
        Self::Build(value)
    }
}

#[derive(Debug)]
pub(crate) enum ParsingError {
    Int(ParseIntError),
    Float(ParseFloatError),
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

impl Error for ParsingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Int(err) => Some(err),
            Self::Float(err) => Some(err),
        }
    }
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ParsingError::Int(err) => write!(f, "Error parsing INT: {}", err),
            ParsingError::Float(err) => write!(f, "Error parsing FLOAT: {}", err),
        }
    }
}

#[derive(Debug)]
pub(crate) enum BuilderError {
    InvalidOperator(PhysicalOperator),
    NumChildrenMismatch(usize, usize),
    Other(String),
}

impl Display for BuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidOperator(o) => write!(f, "Invalid operator found {o}"),
            Self::NumChildrenMismatch(u, i) => write!(
                f,
                "Found an incorrect number of children for an operator: {u}, expected: {i}"
            ),
            Self::Other(str) => f.write_str(str),
        }
    }
}

/// Error type for DataType operations
#[derive(Debug)]
pub(crate) enum TypeError {
    UnexpectedDataType(DataTypeKind),
    ParseError(ParsingError),
    /// Attempted operation on Null variant
    NullOperation,
    /// Type mismatch between operands
    TypeMismatch {
        left: DataTypeKind,
        right: DataTypeKind,
    },
    /// Cannot cast between types
    CastError {
        from: DataTypeKind,
        to: DataTypeKind,
    },
    /// Type is not comparable
    NotComparable(DataTypeKind),
    /// Type does not support this operation
    UnsupportedOperation {
        kind: DataTypeKind,
        operation: &'static str,
    },
    Other(String),
}

impl Display for TypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NullOperation => write!(f, "cannot perform operation on Null"),
            Self::TypeMismatch { left, right } => {
                write!(f, "type mismatch: {} vs {}", left, right)
            }
            Self::CastError { from, to } => {
                write!(f, "cannot cast {} to {}", from, to)
            }
            Self::NotComparable(k) => {
                write!(f, "type {} is not comparable", k)
            }
            Self::UnsupportedOperation { kind, operation } => {
                write!(f, "type {} does not support {}", kind, operation)
            }
            Self::UnexpectedDataType(data) => write!(f, "Invalid data type: {}", data),

            Self::ParseError(err) => write!(f, "Parse error: {}", err),
            Self::Other(str) => f.write_str(str),
        }
    }
}

impl From<IoError> for TypeError {
    fn from(value: IoError) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<TypeError> for IoError {
    fn from(value: TypeError) -> Self {
        IoError::new(ErrorKind::InvalidData, "Type error occurred")
    }
}

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

#[derive(Debug)]
pub(crate) enum EvaluationError {
    TypeError(TypeError),
    InvalidExpression(BoundExpression),
    InvalidArguments(Function),
    ColumnOutOfBounds(usize, usize),
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidExpression(expr) => write!(f, "Invalid expression {expr}"),
            Self::TypeError(ty) => write!(f, "Type error {ty}"),
            Self::ColumnOutOfBounds(i, u) => {
                write!(f, "Column {i} out of bounds. (row has {u} columns)")
            }
            Self::InvalidArguments(func) => write!(f, "invalid iput arguments for {func}"),
        }
    }
}

impl From<TypeError> for EvaluationError {
    fn from(value: TypeError) -> Self {
        Self::TypeError(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AnalyzerError {
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
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlreadyExists {
    Table(String),
    Index(String),
    Constraint(String),
    Column(String, String),
}

impl Display for AlreadyExists {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Index(index) => write!(f, "index {index} already exists"),
            Self::Table(table) => write!(f, "table {table} already exists"),
            Self::Constraint(ct) => write!(f, "constraint {ct} already exists"),
            Self::Column(table, column) => {
                write!(f, "column {column} already exists on table {table}")
            }
        }
    }
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::ColumnValueCountMismatch(row) => write!(f,"number of columns doesn't match values on row {row}"),
            Self::NotFound(obj) => write!(f, "Object not found in the database {obj}"),
            Self::MultiplePrimaryKeys => f.write_str("only one primary key per table is allowed"),
            Self::MissingColumns => {
                f.write_str("default values are not supported, all columns must be specified")
            }
            Self::DuplicatedColumn(col) => write!(f, "column '{col}' specified more than once"),
            Self::AlreadyExists(already_exists) => write!(f, "{already_exists}"),

            Self::ArithmeticOverflow(num, data_type) => {
                write!(f, "integer {num} out of range for data type {data_type}")
            }
            Self::ConstraintViolation(ct, column) => {
                write!(f, "constraint {ct} violated for column {column}")
            }
            Self::InvalidExpression => {
                f.write_str( "Invalid expression found!")
            }
            Self::AlreadyStarted => {
                f.write_str("There is already an opened transaction on this session. Cannot start another transaction on the same session until the current one finishes")
            },
            Self::NotStarted => {
                f.write_str("Transaction has not started. Must be on a transaction in order to call [ROLLBACK] or [COMMIT].")
            },
            Self::InvalidDataType(dtype) => {
                write!(f, "Invalid datatype for expression: {dtype}")
            },
            Self::InvalidValue(dtype) => {
                write!(f, "Invalid value for datatype: {dtype}")
            },
            Self::InvalidFormat(s, o) => {
                write!(f, "Invalid formatting for string: {s}, expected: {o}")
            },
            Self::AliasNotProvided => {
                write!(f, "An alias is required in order to reference columns when referencing multiple tables in the same statement.")
            },
            Self::DuplicatedConstraint(s) => {
                write!(f, "Duplicated constraint on column {s}")
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum ParserError {
    InvalidExpression(String),
    UnexpectedToken(Token),
    UnexpectedEof,
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidExpression(s) => write!(f, "invalid expression {s}"),
            Self::UnexpectedToken(s) => write!(f, "unexpected token  {s}"),
            Self::UnexpectedEof => f.write_str("unexpected EOF reached"),
        }
    }
}

/// Errors that can occur during binding
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BinderError {
    /// Table not found in catalog
    TableNotFound(String),
    /// Column not found in any table in scope
    ColumnNotFound(String),
    /// Column reference is ambiguous (exists in multiple tables)
    AmbiguousColumn(String),
    /// Type mismatch in expression
    TypeMismatch {
        expected: DataTypeKind,
        found: DataTypeKind,
        context: String,
    },
    /// Invalid expression
    InvalidExpression(String),
    /// Alias required but not provided
    AliasRequired(String),
    /// Duplicate alias in scope
    DuplicateAlias(String),
    /// CTE not found
    CteNotFound(String),
    /// Internal error
    Internal(String),
    ColumnCountMismatch {
        expected: usize,
        found: usize,
    },
}

impl Display for BinderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            BinderError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            BinderError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            BinderError::AmbiguousColumn(name) => {
                write!(f, "Ambiguous column reference: {}", name)
            }
            BinderError::ColumnCountMismatch { expected, found } => write!(
                f,
                "Column count mismatch. Expected: {expected} but found {found}"
            ),
            BinderError::TypeMismatch {
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
            BinderError::InvalidExpression(msg) => write!(f, "Invalid expression: {}", msg),
            BinderError::AliasRequired(msg) => write!(f, "Alias required: {}", msg),
            BinderError::DuplicateAlias(alias) => write!(f, "Duplicate alias: {}", alias),
            BinderError::CteNotFound(name) => write!(f, "CTE not found: {}", name),
            BinderError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

/// Errors that can occur during planning
#[derive(Debug)]
pub(crate) enum PlanError {
    /// Table not found
    TableNotFound(String),
    /// Column not found
    ColumnNotFound(String),
    /// Ambiguous column reference
    AmbiguousColumn(String),
    /// Type mismatch
    TypeMismatch(DataTypeKind, DataTypeKind),
    /// Invalid expression
    InvalidExpr(String),
    /// Unsupported operation
    Unsupported(String),
    /// Internal error
    Internal(String),
    /// Invalid plan
    InvalidPlan(String),
}

impl Display for PlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            PlanError::TableNotFound(t) => write!(f, "Table not found: {}", t),
            PlanError::ColumnNotFound(c) => write!(f, "Column not found: {}", c),
            PlanError::AmbiguousColumn(c) => write!(f, "Ambiguous column: {}", c),
            PlanError::TypeMismatch(a, b) => write!(f, "Type mismatch: {:?} vs {:?}", a, b),
            PlanError::InvalidExpr(e) => write!(f, "Invalid expression: {}", e),
            PlanError::Unsupported(s) => write!(f, "Unsupported: {}", s),
            PlanError::Internal(s) => write!(f, "Internal error: {}", s),
            PlanError::InvalidPlan(s) => write!(f, "Invalid plan: {}", s),
        }
    }
}

impl From<IoError> for PlanError {
    fn from(e: IoError) -> Self {
        PlanError::Internal(e.to_string())
    }
}

impl From<String> for PlanError {
    fn from(value: String) -> Self {
        Self::Internal(value)
    }
}

#[derive(Debug)]
pub(crate) enum OptimizerError {
    NoPlanFound(String),
    Internal(String),
    Unsupported(String),
    InvalidState(String),
    CostOverflow,
    Timeout,
}

impl Display for OptimizerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NoPlanFound(msg) => write!(f, "No plan found: {}", msg),
            Self::Internal(msg) => write!(f, "Internal error: {}", msg),
            Self::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
            Self::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            Self::CostOverflow => write!(f, "Cost overflow"),
            Self::Timeout => write!(f, "Optimization timeout"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum SQLError {
    ParserError(ParserError),
    AnalyzerError(AnalyzerError),
    BinderError(BinderError),
    PlannerError(PlanError),
    // Optimizer error
    Optimization(OptimizerError),
}

impl Display for SQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AnalyzerError(e) => write!(f, "analyzer error {e}"),
            Self::ParserError(e) => write!(f, "parser error {e}"),
            Self::BinderError(e) => write!(f, "binder error {e}"),
            Self::PlannerError(e) => write!(f, "planner error {e}"),
            Self::Optimization(e) => write!(f, "Optimizer error {e}"),
        }
    }
}

impl From<ParserError> for SQLError {
    fn from(value: ParserError) -> Self {
        Self::ParserError(value)
    }
}

impl From<OptimizerError> for SQLError {
    fn from(value: OptimizerError) -> Self {
        Self::Optimization(value)
    }
}

impl From<AnalyzerError> for SQLError {
    fn from(value: AnalyzerError) -> Self {
        Self::AnalyzerError(value)
    }
}

impl From<PlanError> for SQLError {
    fn from(value: PlanError) -> Self {
        Self::PlannerError(value)
    }
}

impl From<BinderError> for SQLError {
    fn from(value: BinderError) -> Self {
        Self::BinderError(value)
    }
}

#[derive(Debug)]
pub(crate) enum TransactionError {
    Aborted(TransactionId),
    NotActive(TransactionId),
    WriteWriteConflict(TransactionId, LogicalId),
    NotFound(TransactionId),
    TupleNotVisible(TransactionId, LogicalId),
    Sql(SQLError),
    QueryExecution(QueryExecutionError),
    Other(String),
}

impl From<QueryExecutionError> for TransactionError {
    fn from(value: QueryExecutionError) -> Self {
        Self::QueryExecution(value)
    }
}

impl From<SQLError> for TransactionError {
    fn from(value: SQLError) -> Self {
        Self::Sql(value)
    }
}

impl From<ParserError> for TransactionError {
    fn from(value: ParserError) -> Self {
        Self::Sql(SQLError::from(value))
    }
}

impl From<AnalyzerError> for TransactionError {
    fn from(value: AnalyzerError) -> Self {
        Self::Sql(SQLError::from(value))
    }
}

impl From<BinderError> for TransactionError {
    fn from(value: BinderError) -> Self {
        Self::Sql(SQLError::from(value))
    }
}

impl From<PlanError> for TransactionError {
    fn from(value: PlanError) -> Self {
        Self::Sql(SQLError::from(value))
    }
}

impl From<OptimizerError> for TransactionError {
    fn from(value: OptimizerError) -> Self {
        Self::Sql(SQLError::from(value))
    }
}

impl From<BuilderError> for TransactionError {
    fn from(value: BuilderError) -> Self {
        Self::QueryExecution(QueryExecutionError::Build(value))
    }
}

impl From<String> for TransactionError {
    fn from(value: String) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<IoError> for TransactionError {
    fn from(value: IoError) -> Self {
        Self::QueryExecution(QueryExecutionError::Io(value))
    }
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TransactionError::Aborted(id) => write!(f, "Transaction with id {id} was aborted"),
            TransactionError::NotActive(id) => {
                write!(f, "Transaction with id {id} is not on active state")
            }
            TransactionError::QueryExecution(e) => write!(f, "Query execution error: {e}"),
            Self::WriteWriteConflict(txid, tuple) => {
                write!(
                    f,
                    "Transaction {} conflict: tuple {} was modified by another committed transaction",
                    txid, tuple
                )
            }
            TransactionError::Sql(err) => write!(f, "SQL error: {err}"),
            TransactionError::NotFound(id) => write!(f, "Transaction {id} not found"),
            TransactionError::TupleNotVisible(id, logical) => write!(
                f,
                "Tuple with id {logical} cannot be seen by transaction {id}."
            ),
            TransactionError::Other(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

pub(crate) type ParseResult<T> = Result<T, ParserError>;
pub(crate) type AnalyzerResult<T> = Result<T, AnalyzerError>;
pub(crate) type BinderResult<T> = Result<T, BinderError>;
pub(crate) type SQLResult<T> = Result<T, SQLError>;
pub(crate) type TransactionResult<T> = Result<T, TransactionError>;
pub(crate) type BuilderResult<T> = Result<T, BuilderError>;
pub(crate) type OptimizerResult<T> = Result<T, OptimizerError>;
pub(crate) type EvalResult<T> = Result<T, EvaluationError>;
pub(crate) type TypeResult<T> = Result<T, TypeError>;
pub(crate) type ExecutionResult<T> = Result<T, QueryExecutionError>;

impl Error for BuilderError {}
impl Error for TransactionError {}
impl Error for SQLError {}
impl Error for PlanError {}
impl Error for OptimizerError {}
impl Error for BinderError {}
impl Error for EvaluationError {}
impl Error for QueryExecutionError {}
impl Error for TypeError {}
