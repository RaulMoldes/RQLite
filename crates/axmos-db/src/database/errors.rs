use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    num::{ParseFloatError, ParseIntError},
};

use crate::{
    ObjectId,
    database::schema::ObjectType,
    sql::lexer::Token,
    transactions::LogicalId,
    types::{DataTypeKind, TransactionId},
};

#[derive(Debug)]
pub enum ParsingError {
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
pub enum TypeError {
    UnexpectedDataType(DataTypeKind),
    ParseError(ParsingError),
    Other,
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

impl Display for TypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TypeError::UnexpectedDataType(data) => write!(f, "Invalid data type: {}", data),

            TypeError::ParseError(err) => write!(f, "Parse error: {}", err),

            TypeError::Other => write!(f, "Unknown error"),
        }
    }
}

impl Error for TypeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TypeError::ParseError(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzerError {
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
pub enum AlreadyExists {
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
pub enum ParserError {
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
pub enum BinderError {
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

impl Error for BinderError {}

/// Errors that can occur during planning
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
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

impl Error for PlanError {}

impl From<IoError> for PlanError {
    fn from(e: IoError) -> Self {
        PlanError::Internal(e.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SQLError {
    ParserError(ParserError),
    AnalyzerError(AnalyzerError),
    BindererError(BinderError),
    PlannerError(PlanError),
}

impl Display for SQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AnalyzerError(e) => write!(f, "analyzer error {e}"),
            Self::ParserError(e) => write!(f, "parser error {e}"),
            Self::BindererError(e) => write!(f, "binder error {e}"),
            Self::PlannerError(e) => write!(f, "planner error {e}"),
        }
    }
}

impl From<ParserError> for SQLError {
    fn from(value: ParserError) -> Self {
        Self::ParserError(value)
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
        Self::BindererError(value)
    }
}

#[derive(Debug)]
pub enum DatabaseError {
    TypeError(TypeError),
    IOError(IoError),
    TransactionError(TransactionError),
    Sql(SQLError),
    Other(String),
}

impl From<TypeError> for DatabaseError {
    fn from(value: TypeError) -> Self {
        Self::TypeError(value)
    }
}

impl From<IoError> for DatabaseError {
    fn from(value: IoError) -> Self {
        Self::IOError(value)
    }
}

impl From<SQLError> for DatabaseError {
    fn from(value: SQLError) -> Self {
        Self::Sql(value)
    }
}

impl From<TransactionError> for DatabaseError {
    fn from(value: TransactionError) -> Self {
        Self::TransactionError(value)
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            DatabaseError::TypeError(err) => write!(f, "Type error: {}", err),

            DatabaseError::IOError(err) => write!(f, "IO error: {}", err),

            DatabaseError::Sql(err) => write!(f, "SQL error: {}", err),

            DatabaseError::Other(msg) => write!(f, "Other error: {}", msg),
            DatabaseError::TransactionError(msg) => {
                write!(f, "Transaction error {}", msg)
            }
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DatabaseError::TypeError(err) => Some(err),
            DatabaseError::IOError(err) => Some(err),
            DatabaseError::TransactionError(err) => Some(err),
            DatabaseError::Sql(err) => Some(err),
            DatabaseError::Other(_) => None,
        }
    }
}

#[derive(Debug)]
pub enum TransactionError {
    Aborted(TransactionId),
    NotActive(TransactionId),
    AlreadyDeleted(LogicalId),
    InvalidObjectType(ObjectId, ObjectType, ObjectType),
    WriteWriteConflict(TransactionId, LogicalId),
    NotFound(TransactionId),
    ObjectNotFound(ObjectId),
    TupleNotVisible(TransactionId, LogicalId),
    RelationAlreadyExists(String),
    Io(IoError),
    Other(String),
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TransactionError::Aborted(id) => write!(f, "Transaction with id {id} was aborted"),
            TransactionError::ObjectNotFound(id) => write!(f, "Object with id {id} was anot found"),
            TransactionError::InvalidObjectType(id, o1, o2) => write!(
                f,
                "Object with id {id} is invalid. Expected type {o1} but found {o2}"
            ),
            TransactionError::RelationAlreadyExists(str) => write!(
                f,
                "Cannot create relation. Relation with name: {str}, already exists"
            ),
            TransactionError::NotActive(id) => {
                write!(f, "Transaction with id {id} is not on active state")
            }
            TransactionError::AlreadyDeleted(logical) => {
                write!(f, "Tuple {logical} is already deleted")
            }
            TransactionError::Io(e) => write!(f, "IO Error: {e}"),
            Self::WriteWriteConflict(txid, tuple) => {
                write!(
                    f,
                    "Transaction {} conflict: tuple {} was modified by another committed transaction",
                    txid, tuple
                )
            }
            TransactionError::NotFound(id) => write!(f, "Transaction {id} not found"),
            TransactionError::TupleNotVisible(id, logical) => write!(
                f,
                "Tuple with id {logical} cannot be seen by transaction {id}."
            ),
            TransactionError::Other(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl Error for TransactionError {}
impl Error for SQLError {}

impl From<TransactionError> for String {
    fn from(value: TransactionError) -> Self {
        value.to_string()
    }
}

impl From<IoError> for TransactionError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

pub type ParseResult<T> = Result<T, ParserError>;
pub type AnalyzerResult<T> = Result<T, AnalyzerError>;
pub type BinderResult<T> = Result<T, BinderError>;
pub type SQLResult<T> = Result<T, SQLError>;
pub type DatabaseResult<T> = Result<T, DatabaseError>;
pub type TransactionResult<T> = Result<T, TransactionError>;
