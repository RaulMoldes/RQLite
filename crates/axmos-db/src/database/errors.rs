use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    error::Error,
    num::{ParseFloatError, ParseIntError},
};

use crate::{
    sql::lexer::Token,
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidExpression(s) => write!(f, "invalid expression {s}"),
            Self::UnexpectedToken(s) => write!(f, "unexpected token  {s}"),
            Self::UnexpectedEof => f.write_str("unexpected EOF reached"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SQLError {
    ParserError(ParserError),
    AnalyzerError(AnalyzerError),
}

impl Display for SQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnalyzerError(e) => write!(f, "analyzer error {e}"),
            Self::ParserError(e) => write!(f, "parser error {e}"),
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    NotAcquire(TransactionId),
    TimedOut(TransactionId),
    Deadlock(TransactionId, TransactionId),
    NotFound(TransactionId),
    Io(IoError),
    Other(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Aborted(id) => write!(f, "Transaction with id {id} was aborted"),
            TransactionError::TimedOut(id) => write!(f, "Transaction with id {id} timed out."),
            TransactionError::NotAcquire(id) => {
                write!(
                    f,
                    "Transaction with id {id} is not on acquire state and therefore is not allowed to acquire any more locks."
                )
            }
            TransactionError::Io(e) => write!(f, "IO Error: {e}"),
            TransactionError::Deadlock(id, other) => write!(
                f,
                "Deadlock detected between current transaction {id} and transaction {other}"
            ),
            TransactionError::NotFound(id) => write!(f, "Transaction {id} not found"),
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
