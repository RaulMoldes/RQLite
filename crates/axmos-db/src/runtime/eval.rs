// src/sql/executor/eval.rs
//! Expression evaluation for query execution.
//!
//! This module provides evaluation of BoundExpressions against rows during query execution.

use crate::{
    TypeSystemError, TypeSystemResult,
    schema::Schema,
    sql::{
        binder::bounds::{BoundColumnRef, BoundExpression, ScalarFunction},
        parser::ast::{BinaryOperator, UnaryOperator},
    },
    storage::tuple::Row,
    types::{Blob, DataType, DataTypeKind, bool::Bool},
};
use std::{
    collections::HashSet,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug)]
pub enum EvaluationError {
    InvalidArguments(ScalarFunction),
    InvalidExpression(String),
    ColumnIndexOutOfBounds(usize, usize),
    TypeError(TypeSystemError),
}

impl Error for EvaluationError {}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidArguments(func) => {
                write!(f, "invalid arguments for function {}", func.name())
            }
            Self::InvalidExpression(expr) => write!(f, "invalid expression found {}", expr),
            Self::ColumnIndexOutOfBounds(usize, max) => write!(
                f,
                "column index out of bounds. Max allowed value is {max}, but found {usize}"
            ),
            Self::TypeError(err) => write!(f, "Type error: {err}"),
        }
    }
}

impl From<TypeSystemError> for EvaluationError {
    fn from(value: TypeSystemError) -> Self {
        Self::TypeError(value)
    }
}

pub type EvaluationResult<T> = Result<T, EvaluationError>;

/// Evaluates a bound expression against a row and schema.
pub(crate) struct ExpressionEvaluator<'a> {
    row: &'a Row,
    schema: &'a Schema,
}

impl<'a> ExpressionEvaluator<'a> {
    pub(crate) fn new(row: &'a Row, schema: &'a Schema) -> Self {
        Self { row, schema }
    }

    /// Evaluate an expression and return the result as a DataType
    pub(crate) fn evaluate(&self, expr: &BoundExpression) -> EvaluationResult<DataType> {
        match expr {
            BoundExpression::Literal { value } => Ok(value.clone()),
            BoundExpression::ColumnRef(col_ref) => self.eval_column(col_ref.clone()),
            BoundExpression::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => {
                let left = self.evaluate(left)?;
                let right = self.evaluate(right)?;
                self.eval_binary_op(left, right, *op, Some(*result_type))
            }
            BoundExpression::UnaryOp {
                op,
                expr,
                result_type,
            } => {
                let operand = self.evaluate(expr)?;
                self.eval_unary_op(operand, *op, Some(*result_type))
            }
            BoundExpression::IsNull { expr, negated } => {
                let evaluated = self.evaluate(expr)?;
                Ok(DataType::Bool(Bool(
                    matches!(evaluated, DataType::Null) || *negated,
                )))
            }
            BoundExpression::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let inner = self.evaluate(expr)?;
                let low = self.evaluate(low)?;
                let high = self.evaluate(high)?;

                Ok(DataType::Bool(Bool(
                    (inner >= low && inner <= high) || *negated,
                )))
            }
            BoundExpression::Exists { query, negated } => {
                todo!("Subquery evaluation is not yet implemented")
            }
            BoundExpression::InList {
                expr,
                list,
                negated,
            } => {
                let mut set: HashSet<DataType> = HashSet::new();
                for exp in list {
                    let eval = self.evaluate(exp)?;
                    set.insert(eval);
                }

                let evaluated = self.evaluate(expr)?;
                Ok(DataType::Bool(Bool(set.contains(&evaluated) || *negated)))
            }
            BoundExpression::Subquery { query, result_type } => {
                todo!("Subquery evaluation is not yet implemented")
            }
            BoundExpression::InSubquery {
                expr,
                query,
                negated,
            } => {
                todo!("Subquery evaluation is not yet implemented")
            }
            BoundExpression::Function {
                func,
                args,
                distinct,
                return_type,
            } => {
                let mut arg_list = Vec::new();
                for expr in args {
                    let evaluated = self.evaluate(expr)?;
                    arg_list.push(evaluated);
                }
                match func {
                    ScalarFunction::Abs => {
                        let result = Abs::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Cast => {
                        if arg_list.len() != 1 {
                            return Err(EvaluationError::InvalidArguments(ScalarFunction::Cast));
                        };

                        let result = arg_list[0].try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Ceil => {
                        let result = Ceil::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Coalesce => {
                        let result = Coalesce::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Concat => {
                        let result = Concat::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }

                    ScalarFunction::Floor => {
                        let result = Floor::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::NullIf => {
                        let result = NullIf::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Round => {
                        let result = Round::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::LTrim => {
                        let result = LTrim::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::RTrim => {
                        let result = RTrim::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Length => {
                        let result = Length::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Lower => {
                        let result = Lower::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Upper => {
                        let result = Upper::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }
                    ScalarFunction::Sqrt => {
                        let result = Sqrt::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(result)
                    }

                    ScalarFunction::Unknown(name) => {
                        Err(EvaluationError::InvalidExpression(name.to_string()))
                    }
                }
            }
            _ => unreachable!("Should not reach here when calling the evaluator"),
        }
    }

    /// Evaluate expression as boolean (for WHERE clauses, etc.)
    pub(crate) fn evaluate_as_bool(&self, expr: &BoundExpression) -> EvaluationResult<bool> {
        let result = self.evaluate(expr)?;
        match &result {
            DataType::Bool(Bool(b)) => Ok(*b),
            DataType::Null => Ok(false), // NULL is treated as false in boolean context
            other => Err(EvaluationError::TypeError(
                TypeSystemError::UnexpectedDataType(result.kind()),
            )),
        }
    }

    fn eval_column(&self, col_ref: BoundColumnRef) -> EvaluationResult<DataType> {
        let idx = col_ref.column_idx;
        // column_index is the index into the values portion of the schema
        if idx >= self.row.len() {
            return Err(EvaluationError::ColumnIndexOutOfBounds(idx, self.row.len()));
        }

        Ok(self.row[idx].clone())
    }

    fn logical_and(left: &DataType, right: &DataType) -> TypeSystemResult<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        let l = left
            .as_bool()
            .ok_or(TypeSystemError::UnexpectedDataType(left.kind()))?
            .value();
        let r = left
            .as_bool()
            .ok_or(TypeSystemError::UnexpectedDataType(right.kind()))?
            .value();

        let result = l && r;

        Ok(DataType::Bool(Bool(result)))
    }

    fn logical_or(left: &DataType, right: &DataType) -> TypeSystemResult<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        let l = left
            .as_bool()
            .ok_or(TypeSystemError::UnexpectedDataType(left.kind()))?
            .value();
        let r = left
            .as_bool()
            .ok_or(TypeSystemError::UnexpectedDataType(right.kind()))?
            .value();

        let result = l || r;

        Ok(DataType::Bool(Bool(result)))
    }

    fn string_concat(str_a: &DataType, str_b: &DataType) -> TypeSystemResult<DataType> {
        let a_blob = str_a
            .as_blob()
            .ok_or(TypeSystemError::UnexpectedDataType(str_a.kind()))?;
        let b_blob = str_b
            .as_blob()
            .ok_or(TypeSystemError::UnexpectedDataType(str_b.kind()))?;

        let a_str = a_blob.to_string();
        let b_str = b_blob.to_string();

        // Concatenate strings
        let concatenated = format!("{}{}", a_str, b_str);

        // Convert back to Blob and wrap as Text
        Ok(DataType::Blob(Blob::from(concatenated.as_str())))
    }

    fn eval_binary_op(
        &self,
        left: DataType,
        right: DataType,
        bin_op: BinaryOperator,
        result: Option<DataTypeKind>,
    ) -> EvaluationResult<DataType> {
        // Handle NULL propagation for most operators
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            // AND/OR have special NULL handling
            match bin_op {
                BinaryOperator::And => {
                    // FALSE AND NULL = FALSE, TRUE AND NULL = NULL
                    if let Some(Bool(b)) = left.as_bool()
                        && !b
                    {
                        return Ok(DataType::Bool(Bool(false)));
                    } else if let Some(Bool(b)) = right.as_bool()
                        && !b
                    {
                        return Ok(DataType::Bool(Bool(false)));
                    };
                }
                BinaryOperator::Or => {
                    // TRUE OR NULL = TRUE, FALSE OR NULL = NULL
                    if let Some(Bool(b)) = left.as_bool()
                        && *b
                    {
                        return Ok(DataType::Bool(Bool(true)));
                    } else if let Some(Bool(b)) = right.as_bool()
                        && *b
                    {
                        return Ok(DataType::Bool(Bool(true)));
                    };
                }
                _ => return Ok(DataType::Null),
            }
        }

        match bin_op {
            // Comparison operators
            BinaryOperator::Eq => Ok(DataType::Bool(Bool(left == right))),
            BinaryOperator::Neq => Ok(DataType::Bool(Bool(left != right))),
            BinaryOperator::Lt => Ok(DataType::Bool(Bool(left < right))),
            BinaryOperator::Le => Ok(DataType::Bool(Bool(left <= right))),
            BinaryOperator::Gt => Ok(DataType::Bool(Bool(left > right))),
            BinaryOperator::Ge => Ok(DataType::Bool(Bool(left >= right))),

            // Logical operators
            BinaryOperator::And => {
                let bool = Self::logical_and(&left, &right)?;
                Ok(bool)
            }
            BinaryOperator::Or => {
                let bool = Self::logical_or(&left, &right)?;
                Ok(bool)
            }

            // Arithmetic operators use promoted operations (see the type system module for details)
            BinaryOperator::Plus => left.add(&right).map_err(EvaluationError::from),
            BinaryOperator::Minus => left.sub(&right).map_err(EvaluationError::from),
            BinaryOperator::Multiply => left.mul(&right).map_err(EvaluationError::from),
            BinaryOperator::Divide => left.div(&right).map_err(EvaluationError::from),
            BinaryOperator::Modulo => left.rem(&right).map_err(EvaluationError::from),

            // String operators
            BinaryOperator::Concat => {
                let result = Self::string_concat(&left, &right)?;
                Ok(result)
            }

            BinaryOperator::Is => todo!(),
            BinaryOperator::IsNot => todo!(),
            BinaryOperator::In => todo!(),
            BinaryOperator::Like => todo!(),
            BinaryOperator::NotLike => todo!(),
            BinaryOperator::NotIn => todo!(),
        }
    }

    fn eval_unary_op(
        &self,
        operand: DataType,
        unary_op: UnaryOperator,
        result_type: Option<DataTypeKind>,
    ) -> EvaluationResult<DataType> {
        if matches!(operand, DataType::Null) {
            return Ok(DataType::Null);
        }

        match unary_op {
            UnaryOperator::Not => match operand {
                DataType::Bool(Bool(b)) => Ok(DataType::Bool(Bool(!b))),
                _ => Err(EvaluationError::TypeError(
                    TypeSystemError::UnexpectedDataType(operand.kind()),
                )),
            },
            UnaryOperator::Minus => match operand {
                DataType::BigInt(i) => Ok(DataType::BigInt((-i.0).into())),
                DataType::Int(i) => Ok(DataType::Int((-i.0).into())),
                DataType::Double(f) => Ok(DataType::Double((-f.0).into())),
                DataType::Float(f) => Ok(DataType::Float((-f.0).into())),
                _ => Err(EvaluationError::TypeError(
                    TypeSystemError::UnexpectedDataType(operand.kind()),
                )),
            },
            UnaryOperator::Plus => Ok(operand), // Unary plus is a no-op
        }
    }
}

pub(crate) trait Callable {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType>;
}

struct Abs;

impl Callable for Abs {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Abs));
        };

        let first = &args[0];
        Ok(first.abs())
    }
}

struct Ceil;

impl Callable for Ceil {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        let first = &args[0];
        Ok(first.ceil())
    }
}

struct Floor;

impl Callable for Floor {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        let first = &args[0];
        Ok(first.floor())
    }
}

struct Round;

impl Callable for Round {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        let first = &args[0];
        Ok(first.round())
    }
}

struct Sqrt;

impl Callable for Sqrt {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        let first = &args[0];
        Ok(first.sqrt())
    }
}

struct Coalesce;

impl Callable for Coalesce {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 2 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        if args[0].is_null() {
            return Ok(args[1].clone());
        }
        Ok(args[0].clone())
    }
}

struct Concat;

impl Callable for Concat {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.is_empty() {
            return Ok(DataType::Blob(Blob::from("")));
        }

        if !args.iter().all(|p| p.is_blob()) {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Ceil));
        };

        let mut result = String::new();

        for arg in args {
            match arg {
                DataType::Blob(blob) => {
                    let s = blob.to_string();
                    result.push_str(&s);
                }
                other => {
                    return Err(EvaluationError::TypeError(
                        TypeSystemError::UnexpectedDataType(other.kind()),
                    ));
                }
            }
        }

        Ok(DataType::Blob(Blob::from(result.as_str())))
    }
}

struct NullIf;

impl Callable for NullIf {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 || !matches!(args[1], DataType::Bool(_)) {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::NullIf));
        };

        let condition = args[1]
            .as_bool()
            .ok_or(EvaluationError::TypeError(
                TypeSystemError::UnexpectedDataType(args[1].kind()),
            ))?
            .value();

        if condition {
            return Ok(DataType::Null);
        }
        Ok(args[0].clone())
    }
}

struct LTrim;
struct RTrim;
struct Lower;
struct Upper;
struct Length;

impl Callable for LTrim {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::LTrim));
        }

        let blob = args[0].as_blob().ok_or(EvaluationError::TypeError(
            TypeSystemError::UnexpectedDataType(args[0].kind()),
        ))?;

        let s = blob.to_string();
        let trimmed = s.trim_start(); // left trim
        Ok(DataType::Blob(Blob::from(trimmed)))
    }
}

impl Callable for RTrim {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::RTrim));
        }

        let blob = args[0].as_blob().ok_or(EvaluationError::TypeError(
            TypeSystemError::UnexpectedDataType(args[0].kind()),
        ))?;
        let s = blob.to_string();
        let trimmed = s.trim_end(); // right trim
        Ok(DataType::Blob(Blob::from(trimmed)))
    }
}

impl Callable for Lower {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Lower));
        }

        let blob = args[0].as_blob().ok_or(EvaluationError::TypeError(
            TypeSystemError::UnexpectedDataType(args[0].kind()),
        ))?;

        let s = blob.to_string();
        Ok(DataType::Blob(Blob::from(s.to_lowercase().as_str())))
    }
}

impl Callable for Upper {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Upper));
        }
        let blob = args[0].as_blob().ok_or(EvaluationError::TypeError(
            TypeSystemError::UnexpectedDataType(args[0].kind()),
        ))?;

        let s = blob.to_string();
        Ok(DataType::Blob(Blob::from(s.to_uppercase().as_str())))
    }
}

impl Callable for Length {
    fn call(args: Vec<DataType>) -> EvaluationResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(ScalarFunction::Length));
        }

        let blob = args[0].as_blob().ok_or(EvaluationError::TypeError(
            TypeSystemError::UnexpectedDataType(args[0].kind()),
        ))?;
        let s = blob.to_string();
        Ok(DataType::BigUInt(crate::UInt64::from(
            s.chars().count() as u64
        )))
    }
}
