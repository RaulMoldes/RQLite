// src/sql/executor/eval.rs
//! Expression evaluation for query execution.
//!
//! This module provides evaluation of BoundExpressions against rows during query execution.

use crate::{
    database::{
        errors::{EvalResult, EvaluationError, TypeError, TypeResult},
        schema::Schema,
    },
    sql::binder::ast::{BoundColumnRef, BoundExpression, Function},
    sql::parser::ast::{BinaryOperator, UnaryOperator},
    types::{Blob, DataType, DataTypeKind, UInt8},
};
use std::collections::HashSet;

use super::Row;

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
    pub(crate) fn evaluate(&self, expr: BoundExpression) -> EvalResult<DataType> {
        match expr {
            BoundExpression::Literal { value } => Ok(value),
            BoundExpression::ColumnRef(col_ref) => self.eval_column(col_ref),
            BoundExpression::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => {
                let left = self.evaluate(*left)?;
                let right = self.evaluate(*right)?;
                self.eval_binary_op(left, right, op, Some(result_type))
            }
            BoundExpression::UnaryOp {
                op,
                expr,
                result_type,
            } => {
                let operand = self.evaluate(*expr)?;
                self.eval_unary_op(operand, op, Some(result_type))
            }
            BoundExpression::IsNull { expr, negated } => {
                let evaluated = self.evaluate(*expr)?;
                Ok(DataType::Boolean(UInt8::from(
                    matches!(evaluated, DataType::Null) || negated,
                )))
            }
            BoundExpression::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let inner = self.evaluate(*expr)?;
                let low = self.evaluate(*low)?;
                let high = self.evaluate(*high)?;

                Ok(DataType::Boolean(UInt8::from(
                    (inner >= low && inner <= high) || negated,
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

                let evaluated = self.evaluate(*expr)?;
                Ok(DataType::Boolean(UInt8::from(
                    set.contains(&evaluated) || negated,
                )))
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
                    Function::Abs => {
                        let result = Abs::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Cast => {
                        if arg_list.len() != 1 {
                            return Err(EvaluationError::InvalidArguments(Function::Cast));
                        };

                        let result = arg_list[0].try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Ceil => {
                        let result = Ceil::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Coalesce => {
                        let result = Coalesce::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Concat => {
                        let result = Concat::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::CurrentDate => {
                        let result = DataType::Date(crate::Date::today());
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::CurrentTime => {
                        let result = DataType::DateTime(crate::DateTime::now());
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }

                    Function::CurrentTimestamp => {
                        let result = DataType::DateTime(crate::DateTime::now());
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Floor => {
                        let result = Floor::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::NullIf => {
                        let result = NullIf::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Round => {
                        let result = Round::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::LTrim => {
                        let result = LTrim::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::RTrim => {
                        let result = RTrim::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Length => {
                        let result = Length::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Lower => {
                        let result = Lower::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Upper => {
                        let result = Upper::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }
                    Function::Sqrt => {
                        let result = Sqrt::call(arg_list)?;
                        let result = result.try_cast(return_type)?;
                        Ok(result)
                    }

                    _ => todo!("Functions not implemented"),
                }
            }
            BoundExpression::Aggregate {
                func,
                arg,
                distinct,
                return_type,
            } => Err(EvaluationError::InvalidExpression(
                BoundExpression::Aggregate {
                    func,
                    arg,
                    distinct,
                    return_type,
                },
            )),
            BoundExpression::Star => Err(EvaluationError::InvalidExpression(BoundExpression::Star)),
            BoundExpression::Case {
                operand,
                when_then,
                else_expr,
                result_type,
            } => Err(EvaluationError::InvalidExpression(BoundExpression::Case {
                operand,
                when_then,
                else_expr,
                result_type,
            })),
            BoundExpression::Cast { expr, target_type } => {
                let evaluated = self.evaluate(*expr)?;
                let result = evaluated.try_cast(target_type)?;
                Ok(result)
            }
        }
    }

    /// Evaluate expression as boolean (for WHERE clauses, etc.)
    pub(crate) fn evaluate_as_bool(&self, expr: BoundExpression) -> EvalResult<bool> {
        let result = self.evaluate(expr)?;
        match &result {
            DataType::Boolean(b) => Ok(b.0 != 0),
            DataType::Null => Ok(false), // NULL is treated as false in boolean context
            other => Err(EvaluationError::Type(TypeError::UnexpectedDataType(
                result.kind(),
            ))),
        }
    }

    fn eval_column(&self, col_ref: BoundColumnRef) -> EvalResult<DataType> {
        let idx = col_ref.column_idx;
        // column_index is the index into the values portion of the schema
        if idx >= self.row.len() {
            return Err(EvaluationError::ColumnOutOfBounds(idx, self.row.len()));
        }

        Ok(self.row[idx].clone())
    }

    fn logical_and(left: &DataType, right: &DataType) -> TypeResult<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        let l = match left {
            DataType::Boolean(v) => bool::from(*v),
            _ => return Err(TypeError::UnexpectedDataType(left.kind())),
        };

        let r = match right {
            DataType::Boolean(v) => bool::from(*v),
            _ => return Err(TypeError::UnexpectedDataType(right.kind())),
        };

        let result = l && r;

        Ok(DataType::Boolean(UInt8::from(result)))
    }

    fn logical_or(left: &DataType, right: &DataType) -> TypeResult<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        // Extract bool values
        let l = match left {
            DataType::Boolean(v) => bool::from(*v),
            _ => return Err(TypeError::UnexpectedDataType(left.kind())),
        };

        let r = match right {
            DataType::Boolean(v) => bool::from(*v),
            _ => return Err(TypeError::UnexpectedDataType(right.kind())),
        };

        let result = l || r;

        Ok(DataType::Boolean(UInt8::from(result)))
    }

    fn string_concat(str_a: &DataType, str_b: &DataType) -> TypeResult<DataType> {
        match (str_a, str_b) {
            (DataType::Text(a_blob), DataType::Text(b_blob)) => {
                // Convert each Blob to String using UTF-8
                let a_str = a_blob.to_string();
                let b_str = b_blob.to_string();

                // Concatenate strings
                let concatenated = format!("{}{}", a_str, b_str);

                // Convert back to Blob and wrap as Text
                Ok(DataType::Text(Blob::from(concatenated.as_str())))
            }
            (a, DataType::Text(_)) => Err(TypeError::TypeMismatch {
                left: a.kind(),
                right: DataTypeKind::Text,
            }),
            (DataType::Text(_), b) => Err(TypeError::TypeMismatch {
                left: b.kind(),
                right: DataTypeKind::Text,
            }),
            _ => Err(TypeError::Other(
                "Types must be string for string concat".to_string(),
            )),
        }
    }

    fn eval_binary_op(
        &self,
        left: DataType,
        right: DataType,
        bin_op: BinaryOperator,
        result: Option<DataTypeKind>,
    ) -> EvalResult<DataType> {
        // Handle NULL propagation for most operators
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            // AND/OR have special NULL handling
            match bin_op {
                BinaryOperator::And => {
                    // FALSE AND NULL = FALSE, TRUE AND NULL = NULL
                    if let DataType::Boolean(b) = &left {
                        if b.0 == 0 {
                            return Ok(DataType::Boolean(false.into()));
                        }
                    }
                    if let DataType::Boolean(b) = &right {
                        if b.0 == 0 {
                            return Ok(DataType::Boolean(false.into()));
                        }
                    }
                    return Ok(DataType::Null);
                }
                BinaryOperator::Or => {
                    // TRUE OR NULL = TRUE, FALSE OR NULL = NULL
                    if let DataType::Boolean(b) = &left {
                        if b.0 != 0 {
                            return Ok(DataType::Boolean(true.into()));
                        }
                    }
                    if let DataType::Boolean(b) = &right {
                        if b.0 != 0 {
                            return Ok(DataType::Boolean(true.into()));
                        }
                    }
                    return Ok(DataType::Null);
                }
                _ => return Ok(DataType::Null),
            }
        }

        match bin_op {
            // Comparison operators
            BinaryOperator::Eq => Ok(DataType::Boolean(UInt8::from(left == right))),
            BinaryOperator::Neq => Ok(DataType::Boolean(UInt8::from(left != right))),
            BinaryOperator::Lt => Ok(DataType::Boolean(UInt8::from(left < right))),
            BinaryOperator::Le => Ok(DataType::Boolean(UInt8::from(left <= right))),
            BinaryOperator::Gt => Ok(DataType::Boolean(UInt8::from(left > right))),
            BinaryOperator::Ge => Ok(DataType::Boolean(UInt8::from(left >= right))),

            // Logical operators
            BinaryOperator::And => {
                let bool = Self::logical_and(&left, &right)?;
                Ok(bool)
            }
            BinaryOperator::Or => {
                let bool = Self::logical_or(&left, &right)?;
                Ok(bool)
            }
            // Arithmetic operators
            BinaryOperator::Plus => {
                let result = left.arithmetic_op(&right, result, |a, b| a + b)?;
                Ok(result)
            }
            BinaryOperator::Minus => {
                let result = left.arithmetic_op(&right, result, |a, b| a - b)?;
                Ok(result)
            }
            BinaryOperator::Multiply => {
                let result = left.arithmetic_op(&right, result, |a, b| a * b)?;
                Ok(result)
            }
            BinaryOperator::Divide => {
                let result = left.arithmetic_op(&right, result, |a, b| a / b)?;
                Ok(result)
            }
            BinaryOperator::Modulo => {
                let result = left.arithmetic_op(&right, result, |a, b| a % b)?;
                Ok(result)
            }

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
    ) -> EvalResult<DataType> {
        if matches!(operand, DataType::Null) {
            return Ok(DataType::Null);
        }

        match unary_op {
            UnaryOperator::Not => match operand {
                DataType::Boolean(b) => Ok(DataType::Boolean((b.0 == 0).into())),
                _ => Err(EvaluationError::Type(TypeError::UnexpectedDataType(
                    operand.kind(),
                ))),
            },
            UnaryOperator::Minus => match operand {
                DataType::BigInt(i) => Ok(DataType::BigInt((-i.0).into())),
                DataType::Int(i) => Ok(DataType::Int((-i.0).into())),
                DataType::HalfInt(i) => Ok(DataType::HalfInt((-i.0).into())),
                DataType::SmallInt(i) => Ok(DataType::SmallInt((-i.0).into())),
                DataType::Double(f) => Ok(DataType::Double((-f.0).into())),
                DataType::Float(f) => Ok(DataType::Float((-f.0).into())),
                _ => Err(EvaluationError::Type(TypeError::UnexpectedDataType(
                    operand.kind(),
                ))),
            },
            UnaryOperator::Plus => Ok(operand), // Unary plus is a no-op
        }
    }
}

pub(crate) trait Callable {
    fn call(args: Vec<DataType>) -> EvalResult<DataType>;
}

struct Abs;

impl Callable for Abs {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(Function::Abs));
        };

        let first = &args[0];
        Ok(first.abs())
    }
}

struct Ceil;

impl Callable for Ceil {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        let first = &args[0];
        Ok(first.ceil())
    }
}

struct Floor;

impl Callable for Floor {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        let first = &args[0];
        Ok(first.floor())
    }
}

struct Round;

impl Callable for Round {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        let first = &args[0];
        Ok(first.round())
    }
}

struct Sqrt;

impl Callable for Sqrt {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !args[0].is_numeric() {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        let first = &args[0];
        Ok(first.sqrt())
    }
}

struct Coalesce;

impl Callable for Coalesce {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 2 {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        if matches!(args[0], DataType::Null) {
            return Ok(args[1].clone());
        }
        Ok(args[0].clone())
    }
}

struct Concat;

impl Callable for Concat {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.is_empty() {
            return Ok(DataType::Text(Blob::from("")));
        }

        if !args.iter().all(|p| matches!(p, DataType::Text(_))) {
            return Err(EvaluationError::InvalidArguments(Function::Ceil));
        };

        let mut result = String::new();

        for arg in args {
            match arg {
                DataType::Text(blob) => {
                    let s = blob.to_string();
                    result.push_str(&s);
                }
                other => {
                    return Err(EvaluationError::Type(TypeError::TypeMismatch {
                        left: other.kind(),
                        right: DataTypeKind::Text,
                    }));
                }
            }
        }

        Ok(DataType::Text(Blob::from(result.as_str())))
    }
}

struct NullIf;

impl Callable for NullIf {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 || !matches!(args[1], DataType::Boolean(_)) {
            return Err(EvaluationError::InvalidArguments(Function::NullIf));
        };

        let DataType::Boolean(u) = args[1] else {
            return Err(EvaluationError::InvalidArguments(Function::NullIf));
        };

        let condition = bool::from(u);

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
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(Function::LTrim));
        }

        match &args[0] {
            DataType::Text(blob) => {
                let s = blob.to_string();
                let trimmed = s.trim_start(); // left trim
                Ok(DataType::Text(Blob::from(trimmed)))
            }
            other => Err(EvaluationError::Type(TypeError::TypeMismatch {
                left: other.kind(),
                right: DataTypeKind::Text,
            })),
        }
    }
}

impl Callable for RTrim {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(Function::RTrim));
        }

        match &args[0] {
            DataType::Text(blob) => {
                let s = blob.to_string();
                let trimmed = s.trim_end(); // right trim
                Ok(DataType::Text(Blob::from(trimmed)))
            }
            other => Err(EvaluationError::Type(TypeError::TypeMismatch {
                left: other.kind(),
                right: DataTypeKind::Text,
            })),
        }
    }
}

impl Callable for Lower {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(Function::Lower));
        }

        match &args[0] {
            DataType::Text(blob) => {
                let s = blob.to_string();
                Ok(DataType::Text(Blob::from(s.to_lowercase().as_str())))
            }
            other => Err(EvaluationError::Type(TypeError::TypeMismatch {
                left: other.kind(),
                right: DataTypeKind::Text,
            })),
        }
    }
}

impl Callable for Upper {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(Function::Upper));
        }

        match &args[0] {
            DataType::Text(blob) => {
                let s = blob.to_string();
                Ok(DataType::Text(Blob::from(s.to_uppercase().as_str())))
            }
            other => Err(EvaluationError::Type(TypeError::TypeMismatch {
                left: other.kind(),
                right: DataTypeKind::Text,
            })),
        }
    }
}

impl Callable for Length {
    fn call(args: Vec<DataType>) -> EvalResult<DataType> {
        if args.len() != 1 {
            return Err(EvaluationError::InvalidArguments(Function::Length));
        }

        match &args[0] {
            DataType::Text(blob) => {
                let s = blob.to_string();
                Ok(DataType::BigUInt(crate::UInt64::from(
                    s.chars().count() as u64
                )))
            }
            other => Err(EvaluationError::Type(TypeError::TypeMismatch {
                left: other.kind(),
                right: DataTypeKind::Text,
            })),
        }
    }
}
