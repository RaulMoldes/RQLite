// src/sql/executor/eval.rs
//! Expression evaluation for query execution.
//!
//! This module provides evaluation of BoundExpressions against rows during query execution.

use crate::{
    TypeSystemError, TypeSystemResult,
    schema::Schema,
    sql::{
        binder::bounds::{Binding, BoundExpression, ScalarFunction},
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

    pub(crate) fn evaluate_as_single_value(
        &self,
        expr: &BoundExpression,
    ) -> EvaluationResult<DataType> {
        let mut evaluated = self.evaluate(expr)?;
        if evaluated.len() != 1 {
            return Err(EvaluationError::InvalidExpression(
                "invalid evalution result. expected single value found list".to_string(),
            ));
        }
        return Ok(evaluated
            .pop()
            .expect("Already checked the length of the evaluation result."));
    }

    /// Evaluate an expression and return the result as a DataType
    pub(crate) fn evaluate(&self, expr: &BoundExpression) -> EvaluationResult<Vec<DataType>> {
        match expr {
            BoundExpression::Literal { value } => Ok(vec![value.clone()]),
            BoundExpression::ColumnBinding(col_ref) => Ok(vec![self.eval_column(col_ref.clone())?]),
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
                let mut operand = self.evaluate(expr)?;
                if operand.len() != 1 {
                    return Err(EvaluationError::InvalidExpression(
                        "cannot apply unary operators to lists of values!".to_string(),
                    ));
                };
                Ok(vec![self.eval_unary_op(
                    operand.pop().unwrap(),
                    *op,
                    Some(*result_type),
                )?])
            }
            BoundExpression::IsNull { expr, negated } => {
                let evaluated = self.evaluate(expr)?;
                if evaluated.len() > 1 {
                    return Err(EvaluationError::InvalidExpression(
                        "cannot apply unary operators to lists of values!".to_string(),
                    ));
                };
                Ok(vec![DataType::Bool(Bool(
                    matches!(evaluated[0], DataType::Null) || *negated,
                ))])
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
                if inner.len() > 1 || low.len() > 1 || high.len() > 1 {
                    return Err(EvaluationError::InvalidExpression(
                        "cannot apply unary operators to lists of values!".to_string(),
                    ));
                };

                Ok(vec![DataType::Bool(Bool(
                    (inner[0] >= low[0] && inner[0] <= high[0]) || *negated,
                ))])
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
                    for item in eval {
                        set.insert(item);
                    }
                }

                let evaluated = self.evaluate(expr)?;
                if evaluated.len() > 1 {
                    return Err(EvaluationError::InvalidExpression(
                        "cannot apply unary operators to lists of values!".to_string(),
                    ));
                };
                Ok(vec![DataType::Bool(Bool(
                    set.contains(&evaluated[0]) || *negated,
                ))])
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
                    arg_list.extend(evaluated);
                }
                match func {
                    ScalarFunction::Abs => {
                        let result = Abs::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Cast => {
                        if arg_list.len() != 1 {
                            return Err(EvaluationError::InvalidArguments(ScalarFunction::Cast));
                        };

                        let result = arg_list[0].try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Ceil => {
                        let result = Ceil::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Coalesce => {
                        let result = Coalesce::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Concat => {
                        let result = Concat::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }

                    ScalarFunction::Floor => {
                        let result = Floor::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::NullIf => {
                        let result = NullIf::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Round => {
                        let result = Round::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::LTrim => {
                        let result = LTrim::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::RTrim => {
                        let result = RTrim::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Length => {
                        let result = Length::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Lower => {
                        let result = Lower::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Upper => {
                        let result = Upper::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
                    }
                    ScalarFunction::Sqrt => {
                        let result = Sqrt::call(arg_list)?;
                        let result = result.try_cast(*return_type)?;
                        Ok(vec![result])
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
        if result.len() > 1 {
            return Err(EvaluationError::InvalidExpression(
                "cannot evaluate a list to bool!".to_string(),
            ));
        };
        match &result[0] {
            DataType::Bool(Bool(b)) => Ok(*b),
            DataType::Null => Ok(false), // NULL is treated as false in boolean context
            other => Err(EvaluationError::TypeError(
                TypeSystemError::UnexpectedDataType(result[0].kind()),
            )),
        }
    }

    fn eval_column(&self, col_ref: Binding) -> EvaluationResult<DataType> {
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
        let r = right
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
        let r = right
            .as_bool()
            .ok_or(TypeSystemError::UnexpectedDataType(right.kind()))?
            .value();

        let result = l || r;

        Ok(DataType::Bool(Bool(result)))
    }



    fn string_like(str_lhs: &DataType, pattern: &DataType, negated: bool) -> TypeSystemResult<DataType> {
        let lhs_blob = str_lhs
            .as_blob()
            .ok_or(TypeSystemError::UnexpectedDataType(str_lhs.kind()))?;
        let pattern_blob = pattern
            .as_blob()
            .ok_or(TypeSystemError::UnexpectedDataType(pattern.kind()))?;

        // Convert back to Blob and wrap as Text
        Ok(DataType::Bool(Bool(!negated && lhs_blob.like(pattern_blob.as_str()?)?)))
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
        left: Vec<DataType>,
        right: Vec<DataType>,
        bin_op: BinaryOperator,
        result: Option<DataTypeKind>,
    ) -> EvaluationResult<Vec<DataType>> {
        if left.len() == 1 && right.len() == 1 {
            // Handle NULL propagation for most operators
            if matches!(left[0], DataType::Null)
                || right.len() == 1 && matches!(right[0], DataType::Null)
            {
                // AND/OR have special NULL handling
                match bin_op {
                    BinaryOperator::And => {
                        // FALSE AND NULL = FALSE, TRUE AND NULL = NULL
                        if let Some(Bool(b)) = left[0].as_bool()
                            && !b
                        {
                            return Ok(vec![DataType::Bool(Bool(false))]);
                        } else if let Some(Bool(b)) = right[0].as_bool()
                            && !b
                        {
                            return Ok(vec![DataType::Bool(Bool(false))]);
                        };
                    }
                    BinaryOperator::Or => {
                        // TRUE OR NULL = TRUE, FALSE OR NULL = NULL
                        if let Some(Bool(b)) = left[0].as_bool()
                            && *b
                        {
                            return Ok(vec![DataType::Bool(Bool(true))]);
                        } else if let Some(Bool(b)) = right[0].as_bool()
                            && *b
                        {
                            return Ok(vec![DataType::Bool(Bool(true))]);
                        };
                    }
                    _ => return Ok(vec![DataType::Null]),
                }
            }

            match bin_op {
                // Comparison operators
                BinaryOperator::Eq | BinaryOperator::In | BinaryOperator::Is => {
                    Ok(vec![DataType::Bool(Bool(left == right))])
                }
                BinaryOperator::Neq | BinaryOperator::NotIn | BinaryOperator::IsNot => {
                    Ok(vec![DataType::Bool(Bool(left != right))])
                }
                BinaryOperator::Lt => Ok(vec![DataType::Bool(Bool(left < right))]),
                BinaryOperator::Le => Ok(vec![DataType::Bool(Bool(left <= right))]),
                BinaryOperator::Gt => Ok(vec![DataType::Bool(Bool(left > right))]),
                BinaryOperator::Ge => Ok(vec![DataType::Bool(Bool(left >= right))]),

                // Logical operators
                BinaryOperator::And => {
                    let bool = Self::logical_and(&left[0], &right[0])?;
                    Ok(vec![bool])
                }
                BinaryOperator::Or => {
                    let bool = Self::logical_or(&left[0], &right[0])?;
                    Ok(vec![bool])
                }

                // Arithmetic operators use promoted operations (see the type system module for details)
                BinaryOperator::Plus => {
                    Ok(vec![left[0].add(&right[0]).map_err(EvaluationError::from)?])
                }
                BinaryOperator::Minus => {
                    Ok(vec![left[0].sub(&right[0]).map_err(EvaluationError::from)?])
                }
                BinaryOperator::Multiply => {
                    Ok(vec![left[0].mul(&right[0]).map_err(EvaluationError::from)?])
                }
                BinaryOperator::Divide => {
                    Ok(vec![left[0].div(&right[0]).map_err(EvaluationError::from)?])
                }
                BinaryOperator::Modulo => {
                    Ok(vec![left[0].rem(&right[0]).map_err(EvaluationError::from)?])
                }

                // String operators
                BinaryOperator::Concat => {
                    let result = Self::string_concat(&left[0], &right[0])?;
                    Ok(vec![result])
                }
                BinaryOperator::Like => {
                    let result = Self::string_like(&left[0], &right[0], false)?;
                    Ok(vec![result])
                },
                BinaryOperator::NotLike => {
                    let result = Self::string_like(&left[0], &right[0], true)?;
                    Ok(vec![result])
                },
            }
        } else if right.len() > 1 {
            match bin_op {
                BinaryOperator::In => {
                    return Ok(vec![DataType::Bool(Bool(right.contains(&left[0])))]);
                }
                BinaryOperator::NotIn => {
                    return Ok(vec![DataType::Bool(Bool(!right.contains(&left[0])))]);
                }
                _ => Err(EvaluationError::InvalidExpression(
                    "only in and not in operators can be applied to lists".to_string(),
                )),
            }
        } else {
            return Err(EvaluationError::InvalidExpression(
                "invalid arguments for binary operators. cannot compare two lists".to_string(),
            ));
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

/// Evaluate a literal expression to get a DataType value.
/// Only supports literal values for DEFAULT constraints.
pub(crate) fn eval_literal_expr(expr: &BoundExpression) -> Option<DataType> {
    match expr {
        BoundExpression::Literal { value } => Some(value.clone()),
        _ => None, // Non-literal defaults are not supported yet
    }
}
