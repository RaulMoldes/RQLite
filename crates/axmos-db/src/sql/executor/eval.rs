// src/sql/executor/eval.rs
//! Expression evaluation for query execution.
//!
//! This module provides evaluation of BoundExpressions against rows during query execution.

use std::collections::HashSet;
use std::io::{self, Error as IoError, ErrorKind};

use crate::sql::binder::ast::Function;
use crate::{
    database::schema::Schema,
    impl_number_op,
    sql::ast::{BinaryOperator, UnaryOperator},
    sql::binder::ast::{BoundColumnRef, BoundExpression},
    types::{
        Blob, DataType, DataTypeKind, Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16,
        UInt32, UInt64,
    },
};

use super::Row;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumClass {
    Signed,
    Unsigned,
    Float,
}

enum Number {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

fn number_to_datatype(n: Number, hint: Option<DataTypeKind>) -> DataType {
    match n {
        Number::Float(v) => {
            match hint.unwrap_or(DataTypeKind::Double) {
                DataTypeKind::Float => DataType::Float(Float32(v as f32)),
                DataTypeKind::Double => DataType::Double(Float64(v)),
                _ => DataType::Double(Float64(v)), // fallback
            }
        }
        Number::Signed(v) => {
            match hint.unwrap_or(DataTypeKind::BigInt) {
                DataTypeKind::SmallInt => DataType::SmallInt(Int8(v as i8)),
                DataTypeKind::HalfInt => DataType::HalfInt(Int16(v as i16)),
                DataTypeKind::Int => DataType::Int(Int32(v as i32)),
                DataTypeKind::BigInt => DataType::BigInt(Int64(v)),
                _ => DataType::BigInt(Int64(v)), // fallback
            }
        }
        Number::Unsigned(v) => {
            match hint.unwrap_or(DataTypeKind::BigUInt) {
                DataTypeKind::SmallUInt => DataType::SmallUInt(UInt8(v as u8)),
                DataTypeKind::HalfUInt => DataType::HalfUInt(UInt16(v as u16)),
                DataTypeKind::UInt => DataType::UInt(UInt32(v as u32)),
                DataTypeKind::BigUInt => DataType::BigUInt(UInt64(v)),
                _ => DataType::BigUInt(UInt64(v)), // fallback
            }
        }
    }
}

impl_number_op!(number_add, +);
impl_number_op!(number_sub, -);
impl_number_op!(number_mul, *);
impl_number_op!(number_div, /);
impl_number_op!(number_mod, %);

fn classify(v: &DataType) -> io::Result<NumClass> {
    match v {
        DataType::SmallInt(_) | DataType::HalfInt(_) | DataType::Int(_) | DataType::BigInt(_) => {
            Ok(NumClass::Signed)
        }

        DataType::SmallUInt(_)
        | DataType::HalfUInt(_)
        | DataType::UInt(_)
        | DataType::BigUInt(_) => Ok(NumClass::Unsigned),

        DataType::Float(_) | DataType::Double(_) => Ok(NumClass::Float),

        DataType::Null => Err(IoError::new(ErrorKind::InvalidInput, "NULL not allowed")),
        _ => Err(IoError::new(
            ErrorKind::InvalidInput,
            format!("Not numeric: {v:?}"),
        )),
    }
}

fn promote_class(a: NumClass, b: NumClass) -> NumClass {
    match (a, b) {
        (NumClass::Float, _) | (_, NumClass::Float) => NumClass::Float,
        (NumClass::Signed, NumClass::Unsigned) | (NumClass::Unsigned, NumClass::Signed) => {
            NumClass::Signed
        }
        (NumClass::Signed, NumClass::Signed) => NumClass::Signed,
        (NumClass::Unsigned, NumClass::Unsigned) => NumClass::Unsigned,
    }
}

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
    pub(crate) fn evaluate(&self, expr: BoundExpression) -> io::Result<DataType> {
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
                    Function::Abs => todo!("Functions not implemented"),
                    Function::Cast => todo!("Functions not implemented"),
                    Function::Ceil => todo!("Functions not implemented"),
                    Function::Coalesce => todo!("Functions not implemented"),
                    Function::Concat => todo!("Functions not implemented"),
                    Function::CurrentDate => todo!("Functions not implemented"),
                    Function::CurrentTime => todo!("Functions not implemented"),
                    Function::Extract => todo!("Functions not implemented"),
                    Function::CurrentTimestamp => todo!("Functions not implemented"),
                    Function::DatePart => todo!("Functions not implemented"),
                    Function::Floor => todo!("Functions not implemented"),
                    Function::IfNull => todo!("Functions not implemented"),
                    Function::LTrim => todo!("Functions not implemented"),
                    Function::RTrim => todo!("Functions not implemented"),
                    Function::Mod => todo!("Functions not implemented"),
                    Function::Sqrt => todo!("Functions not implemented"),
                    Function::Trunc => todo!("Functions not implemented"),
                    Function::Now => todo!("Functions not implemented"),
                    Function::Length => todo!("Functions not implemented"),
                    Function::Lower => todo!("Functions not implemented"),
                    Function::Upper => todo!("Functions not implemented"),
                    _ => todo!("Functions not implemented"),
                }
            }
            BoundExpression::Aggregate {
                func,
                arg,
                distinct,
                return_type,
            } => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Aggregate functions should not appear in row-level evaluation",
            )),
            BoundExpression::Star => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Star should not appear in row-level evaluation",
            )),
            BoundExpression::Case {
                operand,
                when_then,
                else_expr,
                result_type,
            } => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Case should not appear in row-level evaluation",
            )),
            BoundExpression::Cast { expr, target_type } => {
                let evaluated = self.evaluate(*expr)?;
                evaluated
                    .try_cast(target_type)
                    .map_err(|_| IoError::new(ErrorKind::InvalidInput, "Unable to cast"))
            }
        }
    }

    /// Evaluate expression as boolean (for WHERE clauses, etc.)
    pub(crate) fn evaluate_as_bool(&self, expr: BoundExpression) -> io::Result<bool> {
        let result = self.evaluate(expr)?;
        match result {
            DataType::Boolean(b) => Ok(b.0 != 0),
            DataType::Null => Ok(false), // NULL is treated as false in boolean context
            other => Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("Expected boolean, got {:?}", other),
            )),
        }
    }

    fn eval_column(&self, col_ref: BoundColumnRef) -> io::Result<DataType> {
        let idx = col_ref.column_idx;
        // column_index is the index into the values portion of the schema
        if idx >= self.row.len() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Column index {} out of bounds (row has {} columns)",
                    idx,
                    self.row.len()
                ),
            ));
        }

        Ok(self.row[idx].clone())
    }

    fn logical_and(left: &DataType, right: &DataType) -> io::Result<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        let l = match left {
            DataType::Boolean(v) => bool::from(*v),
            _ => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("logical AND requires Boolean types, got {left:?}"),
                ));
            }
        };

        let r = match right {
            DataType::Boolean(v) => bool::from(*v),
            _ => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("logical AND requires Boolean types, got {right:?}"),
                ));
            }
        };

        let result = l && r;

        Ok(DataType::Boolean(UInt8::from(result)))
    }

    fn logical_or(left: &DataType, right: &DataType) -> io::Result<DataType> {
        // Handle NULL semantics first (ANSI SQL logic)
        if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
            return Ok(DataType::Null);
        }

        // Extract bool values
        let l = match left {
            DataType::Boolean(v) => bool::from(*v),
            _ => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("logical AND requires Boolean types, got {left:?}"),
                ));
            }
        };

        let r = match right {
            DataType::Boolean(v) => bool::from(*v),
            _ => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("logical AND requires Boolean types, got {right:?}"),
                ));
            }
        };

        let result = l || r;

        Ok(DataType::Boolean(UInt8::from(result)))
    }

    fn try_as_int(value: &DataType) -> io::Result<i64> {
        match value {
            DataType::Null => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot use NULL in arithmetic",
            )),

            DataType::SmallInt(v) => Ok(i64::from(v.0)),
            DataType::HalfInt(v) => Ok(i64::from(v.0)),
            DataType::Int(v) => Ok(i64::from(v.0)),
            DataType::BigInt(v) => Ok(i64::from(v.0)),

            _ => Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("Type {value:?} is not signed int"),
            )),
        }
    }

    fn try_as_uint(value: &DataType) -> io::Result<u64> {
        match value {
            DataType::Null => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot use NULL in arithmetic",
            )),

            DataType::SmallUInt(v) => Ok(u64::from(v.0)),
            DataType::HalfUInt(v) => Ok(u64::from(v.0)),
            DataType::UInt(v) => Ok(u64::from(v.0)),
            DataType::BigUInt(v) => Ok(u64::from(v.0)),

            _ => Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("Type {value:?} is not unsigned int"),
            )),
        }
    }

    fn try_as_float(value: &DataType) -> io::Result<f64> {
        match value {
            DataType::Null => Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot use NULL in arithmetic",
            )),

            DataType::Float(v) => Ok(v.0 as f64),
            DataType::Double(v) => Ok(v.0),

            _ => Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("Type {value:?} is not float"),
            )),
        }
    }

    fn extract_num(value: &DataType, class: NumClass) -> io::Result<Number> {
        match class {
            NumClass::Float => Ok(Number::Float(Self::try_as_float(value)?)),
            NumClass::Signed => Ok(Number::Signed(Self::try_as_int(value)?)),
            NumClass::Unsigned => Ok(Number::Unsigned(Self::try_as_uint(value)?)),
        }
    }

    fn arithmetic_op<F>(
        &self,
        left: &DataType,
        right: &DataType,
        result_type: Option<DataTypeKind>,
        op: F,
    ) -> io::Result<DataType>
    where
        F: Fn(Number, Number) -> Number,
    {
        // 1. Classify the input types
        let c1 = classify(left)?;
        let c2 = classify(right)?;

        // 2. Determine promotion class
        let class = promote_class(c1, c2);

        // 3. Extract numbers according to promoted class
        let l = Self::extract_num(left, class)?;
        let r = Self::extract_num(right, class)?;

        // 4. Perform the operation
        let result = op(l, r);

        // 5. Convert back to DataType respecting result_type hint
        Ok(number_to_datatype(result, result_type))
    }

    fn string_concat(str_a: &DataType, str_b: &DataType) -> io::Result<DataType> {
        match (str_a, str_b) {
            (DataType::Text(a_blob), DataType::Text(b_blob)) => {
                // Convert each Blob to String using UTF-8
                let a_str = a_blob.to_string(crate::TextEncoding::Utf8);
                let b_str = b_blob.to_string(crate::TextEncoding::Utf8);

                // Concatenate strings
                let concatenated = format!("{}{}", a_str, b_str);

                // Convert back to Blob and wrap as Text
                Ok(DataType::Text(Blob::from(concatenated.as_str())))
            }
            (a, b) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Both arguments must be Text, got {:?} and {:?}", a, b),
            )),
        }
    }

    fn eval_binary_op(
        &self,
        left: DataType,
        right: DataType,
        bin_op: BinaryOperator,
        result: Option<DataTypeKind>,
    ) -> io::Result<DataType> {
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
            BinaryOperator::And => Self::logical_and(&left, &right),
            BinaryOperator::Or => Self::logical_or(&left, &right),

            // Arithmetic operators
            BinaryOperator::Plus => self.arithmetic_op(&left, &right, result, number_add),
            BinaryOperator::Divide => self.arithmetic_op(&left, &right, result, number_div),
            BinaryOperator::Minus => self.arithmetic_op(&left, &right, result, number_sub),
            BinaryOperator::Multiply => self.arithmetic_op(&left, &right, result, number_mul),
            BinaryOperator::Modulo => self.arithmetic_op(&left, &right, result, number_mod),

            // String operators
            BinaryOperator::Concat => Self::string_concat(&left, &right),

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
    ) -> io::Result<DataType> {
        if matches!(operand, DataType::Null) {
            return Ok(DataType::Null);
        }

        match unary_op {
            UnaryOperator::Not => match operand {
                DataType::Boolean(b) => Ok(DataType::Boolean((b.0 == 0).into())),
                _ => Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "NOT requires boolean operand",
                )),
            },
            UnaryOperator::Minus => match operand {
                DataType::BigInt(i) => Ok(DataType::BigInt((-i.0).into())),
                DataType::Int(i) => Ok(DataType::Int((-i.0).into())),
                DataType::HalfInt(i) => Ok(DataType::HalfInt((-i.0).into())),
                DataType::SmallInt(i) => Ok(DataType::SmallInt((-i.0).into())),
                DataType::Double(f) => Ok(DataType::Double((-f.0).into())),
                DataType::Float(f) => Ok(DataType::Float((-f.0).into())),
                _ => Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "Negation requires signed numeric operand",
                )),
            },
            UnaryOperator::Plus => Ok(operand), // Unary plus is a no-op
        }
    }
}
