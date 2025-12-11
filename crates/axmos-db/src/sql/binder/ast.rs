use crate::{
    database::schema::Schema,
    sql::ast::{BinaryOperator, JoinType, UnaryOperator},
    types::{DataType, DataTypeKind, ObjectId},
};

use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BoundColumnRef {
    pub table_id: ObjectId,
    pub scope_table_index: usize,
    pub column_idx: usize,
    pub data_type: DataTypeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Function {
    Length,
    Upper,
    Lower,
    Trim,
    LTrim,
    RTrim,
    Concat,
    Abs,
    Round,
    Ceil,
    Floor,
    Sqrt,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,

    Coalesce,
    NullIf,
    Cast,
}

impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let name = match self {
            Function::Length => "Length",
            Function::Upper => "Upper",
            Function::Lower => "Lower",
            Function::Trim => "Trim",
            Function::LTrim => "LTrim",
            Function::RTrim => "RTrim",
            Function::Concat => "Concat",

            Function::Abs => "Abs",
            Function::Round => "Round",
            Function::Ceil => "Ceil",
            Function::Floor => "Floor",

            Function::Sqrt => "Sqrt",

            Function::CurrentDate => "CurrentDate",
            Function::CurrentTime => "CurrentTime",
            Function::CurrentTimestamp => "CurrentTimestamp",

            Function::Coalesce => "Coalesce",
            Function::NullIf => "NullIf",
            Function::Cast => "Cast",
        };
        write!(f, "{}", name)
    }
}

impl BoundExpression {
    pub fn is_equi_condition(&self) -> bool {
        match self {
            BoundExpression::BinaryOp {
                left, op, right, ..
            } => {
                if *op == BinaryOperator::Eq {
                    matches!(left.as_ref(), BoundExpression::ColumnRef(_))
                        && matches!(right.as_ref(), BoundExpression::ColumnRef(_))
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn extract_non_equi(&self) -> Option<BoundExpression> {
        match self {
            BoundExpression::BinaryOp {
                left,
                op,
                right,
                result_type,
            } if *op == BinaryOperator::And => {
                let left_non_equi = left.extract_non_equi();
                let right_non_equi = right.extract_non_equi();

                match (left_non_equi, right_non_equi) {
                    (Some(l), Some(r)) => Some(BoundExpression::BinaryOp {
                        left: Box::new(l),
                        op: BinaryOperator::And,
                        right: Box::new(r),
                        result_type: *result_type,
                    }),
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                }
            }
            _ if self.is_equi_condition() => None,
            _ => Some(self.clone()),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BoundExpression {
    ColumnRef(BoundColumnRef),
    Literal {
        value: DataType,
    },
    BinaryOp {
        left: Box<BoundExpression>,
        op: BinaryOperator,
        right: Box<BoundExpression>,
        result_type: DataTypeKind,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<BoundExpression>,
        result_type: DataTypeKind,
    },
    Function {
        func: Function,
        args: Vec<BoundExpression>,
        distinct: bool,
        return_type: DataTypeKind,
    },
    Aggregate {
        func: AggregateFunction,
        arg: Option<Box<BoundExpression>>,
        distinct: bool,
        return_type: DataTypeKind,
    },
    Case {
        operand: Option<Box<BoundExpression>>,
        when_then: Vec<(BoundExpression, BoundExpression)>,
        else_expr: Option<Box<BoundExpression>>,
        result_type: DataTypeKind,
    },
    Subquery {
        query: Box<BoundSelect>,
        result_type: DataTypeKind,
    },
    Exists {
        query: Box<BoundSelect>,
        negated: bool,
    },
    InList {
        expr: Box<BoundExpression>,
        list: Vec<BoundExpression>,
        negated: bool,
    },
    InSubquery {
        expr: Box<BoundExpression>,
        query: Box<BoundSelect>,
        negated: bool,
    },
    Between {
        expr: Box<BoundExpression>,
        low: Box<BoundExpression>,
        high: Box<BoundExpression>,
        negated: bool,
    },
    IsNull {
        expr: Box<BoundExpression>,
        negated: bool,
    },
    Star,
    Cast {
        expr: Box<BoundExpression>,
        target_type: DataTypeKind,
    },
}

impl BoundExpression {
    pub fn data_type(&self) -> DataTypeKind {
        match self {
            BoundExpression::ColumnRef(cr) => cr.data_type,
            BoundExpression::Literal { value } => value.kind(),
            BoundExpression::BinaryOp { result_type, .. } => *result_type,
            BoundExpression::UnaryOp { result_type, .. } => *result_type,
            BoundExpression::Function { return_type, .. } => *return_type,
            BoundExpression::Aggregate { return_type, .. } => *return_type,
            BoundExpression::Case { result_type, .. } => *result_type,
            BoundExpression::Subquery { result_type, .. } => *result_type,
            BoundExpression::Exists { .. }
            | BoundExpression::InList { .. }
            | BoundExpression::InSubquery { .. }
            | BoundExpression::Between { .. }
            | BoundExpression::IsNull { .. } => DataTypeKind::Boolean,
            BoundExpression::Star => DataTypeKind::Null,
            BoundExpression::Cast { target_type, .. } => *target_type,
        }
    }
}

impl Display for BoundExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            BoundExpression::BinaryOp {
                left, op, right, ..
            } => write!(f, "{} {} {}", left, op, right),
            BoundExpression::UnaryOp { op, expr, .. } => write!(f, "{}{}", op, expr),
            BoundExpression::ColumnRef(col) => write!(f, "#{}", col.column_idx),
            BoundExpression::Literal { value } => write!(f, "{}", value),
            BoundExpression::IsNull { expr, negated } => {
                if *negated {
                    write!(f, "{} IS NOT NULL", expr)
                } else {
                    write!(f, "{} IS NULL", expr)
                }
            }
            BoundExpression::Cast { expr, target_type } => {
                write!(f, "CAST({} AS {})", expr, target_type)
            }
            BoundExpression::Function { func, args, .. } => {
                let arg_strs: Vec<_> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{:?}({})", func, arg_strs.join(", "))
            }
            BoundExpression::Aggregate {
                func,
                arg,
                distinct,
                ..
            } => {
                let arg_str = arg
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "*".to_string());
                if *distinct {
                    write!(f, "{:?}(DISTINCT {})", func, arg_str)
                } else {
                    write!(f, "{:?}({})", func, arg_str)
                }
            }
            BoundExpression::Star => write!(f, "*"),
            BoundExpression::Subquery { .. } => write!(f, "(subquery)"),
            BoundExpression::InList { expr, negated, .. } => {
                if *negated {
                    write!(f, "{} NOT IN (...)", expr)
                } else {
                    write!(f, "{} IN (...)", expr)
                }
            }
            BoundExpression::InSubquery { expr, negated, .. } => {
                if *negated {
                    write!(f, "{} NOT IN (subquery)", expr)
                } else {
                    write!(f, "{} IN (subquery)", expr)
                }
            }
            BoundExpression::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    write!(f, "{} NOT BETWEEN {} AND {}", expr, low, high)
                } else {
                    write!(f, "{} BETWEEN {} AND {}", expr, low, high)
                }
            }
            BoundExpression::Exists { negated, .. } => {
                if *negated {
                    write!(f, "NOT EXISTS (subquery)")
                } else {
                    write!(f, "EXISTS (subquery)")
                }
            }
            BoundExpression::Case { .. } => write!(f, "CASE...END"),
        }
    }
}

impl BoundExpression {
    /// Helper to summarize expressions
    pub fn summarize(&self, max_len: usize) -> String {
        let result = match self {
            BoundExpression::ColumnRef(col) => Some(format!("col[{}]", col.column_idx)),
            BoundExpression::Literal { value } => Some(value.to_string()),
            _ => None,
        };

        if let Some(str) = result {
            return str;
        };

        // Fallback for non-marker types
        let full = self.to_string();
        if full.len() > max_len {
            format!("{}...", &full[..max_len.saturating_sub(3)])
        } else {
            full
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundSelectItem {
    pub expr: BoundExpression,
    pub output_idx: usize,
    pub output_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BoundTableRef {
    BaseTable {
        table_id: ObjectId,
        schema: Schema,
    },
    Subquery {
        query: Box<BoundSelect>,
        schema: Schema,
    },
    Join {
        left: Box<BoundTableRef>,
        right: Box<BoundTableRef>,
        join_type: JoinType,
        condition: Option<BoundExpression>,
        schema: Schema,
    },
    Cte {
        cte_idx: usize,
        schema: Schema,
    },
}

impl BoundTableRef {
    pub fn schema(&self) -> &Schema {
        match self {
            BoundTableRef::BaseTable { schema, .. } => schema,
            BoundTableRef::Subquery { schema, .. } => schema,
            BoundTableRef::Join { schema, .. } => schema,
            BoundTableRef::Cte { schema, .. } => schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundOrderBy {
    pub expr: BoundExpression,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundSelect {
    pub distinct: bool,
    pub columns: Vec<BoundSelectItem>,
    pub from: Option<BoundTableRef>,
    pub where_clause: Option<BoundExpression>,
    pub group_by: Vec<BoundExpression>,
    pub having: Option<BoundExpression>,
    pub order_by: Vec<BoundOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundInsert {
    pub table_id: ObjectId,
    pub columns: Vec<usize>,
    pub source: BoundInsertSource,
    pub table_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundInsertSource {
    Values(Vec<Vec<BoundExpression>>),
    Query(Box<BoundSelect>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundUpdate {
    pub table_id: ObjectId,
    pub assignments: Vec<BoundAssignment>,
    pub filter: Option<BoundExpression>,
    pub table_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundAssignment {
    pub column_idx: usize,
    pub value: BoundExpression,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundDelete {
    pub table_id: ObjectId,
    pub filter: Option<BoundExpression>,
    pub table_schema: Schema,
}

// DDL types
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundCreateTable {
    pub table_name: String,
    pub columns: Vec<BoundColumnDef>,
    pub constraints: Vec<BoundTableConstraint>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundColumnDef {
    pub name: String,
    pub data_type: DataTypeKind,
    pub constraints: Vec<BoundColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
    ForeignKey {
        ref_table_id: ObjectId,
        ref_column_idx: usize,
    },
    Default(BoundExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableConstraint {
    PrimaryKey(Vec<usize>),
    Unique(Vec<usize>),
    ForeignKey {
        columns: Vec<usize>,
        ref_table_id: ObjectId,
        ref_columns: Vec<usize>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundCreateIndex {
    pub index_name: String,
    pub table_id: ObjectId,
    pub columns: Vec<BoundIndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundIndexColumn {
    pub column_idx: usize,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundAlterTable {
    pub table_id: ObjectId,
    pub action: BoundAlterAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAlterAction {
    AddColumn(BoundColumnDef),
    DropColumn(usize),
    AlterColumn {
        column_idx: usize,
        new_type: Option<DataTypeKind>,
        set_default: Option<BoundExpression>,
        drop_default: bool,
        set_not_null: bool,
        drop_not_null: bool,
    },
    AddConstraint(BoundTableConstraint),
    DropConstraint(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundDropTable {
    pub table_id: ObjectId, // Cannot drop a table that does not exist
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundTransaction {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundWith {
    pub recursive: bool,
    pub ctes: Vec<BoundSelect>,
    pub body: Box<BoundSelect>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BoundStatement {
    Select(BoundSelect),
    Insert(BoundInsert),
    Update(BoundUpdate),
    Delete(BoundDelete),
    CreateTable(BoundCreateTable),
    CreateIndex(BoundCreateIndex),
    AlterTable(BoundAlterTable),
    DropTable(BoundDropTable),
    Transaction(BoundTransaction),
    With(BoundWith),
}
