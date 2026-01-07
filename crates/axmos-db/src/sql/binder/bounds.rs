// sql/binder/bound.rs

use crate::{
    schema::base::Schema,
    sql::parser::ast::{BinaryOperator, JoinType, UnaryOperator},
    types::{DataType, DataTypeKind, ObjectId},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Binding {
    pub table_id: Option<ObjectId>,
    pub scope_index: usize,
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

impl AggregateFunction {
    pub fn name(&self) -> &'static str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarFunction {
    Length,
    Upper,
    Lower,
    Cast,
    LTrim,
    RTrim,
    Concat,
    Abs,
    Round,
    Ceil,
    Floor,
    Sqrt,
    Coalesce,
    NullIf,
    Unknown(String),
}

impl ScalarFunction {
    pub fn name(&self) -> String {
        match self {
            ScalarFunction::Length => "LENGTH".to_string(),
            ScalarFunction::Upper => "UPPER".to_string(),
            ScalarFunction::Lower => "LOWER".to_string(),
            ScalarFunction::Cast => "CAST".to_string(),
            ScalarFunction::LTrim => "LTRIM".to_string(),
            ScalarFunction::RTrim => "RTRIM".to_string(),
            ScalarFunction::Concat => "CONCAT".to_string(),
            ScalarFunction::Abs => "ABS".to_string(),
            ScalarFunction::Round => "ROUND".to_string(),
            ScalarFunction::Ceil => "CEIL".to_string(),
            ScalarFunction::Floor => "FLOOR".to_string(),
            ScalarFunction::Sqrt => "SQRT".to_string(),
            ScalarFunction::Coalesce => "COALESCE".to_string(),
            ScalarFunction::NullIf => "NULLIF".to_string(),
            ScalarFunction::Unknown(name) => name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpression {
    ColumnBinding(Binding),
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
        func: ScalarFunction,
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
}

impl BoundExpression {
    pub fn data_type(&self) -> DataTypeKind {
        match self {
            Self::ColumnBinding(cr) => cr.data_type,
            Self::Literal { value } => value.kind(),
            Self::BinaryOp { result_type, .. } => *result_type,
            Self::UnaryOp { result_type, .. } => *result_type,
            Self::Function { return_type, .. } => *return_type,
            Self::Aggregate { return_type, .. } => *return_type,
            Self::Case { result_type, .. } => *result_type,
            Self::Subquery { result_type, .. } => *result_type,
            Self::Exists { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::Between { .. }
            | Self::IsNull { .. } => DataTypeKind::Bool,
            Self::Star => DataTypeKind::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelectItem {
    pub expr: BoundExpression,
    pub output_idx: usize,
    pub output_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableRef {
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
            Self::BaseTable { schema, .. }
            | Self::Subquery { schema, .. }
            | Self::Join { schema, .. }
            | Self::Cte { schema, .. } => schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrderBy {
    pub expr: BoundExpression,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelect {
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
pub struct BoundInsert {
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
pub struct BoundUpdate {
    pub table_id: ObjectId,
    pub assignments: Vec<BoundAssignment>,
    pub filter: Option<BoundExpression>,
    pub table_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAssignment {
    pub column_idx: usize,
    pub value: BoundExpression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundDelete {
    pub table_id: ObjectId,
    pub filter: Option<BoundExpression>,
    pub table_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCreateTable {
    pub table_name: String,
    pub columns: Vec<BoundColumnDef>,
    pub constraints: Vec<BoundTableConstraint>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumnDef {
    pub name: String,
    pub data_type: DataTypeKind,
    pub is_non_null: bool,
    pub default: Option<BoundExpression>,
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
pub struct BoundCreateIndex {
    pub index_name: String,
    pub table_id: ObjectId,
    pub columns: Vec<usize>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAlterTable {
    pub table_id: ObjectId,
    pub action: BoundAlterAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAlterAction {
    AddColumn(BoundColumnDef),
    DropColumn(usize),
    AlterColumn {
        column_idx: usize,
        action_type: BoundAlterColumnAction,
    },
    AddConstraint(BoundTableConstraint),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAlterColumnAction {
    SetDataType(DataTypeKind),
    SetDefault(BoundExpression),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundDropTable {
    pub table_id: Option<ObjectId>,
    pub table_name: String,
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
pub struct BoundWith {
    pub recursive: bool,
    pub ctes: Vec<BoundSelect>,
    pub body: Box<BoundSelect>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundStatement {
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
