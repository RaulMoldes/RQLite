use crate::{
    database::schema::Schema,
    sql::ast::{BinaryOperator, JoinType, UnaryOperator},
    types::{DataType, DataTypeKind, ObjectId},
};

#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumnRef {
    pub table_idx: usize,
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
    Substr,
    Concat,
    Replace,
    Abs,
    Round,
    Ceil,
    Floor,
    Trunc,
    Mod,
    Power,
    Sqrt,
    Now,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Extract,
    DatePart,
    Coalesce,
    NullIf,
    IfNull,
    Cast,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpression {
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

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelectItem {
    pub expr: BoundExpression,
    pub output_idx: usize,
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
            BoundTableRef::BaseTable { schema, .. } => schema,
            BoundTableRef::Subquery { schema, .. } => schema,
            BoundTableRef::Join { schema, .. } => schema,
            BoundTableRef::Cte { schema, .. } => schema,
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

// DDL types
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
    Check(BoundExpression),
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
    Check(BoundExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCreateIndex {
    pub index_name: String,
    pub table_id: ObjectId,
    pub columns: Vec<BoundIndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundIndexColumn {
    pub column_idx: usize,
    pub ascending: bool,
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
pub struct BoundDropTable {
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
