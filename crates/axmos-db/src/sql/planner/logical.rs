//! Logical operators represent the logical structure of a query plan.
//!
//! These operators are algebra-level representations that describe what the query does,
//! not how it will be executed. This is defined by physical operators.

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use crate::database::schema::Schema;
use crate::sql::ast::{BinaryOperator, JoinType};
use crate::sql::binder::ast::*;
use crate::types::ObjectId;

use super::memo::GroupId;
use super::prop::LogicalProperties;

/// A logical expression in the memo
#[derive(Debug, Clone)]
pub struct LogicalExpr {
    pub id: super::ExprId,
    pub op: LogicalOperator,
    pub children: Vec<GroupId>,
    pub properties: LogicalProperties,
}

impl LogicalExpr {
    pub fn new(op: LogicalOperator, children: Vec<GroupId>) -> Self {
        let properties = LogicalProperties::default();
        Self {
            id: super::ExprId::default(),
            op,
            children,
            properties,
        }
    }

    pub fn with_properties(mut self, props: LogicalProperties) -> Self {
        self.properties = props;
        self
    }
}

/// Logical operators - describe the logical structure of a query
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    // Scan operators
    TableScan(TableScanOp),
    IndexScan(IndexScanOp),

    // Selection and projection
    Filter(FilterOp),
    Project(ProjectOp),

    // Join operators
    Join(JoinOp),

    // Set operations
    Union(UnionOp),
    Intersect(IntersectOp),
    Except(ExceptOp),

    // Aggregation
    Aggregate(AggregateOp),

    // Sorting and limiting
    Sort(SortOp),
    Limit(LimitOp),

    // Distinct
    Distinct(DistinctOp),

    // DML operations
    Insert(InsertOp),
    Update(UpdateOp),
    Delete(DeleteOp),

    // Values (for INSERT ... VALUES)
    Values(ValuesOp),

    // Empty relation (for queries with no FROM)
    Empty(EmptyOp),
}

// Implement display for logical plan
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub op: LogicalOperator,
    pub children: Vec<LogicalPlan>,
    pub properties: LogicalProperties,
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.explain())
    }
}

impl LogicalOperator {
    /// Get the output schema of this operator
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::TableScan(op) => &op.schema,
            Self::IndexScan(op) => &op.schema,
            Self::Filter(op) => &op.input_schema,
            Self::Project(op) => &op.output_schema,
            Self::Join(op) => &op.output_schema,
            Self::Union(op) => &op.schema,
            Self::Intersect(op) => &op.schema,
            Self::Except(op) => &op.schema,
            Self::Aggregate(op) => &op.output_schema,
            Self::Sort(op) => &op.schema,
            Self::Limit(op) => &op.schema,
            Self::Distinct(op) => &op.schema,
            Self::Insert(op) => &op.table_schema,
            Self::Update(op) => &op.table_schema,
            Self::Delete(op) => &op.table_schema,
            Self::Values(op) => &op.schema,
            Self::Empty(op) => &op.schema,
        }
    }

    /// Check if this operator is a leaf (has no children)
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::TableScan(_) | Self::IndexScan(_) | Self::Values(_) | Self::Empty(_)
        )
    }

    /// Get the arity (number of children) of this operator
    pub fn arity(&self) -> usize {
        match self {
            Self::TableScan(_) | Self::IndexScan(_) | Self::Values(_) | Self::Empty(_) => 0,
            Self::Filter(_)
            | Self::Project(_)
            | Self::Sort(_)
            | Self::Limit(_)
            | Self::Distinct(_)
            | Self::Aggregate(_)
            | Self::Insert(_)
            | Self::Update(_)
            | Self::Delete(_) => 1,
            Self::Join(_) | Self::Union(_) | Self::Intersect(_) | Self::Except(_) => 2,
        }
    }
}

/// Table scan operator - reads all rows from a table
#[derive(Debug, Clone, PartialEq)]
pub struct TableScanOp {
    pub table_id: ObjectId,
    pub table_name: String,
    pub schema: Schema,
    /// Columns to read (if None, read all)
    pub columns: Option<Vec<usize>>,
    /// Inline filter predicate (pushed down)
    pub predicate: Option<BoundExpression>,
}

impl TableScanOp {
    pub fn new(table_id: ObjectId, table_name: String, schema: Schema) -> Self {
        Self {
            table_id,
            table_name,
            schema,
            columns: None,
            predicate: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn with_predicate(mut self, predicate: BoundExpression) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Index scan operator - reads rows using an index
#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanOp {
    pub table_id: ObjectId,
    pub index_name: String,
    pub schema: Schema,
    /// The indexed columns
    pub index_columns: Vec<usize>,
    /// Range predicates on the index
    pub range_start: Option<BoundExpression>,
    pub range_end: Option<BoundExpression>,
    /// Additional filter after index lookup
    pub residual_predicate: Option<BoundExpression>,
}

/// Filter operator - applies a predicate to filter rows
#[derive(Debug, Clone, PartialEq)]
pub struct FilterOp {
    pub predicate: BoundExpression,
    pub input_schema: Schema,
}

impl FilterOp {
    pub fn new(predicate: BoundExpression, input_schema: Schema) -> Self {
        Self {
            predicate,
            input_schema,
        }
    }
}

/// Project operator - selects and computes columns
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectOp {
    pub expressions: Vec<ProjectExpr>,
    pub input_schema: Schema,
    pub output_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectExpr {
    pub expr: BoundExpression,
    pub alias: Option<String>,
}

impl ProjectOp {
    pub fn new(expressions: Vec<ProjectExpr>, input_schema: Schema, output_schema: Schema) -> Self {
        Self {
            expressions,
            input_schema,
            output_schema,
        }
    }
}

/// Join operator
#[derive(Debug, Clone, PartialEq)]
pub struct JoinOp {
    pub join_type: JoinType,
    pub condition: Option<BoundExpression>,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
}

impl JoinOp {
    pub fn new(
        join_type: JoinType,
        condition: Option<BoundExpression>,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        let output_schema = Self::merge_schemas(&left_schema, &right_schema);
        Self {
            join_type,
            condition,
            left_schema,
            right_schema,
            output_schema,
        }
    }

    fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
        let mut cols = left.columns().clone();
        cols.extend(right.columns().clone());
        Schema::from_columns(&cols, 0)
    }

    /// Check if this is an equi-join (join on equality conditions)
    pub fn is_equi_join(&self) -> bool {
        match &self.condition {
            Some(expr) => Self::is_equi_condition(expr),
            None => false,
        }
    }

    fn is_equi_condition(expr: &BoundExpression) -> bool {
        match expr {
            BoundExpression::BinaryOp {
                op, left, right, ..
            } => match op {
                BinaryOperator::Eq => {
                    matches!(left.as_ref(), BoundExpression::ColumnRef(_))
                        && matches!(right.as_ref(), BoundExpression::ColumnRef(_))
                }
                BinaryOperator::And => {
                    Self::is_equi_condition(left) && Self::is_equi_condition(right)
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Extract equi-join keys (left column index, right column index)
    pub fn extract_equi_keys(&self) -> Vec<(usize, usize)> {
        let mut keys = Vec::new();
        if let Some(cond) = &self.condition {
            Self::collect_equi_keys(cond, &mut keys);
        }
        keys
    }

    fn collect_equi_keys(expr: &BoundExpression, keys: &mut Vec<(usize, usize)>) {
        match expr {
            BoundExpression::BinaryOp {
                op, left, right, ..
            } => match op {
                BinaryOperator::Eq => {
                    if let (BoundExpression::ColumnRef(l), BoundExpression::ColumnRef(r)) =
                        (left.as_ref(), right.as_ref())
                    {
                        keys.push((l.column_idx, r.column_idx));
                    }
                }
                BinaryOperator::And => {
                    Self::collect_equi_keys(left, keys);
                    Self::collect_equi_keys(right, keys);
                }
                _ => {}
            },
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnionOp {
    pub all: bool, // UNION ALL vs UNION
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntersectOp {
    pub all: bool,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExceptOp {
    pub all: bool,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateOp {
    pub group_by: Vec<BoundExpression>,
    pub aggregates: Vec<AggregateExpr>,
    pub input_schema: Schema,
    pub output_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub arg: Option<BoundExpression>,
    pub distinct: bool,
    pub output_idx: usize,
}

impl AggregateOp {
    pub fn new(
        group_by: Vec<BoundExpression>,
        aggregates: Vec<AggregateExpr>,
        input_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input_schema,
            output_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortOp {
    pub order_by: Vec<SortExpr>,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: BoundExpression,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortOp {
    pub fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self { order_by, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitOp {
    pub limit: usize,
    pub offset: Option<usize>,
    pub schema: Schema,
}

impl LimitOp {
    pub fn new(limit: usize, offset: Option<usize>, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DistinctOp {
    pub schema: Schema,
}

impl DistinctOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertOp {
    pub table_id: ObjectId,
    pub columns: Vec<usize>,
    pub table_schema: Schema,
}

impl InsertOp {
    pub fn new(table_id: ObjectId, columns: Vec<usize>, table_schema: Schema) -> Self {
        Self {
            table_id,
            columns,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateOp {
    pub table_id: ObjectId,
    pub assignments: Vec<(usize, BoundExpression)>,
    pub table_schema: Schema,
}

impl UpdateOp {
    pub fn new(
        table_id: ObjectId,
        assignments: Vec<(usize, BoundExpression)>,
        table_schema: Schema,
    ) -> Self {
        Self {
            table_id,
            assignments,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteOp {
    pub table_id: ObjectId,
    pub table_schema: Schema,
}

impl DeleteOp {
    pub fn new(table_id: ObjectId, table_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValuesOp {
    pub rows: Vec<Vec<BoundExpression>>,
    pub schema: Schema,
}

impl ValuesOp {
    pub fn new(rows: Vec<Vec<BoundExpression>>, schema: Schema) -> Self {
        Self { rows, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmptyOp {
    pub schema: Schema,
}

impl EmptyOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}
