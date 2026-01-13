//! Logical operators represent the logical structure of a query plan.
//!
//! These operators describe WHAT the query does, not HOW it will be executed.

use crate::{
    DataType,
    schema::Schema,
    sql::{
        binder::bounds::*,
        parser::ast::{BinaryOperator, JoinType},
    },
    types::ObjectId,
};

use super::prop::LogicalProperties;
use super::{ExprId, memo::GroupId};

/// A logical expression in the memo.
#[derive(Debug, Clone)]
pub struct LogicalExpr {
    pub id: ExprId,
    pub op: LogicalOperator,
    pub children: Vec<GroupId>,
    pub properties: LogicalProperties,
}

impl LogicalExpr {
    pub fn new(op: LogicalOperator, children: Vec<GroupId>) -> Self {
        Self {
            id: super::ExprId::default(),
            op,
            children,
            properties: LogicalProperties::default(),
        }
    }

    pub fn with_properties(mut self, props: LogicalProperties) -> Self {
        self.properties = props;
        self
    }
}

/// Logical operators.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    TableScan(TableScanOp),
    IndexScan(IndexScanOp),
    Filter(FilterOp),
    Project(ProjectOp),
    Join(JoinOp),
    Aggregate(AggregateOp),
    Sort(SortOp),
    Limit(LimitOp),
    Distinct(DistinctOp),
    Insert(InsertOp),
    Update(UpdateOp),
    Delete(DeleteOp),
    Values(ValuesOp),
    Empty(EmptyOp),
    Materialize(MaterializeOp),
}

impl LogicalOperator {
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::TableScan(op) => &op.schema,
            Self::IndexScan(op) => &op.schema,
            Self::Filter(op) => &op.input_schema,
            Self::Project(op) => &op.output_schema,
            Self::Join(op) => &op.output_schema,
            Self::Aggregate(op) => &op.output_schema,
            Self::Sort(op) => &op.schema,
            Self::Limit(op) => &op.schema,
            Self::Distinct(op) => &op.schema,
            Self::Insert(op) => &op.table_schema,
            Self::Update(op) => &op.table_schema,
            Self::Delete(op) => &op.table_schema,
            Self::Values(op) => &op.schema,
            Self::Empty(op) => &op.schema,
            Self::Materialize(op) => &op.schema,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::TableScan(_) | Self::IndexScan(_) | Self::Values(_) | Self::Empty(_)
        )
    }

    pub fn arity(&self) -> usize {
        match self {
            Self::TableScan(_)
            | Self::IndexScan(_)
            | Self::Values(_)
            | Self::Empty(_)
            | Self::Materialize(_) => 0,
            Self::Filter(_)
            | Self::Project(_)
            | Self::Sort(_)
            | Self::Limit(_)
            | Self::Distinct(_)
            | Self::Aggregate(_)
            | Self::Insert(_)
            | Self::Update(_)
            | Self::Delete(_) => 1,
            Self::Join(_) => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MaterializeOp {
    pub(crate) schema: Schema,
}

impl MaterializeOp {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Logical plan tree (for display/debugging).
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub op: LogicalOperator,
    pub children: Vec<LogicalPlan>,
    pub properties: LogicalProperties,
}

/// Table scan operator.
#[derive(Debug, Clone, PartialEq)]
pub struct TableScanOp {
    pub table_id: ObjectId,
    pub table_name: String,
    pub schema: Schema,
    pub table_schema: Schema,
    pub columns: Option<Vec<usize>>,
    pub predicate: Option<BoundExpression>,
}

impl TableScanOp {
    pub fn new(table_id: ObjectId, table_name: String, schema: Schema) -> Self {
        Self {
            table_id,
            table_name,
            table_schema: schema.clone(),
            schema,
            columns: None,
            predicate: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        let projected_cols: Vec<_> = columns
            .iter()
            .filter_map(|&idx| self.table_schema.column(idx).cloned())
            .collect();
        self.schema = Schema::new_table_with_num_keys(projected_cols, 0);
        self.columns = Some(columns);
        self
    }

    pub fn with_predicate(mut self, predicate: BoundExpression) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Index scan operator.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanOp {
    pub table_id: ObjectId,
    pub index_id: ObjectId,
    pub schema: Schema,
    pub index_columns: Vec<usize>,
    pub range_start: Option<Vec<IndexRangeBound>>,
    pub range_end: Option<Vec<IndexRangeBound>>,
    pub residual_predicate: Option<BoundExpression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexRangeBound {
    pub inclusive: bool,
    pub value: DataType,
    pub col_idx: usize,
}

impl IndexScanOp {
    pub fn new(
        table_id: ObjectId,
        index_id: ObjectId,
        schema: Schema,
        index_columns: Vec<usize>,
    ) -> Self {
        Self {
            table_id,
            index_id,
            schema,
            index_columns,
            range_start: None,
            range_end: None,
            residual_predicate: None,
        }
    }

    pub fn with_range(
        mut self,
        start: Option<Vec<IndexRangeBound>>,
        end: Option<Vec<IndexRangeBound>>,
    ) -> Self {
        self.range_start = start;
        self.range_end = end;
        self
    }

    pub fn with_residual(mut self, pred: BoundExpression) -> Self {
        self.residual_predicate = Some(pred);
        self
    }
}

/// Filter operator.
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

/// Project operator.
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

/// Join operator.
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
        let mut cols = left.clone().into_column_list();
        cols.extend(right.clone().into_column_list());
        Schema::new_table_with_num_keys(cols, 0)
    }

    /// Checks if this is an equi-join.
    pub fn is_equi_join(&self) -> bool {
        self.condition
            .as_ref()
            .map_or(false, |c| Self::is_equi_condition(c))
    }

    fn is_equi_condition(expr: &BoundExpression) -> bool {
        match expr {
            BoundExpression::BinaryOp {
                op, left, right, ..
            } => match op {
                BinaryOperator::Eq => {
                    matches!(left.as_ref(), BoundExpression::ColumnBinding(_))
                        && matches!(right.as_ref(), BoundExpression::ColumnBinding(_))
                }
                BinaryOperator::And => {
                    Self::is_equi_condition(left) && Self::is_equi_condition(right)
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Extracts equi-join keys as (left_col, right_col) pairs.
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
                    if let (BoundExpression::ColumnBinding(l), BoundExpression::ColumnBinding(r)) =
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

/// Aggregate operator.
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

/// Sort operator.
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

/// Limit operator.
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

/// Distinct operator.
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctOp {
    pub schema: Schema,
}

impl DistinctOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Insert operator.
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

/// Update operator.
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

/// Delete operator.
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

/// Values operator (literal rows).
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

/// Empty operator (no rows).
#[derive(Debug, Clone, PartialEq)]
pub struct EmptyOp {
    pub schema: Schema,
}

impl EmptyOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}
