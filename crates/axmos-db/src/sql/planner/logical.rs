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
pub(crate) struct LogicalExpr {
    pub(crate) id: super::ExprId,
    pub(crate) op: LogicalOperator,
    pub(crate) children: Vec<GroupId>,
    pub(crate) properties: LogicalProperties,
}

impl LogicalExpr {
    pub(crate) fn new(op: LogicalOperator, children: Vec<GroupId>) -> Self {
        let properties = LogicalProperties::default();
        Self {
            id: super::ExprId::default(),
            op,
            children,
            properties,
        }
    }

    pub(crate) fn with_properties(mut self, props: LogicalProperties) -> Self {
        self.properties = props;
        self
    }
}

/// Logical operators - describe the logical structure of a query
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalOperator {
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
pub(crate) struct LogicalPlan {
    pub(crate) op: LogicalOperator,
    pub(crate) children: Vec<LogicalPlan>,
    pub(crate) properties: LogicalProperties,
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.explain())
    }
}

impl LogicalOperator {
    /// Get the output schema of this operator
    pub(crate) fn output_schema(&self) -> &Schema {
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
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::TableScan(_) | Self::IndexScan(_) | Self::Values(_) | Self::Empty(_)
        )
    }

    /// Get the arity (number of children) of this operator
    pub(crate) fn arity(&self) -> usize {
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
pub(crate) struct TableScanOp {
    pub(crate) table_id: ObjectId,
    pub(crate) table_name: String,
    pub(crate) schema: Schema, // Output schema (may be projected)
    pub(crate) table_schema: Schema,
    /// Columns to read (if None, read all)
    pub(crate) columns: Option<Vec<usize>>,
    /// Inline filter predicate (pushed down)
    pub(crate) predicate: Option<BoundExpression>,
}

impl TableScanOp {
    pub(crate) fn new(table_id: ObjectId, table_name: String, schema: Schema) -> Self {
        Self {
            table_id,
            table_name,
            table_schema: schema.clone(),
            schema,
            columns: None,
            predicate: None,
        }
    }

    pub(crate) fn with_columns(mut self, columns: Vec<usize>) -> Self {
        let projected_cols: Vec<_> = columns
            .iter()
            .map(|&idx| self.table_schema.columns()[idx].clone())
            .collect();
        self.schema = Schema::from_columns(&projected_cols, 0);
        self.columns = Some(columns);
        self
    }

    pub(crate) fn with_predicate(mut self, predicate: BoundExpression) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Index scan operator - reads rows using an index
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexScanOp {
    pub(crate) table_id: ObjectId,
    pub(crate) index_id: ObjectId,

    pub(crate) schema: Schema,
    /// The indexed columns
    pub(crate) index_columns: Vec<usize>,
    /// Range predicates on the index
    pub(crate) range_start: Option<BoundExpression>,
    pub(crate) range_end: Option<BoundExpression>,
    /// Additional filter after index lookup
    pub(crate) residual_predicate: Option<BoundExpression>,
}

impl IndexScanOp {
    pub(crate) fn new(
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

    pub(crate) fn with_range_start(mut self, range_start: BoundExpression) -> Self {
        self.range_start = Some(range_start);
        self
    }

    pub(crate) fn with_range_end(mut self, range_end: BoundExpression) -> Self {
        self.range_end = Some(range_end);
        self
    }

    pub(crate) fn with_predicate(mut self, pred: BoundExpression) -> Self {
        self.residual_predicate = Some(pred);
        self
    }
}

/// Filter operator - applies a predicate to filter rows
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FilterOp {
    pub(crate) predicate: BoundExpression,
    pub(crate) input_schema: Schema,
}

impl FilterOp {
    pub(crate) fn new(predicate: BoundExpression, input_schema: Schema) -> Self {
        Self {
            predicate,
            input_schema,
        }
    }
}

/// Project operator - selects and computes columns
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectOp {
    pub(crate) expressions: Vec<ProjectExpr>,
    pub(crate) input_schema: Schema,
    pub(crate) output_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectExpr {
    pub(crate) expr: BoundExpression,
    pub(crate) alias: Option<String>,
}

impl ProjectOp {
    pub(crate) fn new(
        expressions: Vec<ProjectExpr>,
        input_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            expressions,
            input_schema,
            output_schema,
        }
    }
}

/// Join operator
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinOp {
    pub(crate) join_type: JoinType,
    pub(crate) condition: Option<BoundExpression>,
    pub(crate) left_schema: Schema,
    pub(crate) right_schema: Schema,
    pub(crate) output_schema: Schema,
}

impl JoinOp {
    pub(crate) fn new(
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
    pub(crate) fn is_equi_join(&self) -> bool {
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
    pub(crate) fn extract_equi_keys(&self) -> Vec<(usize, usize)> {
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
pub(crate) struct UnionOp {
    pub(crate) all: bool, // UNION ALL vs UNION
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IntersectOp {
    pub(crate) all: bool,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExceptOp {
    pub(crate) all: bool,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateOp {
    pub(crate) group_by: Vec<BoundExpression>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) input_schema: Schema,
    pub(crate) output_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateExpr {
    pub(crate) func: AggregateFunction,
    pub(crate) arg: Option<BoundExpression>,
    pub(crate) distinct: bool,
    pub(crate) output_idx: usize,
}

impl AggregateOp {
    pub(crate) fn new(
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
pub(crate) struct SortOp {
    pub(crate) order_by: Vec<SortExpr>,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortExpr {
    pub(crate) expr: BoundExpression,
    pub(crate) asc: bool,
    pub(crate) nulls_first: bool,
}

impl SortOp {
    pub(crate) fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self { order_by, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LimitOp {
    pub(crate) limit: usize,
    pub(crate) offset: Option<usize>,
    pub(crate) schema: Schema,
}

impl LimitOp {
    pub(crate) fn new(limit: usize, offset: Option<usize>, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DistinctOp {
    pub(crate) schema: Schema,
}

impl DistinctOp {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertOp {
    pub(crate) table_id: ObjectId,
    pub(crate) columns: Vec<usize>,
    pub(crate) table_schema: Schema,
}

impl InsertOp {
    pub(crate) fn new(table_id: ObjectId, columns: Vec<usize>, table_schema: Schema) -> Self {
        Self {
            table_id,
            columns,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateOp {
    pub(crate) table_id: ObjectId,
    pub(crate) assignments: Vec<(usize, BoundExpression)>,
    pub(crate) table_schema: Schema,
}

impl UpdateOp {
    pub(crate) fn new(
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
pub(crate) struct DeleteOp {
    pub(crate) table_id: ObjectId,
    pub(crate) table_schema: Schema,
}

impl DeleteOp {
    pub(crate) fn new(table_id: ObjectId, table_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValuesOp {
    pub(crate) rows: Vec<Vec<BoundExpression>>,
    pub(crate) schema: Schema,
}

impl ValuesOp {
    pub(crate) fn new(rows: Vec<Vec<BoundExpression>>, schema: Schema) -> Self {
        Self { rows, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EmptyOp {
    pub(crate) schema: Schema,
}

impl EmptyOp {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}
