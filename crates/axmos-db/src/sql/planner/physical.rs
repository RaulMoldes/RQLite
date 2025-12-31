//! Physical operators represent concrete execution strategies.
//!
//! These operators describe HOW the query will be executed.

use crate::{
    schema::Schema,
    sql::{binder::bounds::*, parser::ast::JoinType},
    types::ObjectId,
};

use super::memo::GroupId;
use super::prop::{PhysicalProperties, RequiredProperties};
use super::{ExprId, logical::*};

/// A physical expression in the memo.
#[derive(Debug, Clone)]
pub struct PhysicalExpr {
    pub id: ExprId,
    pub op: PhysicalOperator,
    pub children: Vec<GroupId>,
    pub cost: f64,
    pub properties: PhysicalProperties,
}

impl PhysicalExpr {
    pub fn new(op: PhysicalOperator, children: Vec<GroupId>) -> Self {
        let properties = op.provided_properties();
        Self {
            id: ExprId::default(),
            op,
            children,
            cost: 0.0,
            properties,
        }
    }
}

/// The final physical plan after optimization.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub op: PhysicalOperator,
    pub children: Vec<PhysicalPlan>,
    pub cost: f64,
    pub properties: PhysicalProperties,
}

impl PhysicalPlan {
    pub fn total_cost(&self) -> f64 {
        self.cost
    }
}

/// Physical operators.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalOperator {
    // Scans
    SeqScan(SeqScanOp),
    IndexScan(PhysIndexScanOp),

    // Relational
    Filter(PhysFilterOp),
    Project(PhysProjectOp),

    // Joins
    NestedLoopJoin(NestedLoopJoinOp),
    HashJoin(HashJoinOp),
    MergeJoin(MergeJoinOp),

    // Aggregation
    HashAggregate(HashAggregateOp),

    // Sort
    Sort(SortOp),

    // Other
    Limit(PhysLimitOp),
    Distinct(HashDistinctOp),

    // DML
    Insert(PhysInsertOp),
    Update(PhysUpdateOp),
    Delete(PhysDeleteOp),

    // Literal
    Values(PhysValuesOp),
    Empty(PhysEmptyOp),
    Materialize(PhysMaterializeOp),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysMaterializeOp {
    pub(crate) schema: Schema,
}

impl PhysMaterializeOp {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

impl PhysicalOperator {
    /// Returns the name of each operator.
    pub fn name(&self) -> &'static str {
        match self {
            Self::SeqScan(_) => "SeqScan",
            Self::IndexScan(_) => "IndexScan",
            Self::Filter(_) => "Filter",
            Self::Project(_) => "Project",
            Self::NestedLoopJoin(_) => "NestedLoopJoin",
            Self::HashJoin(_) => "HashJoin",
            Self::MergeJoin(_) => "MergeJoin",
            Self::HashAggregate(_) => "HashAggregate",
            Self::Sort(_) => "Sort",
            Self::Limit(_) => "Limit",
            Self::Distinct(_) => "Distinct",
            Self::Insert(_) => "Insert",
            Self::Update(_) => "Update",
            Self::Delete(_) => "Delete",
            Self::Values(_) => "Values",
            Self::Empty(_) => "Empty",
            Self::Materialize(_) => "Materialize",
        }
    }

    /// Returns a reference to the output schema produced by each physical operator.
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::SeqScan(op) => &op.output_schema,
            Self::IndexScan(op) => &op.schema,
            Self::Filter(op) => &op.schema,
            Self::Project(op) => &op.output_schema,
            Self::NestedLoopJoin(op) => &op.output_schema,
            Self::HashJoin(op) => &op.output_schema,
            Self::MergeJoin(op) => &op.output_schema,
            Self::HashAggregate(op) => &op.output_schema,
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

    /// Sort and merge join operators are the only ones that currently provide some kind of ordering
    pub fn provided_properties(&self) -> PhysicalProperties {
        match self {
            Self::Sort(op) => PhysicalProperties {
                ordering: op
                    .order_by
                    .iter()
                    .map(|s| (s.expr.clone(), s.asc))
                    .collect(),
            },
            Self::MergeJoin(op) => PhysicalProperties {
                ordering: op.output_ordering.clone(),
            },
            _ => PhysicalProperties::default(),
        }
    }

    /// Required child properties by each operator.
    pub fn required_child_properties(
        &self,
        child_idx: usize,
        _required: &RequiredProperties,
    ) -> RequiredProperties {
        match self {
            Self::MergeJoin(op) => {
                if child_idx == 0 {
                    RequiredProperties {
                        ordering: op.left_ordering.clone(),
                    }
                } else {
                    RequiredProperties {
                        ordering: op.right_ordering.clone(),
                    }
                }
            }
            _ => RequiredProperties::default(),
        }
    }
}

/// Sequential scan.
#[derive(Debug, Clone, PartialEq)]
pub struct SeqScanOp {
    pub table_id: ObjectId,
    pub table_schema: Schema,
    pub output_schema: Schema,
    pub columns: Option<Vec<usize>>,
    pub predicate: Option<BoundExpression>,
}

impl SeqScanOp {
    pub fn new(table_id: ObjectId, input_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema: input_schema.clone(),
            output_schema: input_schema,
            columns: None,
            predicate: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        let projected_cols: Vec<_> = columns
            .iter()
            .filter_map(|&idx| self.table_schema.column(idx).cloned())
            .collect();
        self.output_schema = Schema::new_table_with_num_keys(projected_cols, 0);
        self.columns = Some(columns);
        self
    }

    pub fn with_predicate(mut self, predicate: BoundExpression) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Index scan.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysIndexScanOp {
    pub table_id: ObjectId,
    pub index_id: ObjectId,
    pub schema: Schema,
    pub index_columns: Vec<usize>,
    pub range_start: Option<BoundExpression>,
    pub range_end: Option<BoundExpression>,
    pub residual: Option<BoundExpression>,
}

impl From<IndexScanOp> for PhysIndexScanOp {
    fn from(op: IndexScanOp) -> Self {
        Self {
            table_id: op.table_id,
            index_id: op.index_id,
            schema: op.schema,
            index_columns: op.index_columns,
            range_start: op.range_start,
            range_end: op.range_end,
            residual: op.residual_predicate,
        }
    }
}

impl PhysIndexScanOp {
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
            residual: None,
        }
    }
}

/// Physical filter.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysFilterOp {
    pub predicate: BoundExpression,
    pub schema: Schema,
}

impl PhysFilterOp {
    pub fn new(predicate: BoundExpression, schema: Schema) -> Self {
        Self { predicate, schema }
    }
}

/// Physical project.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysProjectOp {
    pub expressions: Vec<ProjectExpr>,
    pub input_schema: Schema,
    pub output_schema: Schema,
}

impl PhysProjectOp {
    pub fn new(expressions: Vec<ProjectExpr>, input_schema: Schema, output_schema: Schema) -> Self {
        Self {
            expressions,
            input_schema,
            output_schema,
        }
    }
}

/// Nested loop join.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedLoopJoinOp {
    pub join_type: JoinType,
    pub condition: Option<BoundExpression>,
    pub output_schema: Schema,
}

impl NestedLoopJoinOp {
    pub fn new(
        join_type: JoinType,
        condition: Option<BoundExpression>,
        output_schema: Schema,
    ) -> Self {
        Self {
            join_type,
            condition,
            output_schema,
        }
    }
}

/// Hash join.
#[derive(Debug, Clone, PartialEq)]
pub struct HashJoinOp {
    pub join_type: JoinType,
    pub left_keys: Vec<BoundExpression>,
    pub right_keys: Vec<BoundExpression>,
    pub condition: Option<BoundExpression>,
    pub output_schema: Schema,
}

impl HashJoinOp {
    pub fn new(
        join_type: JoinType,
        left_keys: Vec<BoundExpression>,
        right_keys: Vec<BoundExpression>,
        condition: Option<BoundExpression>,
        output_schema: Schema,
    ) -> Self {
        Self {
            join_type,
            left_keys,
            right_keys,
            condition,
            output_schema,
        }
    }
}

/// Merge join.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeJoinOp {
    pub join_type: JoinType,
    pub left_keys: Vec<BoundExpression>,
    pub right_keys: Vec<BoundExpression>,
    pub left_ordering: Vec<OrderingSpec>,
    pub right_ordering: Vec<OrderingSpec>,
    pub output_ordering: Vec<(BoundExpression, bool)>,
    pub output_schema: Schema,
}

/// Hash aggregate.
#[derive(Debug, Clone, PartialEq)]
pub struct HashAggregateOp {
    pub group_by: Vec<BoundExpression>,
    pub aggregates: Vec<AggregateExpr>,
    pub input_schema: Schema,
    pub output_schema: Schema,
}

impl HashAggregateOp {
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

/// Physical limit.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysLimitOp {
    pub limit: usize,
    pub offset: Option<usize>,
    pub schema: Schema,
}

impl PhysLimitOp {
    pub fn new(limit: usize, offset: Option<usize>, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            schema,
        }
    }
}

/// Hash distinct.
#[derive(Debug, Clone, PartialEq)]
pub struct HashDistinctOp {
    pub schema: Schema,
}

impl HashDistinctOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Physical insert.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysInsertOp {
    pub table_id: ObjectId,
    pub columns: Vec<usize>,
    pub table_schema: Schema,
}

impl PhysInsertOp {
    pub fn new(table_id: ObjectId, columns: Vec<usize>, table_schema: Schema) -> Self {
        Self {
            table_id,
            columns,
            table_schema,
        }
    }
}

/// Physical update.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysUpdateOp {
    pub table_id: ObjectId,
    pub assignments: Vec<(usize, BoundExpression)>,
    pub table_schema: Schema,
}

impl PhysUpdateOp {
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

/// Physical delete.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysDeleteOp {
    pub table_id: ObjectId,
    pub table_schema: Schema,
}

impl PhysDeleteOp {
    pub fn new(table_id: ObjectId, table_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema,
        }
    }
}

/// Physical values.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysValuesOp {
    pub rows: Vec<Vec<BoundExpression>>,
    pub schema: Schema,
}

impl PhysValuesOp {
    pub fn new(rows: Vec<Vec<BoundExpression>>, schema: Schema) -> Self {
        Self { rows, schema }
    }
}

/// Physical empty.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysEmptyOp {
    pub schema: Schema,
}

impl PhysEmptyOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Ordering specification (hashable, for RequiredProperties).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderingSpec {
    pub column_idx: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl OrderingSpec {
    pub fn new(column_idx: usize, ascending: bool) -> Self {
        Self {
            column_idx,
            ascending,
            nulls_first: ascending,
        }
    }

    pub fn asc(column_idx: usize) -> Self {
        Self::new(column_idx, true)
    }

    pub fn desc(column_idx: usize) -> Self {
        Self::new(column_idx, false)
    }
}
