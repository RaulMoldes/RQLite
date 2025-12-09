//! Physical operators represent concrete execution strategies.
//!
//! Unlike logical operators that describe WHAT the query does, physical operators
//! describe HOW the query will be executed. Each physical operator corresponds to
//! a specific iterator implementation.

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use crate::{
    database::schema::Schema,
    sql::{ast::JoinType, binder::ast::*},
    types::ObjectId,
};

use super::logical::*;
use super::memo::GroupId;
use super::prop::{PhysicalProperties, RequiredProperties};

/// A physical expression in the memo
#[derive(Debug, Clone)]
pub(crate) struct PhysicalExpr {
    pub(crate) id: super::ExprId,
    pub(crate) op: PhysicalOperator,
    pub(crate) children: Vec<GroupId>,
    pub(crate) cost: f64,
    pub(crate) properties: PhysicalProperties,
}

impl PhysicalExpr {
    pub(crate) fn new(op: PhysicalOperator, children: Vec<GroupId>) -> Self {
        let properties = op.provided_properties();
        Self {
            id: super::ExprId::default(),
            op,
            children,
            cost: 0.0,
            properties,
        }
    }
}

/// The final physical plan after optimization
#[derive(Debug, Clone)]
pub(crate) struct PhysicalPlan {
    pub(crate) op: PhysicalOperator,
    pub(crate) children: Vec<PhysicalPlan>,
    pub(crate) cost: f64,
    pub(crate) properties: PhysicalProperties,
}

impl PhysicalPlan {
    /// Get the total cost
    pub(crate) fn total_cost(&self) -> f64 {
        self.cost
    }
}

impl Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.explain())
    }
}

/// Physical operators - concrete execution strategies
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PhysicalOperator {
    // Scan implementations
    SeqScan(SeqScanOp),
    IndexScan(PhysIndexScanOp),
    IndexOnlyScan(IndexOnlyScanOp),

    // Filter
    Filter(PhysFilterOp),

    // Project
    Project(PhysProjectOp),

    // Join implementations
    NestedLoopJoin(NestedLoopJoinOp),
    HashJoin(HashJoinOp),
    MergeJoin(MergeJoinOp),

    // Aggregation implementations
    HashAggregate(HashAggregateOp),
    SortAggregate(SortAggregateOp),

    // Sort implementations
    ExternalSort(ExternalSortOp),
    TopN(TopNOp),

    // Set operations
    HashUnion(HashUnionOp),
    HashIntersect(HashIntersectOp),
    HashExcept(HashExceptOp),

    // Distinct implementations
    HashDistinct(HashDistinctOp),
    SortDistinct(SortDistinctOp),

    // Limit
    Limit(PhysLimitOp),

    // DML
    Insert(PhysInsertOp),
    Update(PhysUpdateOp),
    Delete(PhysDeleteOp),

    // Values (produces rows from literals)
    Values(PhysValuesOp),

    // Empty result
    Empty(PhysEmptyOp),

    // Enforcers (to satisfy required properties)
    Sort(EnforcerSortOp),
    Exchange(ExchangeOp),
}

impl PhysicalOperator {
    /// Get the operator name
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::SeqScan(_) => "SeqScan",
            Self::IndexScan(_) => "IndexScan",
            Self::IndexOnlyScan(_) => "IndexOnlyScan",
            Self::Filter(_) => "Filter",
            Self::Project(_) => "Project",
            Self::NestedLoopJoin(_) => "NestedLoopJoin",
            Self::HashJoin(_) => "HashJoin",
            Self::MergeJoin(_) => "MergeJoin",
            Self::HashAggregate(_) => "HashAggregate",
            Self::SortAggregate(_) => "SortAggregate",
            Self::ExternalSort(_) => "ExternalSort",
            Self::TopN(_) => "TopN",
            Self::HashUnion(_) => "HashUnion",
            Self::HashIntersect(_) => "HashIntersect",
            Self::HashExcept(_) => "HashExcept",
            Self::HashDistinct(_) => "HashDistinct",
            Self::SortDistinct(_) => "SortDistinct",
            Self::Limit(_) => "Limit",
            Self::Insert(_) => "Insert",
            Self::Update(_) => "Update",
            Self::Delete(_) => "Delete",
            Self::Values(_) => "Values",
            Self::Empty(_) => "Empty",
            Self::Sort(_) => "Sort",
            Self::Exchange(_) => "Exchange",
        }
    }

    /// Get the output schema
    pub(crate) fn output_schema(&self) -> &Schema {
        match self {
            Self::SeqScan(op) => &op.output_schema,
            Self::IndexScan(op) => &op.schema,
            Self::IndexOnlyScan(op) => &op.schema,
            Self::Filter(op) => &op.schema,
            Self::Project(op) => &op.output_schema,
            Self::NestedLoopJoin(op) => &op.output_schema,
            Self::HashJoin(op) => &op.output_schema,
            Self::MergeJoin(op) => &op.output_schema,
            Self::HashAggregate(op) => &op.output_schema,
            Self::SortAggregate(op) => &op.output_schema,
            Self::ExternalSort(op) => &op.schema,
            Self::TopN(op) => &op.schema,
            Self::HashUnion(op) => &op.schema,
            Self::HashIntersect(op) => &op.schema,
            Self::HashExcept(op) => &op.schema,
            Self::HashDistinct(op) => &op.schema,
            Self::SortDistinct(op) => &op.schema,
            Self::Limit(op) => &op.schema,
            Self::Insert(op) => &op.table_schema,
            Self::Update(op) => &op.table_schema,
            Self::Delete(op) => &op.table_schema,
            Self::Values(op) => &op.schema,
            Self::Empty(op) => &op.schema,
            Self::Sort(op) => &op.schema,
            Self::Exchange(op) => &op.schema,
        }
    }

    /// Get the properties provided by this operator
    pub(crate) fn provided_properties(&self) -> PhysicalProperties {
        match self {
            // ExternalSort and EnforcerSort are different types, handle separately
            Self::ExternalSort(op) => PhysicalProperties {
                ordering: op
                    .order_by
                    .iter()
                    .map(|s| (s.expr.clone(), s.asc))
                    .collect(),
                ..Default::default()
            },
            Self::Sort(op) => PhysicalProperties {
                ordering: op
                    .order_by
                    .iter()
                    .map(|s| (s.expr.clone(), s.asc))
                    .collect(),
                ..Default::default()
            },
            Self::MergeJoin(op) => PhysicalProperties {
                ordering: op.output_ordering.clone(),
                ..Default::default()
            },
            _ => PhysicalProperties::default(),
        }
    }

    /// Get required properties for a child
    pub(crate) fn required_child_properties(
        &self,
        child_idx: usize,
        _required: &RequiredProperties,
    ) -> RequiredProperties {
        match self {
            Self::MergeJoin(op) => {
                // Merge join requires both inputs to be sorted
                if child_idx == 0 {
                    RequiredProperties {
                        ordering: op.left_ordering.clone(),
                        ..Default::default()
                    }
                } else {
                    RequiredProperties {
                        ordering: op.right_ordering.clone(),
                        ..Default::default()
                    }
                }
            }
            Self::SortAggregate(op) => {
                // Sort aggregate requires input sorted by group-by columns
                RequiredProperties {
                    ordering: op.group_ordering.clone(),
                    ..Default::default()
                }
            }
            _ => RequiredProperties::default(),
        }
    }
}

// Scan operators.
/// Sequential scan (reads all rows from a table).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeqScanOp {
    pub(crate) table_id: ObjectId,
    pub(crate) table_schema: Schema, // Input table schema for reading tuples.
    pub(crate) output_schema: Schema,
    pub(crate) columns: Option<Vec<usize>>,
    pub(crate) predicate: Option<BoundExpression>,
}

impl SeqScanOp {
    pub(crate) fn new(table_id: ObjectId, input_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema: input_schema.clone(),
            output_schema: input_schema, // By default, the output schema matches the input schema
            columns: None,
            predicate: None,
        }
    }

    pub(crate) fn with_columns(mut self, columns: Vec<usize>) -> Self {
        // Build projected schema from selected columns
        let projected_cols: Vec<_> = columns
            .iter()
            .map(|&idx| self.table_schema.columns()[idx].clone())
            .collect();
        self.output_schema = Schema::from_columns(&projected_cols, 0);
        self.columns = Some(columns);
        self
    }

    pub(crate) fn with_predicate(mut self, predicate: BoundExpression) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Get the mapping from output column index to table column index
    pub(crate) fn column_mapping(&self) -> Option<&Vec<usize>> {
        self.columns.as_ref()
    }

    /// Get inverse mapping: table column index -> output column index
    pub(crate) fn inverse_column_mapping(&self) -> Option<HashMap<usize, usize>> {
        self.columns.as_ref().map(|cols| {
            cols.iter()
                .enumerate()
                .map(|(output_idx, &table_idx)| (table_idx, output_idx))
                .collect()
        })
    }
}

/// Index scan (uses an index to find rows)
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysIndexScanOp {
    pub(crate) table_id: ObjectId,
    pub(crate) index_id: ObjectId,
    pub(crate) schema: Schema,
    pub(crate) index_columns: Vec<usize>,
    pub(crate) range_start: Option<BoundExpression>,
    pub(crate) range_end: Option<BoundExpression>,
    pub(crate) residual: Option<BoundExpression>,
}

impl From<IndexScanOp> for PhysIndexScanOp {
    fn from(value: IndexScanOp) -> Self {
        Self {
            table_id: value.table_id,
            index_id: value.index_id,
            schema: value.schema,
            index_columns: value.index_columns,
            range_start: value.range_start,
            range_end: value.range_end,
            residual: value.residual_predicate,
        }
    }
}

impl PhysIndexScanOp {
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
            residual: None,
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
        self.residual = Some(pred);
        self
    }
}

/// Index-only scan (returns data directly from index)
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexOnlyScanOp {
    pub(crate) table_id: ObjectId,
    pub(crate) index_id: ObjectId,
    pub(crate) schema: Schema,
    pub(crate) index_columns: Vec<usize>,
    pub(crate) range_start: Option<BoundExpression>,
    pub(crate) range_end: Option<BoundExpression>,
}

// Filter and Project

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysFilterOp {
    pub(crate) predicate: BoundExpression,
    pub(crate) schema: Schema,
}

impl PhysFilterOp {
    pub(crate) fn new(predicate: BoundExpression, schema: Schema) -> Self {
        Self { predicate, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysProjectOp {
    pub(crate) expressions: Vec<ProjectExpr>,
    pub(crate) input_schema: Schema,
    pub(crate) output_schema: Schema,
}

impl PhysProjectOp {
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

// Join Operators
/// Nested loop join - O(n*m) complexity, works for all join types
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NestedLoopJoinOp {
    pub(crate) join_type: JoinType,
    pub(crate) condition: Option<BoundExpression>,
    pub(crate) output_schema: Schema,
}

impl NestedLoopJoinOp {
    pub(crate) fn new(
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

/// Hash join - O(n+m) for equi-joins
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashJoinOp {
    pub(crate) join_type: JoinType,
    pub(crate) left_keys: Vec<BoundExpression>,
    pub(crate) right_keys: Vec<BoundExpression>,
    pub(crate) condition: Option<BoundExpression>,
    pub(crate) output_schema: Schema,
}

impl HashJoinOp {
    pub(crate) fn new(
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

/// Merge join - O(n+m) for sorted inputs with equi-join
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MergeJoinOp {
    pub(crate) join_type: JoinType,
    pub(crate) left_keys: Vec<BoundExpression>,
    pub(crate) right_keys: Vec<BoundExpression>,
    pub(crate) left_ordering: Vec<OrderingSpec>,
    pub(crate) right_ordering: Vec<OrderingSpec>,
    pub(crate) output_ordering: Vec<(BoundExpression, bool)>,
    pub(crate) output_schema: Schema,
}

// Aggregations
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashAggregateOp {
    pub(crate) group_by: Vec<BoundExpression>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) input_schema: Schema,
    pub(crate) output_schema: Schema,
}

impl HashAggregateOp {
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
pub(crate) struct SortAggregateOp {
    pub(crate) group_by: Vec<BoundExpression>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) group_ordering: Vec<OrderingSpec>,
    pub(crate) input_schema: Schema,
    pub(crate) output_schema: Schema,
}

// Sort Operators

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExternalSortOp {
    pub(crate) order_by: Vec<SortExpr>,
    pub(crate) schema: Schema,
    pub(crate) memory_limit: Option<usize>,
}

impl ExternalSortOp {
    pub(crate) fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self {
            order_by,
            schema,
            memory_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopNOp {
    pub(crate) order_by: Vec<SortExpr>,
    pub(crate) limit: usize,
    pub(crate) offset: Option<usize>,
    pub(crate) schema: Schema,
}

impl TopNOp {
    pub(crate) fn new(
        order_by: Vec<SortExpr>,
        limit: usize,
        offset: Option<usize>,
        schema: Schema,
    ) -> Self {
        Self {
            order_by,
            limit,
            offset,
            schema,
        }
    }
}

// Set operations

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashUnionOp {
    pub(crate) all: bool,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashIntersectOp {
    pub(crate) all: bool,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashExceptOp {
    pub(crate) all: bool,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HashDistinctOp {
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortDistinctOp {
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysLimitOp {
    pub(crate) limit: usize,
    pub(crate) offset: Option<usize>,
    pub(crate) schema: Schema,
}

impl PhysLimitOp {
    pub(crate) fn new(limit: usize, offset: Option<usize>, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            schema,
        }
    }
}

// DML Physical operators

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysInsertOp {
    pub(crate) table_id: ObjectId,
    pub(crate) columns: Vec<usize>,
    pub(crate) table_schema: Schema,
}

impl PhysInsertOp {
    pub(crate) fn new(table_id: ObjectId, columns: Vec<usize>, table_schema: Schema) -> Self {
        Self {
            table_id,
            columns,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysUpdateOp {
    pub(crate) table_id: ObjectId,
    pub(crate) assignments: Vec<(usize, BoundExpression)>,
    pub(crate) table_schema: Schema,
}

impl PhysUpdateOp {
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
pub(crate) struct PhysDeleteOp {
    pub(crate) table_id: ObjectId,
    pub(crate) table_schema: Schema,
}

impl PhysDeleteOp {
    pub(crate) fn new(table_id: ObjectId, table_schema: Schema) -> Self {
        Self {
            table_id,
            table_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysValuesOp {
    pub(crate) rows: Vec<Vec<BoundExpression>>,
    pub(crate) schema: Schema,
}

impl PhysValuesOp {
    pub(crate) fn new(rows: Vec<Vec<BoundExpression>>, schema: Schema) -> Self {
        Self { rows, schema }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysEmptyOp {
    pub(crate) schema: Schema,
}

impl PhysEmptyOp {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Sort enforcer adds sorting to satisfy ordering requirements
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EnforcerSortOp {
    pub(crate) order_by: Vec<SortExpr>,
    pub(crate) schema: Schema,
}

impl EnforcerSortOp {
    pub(crate) fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self { order_by, schema }
    }
}

/// Exchange enforcer redistributes data (for parallel/distributed execution)
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExchangeOp {
    pub(crate) distribution: Distribution,
    pub(crate) schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Distribution {
    Any,
    Single,
    Hash(Vec<usize>),
    Broadcast,
}

/// Ordering specification that doesn't require Eq/Hash on BoundExpression
/// Used for RequiredProperties where we need Eq + Hash
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OrderingSpec {
    /// Column index in the schema
    pub(crate) column_idx: usize,
    /// Ascending order
    pub(crate) ascending: bool,
    /// Nulls first
    pub(crate) nulls_first: bool,
}

impl OrderingSpec {
    pub(crate) fn new(column_idx: usize, ascending: bool) -> Self {
        Self {
            column_idx,
            ascending,
            nulls_first: ascending,
        }
    }

    pub(crate) fn asc(column_idx: usize) -> Self {
        Self::new(column_idx, true)
    }

    pub(crate) fn desc(column_idx: usize) -> Self {
        Self::new(column_idx, false)
    }

    pub(crate) fn with_nulls_first(mut self, nulls_first: bool) -> Self {
        self.nulls_first = nulls_first;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{database::schema::Column, types::DataTypeKind};

    fn test_schema() -> Schema {
        Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::Int, "id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
            ],
            1,
        )
    }

    #[test]
    fn test_physical_plan_explain() {
        let schema = test_schema();
        let plan = PhysicalPlan {
            op: PhysicalOperator::SeqScan(SeqScanOp::new(ObjectId::new(), schema.clone())),
            children: vec![],
            cost: 100.0,
            properties: PhysicalProperties::default(),
        };

        let explain = plan.explain();

        println!("EXPLAIN {explain}");
        assert!(explain.contains("SeqScan"));
        assert!(explain.contains("users"));
    }

    #[test]
    fn test_hash_join_output_schema() {
        let left_schema = test_schema();
        let right_schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::Int, "order_id", None),
                Column::new_unindexed(DataTypeKind::Double, "amount", None),
            ],
            1,
        );

        let mut output_cols = left_schema.columns().clone();
        output_cols.extend(right_schema.columns().clone());
        let output_schema = Schema::from_columns(&output_cols, 0);

        let join = HashJoinOp::new(JoinType::Inner, vec![], vec![], None, output_schema.clone());

        assert_eq!(
            PhysicalOperator::HashJoin(join)
                .output_schema()
                .columns()
                .len(),
            4
        );
    }
}
