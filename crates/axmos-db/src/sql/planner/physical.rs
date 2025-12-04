//! Physical operators represent concrete execution strategies.
//!
//! Unlike logical operators that describe WHAT the query does, physical operators
//! describe HOW the query will be executed. Each physical operator corresponds to
//! a specific iterator implementation.

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

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
pub struct PhysicalExpr {
    pub id: super::ExprId,
    pub op: PhysicalOperator,
    pub children: Vec<GroupId>,
    pub cost: f64,
    pub properties: PhysicalProperties,
}

impl PhysicalExpr {
    pub fn new(op: PhysicalOperator, children: Vec<GroupId>) -> Self {
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
pub struct PhysicalPlan {
    pub op: PhysicalOperator,
    pub children: Vec<PhysicalPlan>,
    pub cost: f64,
    pub properties: PhysicalProperties,
}

impl PhysicalPlan {
    /// Get the total cost
    pub fn total_cost(&self) -> f64 {
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
pub enum PhysicalOperator {
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
    pub fn name(&self) -> &'static str {
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
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::SeqScan(op) => &op.schema,
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
    pub fn provided_properties(&self) -> PhysicalProperties {
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
    pub fn required_child_properties(
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
pub struct SeqScanOp {
    pub table_id: ObjectId,
    pub table_name: String,
    pub schema: Schema,
    pub columns: Option<Vec<usize>>,
    pub predicate: Option<BoundExpression>,
}

impl SeqScanOp {
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

/// Index scan (uses an index to find rows)
#[derive(Debug, Clone, PartialEq)]
pub struct PhysIndexScanOp {
    pub table_id: ObjectId,
    pub table_name: String,
    pub index_name: String,
    pub schema: Schema,
    pub index_columns: Vec<usize>,
    pub range_start: Option<BoundExpression>,
    pub range_end: Option<BoundExpression>,
    pub residual: Option<BoundExpression>,
}

/// Index-only scan (returns data directly from index)
#[derive(Debug, Clone, PartialEq)]
pub struct IndexOnlyScanOp {
    pub table_id: ObjectId,
    pub table_name: String,
    pub index_name: String,
    pub schema: Schema,
    pub index_columns: Vec<usize>,
    pub range_start: Option<BoundExpression>,
    pub range_end: Option<BoundExpression>,
}

// Filter and Project

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

// Join Operators
/// Nested loop join - O(n*m) complexity, works for all join types
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

/// Hash join - O(n+m) for equi-joins
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

/// Merge join - O(n+m) for sorted inputs with equi-join
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

// Aggregations
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

#[derive(Debug, Clone, PartialEq)]
pub struct SortAggregateOp {
    pub group_by: Vec<BoundExpression>,
    pub aggregates: Vec<AggregateExpr>,
    pub group_ordering: Vec<OrderingSpec>,
    pub input_schema: Schema,
    pub output_schema: Schema,
}

// Sort Operators

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalSortOp {
    pub order_by: Vec<SortExpr>,
    pub schema: Schema,
    pub memory_limit: Option<usize>,
}

impl ExternalSortOp {
    pub fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self {
            order_by,
            schema,
            memory_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopNOp {
    pub order_by: Vec<SortExpr>,
    pub limit: usize,
    pub offset: Option<usize>,
    pub schema: Schema,
}

impl TopNOp {
    pub fn new(
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
pub struct HashUnionOp {
    pub all: bool,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HashIntersectOp {
    pub all: bool,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HashExceptOp {
    pub all: bool,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HashDistinctOp {
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortDistinctOp {
    pub schema: Schema,
}

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

// DML Physical operators

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

#[derive(Debug, Clone, PartialEq)]
pub struct PhysEmptyOp {
    pub schema: Schema,
}

impl PhysEmptyOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Sort enforcer adds sorting to satisfy ordering requirements
#[derive(Debug, Clone, PartialEq)]
pub struct EnforcerSortOp {
    pub order_by: Vec<SortExpr>,
    pub schema: Schema,
}

impl EnforcerSortOp {
    pub fn new(order_by: Vec<SortExpr>, schema: Schema) -> Self {
        Self { order_by, schema }
    }
}

/// Exchange enforcer redistributes data (for parallel/distributed execution)
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeOp {
    pub distribution: Distribution,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    Any,
    Single,
    Hash(Vec<usize>),
    Broadcast,
}

/// Ordering specification that doesn't require Eq/Hash on BoundExpression
/// Used for RequiredProperties where we need Eq + Hash
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderingSpec {
    /// Column index in the schema
    pub column_idx: usize,
    /// Ascending order
    pub ascending: bool,
    /// Nulls first
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
            op: PhysicalOperator::SeqScan(SeqScanOp::new(
                ObjectId::new(),
                "users".to_string(),
                schema.clone(),
            )),
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
