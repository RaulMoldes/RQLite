//! Properties in the Cascades optimizer.
//!
//! Logical properties describe what an expression produces (schema, cardinality).
//! Physical properties describe how data is organized (ordering, distribution).
//! Required properties specify what a parent needs from its children.

use std::collections::HashSet;

use crate::{
    database::schema::Schema,
    sql::{
        ast::{BinaryOperator, JoinType},
        binder::ast::BoundExpression,
        planner::physical::OrderingSpec,
    },
    types::ObjectId,
};

use super::logical::*;
use super::stats::StatisticsProvider;

const DEFAULT_ROW_COUNT: f64 = 1000.0;
const DEFAULT_AVG_ROW_SIZE: f64 = 100.0;
const DEFAULT_SELECTIVITY: f64 = 0.1;
const DEFAULT_JOIN_SELECTIVITY: f64 = 0.1;
const DEFAULT_DISTINCT_RATIO: f64 = 0.1;

/// Logical properties are derived from the logical structure of the plan.
/// These are shared by all equivalent expressions in a group.
#[derive(Debug, Clone, Default)]
pub struct LogicalProperties {
    /// Output schema
    pub schema: Schema,
    /// Estimated cardinality (row count)
    pub cardinality: f64,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Whether the output is known to have unique rows
    pub unique: bool,
    /// Columns that uniquely identify rows (e.g., primary key)
    pub unique_columns: Option<Vec<usize>>,
    /// Columns known to be non-null
    pub not_null_columns: HashSet<usize>,
    /// Functional dependencies (column A determines column B)
    pub functional_deps: Vec<FunctionalDependency>,
}

impl LogicalProperties {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            cardinality: 0.0,
            avg_row_size: 0.0,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    pub fn with_cardinality(mut self, cardinality: f64) -> Self {
        self.cardinality = cardinality;
        self
    }

    pub fn with_avg_row_size(mut self, avg_row_size: f64) -> Self {
        self.avg_row_size = avg_row_size;
        self
    }

    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    pub fn with_unique_columns(mut self, columns: Vec<usize>) -> Self {
        self.unique_columns = Some(columns);
        self.unique = true;
        self
    }

    pub fn with_not_null(mut self, columns: HashSet<usize>) -> Self {
        self.not_null_columns = columns;
        self
    }

    /// Merge properties from two inputs (for joins)
    pub fn merge_for_join(left: &Self, right: &Self, selectivity: f64) -> Self {
        let mut merged_cols = left.schema.columns().clone();
        merged_cols.extend(right.schema.columns().clone());
        let schema = Schema::from_columns(&merged_cols, 0);

        Self {
            schema,
            cardinality: left.cardinality * right.cardinality * selectivity,
            avg_row_size: left.avg_row_size + right.avg_row_size,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    /// Derive properties after applying a filter
    pub fn with_filter(mut self, selectivity: f64) -> Self {
        self.cardinality *= selectivity;
        self
    }

    /// Derive properties after applying a limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.cardinality = self.cardinality.min(limit as f64);
        self
    }
}

/// A functional dependency between two columnsets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionalDependency {
    /// Determinant columns (left side)
    pub determinant: Vec<usize>,
    /// Dependent columns (right side)
    pub dependent: Vec<usize>,
}

impl FunctionalDependency {
    pub fn new(determinant: Vec<usize>, dependent: Vec<usize>) -> Self {
        Self {
            determinant,
            dependent,
        }
    }
}

/// Physical properties describe how data is physically organized.
#[derive(Debug, Clone, Default)]
pub struct PhysicalProperties {
    /// Output ordering (expression, ascending)
    pub ordering: Vec<(BoundExpression, bool)>,
    /// Data distribution
    pub distribution: Distribution,
}

impl PhysicalProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<(BoundExpression, bool)>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn with_distribution(mut self, distribution: Distribution) -> Self {
        self.distribution = distribution;
        self
    }

    /// Check if these properties satisfy the required properties
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        if !self.satisfies_ordering(&required.ordering) {
            return false;
        }
        self.satisfies_distribution(&required.distribution)
    }

    fn satisfies_ordering(&self, required: &[OrderingSpec]) -> bool {
        if required.is_empty() {
            return true;
        }

        if self.ordering.len() < required.len() {
            return false;
        }

        for (i, req_spec) in required.iter().enumerate() {
            if let Some((prov_expr, prov_asc)) = self.ordering.get(i) {
                if !Self::expr_matches_column(prov_expr, req_spec.column_idx) {
                    return false;
                }
                if *prov_asc != req_spec.ascending {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn satisfies_distribution(&self, required: &Distribution) -> bool {
        match (required, &self.distribution) {
            (Distribution::Any, _) => true,
            (Distribution::Single, Distribution::Single) => true,
            (Distribution::Hash(req_cols), Distribution::Hash(prov_cols)) => req_cols == prov_cols,
            (Distribution::Broadcast, Distribution::Broadcast) => true,
            (Distribution::Replicated, Distribution::Replicated) => true,
            _ => false,
        }
    }

    fn expr_matches_column(expr: &BoundExpression, column_idx: usize) -> bool {
        match expr {
            BoundExpression::ColumnRef(col_ref) => col_ref.column_idx == column_idx,
            _ => false,
        }
    }

    /// Convert ordering to OrderingSpec (for comparison with required)
    pub fn ordering_as_specs(&self) -> Vec<OrderingSpec> {
        self.ordering
            .iter()
            .filter_map(|(expr, asc)| {
                if let BoundExpression::ColumnRef(col_ref) = expr {
                    Some(OrderingSpec::new(col_ref.column_idx, *asc))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Data distribution for parallel/distributed execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Distribution {
    #[default]
    Any,
    Single,
    Hash(Vec<usize>),
    Range(Vec<usize>),
    Broadcast,
    Replicated,
}

impl Distribution {
    pub fn is_compatible(&self, other: &Self) -> bool {
        match (self, other) {
            (Distribution::Any, _) | (_, Distribution::Any) => true,
            (a, b) => a == b,
        }
    }

    /// Check if this distribution is more restrictive than another
    pub fn is_more_restrictive(&self, other: &Self) -> bool {
        match (self, other) {
            (_, Distribution::Any) => true,
            (Distribution::Any, _) => false,
            (Distribution::Single, Distribution::Single) => false,
            (Distribution::Hash(a), Distribution::Hash(b)) => a.len() > b.len(),
            _ => false,
        }
    }
}

/// Partitioning scheme for parallel execution
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Partitioning {
    pub columns: Vec<usize>,
    pub num_partitions: usize,
}

impl Partitioning {
    pub fn new(columns: Vec<usize>, num_partitions: usize) -> Self {
        Self {
            columns,
            num_partitions,
        }
    }
}

/// Required properties (what a parent operator needs from its children).
/// Must be Eq + Hash for use as HashMap key in memo's best_physical.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RequiredProperties {
    pub ordering: Vec<OrderingSpec>,
    pub distribution: Distribution,
    pub partitioning: Option<Partitioning>,
}

impl RequiredProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<OrderingSpec>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn with_distribution(mut self, distribution: Distribution) -> Self {
        self.distribution = distribution;
        self
    }

    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = Some(partitioning);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ordering.is_empty()
            && matches!(self.distribution, Distribution::Any)
            && self.partitioning.is_none()
    }

    /// Combine with another set of required properties.
    ///
    /// For ordering: If one is a prefix of the other, take the longer one.
    /// If they conflict (different columns at same position), returns None.
    ///
    /// For distribution: Takes the more restrictive one, or None if incompatible.
    pub fn union(&self, other: &RequiredProperties) -> Option<RequiredProperties> {
        // Union ordering - must be compatible (one prefix of other or identical)
        let ordering = self.union_ordering(&other.ordering)?;

        // Union distribution - must be compatible
        let distribution = self.union_distribution(&other.distribution)?;

        // Union partitioning - take first non-None, or None if incompatible
        let partitioning = match (&self.partitioning, &other.partitioning) {
            (None, None) => None,
            (Some(p), None) | (None, Some(p)) => Some(p.clone()),
            (Some(p1), Some(p2)) => {
                if p1 == p2 {
                    Some(p1.clone())
                } else {
                    // Incompatible partitioning
                    return None;
                }
            }
        };

        Some(RequiredProperties {
            ordering,
            distribution,
            partitioning,
        })
    }

    /// Union two ordering requirements.
    /// Returns None if they are incompatible.
    fn union_ordering(&self, other: &[OrderingSpec]) -> Option<Vec<OrderingSpec>> {
        if self.ordering.is_empty() {
            return Some(other.to_vec());
        }
        if other.is_empty() {
            return Some(self.ordering.clone());
        }

        // Check compatibility: one must be a prefix of the other
        let min_len = self.ordering.len().min(other.len());

        for i in 0..min_len {
            if self.ordering[i] != other[i] {
                // Conflict at position i - incompatible
                return None;
            }
        }

        // Compatible - return the longer one
        if self.ordering.len() >= other.len() {
            Some(self.ordering.clone())
        } else {
            Some(other.to_vec())
        }
    }

    /// Union two distribution requirements.
    /// Returns None if they are incompatible.
    fn union_distribution(&self, other: &Distribution) -> Option<Distribution> {
        match (&self.distribution, other) {
            // Any is compatible with everything - take the more specific
            (Distribution::Any, d) | (d, Distribution::Any) => Some(d.clone()),

            // Same distributions are compatible
            (Distribution::Single, Distribution::Single) => Some(Distribution::Single),
            (Distribution::Broadcast, Distribution::Broadcast) => Some(Distribution::Broadcast),
            (Distribution::Replicated, Distribution::Replicated) => Some(Distribution::Replicated),

            // Hash distributions must have same columns
            (Distribution::Hash(a), Distribution::Hash(b)) => {
                if a == b {
                    Some(Distribution::Hash(a.clone()))
                } else {
                    None // Incompatible hash distributions
                }
            }

            // Range distributions must have same columns
            (Distribution::Range(a), Distribution::Range(b)) => {
                if a == b {
                    Some(Distribution::Range(a.clone()))
                } else {
                    None
                }
            }

            // Different distribution types are incompatible
            _ => None,
        }
    }

    /// Create requirements for a sort-merge join's left child
    pub fn for_merge_join_left(join_keys: &[usize]) -> Self {
        Self {
            ordering: join_keys
                .iter()
                .map(|&col| OrderingSpec::asc(col))
                .collect(),
            distribution: Distribution::Any,
            partitioning: None,
        }
    }

    /// Create requirements for a sort-merge join's right child
    pub fn for_merge_join_right(join_keys: &[usize]) -> Self {
        Self {
            ordering: join_keys
                .iter()
                .map(|&col| OrderingSpec::asc(col))
                .collect(),
            distribution: Distribution::Any,
            partitioning: None,
        }
    }

    /// Create requirements for hash join build side
    pub fn for_hash_join_build(hash_keys: &[usize]) -> Self {
        Self {
            ordering: Vec::new(),
            distribution: Distribution::Hash(hash_keys.to_vec()),
            partitioning: None,
        }
    }
}

/// Enforcer type (physical operators that enforce required properties)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcerType {
    Sort(Vec<OrderingSpec>),
    Exchange(Distribution),
}

impl EnforcerType {
    pub fn estimate_cost(&self, input_rows: f64, avg_row_size: f64, page_size: u32) -> f64 {
        match self {
            Self::Sort(_) => {
                if input_rows <= 1.0 {
                    return 0.0;
                }
                let cpu_cost = input_rows * input_rows.log2() * 0.0025;

                let memory_threshold = 256.0 * 1024.0 * 1024.0;
                let data_size = input_rows * avg_row_size;

                let io_cost = if data_size > memory_threshold {
                    let pages = (input_rows * avg_row_size / page_size as f64).ceil();
                    let mem_pages = memory_threshold / page_size as f64;
                    let passes = (pages / mem_pages).log2().ceil().max(1.0);
                    pages * passes * 2.0
                } else {
                    0.0
                };

                cpu_cost + io_cost
            }
            Self::Exchange(_) => {
                let serialize_cost = input_rows * 0.05;
                let network_cost = input_rows * avg_row_size / 1000.0;
                let deserialize_cost = input_rows * 0.05;
                serialize_cost + network_cost + deserialize_cost
            }
        }
    }
}

pub struct PropertyDeriver<'a, P: StatisticsProvider> {
    provider: &'a P,
}

impl<'a, P: StatisticsProvider> PropertyDeriver<'a, P> {
    pub fn new(provider: &'a P) -> Self {
        Self { provider }
    }

    pub fn derive(
        &self,
        op: &LogicalOperator,
        children_props: &[&LogicalProperties],
    ) -> LogicalProperties {
        match op {
            LogicalOperator::TableScan(scan) => self.derive_table_scan(scan),
            LogicalOperator::IndexScan(scan) => self.derive_index_scan(scan),
            LogicalOperator::Filter(filter) => self.derive_filter(filter, children_props),
            LogicalOperator::Project(project) => self.derive_project(project, children_props),
            LogicalOperator::Join(join) => self.derive_join(join, children_props),
            LogicalOperator::Aggregate(agg) => self.derive_aggregate(agg, children_props),
            LogicalOperator::Sort(sort) => self.derive_sort(sort, children_props),
            LogicalOperator::Limit(limit) => self.derive_limit(limit, children_props),
            LogicalOperator::Distinct(distinct) => self.derive_distinct(distinct, children_props),
            LogicalOperator::Union(union_op) => self.derive_union(union_op, children_props),
            LogicalOperator::Intersect(intersect) => {
                self.derive_intersect(intersect, children_props)
            }
            LogicalOperator::Except(except) => self.derive_except(except, children_props),
            LogicalOperator::Values(values) => self.derive_values(values),
            LogicalOperator::Empty(empty) => self.derive_empty(empty),
            LogicalOperator::Insert(insert) => self.derive_insert(insert, children_props),
            LogicalOperator::Update(update) => self.derive_update(update, children_props),
            LogicalOperator::Delete(delete) => self.derive_delete(delete, children_props),
            LogicalOperator::Materialize(mat) => self.derive_materialize(mat, children_props),
        }
    }

    fn derive_materialize(
        &self,
        mat: &MaterializeOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children
            .first()
            .copied()
            .expect("Materialize requires one child");

        // Materialize preserves all properties from input
        LogicalProperties {
            schema: mat.schema.clone(),
            cardinality: input.cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
            functional_deps: input.functional_deps.clone(),
        }
    }

    fn derive_table_scan(&self, scan: &TableScanOp) -> LogicalProperties {
        let stats = self.provider.get_table_stats(scan.table_id);

        let (row_count, avg_row_size) = stats
            .as_ref()
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        let cardinality = if let Some(pred) = &scan.predicate {
            let selectivity = self.estimate_selectivity(pred, scan.table_id, &scan.schema);
            row_count * selectivity
        } else {
            row_count
        };

        let unique_columns = self.find_unique_columns(&scan.schema);
        let not_null_columns = self.find_not_null_columns(&scan.schema);

        LogicalProperties {
            schema: scan.schema.clone(),
            cardinality,
            avg_row_size,
            unique: unique_columns.is_some(),
            unique_columns,
            not_null_columns,
            functional_deps: Vec::new(),
        }
    }

    fn derive_index_scan(&self, scan: &IndexScanOp) -> LogicalProperties {
        let table_stats = self.provider.get_table_stats(scan.table_id);
        let index_stats = self.provider.get_index_stats(scan.index_id);

        let (table_rows, avg_row_size) = table_stats
            .as_ref()
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        let selectivity = index_stats
            .as_ref()
            .filter(|s| s.distinct_keys > 0)
            .map(|s| 1.0 / s.distinct_keys as f64)
            .unwrap_or(DEFAULT_SELECTIVITY);

        let mut cardinality = (table_rows * selectivity).max(1.0);

        if let Some(residual) = &scan.residual_predicate {
            let residual_sel = self.estimate_selectivity(residual, scan.table_id, &scan.schema);
            cardinality *= residual_sel;
        }

        let unique_columns = self.find_unique_columns(&scan.schema);
        let not_null_columns = self.find_not_null_columns(&scan.schema);

        LogicalProperties {
            schema: scan.schema.clone(),
            cardinality: cardinality.max(1.0),
            avg_row_size,
            unique: unique_columns.is_some(),
            unique_columns,
            not_null_columns,
            functional_deps: Vec::new(),
        }
    }

    fn derive_filter(
        &self,
        filter: &FilterOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children
            .first()
            .copied()
            .expect("Filter requires one child");

        let table_id = ObjectId::default();
        let selectivity = self.estimate_selectivity(&filter.predicate, table_id, &input.schema);
        let cardinality = (input.cardinality * selectivity).max(1.0);

        let mut not_null_columns = input.not_null_columns.clone();
        self.update_not_null_from_predicate(&filter.predicate, &mut not_null_columns);

        LogicalProperties {
            schema: input.schema.clone(),
            cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns,
            functional_deps: input.functional_deps.clone(),
        }
    }

    fn derive_project(
        &self,
        project: &ProjectOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children
            .first()
            .copied()
            .expect("Project requires one child");

        let output_cols = project.output_schema.columns().len();
        let input_cols = project.input_schema.columns().len().max(1);
        let size_ratio = (output_cols as f64 / input_cols as f64).min(1.0);

        let unique_columns = self.remap_unique_columns(&input.unique_columns, &project.expressions);

        let not_null_columns =
            self.remap_not_null_columns(&input.not_null_columns, &project.expressions);

        LogicalProperties {
            schema: project.output_schema.clone(),
            cardinality: input.cardinality,
            avg_row_size: input.avg_row_size * size_ratio,
            unique: unique_columns.is_some(),
            unique_columns,
            not_null_columns,
            functional_deps: Vec::new(),
        }
    }

    fn derive_join(&self, join: &JoinOp, children: &[&LogicalProperties]) -> LogicalProperties {
        let left = children.get(0).copied().expect("Join requires left child");
        let right = children.get(1).copied().expect("Join requires right child");

        let selectivity = if let Some(cond) = &join.condition {
            self.estimate_join_selectivity(cond, left, right)
        } else {
            1.0
        };

        let (cardinality, unique, unique_columns) = match join.join_type {
            JoinType::Inner => {
                let card = left.cardinality * right.cardinality * selectivity;
                let uniq = self.is_join_unique(join, left, right);
                let uniq_cols = if uniq {
                    left.unique_columns.clone()
                } else {
                    None
                };
                (card, uniq, uniq_cols)
            }
            JoinType::Left => {
                let card =
                    (left.cardinality * right.cardinality * selectivity).max(left.cardinality);
                (card, false, None)
            }
            JoinType::Right => {
                let card =
                    (left.cardinality * right.cardinality * selectivity).max(right.cardinality);
                (card, false, None)
            }
            JoinType::Full => {
                let inner_card = left.cardinality * right.cardinality * selectivity;
                let card = inner_card + left.cardinality + right.cardinality;
                (card, false, None)
            }
            JoinType::Cross => {
                let card = left.cardinality * right.cardinality;
                (card, false, None)
            }
        };

        let not_null_columns = self.merge_not_null_for_join(
            &left.not_null_columns,
            &right.not_null_columns,
            left.schema.columns().len(),
            join.join_type,
        );

        LogicalProperties {
            schema: join.output_schema.clone(),
            cardinality: cardinality.max(1.0),
            avg_row_size: left.avg_row_size + right.avg_row_size,
            unique,
            unique_columns,
            not_null_columns,
            functional_deps: Vec::new(),
        }
    }

    fn derive_aggregate(
        &self,
        agg: &AggregateOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children
            .first()
            .copied()
            .expect("Aggregate requires one child");

        let cardinality = if agg.group_by.is_empty() {
            1.0
        } else {
            let estimated_groups = self.estimate_distinct_groups(&agg.group_by, input);
            estimated_groups.min(input.cardinality)
        };

        let group_by_size: f64 = agg.group_by.len() as f64 * 8.0;
        let agg_size: f64 = agg.aggregates.len() as f64 * 8.0;
        let avg_row_size = (group_by_size + agg_size).max(16.0);

        let unique = !agg.group_by.is_empty();
        let unique_columns = if unique {
            Some((0..agg.group_by.len()).collect())
        } else {
            None
        };

        LogicalProperties {
            schema: agg.output_schema.clone(),
            cardinality,
            avg_row_size,
            unique,
            unique_columns,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_sort(&self, sort: &SortOp, children: &[&LogicalProperties]) -> LogicalProperties {
        let input = children.first().copied().expect("Sort requires one child");

        LogicalProperties {
            schema: sort.schema.clone(),
            cardinality: input.cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
            functional_deps: input.functional_deps.clone(),
        }
    }

    fn derive_limit(&self, limit: &LimitOp, children: &[&LogicalProperties]) -> LogicalProperties {
        let input = children.first().copied().expect("Limit requires one child");

        let offset = limit.offset.unwrap_or(0) as f64;
        let available = (input.cardinality - offset).max(0.0);
        let cardinality = available.min(limit.limit as f64);

        LogicalProperties {
            schema: limit.schema.clone(),
            cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
            functional_deps: input.functional_deps.clone(),
        }
    }

    fn derive_distinct(
        &self,
        distinct: &DistinctOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children
            .first()
            .copied()
            .expect("Distinct requires one child");

        let cardinality = (input.cardinality * 0.9).max(1.0);

        LogicalProperties {
            schema: distinct.schema.clone(),
            cardinality,
            avg_row_size: input.avg_row_size,
            unique: true,
            unique_columns: Some((0..distinct.schema.columns().len()).collect()),
            not_null_columns: input.not_null_columns.clone(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_union(
        &self,
        union_op: &UnionOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let left = children.get(0).copied().expect("Union requires left child");
        let right = children
            .get(1)
            .copied()
            .expect("Union requires right child");

        let total = left.cardinality + right.cardinality;
        let cardinality = if union_op.all { total } else { total * 0.7 };

        LogicalProperties {
            schema: union_op.schema.clone(),
            cardinality,
            avg_row_size: left.avg_row_size.max(right.avg_row_size),
            unique: !union_op.all,
            unique_columns: if union_op.all {
                None
            } else {
                Some((0..union_op.schema.columns().len()).collect())
            },
            not_null_columns: left
                .not_null_columns
                .intersection(&right.not_null_columns)
                .copied()
                .collect(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_intersect(
        &self,
        intersect: &IntersectOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let left = children
            .get(0)
            .copied()
            .expect("Intersect requires left child");
        let right = children
            .get(1)
            .copied()
            .expect("Intersect requires right child");

        let cardinality = left.cardinality.min(right.cardinality) * 0.5;

        LogicalProperties {
            schema: intersect.schema.clone(),
            cardinality: cardinality.max(1.0),
            avg_row_size: left.avg_row_size,
            unique: !intersect.all,
            unique_columns: if intersect.all {
                None
            } else {
                Some((0..intersect.schema.columns().len()).collect())
            },
            not_null_columns: left
                .not_null_columns
                .union(&right.not_null_columns)
                .copied()
                .collect(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_except(
        &self,
        except: &ExceptOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let left = children
            .get(0)
            .copied()
            .expect("Except requires left child");
        let right = children
            .get(1)
            .copied()
            .expect("Except requires right child");

        let cardinality = (left.cardinality - right.cardinality * 0.3).max(1.0);

        LogicalProperties {
            schema: except.schema.clone(),
            cardinality,
            avg_row_size: left.avg_row_size,
            unique: !except.all,
            unique_columns: if except.all {
                None
            } else {
                Some((0..except.schema.columns().len()).collect())
            },
            not_null_columns: left.not_null_columns.clone(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_values(&self, values: &ValuesOp) -> LogicalProperties {
        let row_count = values.rows.len() as f64;
        let col_count = values.rows.first().map(|r| r.len()).unwrap_or(1);
        let avg_row_size = col_count as f64 * 8.0;

        LogicalProperties {
            schema: values.schema.clone(),
            cardinality: row_count,
            avg_row_size,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_empty(&self, empty: &EmptyOp) -> LogicalProperties {
        LogicalProperties {
            schema: empty.schema.clone(),
            cardinality: 1.0,
            avg_row_size: 0.0,
            unique: true,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_insert(
        &self,
        insert: &InsertOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().copied();
        let cardinality = input.map(|i| i.cardinality).unwrap_or(1.0);

        LogicalProperties {
            schema: insert.table_schema.clone(),
            cardinality,
            avg_row_size: DEFAULT_AVG_ROW_SIZE,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_update(
        &self,
        update: &UpdateOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().copied();
        let cardinality = input.map(|i| i.cardinality).unwrap_or(1.0);

        LogicalProperties {
            schema: update.table_schema.clone(),
            cardinality,
            avg_row_size: DEFAULT_AVG_ROW_SIZE,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn derive_delete(
        &self,
        delete: &DeleteOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().copied();
        let cardinality = input.map(|i| i.cardinality).unwrap_or(1.0);

        LogicalProperties {
            schema: delete.table_schema.clone(),
            cardinality,
            avg_row_size: DEFAULT_AVG_ROW_SIZE,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
            functional_deps: Vec::new(),
        }
    }

    fn estimate_selectivity(
        &self,
        predicate: &BoundExpression,
        table_id: ObjectId,
        schema: &Schema,
    ) -> f64 {
        match predicate {
            BoundExpression::BinaryOp {
                left, op, right, ..
            } => match op {
                BinaryOperator::Eq => {
                    if let Some(col_idx) = self.extract_column_idx(left) {
                        if self.is_literal(right) {
                            return self.estimate_equality_selectivity(table_id, col_idx);
                        }
                    }
                    if let Some(col_idx) = self.extract_column_idx(right) {
                        if self.is_literal(left) {
                            return self.estimate_equality_selectivity(table_id, col_idx);
                        }
                    }
                    DEFAULT_SELECTIVITY
                }
                BinaryOperator::Neq => {
                    if let Some(col_idx) = self.extract_column_idx(left) {
                        if self.is_literal(right) {
                            return 1.0 - self.estimate_equality_selectivity(table_id, col_idx);
                        }
                    }
                    0.9
                }
                BinaryOperator::Lt
                | BinaryOperator::Le
                | BinaryOperator::Gt
                | BinaryOperator::Ge => 0.33,
                BinaryOperator::And => {
                    let left_sel = self.estimate_selectivity(left, table_id, schema);
                    let right_sel = self.estimate_selectivity(right, table_id, schema);
                    left_sel * right_sel
                }
                BinaryOperator::Or => {
                    let left_sel = self.estimate_selectivity(left, table_id, schema);
                    let right_sel = self.estimate_selectivity(right, table_id, schema);
                    (left_sel + right_sel - left_sel * right_sel).min(1.0)
                }
                BinaryOperator::Like => 0.1,
                _ => 0.5,
            },
            BoundExpression::IsNull { negated, .. } => {
                if *negated {
                    0.95
                } else {
                    0.05
                }
            }
            BoundExpression::InList { list, negated, .. } => {
                let base_sel = (list.len() as f64 * 0.05).min(0.8);
                if *negated { 1.0 - base_sel } else { base_sel }
            }
            BoundExpression::Between { negated, .. } => {
                if *negated {
                    0.75
                } else {
                    0.25
                }
            }
            BoundExpression::Exists { negated, .. } => {
                if *negated {
                    0.3
                } else {
                    0.7
                }
            }
            BoundExpression::Literal { value } => {
                use crate::types::DataType;
                match value {
                    DataType::Boolean(b) => {
                        if b.0 != 0 {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    DataType::Null => 0.5,
                    _ => 0.5,
                }
            }
            _ => 0.5,
        }
    }

    fn estimate_equality_selectivity(&self, table_id: ObjectId, col_idx: usize) -> f64 {
        if let Some(col_stats) = self.provider.get_column_stats(table_id, col_idx) {
            if col_stats.ndv > 0 {
                return 1.0 / col_stats.ndv as f64;
            }
        }
        DEFAULT_SELECTIVITY
    }

    fn estimate_join_selectivity(
        &self,
        condition: &BoundExpression,
        left: &LogicalProperties,
        right: &LogicalProperties,
    ) -> f64 {
        match condition {
            BoundExpression::BinaryOp {
                op: BinaryOperator::Eq,
                left: l_expr,
                right: r_expr,
                ..
            } => {
                if let (Some(_l_col), Some(_r_col)) = (
                    self.extract_column_idx(l_expr),
                    self.extract_column_idx(r_expr),
                ) {
                    let left_distinct = left.cardinality * DEFAULT_DISTINCT_RATIO;
                    let right_distinct = right.cardinality * DEFAULT_DISTINCT_RATIO;
                    let max_distinct = left_distinct.max(right_distinct).max(1.0);
                    return 1.0 / max_distinct;
                }
                DEFAULT_JOIN_SELECTIVITY
            }
            BoundExpression::BinaryOp {
                op: BinaryOperator::And,
                left: l,
                right: r,
                ..
            } => {
                let left_sel = self.estimate_join_selectivity(l, left, right);
                let right_sel = self.estimate_join_selectivity(r, left, right);
                left_sel * right_sel
            }
            _ => DEFAULT_JOIN_SELECTIVITY,
        }
    }

    fn estimate_distinct_groups(
        &self,
        group_by: &[BoundExpression],
        input: &LogicalProperties,
    ) -> f64 {
        if group_by.is_empty() {
            return 1.0;
        }

        let base_estimate = input.cardinality * DEFAULT_DISTINCT_RATIO;

        let group_factor = match group_by.len() {
            1 => 1.0,
            2 => 1.5,
            3 => 2.0,
            _ => 2.5,
        };

        (base_estimate * group_factor).max(1.0)
    }

    fn is_join_unique(
        &self,
        join: &JoinOp,
        left: &LogicalProperties,
        right: &LogicalProperties,
    ) -> bool {
        if let Some(cond) = &join.condition {
            if let Some(left_unique) = &left.unique_columns {
                if let Some(right_unique) = &right.unique_columns {
                    let (left_keys, right_keys) = self.extract_join_keys(cond);

                    let left_is_unique = left_keys.iter().all(|k| left_unique.contains(k));
                    let right_is_unique = right_keys.iter().all(|k| right_unique.contains(k));

                    return left_is_unique && right_is_unique;
                }
            }
        }
        false
    }

    fn extract_join_keys(&self, condition: &BoundExpression) -> (Vec<usize>, Vec<usize>) {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        self.collect_join_keys(condition, &mut left_keys, &mut right_keys);

        (left_keys, right_keys)
    }

    fn collect_join_keys(
        &self,
        expr: &BoundExpression,
        left_keys: &mut Vec<usize>,
        right_keys: &mut Vec<usize>,
    ) {
        match expr {
            BoundExpression::BinaryOp {
                op: BinaryOperator::Eq,
                left,
                right,
                ..
            } => {
                if let (Some(l_col), Some(r_col)) = (
                    self.extract_column_idx(left),
                    self.extract_column_idx(right),
                ) {
                    left_keys.push(l_col);
                    right_keys.push(r_col);
                }
            }
            BoundExpression::BinaryOp {
                op: BinaryOperator::And,
                left,
                right,
                ..
            } => {
                self.collect_join_keys(left, left_keys, right_keys);
                self.collect_join_keys(right, left_keys, right_keys);
            }
            _ => {}
        }
    }

    fn find_unique_columns(&self, schema: &Schema) -> Option<Vec<usize>> {
        let mut unique_cols = Vec::new();
        for (idx, col) in schema.columns().iter().enumerate() {
            // Asumimos que las columnas marcadas como key son únicas
            // Esto depende de la implementación de tu Schema
            if idx < schema.num_keys as usize {
                unique_cols.push(idx);
            }
        }
        if unique_cols.is_empty() {
            None
        } else {
            Some(unique_cols)
        }
    }

    fn find_not_null_columns(&self, schema: &Schema) -> HashSet<usize> {
        let mut not_null = HashSet::new();
        // Las columnas key típicamente son not null
        for idx in 0..schema.num_keys as usize {
            not_null.insert(idx);
        }
        not_null
    }

    fn update_not_null_from_predicate(
        &self,
        predicate: &BoundExpression,
        not_null: &mut HashSet<usize>,
    ) {
        match predicate {
            BoundExpression::IsNull {
                expr,
                negated: true,
            } => {
                if let Some(col_idx) = self.extract_column_idx(expr) {
                    not_null.insert(col_idx);
                }
            }
            BoundExpression::BinaryOp {
                op: BinaryOperator::And,
                left,
                right,
                ..
            } => {
                self.update_not_null_from_predicate(left, not_null);
                self.update_not_null_from_predicate(right, not_null);
            }
            BoundExpression::BinaryOp { left, right, .. } => {
                if let Some(col_idx) = self.extract_column_idx(left) {
                    if self.is_literal(right) {
                        not_null.insert(col_idx);
                    }
                }
                if let Some(col_idx) = self.extract_column_idx(right) {
                    if self.is_literal(left) {
                        not_null.insert(col_idx);
                    }
                }
            }
            _ => {}
        }
    }

    fn merge_not_null_for_join(
        &self,
        left_not_null: &HashSet<usize>,
        right_not_null: &HashSet<usize>,
        left_col_count: usize,
        join_type: JoinType,
    ) -> HashSet<usize> {
        let mut result = HashSet::new();

        match join_type {
            JoinType::Inner => {
                result.extend(left_not_null);
                for col in right_not_null {
                    result.insert(col + left_col_count);
                }
            }
            JoinType::Left => {
                result.extend(left_not_null);
            }
            JoinType::Right => {
                for col in right_not_null {
                    result.insert(col + left_col_count);
                }
            }
            JoinType::Full | JoinType::Cross => {}
        }

        result
    }

    fn remap_unique_columns(
        &self,
        input_unique: &Option<Vec<usize>>,
        expressions: &[ProjectExpr],
    ) -> Option<Vec<usize>> {
        let input_unique = input_unique.as_ref()?;

        let mut output_unique = Vec::new();

        for (output_idx, proj_expr) in expressions.iter().enumerate() {
            if let Some(input_col) = self.extract_column_idx(&proj_expr.expr) {
                if input_unique.contains(&input_col) {
                    output_unique.push(output_idx);
                }
            }
        }

        if output_unique.len() == input_unique.len() {
            Some(output_unique)
        } else {
            None
        }
    }

    fn remap_not_null_columns(
        &self,
        input_not_null: &HashSet<usize>,
        expressions: &[ProjectExpr],
    ) -> HashSet<usize> {
        let mut output_not_null = HashSet::new();

        for (output_idx, proj_expr) in expressions.iter().enumerate() {
            if let Some(input_col) = self.extract_column_idx(&proj_expr.expr) {
                if input_not_null.contains(&input_col) {
                    output_not_null.insert(output_idx);
                }
            }
            if matches!(&proj_expr.expr, BoundExpression::Literal { value } if !value.is_null()) {
                output_not_null.insert(output_idx);
            }
        }

        output_not_null
    }

    fn extract_column_idx(&self, expr: &BoundExpression) -> Option<usize> {
        match expr {
            BoundExpression::ColumnRef(col_ref) => Some(col_ref.column_idx),
            _ => None,
        }
    }

    fn is_literal(&self, expr: &BoundExpression) -> bool {
        matches!(expr, BoundExpression::Literal { .. })
    }
}
