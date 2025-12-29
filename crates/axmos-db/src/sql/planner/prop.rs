//! Properties in the Cascades optimizer.
//!
//! Logical properties describe what an expression produces (schema, cardinality).
//! Physical properties describe how data is organized (ordering).

use std::collections::HashSet;

use crate::{
    schema::{Schema, catalog::StatsProvider, stats::Selectivity},
    sql::{
        binder::bounds::BoundExpression,
        parser::ast::{BinaryOperator, JoinType},
        planner::physical::OrderingSpec,
    },
    types::ObjectId,
};

use super::logical::*;

const DEFAULT_ROW_COUNT: f64 = 1000.0;
const DEFAULT_AVG_ROW_SIZE: f64 = 100.0;

/// Logical properties derived from the logical structure of the plan.
#[derive(Debug, Clone, Default)]
pub struct LogicalProperties {
    pub schema: Schema,
    pub cardinality: f64,
    pub avg_row_size: f64,
    pub unique: bool,
    pub unique_columns: Option<Vec<usize>>,
    pub not_null_columns: HashSet<usize>,
}

impl LogicalProperties {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            ..Default::default()
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
}

/// Physical properties for single-node execution.
#[derive(Debug, Clone, Default)]
pub struct PhysicalProperties {
    pub ordering: Vec<(BoundExpression, bool)>,
}

impl PhysicalProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<(BoundExpression, bool)>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        if required.ordering.is_empty() {
            return true;
        }
        if self.ordering.len() < required.ordering.len() {
            return false;
        }

        required.ordering.iter().enumerate().all(|(i, req)| {
            self.ordering.get(i).map_or(false, |(expr, asc)| {
                matches!(expr, BoundExpression::ColumnRef(c) if c.column_idx == req.column_idx)
                    && *asc == req.ascending
            })
        })
    }

    pub fn ordering_as_specs(&self) -> Vec<OrderingSpec> {
        self.ordering
            .iter()
            .filter_map(|(expr, asc)| match expr {
                BoundExpression::ColumnRef(c) => Some(OrderingSpec::new(c.column_idx, *asc)),
                _ => None,
            })
            .collect()
    }
}

/// Required properties that a parent operator needs from its children.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RequiredProperties {
    pub ordering: Vec<OrderingSpec>,
}

impl RequiredProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<OrderingSpec>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ordering.is_empty()
    }

    pub fn for_merge_join(keys: &[usize]) -> Self {
        Self {
            ordering: keys.iter().map(|&col| OrderingSpec::asc(col)).collect(),
        }
    }

    pub fn union(&self, other: &Self) -> Option<Self> {
        if self.ordering.is_empty() {
            return Some(other.clone());
        }
        if other.ordering.is_empty() {
            return Some(self.clone());
        }

        let min_len = self.ordering.len().min(other.ordering.len());
        for i in 0..min_len {
            if self.ordering[i] != other.ordering[i] {
                return None;
            }
        }

        if self.ordering.len() >= other.ordering.len() {
            Some(self.clone())
        } else {
            Some(other.clone())
        }
    }
}

/// Enforcer types for satisfying required properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcerType {
    Sort(Vec<OrderingSpec>),
}

impl EnforcerType {
    pub fn estimate_cost(&self, input_rows: f64, avg_row_size: f64, page_size: u32) -> f64 {
        match self {
            Self::Sort(_) => {
                if input_rows <= 1.0 {
                    return 0.0;
                }

                let cpu_cost = input_rows * input_rows.log2() * 0.0025;
                let memory_limit = 256.0 * 1024.0 * 1024.0;
                let data_size = input_rows * avg_row_size;

                if data_size > memory_limit {
                    let pages = (data_size / page_size as f64).ceil();
                    let passes = (pages / (memory_limit / page_size as f64))
                        .log2()
                        .ceil()
                        .max(1.0);
                    cpu_cost + pages * passes * 2.0
                } else {
                    cpu_cost
                }
            }
        }
    }
}

/// Derives logical properties for operators.
pub struct PropertyDeriver<S: StatsProvider> {
    provider: S,
}

impl<S: StatsProvider> PropertyDeriver<S> {
    pub fn new(provider: S) -> Self {
        Self { provider }
    }

    pub fn into_provider(self) -> S {
        self.provider
    }

    pub fn provider(&self) -> &S {
        &self.provider
    }

    pub fn derive(
        &self,
        op: &LogicalOperator,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        match op {
            LogicalOperator::TableScan(s) => self.derive_table_scan(s),
            LogicalOperator::IndexScan(s) => self.derive_index_scan(s),
            LogicalOperator::Filter(f) => self.derive_filter(f, children),
            LogicalOperator::Project(p) => self.derive_project(p, children),
            LogicalOperator::Join(j) => self.derive_join(j, children),
            LogicalOperator::Aggregate(a) => self.derive_aggregate(a, children),
            LogicalOperator::Sort(s) => self.derive_passthrough(&s.schema, children),
            LogicalOperator::Limit(l) => self.derive_limit(l, children),
            LogicalOperator::Distinct(d) => self.derive_distinct(d, children),
            LogicalOperator::Values(v) => self.derive_values(v),
            LogicalOperator::Empty(e) => self.derive_empty(e),
            LogicalOperator::Insert(i) => self.derive_dml(&i.table_schema, children),
            LogicalOperator::Update(u) => self.derive_dml(&u.table_schema, children),
            LogicalOperator::Delete(d) => self.derive_dml(&d.table_schema, children),
            LogicalOperator::Materialize(m) => self.derive_passthrough(&m.schema, children),
        }
    }

    fn derive_table_scan(&self, scan: &TableScanOp) -> LogicalProperties {
        let stats = self.provider.get_stats(scan.table_id);

        let (row_count, avg_row_size) = stats
            .as_ref()
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        let cardinality = match &scan.predicate {
            Some(pred) => {
                self.apply_selectivity(row_count, self.estimate_selectivity(pred, scan.table_id))
            }
            None => row_count,
        };

        LogicalProperties {
            schema: scan.schema.clone(),
            cardinality,
            avg_row_size,
            unique: scan.schema.num_keys > 0,
            unique_columns: self.key_columns(&scan.schema),
            not_null_columns: self.key_not_null(&scan.schema),
        }
    }

    fn derive_index_scan(&self, scan: &IndexScanOp) -> LogicalProperties {
        let stats = self.provider.get_stats(scan.table_id);

        let (row_count, avg_row_size) = stats
            .as_ref()
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        // Use NDV from first index column
        let ndv = self
            .provider
            .get_column_stats(scan.index_id, 0)
            .map(|c| c.ndv)
            .unwrap_or(100);

        let index_sel = if ndv > 0 {
            Selectivity::from(1.0 / ndv as f64)
        } else {
            Selectivity::Low
        };

        let mut cardinality = self.apply_selectivity(row_count, index_sel);

        if let Some(residual) = &scan.residual_predicate {
            let residual_sel = self.estimate_selectivity(residual, scan.table_id);
            cardinality = self.apply_selectivity(cardinality, residual_sel);
        }

        LogicalProperties {
            schema: scan.schema.clone(),
            cardinality: cardinality.max(1.0),
            avg_row_size,
            unique: scan.schema.num_keys > 0,
            unique_columns: self.key_columns(&scan.schema),
            not_null_columns: self.key_not_null(&scan.schema),
        }
    }

    fn derive_filter(
        &self,
        filter: &FilterOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().expect("Filter requires input");
        let sel = self.estimate_selectivity(&filter.predicate, ObjectId::default());
        let cardinality = self.apply_selectivity(input.cardinality, sel).max(1.0);

        LogicalProperties {
            schema: input.schema.clone(),
            cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
        }
    }

    fn derive_project(
        &self,
        project: &ProjectOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().expect("Project requires input");

        let out_cols = project.output_schema.num_columns();
        let in_cols = project.input_schema.num_columns().max(1);
        let size_ratio = (out_cols as f64 / in_cols as f64).min(1.0);

        let unique_columns = input.unique_columns.as_ref().and_then(|uniq| {
            let remapped: Vec<usize> = project
                .expressions
                .iter()
                .enumerate()
                .filter_map(|(out_idx, pe)| {
                    if let BoundExpression::ColumnRef(c) = &pe.expr {
                        if uniq.contains(&c.column_idx) {
                            return Some(out_idx);
                        }
                    }
                    None
                })
                .collect();

            if remapped.len() == uniq.len() {
                Some(remapped)
            } else {
                None
            }
        });

        LogicalProperties {
            schema: project.output_schema.clone(),
            cardinality: input.cardinality,
            avg_row_size: input.avg_row_size * size_ratio,
            unique: unique_columns.is_some(),
            unique_columns,
            not_null_columns: HashSet::new(),
        }
    }

    fn derive_join(&self, join: &JoinOp, children: &[&LogicalProperties]) -> LogicalProperties {
        let left = children.get(0).expect("Join requires left");
        let right = children.get(1).expect("Join requires right");

        let sel = join
            .condition
            .as_ref()
            .map(|c| self.estimate_join_selectivity(c))
            .unwrap_or(Selectivity::High);

        let cardinality = match join.join_type {
            JoinType::Inner => self.apply_selectivity(left.cardinality * right.cardinality, sel),
            JoinType::Left => self
                .apply_selectivity(left.cardinality * right.cardinality, sel)
                .max(left.cardinality),
            JoinType::Right => self
                .apply_selectivity(left.cardinality * right.cardinality, sel)
                .max(right.cardinality),
            JoinType::Full => {
                let inner = self.apply_selectivity(left.cardinality * right.cardinality, sel);
                inner + left.cardinality + right.cardinality
            }
            JoinType::Cross => left.cardinality * right.cardinality,
        };

        LogicalProperties {
            schema: join.output_schema.clone(),
            cardinality: cardinality.max(1.0),
            avg_row_size: left.avg_row_size + right.avg_row_size,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
        }
    }

    fn derive_aggregate(
        &self,
        agg: &AggregateOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().expect("Aggregate requires input");

        let cardinality = if agg.group_by.is_empty() {
            1.0
        } else {
            (input.cardinality * 0.1 * agg.group_by.len() as f64)
                .min(input.cardinality)
                .max(1.0)
        };

        let avg_row_size = (agg.group_by.len() + agg.aggregates.len()) as f64 * 8.0;

        LogicalProperties {
            schema: agg.output_schema.clone(),
            cardinality,
            avg_row_size: avg_row_size.max(16.0),
            unique: !agg.group_by.is_empty(),
            unique_columns: if agg.group_by.is_empty() {
                None
            } else {
                Some((0..agg.group_by.len()).collect())
            },
            not_null_columns: HashSet::new(),
        }
    }

    fn derive_limit(&self, limit: &LimitOp, children: &[&LogicalProperties]) -> LogicalProperties {
        let input = children.first().expect("Limit requires input");
        let offset = limit.offset.unwrap_or(0) as f64;
        let cardinality = (input.cardinality - offset)
            .max(0.0)
            .min(limit.limit as f64);

        LogicalProperties {
            schema: limit.schema.clone(),
            cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
        }
    }

    fn derive_distinct(
        &self,
        distinct: &DistinctOp,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().expect("Distinct requires input");

        LogicalProperties {
            schema: distinct.schema.clone(),
            cardinality: (input.cardinality * 0.9).max(1.0),
            avg_row_size: input.avg_row_size,
            unique: true,
            unique_columns: Some((0..distinct.schema.num_columns()).collect()),
            not_null_columns: input.not_null_columns.clone(),
        }
    }

    fn derive_values(&self, values: &ValuesOp) -> LogicalProperties {
        let row_count = values.rows.len() as f64;
        let col_count = values.rows.first().map(|r| r.len()).unwrap_or(1);

        LogicalProperties {
            schema: values.schema.clone(),
            cardinality: row_count.max(1.0),
            avg_row_size: col_count as f64 * 8.0,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
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
        }
    }

    fn derive_passthrough(
        &self,
        schema: &Schema,
        children: &[&LogicalProperties],
    ) -> LogicalProperties {
        let input = children.first().expect("Operator requires input");

        LogicalProperties {
            schema: schema.clone(),
            cardinality: input.cardinality,
            avg_row_size: input.avg_row_size,
            unique: input.unique,
            unique_columns: input.unique_columns.clone(),
            not_null_columns: input.not_null_columns.clone(),
        }
    }

    fn derive_dml(&self, schema: &Schema, children: &[&LogicalProperties]) -> LogicalProperties {
        let cardinality = children.first().map(|c| c.cardinality).unwrap_or(1.0);

        LogicalProperties {
            schema: schema.clone(),
            cardinality,
            avg_row_size: DEFAULT_AVG_ROW_SIZE,
            unique: false,
            unique_columns: None,
            not_null_columns: HashSet::new(),
        }
    }
}

// Selectivity estimation
impl<S: StatsProvider> PropertyDeriver<S> {
    fn estimate_selectivity(&self, pred: &BoundExpression, table_id: ObjectId) -> Selectivity {
        match pred {
            BoundExpression::BinaryOp {
                left, op, right, ..
            } => match op {
                BinaryOperator::Eq => {
                    if let Some(col_idx) = Self::extract_column_idx(left) {
                        if Self::is_literal(right) {
                            if let Some(col_stats) =
                                self.provider.get_column_stats(table_id, col_idx)
                            {
                                let row_count = self
                                    .provider
                                    .get_stats(table_id)
                                    .map(|s| s.row_count as f64)
                                    .unwrap_or(DEFAULT_ROW_COUNT);
                                return col_stats.selectivity_equality(row_count);
                            }
                        }
                    }
                    Selectivity::Low
                }
                BinaryOperator::Neq => Selectivity::High,
                BinaryOperator::Lt
                | BinaryOperator::Le
                | BinaryOperator::Gt
                | BinaryOperator::Ge => Selectivity::Mid,
                BinaryOperator::And => {
                    let l = self.estimate_selectivity(left, table_id);
                    let r = self.estimate_selectivity(right, table_id);
                    if l < r { l } else { r }
                }
                BinaryOperator::Or => {
                    let l = self.estimate_selectivity(left, table_id);
                    let r = self.estimate_selectivity(right, table_id);
                    if l > r { l } else { r }
                }
                BinaryOperator::Like => Selectivity::Low,
                _ => Selectivity::Mid,
            },
            BoundExpression::IsNull { negated, .. } => {
                if *negated {
                    Selectivity::High
                } else {
                    Selectivity::Empty
                }
            }
            BoundExpression::InList { list, negated, .. } => {
                let base = if list.len() <= 2 {
                    Selectivity::Low
                } else if list.len() <= 10 {
                    Selectivity::Mid
                } else {
                    Selectivity::High
                };
                if *negated {
                    match base {
                        Selectivity::Low => Selectivity::High,
                        Selectivity::High => Selectivity::Low,
                        other => other,
                    }
                } else {
                    base
                }
            }
            BoundExpression::Between { negated, .. } => {
                if *negated {
                    Selectivity::High
                } else {
                    Selectivity::Mid
                }
            }
            BoundExpression::Exists { negated, .. } => {
                if *negated {
                    Selectivity::Mid
                } else {
                    Selectivity::High
                }
            }
            _ => Selectivity::Mid,
        }
    }

    fn estimate_join_selectivity(&self, cond: &BoundExpression) -> Selectivity {
        match cond {
            BoundExpression::BinaryOp {
                op: BinaryOperator::Eq,
                left,
                right,
                ..
            } => {
                if Self::extract_column_idx(left).is_some()
                    && Self::extract_column_idx(right).is_some()
                {
                    Selectivity::Low
                } else {
                    Selectivity::Mid
                }
            }
            BoundExpression::BinaryOp {
                op: BinaryOperator::And,
                left,
                right,
                ..
            } => {
                let l = self.estimate_join_selectivity(left);
                let r = self.estimate_join_selectivity(right);
                if l < r { l } else { r }
            }
            _ => Selectivity::Mid,
        }
    }

    fn apply_selectivity(&self, rows: f64, sel: Selectivity) -> f64 {
        let factor = match sel {
            Selectivity::High => 0.75,
            Selectivity::Mid => 0.33,
            Selectivity::Low => 0.1,
            Selectivity::Empty => 0.01,
            Selectivity::Uncomputable => 0.5,
        };
        rows * factor
    }

    fn extract_column_idx(expr: &BoundExpression) -> Option<usize> {
        match expr {
            BoundExpression::ColumnRef(c) => Some(c.column_idx),
            _ => None,
        }
    }

    fn is_literal(expr: &BoundExpression) -> bool {
        matches!(expr, BoundExpression::Literal { .. })
    }

    fn key_columns(&self, schema: &Schema) -> Option<Vec<usize>> {
        if schema.num_keys > 0 {
            Some((0..schema.num_keys as usize).collect())
        } else {
            None
        }
    }

    fn key_not_null(&self, schema: &Schema) -> HashSet<usize> {
        (0..schema.num_keys as usize).collect()
    }
}
