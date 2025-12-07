//! Cost model for the optimizer.

use super::physical::*;
use super::stats::StatisticsProvider;

const DEFAULT_PAGE_IO_COST: f64 = 1.0;
const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
const RANDOM_IO_MULTIPLIER: f64 = 1.2;
const MEMORY_THRESHOLD: u64 = 256 * 1024 * 1024;

const DEFAULT_ROW_COUNT: f64 = 1000.0;
const DEFAULT_AVG_ROW_SIZE: f64 = 100.0;
const DEFAULT_SELECTIVITY: f64 = 0.1;
const DEFAULT_INDEX_HEIGHT: usize = 3;

/// Derived statistics passed during cost computation.
#[derive(Debug, Clone, Default)]
pub struct DerivedStats {
    pub row_count: f64,
    pub avg_row_size: f64,
}

impl DerivedStats {
    pub fn new(row_count: f64, avg_row_size: f64) -> Self {
        Self {
            row_count,
            avg_row_size,
        }
    }

    pub fn unknown() -> Self {
        Self {
            row_count: DEFAULT_ROW_COUNT,
            avg_row_size: DEFAULT_AVG_ROW_SIZE,
        }
    }
}

/// Cost model trait
pub trait CostModel {
    fn compute_operator_cost<P: StatisticsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &P,
        children_stats: &[DerivedStats],
    ) -> f64;

    fn derive_stats<P: StatisticsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &P,
        children_stats: &[DerivedStats],
    ) -> DerivedStats;
}

/// Main cost model implementation.
pub struct AxmosCostModel {
    pub page_size: u32,
    pub page_io_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_operator_cost: f64,
    pub random_io_multiplier: f64,
    pub memory_threshold: u64,
}

impl AxmosCostModel {
    pub fn new(page_size: u32) -> Self {
        Self {
            page_size,
            page_io_cost: DEFAULT_PAGE_IO_COST,
            cpu_tuple_cost: DEFAULT_CPU_TUPLE_COST,
            cpu_operator_cost: DEFAULT_CPU_OPERATOR_COST,
            random_io_multiplier: RANDOM_IO_MULTIPLIER,
            memory_threshold: MEMORY_THRESHOLD,
        }
    }

    pub fn with_page_io_cost(mut self, cost: f64) -> Self {
        self.page_io_cost = cost;
        self
    }

    pub fn with_cpu_tuple_cost(mut self, cost: f64) -> Self {
        self.cpu_tuple_cost = cost;
        self
    }

    #[inline]
    fn estimate_pages(&self, rows: f64, avg_row_size: f64) -> f64 {
        let rows_per_page = (self.page_size as f64 / avg_row_size.max(1.0)).max(1.0);
        (rows / rows_per_page).ceil().max(1.0)
    }
}

impl CostModel for AxmosCostModel {
    fn compute_operator_cost<P: StatisticsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &P,
        children_stats: &[DerivedStats],
    ) -> f64 {
        match op {
            PhysicalOperator::SeqScan(scan) => {
                let (row_count, avg_row_size) = provider
                    .get_table_stats(scan.table_id)
                    .map(|s| (s.row_count as f64, s.avg_row_size))
                    .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

                let pages = self.estimate_pages(row_count, avg_row_size);
                let io_cost = pages * self.page_io_cost;
                let cpu_cost = row_count * self.cpu_tuple_cost;

                let filter_cost = if scan.predicate.is_some() {
                    row_count * self.cpu_operator_cost
                } else {
                    0.0
                };

                io_cost + cpu_cost + filter_cost
            }

            PhysicalOperator::IndexScan(scan) => {
                let table_stats = provider.get_table_stats(scan.table_id);
                let index_stats = provider.get_index_stats(scan.index_id);

                let (table_rows, avg_row_size) = table_stats
                    .as_ref()
                    .map(|s| (s.row_count as f64, s.avg_row_size))
                    .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

                let height = index_stats
                    .as_ref()
                    .map(|s| s.height)
                    .unwrap_or(DEFAULT_INDEX_HEIGHT);

                let selectivity = index_stats
                    .as_ref()
                    .filter(|s| s.distinct_keys > 0)
                    .map(|s| 1.0 / s.distinct_keys as f64)
                    .unwrap_or(DEFAULT_SELECTIVITY);

                let selected_rows = (table_rows * selectivity).max(1.0);

                let traversal_io = height as f64 * self.page_io_cost;
                let leaf_pages = (selected_rows / 100.0).ceil().max(1.0);
                let leaf_io = leaf_pages * self.page_io_cost;
                let heap_pages = self.estimate_pages(selected_rows, avg_row_size);
                let heap_io = heap_pages * self.page_io_cost * self.random_io_multiplier * 0.5;
                let cpu_cost = selected_rows * self.cpu_tuple_cost;

                let residual_cost = if scan.residual.is_some() {
                    selected_rows * self.cpu_operator_cost
                } else {
                    0.0
                };

                traversal_io + leaf_io + heap_io + cpu_cost + residual_cost
            }

            PhysicalOperator::IndexOnlyScan(scan) => {
                let table_stats = provider.get_table_stats(scan.table_id);
                let index_stats = provider.get_index_stats(scan.index_id);

                let table_rows = table_stats
                    .map(|s| s.row_count as f64)
                    .unwrap_or(DEFAULT_ROW_COUNT);

                let height = index_stats
                    .as_ref()
                    .map(|s| s.height)
                    .unwrap_or(DEFAULT_INDEX_HEIGHT);

                let selectivity = index_stats
                    .as_ref()
                    .filter(|s| s.distinct_keys > 0)
                    .map(|s| 1.0 / s.distinct_keys as f64)
                    .unwrap_or(DEFAULT_SELECTIVITY);

                let selected_rows = (table_rows * selectivity).max(1.0);

                let traversal_io = height as f64 * self.page_io_cost;
                let leaf_pages = (selected_rows / 100.0).ceil().max(1.0);
                let leaf_io = leaf_pages * self.page_io_cost;
                let cpu_cost = selected_rows * self.cpu_tuple_cost;

                traversal_io + leaf_io + cpu_cost
            }

            PhysicalOperator::Filter(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                input.row_count * self.cpu_operator_cost
            }

            PhysicalOperator::Project(proj) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let num_exprs = proj.expressions.len().max(1);
                input.row_count * self.cpu_operator_cost * num_exprs as f64
            }

            PhysicalOperator::NestedLoopJoin(join) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let inner_size = right.row_count * right.avg_row_size;
                let inner_pages = self.estimate_pages(right.row_count, right.avg_row_size);

                let io_cost = if inner_size < self.memory_threshold as f64 {
                    inner_pages * self.page_io_cost
                } else {
                    left.row_count * inner_pages * self.page_io_cost
                };

                let cpu_cost = left.row_count * right.row_count * self.cpu_tuple_cost;

                let condition_cost = if join.condition.is_some() {
                    left.row_count * right.row_count * self.cpu_operator_cost
                } else {
                    0.0
                };

                io_cost + cpu_cost + condition_cost
            }

            PhysicalOperator::HashJoin(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let (build, probe) = if left.row_count <= right.row_count {
                    (&left, &right)
                } else {
                    (&right, &left)
                };

                let build_pages = self.estimate_pages(build.row_count, build.avg_row_size);
                let build_io = build_pages * self.page_io_cost;
                let build_cpu = build.row_count * self.cpu_tuple_cost * 1.5;

                let probe_pages = self.estimate_pages(probe.row_count, probe.avg_row_size);
                let probe_io = probe_pages * self.page_io_cost;
                let probe_cpu = probe.row_count * self.cpu_tuple_cost * 1.2;

                let build_size = build.row_count * build.avg_row_size;
                let spill_cost = if build_size > self.memory_threshold as f64 {
                    build_pages * self.page_io_cost * 2.0
                } else {
                    0.0
                };

                build_io + build_cpu + probe_io + probe_cpu + spill_cost
            }

            PhysicalOperator::MergeJoin(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let left_pages = self.estimate_pages(left.row_count, left.avg_row_size);
                let right_pages = self.estimate_pages(right.row_count, right.avg_row_size);
                let io_cost = (left_pages + right_pages) * self.page_io_cost;
                let cpu_cost = (left.row_count + right.row_count) * self.cpu_tuple_cost;

                io_cost + cpu_cost
            }

            PhysicalOperator::HashAggregate(agg) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let num_groups = if agg.group_by.is_empty() {
                    1.0
                } else {
                    (input.row_count * 0.1).max(1.0)
                };

                let build_cpu = input.row_count * self.cpu_tuple_cost * 1.5;
                let output_cpu = num_groups * self.cpu_tuple_cost;

                let input_size = input.row_count * input.avg_row_size;
                let spill_cost = if input_size > self.memory_threshold as f64 {
                    let pages = self.estimate_pages(input.row_count, input.avg_row_size);
                    pages * self.page_io_cost * 2.0
                } else {
                    0.0
                };

                build_cpu + output_cpu + spill_cost
            }

            PhysicalOperator::SortAggregate(agg) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let num_groups = if agg.group_by.is_empty() {
                    1.0
                } else {
                    (input.row_count * 0.1).max(1.0)
                };

                let scan_cpu = input.row_count * self.cpu_tuple_cost;
                let output_cpu = num_groups * self.cpu_tuple_cost;

                scan_cpu + output_cpu
            }

            PhysicalOperator::Sort(_) | PhysicalOperator::ExternalSort(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                if input.row_count <= 1.0 {
                    return 0.0;
                }

                let cpu_cost = input.row_count * input.row_count.log2() * self.cpu_operator_cost;

                let input_size = input.row_count * input.avg_row_size;
                let io_cost = if input_size > self.memory_threshold as f64 {
                    let pages = self.estimate_pages(input.row_count, input.avg_row_size);
                    let mem_pages = self.memory_threshold as f64 / self.page_size as f64;
                    let passes = (pages / mem_pages).log2().ceil().max(1.0);
                    pages * passes * 2.0 * self.page_io_cost
                } else {
                    0.0
                };

                cpu_cost + io_cost
            }

            PhysicalOperator::TopN(topn) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let k = topn.limit as f64;

                input.row_count * self.cpu_tuple_cost
                    + input.row_count * k.log2().max(1.0) * self.cpu_operator_cost
            }

            PhysicalOperator::Limit(limit_op) => {
                let output = limit_op.limit as f64;
                output * self.cpu_tuple_cost
            }

            PhysicalOperator::HashDistinct(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let build_cpu = input.row_count * self.cpu_tuple_cost * 1.5;
                let distinct_rows = input.row_count * 0.9;
                let output_cpu = distinct_rows * self.cpu_tuple_cost;

                let input_size = input.row_count * input.avg_row_size;
                let spill_cost = if input_size > self.memory_threshold as f64 {
                    let pages = self.estimate_pages(input.row_count, input.avg_row_size);
                    pages * self.page_io_cost * 2.0
                } else {
                    0.0
                };

                build_cpu + output_cpu + spill_cost
            }

            PhysicalOperator::SortDistinct(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                input.row_count * self.cpu_tuple_cost
            }

            PhysicalOperator::HashUnion(union_op) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let total = left.row_count + right.row_count;

                if union_op.all {
                    total * self.cpu_tuple_cost
                } else {
                    let build_cpu = total * self.cpu_tuple_cost * 1.5;
                    let output_rows = total * 0.7;
                    build_cpu + output_rows * self.cpu_tuple_cost
                }
            }

            PhysicalOperator::HashIntersect(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let (build, probe) = if left.row_count <= right.row_count {
                    (&left, &right)
                } else {
                    (&right, &left)
                };

                let build_cpu = build.row_count * self.cpu_tuple_cost * 1.5;
                let probe_cpu = probe.row_count * self.cpu_tuple_cost * 1.2;
                let output_rows = build.row_count.min(probe.row_count) * 0.5;

                build_cpu + probe_cpu + output_rows * self.cpu_tuple_cost
            }

            PhysicalOperator::HashExcept(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let build_cpu = right.row_count * self.cpu_tuple_cost * 1.5;
                let probe_cpu = left.row_count * self.cpu_tuple_cost * 1.2;
                let output_rows = (left.row_count - right.row_count * 0.3).max(0.0);

                build_cpu + probe_cpu + output_rows * self.cpu_tuple_cost
            }

            PhysicalOperator::Insert(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let heap_io =
                    self.estimate_pages(input.row_count, input.avg_row_size) * self.page_io_cost;
                let index_io = input.row_count * 2.0 * self.page_io_cost * 0.1;
                let cpu_cost = input.row_count * self.cpu_tuple_cost * 2.0;

                heap_io + index_io + cpu_cost
            }

            PhysicalOperator::Update(update) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let read_io =
                    self.estimate_pages(input.row_count, input.avg_row_size) * self.page_io_cost;
                let write_io = self.estimate_pages(input.row_count, input.avg_row_size)
                    * self.page_io_cost
                    * self.random_io_multiplier;
                let num_cols = update.assignments.len() as f64;
                let index_io = input.row_count * num_cols * self.page_io_cost * 0.1;
                let cpu_cost = input.row_count * self.cpu_tuple_cost * (2.0 + num_cols);

                read_io + write_io + index_io + cpu_cost
            }

            PhysicalOperator::Delete(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let delete_io = self.estimate_pages(input.row_count, input.avg_row_size)
                    * self.page_io_cost
                    * self.random_io_multiplier;
                let index_io = input.row_count * 2.0 * self.page_io_cost * 0.1;
                let cpu_cost = input.row_count * self.cpu_tuple_cost;

                delete_io + index_io + cpu_cost
            }

            PhysicalOperator::Values(values) => {
                let rows = values.rows.len() as f64;
                let cols = values.rows.first().map(|r| r.len()).unwrap_or(1) as f64;
                rows * cols * self.cpu_operator_cost
            }

            PhysicalOperator::Empty(_) => 0.0,

            PhysicalOperator::Exchange(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let serialize_cpu = input.row_count * self.cpu_tuple_cost * 5.0;
                let network_cost = input.row_count * input.avg_row_size / 1000.0;
                let deserialize_cpu = input.row_count * self.cpu_tuple_cost * 5.0;

                serialize_cpu + network_cost + deserialize_cpu
            }
        }
    }

    fn derive_stats<P: StatisticsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &P,
        children_stats: &[DerivedStats],
    ) -> DerivedStats {
        match op {
            PhysicalOperator::SeqScan(scan) => {
                let (rows, size) = provider
                    .get_table_stats(scan.table_id)
                    .map(|s| (s.row_count as f64, s.avg_row_size))
                    .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

                let selectivity = if scan.predicate.is_some() {
                    DEFAULT_SELECTIVITY
                } else {
                    1.0
                };

                DerivedStats::new(rows * selectivity, size)
            }

            PhysicalOperator::IndexScan(scan) => {
                let table_stats = provider.get_table_stats(scan.table_id);
                let index_stats = provider.get_index_stats(scan.index_id);

                let (rows, size) = table_stats
                    .as_ref()
                    .map(|s| (s.row_count as f64, s.avg_row_size))
                    .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

                let selectivity = index_stats
                    .as_ref()
                    .filter(|s| s.distinct_keys > 0)
                    .map(|s| 1.0 / s.distinct_keys as f64)
                    .unwrap_or(DEFAULT_SELECTIVITY);

                DerivedStats::new((rows * selectivity).max(1.0), size)
            }

            PhysicalOperator::IndexOnlyScan(scan) => {
                let table_stats = provider.get_table_stats(scan.table_id);
                let index_stats = provider.get_index_stats(scan.index_id);

                let rows = table_stats
                    .map(|s| s.row_count as f64)
                    .unwrap_or(DEFAULT_ROW_COUNT);

                let avg_size = index_stats
                    .as_ref()
                    .map(|s| s.avg_key_size)
                    .unwrap_or(DEFAULT_AVG_ROW_SIZE * 0.3);

                let selectivity = index_stats
                    .as_ref()
                    .filter(|s| s.distinct_keys > 0)
                    .map(|s| 1.0 / s.distinct_keys as f64)
                    .unwrap_or(DEFAULT_SELECTIVITY);

                DerivedStats::new((rows * selectivity).max(1.0), avg_size)
            }

            PhysicalOperator::Filter(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                DerivedStats::new(input.row_count * DEFAULT_SELECTIVITY, input.avg_row_size)
            }

            PhysicalOperator::Project(proj) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let size_ratio =
                    proj.expressions.len() as f64 / proj.input_schema.columns().len().max(1) as f64;
                DerivedStats::new(input.row_count, input.avg_row_size * size_ratio.min(1.0))
            }

            PhysicalOperator::NestedLoopJoin(_)
            | PhysicalOperator::HashJoin(_)
            | PhysicalOperator::MergeJoin(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let output_rows = (left.row_count * right.row_count * 0.1).max(1.0);
                let output_size = left.avg_row_size + right.avg_row_size;

                DerivedStats::new(output_rows, output_size)
            }

            PhysicalOperator::HashAggregate(agg) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let num_groups = if agg.group_by.is_empty() {
                    1.0
                } else {
                    (input.row_count * 0.1).max(1.0)
                };

                let output_size = (agg.group_by.len() + agg.aggregates.len()) as f64 * 8.0;

                DerivedStats::new(num_groups, output_size.max(16.0))
            }
            PhysicalOperator::SortAggregate(agg) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());

                let num_groups = if agg.group_by.is_empty() {
                    1.0
                } else {
                    (input.row_count * 0.1).max(1.0)
                };

                let output_size = (agg.group_by.len() + agg.aggregates.len()) as f64 * 8.0;

                DerivedStats::new(num_groups, output_size.max(16.0))
            }

            PhysicalOperator::Sort(_) | PhysicalOperator::ExternalSort(_) => children_stats
                .first()
                .cloned()
                .unwrap_or(DerivedStats::unknown()),

            PhysicalOperator::TopN(topn) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let limit = topn.limit as f64;
                DerivedStats::new(limit.min(input.row_count), input.avg_row_size)
            }

            PhysicalOperator::Limit(limit_op) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let limit = limit_op.limit as f64;
                DerivedStats::new(limit.min(input.row_count), input.avg_row_size)
            }

            PhysicalOperator::HashDistinct(_) | PhysicalOperator::SortDistinct(_) => {
                let input = children_stats
                    .first()
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                DerivedStats::new(input.row_count * 0.9, input.avg_row_size)
            }

            PhysicalOperator::HashUnion(u) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let total = left.row_count + right.row_count;
                let rows = if u.all { total } else { total * 0.7 };
                DerivedStats::new(rows, left.avg_row_size.max(right.avg_row_size))
            }

            PhysicalOperator::HashIntersect(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let rows = left.row_count.min(right.row_count) * 0.5;
                DerivedStats::new(rows, left.avg_row_size)
            }

            PhysicalOperator::HashExcept(_) => {
                let left = children_stats
                    .get(0)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let right = children_stats
                    .get(1)
                    .cloned()
                    .unwrap_or(DerivedStats::unknown());
                let rows = (left.row_count - right.row_count * 0.3).max(0.0);
                DerivedStats::new(rows, left.avg_row_size)
            }

            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => DerivedStats::new(1.0, 8.0),

            PhysicalOperator::Values(v) => {
                let rows = v.rows.len() as f64;
                let cols = v.rows.first().map(|r| r.len()).unwrap_or(1);
                DerivedStats::new(rows, cols as f64 * 8.0)
            }

            PhysicalOperator::Empty(_) => DerivedStats::new(0.0, 0.0),

            PhysicalOperator::Exchange(_) => children_stats
                .first()
                .cloned()
                .unwrap_or(DerivedStats::unknown()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        TRANSACTION_ZERO,
        database::{
            Database,
            schema::Schema,
            stats::{
                CatalogStatsProvider, StatsComputeConfig, StatsComputeError, StatsComputeResult,
            },
        },
        io::pager::{Pager, SharedPager},
        sql::{ast::JoinType, binder::ast::BoundExpression},
        storage::tuple::{OwnedTuple, Tuple},
        types::{Blob, DataType, DataTypeKind, Int32, Int64, UInt8, UInt64},
    };
    use std::{io, path::Path};
    use tempfile::NamedTempFile;

    fn create_test_pager(path: impl AsRef<Path>, cache_size: usize) -> io::Result<Pager> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(&path);

        let config = crate::AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(cache_size as u16),
            incremental_vacuum_mode: crate::IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: crate::TextEncoding::Utf8,
        };

        Pager::from_config(config, &path)
    }

    fn create_test_database() -> io::Result<(Database, NamedTempFile)> {
        let temp_file = NamedTempFile::new()?;
        let pager = SharedPager::from(create_test_pager(temp_file.path(), 100)?);
        let db = Database::new(pager, 3, 2)?;
        Ok((db, temp_file))
    }

    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_column("id", DataTypeKind::BigInt, true, true, false);
        schema.add_column("name", DataTypeKind::Text, false, false, false);
        schema.add_column("age", DataTypeKind::Int, false, false, false);
        schema.set_num_keys(1);
        schema
    }

    fn create_index_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_column("name", DataTypeKind::Text, true, true, false);
        schema.add_column("row_id", DataTypeKind::BigUInt, false, false, false);
        schema.set_num_keys(1);
        schema
    }

    fn populate_table(
        db: &Database,
        table_name: &str,
        schema: &Schema,
        count: i64,
    ) -> io::Result<()> {
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let relation = catalog.get_relation(table_name, worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker)?;

        for i in 0..count {
            let tuple = Tuple::new(
                &[
                    DataType::BigInt(Int64(i)),
                    DataType::Text(Blob::from(format!("User {}", i).as_str())),
                    DataType::Int(crate::types::Int32((i % 50) as i32 + 18)),
                ],
                schema,
                TRANSACTION_ZERO,
            )?;
            let owned: OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();
        Ok(())
    }

    fn populate_index(
        db: &Database,
        index_name: &str,
        schema: &Schema,
        count: u64,
    ) -> io::Result<()> {
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let relation = catalog.get_relation(index_name, worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.index_btree(root, DataTypeKind::Text, worker)?;

        for i in 0..count {
            let tuple = Tuple::new(
                &[
                    DataType::Text(Blob::from(format!("user{}@example.com", i).as_str())),
                    DataType::BigUInt(UInt64(i)),
                ],
                schema,
                TRANSACTION_ZERO,
            )?;
            let owned: OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();
        Ok(())
    }

    #[test]
    fn test_seq_scan_cost_with_catalog_stats() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        let id = catalog.create_table("users", schema.clone(), worker.clone())?;
        populate_table(&db, "users", &schema, 1000)?;

        // Run ANALYZE to compute statistics
        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("users", worker.clone(), &config)?;

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let scan_op = PhysicalOperator::SeqScan(SeqScanOp::new(id, schema.clone()));

        let cost = cost_model.compute_operator_cost(&scan_op, &provider, &[]);

        assert!(cost > 0.0);

        // Verify stats are being used correctly
        let stats = provider
            .get_table_stats(id)
            .ok_or(StatsComputeError::InvalidObjectType)?;
        assert_eq!(stats.row_count, 1000);

        // Cost should scale with row count
        let expected_pages = (1000.0 * stats.avg_row_size / 4096.0).ceil().max(1.0);
        let expected_io = expected_pages * 1.0; // page_io_cost = 1.0
        let expected_cpu = 1000.0 * 0.01; // cpu_tuple_cost = 0.01

        assert!((cost - (expected_io + expected_cpu)).abs() < 1.0);

        Ok(())
    }

    #[test]
    fn test_seq_scan_cost_scales_with_table_size() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();

        // Create small table
        let small_id = catalog.create_table("small_users", schema.clone(), worker.clone())?;
        populate_table(&db, "small_users", &schema, 100)?;

        // Create large table
        let large_id = catalog.create_table("large_users", schema.clone(), worker.clone())?;
        populate_table(&db, "large_users", &schema, 10000)?;

        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("small_users", worker.clone(), &config)?;
        catalog.recompute_table_statistics("large_users", worker.clone(), &config)?;

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let small_scan = PhysicalOperator::SeqScan(SeqScanOp::new(small_id, schema.clone()));

        let large_scan = PhysicalOperator::SeqScan(SeqScanOp::new(large_id, schema.clone()));

        let small_cost = cost_model.compute_operator_cost(&small_scan, &provider, &[]);
        let large_cost = cost_model.compute_operator_cost(&large_scan, &provider, &[]);

        // Large table should cost significantly more
        assert!(large_cost > small_cost * 50.0);

        Ok(())
    }

    #[test]
    fn test_index_scan_cheaper_than_seq_scan_for_selective() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let table_schema = create_test_schema();
        let index_schema = create_index_schema();

        let table_id = catalog.create_table("users", table_schema.clone(), worker.clone())?;
        let index_id =
            catalog.create_index("idx_users_name", index_schema.clone(), worker.clone())?;

        populate_table(&db, "users", &table_schema, 100000)?;
        populate_index(&db, "idx_users_name", &index_schema, 100000)?;

        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("users", worker.clone(), &config)?;
        catalog.recompute_index_statistics("idx_users_name", worker.clone(), &config)?;

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let seq_scan = PhysicalOperator::SeqScan(SeqScanOp::new(table_id, table_schema.clone()));

        let index_scan = PhysicalOperator::IndexScan(PhysIndexScanOp {
            table_id,

            index_id,
            schema: table_schema.clone(),
            index_columns: vec![1],
            range_start: None,
            range_end: None,
            residual: None,
        });

        let seq_cost = cost_model.compute_operator_cost(&seq_scan, &provider, &[]);
        let idx_cost = cost_model.compute_operator_cost(&index_scan, &provider, &[]);

        // For highly selective index lookup, index scan should be cheaper
        assert!(idx_cost < seq_cost);

        Ok(())
    }

    #[test]
    fn test_hash_join_cheaper_than_nested_loop() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let left_stats = DerivedStats::new(10000.0, 100.0);
        let right_stats = DerivedStats::new(10000.0, 100.0);
        let children = vec![left_stats, right_stats];

        let schema = Schema::new();

        let nested_loop = PhysicalOperator::NestedLoopJoin(NestedLoopJoinOp::new(
            crate::sql::ast::JoinType::Inner,
            None,
            schema.clone(),
        ));

        let hash_join = PhysicalOperator::HashJoin(HashJoinOp::new(
            JoinType::Inner,
            vec![],
            vec![],
            None,
            schema.clone(),
        ));

        let nl_cost = cost_model.compute_operator_cost(&nested_loop, &provider, &children);
        let hj_cost = cost_model.compute_operator_cost(&hash_join, &provider, &children);

        // Hash join should be much cheaper for equal-sized inputs
        assert!(hj_cost < nl_cost);
        // Nested loop is O(n*m), hash join is O(n+m)
        assert!(nl_cost > hj_cost * 100.0);

        Ok(())
    }

    #[test]
    fn test_derive_stats_from_scan() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        let table_id = catalog.create_table("users", schema.clone(), worker.clone())?;
        populate_table(&db, "users", &schema, 5000)?;

        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("users", worker.clone(), &config)?;

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let scan_op = PhysicalOperator::SeqScan(SeqScanOp::new(table_id, schema.clone()));

        let derived = cost_model.derive_stats(&scan_op, &provider, &[]);

        assert_eq!(derived.row_count, 5000.0);
        assert!(derived.avg_row_size > 0.0);

        Ok(())
    }

    #[test]
    fn test_derive_stats_propagates_through_filter() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        let table_id = catalog.create_table("users", schema.clone(), worker.clone())?;
        populate_table(&db, "users", &schema, 10000)?;

        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("users", worker.clone(), &config)?;

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        // First derive scan stats
        let scan_op = PhysicalOperator::SeqScan(SeqScanOp::new(table_id, schema.clone()));
        let scan_stats = cost_model.derive_stats(&scan_op, &provider, &[]);

        // Then derive filter stats
        let filter_op = PhysicalOperator::Filter(PhysFilterOp::new(
            BoundExpression::Literal {
                value: DataType::Boolean(UInt8::from(true)),
            },
            schema.clone(),
        ));
        let filter_stats = cost_model.derive_stats(&filter_op, &provider, &[scan_stats.clone()]);

        // Filter should reduce row count by default selectivity (10%)
        assert_eq!(filter_stats.row_count, scan_stats.row_count * 0.1);
        assert_eq!(filter_stats.avg_row_size, scan_stats.avg_row_size);

        Ok(())
    }

    #[test]
    fn test_derive_stats_for_join() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let left_stats = DerivedStats::new(1000.0, 50.0);
        let right_stats = DerivedStats::new(500.0, 30.0);

        let join_op = PhysicalOperator::HashJoin(HashJoinOp::new(
            JoinType::Inner,
            vec![],
            vec![],
            None,
            Schema::new(),
        ));

        let join_stats = cost_model.derive_stats(
            &join_op,
            &provider,
            &[left_stats.clone(), right_stats.clone()],
        );

        // Join output: left * right * selectivity (0.1)
        let expected_rows = 1000.0 * 500.0 * 0.1;
        assert_eq!(join_stats.row_count, expected_rows);

        // Output size is sum of both sides
        assert_eq!(join_stats.avg_row_size, 50.0 + 30.0);

        Ok(())
    }

    #[test]
    fn test_limit_reduces_cost() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let input_stats = DerivedStats::new(100000.0, 100.0);

        let limit_10 = PhysicalOperator::Limit(PhysLimitOp::new(10, None, Schema::new()));
        let limit_1000 = PhysicalOperator::Limit(PhysLimitOp::new(1000, None, Schema::new()));

        let cost_10 =
            cost_model.compute_operator_cost(&limit_10, &provider, &[input_stats.clone()]);
        let cost_1000 =
            cost_model.compute_operator_cost(&limit_1000, &provider, &[input_stats.clone()]);

        // Smaller limit should be cheaper
        assert!(cost_10 < cost_1000);

        // Derived stats should reflect limit
        let stats_10 = cost_model.derive_stats(&limit_10, &provider, &[input_stats.clone()]);
        let stats_1000 = cost_model.derive_stats(&limit_1000, &provider, &[input_stats.clone()]);

        assert_eq!(stats_10.row_count, 10.0);
        assert_eq!(stats_1000.row_count, 1000.0);

        Ok(())
    }

    #[test]
    fn test_empty_has_zero_cost() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let empty_op = PhysicalOperator::Empty(PhysEmptyOp::new(Schema::new()));
        let cost = cost_model.compute_operator_cost(&empty_op, &provider, &[]);

        assert_eq!(cost, 0.0);

        let stats = cost_model.derive_stats(&empty_op, &provider, &[]);
        assert_eq!(stats.row_count, 0.0);

        Ok(())
    }

    #[test]
    fn test_sort_cost_increases_with_size() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let small_input = DerivedStats::new(100.0, 100.0);
        let large_input = DerivedStats::new(100000.0, 100.0);

        let sort_op = PhysicalOperator::Sort(EnforcerSortOp::new(vec![], Schema::new()));

        let small_cost = cost_model.compute_operator_cost(&sort_op, &provider, &[small_input]);
        let large_cost = cost_model.compute_operator_cost(&sort_op, &provider, &[large_input]);

        // O(n log n) - large should be significantly more expensive
        assert!(large_cost > small_cost * 100.0);

        Ok(())
    }

    #[test]
    fn test_aggregate_reduces_rows() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let input_stats = DerivedStats::new(10000.0, 100.0);

        // Aggregate with group by
        let agg_op = PhysicalOperator::HashAggregate(HashAggregateOp::new(
            vec![BoundExpression::Literal {
                value: DataType::Int(Int32(1)),
            }],
            vec![],
            Schema::new(),
            Schema::new(),
        ));

        let agg_stats = cost_model.derive_stats(&agg_op, &provider, &[input_stats.clone()]);

        // Should reduce to ~10% of input (default group estimate)
        assert!(agg_stats.row_count < input_stats.row_count);
        assert_eq!(agg_stats.row_count, 1000.0); // 10000 * 0.1

        Ok(())
    }

    #[test]
    fn test_values_cost_scales_with_rows() -> StatsComputeResult<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let cost_model = AxmosCostModel::new(4096);

        let small_values = PhysicalOperator::Values(PhysValuesOp::new(
            vec![
                vec![BoundExpression::Literal {
                    value: DataType::Int(crate::types::Int32(1)),
                }];
                10
            ],
            Schema::new(),
        ));

        let large_values = PhysicalOperator::Values(PhysValuesOp::new(
            vec![
                vec![BoundExpression::Literal {
                    value: DataType::Int(Int32(1)),
                }];
                1000
            ],
            Schema::new(),
        ));

        let small_cost = cost_model.compute_operator_cost(&small_values, &provider, &[]);
        let large_cost = cost_model.compute_operator_cost(&large_values, &provider, &[]);

        assert!(large_cost > small_cost * 50.0);

        Ok(())
    }
}
