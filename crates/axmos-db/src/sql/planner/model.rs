//! Cost model for the Cascades optimizer.
//!
//! Estimates execution cost of physical operators based on I/O and CPU costs.
//! Designed for single-node execution.

use crate::schema::catalog::StatsProvider;

use super::physical::*;

const DEFAULT_PAGE_IO_COST: f64 = 1.0;
const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
const RANDOM_IO_MULTIPLIER: f64 = 1.2;
const MEMORY_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB

const DEFAULT_ROW_COUNT: f64 = 1000.0;
const DEFAULT_AVG_ROW_SIZE: f64 = 100.0;
const DEFAULT_SELECTIVITY: f64 = 0.1;
const DEFAULT_INDEX_HEIGHT: usize = 3;
const DEFAULT_NDV: u64 = 100;

/// Statistics derived during cost computation.
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

    pub fn data_size(&self) -> f64 {
        self.row_count * self.avg_row_size
    }
}

/// Cost model trait.
pub trait CostModel {
    fn compute_cost<S: StatsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &S,
        children_stats: &[DerivedStats],
    ) -> f64;

    fn derive_stats<S: StatsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &S,
        children_stats: &[DerivedStats],
    ) -> DerivedStats;
}

/// Default cost model implementation.
pub struct DefaultCostModel {
    pub page_size: u32,
    pub page_io_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_operator_cost: f64,
    pub random_io_multiplier: f64,
    pub memory_threshold: u64,
}

impl DefaultCostModel {
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

    fn estimate_pages(&self, rows: f64, avg_row_size: f64) -> f64 {
        let rows_per_page = (self.page_size as f64 / avg_row_size.max(1.0)).max(1.0);
        (rows / rows_per_page).ceil().max(1.0)
    }

    fn exceeds_memory(&self, rows: f64, avg_row_size: f64) -> bool {
        rows * avg_row_size > self.memory_threshold as f64
    }

    fn external_sort_io_cost(&self, rows: f64, avg_row_size: f64) -> f64 {
        let pages = self.estimate_pages(rows, avg_row_size);
        let mem_pages = self.memory_threshold as f64 / self.page_size as f64;
        let passes = (pages / mem_pages).log2().ceil().max(1.0);
        pages * passes * 2.0 * self.page_io_cost
    }
}

impl CostModel for DefaultCostModel {
    fn compute_cost<S: StatsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &S,
        children_stats: &[DerivedStats],
    ) -> f64 {
        match op {
            PhysicalOperator::SeqScan(scan) => self.cost_seq_scan(scan, provider),
            PhysicalOperator::IndexScan(scan) => self.cost_index_scan(scan, provider),
            PhysicalOperator::Filter(_) => self.cost_filter(children_stats),
            PhysicalOperator::Project(proj) => self.cost_project(proj, children_stats),
            PhysicalOperator::Materialize(_) => self.cost_materialize(children_stats),
            PhysicalOperator::NestedLoopJoin(join) => {
                self.cost_nested_loop_join(join, children_stats)
            }
            PhysicalOperator::HashJoin(_) => self.cost_hash_join(children_stats),
            PhysicalOperator::MergeJoin(_) => self.cost_merge_join(children_stats),
            PhysicalOperator::HashAggregate(agg) => self.cost_hash_aggregate(agg, children_stats),

            PhysicalOperator::Sort(_) => self.cost_sort(children_stats),
            PhysicalOperator::Distinct(_) => self.cost_distinct(children_stats),

            PhysicalOperator::Limit(limit) => self.cost_limit(limit),

            PhysicalOperator::Insert(_) => self.cost_insert(children_stats),
            PhysicalOperator::Update(update) => self.cost_update(update, children_stats),
            PhysicalOperator::Delete(_) => self.cost_delete(children_stats),
            PhysicalOperator::Values(values) => self.cost_values(values),
            PhysicalOperator::Empty(_) => 0.0,
        }
    }

    fn derive_stats<S: StatsProvider>(
        &self,
        op: &PhysicalOperator,
        provider: &S,
        children_stats: &[DerivedStats],
    ) -> DerivedStats {
        match op {
            PhysicalOperator::SeqScan(scan) => self.stats_seq_scan(scan, provider),
            PhysicalOperator::IndexScan(scan) => self.stats_index_scan(scan, provider),
            PhysicalOperator::Distinct(_) => self.stats_distinct(children_stats),

            PhysicalOperator::Filter(_) => self.stats_filter(children_stats),
            PhysicalOperator::Project(proj) => self.stats_project(proj, children_stats),
            PhysicalOperator::Materialize(_) => self.stats_passthrough(children_stats),
            PhysicalOperator::NestedLoopJoin(_)
            | PhysicalOperator::HashJoin(_)
            | PhysicalOperator::MergeJoin(_) => self.stats_join(children_stats),
            PhysicalOperator::HashAggregate(agg) => self.stats_hash_aggregate(agg, children_stats),

            PhysicalOperator::Sort(_) => self.stats_passthrough(children_stats),

            PhysicalOperator::Limit(limit) => self.stats_limit(limit, children_stats),

            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => DerivedStats::new(1.0, 8.0),
            PhysicalOperator::Values(v) => self.stats_values(v),
            PhysicalOperator::Empty(_) => DerivedStats::new(0.0, 0.0),
        }
    }
}

// Cost computation
impl DefaultCostModel {
    fn cost_seq_scan<S: StatsProvider>(&self, scan: &SeqScanOp, provider: &S) -> f64 {
        let (rows, avg_size, pages) = provider
            .get_stats(scan.table_id)
            .map(|s| (s.row_count as f64, s.avg_row_size, s.page_count as f64))
            .unwrap_or((
                DEFAULT_ROW_COUNT,
                DEFAULT_AVG_ROW_SIZE,
                self.estimate_pages(DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE),
            ));

        let io_cost = pages * self.page_io_cost;
        let cpu_cost = rows * self.cpu_tuple_cost;
        let filter_cost = scan
            .predicate
            .as_ref()
            .map_or(0.0, |_| rows * self.cpu_operator_cost);

        io_cost + cpu_cost + filter_cost
    }

    fn cost_index_scan<S: StatsProvider>(&self, scan: &PhysIndexScanOp, provider: &S) -> f64 {
        let (table_rows, avg_size) = provider
            .get_stats(scan.table_id)
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        // Use NDV from first index column for selectivity
        let ndv = provider
            .get_column_stats(scan.index_id, 0)
            .map(|c| c.ndv)
            .unwrap_or(DEFAULT_NDV);

        let selectivity = if ndv > 0 {
            1.0 / ndv as f64
        } else {
            DEFAULT_SELECTIVITY
        };
        let selected_rows = (table_rows * selectivity).max(1.0);

        let traversal_io = DEFAULT_INDEX_HEIGHT as f64 * self.page_io_cost;
        let leaf_io = (selected_rows / 100.0).ceil().max(1.0) * self.page_io_cost;
        let heap_io = self.estimate_pages(selected_rows, avg_size)
            * self.page_io_cost
            * self.random_io_multiplier
            * 0.5;
        let cpu_cost = selected_rows * self.cpu_tuple_cost;
        let residual_cost = scan
            .residual
            .as_ref()
            .map_or(0.0, |_| selected_rows * self.cpu_operator_cost);

        traversal_io + leaf_io + heap_io + cpu_cost + residual_cost
    }

    fn cost_filter(&self, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        input.row_count * self.cpu_operator_cost
    }

    fn cost_project(&self, proj: &PhysProjectOp, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        input.row_count * self.cpu_operator_cost * proj.expressions.len().max(1) as f64
    }

    fn cost_materialize(&self, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let pages = self.estimate_pages(input.row_count, input.avg_row_size);
        pages * self.page_io_cost * 0.1 + input.row_count * self.cpu_tuple_cost
    }

    fn cost_nested_loop_join(&self, join: &NestedLoopJoinOp, children: &[DerivedStats]) -> f64 {
        let left = children
            .get(0)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let right = children
            .get(1)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);

        let inner_size = right.data_size();
        let inner_pages = self.estimate_pages(right.row_count, right.avg_row_size);

        let io_cost = if inner_size < self.memory_threshold as f64 {
            inner_pages * self.page_io_cost
        } else {
            left.row_count * inner_pages * self.page_io_cost
        };

        let cpu_cost = left.row_count * right.row_count * self.cpu_tuple_cost;
        let cond_cost = join.condition.as_ref().map_or(0.0, |_| {
            left.row_count * right.row_count * self.cpu_operator_cost
        });

        io_cost + cpu_cost + cond_cost
    }

    fn cost_hash_join(&self, children: &[DerivedStats]) -> f64 {
        let left = children
            .get(0)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let right = children
            .get(1)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);

        let (build, probe) = if left.row_count <= right.row_count {
            (&left, &right)
        } else {
            (&right, &left)
        };

        let build_pages = self.estimate_pages(build.row_count, build.avg_row_size);
        let probe_pages = self.estimate_pages(probe.row_count, probe.avg_row_size);

        let build_cost =
            build_pages * self.page_io_cost + build.row_count * self.cpu_tuple_cost * 1.5;
        let probe_cost =
            probe_pages * self.page_io_cost + probe.row_count * self.cpu_tuple_cost * 1.2;

        let spill_cost = if self.exceeds_memory(build.row_count, build.avg_row_size) {
            build_pages * self.page_io_cost * 2.0
        } else {
            0.0
        };

        build_cost + probe_cost + spill_cost
    }

    fn cost_distinct(&self, children: &[DerivedStats]) -> f64 {
        children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown)
            .row_count
            * self.cpu_tuple_cost
    }

    fn cost_merge_join(&self, children: &[DerivedStats]) -> f64 {
        let left = children
            .get(0)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let right = children
            .get(1)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);

        let io_cost = (self.estimate_pages(left.row_count, left.avg_row_size)
            + self.estimate_pages(right.row_count, right.avg_row_size))
            * self.page_io_cost;
        let cpu_cost = (left.row_count + right.row_count) * self.cpu_tuple_cost;

        io_cost + cpu_cost
    }

    fn cost_hash_aggregate(&self, agg: &HashAggregateOp, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);

        let num_groups = if agg.group_by.is_empty() {
            1.0
        } else {
            (input.row_count * 0.1).max(1.0)
        };

        let build_cpu = input.row_count * self.cpu_tuple_cost * 1.5;
        let output_cpu = num_groups * self.cpu_tuple_cost;

        let spill_cost = if self.exceeds_memory(input.row_count, input.avg_row_size) {
            self.estimate_pages(input.row_count, input.avg_row_size) * self.page_io_cost * 2.0
        } else {
            0.0
        };

        build_cpu + output_cpu + spill_cost
    }

    fn cost_limit(&self, limit: &PhysLimitOp) -> f64 {
        limit.limit as f64 * self.cpu_tuple_cost
    }

    fn cost_insert(&self, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        self.estimate_pages(input.row_count, input.avg_row_size) * self.page_io_cost
            + input.row_count * 2.0 * self.page_io_cost * 0.1
            + input.row_count * self.cpu_tuple_cost * 2.0
    }

    fn cost_update(&self, update: &PhysUpdateOp, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let pages = self.estimate_pages(input.row_count, input.avg_row_size);
        let num_cols = update.assignments.len() as f64;
        pages * self.page_io_cost * (1.0 + self.random_io_multiplier)
            + input.row_count * num_cols * self.page_io_cost * 0.1
            + input.row_count * self.cpu_tuple_cost * (2.0 + num_cols)
    }

    fn cost_delete(&self, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let pages = self.estimate_pages(input.row_count, input.avg_row_size);
        pages * self.page_io_cost * self.random_io_multiplier
            + input.row_count * 2.0 * self.page_io_cost * 0.1
            + input.row_count * self.cpu_tuple_cost
    }

    fn cost_values(&self, values: &PhysValuesOp) -> f64 {
        let rows = values.rows.len() as f64;
        let cols = values.rows.first().map(|r| r.len()).unwrap_or(1) as f64;
        rows * cols * self.cpu_operator_cost
    }
}

// Statistics derivation
impl DefaultCostModel {
    fn stats_seq_scan<S: StatsProvider>(&self, scan: &SeqScanOp, provider: &S) -> DerivedStats {
        let (rows, size) = provider
            .get_stats(scan.table_id)
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        let selectivity = scan.predicate.as_ref().map_or(1.0, |_| DEFAULT_SELECTIVITY);
        DerivedStats::new(rows * selectivity, size)
    }

    fn stats_index_scan<S: StatsProvider>(
        &self,
        scan: &PhysIndexScanOp,
        provider: &S,
    ) -> DerivedStats {
        let (rows, size) = provider
            .get_stats(scan.table_id)
            .map(|s| (s.row_count as f64, s.avg_row_size))
            .unwrap_or((DEFAULT_ROW_COUNT, DEFAULT_AVG_ROW_SIZE));

        let ndv = provider
            .get_column_stats(scan.index_id, 0)
            .map(|c| c.ndv)
            .unwrap_or(DEFAULT_NDV);

        let selectivity = if ndv > 0 {
            1.0 / ndv as f64
        } else {
            DEFAULT_SELECTIVITY
        };
        DerivedStats::new((rows * selectivity).max(1.0), size)
    }

    fn stats_filter(&self, children: &[DerivedStats]) -> DerivedStats {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        DerivedStats::new(input.row_count * DEFAULT_SELECTIVITY, input.avg_row_size)
    }

    fn stats_project(&self, proj: &PhysProjectOp, children: &[DerivedStats]) -> DerivedStats {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let in_cols = proj.input_schema.num_columns().max(1) as f64;
        let out_cols = proj.expressions.len() as f64;
        DerivedStats::new(
            input.row_count,
            input.avg_row_size * (out_cols / in_cols).min(1.0),
        )
    }

    fn stats_passthrough(&self, children: &[DerivedStats]) -> DerivedStats {
        children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown)
    }

    fn stats_join(&self, children: &[DerivedStats]) -> DerivedStats {
        let left = children
            .get(0)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let right = children
            .get(1)
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        DerivedStats::new(
            (left.row_count * right.row_count * DEFAULT_SELECTIVITY).max(1.0),
            left.avg_row_size + right.avg_row_size,
        )
    }

    fn stats_hash_aggregate(
        &self,
        agg: &HashAggregateOp,
        children: &[DerivedStats],
    ) -> DerivedStats {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        let num_groups = if agg.group_by.is_empty() {
            1.0
        } else {
            (input.row_count * 0.1).max(1.0)
        };
        let output_size = (agg.group_by.len() + agg.aggregates.len()) as f64 * 8.0;
        DerivedStats::new(num_groups, output_size.max(16.0))
    }

    fn stats_limit(&self, limit: &PhysLimitOp, children: &[DerivedStats]) -> DerivedStats {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        DerivedStats::new(
            (limit.limit as f64).min(input.row_count),
            input.avg_row_size,
        )
    }

    fn stats_distinct(&self, children: &[DerivedStats]) -> DerivedStats {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        DerivedStats::new(input.row_count * 0.9, input.avg_row_size)
    }

    fn stats_values(&self, values: &PhysValuesOp) -> DerivedStats {
        let rows = values.rows.len() as f64;
        let cols = values.rows.first().map(|r| r.len()).unwrap_or(1);
        DerivedStats::new(rows, cols as f64 * 8.0)
    }

    fn cost_sort(&self, children: &[DerivedStats]) -> f64 {
        let input = children
            .first()
            .cloned()
            .unwrap_or_else(DerivedStats::unknown);
        if input.row_count <= 1.0 {
            return 0.0;
        }

        let cpu_cost = input.row_count * input.row_count.log2() * self.cpu_operator_cost;
        let io_cost = if self.exceeds_memory(input.row_count, input.avg_row_size) {
            self.external_sort_io_cost(input.row_count, input.avg_row_size)
        } else {
            0.0
        };

        cpu_cost + io_cost
    }
}
