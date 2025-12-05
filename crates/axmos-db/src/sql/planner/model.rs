//! Cost model for the optimizer.
//!
//! The cost model estimates the cost of physical operators to help the optimizer
//! choose the best execution plan. Cost is typically measured in abstract units
//! that represent CPU, I/O, and memory usage.

use crate::types::ObjectId;

use super::physical::*;
use super::stats::{Statistics, StatisticsProvider};

/// Cost model trait
pub trait CostModel: Send + Sync {
    /// Compute the cost of a physical operator
    fn compute_operator_cost(&self, op: &PhysicalOperator, stats: &Statistics) -> f64;

    /// Compute the total cost including children
    fn compute_total_cost(&self, plan: &PhysicalPlan) -> f64 {
        let self_cost = self.compute_operator_cost(&plan.op, &Statistics::default());
        let children_cost: f64 = plan
            .children
            .iter()
            .map(|c| self.compute_total_cost(c))
            .sum();
        self_cost + children_cost
    }
}

/// Default cost model implementation
pub struct DefaultCostModel {
    pub page_size: u32,
    /// Cost per page I/O
    pub page_io_cost: f64,
    /// Cost per CPU tuple processing
    pub cpu_tuple_cost: f64,
    /// Cost per CPU operator evaluation
    pub cpu_operator_cost: f64,
    /// Random I/O cost multiplier (vs sequential)
    pub random_io_multiplier: f64,
    /// Memory threshold for spilling to disk
    pub memory_threshold: usize,
}

impl DefaultCostModel {
    pub fn new() -> Self {
        Self {
            page_size: 4096,
            page_io_cost: 1.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            random_io_multiplier: 1.2,
            memory_threshold: 256 * 1024 * 1024, // 256MB
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

    /// Estimate pages from row count and row size
    fn estimate_pages(&self, rows: f64, avg_row_size: f64) -> f64 {
        let page_size = self.page_size as f64; // Default page size
        let rows_per_page = (page_size / avg_row_size).max(1.0);
        (rows / rows_per_page).ceil()
    }

    /// Sequential scan cost
    fn seq_scan_cost(&self, stats: &Statistics) -> f64 {
        let pages = self.estimate_pages(stats.row_count, stats.avg_row_size);
        let io_cost = pages * self.page_io_cost;
        let cpu_cost = stats.row_count * self.cpu_tuple_cost;
        io_cost + cpu_cost
    }

    /// Index scan cost
    fn index_scan_cost(&self, stats: &Statistics, selectivity: f64) -> f64 {
        let rows = stats.row_count * selectivity;

        // Index traversal cost (log-based)
        let index_pages = (stats.row_count.log2() + 1.0).ceil();
        let index_io = index_pages * self.page_io_cost * self.random_io_multiplier;

        // Heap access cost (random I/O for each row)
        let heap_io = rows * self.page_io_cost * self.random_io_multiplier * 0.5; // Assume some clustering
        //let heap_io = 0.0;
        println!("INDEX IO: {index_io}");

        // CPU cost
        let cpu_cost = rows * self.cpu_tuple_cost;

        index_io + heap_io + cpu_cost
    }

    /// Filter cost
    fn filter_cost(&self, stats: &Statistics) -> f64 {
        stats.row_count * self.cpu_operator_cost
    }

    /// Project cost
    fn project_cost(&self, stats: &Statistics, num_exprs: usize) -> f64 {
        stats.row_count * self.cpu_operator_cost * num_exprs as f64
    }

    /// Nested loop join cost
    fn nested_loop_cost(&self, left_rows: f64, right_rows: f64) -> f64 {
        let io_cost = self.page_io_cost * 2.0; // Both inputs
        let cpu_cost = left_rows * right_rows * self.cpu_tuple_cost;
        io_cost + cpu_cost
    }

    /// Hash join cost
    fn hash_join_cost(&self, build_rows: f64, probe_rows: f64) -> f64 {
        // Build phase: hash all rows from build side
        let build_cost = build_rows * self.cpu_tuple_cost * 1.5; // Extra cost for hashing

        // Probe phase: probe for each row from probe side
        let probe_cost = probe_rows * self.cpu_tuple_cost * 1.2; // Probe is slightly cheaper

        // Memory consideration
        let spill_cost = if build_rows * 100.0 > self.memory_threshold as f64 {
            build_rows * self.page_io_cost * 0.1 // Spill overhead
        } else {
            0.0
        };

        build_cost + probe_cost + spill_cost
    }

    /// Merge join cost (assumes sorted inputs)
    fn merge_join_cost(&self, left_rows: f64, right_rows: f64) -> f64 {
        // Merge join is linear in both inputs
        (left_rows + right_rows) * self.cpu_tuple_cost
    }

    /// Sort cost
    fn sort_cost(&self, stats: &Statistics) -> f64 {
        let n = stats.row_count;
        if n <= 1.0 {
            return 0.0;
        }

        // O(n log n) complexity
        let cpu_cost = n * n.log2() * self.cpu_operator_cost;

        // External sort I/O if exceeds memory
        let io_cost = if stats.row_count * stats.avg_row_size > self.memory_threshold as f64 {
            let pages = self.estimate_pages(n, stats.avg_row_size);
            // Multiple passes for external sort
            let passes = (pages / (self.memory_threshold as f64 / 4096.0))
                .log2()
                .ceil()
                .max(1.0);
            pages * passes * 2.0 * self.page_io_cost
        } else {
            0.0
        };

        cpu_cost + io_cost
    }

    /// Hash aggregate cost
    fn hash_aggregate_cost(&self, stats: &Statistics, num_groups: f64) -> f64 {
        let build_cost = stats.row_count * self.cpu_tuple_cost * 1.5;
        let output_cost = num_groups * self.cpu_tuple_cost;
        build_cost + output_cost
    }

    /// Sort aggregate cost
    fn sort_aggregate_cost(&self, stats: &Statistics, num_groups: f64) -> f64 {
        // Already sorted, just scan and aggregate
        let scan_cost = stats.row_count * self.cpu_tuple_cost;
        let output_cost = num_groups * self.cpu_tuple_cost;
        scan_cost + output_cost
    }
}

impl Default for DefaultCostModel {
    fn default() -> Self {
        Self::new()
    }
}

impl CostModel for DefaultCostModel {
    fn compute_operator_cost(&self, op: &PhysicalOperator, stats: &Statistics) -> f64 {
        match op {
            PhysicalOperator::SeqScan(_) => self.seq_scan_cost(stats),

            PhysicalOperator::IndexScan(_) => {
                // Assume default selectivity of 10%
                self.index_scan_cost(stats, 0.1)
            }

            PhysicalOperator::IndexOnlyScan(_) => {
                // Index-only scan is cheaper (no heap access)
                self.index_scan_cost(stats, 0.1) * 0.5
            }

            PhysicalOperator::Filter(_) => self.filter_cost(stats),

            PhysicalOperator::Project(proj) => self.project_cost(stats, proj.expressions.len()),

            PhysicalOperator::NestedLoopJoin(_) => {
                // Estimate based on current stats (outer relation)
                // In reality, we'd need stats from both sides
                self.nested_loop_cost(stats.row_count, stats.row_count)
            }

            PhysicalOperator::HashJoin(_) => {
                self.hash_join_cost(stats.row_count * 0.5, stats.row_count * 0.5)
            }

            PhysicalOperator::MergeJoin(_) => {
                self.merge_join_cost(stats.row_count * 0.5, stats.row_count * 0.5)
            }

            PhysicalOperator::HashAggregate(_) => {
                let num_groups = (stats.row_count / 10.0).max(1.0);
                self.hash_aggregate_cost(stats, num_groups)
            }

            PhysicalOperator::SortAggregate(_) => {
                let num_groups = (stats.row_count / 10.0).max(1.0);
                self.sort_aggregate_cost(stats, num_groups)
            }

            PhysicalOperator::ExternalSort(_) | PhysicalOperator::Sort(_) => self.sort_cost(stats),

            PhysicalOperator::TopN(op) => {
                // TopN is cheaper than full sort
                let limit = op.limit as f64;
                stats.row_count * self.cpu_tuple_cost
                    + limit * limit.log2() * self.cpu_operator_cost
            }

            PhysicalOperator::Limit(_) => {
                // Limit is essentially free
                self.cpu_tuple_cost
            }

            PhysicalOperator::HashDistinct(_) => {
                // Similar to hash aggregate
                self.hash_aggregate_cost(stats, stats.row_count * 0.9)
            }

            PhysicalOperator::SortDistinct(_) => {
                // Scan already-sorted data
                stats.row_count * self.cpu_tuple_cost
            }

            PhysicalOperator::HashUnion(_)
            | PhysicalOperator::HashIntersect(_)
            | PhysicalOperator::HashExcept(_) => {
                // Build hash table from one side, probe with other
                self.hash_join_cost(stats.row_count * 0.5, stats.row_count * 0.5)
            }

            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => {
                // DML cost: I/O per row + index updates
                stats.row_count * (self.page_io_cost + self.cpu_tuple_cost * 2.0)
            }

            PhysicalOperator::Values(v) => {
                // Just produce the values
                v.rows.len() as f64 * self.cpu_tuple_cost
            }

            PhysicalOperator::Empty(_) => 0.0,

            PhysicalOperator::Exchange(_) => {
                // Network cost for distributed execution
                stats.row_count * self.cpu_tuple_cost * 10.0
            }
        }
    }
}

/// Cost-aware statistics provider wrapper
pub struct CostAwareStats<S: StatisticsProvider> {
    provider: S,
    cost_model: DefaultCostModel,
}

impl<S: StatisticsProvider> CostAwareStats<S> {
    pub fn new(provider: S) -> Self {
        Self {
            provider,
            cost_model: DefaultCostModel::new(),
        }
    }

    /// Estimate cost for scanning a table with optional predicate
    pub fn estimate_scan_cost(&self, table_id: ObjectId, selectivity: Option<f64>) -> f64 {
        let stats = self.provider.get_table_stats(table_id).unwrap_or_default();

        let sel = selectivity.unwrap_or(1.0);
        let effective_rows = stats.row_count as f64 * sel;

        let effective_stats = Statistics::new(effective_rows, stats.avg_row_size);
        self.cost_model.seq_scan_cost(&effective_stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cost_model() {
        let cost_model = DefaultCostModel::new();

        let stats = Statistics::new(1000.0, 100.0);

        // Sequential scan cost
        let seq_cost = cost_model.seq_scan_cost(&stats);
        assert!(seq_cost > 0.0);

        // Index scan should be cheaper for selective queries
        let idx_cost = cost_model.index_scan_cost(&stats, 0.01);

        assert!(idx_cost < seq_cost);

        // Sort cost
        let sort_cost = cost_model.sort_cost(&stats);
        assert!(sort_cost > 0.0);
    }

    #[test]
    fn test_join_costs() {
        let cost_model = DefaultCostModel::new();

        let small = 100.0;
        let large = 10000.0;

        // Nested loop is quadratic
        let nl_cost = cost_model.nested_loop_cost(small, large);

        // Hash join is linear
        let hash_cost = cost_model.hash_join_cost(small, large);

        // Hash should be cheaper for large joins
        assert!(hash_cost < nl_cost);

        // Merge join is also linear
        let merge_cost = cost_model.merge_join_cost(small, large);
        assert!(merge_cost < nl_cost);
    }

    #[test]
    fn test_physical_operator_costs() {
        let cost_model = DefaultCostModel::new();
        let stats = Statistics::new(1000.0, 100.0);

        // Test various operators
        let scan = PhysicalOperator::SeqScan(SeqScanOp::new(
            ObjectId::new(),
            "test".to_string(),
            crate::database::schema::Schema::new(),
        ));

        let cost = cost_model.compute_operator_cost(&scan, &stats);
        assert!(cost > 0.0);
    }

    #[test]
    fn test_sort_cost_scaling() {
        let cost_model = DefaultCostModel::new();

        // Small dataset
        let small_stats = Statistics::new(100.0, 100.0);
        let small_cost = cost_model.sort_cost(&small_stats);

        // Large dataset should cost more (O(n log n))
        let large_stats = Statistics::new(10000.0, 100.0);
        let large_cost = cost_model.sort_cost(&large_stats);

        assert!(large_cost > small_cost);

        // Very large dataset may trigger external sort
        let huge_stats = Statistics::new(10000000.0, 100.0);
        let huge_cost = cost_model.sort_cost(&huge_stats);

        assert!(huge_cost > large_cost);
    }
}
