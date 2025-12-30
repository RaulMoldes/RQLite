//! Cascades-style query optimizer.
//!
//! This module implements a cost-based query optimizer inspired by the
//! Columbia/Cascades framework used in SQL Server and other databases.
//!
//! References:
//! - https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf

pub(crate) mod exporter;
pub(crate) mod logical;
pub(crate) mod memo;
pub(crate) mod model;
pub(crate) mod physical;
pub(crate) mod plan;
pub(crate) mod prop;
pub(crate) mod rules;

use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::{
    io::pager::BtreeBuilder,
    multithreading::coordinator::Snapshot,
    schema::catalog::{CatalogTrait, StatisticsProvider},
    sql::{binder::bounds::*, planner::prop::PropertyDeriver},
};

pub(crate) use memo::{ExprId, GroupId, Memo};
pub(crate) use model::{CostModel, DefaultCostModel, DerivedStats};
pub(crate) use physical::{PhysicalExpr, PhysicalPlan};
pub(crate) use prop::RequiredProperties;
pub(crate) use rules::{ImplementationRule, TransformationRule};

use plan::Planner;
use std::cell::Cell;
/// Planner errors.
#[derive(Debug)]
pub enum PlannerError {
    /// No valid plan found during optimization.
    NoPlanFound(String),
    /// Internal state error.
    InvalidState,
    /// DDL/transaction statements are not supported by the planner.
    UnsupportedStatement,
    /// Referenced item not found.
    UnboundedItem(usize),
    /// Cost calculation overflow.
    CostOverflow,
    /// Optimization timed out.
    Timeout,
    /// Other errors.
    Other(String),
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NoPlanFound(msg) => write!(f, "no plan found: {}", msg),
            Self::UnsupportedStatement => {
                write!(f, "DDL and transaction statements are not supported")
            }
            Self::InvalidState => write!(f, "invalid planner state"),
            Self::CostOverflow => write!(f, "cost overflow"),
            Self::Timeout => write!(f, "optimization timeout"),
            Self::UnboundedItem(id) => write!(f, "item with id {} not found", id),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PlannerError {}

pub type PlannerResult<T> = Result<T, PlannerError>;

/// Configuration for the optimizer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OptimizerConfig {
    pub(crate) max_optimization_time_ms: u64,
    pub(crate) max_transformations: usize,
    pub(crate) enable_pruning: bool,
    pub(crate) upper_bound: f64,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_optimization_time_ms: 5000,
            max_transformations: 10000,
            enable_pruning: true,
            upper_bound: f64::MAX,
        }
    }
}

/// Statistics collected during optimization.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OptimizationStats {
    pub(crate) optimization_time_ms: u64,
    pub(crate) groups_created: u16,
    pub(crate) logical_exprs: u16,
    pub(crate) physical_exprs: u16,
    pub(crate) transformations_applied: u16,
}

/// Cascades-style query optimizer.
pub struct CascadesOptimizer<C: CatalogTrait + Clone> {
    cost_model: DefaultCostModel,
    deriver: PropertyDeriver<StatisticsProvider<C>>,
    transformation_rules: Vec<Box<dyn TransformationRule>>,
    implementation_rules: Vec<Box<dyn ImplementationRule>>,
    config: OptimizerConfig,
    transformations_applied: Cell<u16>,
    stats: Option<OptimizationStats>,
}

impl<C: CatalogTrait + Clone> CascadesOptimizer<C> {
    /// Creates a new optimizer with the given catalog context.
    pub(crate) fn new(
        page_size: u32,
        catalog: C,
        builder: BtreeBuilder,
        snapshot: Snapshot,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            cost_model: DefaultCostModel::new(page_size),
            deriver: PropertyDeriver::new(StatisticsProvider::new(catalog, builder, snapshot)),
            transformation_rules: rules::transformation_rules(),
            implementation_rules: rules::implementation_rules(),
            config,
            transformations_applied: Cell::new(0),
            stats: None,
        }
    }

    /// Creates an optimizer with default configuration.
    pub fn with_defaults(catalog: C, builder: BtreeBuilder, snapshot: Snapshot) -> Self {
        Self::new(
            crate::DEFAULT_PAGE_SIZE as u32,
            catalog,
            builder,
            snapshot,
            OptimizerConfig::default(),
        )
    }

    /// Adds custom transformation rules.
    pub(crate) fn with_transformation_rules(
        mut self,
        rules: Vec<Box<dyn TransformationRule>>,
    ) -> Self {
        self.transformation_rules.extend(rules);
        self
    }

    /// Adds custom implementation rules.
    pub(crate) fn with_implementation_rules(
        mut self,
        rules: Vec<Box<dyn ImplementationRule>>,
    ) -> Self {
        self.implementation_rules.extend(rules);
        self
    }

    /// Resets the stats before optimization
    fn take_stats(&mut self) -> Option<OptimizationStats> {
        self.stats.take()
    }

    /// Main optimization entry point.
    ///
    /// Takes a bound statement and returns an optimized physical plan.
    pub fn optimize(&mut self, stmt: &BoundStatement) -> PlannerResult<PhysicalPlan> {
        let start = std::time::Instant::now();

        // Reset state
        let mut memo = Memo::new();
        let _ = self.take_stats();

        let mut stats = OptimizationStats::default();

        // Phase 1: Build initial logical plan
        let root_group = {
            let mut planner = Planner::new(&mut memo, &self.deriver);
            planner.build(stmt)?
        };

        // Phase 2-3: Explore and implement
        let required = RequiredProperties::default();
        self.optimize_group(&mut memo, root_group, &required, self.config.upper_bound)?;

        // Phase 4: Extract best plan
        let plan = self.extract_plan(&mut memo, root_group, &required)?;

        // Record stats
        stats.optimization_time_ms = start.elapsed().as_millis() as u64;
        stats.groups_created = memo.num_groups() as u16;

        self.stats = Some(stats);

        Ok(plan)
    }

    /// Optimizes a single group for the given required properties.
    fn optimize_group(
        &self,
        memo: &mut Memo,
        group_id: GroupId,
        required: &RequiredProperties,
        upper_bound: f64,
    ) -> PlannerResult<Option<f64>> {
        // Check if we already have a winner
        if let Some(winner) = memo
            .get_group(group_id)
            .and_then(|g| g.get_winner(required))
        {
            return Ok(Some(winner.cost));
        }

        // Check if we've already explored this group with these requirements
        if memo
            .get_group(group_id)
            .map(|g| g.has_explored(required))
            .unwrap_or(false)
        {
            return Ok(None);
        }

        // Explore logical equivalences
        self.explore_group(memo, group_id)?;

        // Implement physical operators
        self.implement_group(memo, group_id, required, upper_bound)
    }

    /// Explores a group by applying transformation rules.
    fn explore_group(&self, memo: &mut Memo, group_id: GroupId) -> PlannerResult<()> {
        // Skip if already explored
        if memo.get_group(group_id).map(|g| g.explored).unwrap_or(true) {
            return Ok(());
        }

        let expr_count = memo
            .get_group(group_id)
            .map(|g| g.logical_exprs.len())
            .unwrap_or(0);

        // Explore each logical expression
        for expr_idx in 0..expr_count {
            let expr_id = ExprId::logical(group_id, expr_idx);
            self.explore_expr(memo, expr_id)?;
        }

        // Mark as explored
        if let Some(group) = memo.get_group_mut(group_id) {
            group.explored = true;
        }

        Ok(())
    }

    /// Explores a single expression by applying transformation rules.
    fn explore_expr(&self, memo: &mut Memo, expr_id: ExprId) -> PlannerResult<()> {
        let expr = match memo.get_logical_expr(expr_id) {
            Some(e) => e.clone(),
            None => return Ok(()),
        };

        // First explore children
        for &child_group in &expr.children {
            self.explore_group(memo, child_group)?;
        }

        // Apply transformation rules
        for rule in self.transformation_rules.iter() {
            if rule.matches(&expr, &memo) {
                let new_exprs = rule.apply(&expr, memo)?;

                for new_expr in new_exprs {
                    if memo
                        .add_logical_expr_to_group(expr_id.group_id, new_expr)
                        .is_some()
                    {
                        let transformations_applied = self.transformations_applied.get() + 1;
                        self.transformations_applied.set(transformations_applied);
                    }
                }
            }
        }

        Ok(())
    }

    /// Implements a group by generating physical operators.
    fn implement_group(
        &self,
        memo: &mut Memo,
        group_id: GroupId,
        required: &RequiredProperties,
        mut upper_bound: f64,
    ) -> PlannerResult<Option<f64>> {
        let expr_count = memo
            .get_group(group_id)
            .map(|g| g.logical_exprs.len())
            .unwrap_or(0);

        let mut best_cost: Option<f64> = None;
        let mut best_expr_id: Option<ExprId> = None;

        // Try each logical expression
        for expr_idx in 0..expr_count {
            let logical_id = ExprId::logical(group_id, expr_idx);

            let logical_expr = match memo.get_logical_expr(logical_id) {
                Some(e) => e.clone(),
                None => continue,
            };

            for rule in self.implementation_rules.iter() {
                let physical_exprs = rule.implement(&logical_expr, required, &memo)?;

                for phys_expr in physical_exprs {
                    let cost =
                        self.compute_expr_cost(memo, group_id, &phys_expr, required, upper_bound)?;

                    if let Some(c) = cost {
                        if best_cost.map(|bc| c < bc).unwrap_or(true) {
                            let phys_id = memo.add_physical_expr_to_group(group_id, phys_expr);
                            best_cost = Some(c);
                            best_expr_id = Some(phys_id);

                            if self.config.enable_pruning {
                                upper_bound = c;
                            }
                        }
                    }
                }
            }
        }

        // Record winner
        let winner = best_expr_id.map(|id| memo::Winner {
            expr_id: id,
            cost: best_cost.unwrap(),
        });

        if let Some(group) = memo.get_group_mut(group_id) {
            group.set_winner(required.clone(), winner);
        }

        Ok(best_cost)
    }

    /// Computes the cost of a physical expression.
    fn compute_expr_cost(
        &self,
        memo: &mut Memo,
        group_id: GroupId,
        phys_expr: &PhysicalExpr,
        parent_required: &RequiredProperties,
        upper_bound: f64,
    ) -> PlannerResult<Option<f64>> {
        let mut children_stats = Vec::with_capacity(phys_expr.children.len());
        let mut children_cost = 0.0;

        // Recursively optimize children
        for (i, &child_group) in phys_expr.children.iter().enumerate() {
            let child_required = phys_expr.op.required_child_properties(i, parent_required);

            let remaining = upper_bound - children_cost;
            if remaining <= 0.0 {
                return Ok(None); // Pruned
            }

            match self.optimize_group(memo, child_group, &child_required, remaining)? {
                Some(cost) => {
                    children_cost += cost;
                    let child_stats = self.derive_group_stats(memo, child_group);
                    children_stats.push(child_stats);
                }
                None => return Ok(None), // No valid child plan
            }
        }

        // Compute this operator's cost
        let op_cost =
            self.cost_model
                .compute_cost(&phys_expr.op, self.deriver.provider(), &children_stats);

        let total = children_cost + op_cost;

        if total >= upper_bound {
            return Ok(None); // Pruned
        }

        Ok(Some(total))
    }

    /// Derives statistics for a group.
    fn derive_group_stats(&self, memo: &mut Memo, group_id: GroupId) -> DerivedStats {
        let group = match memo.get_group(group_id) {
            Some(g) => g,
            None => return DerivedStats::unknown(),
        };

        DerivedStats::new(
            group.logical_props.cardinality,
            group.logical_props.avg_row_size,
        )
    }

    /// Extracts the final physical plan from the memo.
    fn extract_plan(
        &self,
        memo: &mut Memo,
        group_id: GroupId,
        required: &RequiredProperties,
    ) -> PlannerResult<PhysicalPlan> {
        let group = memo.get_group(group_id).ok_or(PlannerError::InvalidState)?;

        let winner = group.get_winner(required).ok_or_else(|| {
            PlannerError::NoPlanFound(format!(
                "No winner for group {:?} with {:?}",
                group_id, required
            ))
        })?;

        let phys_expr = memo
            .get_physical_expr(winner.expr_id)
            .ok_or(PlannerError::InvalidState)?
            .clone();
        let winner_cost = winner.cost;
        // Recursively extract child plans
        let mut children = Vec::with_capacity(phys_expr.children.len());
        for (i, &child_group) in phys_expr.children.iter().enumerate() {
            let child_required = phys_expr.op.required_child_properties(i, required);
            children.push(self.extract_plan(memo, child_group, &child_required)?);
        }

        Ok(PhysicalPlan {
            op: phys_expr.op.clone(),
            children,
            cost: winner_cost,
            properties: phys_expr.properties.clone(),
        })
    }
}
