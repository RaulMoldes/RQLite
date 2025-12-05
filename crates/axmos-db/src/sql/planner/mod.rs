//! # Cascades Query Optimizer
//!
//! This module implements a Cascades-style query optimizer that transforms bound SQL statements into physical execution plans.
//!
//! ## References
//!
//! - Microsoft Research: "Extensible Query Optimizers in Practice" (https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf)
//!
//! - Goetz Graefe: "The Cascades Framework for Query Optimization" (https://15721.courses.cs.cmu.edu/spring2016/papers/graefe-ieee1995.pdf)
pub(crate) mod exporter;
pub(crate) mod logical;
pub(crate) mod memo;
pub(crate) mod model;
pub(crate) mod physical;
pub(crate) mod plan;
pub(crate) mod prop;
pub(crate) mod rules;
pub(crate) mod stats;

use std::{
    collections::HashSet,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use crate::{database::SharedCatalog, sql::binder::ast::*, transactions::worker::Worker};

pub(crate) use memo::{ExprId, GroupId, Memo};
pub(crate) use model::CostModel;
pub(crate) use physical::PhysicalPlan;
pub(crate) use plan::PlanBuilder;
pub(crate) use prop::RequiredProperties;
pub(crate) use rules::{ImplementationRule, TransformationRule};
pub(crate) use stats::{Statistics, StatisticsProvider};

/// Result type for optimizer operations
pub(crate) type OptimizerResult<T> = Result<T, OptimizerError>;

/// Optimizer errors
#[derive(Debug, Clone)]
pub(crate) enum OptimizerError {
    /// No valid plan found
    NoPlanFound(String),
    /// Internal error during optimization
    Internal(String),
    /// Unsupported operation
    Unsupported(String),
    /// Invalid state
    InvalidState(String),
    /// Cost overflow
    CostOverflow,
    /// Timeout during optimization
    Timeout,
}

impl Display for OptimizerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NoPlanFound(msg) => write!(f, "No plan found: {}", msg),
            Self::Internal(msg) => write!(f, "Internal error: {}", msg),
            Self::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
            Self::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            Self::CostOverflow => write!(f, "Cost overflow"),
            Self::Timeout => write!(f, "Optimization timeout"),
        }
    }
}

impl Error for OptimizerError {}

/// Configuration for the optimizer
#[derive(Debug, Clone)]
pub(crate) struct OptimizerConfig {
    /// Maximum time for optimization in milliseconds
    pub(crate) max_optimization_time_ms: u64,
    /// Maximum number of transformation rule applications
    pub(crate) max_transformations: usize,
    /// Enable cost-based pruning
    pub(crate) enable_pruning: bool,
    /// Cost threshold for pruning
    pub(crate) cost_threshold: f64,
    /// Enable join reordering
    pub(crate) enable_join_reorder: bool,
    /// Maximum tables for exhaustive join enumeration
    pub(crate) max_join_enumeration_tables: usize,
    /// Enable predicate pushdown
    pub(crate) enable_predicate_pushdown: bool,
    /// Enable projection pushdown
    pub(crate) enable_projection_pushdown: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_optimization_time_ms: 5000,
            max_transformations: 10000,
            enable_pruning: true,
            cost_threshold: f64::MAX,
            enable_join_reorder: true,
            max_join_enumeration_tables: 10,
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
        }
    }
}

/// Main Cascades optimizer
pub(crate) struct CascadesOptimizer<Cost: CostModel, Prov: StatisticsProvider> {
    /// Memo structure for storing expressions
    memo: Memo,
    /// Catalog reference for metadata
    catalog: SharedCatalog,
    /// Worker for catalog access
    worker: Worker,
    /// Cost model
    cost_model: Arc<Cost>,
    /// Statistics provider
    stats_provider: Arc<Prov>,
    /// Transformation rules
    transformation_rules: Vec<Arc<dyn TransformationRule>>,
    /// Implementation rules
    implementation_rules: Vec<Arc<dyn ImplementationRule>>,
    /// Configuration
    config: OptimizerConfig,
    /// Optimization statistics
    stats: OptimizationStats,
}

/// Statistics about the optimization process
#[derive(Debug, Default, Clone)]
pub(crate) struct OptimizationStats {
    pub(crate) groups_created: usize,
    pub(crate) expressions_added: usize,
    pub(crate) transformations_applied: usize,
    pub(crate) implementations_generated: usize,
    pub(crate) pruned_expressions: usize,
    pub(crate) optimization_time_ms: u64,
}

impl<C, S> CascadesOptimizer<C, S>
where
    C: CostModel,
    S: StatisticsProvider,
{
    /// Create a new optimizer
    pub(crate) fn new(
        catalog: SharedCatalog,
        worker: Worker,
        config: OptimizerConfig,
        cost_model: C,
        stats: S,
    ) -> Self {
        Self {
            memo: Memo::new(),
            catalog,
            worker,
            cost_model: Arc::new(cost_model),
            stats_provider: Arc::new(stats),
            transformation_rules: rules::default_transformation_rules(),
            implementation_rules: rules::default_implementation_rules(),
            config,
            stats: OptimizationStats::default(),
        }
    }

    /// Set a custom cost model
    pub(crate) fn with_cost_model(mut self, cost_model: Arc<C>) -> Self {
        self.cost_model = cost_model;
        self
    }

    /// Set a custom statistics provider
    pub(crate) fn with_stats_provider(mut self, provider: Arc<S>) -> Self {
        self.stats_provider = provider;
        self
    }

    /// Add custom transformation rules
    pub(crate) fn with_transformation_rules(
        mut self,
        rules: Vec<Arc<dyn TransformationRule>>,
    ) -> Self {
        self.transformation_rules.extend(rules);
        self
    }

    /// Add custom implementation rules
    pub(crate) fn with_implementation_rules(
        mut self,
        rules: Vec<Arc<dyn ImplementationRule>>,
    ) -> Self {
        self.implementation_rules.extend(rules);
        self
    }

    /// Optimize a bound statement and produce a physical plan
    pub(crate) fn optimize(&mut self, stmt: &BoundStatement) -> OptimizerResult<PhysicalPlan> {
        let start_time = std::time::Instant::now();

        // Reset stats
        self.stats = OptimizationStats::default();
        self.memo = Memo::new();

        // Phase 1: Build initial logical plan and insert into memo
        let root_group = self.build_logical_plan(stmt)?;

        // Phase 2: Exploration (Apply transformation rules)
        self.explore(root_group)?;

        // Phase 3: Implementation - generate physical alternatives
        let required_props = RequiredProperties::default();
        self.implement(root_group, &required_props)?;

        // Phase 4: Extract best plan
        let plan = self.extract_best_plan(root_group, &required_props)?;

        self.stats.optimization_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(plan)
    }

    /// Build the initial logical plan from a bound statement
    fn build_logical_plan(&mut self, stmt: &BoundStatement) -> OptimizerResult<GroupId> {
        let builder = PlanBuilder::new(&mut self.memo, self.catalog.clone(), self.worker.clone());
        builder.build(stmt)
    }

    /// Exploration phase: apply transformation rules to find equivalent plans
    fn explore(&mut self, root_group: GroupId) -> OptimizerResult<()> {
        let mut explored = HashSet::new();
        let mut worklist = vec![root_group];

        while let Some(group_id) = worklist.pop() {
            if explored.contains(&group_id) {
                continue;
            }
            explored.insert(group_id);

            if self.stats.transformations_applied >= self.config.max_transformations {
                break;
            }

            // Get expressions in this group
            let expr_ids: Vec<ExprId> = {
                let group = self
                    .memo
                    .get_group(group_id)
                    .ok_or_else(|| OptimizerError::InvalidState("Group not found".into()))?;
                group.logical_exprs.iter().map(|e| e.id).collect()
            };

            // Try each transformation rule on each expression
            for expr_id in expr_ids {
                for rule in &self.transformation_rules.clone() {
                    if let Some(expr) = self.memo.get_logical_expr(expr_id) {
                        if rule.matches(&expr, &self.memo) {
                            let new_exprs = rule.apply(&expr, &mut self.memo)?;
                            for new_expr in new_exprs {
                                self.memo.add_logical_expr_to_group(group_id, new_expr);
                                self.stats.transformations_applied += 1;
                            }
                        }
                    }
                }
            }

            // Recursively explore children
            let child_groups: Vec<GroupId> = {
                let group = self.memo.get_group(group_id).unwrap();
                group
                    .logical_exprs
                    .iter()
                    .flat_map(|e| e.children.clone())
                    .collect()
            };

            for child in child_groups {
                if !explored.contains(&child) {
                    worklist.push(child);
                }
            }
        }

        Ok(())
    }

    /// Implementation phase: generate physical operators
    fn implement(
        &mut self,
        group_id: GroupId,
        required_props: &RequiredProperties,
    ) -> OptimizerResult<Option<f64>> {
        // Check if already computed
        if let Some(group) = self.memo.get_group(group_id) {
            if let Some(&(cost, _)) = group.best_physical.get(required_props) {
                return Ok(Some(cost));
            }
        }

        let mut best_cost = f64::MAX;
        let mut best_expr_id: Option<ExprId> = None;

        // Get logical expressions
        let logical_exprs: Vec<_> = {
            let group = self
                .memo
                .get_group(group_id)
                .ok_or_else(|| OptimizerError::InvalidState("Group not found".into()))?;
            group.logical_exprs.clone()
        };

        // Try each implementation rule on each logical expression
        for logical_expr in &logical_exprs {
            for rule in &self.implementation_rules.clone() {
                if rule.matches(logical_expr, &self.memo) {
                    let physical_exprs =
                        rule.implement(logical_expr, required_props, &self.memo)?;

                    for mut phys_expr in physical_exprs {
                        // Recursively implement children and compute cost
                        let mut total_cost = self
                            .cost_model
                            .compute_operator_cost(&phys_expr.op, &self.get_group_stats(group_id));

                        let mut valid = true;
                        for (i, &child_group) in phys_expr.children.iter().enumerate() {
                            let child_required =
                                phys_expr.op.required_child_properties(i, required_props);
                            match self.implement(child_group, &child_required)? {
                                Some(child_cost) => total_cost += child_cost,
                                None => {
                                    valid = false;
                                    break;
                                }
                            }
                        }

                        if valid && total_cost < best_cost {
                            // Apply cost-based pruning
                            if self.config.enable_pruning && total_cost > self.config.cost_threshold
                            {
                                self.stats.pruned_expressions += 1;
                                continue;
                            }

                            best_cost = total_cost;
                            phys_expr.cost = total_cost;
                            let expr_id = self.memo.add_physical_expr_to_group(group_id, phys_expr);
                            best_expr_id = Some(expr_id);
                            self.stats.implementations_generated += 1;
                        }
                    }
                }
            }
        }

        // Record best physical expression for this group and properties
        if let Some(expr_id) = best_expr_id {
            if let Some(group) = self.memo.get_group_mut(group_id) {
                group
                    .best_physical
                    .insert(required_props.clone(), (best_cost, expr_id));
            }
            Ok(Some(best_cost))
        } else {
            Ok(None)
        }
    }

    /// Extract the best physical plan from the memo
    fn extract_best_plan(
        &self,
        group_id: GroupId,
        required_props: &RequiredProperties,
    ) -> OptimizerResult<PhysicalPlan> {
        let group = self
            .memo
            .get_group(group_id)
            .ok_or_else(|| OptimizerError::NoPlanFound("Root group not found".into()))?;

        let (_, expr_id) = group.best_physical.get(required_props).ok_or_else(|| {
            OptimizerError::NoPlanFound(format!(
                "No physical plan for group {:?} with props {:?}",
                group_id, required_props
            ))
        })?;

        let phys_expr = self
            .memo
            .get_physical_expr(*expr_id)
            .ok_or_else(|| OptimizerError::InvalidState("Physical expr not found".into()))?;

        // Recursively extract children
        let mut children = Vec::new();
        for (i, &child_group) in phys_expr.children.iter().enumerate() {
            let child_required = phys_expr.op.required_child_properties(i, required_props);
            let child_plan = self.extract_best_plan(child_group, &child_required)?;
            children.push(child_plan);
        }

        Ok(PhysicalPlan {
            op: phys_expr.op.clone(),
            children,
            cost: phys_expr.cost,
            properties: phys_expr.properties.clone(),
        })
    }

    /// Get statistics for a group (placeholder for now)
    fn get_group_stats(&self, group_id: GroupId) -> Statistics {
        if let Some(group) = self.memo.get_group(group_id) {
            group.stats.clone()
        } else {
            Statistics::default()
        }
    }

    /// Get optimization statistics
    pub(crate) fn get_stats(&self) -> &OptimizationStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_config_default() {
        let config = OptimizerConfig::default();
        assert!(config.enable_pruning);
        assert!(config.enable_predicate_pushdown);
    }
}
