pub(crate) mod exporter;
pub(crate) mod logical;
pub(crate) mod memo;
pub(crate) mod model;
pub(crate) mod physical;
pub(crate) mod plan;
pub(crate) mod prop;
pub(crate) mod rules;
pub(crate) mod stats;

use std::sync::Arc;

use crate::{
    database::{
        SharedCatalog,
        errors::{OptimizerError, OptimizerResult},
    },
    sql::{
        binder::ast::*,
        executor::ddl::{DdlExecutor, DdlOutcome},
    },
    transactions::accessor::RcPageAccessor,
};

pub(crate) use memo::{ExprId, GroupId, Memo};
pub(crate) use model::{CostModel, DerivedStats};
pub(crate) use physical::PhysicalPlan;
pub(crate) use plan::PlanBuilder;
pub(crate) use prop::RequiredProperties;
pub(crate) use rules::{ImplementationRule, TransformationRule};
pub(crate) use stats::StatisticsProvider;

#[derive(Debug, Clone)]
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

/// Result of processing a statement
#[derive(Debug)]
pub(crate) enum ProcessedStatement {
    /// A physical plan ready for execution
    Plan(PhysicalPlan),
    /// DDL was executed directly, here's the result
    DdlExecuted(DdlOutcome),
}

#[derive(Debug, Default, Clone)]
pub(crate) struct OptimizationStats {
    pub(crate) groups_created: usize,
    pub(crate) logical_exprs: usize,
    pub(crate) physical_exprs: usize,
    pub(crate) transformations_applied: usize,
    pub(crate) optimization_time_ms: u64,
}

/// Cascades style query optimizer inspired by SQL Server.
/// https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf
pub(crate) struct CascadesOptimizer<C: CostModel, P: StatisticsProvider> {
    memo: Memo,
    catalog: SharedCatalog,
    accessor: RcPageAccessor,
    cost_model: Arc<C>,
    stats_provider: Arc<P>,
    transformation_rules: Vec<Arc<dyn TransformationRule>>,
    implementation_rules: Vec<Arc<dyn ImplementationRule>>,
    config: OptimizerConfig,
    opt_stats: OptimizationStats,
}

impl<C: CostModel, P: StatisticsProvider> CascadesOptimizer<C, P> {
    pub(crate) fn new(
        catalog: SharedCatalog,
        accessor: RcPageAccessor,
        config: OptimizerConfig,
        cost_model: C,
        stats_provider: P,
    ) -> Self {
        Self {
            memo: Memo::new(),
            catalog,
            accessor,
            cost_model: Arc::new(cost_model),
            stats_provider: Arc::new(stats_provider),
            transformation_rules: rules::default_transformation_rules(),
            implementation_rules: rules::default_implementation_rules(),
            config,
            opt_stats: OptimizationStats::default(),
        }
    }

    pub(crate) fn with_transformation_rules(
        mut self,
        rules: Vec<Arc<dyn TransformationRule>>,
    ) -> Self {
        self.transformation_rules.extend(rules);
        self
    }

    pub(crate) fn with_implementation_rules(
        mut self,
        rules: Vec<Arc<dyn ImplementationRule>>,
    ) -> Self {
        self.implementation_rules.extend(rules);
        self
    }

    /// Process a bound statement.
    /// - DDL statements are executed immediately
    /// - DML/DQL statements are optimized into physical plans
    pub(crate) fn process(&mut self, stmt: &BoundStatement) -> OptimizerResult<ProcessedStatement> {
        // DDL doesn't need optimization (execute directly)
        if DdlExecutor::is_ddl(stmt) {
            let ddl_executor = DdlExecutor::new(self.catalog.clone(), self.accessor.clone());
            let result = ddl_executor.execute(stmt)?;
            return Ok(ProcessedStatement::DdlExecuted(result));
        }

        // DML/DQL goes through the optimizer
        let plan = self.optimize(stmt)?;
        Ok(ProcessedStatement::Plan(plan))
    }

    /// Main optimization function.
    /// Takes in an [`BoundStatement`] produced by the binder and optimizes it
    /// in the following steps:
    ///
    /// Step 1 is plan building. Performed by the [build_initial_plan] function.
    /// This function creates a plan builder that builds a logical plan.
    /// It uses the [memo] table and goes adding groups to it by observing logical expressions in the statement.
    /// See [plan.rs] for details.
    ///
    /// Once the plan is built, we can apply transformation rules to it in order to create an optimized plan.
    /// Transformation rules take in logical operators and produce new logical operators.
    /// An example of this is the [`ProjectPushown`] which literally pushes down projections so that they are computed as soon as possible reducing the total amount of data to be processed.
    ///
    /// The [optimize_group] function, explores each group in the memo table starting from the root. Each group is made up of logical expressions that can be optimized. The [explore_group] call explores logical expressions by means of identifying opportunities to apply transformation rules to each group. Once it finds an opportunity to optimize, applies the transformation rule, which will generate a new group that is inserted again in the meta-table and optimized. Finally, the group is [implemented] by means of applying [ImplementationRules] to it, that is, by identifying the most optimal physical operators for each group.
    ///
    /// Once all groups have been implemented, we are done.
    fn optimize(&mut self, stmt: &BoundStatement) -> OptimizerResult<PhysicalPlan> {
        let start = std::time::Instant::now();

        self.memo = Memo::new();
        self.opt_stats = OptimizationStats::default();

        let root_group = self.build_initial_plan(stmt)?;
        let required = RequiredProperties::default();

        self.optimize_group(root_group, &required, self.config.upper_bound)?;

        let plan = self.extract_plan(root_group, &required)?;

        self.opt_stats.optimization_time_ms = start.elapsed().as_millis() as u64;
        self.opt_stats.groups_created = self.memo.num_groups();

        Ok(plan)
    }

    fn build_initial_plan(&mut self, stmt: &BoundStatement) -> OptimizerResult<GroupId> {
        let builder = PlanBuilder::new(
            &mut self.memo,
            self.catalog.clone(),
            self.accessor.clone(),
            self.stats_provider.as_ref(),
        );
        builder.build(stmt)
    }

    fn optimize_group(
        &mut self,
        group_id: GroupId,
        required: &RequiredProperties,
        upper_bound: f64,
    ) -> OptimizerResult<Option<f64>> {
        if let Some(winner) = self
            .memo
            .get_group(group_id)
            .and_then(|g| g.get_winner(required))
        {
            return Ok(Some(winner.cost));
        }

        if self
            .memo
            .get_group(group_id)
            .map(|g| g.has_explored(required))
            .unwrap_or(false)
        {
            return Ok(None);
        }

        self.explore_group(group_id)?;

        let result = self.implement_group(group_id, required, upper_bound)?;

        Ok(result)
    }

    fn explore_group(&mut self, group_id: GroupId) -> OptimizerResult<()> {
        if self
            .memo
            .get_group(group_id)
            .map(|g| g.explored)
            .unwrap_or(true)
        {
            return Ok(());
        }

        let expr_count = self
            .memo
            .get_group(group_id)
            .map(|g| g.logical_exprs.len())
            .unwrap_or(0);

        for expr_idx in 0..expr_count {
            let expr_id = ExprId::logical(group_id, expr_idx);
            self.explore_expr(expr_id)?;
        }

        if let Some(group) = self.memo.get_group_mut(group_id) {
            group.explored = true;
        }

        Ok(())
    }

    fn explore_expr(&mut self, expr_id: ExprId) -> OptimizerResult<()> {
        let expr = match self.memo.get_logical_expr(expr_id) {
            Some(e) => e.clone(),
            None => return Ok(()),
        };

        for &child_group in &expr.children {
            self.explore_group(child_group)?;
        }

        for rule in self.transformation_rules.clone() {
            if rule.matches(&expr, &self.memo) {
                let new_exprs = rule.apply(&expr, &mut self.memo)?;
                for new_expr in new_exprs {
                    if self
                        .memo
                        .add_logical_expr_to_group(expr_id.group_id, new_expr)
                        .is_some()
                    {
                        self.opt_stats.transformations_applied += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn implement_group(
        &mut self,
        group_id: GroupId,
        required: &RequiredProperties,
        mut upper_bound: f64,
    ) -> OptimizerResult<Option<f64>> {
        let expr_count = self
            .memo
            .get_group(group_id)
            .map(|g| g.logical_exprs.len())
            .unwrap_or(0);

        let mut best_cost: Option<f64> = None;
        let mut best_expr_id: Option<ExprId> = None;

        for expr_idx in 0..expr_count {
            let logical_id = ExprId::logical(group_id, expr_idx);

            let logical_expr = match self.memo.get_logical_expr(logical_id) {
                Some(e) => e.clone(),
                None => continue,
            };

            for rule in self.implementation_rules.clone() {
                if !rule.matches(&logical_expr, &self.memo) {
                    continue;
                }

                let physical_exprs = rule.implement(&logical_expr, required, &self.memo)?;

                for phys_expr in physical_exprs {
                    let cost =
                        self.compute_expr_cost(group_id, &phys_expr, required, upper_bound)?;

                    if let Some(c) = cost {
                        if best_cost.map(|bc| c < bc).unwrap_or(true) {
                            let phys_id = self.memo.add_physical_expr_to_group(group_id, phys_expr);
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

        let winner = best_expr_id.map(|id| memo::Winner {
            expr_id: id,
            cost: best_cost.unwrap(),
        });

        if let Some(group) = self.memo.get_group_mut(group_id) {
            group.set_winner(required.clone(), winner);
        }

        Ok(best_cost)
    }

    fn compute_expr_cost(
        &mut self,
        group_id: GroupId,
        phys_expr: &physical::PhysicalExpr,
        parent_required: &RequiredProperties,
        upper_bound: f64,
    ) -> OptimizerResult<Option<f64>> {
        let mut children_stats = Vec::with_capacity(phys_expr.children.len());
        let mut children_cost = 0.0;

        for (i, &child_group) in phys_expr.children.iter().enumerate() {
            let child_required = phys_expr.op.required_child_properties(i, parent_required);

            let remaining = upper_bound - children_cost;
            if remaining <= 0.0 {
                return Ok(None);
            }

            match self.optimize_group(child_group, &child_required, remaining)? {
                Some(cost) => {
                    children_cost += cost;
                    let child_stats = self.derive_group_stats(child_group);
                    children_stats.push(child_stats);
                }
                None => return Ok(None),
            }
        }

        let op_cost = self.cost_model.compute_operator_cost(
            &phys_expr.op,
            self.stats_provider.as_ref(),
            &children_stats,
        );

        let total = children_cost + op_cost;

        if total >= upper_bound {
            return Ok(None);
        }

        Ok(Some(total))
    }

    fn derive_group_stats(&self, group_id: GroupId) -> DerivedStats {
        let group = match self.memo.get_group(group_id) {
            Some(g) => g,
            None => return DerivedStats::unknown(),
        };

        DerivedStats::new(
            group.logical_props.cardinality,
            group.logical_props.avg_row_size,
        )
    }

    fn extract_plan(
        &self,
        group_id: GroupId,
        required: &RequiredProperties,
    ) -> OptimizerResult<PhysicalPlan> {
        let group = self.memo.get_group(group_id).ok_or_else(|| {
            OptimizerError::InvalidState(format!("Group {:?} not found", group_id))
        })?;

        let winner = group.get_winner(required).ok_or_else(|| {
            OptimizerError::NoPlanFound(format!(
                "No winner for group {:?} with {:?}",
                group_id, required
            ))
        })?;

        let phys_expr = self
            .memo
            .get_physical_expr(winner.expr_id)
            .ok_or_else(|| OptimizerError::InvalidState("Winner expression not found".into()))?;

        let mut children = Vec::with_capacity(phys_expr.children.len());
        for (i, &child_group) in phys_expr.children.iter().enumerate() {
            let child_required = phys_expr.op.required_child_properties(i, required);
            children.push(self.extract_plan(child_group, &child_required)?);
        }

        Ok(PhysicalPlan {
            op: phys_expr.op.clone(),
            children,
            cost: winner.cost,
            properties: phys_expr.properties.clone(),
        })
    }

    pub(crate) fn get_stats(&self) -> &OptimizationStats {
        &self.opt_stats
    }
}
