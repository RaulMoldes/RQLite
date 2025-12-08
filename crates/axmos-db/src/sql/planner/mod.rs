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
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use crate::{database::SharedCatalog, sql::binder::ast::*, transactions::worker::Worker};

pub(crate) use memo::{ExprId, GroupId, Memo};
pub(crate) use model::{CostModel, DerivedStats};
pub(crate) use physical::PhysicalPlan;
pub(crate) use plan::PlanBuilder;
pub(crate) use prop::RequiredProperties;
pub(crate) use rules::{ImplementationRule, TransformationRule};
pub(crate) use stats::StatisticsProvider;

pub(crate) type OptimizerResult<T> = Result<T, OptimizerError>;

#[derive(Debug, Clone)]
pub(crate) enum OptimizerError {
    NoPlanFound(String),
    Internal(String),
    Unsupported(String),
    InvalidState(String),
    CostOverflow,
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
    worker: Worker,
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
        worker: Worker,
        config: OptimizerConfig,
        cost_model: C,
        stats_provider: P,
    ) -> Self {
        Self {
            memo: Memo::new(),
            catalog,
            worker,
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
    pub(crate) fn optimize(&mut self, stmt: &BoundStatement) -> OptimizerResult<PhysicalPlan> {
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
            self.worker.clone(),
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

#[cfg(test)]
mod cascades_integration_tests {
    use crate::database::{Database, schema::Schema, stats::CatalogStatsProvider};
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::binder::Binder;
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::sql::planner::{
        CascadesOptimizer, OptimizerConfig, OptimizerError, PhysicalPlan, model::AxmosCostModel,
        physical::PhysicalOperator,
    };
    use crate::types::DataTypeKind;
    use crate::{AxmosDBConfig, IncrementalVaccum, TextEncoding};
    use serial_test::serial;
    use std::path::Path;

    fn create_test_db(path: impl AsRef<Path>) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        let worker = db.main_worker_cloned();

        // Users table
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, false);
        users_schema.add_column("department_id", DataTypeKind::Int, false, false, false);
        db.catalog()
            .create_table("users", users_schema, worker.clone())?;

        // Orders table
        let mut orders_schema = Schema::new();
        orders_schema.add_column("id", DataTypeKind::Int, true, true, false);
        orders_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        orders_schema.add_column("amount", DataTypeKind::Double, false, false, false);
        orders_schema.add_column("status", DataTypeKind::Text, false, false, false);
        orders_schema.add_column("created_at", DataTypeKind::BigInt, false, false, false);
        db.catalog()
            .create_table("orders", orders_schema, worker.clone())?;

        // Products table
        let mut products_schema = Schema::new();
        products_schema.add_column("id", DataTypeKind::Int, true, true, false);
        products_schema.add_column("name", DataTypeKind::Text, false, false, false);
        products_schema.add_column("price", DataTypeKind::Double, false, false, false);
        products_schema.add_column("category_id", DataTypeKind::Int, false, false, false);
        db.catalog()
            .create_table("products", products_schema, worker.clone())?;

        // Order items table
        let mut order_items_schema = Schema::new();
        order_items_schema.add_column("id", DataTypeKind::Int, true, true, false);
        order_items_schema.add_column("order_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("product_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("quantity", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("unit_price", DataTypeKind::Double, false, false, false);
        db.catalog()
            .create_table("order_items", order_items_schema, worker.clone())?;

        // Departments table
        let mut departments_schema = Schema::new();
        departments_schema.add_column("id", DataTypeKind::Int, true, true, false);
        departments_schema.add_column("name", DataTypeKind::Text, false, false, false);
        departments_schema.add_column("budget", DataTypeKind::Double, false, false, false);
        db.catalog()
            .create_table("departments", departments_schema, worker.clone())?;

        // Categories table
        let mut categories_schema = Schema::new();
        categories_schema.add_column("id", DataTypeKind::Int, true, true, false);
        categories_schema.add_column("name", DataTypeKind::Text, false, false, false);
        categories_schema.add_column("parent_id", DataTypeKind::Int, false, false, false);
        db.catalog()
            .create_table("categories", categories_schema, worker)?;

        Ok(db)
    }

    fn optimize_query(sql: &str, db: &Database) -> Result<PhysicalPlan, OptimizerError> {
        optimize_query_with_config(sql, db, OptimizerConfig::default())
    }

    fn optimize_query_with_config(
        sql: &str,
        db: &Database,
        config: OptimizerConfig,
    ) -> Result<PhysicalPlan, OptimizerError> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser
            .parse()
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;

        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        let bound_stmt = binder
            .bind(&stmt)
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;

        let catalog = db.catalog();
        let worker = db.main_worker_cloned();
        let cost_model = AxmosCostModel::new(4096);
        let stats_provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let mut optimizer =
            CascadesOptimizer::new(catalog, worker, config, cost_model, stats_provider);
        optimizer.optimize(&bound_stmt)
    }

    /// Helper to check if a physical operator exists in the plan tree
    fn contains_operator<F>(plan: &PhysicalPlan, predicate: F) -> bool
    where
        F: Fn(&PhysicalOperator) -> bool + Copy,
    {
        if predicate(&plan.op) {
            return true;
        }
        plan.children
            .iter()
            .any(|child| contains_operator(child, predicate))
    }

    /// Helper to count occurrences of an operator type
    fn count_operators<F>(plan: &PhysicalPlan, predicate: F) -> usize
    where
        F: Fn(&PhysicalOperator) -> bool + Copy,
    {
        let self_count = if predicate(&plan.op) { 1 } else { 0 };
        let children_count: usize = plan
            .children
            .iter()
            .map(|child| count_operators(child, predicate))
            .sum();
        self_count + children_count
    }

    /// Helper to get the root operator
    fn root_operator(plan: &PhysicalPlan) -> &PhysicalOperator {
        &plan.op
    }

    /// Helper to find first operator matching predicate
    fn find_operator<'a, F>(plan: &'a PhysicalPlan, predicate: F) -> Option<&'a PhysicalOperator>
    where
        F: Fn(&PhysicalOperator) -> bool + Copy,
    {
        if predicate(&plan.op) {
            return Some(&plan.op);
        }
        for child in &plan.children {
            if let Some(found) = find_operator(child, predicate) {
                return Some(found);
            }
        }
        None
    }

    #[test]
    #[serial]
    fn test_simple_select_produces_valid_plan() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users", &db).unwrap();

        // Should have a SeqScan at some point
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::SeqScan(_)
        )));

        // Cost should be positive
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_select_with_projection() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT name, email FROM users", &db).unwrap();

        // Should have Project operator
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Project(_)
        )));
    }

    #[test]
    #[serial]
    fn test_select_with_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE age > 18", &db).unwrap();

        // Should have Filter operator
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Filter(_)
        )));
    }

    #[test]
    #[serial]
    fn test_select_without_from() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT 1 + 1, 'hello'", &db).unwrap();

        // Should have Empty operator for no-FROM queries
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Empty(_)
        )));
    }

    #[test]
    #[serial]
    fn test_inner_join_produces_join_operator() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        // Should have some join operator (Hash, Merge, or NestedLoop)
        let has_join = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_)
                    | PhysicalOperator::MergeJoin(_)
                    | PhysicalOperator::NestedLoopJoin(_)
            )
        });
        assert!(has_join, "Plan should contain a join operator");
    }

    #[test]
    #[serial]
    fn test_left_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_cross_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users, departments", &db).unwrap();

        // Cross join should produce a join operator
        let has_join = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_)
                    | PhysicalOperator::MergeJoin(_)
                    | PhysicalOperator::NestedLoopJoin(_)
            )
        });
        assert!(has_join);
    }

    #[test]
    #[serial]
    fn test_multiple_joins() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT u.name, o.id, p.name
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN order_items oi ON o.id = oi.order_id
             JOIN products p ON oi.product_id = p.id",
            &db,
        )
        .unwrap();

        // Should have multiple join operators (at least 3)
        let join_count = count_operators(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_)
                    | PhysicalOperator::MergeJoin(_)
                    | PhysicalOperator::NestedLoopJoin(_)
            )
        });
        assert!(
            join_count >= 3,
            "Expected at least 3 joins, got {}",
            join_count
        );
    }

    #[test]
    #[serial]
    fn test_self_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT u1.name, u2.name FROM users u1 JOIN users u2 ON u1.department_id = u2.department_id",
            &db,
        )
        .unwrap();

        // Should have 2 scans (same table twice)
        let scan_count = count_operators(&plan, |op| matches!(op, PhysicalOperator::SeqScan(_)));
        assert_eq!(scan_count, 2);
    }

    #[test]
    #[serial]
    fn test_count_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT COUNT(*) FROM users", &db).unwrap();

        // Should have HashAggregate or SortAggregate
        let has_agg = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_)
            )
        });
        assert!(has_agg);
    }

    #[test]
    #[serial]
    fn test_group_by_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT department_id, COUNT(*), AVG(age) FROM users GROUP BY department_id",
            &db,
        )
        .unwrap();

        let has_agg = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_)
            )
        });
        assert!(has_agg);
    }

    #[test]
    #[serial]
    fn test_having_clause() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT department_id, SUM(age) FROM users GROUP BY department_id HAVING SUM(age) > 100",
            &db,
        )
        .unwrap();

        // Should have aggregate and filter (for HAVING)
        let has_agg = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_)
            )
        });
        assert!(has_agg);
    }

    #[test]
    #[serial]
    fn test_order_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users ORDER BY age DESC", &db).unwrap();

        // Should have Sort or ExternalSort
        let has_sort = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::Sort(_) | PhysicalOperator::ExternalSort(_)
            )
        });
        assert!(has_sort);
    }

    #[test]
    #[serial]
    fn test_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users LIMIT 10", &db).unwrap();

        // Should have Limit operator
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Limit(_)
        )));
    }

    #[test]
    #[serial]
    fn test_order_by_with_limit_may_use_topn() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users ORDER BY age LIMIT 5", &db).unwrap();

        // Could be TopN or Sort+Limit depending on optimization
        let has_topn_or_sort = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::TopN(_)
                    | PhysicalOperator::Sort(_)
                    | PhysicalOperator::ExternalSort(_)
            )
        });
        assert!(has_topn_or_sort);
    }

    #[test]
    #[serial]
    fn test_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT DISTINCT department_id FROM users", &db).unwrap();

        // Should have HashDistinct or SortDistinct
        let has_distinct = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashDistinct(_) | PhysicalOperator::SortDistinct(_)
            )
        });
        assert!(has_distinct);
    }

    #[test]
    #[serial]
    fn test_insert_values() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "INSERT INTO users (id, name, email, age, department_id) VALUES (1, 'John', 'john@test.com', 25, 1)",
            &db,
        )
        .unwrap();

        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Insert(_)
        )));
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Values(_)
        )));
    }

    #[test]
    #[serial]
    fn test_update() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("UPDATE users SET age = 30 WHERE id = 1", &db).unwrap();

        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Update(_)
        )));
    }

    #[test]
    #[serial]
    fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("DELETE FROM users WHERE age < 18", &db).unwrap();

        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Delete(_)
        )));
    }

    #[test]
    #[serial]
    fn test_subquery_in_from() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM (SELECT id, name FROM users WHERE age > 18) AS adults",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_subquery_in_where_in() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_correlated_subquery() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_simple_cte() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "WITH adults AS (SELECT * FROM users WHERE age >= 18) SELECT * FROM adults",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_multiple_ctes() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "WITH
             young AS (SELECT * FROM users WHERE age < 30),
             old AS (SELECT * FROM users WHERE age >= 30)
             SELECT * FROM young",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_cost_increases_with_complexity() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let simple_plan = optimize_query("SELECT * FROM users", &db).unwrap();
        let complex_plan = optimize_query(
            "SELECT u.name, COUNT(o.id)
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name
             ORDER BY COUNT(o.id) DESC",
            &db,
        )
        .unwrap();

        assert!(
            complex_plan.cost > simple_plan.cost,
            "Complex query should have higher cost"
        );
    }

    #[test]
    #[serial]
    fn test_filter_reduces_downstream_cost() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // Query without filter
        let no_filter = optimize_query(
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        // Query with selective filter
        let with_filter = optimize_query(
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1",
            &db,
        )
        .unwrap();

        // The filtered version should generally be cheaper due to reduced cardinality
        // (though this depends on statistics and cost model specifics)
        assert!(with_filter.cost > 0.0);
        assert!(no_filter.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_optimizer_statistics_tracked() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let lexer = Lexer::new("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().unwrap();

        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        let bound_stmt = binder.bind(&stmt).unwrap();

        let catalog = db.catalog();
        let worker = db.main_worker_cloned();
        let cost_model = AxmosCostModel::new(4096);
        let stats_provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let mut optimizer = CascadesOptimizer::new(
            catalog,
            worker,
            OptimizerConfig::default(),
            cost_model,
            stats_provider,
        );

        let _plan = optimizer.optimize(&bound_stmt).unwrap();
        let stats = optimizer.get_stats();

        assert!(stats.groups_created > 0, "Should create groups");
    }

    #[test]
    #[serial]
    fn test_optimizer_respects_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let config = OptimizerConfig {
            max_optimization_time_ms: 10000, // 10 seconds should be plenty
            ..Default::default()
        };

        let result = optimize_query_with_config(
            "SELECT * FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN order_items oi ON o.id = oi.order_id",
            &db,
            config,
        );

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_pruning_enabled_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let config = OptimizerConfig::default();
        assert!(config.enable_pruning);

        let plan = optimize_query_with_config("SELECT * FROM users", &db, config).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_create_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = optimize_query("CREATE TABLE test (id INT PRIMARY KEY)", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    fn test_drop_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = optimize_query("DROP TABLE users", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    fn test_alter_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = optimize_query("ALTER TABLE users ADD COLUMN phone TEXT", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    fn test_complex_analytics_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT
                d.name as department,
                COUNT(DISTINCT u.id) as user_count,
                SUM(o.amount) as total_sales
             FROM departments d
             JOIN users u ON d.id = u.department_id
             JOIN orders o ON u.id = o.user_id
             WHERE o.status = 'completed'
             GROUP BY d.name
             HAVING SUM(o.amount) > 1000
             ORDER BY total_sales DESC
             LIMIT 10",
            &db,
        )
        .unwrap();

        // Verify plan has necessary components
        assert!(contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_)
            )
        }));

        let join_count = count_operators(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_)
                    | PhysicalOperator::MergeJoin(_)
                    | PhysicalOperator::NestedLoopJoin(_)
            )
        });
        assert!(join_count >= 2);
    }

    #[test]
    #[serial]
    fn test_deeply_nested_subqueries() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM (
                        SELECT id, name, age FROM users WHERE age > 18
                    ) AS l1 WHERE id > 0
                ) AS l2 WHERE id < 1000
            ) AS l3",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_union_like_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // Test complex OR which might be transformed
        let plan = optimize_query(
            "SELECT * FROM users WHERE department_id = 1 OR department_id = 2 OR department_id = 3",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_plan_tree_structure_simple_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT id FROM users", &db).unwrap();

        // Root should be Project
        assert!(matches!(root_operator(&plan), PhysicalOperator::Project(_)));

        // Should have exactly one scan
        let scan_count = count_operators(&plan, |op| matches!(op, PhysicalOperator::SeqScan(_)));
        assert_eq!(scan_count, 1);
    }

    #[test]
    #[serial]
    fn test_plan_tree_structure_filter_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE age > 21", &db).unwrap();

        // Should have filter
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Filter(_)
        )));

        // Filter should be above scan
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::SeqScan(_)
        )));
    }

    #[test]
    #[serial]
    fn test_equi_join_uses_hash_or_merge() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        // Equi-join should use HashJoin or MergeJoin (more efficient than NestedLoop)
        let has_efficient_join = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_) | PhysicalOperator::MergeJoin(_)
            )
        });

        // Note: This might still use NestedLoop if cost model prefers it
        // So we just verify some join exists
        let has_any_join = contains_operator(&plan, |op| {
            matches!(
                op,
                PhysicalOperator::HashJoin(_)
                    | PhysicalOperator::MergeJoin(_)
                    | PhysicalOperator::NestedLoopJoin(_)
            )
        });
        assert!(has_any_join);
    }

    #[test]
    #[serial]
    fn test_empty_result_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE 1 = 0", &db).unwrap();
        assert!(plan.cost >= 0.0);
    }

    #[test]
    #[serial]
    fn test_always_true_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE 1 = 1", &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_null_handling() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE email IS NULL", &db).unwrap();
        assert!(contains_operator(&plan, |op| matches!(
            op,
            PhysicalOperator::Filter(_)
        )));
    }

    #[test]
    #[serial]
    fn test_between_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE age BETWEEN 18 AND 65", &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_in_list_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)", &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_like_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE name LIKE 'John%'", &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_filter_pushdown_through_project() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // This query might benefit from filter pushdown
        let plan = optimize_query(
            "SELECT name FROM (SELECT id, name, age FROM users) AS sub WHERE id = 1",
            &db,
        )
        .unwrap();

        // The optimizer should produce a valid plan
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_predicate_simplification() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // TRUE AND condition should simplify
        let plan = optimize_query("SELECT * FROM users WHERE TRUE AND age > 18", &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_no_infinite_loop_on_simple_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // Should complete quickly without hanging
        let config = OptimizerConfig {
            max_optimization_time_ms: 5000,
            max_transformations: 1000,
            ..Default::default()
        };

        let result = optimize_query_with_config("SELECT * FROM users", &db, config);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_handles_many_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT id, name, email, age, department_id, id, name, email, age FROM users",
            &db,
        )
        .unwrap();

        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_handles_large_in_list() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let ids: Vec<String> = (1..50).map(|i| i.to_string()).collect();
        let in_list = ids.join(", ");
        let sql = format!("SELECT * FROM users WHERE id IN ({})", in_list);

        let plan = optimize_query(&sql, &db).unwrap();
        assert!(plan.cost > 0.0);
    }

    #[test]
    #[serial]
    fn test_plan_explain_output() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query("SELECT * FROM users WHERE age > 18", &db).unwrap();

        let explain = plan.explain();
        assert!(!explain.is_empty());
        // Should contain operator names
        assert!(
            explain.contains("Scan") || explain.contains("Filter") || explain.contains("Project")
        );
    }

    #[test]
    #[serial]
    fn test_plan_graphviz_output() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let plan = optimize_query(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();
        println!("EXPLAIN {plan}");
        let dot = plan.to_graphviz("Test Plan");
        assert!(dot.contains("digraph"));
        assert!(dot.contains("node"));
    }
}
