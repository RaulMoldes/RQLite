//! Plan builder - converts bound statements to logical plans in the memo.
//!
//! This module translates the output of the binder (BoundStatement) into
//! logical operators that can be optimized by the Cascades optimizer.

use crate::database::SharedCatalog;
use crate::database::schema::Schema;
use crate::sql::ast::JoinType;
use crate::sql::binder::ast::*;
use crate::transactions::worker::Worker;
use crate::types::ObjectId;

use super::logical::*;
use super::memo::{GroupId, Memo};
use super::prop::LogicalProperties;
use super::prop::PropertyDeriver;
use super::stats::StatisticsProvider;
use super::{OptimizerError, OptimizerResult};

/// Builds logical plans from bound statements
pub struct PlanBuilder<'a, P: StatisticsProvider> {
    memo: &'a mut Memo,
    catalog: SharedCatalog,
    worker: Worker,

    deriver: PropertyDeriver<'a, P>,
    /// CTE storage for WITH queries - maps CTE index to its built group
    cte_groups: Vec<Option<GroupId>>,
}

impl<'a, P: StatisticsProvider> PlanBuilder<'a, P> {
    pub fn new(
        memo: &'a mut Memo,
        catalog: SharedCatalog,
        worker: Worker,
        stats_provider: &'a P,
    ) -> Self {
        Self {
            memo,
            catalog,
            worker,
            deriver: PropertyDeriver::new(stats_provider),
            cte_groups: Vec::new(),
        }
    }

    pub fn build(mut self, stmt: &BoundStatement) -> OptimizerResult<GroupId> {
        match stmt {
            BoundStatement::Select(select) => self.build_select(select),
            BoundStatement::Insert(insert) => self.build_insert(insert),
            BoundStatement::Update(update) => self.build_update(update),
            BoundStatement::Delete(delete) => self.build_delete(delete),
            BoundStatement::With(with) => self.build_with(with),
            BoundStatement::CreateTable(_)
            | BoundStatement::CreateIndex(_)
            | BoundStatement::AlterTable(_)
            | BoundStatement::DropTable(_)
            | BoundStatement::Transaction(_) => Err(OptimizerError::Unsupported(
                "DDL and transaction statements are executed directly".to_string(),
            )),
        }
    }

    fn build_select(&mut self, select: &BoundSelect) -> OptimizerResult<GroupId> {
        let from_group = match &select.from {
            Some(table_ref) => self.build_table_ref(table_ref)?,
            None => self.build_empty(&select.schema)?,
        };

        let filtered_group = match &select.where_clause {
            Some(predicate) => self.build_filter(from_group, predicate.clone())?,
            None => from_group,
        };

        let aggregated_group =
            if !select.group_by.is_empty() || self.has_aggregates(&select.columns) {
                self.build_aggregate(
                    filtered_group,
                    &select.group_by,
                    &select.columns,
                    &select.schema,
                )?
            } else {
                filtered_group
            };

        let having_group = match &select.having {
            Some(predicate) => self.build_filter(aggregated_group, predicate.clone())?,
            None => aggregated_group,
        };

        let projected_group = self.build_project(having_group, &select.columns, &select.schema)?;

        let distinct_group = if select.distinct {
            self.build_distinct(projected_group, &select.schema)?
        } else {
            projected_group
        };

        let sorted_group = if !select.order_by.is_empty() {
            self.build_sort(distinct_group, &select.order_by, &select.schema)?
        } else {
            distinct_group
        };

        let limited_group = match select.limit {
            Some(limit) => self.build_limit(sorted_group, limit, select.offset, &select.schema)?,
            None => match select.offset {
                Some(offset) if offset > 0 => {
                    self.build_limit(sorted_group, usize::MAX, Some(offset), &select.schema)?
                }
                _ => sorted_group,
            },
        };

        Ok(limited_group)
    }

    fn build_table_ref(&mut self, table_ref: &BoundTableRef) -> OptimizerResult<GroupId> {
        match table_ref {
            BoundTableRef::BaseTable { table_id, schema } => {
                let table_name = self.get_table_name(*table_id)?;
                self.build_table_scan(*table_id, table_name, schema.clone())
            }
            BoundTableRef::Subquery { query, .. } => self.build_select(query),
            BoundTableRef::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                let left_group = self.build_table_ref(left)?;
                let right_group = self.build_table_ref(right)?;
                self.build_join(
                    left_group,
                    right_group,
                    *join_type,
                    condition.clone(),
                    left.schema().clone(),
                    right.schema().clone(),
                )
            }
            BoundTableRef::Cte { cte_idx, .. } => {
                if let Some(Some(group_id)) = self.cte_groups.get(*cte_idx) {
                    Ok(*group_id)
                } else {
                    Err(OptimizerError::Internal(format!(
                        "CTE index {} not found",
                        cte_idx
                    )))
                }
            }
        }
    }

    fn build_table_scan(
        &mut self,
        table_id: ObjectId,
        table_name: String,
        schema: Schema,
    ) -> OptimizerResult<GroupId> {
        let op = LogicalOperator::TableScan(TableScanOp::new(table_id, table_name, schema));
        let props = self.deriver.derive(&op, &[]);
        let expr = LogicalExpr::new(op, vec![]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_filter(
        &mut self,
        input: GroupId,
        predicate: BoundExpression,
    ) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let input_schema = input_props.schema.clone();

        let op = LogicalOperator::Filter(FilterOp::new(predicate, input_schema));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_project(
        &mut self,
        input: GroupId,
        columns: &[BoundSelectItem],
        output_schema: &Schema,
    ) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let input_schema = input_props.schema.clone();

        let expressions: Vec<ProjectExpr> = columns
            .iter()
            .map(|item| ProjectExpr {
                expr: item.expr.clone(),
                alias: None,
            })
            .collect();

        let op = LogicalOperator::Project(ProjectOp::new(
            expressions,
            input_schema,
            output_schema.clone(),
        ));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_join(
        &mut self,
        left: GroupId,
        right: GroupId,
        join_type: JoinType,
        condition: Option<BoundExpression>,
        left_schema: Schema,
        right_schema: Schema,
    ) -> OptimizerResult<GroupId> {
        let left_props = self.get_group_properties(left)?;
        let right_props = self.get_group_properties(right)?;

        let op =
            LogicalOperator::Join(JoinOp::new(join_type, condition, left_schema, right_schema));
        let props = self.deriver.derive(&op, &[&left_props, &right_props]);
        let expr = LogicalExpr::new(op, vec![left, right]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_aggregate(
        &mut self,
        input: GroupId,
        group_by: &[BoundExpression],
        columns: &[BoundSelectItem],
        output_schema: &Schema,
    ) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let input_schema = input_props.schema.clone();

        let mut aggregates = Vec::new();
        for (idx, item) in columns.iter().enumerate() {
            self.collect_aggregates(&item.expr, idx, &mut aggregates);
        }

        let op = LogicalOperator::Aggregate(AggregateOp::new(
            group_by.to_vec(),
            aggregates,
            input_schema,
            output_schema.clone(),
        ));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_sort(
        &mut self,
        input: GroupId,
        order_by: &[BoundOrderBy],
        schema: &Schema,
    ) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;

        let sort_exprs: Vec<SortExpr> = order_by
            .iter()
            .map(|o| SortExpr {
                expr: o.expr.clone(),
                asc: o.asc,
                nulls_first: o.nulls_first,
            })
            .collect();

        let op = LogicalOperator::Sort(SortOp::new(sort_exprs, schema.clone()));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_limit(
        &mut self,
        input: GroupId,
        limit: usize,
        offset: Option<usize>,
        schema: &Schema,
    ) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;

        let op = LogicalOperator::Limit(LimitOp::new(limit, offset, schema.clone()));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_distinct(&mut self, input: GroupId, schema: &Schema) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;

        let op = LogicalOperator::Distinct(DistinctOp::new(schema.clone()));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_empty(&mut self, schema: &Schema) -> OptimizerResult<GroupId> {
        let op = LogicalOperator::Empty(EmptyOp::new(schema.clone()));
        let props = self.deriver.derive(&op, &[]);
        let expr = LogicalExpr::new(op, vec![]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_insert(&mut self, insert: &BoundInsert) -> OptimizerResult<GroupId> {
        let source_group = match &insert.source {
            BoundInsertSource::Values(rows) => self.build_values(rows, &insert.table_schema)?,
            BoundInsertSource::Query(query) => self.build_select(query)?,
        };

        let source_props = self.get_group_properties(source_group)?;

        let op = LogicalOperator::Insert(InsertOp::new(
            insert.table_id,
            insert.columns.clone(),
            insert.table_schema.clone(),
        ));
        let props = self.deriver.derive(&op, &[&source_props]);
        let expr = LogicalExpr::new(op, vec![source_group]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_values(
        &mut self,
        rows: &[Vec<BoundExpression>],
        schema: &Schema,
    ) -> OptimizerResult<GroupId> {
        let op = LogicalOperator::Values(ValuesOp::new(rows.to_vec(), schema.clone()));
        let props = self.deriver.derive(&op, &[]);
        let expr = LogicalExpr::new(op, vec![]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_update(&mut self, update: &BoundUpdate) -> OptimizerResult<GroupId> {
        let table_name = self.get_table_name(update.table_id)?;

        let scan_group =
            self.build_table_scan(update.table_id, table_name, update.table_schema.clone())?;

        let filtered_group = match &update.filter {
            Some(predicate) => self.build_filter(scan_group, predicate.clone())?,
            None => scan_group,
        };

        let filtered_props = self.get_group_properties(filtered_group)?;

        let assignments: Vec<(usize, BoundExpression)> = update
            .assignments
            .iter()
            .map(|a| (a.column_idx, a.value.clone()))
            .collect();

        let op = LogicalOperator::Update(UpdateOp::new(
            update.table_id,
            assignments,
            update.table_schema.clone(),
        ));
        let props = self.deriver.derive(&op, &[&filtered_props]);
        let expr = LogicalExpr::new(op, vec![filtered_group]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_delete(&mut self, delete: &BoundDelete) -> OptimizerResult<GroupId> {
        let table_name = self.get_table_name(delete.table_id)?;

        let scan_group =
            self.build_table_scan(delete.table_id, table_name, delete.table_schema.clone())?;

        let filtered_group = match &delete.filter {
            Some(predicate) => self.build_filter(scan_group, predicate.clone())?,
            None => scan_group,
        };

        let filtered_props = self.get_group_properties(filtered_group)?;

        let op =
            LogicalOperator::Delete(DeleteOp::new(delete.table_id, delete.table_schema.clone()));
        let props = self.deriver.derive(&op, &[&filtered_props]);
        let expr = LogicalExpr::new(op, vec![filtered_group]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_with(&mut self, with: &BoundWith) -> OptimizerResult<GroupId> {
        self.cte_groups = vec![None; with.ctes.len()];

        for (idx, cte_query) in with.ctes.iter().enumerate() {
            let cte_group = self.build_select(cte_query)?;
            self.cte_groups[idx] = Some(cte_group);
        }

        self.build_select(&with.body)
    }

    fn get_group_properties(&self, group_id: GroupId) -> OptimizerResult<LogicalProperties> {
        let group = self
            .memo
            .get_group(group_id)
            .ok_or_else(|| OptimizerError::InvalidState("Group not found".to_string()))?;
        Ok(group.logical_props.clone())
    }

    fn get_table_name(&self, table_id: ObjectId) -> OptimizerResult<String> {
        match self
            .catalog
            .get_relation_unchecked(table_id, self.worker.clone())
        {
            Ok(relation) => Ok(relation.name().to_string()),
            Err(_) => Err(OptimizerError::Internal(format!(
                "Table {:?} not found in catalog",
                table_id
            ))),
        }
    }

    fn has_aggregates(&self, columns: &[BoundSelectItem]) -> bool {
        columns
            .iter()
            .any(|item| self.contains_aggregate(&item.expr))
    }

    fn contains_aggregate(&self, expr: &BoundExpression) -> bool {
        match expr {
            BoundExpression::Aggregate { .. } => true,
            BoundExpression::BinaryOp { left, right, .. } => {
                self.contains_aggregate(left) || self.contains_aggregate(right)
            }
            BoundExpression::UnaryOp { expr, .. } => self.contains_aggregate(expr),
            BoundExpression::Case {
                operand,
                when_then,
                else_expr,
                ..
            } => {
                operand
                    .as_ref()
                    .map_or(false, |e| self.contains_aggregate(e))
                    || when_then
                        .iter()
                        .any(|(c, r)| self.contains_aggregate(c) || self.contains_aggregate(r))
                    || else_expr
                        .as_ref()
                        .map_or(false, |e| self.contains_aggregate(e))
            }
            BoundExpression::Function { args, .. } => {
                args.iter().any(|a| self.contains_aggregate(a))
            }
            BoundExpression::InList { expr, list, .. } => {
                self.contains_aggregate(expr) || list.iter().any(|e| self.contains_aggregate(e))
            }
            BoundExpression::InSubquery { expr, .. } => self.contains_aggregate(expr),
            BoundExpression::Between {
                expr, low, high, ..
            } => {
                self.contains_aggregate(expr)
                    || self.contains_aggregate(low)
                    || self.contains_aggregate(high)
            }
            BoundExpression::IsNull { expr, .. } => self.contains_aggregate(expr),
            _ => false,
        }
    }

    fn collect_aggregates(
        &self,
        expr: &BoundExpression,
        output_idx: usize,
        aggregates: &mut Vec<AggregateExpr>,
    ) {
        match expr {
            BoundExpression::Aggregate {
                func,
                arg,
                distinct,
                ..
            } => {
                aggregates.push(AggregateExpr {
                    func: *func,
                    arg: arg.as_ref().map(|a| *a.clone()),
                    distinct: *distinct,
                    output_idx,
                });
            }
            BoundExpression::BinaryOp { left, right, .. } => {
                self.collect_aggregates(left, output_idx, aggregates);
                self.collect_aggregates(right, output_idx, aggregates);
            }
            BoundExpression::UnaryOp { expr, .. } => {
                self.collect_aggregates(expr, output_idx, aggregates);
            }
            BoundExpression::Case {
                operand,
                when_then,
                else_expr,
                ..
            } => {
                if let Some(op) = operand {
                    self.collect_aggregates(op, output_idx, aggregates);
                }
                for (cond, result) in when_then {
                    self.collect_aggregates(cond, output_idx, aggregates);
                    self.collect_aggregates(result, output_idx, aggregates);
                }
                if let Some(else_e) = else_expr {
                    self.collect_aggregates(else_e, output_idx, aggregates);
                }
            }
            BoundExpression::Function { args, .. } => {
                for arg in args {
                    self.collect_aggregates(arg, output_idx, aggregates);
                }
            }
            BoundExpression::InList { expr, list, .. } => {
                self.collect_aggregates(expr, output_idx, aggregates);
                for item in list {
                    self.collect_aggregates(item, output_idx, aggregates);
                }
            }
            BoundExpression::Between {
                expr, low, high, ..
            } => {
                self.collect_aggregates(expr, output_idx, aggregates);
                self.collect_aggregates(low, output_idx, aggregates);
                self.collect_aggregates(high, output_idx, aggregates);
            }
            BoundExpression::IsNull { expr, .. } => {
                self.collect_aggregates(expr, output_idx, aggregates);
            }
            _ => {}
        }
    }

    /// Convert a memo group to a LogicalPlan tree
    pub fn build_logical_plan(&self, group_id: GroupId) -> OptimizerResult<LogicalPlan> {
        self.build_logical_plan_recursive(group_id, &mut vec![])
    }

    fn build_logical_plan_recursive(
        &self,
        group_id: GroupId,
        visited: &mut Vec<GroupId>,
    ) -> OptimizerResult<LogicalPlan> {
        // Prevent infinite recursion
        if visited.contains(&group_id) {
            return Err(OptimizerError::InvalidState(
                "Cycle detected in logical plan".to_string(),
            ));
        }
        visited.push(group_id);

        let group = self
            .memo
            .get_group(group_id)
            .ok_or_else(|| OptimizerError::InvalidState("Group not found".to_string()))?;

        // Get the best expression from the group (for now, just take the first)
        let expr = group
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No logical expr in group".to_string()))?
            .clone();

        // Build children plans
        let mut children = Vec::new();
        for child_id in &expr.children {
            children.push(self.build_logical_plan_recursive(*child_id, visited)?);
        }

        // Remove from visited for other paths
        visited.pop();

        Ok(LogicalPlan {
            op: expr.op,
            children,
            properties: expr.properties,
        })
    }
}

#[cfg(test)]
mod planner_tests {
    use super::*;
    use crate::database::{Database, stats::CatalogStatsProvider};
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::binder::Binder;
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
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

        // Create users table
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, false);
        let worker = db.main_worker_cloned();
        db.catalog()
            .create_table("users", users_schema, worker.clone())?;

        // Create orders table
        let mut orders_schema = Schema::new();
        orders_schema.add_column("id", DataTypeKind::Int, true, true, false);
        orders_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        orders_schema.add_column("amount", DataTypeKind::Double, false, false, false);
        orders_schema.add_column("status", DataTypeKind::Text, false, false, false);
        db.catalog()
            .create_table("orders", orders_schema, worker.clone())?;

        // Create products table
        let mut products_schema = Schema::new();
        products_schema.add_column("id", DataTypeKind::Int, true, true, false);
        products_schema.add_column("name", DataTypeKind::Text, false, false, false);
        products_schema.add_column("price", DataTypeKind::Double, false, false, false);
        products_schema.add_column("category", DataTypeKind::Text, false, false, false);
        db.catalog()
            .create_table("products", products_schema, worker.clone())?;

        // Create order_items table
        let mut order_items_schema = Schema::new();
        order_items_schema.add_column("id", DataTypeKind::Int, true, true, false);
        order_items_schema.add_column("order_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("product_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("quantity", DataTypeKind::Int, false, false, false);
        db.catalog()
            .create_table("order_items", order_items_schema, worker)?;

        Ok(db)
    }

    fn build_plan(sql: &str, db: &Database) -> OptimizerResult<GroupId> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser
            .parse()
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;

        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        let bound_stmt = binder
            .bind(&stmt)
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;
        let provider = CatalogStatsProvider::new(db.catalog(), db.main_worker_cloned());

        let mut memo = Memo::new();
        let builder = PlanBuilder::new(&mut memo, db.catalog(), db.main_worker_cloned(), &provider);
        let id = builder.build(&bound_stmt)?;
        let displayer =
            PlanBuilder::new(&mut memo, db.catalog(), db.main_worker_cloned(), &provider);
        let plan = displayer.build_logical_plan(id)?;

        Ok(id)
    }

    fn build_plan_with_memo(sql: &str, db: &Database) -> OptimizerResult<(GroupId, Memo)> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser
            .parse()
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;

        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        let bound_stmt = binder
            .bind(&stmt)
            .map_err(|e| OptimizerError::Internal(e.to_string()))?;

        let mut memo = Memo::new();
        let provider = CatalogStatsProvider::new(db.catalog(), db.main_worker_cloned());
        let builder = PlanBuilder::new(&mut memo, db.catalog(), db.main_worker_cloned(), &provider);
        let group_id = builder.build(&bound_stmt)?;

        let displayer =
            PlanBuilder::new(&mut memo, db.catalog(), db.main_worker_cloned(), &provider);
        let plan = displayer.build_logical_plan(group_id)?;
        println!("{}", plan.explain());
        std::fs::write("output.dot", plan.to_graphviz("MY QUERY PLAN")).unwrap();
        Ok((group_id, memo))
    }

    fn get_root_operator(memo: &Memo, group_id: GroupId) -> Option<LogicalOperator> {
        memo.get_group(group_id)
            .and_then(|g| g.logical_exprs.first())
            .map(|e| e.op.clone())
    }

    #[test]
    #[serial]
    fn test_simple_select_all() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT * FROM order_items", &db).unwrap();

        // Should have created groups
        assert!(memo.num_groups() > 0);

        // Root should be a Project
        let root_op = get_root_operator(&memo, group_id);

        assert!(matches!(root_op, Some(LogicalOperator::Project(_))));
    }

    #[test]
    #[serial]
    fn test_select_specific_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT name, email FROM users", &db).unwrap();

        let root_op = get_root_operator(&memo, group_id);

        assert!(matches!(root_op, Some(LogicalOperator::Project(_))));

        // Verify projection has correct number of expressions
        if let Some(LogicalOperator::Project(proj)) = root_op {
            assert_eq!(proj.expressions.len(), 2);
        }
    }

    #[test]
    #[serial]
    fn test_select_without_from() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT 1 + 1", &db).unwrap();

        // Should have an Empty operator as the source
        let group = memo.get_group(group_id).unwrap();
        let children = &group.logical_exprs[0].children;

        if !children.is_empty() {
            let child_op = get_root_operator(&memo, children[0]);
            assert!(matches!(child_op, Some(LogicalOperator::Empty(_))));
        }
    }

    #[test]
    #[serial]
    fn test_select_with_where_equality() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT * FROM users WHERE id = 1", &db).unwrap();

        // Should have a Filter in the plan
        let group = memo.get_group(group_id).unwrap();
        let project_children = &group.logical_exprs[0].children;
        assert!(!project_children.is_empty());

        let filter_op = get_root_operator(&memo, project_children[0]);
        assert!(matches!(filter_op, Some(LogicalOperator::Filter(_))));
    }

    #[test]
    #[serial]
    fn test_select_with_where_comparison() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE age > 18", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_and() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE age > 18 AND name = 'John'", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_or() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE age < 18 OR age > 65", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_in_list() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE id IN (1, 2, 3)", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_between() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE age BETWEEN 18 AND 65", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_is_null() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE email IS NULL", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_is_not_null() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE email IS NOT NULL", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_with_where_like() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE name LIKE 'John%'", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_inner_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        // Find the Join operator
        let mut found_join = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if matches!(expr.op, LogicalOperator::Join(_)) {
                    found_join = true;
                    if let LogicalOperator::Join(join) = &expr.op {
                        assert_eq!(join.join_type, JoinType::Inner);
                    }
                }
            }
        }
        assert!(found_join);
    }

    #[test]
    #[serial]
    fn test_left_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_right_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_cross_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users, orders", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_multiple_joins() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan_with_memo(
            "SELECT u.name, o.id, p.name
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN order_items oi ON o.id = oi.order_id
             JOIN products p ON oi.product_id = p.id",
            &db,
        );

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_self_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan_with_memo(
            "SELECT u1.name, u2.name FROM users u1 JOIN users u2 ON u1.id = u2.id",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_subquery_in_from() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM (SELECT id, name FROM users WHERE age > 18) AS adults",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_subquery_in_where_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_subquery_in_where_in() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_nested_subqueries() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM (SELECT * FROM (SELECT id, name FROM users) AS inner_q) AS outer_q",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_simple_count() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT COUNT(*) FROM users", &db).unwrap();

        // Should have an Aggregate operator
        let mut found_agg = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if matches!(expr.op, LogicalOperator::Aggregate(_)) {
                    found_agg = true;
                }
            }
        }
        assert!(found_agg);
    }

    #[test]
    #[serial]
    fn test_sum_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT SUM(amount) FROM orders", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_avg_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT AVG(age) FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_min_max_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT MIN(age), MAX(age) FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_group_by_single_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT status, COUNT(*) FROM orders GROUP BY status", &db)
                .unwrap();

        // Verify Aggregate has group by
        let mut found = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Aggregate(agg) = &expr.op {
                    if !agg.group_by.is_empty() {
                        found = true;
                    }
                }
            }
        }
        assert!(found);
    }

    #[test]
    #[serial]
    fn test_group_by_multiple_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT user_id, status, SUM(amount) FROM orders GROUP BY user_id, status",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_having_clause() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id HAVING SUM(amount) > 100",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_count_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT COUNT(DISTINCT user_id) FROM orders", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_order_by_single_column_asc() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT * FROM users ORDER BY name", &db).unwrap();

        // Should have a Sort operator
        let mut found_sort = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if matches!(expr.op, LogicalOperator::Sort(_)) {
                    found_sort = true;
                }
            }
        }
        assert!(found_sort);
    }

    #[test]
    #[serial]
    fn test_order_by_single_column_desc() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT * FROM users ORDER BY age DESC", &db).unwrap();

        // Verify Sort has descending order
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Sort(sort) = &expr.op {
                    assert!(!sort.order_by[0].asc);
                }
            }
        }
    }

    #[test]
    #[serial]
    fn test_order_by_multiple_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users ORDER BY age DESC, name ASC", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_limit_only() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT * FROM users LIMIT 10", &db).unwrap();

        // Should have a Limit operator
        let root_op = get_root_operator(&memo, group_id);
        // The root might be Project, Limit should be in the tree
        let mut found_limit = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Limit(limit) = &expr.op {
                    assert_eq!(limit.limit, 10);
                    found_limit = true;
                }
            }
        }
        assert!(found_limit);
    }

    #[test]
    #[serial]
    // Currently fails
    fn test_limit_with_offset() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT * FROM users LIMIT 10 OFFSET 5", &db).unwrap();

        let mut found = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Limit(limit) = &expr.op {
                    assert_eq!(limit.limit, 10);
                    assert_eq!(limit.offset, Some(5));
                    found = true;
                }
            }
        }
        assert!(found);
    }

    #[test]
    #[serial]
    fn test_order_by_with_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users ORDER BY age DESC LIMIT 5", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_select_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT DISTINCT name FROM users", &db).unwrap();

        // Should have a Distinct operator
        let mut found = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if matches!(expr.op, LogicalOperator::Distinct(_)) {
                    found = true;
                }
            }
        }
        assert!(found);
    }

    #[test]
    #[serial]
    fn test_select_distinct_multiple_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT DISTINCT name, age FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_insert_values_single_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'John', 'john@test.com', 25)",
            &db,
        )
        .unwrap();

        // Root should be Insert
        let root_op = get_root_operator(&memo, group_id);
        assert!(matches!(root_op, Some(LogicalOperator::Insert(_))));
    }

    #[test]
    #[serial]
    fn test_insert_values_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo(
            "INSERT INTO users (id, name, email, age) VALUES
             (1, 'John', 'john@test.com', 25),
             (2, 'Jane', 'jane@test.com', 30)",
            &db,
        )
        .unwrap();

        // Should have Values operator with 2 rows
        let mut found = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Values(vals) = &expr.op {
                    assert_eq!(vals.rows.len(), 2);
                    found = true;
                }
            }
        }
        assert!(found);
    }

    #[test]
    #[serial]
    fn test_insert_from_select() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "INSERT INTO users (id, name, email, age) SELECT id, name, email, age FROM users WHERE age > 18",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_insert_partial_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'John', 'j@t.com', 25)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_update_single_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("UPDATE users SET age = 30 WHERE id = 1", &db).unwrap();

        // Root should be Update
        let root_op = get_root_operator(&memo, group_id);
        assert!(matches!(root_op, Some(LogicalOperator::Update(_))));
    }

    #[test]
    #[serial]
    fn test_update_multiple_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("UPDATE users SET name = 'Jane', age = 35 WHERE id = 1", &db)
                .unwrap();

        // Verify multiple assignments
        let root_op = get_root_operator(&memo, group_id);
        if let Some(LogicalOperator::Update(upd)) = root_op {
            assert_eq!(upd.assignments.len(), 2);
        } else {
            panic!("Expected Update operator");
        }
    }

    #[test]
    #[serial]
    fn test_update_with_expression() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("UPDATE users SET age = age + 1 WHERE id = 1", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_update_without_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("UPDATE users SET age = 0", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_delete_with_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("DELETE FROM users WHERE id = 1", &db).unwrap();

        // Root should be Delete
        let root_op = get_root_operator(&memo, group_id);
        assert!(matches!(root_op, Some(LogicalOperator::Delete(_))));
    }

    #[test]
    #[serial]
    fn test_delete_without_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("DELETE FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_delete_with_complex_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("DELETE FROM users WHERE age < 18 OR age > 65", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_simple_cte() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "WITH adults AS (SELECT * FROM users WHERE age >= 18) SELECT * FROM adults",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_multiple_ctes() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "WITH
             adults AS (SELECT * FROM users WHERE age >= 18),
             seniors AS (SELECT * FROM users WHERE age >= 65)
             SELECT * FROM adults",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_create_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("CREATE TABLE test (id INT PRIMARY KEY, name TEXT)", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    fn test_drop_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("DROP TABLE users", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    fn test_alter_table_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("ALTER TABLE users ADD COLUMN phone TEXT", &db);
        assert!(matches!(result, Err(OptimizerError::Unsupported(_))));
    }

    #[test]
    #[serial]
    // currently fails.
    fn test_complex_select_with_all_clauses() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT DISTINCT u.name, SUM(o.amount) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             WHERE u.age >= 18
             GROUP BY u.name
             HAVING SUM(o.amount) > 100
             ORDER BY total DESC
             LIMIT 10",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_deeply_nested_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM (
                        SELECT id, name FROM users WHERE age > 18
                    ) AS level1 WHERE id > 0
                ) AS level2
             ) AS level3",
            &db,
        );
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_multiple_subqueries_in_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM users u
             WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100)
             AND EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_arithmetic_expressions() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT age + 10, age * 2, age / 2 FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_string_functions() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT UPPER(name), LENGTH(email) FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_case_expression() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT name, CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_coalesce_function() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT COALESCE(email, 'no email') FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_empty_table_name() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // This should fail at binder level
        let result = build_plan("SELECT * FROM nonexistent_table", &db);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_alias_handling() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT u.name AS user_name, o.amount AS order_amount
             FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_null_literal() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT NULL FROM users", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_boolean_literals() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan("SELECT * FROM users WHERE TRUE", &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_plan_structure_select_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) =
            build_plan_with_memo("SELECT name FROM users WHERE age > 18", &db).unwrap();

        // Verify structure: Project -> Filter -> TableScan
        let group = memo.get_group(group_id).unwrap();
        assert_eq!(group.logical_exprs.len(), 1);

        let root_expr = &group.logical_exprs[0];
        assert!(matches!(root_expr.op, LogicalOperator::Project(_)));
        assert_eq!(root_expr.children.len(), 1);

        let filter_group = memo.get_group(root_expr.children[0]).unwrap();
        let filter_expr = &filter_group.logical_exprs[0];
        assert!(matches!(filter_expr.op, LogicalOperator::Filter(_)));
        assert_eq!(filter_expr.children.len(), 1);

        let scan_group = memo.get_group(filter_expr.children[0]).unwrap();
        let scan_expr = &scan_group.logical_exprs[0];
        assert!(matches!(scan_expr.op, LogicalOperator::TableScan(_)));
        assert!(scan_expr.children.is_empty());
    }

    #[test]
    #[serial]
    fn test_plan_structure_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        // Find the Join and verify it has 2 children
        let mut found_join = false;
        for group in memo.groups() {
            for expr in &group.logical_exprs {
                if let LogicalOperator::Join(join) = &expr.op {
                    assert_eq!(expr.children.len(), 2);
                    found_join = true;

                    // Both children should be scans
                    for child_id in &expr.children {
                        let child = memo.get_group(*child_id).unwrap();
                        assert!(matches!(
                            child.logical_exprs[0].op,
                            LogicalOperator::TableScan(_)
                        ));
                    }
                }
            }
        }
        assert!(found_join);
    }

    #[test]
    #[serial]
    fn test_schema_propagation() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let (group_id, memo) = build_plan_with_memo("SELECT name, age FROM users", &db).unwrap();

        // Root group schema should have 2 columns
        let root_group = memo.get_group(group_id).unwrap();
        assert_eq!(root_group.logical_props.schema.columns().len(), 2);
    }

    #[test]
    #[serial]
    fn test_large_in_list() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        // Generate a large IN list
        let ids: Vec<String> = (1..100).map(|i| i.to_string()).collect();
        let in_list = ids.join(", ");
        let sql = format!("SELECT * FROM users WHERE id IN ({})", in_list);

        let result = build_plan(&sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_many_columns_projection() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT id, name, email, age, id, name, email, age FROM users",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_chained_filters() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_test_db(dir.path().join("test.db")).unwrap();

        let result = build_plan(
            "SELECT * FROM users WHERE age > 18 AND age < 65 AND name IS NOT NULL AND email IS NOT NULL",
            &db,
        );
        assert!(result.is_ok());
    }
}
