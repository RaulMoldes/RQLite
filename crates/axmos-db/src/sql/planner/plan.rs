//! Plan builder - converts bound statements to logical plans in the memo.
//!
//! This module translates the output of the binder (BoundStatement) into
//! logical operators that can be optimized by the Cascades optimizer.

use crate::database::SharedCatalog;
use crate::database::schema::Schema;
use crate::sql::ast::JoinType;
use crate::sql::binder::ast::*;
use crate::transactions::accessor::RcPageAccessor;
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
    accessor: RcPageAccessor,

    deriver: PropertyDeriver<'a, P>,
    /// CTE storage for WITH queries - maps CTE index to its built group
    cte_groups: Vec<Option<GroupId>>,
}

impl<'a, P: StatisticsProvider> PlanBuilder<'a, P> {
    pub fn new(
        memo: &'a mut Memo,
        catalog: SharedCatalog,
        accessor: RcPageAccessor,
        stats_provider: &'a P,
    ) -> Self {
        Self {
            memo,
            catalog,
            accessor,
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

        // In case there are aggregates, the projection is handled by the aggregate node itself.
        let projected_group = if !self.has_aggregates(&select.columns) {
            self.build_project(having_group, &select.columns, &select.schema)?
        } else {
            aggregated_group
        };

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
                    Err(OptimizerError::Other(format!(
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

        // INSERT MATERIALIZE HERE to solve the Halloween Problem
        let materialized_group = self.build_materialize(filtered_group)?;

        let materialized_props = self.get_group_properties(materialized_group)?;

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
        let props = self.deriver.derive(&op, &[&materialized_props]);
        let expr = LogicalExpr::new(op, vec![materialized_group]).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn build_materialize(&mut self, input: GroupId) -> OptimizerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let schema = input_props.schema.clone();

        let op = LogicalOperator::Materialize(MaterializeOp::new(schema));
        let props = self.deriver.derive(&op, &[&input_props]);
        let expr = LogicalExpr::new(op, vec![input]).with_properties(props);
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

        // INSERT MATERIALIZE HERE to solve the Halloween Problem
        let materialized_group = self.build_materialize(filtered_group)?;

        let materialized_props = self.get_group_properties(materialized_group)?;

        let op =
            LogicalOperator::Delete(DeleteOp::new(delete.table_id, delete.table_schema.clone()));
        let props = self.deriver.derive(&op, &[&materialized_props]);
        let expr = LogicalExpr::new(op, vec![materialized_group]).with_properties(props);
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
            .get_relation_unchecked(table_id, self.accessor.clone())
        {
            Ok(relation) => Ok(relation.name().to_string()),
            Err(_) => Err(OptimizerError::Other(format!(
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

/// TODO: Improve this tests.
#[cfg(test)]
mod planner_tests {
    use super::*;
    use crate::{
        AxmosDBConfig, IncrementalVaccum, TextEncoding,
        database::{
            Database,
            errors::{BinderResult, IntoBoxError},
            stats::CatalogStatsProvider,
        },
        io::pager::{Pager, SharedPager},
        sql::{binder::Binder, lexer::Lexer, parser::Parser},
        types::DataTypeKind,
    };

    use std::path::Path;

    fn users_schema() -> Schema {
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, false);
        users_schema
    }

    fn orders_schema() -> Schema {
        let mut orders_schema = Schema::new();
        orders_schema.add_column("id", DataTypeKind::Int, true, true, false);
        orders_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        orders_schema.add_column("amount", DataTypeKind::Double, false, false, false);
        orders_schema.add_column("status", DataTypeKind::Text, false, false, false);
        orders_schema
    }

    fn products_schema() -> Schema {
        let mut products_schema = Schema::new();
        products_schema.add_column("id", DataTypeKind::Int, true, true, false);
        products_schema.add_column("name", DataTypeKind::Text, false, false, false);
        products_schema.add_column("price", DataTypeKind::Double, false, false, false);
        products_schema.add_column("category", DataTypeKind::Text, false, false, false);
        products_schema
    }

    fn order_items_schema() -> Schema {
        let mut order_items_schema = Schema::new();
        order_items_schema.add_column("id", DataTypeKind::Int, true, true, false);
        order_items_schema.add_column("order_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("product_id", DataTypeKind::Int, false, false, false);
        order_items_schema.add_column("quantity", DataTypeKind::Int, false, false, false);
        order_items_schema
    }

    fn create_db(path: impl AsRef<Path>) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2, 5)?;

        db.task_runner()
            .run(|ctx| {
                let schema = users_schema();
                ctx.catalog()
                    .create_table("users", schema, ctx.accessor())
                    .box_err()?;
                let schema = orders_schema();
                ctx.catalog()
                    .create_table("orders", schema, ctx.accessor())
                    .box_err()?;
                let schema = order_items_schema();
                ctx.catalog()
                    .create_table("order_items", schema, ctx.accessor())
                    .box_err()?;
                let schema = products_schema();
                ctx.catalog()
                    .create_table("products", schema, ctx.accessor())
                    .box_err()?;
                Ok(())
            })
            .unwrap();

        Ok(db)
    }

    fn resolve_sql(sql: String, db: &Database) -> BinderResult<BoundStatement> {
        let stmt = db
            .task_runner()
            .run_with_result(move |ctx| {
                let lexer = Lexer::new(&sql);
                let mut parser = Parser::new(lexer);
                let stmt = parser.parse().box_err()?;
                let mut binder = Binder::new(ctx.catalog(), ctx.accessor());
                binder.bind(&stmt).box_err()
            })
            .unwrap();
        Ok(stmt)
    }

    fn create_plan(
        bound_stmt: BoundStatement,
        db: &Database,
    ) -> OptimizerResult<(GroupId, LogicalPlan)> {
        let result = db.task_runner().run_with_result(move |ctx| {
            let provider = CatalogStatsProvider::new(ctx.catalog(), ctx.accessor());

            let mut memo = Memo::new();
            let builder = PlanBuilder::new(&mut memo, ctx.catalog(), ctx.accessor(), &provider);
            let id = builder.build(&bound_stmt).box_err()?;
            let displayer = PlanBuilder::new(&mut memo, ctx.catalog(), ctx.accessor(), &provider);
            let plan = displayer.build_logical_plan(id).box_err()?;
            Ok((id, plan))
        })?;
        Ok(result)
    }

    fn build_plan(sql: String, db: &Database) -> OptimizerResult<GroupId> {
        let stmt = resolve_sql(sql, db)?;
        let (id, plan) = create_plan(stmt, db)?;
        println!("{plan}");
        Ok(id)
    }
}
