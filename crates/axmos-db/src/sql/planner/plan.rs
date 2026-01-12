//! Plan builder
//!
//! This module translates the output of the binder (`BoundStatement`) into
//! logical operators that can be optimized by the Cascades optimizer.

use crate::{
    schema::{Schema, catalog::StatisticsProvider},
    sql::{
        binder::bounds::*,
        parser::ast::JoinType,
        planner::{PlannerError, PlannerResult},
    },
    types::ObjectId,
};

use super::logical::*;
use super::memo::{GroupId, Memo};
use super::prop::{LogicalProperties, PropertyDeriver};

/// Builds logical plans from bound statements.
pub struct Planner<'a> {
    memo: &'a mut Memo,
    deriver: &'a PropertyDeriver<StatisticsProvider<'a>>,
    /// CTE storage for WITH queries.
    cte_groups: Vec<Option<GroupId>>,
}

impl<'a> Planner<'a> {
    pub fn new(memo: &'a mut Memo, deriver: &'a PropertyDeriver<StatisticsProvider>) -> Self {
        Self {
            memo,
            deriver,
            cte_groups: Vec::new(),
        }
    }

    /// Main entry point: builds a logical plan from a bound statement.
    pub fn build(&mut self, stmt: &BoundStatement) -> PlannerResult<GroupId> {
        match stmt {
            BoundStatement::Select(s) => self.build_select(s),
            BoundStatement::Insert(i) => self.build_insert(i),
            BoundStatement::Update(u) => self.build_update(u),
            BoundStatement::Delete(d) => self.build_delete(d),
            BoundStatement::With(w) => self.build_with(w),
            BoundStatement::CreateTable(_)
            | BoundStatement::CreateIndex(_)
            | BoundStatement::AlterTable(_)
            | BoundStatement::DropTable(_)
            | BoundStatement::Transaction(_) => Err(PlannerError::UnsupportedStatement),
        }
    }

    fn build_select(&mut self, select: &BoundSelect) -> PlannerResult<GroupId> {
        // FROM clause
        let from_group = match &select.from {
            Some(table_ref) => self.build_table_ref(table_ref)?,
            None => self.build_empty(&select.schema)?,
        };

        // WHERE clause
        let filtered = match &select.where_clause {
            Some(pred) => self.build_filter(from_group, pred.clone())?,
            None => from_group,
        };

        // GROUP BY / aggregates
        let aggregated = if !select.group_by.is_empty() || self.has_aggregates(&select.columns) {
            self.build_aggregate(filtered, &select.group_by, &select.columns, &select.schema)?
        } else {
            filtered
        };

        // HAVING clause
        let having_applied = match &select.having {
            Some(pred) => self.build_filter(aggregated, pred.clone())?,
            None => aggregated,
        };

        // ORDER BY (must go before the projection since it needs the full input schema)
        let sorted = if !select.order_by.is_empty() {
            let input_props = self.get_group_properties(having_applied)?;
            self.build_sort(having_applied, &select.order_by, &input_props.schema)?
        } else {
            having_applied
        };

        // SELECT projection (after sort)
        let projected = if !self.has_aggregates(&select.columns) {
            self.build_project(sorted, &select.columns, &select.schema)?
        } else {
            sorted
        };

        // DISTINCT
        let distinct_applied = if select.distinct {
            self.build_distinct(projected, &select.schema)?
        } else {
            projected
        };

        // LIMIT / OFFSET
        let limited = self.apply_limit_offset(
            distinct_applied,
            select.limit,
            select.offset,
            &select.schema,
        )?;

        Ok(limited)
    }

    fn build_table_ref(&mut self, table_ref: &BoundTableRef) -> PlannerResult<GroupId> {
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
            BoundTableRef::Cte { cte_idx, .. } => self
                .cte_groups
                .get(*cte_idx)
                .and_then(|g| *g)
                .ok_or(PlannerError::UnboundedItem(*cte_idx)),
        }
    }

    fn build_table_scan(
        &mut self,
        table_id: ObjectId,
        table_name: String,
        schema: Schema,
    ) -> PlannerResult<GroupId> {
        let op = LogicalOperator::TableScan(TableScanOp::new(table_id, table_name, schema));
        self.insert_with_properties(op, vec![])
    }

    fn build_filter(
        &mut self,
        input: GroupId,
        predicate: BoundExpression,
    ) -> PlannerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let op = LogicalOperator::Filter(FilterOp::new(predicate, input_props.schema.clone()));
        self.insert_with_properties(op, vec![input])
    }

    fn build_project(
        &mut self,
        input: GroupId,
        columns: &[BoundSelectItem],
        output_schema: &Schema,
    ) -> PlannerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;

        let expressions: Vec<ProjectExpr> = columns
            .iter()
            .map(|item| ProjectExpr {
                expr: item.expr.clone(),
                alias: None,
            })
            .collect();

        let op = LogicalOperator::Project(ProjectOp::new(
            expressions,
            input_props.schema.clone(),
            output_schema.clone(),
        ));
        self.insert_with_properties(op, vec![input])
    }

    fn build_join(
        &mut self,
        left: GroupId,
        right: GroupId,
        join_type: JoinType,
        condition: Option<BoundExpression>,
        left_schema: Schema,
        right_schema: Schema,
    ) -> PlannerResult<GroupId> {
        let op =
            LogicalOperator::Join(JoinOp::new(join_type, condition, left_schema, right_schema));
        self.insert_with_properties(op, vec![left, right])
    }

    fn build_aggregate(
        &mut self,
        input: GroupId,
        group_by: &[BoundExpression],
        columns: &[BoundSelectItem],
        output_schema: &Schema,
    ) -> PlannerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;

        let mut aggregates = Vec::new();
        for (idx, item) in columns.iter().enumerate() {
            self.collect_aggregates(&item.expr, idx, &mut aggregates);
        }

        let op = LogicalOperator::Aggregate(AggregateOp::new(
            group_by.to_vec(),
            aggregates,
            input_props.schema.clone(),
            output_schema.clone(),
        ));
        self.insert_with_properties(op, vec![input])
    }

    fn build_sort(
        &mut self,
        input: GroupId,
        order_by: &[BoundOrderBy],
        schema: &Schema,
    ) -> PlannerResult<GroupId> {
        let sort_exprs: Vec<SortExpr> = order_by
            .iter()
            .map(|o| SortExpr {
                expr: o.expr.clone(),
                asc: o.asc,
                nulls_first: o.nulls_first,
            })
            .collect();

        let op = LogicalOperator::Sort(SortOp::new(sort_exprs, schema.clone()));
        self.insert_with_properties(op, vec![input])
    }

    fn build_limit(
        &mut self,
        input: GroupId,
        limit: usize,
        offset: Option<usize>,
        schema: &Schema,
    ) -> PlannerResult<GroupId> {
        let op = LogicalOperator::Limit(LimitOp::new(limit, offset, schema.clone()));
        self.insert_with_properties(op, vec![input])
    }

    fn build_distinct(&mut self, input: GroupId, schema: &Schema) -> PlannerResult<GroupId> {
        let op = LogicalOperator::Distinct(DistinctOp::new(schema.clone()));
        self.insert_with_properties(op, vec![input])
    }

    fn build_empty(&mut self, schema: &Schema) -> PlannerResult<GroupId> {
        let op = LogicalOperator::Empty(EmptyOp::new(schema.clone()));
        self.insert_with_properties(op, vec![])
    }

    fn build_materialize(&mut self, input: GroupId) -> PlannerResult<GroupId> {
        let input_props = self.get_group_properties(input)?;
        let op = LogicalOperator::Materialize(MaterializeOp::new(input_props.schema.clone()));
        self.insert_with_properties(op, vec![input])
    }

    fn build_values(
        &mut self,
        rows: &[Vec<BoundExpression>],
        schema: &Schema,
    ) -> PlannerResult<GroupId> {
        let op = LogicalOperator::Values(ValuesOp::new(rows.to_vec(), schema.clone()));
        self.insert_with_properties(op, vec![])
    }

    fn build_insert(&mut self, insert: &BoundInsert) -> PlannerResult<GroupId> {
        let source_group = match &insert.source {
            BoundInsertSource::Values(rows) => self.build_values(rows, &insert.table_schema)?,
            BoundInsertSource::Query(query) => self.build_select(query)?,
        };

        let op = LogicalOperator::Insert(InsertOp::new(
            insert.table_id,
            insert.columns.clone(),
            insert.table_schema.clone(),
        ));
        self.insert_with_properties(op, vec![source_group])
    }

    fn build_update(&mut self, update: &BoundUpdate) -> PlannerResult<GroupId> {
        let table_name = self.get_table_name(update.table_id)?;

        let scan_group =
            self.build_table_scan(update.table_id, table_name, update.table_schema.clone())?;

        let filtered = match &update.filter {
            Some(pred) => self.build_filter(scan_group, pred.clone())?,
            None => scan_group,
        };

        // Materialize to prevent Halloween problem
        let materialized = self.build_materialize(filtered)?;

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
        self.insert_with_properties(op, vec![materialized])
    }

    fn build_delete(&mut self, delete: &BoundDelete) -> PlannerResult<GroupId> {
        let table_name = self.get_table_name(delete.table_id)?;

        let scan_group =
            self.build_table_scan(delete.table_id, table_name, delete.table_schema.clone())?;

        let filtered = match &delete.filter {
            Some(pred) => self.build_filter(scan_group, pred.clone())?,
            None => scan_group,
        };

        // Materialize to prevent Halloween problem
        let materialized = self.build_materialize(filtered)?;

        let op =
            LogicalOperator::Delete(DeleteOp::new(delete.table_id, delete.table_schema.clone()));
        self.insert_with_properties(op, vec![materialized])
    }

    fn build_with(&mut self, with: &BoundWith) -> PlannerResult<GroupId> {
        self.cte_groups = vec![None; with.ctes.len()];

        for (idx, cte_query) in with.ctes.iter().enumerate() {
            let cte_group = self.build_select(cte_query)?;
            self.cte_groups[idx] = Some(cte_group);
        }

        self.build_select(&with.body)
    }

    /// Applies LIMIT and OFFSET if present.
    fn apply_limit_offset(
        &mut self,
        input: GroupId,
        limit: Option<usize>,
        offset: Option<usize>,
        schema: &Schema,
    ) -> PlannerResult<GroupId> {
        match (limit, offset) {
            (Some(l), _) => self.build_limit(input, l, offset, schema),
            (None, Some(o)) if o > 0 => self.build_limit(input, usize::MAX, Some(o), schema),
            _ => Ok(input),
        }
    }

    /// Inserts a logical expression with derived properties.
    fn insert_with_properties(
        &mut self,
        op: LogicalOperator,
        children: Vec<GroupId>,
    ) -> PlannerResult<GroupId> {
        let child_props: Vec<LogicalProperties> = children
            .iter()
            .map(|&gid| self.get_group_properties(gid))
            .collect::<PlannerResult<_>>()?;

        let child_refs: Vec<&LogicalProperties> = child_props.iter().collect();
        let props = self.deriver.derive(&op, &child_refs);

        let expr = LogicalExpr::new(op, children).with_properties(props);
        Ok(self.memo.insert_logical_expr(expr))
    }

    fn get_group_properties(&self, group_id: GroupId) -> PlannerResult<LogicalProperties> {
        self.memo
            .get_group(group_id)
            .map(|g| g.logical_props.clone())
            .ok_or(PlannerError::InvalidState)
    }

    fn get_table_name(&self, table_id: ObjectId) -> PlannerResult<String> {
        // Note: This requires catalog access with proper context.
        // For now, return a placeholder. In production, this would query the catalog.
        Ok(format!("table_{}", table_id))
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
                if let Some(e) = else_expr {
                    self.collect_aggregates(e, output_idx, aggregates);
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

    /// Converts a memo group to a LogicalPlan tree (for debugging/explain).
    pub fn build_logical_plan(&self, group_id: GroupId) -> PlannerResult<LogicalPlan> {
        self.build_logical_plan_recursive(group_id, &mut Vec::new())
    }

    fn build_logical_plan_recursive(
        &self,
        group_id: GroupId,
        visited: &mut Vec<GroupId>,
    ) -> PlannerResult<LogicalPlan> {
        if visited.contains(&group_id) {
            return Err(PlannerError::Other("cycle detected in plan".to_string()));
        }
        visited.push(group_id);

        let group = self
            .memo
            .get_group(group_id)
            .ok_or(PlannerError::InvalidState)?;

        let expr = group
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?
            .clone();

        let children: Vec<LogicalPlan> = expr
            .children
            .iter()
            .map(|&cid| self.build_logical_plan_recursive(cid, visited))
            .collect::<PlannerResult<_>>()?;

        visited.pop();

        Ok(LogicalPlan {
            op: expr.op,
            children,
            properties: expr.properties,
        })
    }
}
