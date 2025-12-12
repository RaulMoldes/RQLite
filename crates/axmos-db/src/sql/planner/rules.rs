//! Transformation and implementation rules for the Cascades optimizer.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    OBJECT_ZERO,
    database::schema::Schema,
    sql::{
        binder::ast::*,
        parser::ast::{BinaryOperator, JoinType},
    },
    types::{DataType, DataTypeKind, UInt8},
};

use super::logical::*;
use super::memo::Memo;
use super::physical::*;
use super::prop::RequiredProperties;
use super::{OptimizerError, OptimizerResult};

pub trait Rule {
    fn name(&self) -> &'static str;
    fn promise(&self) -> u32 {
        50
    }
}

pub trait TransformationRule: Rule + Send + Sync {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;
    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>>;
}

pub trait ImplementationRule: Rule + Send + Sync {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;
    fn implement(
        &self,
        expr: &LogicalExpr,
        required: &RequiredProperties,
        memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>>;
}

pub fn default_transformation_rules() -> Vec<Arc<dyn TransformationRule>> {
    vec![
        Arc::new(AggregateProjectMergeRule),
        Arc::new(JoinCommutativityRule),
        Arc::new(JoinAssociativityRule),
        Arc::new(FilterMergeRule),
        Arc::new(FilterPushdownJoinRule),
        Arc::new(FilterPushdownProjectRule),
        Arc::new(PredicateSimplificationRule),
        Arc::new(LimitPushdownRule),
        Arc::new(DistinctEliminationRule),
        Arc::new(ProjectMergeRule),
        Arc::new(ProjectRemoveRule),
    ]
}

pub fn default_implementation_rules() -> Vec<Arc<dyn ImplementationRule>> {
    vec![
        Arc::new(TableScanRule),
        Arc::new(IndexScanRule),
        Arc::new(FilterRule),
        Arc::new(ProjectRule),
        Arc::new(JoinRule),
        Arc::new(AggregateRule),
        Arc::new(SortRule),
        Arc::new(MaterializeRule),
        Arc::new(LimitRule),
        Arc::new(DistinctRule),
        Arc::new(SetOperationRule),
        Arc::new(ValuesRule),
        Arc::new(EmptyRule),
        Arc::new(InsertRule),
        Arc::new(UpdateRule),
        Arc::new(DeleteRule),
    ]
}

/// Remap column indices in an expression from old schema to new schema
/// `mapping` maps old_column_idx -> new_column_idx
pub fn remap_expression_columns(
    expr: &BoundExpression,
    mapping: &HashMap<usize, usize>,
) -> Option<BoundExpression> {
    match expr {
        BoundExpression::ColumnRef(col_ref) => {
            let new_idx = mapping.get(&col_ref.column_idx)?;
            Some(BoundExpression::ColumnRef(BoundColumnRef {
                column_idx: *new_idx,
                ..*col_ref
            }))
        }
        BoundExpression::Literal { .. } => Some(expr.clone()),
        BoundExpression::BinaryOp {
            left,
            op,
            right,
            result_type,
        } => Some(BoundExpression::BinaryOp {
            left: Box::new(remap_expression_columns(left, mapping)?),
            op: *op,
            right: Box::new(remap_expression_columns(right, mapping)?),
            result_type: *result_type,
        }),
        BoundExpression::UnaryOp {
            op,
            expr: inner,
            result_type,
        } => Some(BoundExpression::UnaryOp {
            op: *op,
            expr: Box::new(remap_expression_columns(inner, mapping)?),
            result_type: *result_type,
        }),
        BoundExpression::IsNull {
            expr: inner,
            negated,
        } => Some(BoundExpression::IsNull {
            expr: Box::new(remap_expression_columns(inner, mapping)?),
            negated: *negated,
        }),
        BoundExpression::InList {
            expr,
            list,
            negated,
        } => Some(BoundExpression::InList {
            expr: Box::new(remap_expression_columns(expr, mapping)?),
            list: list
                .iter()
                .map(|e| remap_expression_columns(e, mapping))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
        }),
        BoundExpression::Between {
            expr,
            low,
            high,
            negated,
        } => Some(BoundExpression::Between {
            expr: Box::new(remap_expression_columns(expr, mapping)?),
            low: Box::new(remap_expression_columns(low, mapping)?),
            high: Box::new(remap_expression_columns(high, mapping)?),
            negated: *negated,
        }),
        // Handle other cases as needed
        _ => Some(expr.clone()),
    }
}

/// Collect all column indices referenced in an expression
pub fn collect_column_indices(expr: &BoundExpression, indices: &mut HashSet<usize>) {
    match expr {
        BoundExpression::ColumnRef(col_ref) => {
            indices.insert(col_ref.column_idx);
        }
        BoundExpression::BinaryOp { left, right, .. } => {
            collect_column_indices(left, indices);
            collect_column_indices(right, indices);
        }
        BoundExpression::UnaryOp { expr, .. } => {
            collect_column_indices(expr, indices);
        }
        BoundExpression::IsNull { expr, .. } => {
            collect_column_indices(expr, indices);
        }
        BoundExpression::InList { expr, list, .. } => {
            collect_column_indices(expr, indices);
            for e in list {
                collect_column_indices(e, indices);
            }
        }
        BoundExpression::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, indices);
            collect_column_indices(low, indices);
            collect_column_indices(high, indices);
        }
        _ => {}
    }
}

pub struct ProjectionPushdownRule;

impl Rule for ProjectionPushdownRule {
    fn name(&self) -> &'static str {
        "ProjectionPushdown"
    }
    fn promise(&self) -> u32 {
        80
    }
}

impl TransformationRule for ProjectionPushdownRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Matches a pattern of project followed by seq scan , as long as the seq scan does not include a projection already.
        if let LogicalOperator::Project(project) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        if let LogicalOperator::TableScan(scan) = &child_expr.op {
                            // Only push down if scan doesn't already have projection
                            return scan.columns.is_none();
                        }
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Project(project) = &expr.op else {
            return Ok(vec![]);
        };

        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::TableScan(scan) = &child_expr.op else {
            return Ok(vec![]);
        };

        // Collect all columns needed by the projection
        let mut needed_columns = HashSet::new();
        for proj_expr in &project.expressions {
            collect_column_indices(&proj_expr.expr, &mut needed_columns);
        }

        // Sort column indices for consistent output
        let mut columns: Vec<usize> = needed_columns.into_iter().collect();
        columns.sort();

        // If we need all columns, no benefit
        if columns.len() >= scan.schema.columns().len() {
            return Ok(vec![]);
        }

        // Create the mapping mapping: old_idx to new_idx
        let mapping: HashMap<usize, usize> = columns
            .iter()
            .enumerate()
            .map(|(new_idx, &old_idx)| (old_idx, new_idx))
            .collect();

        // Rewrite project expressions with new indices
        let new_expressions: Vec<ProjectExpr> = project
            .expressions
            .iter()
            .map(|pe| ProjectExpr {
                expr: remap_expression_columns(&pe.expr, &mapping)
                    .unwrap_or_else(|| pe.expr.clone()),
                alias: pe.alias.clone(),
            })
            .collect();

        // Build new scan with projection
        let new_scan =
            TableScanOp::new(scan.table_id, scan.table_name.clone(), scan.schema.clone())
                .with_columns(columns);

        // Build new input schema (projected schema)
        let new_input_schema = new_scan.schema.clone(); // After with_columns, this is projected

        // Insert new scan
        let scan_group = memo.insert_logical_expr(LogicalExpr::new(
            LogicalOperator::TableScan(new_scan),
            vec![],
        ));

        // Build new project
        let new_project = ProjectOp::new(
            new_expressions,
            new_input_schema,
            project.output_schema.clone(),
        );

        Ok(vec![
            LogicalExpr::new(LogicalOperator::Project(new_project), vec![scan_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct JoinCommutativityRule;

impl Rule for JoinCommutativityRule {
    fn name(&self) -> &'static str {
        "JoinCommutativity"
    }
    fn promise(&self) -> u32 {
        60
    }
}

impl TransformationRule for JoinCommutativityRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(
            &expr.op,
            LogicalOperator::Join(j) if j.join_type == JoinType::Inner || j.join_type == JoinType::Cross
        )
    }

    fn apply(&self, expr: &LogicalExpr, _memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Join(join) = &expr.op else {
            return Ok(vec![]);
        };
        if expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let swapped_condition = join.condition.as_ref().map(swap_join_condition);
        let new_join = JoinOp::new(
            join.join_type,
            swapped_condition,
            join.right_schema.clone(),
            join.left_schema.clone(),
        );

        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Join(new_join),
                vec![expr.children[1], expr.children[0]],
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct JoinAssociativityRule;

impl Rule for JoinAssociativityRule {
    fn name(&self) -> &'static str {
        "JoinAssociativity"
    }
    fn promise(&self) -> u32 {
        70
    }
}

impl TransformationRule for JoinAssociativityRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        let LogicalOperator::Join(outer_join) = &expr.op else {
            return false;
        };
        if outer_join.join_type != JoinType::Inner || expr.children.len() != 2 {
            return false;
        }

        let left_group = expr.children[0];
        if let Some(left) = memo.get_group(left_group) {
            if let Some(left_expr) = left.logical_exprs.first() {
                if let LogicalOperator::Join(inner_join) = &left_expr.op {
                    return inner_join.join_type == JoinType::Inner;
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Join(outer_join) = &expr.op else {
            return Ok(vec![]);
        };
        let left_group = expr.children[0];
        let c_group = expr.children[1];

        let left = memo
            .get_group(left_group)
            .ok_or_else(|| OptimizerError::InvalidState("Left group not found".into()))?;
        let left_expr = left
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Join(inner_join) = &left_expr.op else {
            return Ok(vec![]);
        };
        if left_expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let a_group = left_expr.children[0];
        let b_group = left_expr.children[1];

        let a_props = memo
            .get_group(a_group)
            .map(|g| g.logical_props.clone())
            .unwrap_or_default();
        let b_props = memo
            .get_group(b_group)
            .map(|g| g.logical_props.clone())
            .unwrap_or_default();
        let c_props = memo
            .get_group(c_group)
            .map(|g| g.logical_props.clone())
            .unwrap_or_default();

        let bc_condition = extract_bc_condition(
            &outer_join.condition,
            &inner_join.condition,
            a_props.schema.columns().len(),
            b_props.schema.columns().len(),
        );
        let bc_join = JoinOp::new(
            JoinType::Inner,
            bc_condition,
            b_props.schema.clone(),
            c_props.schema.clone(),
        );

        let a_bc_condition = extract_a_condition(
            &outer_join.condition,
            &inner_join.condition,
            a_props.schema.columns().len(),
        );
        let bc_expr = LogicalExpr::new(LogicalOperator::Join(bc_join), vec![b_group, c_group]);
        let bc_group = memo.insert_logical_expr(bc_expr);
        let bc_schema = memo
            .get_group(bc_group)
            .map(|g| g.logical_props.schema.clone())
            .unwrap_or_else(Schema::new);
        let a_bc_join = JoinOp::new(
            JoinType::Inner,
            a_bc_condition,
            a_props.schema.clone(),
            bc_schema,
        );

        Ok(vec![
            LogicalExpr::new(LogicalOperator::Join(a_bc_join), vec![a_group, bc_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct FilterMergeRule;

impl Rule for FilterMergeRule {
    fn name(&self) -> &'static str {
        "FilterMerge"
    }
    fn promise(&self) -> u32 {
        80
    }
}

impl TransformationRule for FilterMergeRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Filter(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(&child_expr.op, LogicalOperator::Filter(_));
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(outer_filter) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(*child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Filter(inner_filter) = &child_expr.op else {
            return Ok(vec![]);
        };

        let merged = BoundExpression::BinaryOp {
            left: Box::new(outer_filter.predicate.clone()),
            op: BinaryOperator::And,
            right: Box::new(inner_filter.predicate.clone()),
            result_type: DataTypeKind::Boolean,
        };
        let new_filter = FilterOp::new(merged, inner_filter.input_schema.clone());
        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Filter(new_filter),
                child_expr.children.clone(),
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct FilterPushdownJoinRule;

impl Rule for FilterPushdownJoinRule {
    fn name(&self) -> &'static str {
        "FilterPushdownJoin"
    }
    fn promise(&self) -> u32 {
        90
    }
}

/// The goal of this rule is to push predicates below a join so that they run as soon as possible.
///
/// It transforms trees like:
///
/// ```text
///
/// Filter(predicate)
///  |
///  Join(left, right)
///
/// ```
/// Into something like:
///
/// ```text
///          Join(new_condition)
///         /                   \
/// Filter(left_pred)     Filter(right_pred)
///        |                      |
///      Left                   Right
///
/// ```
impl TransformationRule for FilterPushdownJoinRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Filter(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(&child_expr.op, LogicalOperator::Join(_));
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Join(join) = &child_expr.op else {
            return Ok(vec![]);
        };

        if join.join_type != JoinType::Inner && join.join_type != JoinType::Cross {
            return Ok(vec![]);
        }
        if child_expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let left_group = child_expr.children[0];
        let right_group = child_expr.children[1];
        let left_cols = join.left_schema.columns().len();

        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut join_preds = Vec::new();
        classify_predicates(
            &filter.predicate,
            left_cols,
            &mut left_preds,
            &mut right_preds,
            &mut join_preds,
        );
        let is_left_empty = left_preds.is_empty();
        let is_right_empty = right_preds.is_empty();

        if is_left_empty && is_right_empty {
            return Ok(vec![]);
        }

        let new_cond =
            combine_predicates(join.condition.iter().cloned().chain(join_preds).collect());
        let left_schema = join.left_schema.clone();
        let right_schema = join.right_schema.clone();
        let join_type = join.join_type;

        let new_left = if !is_left_empty {
            let pred = combine_predicates(left_preds).unwrap();
            let f = FilterOp::new(pred, join.left_schema.clone());
            memo.insert_logical_expr(LogicalExpr::new(
                LogicalOperator::Filter(f),
                vec![left_group],
            ))
        } else {
            left_group
        };

        let new_right = if !is_right_empty {
            let shifted: Vec<_> = right_preds
                .into_iter()
                .filter_map(|p| shift_columns(&p, -(left_cols as i32)))
                .collect();
            if let Some(pred) = combine_predicates(shifted) {
                let f = FilterOp::new(pred, right_schema.clone());
                memo.insert_logical_expr(LogicalExpr::new(
                    LogicalOperator::Filter(f),
                    vec![right_group],
                ))
            } else {
                right_group
            }
        } else {
            right_group
        };

        let new_join = JoinOp::new(join_type, new_cond, left_schema, right_schema);
        Ok(vec![
            LogicalExpr::new(LogicalOperator::Join(new_join), vec![new_left, new_right])
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct FilterPushdownProjectRule;

impl Rule for FilterPushdownProjectRule {
    fn name(&self) -> &'static str {
        "FilterPushdownProject"
    }
    fn promise(&self) -> u32 {
        85
    }
}

impl TransformationRule for FilterPushdownProjectRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Filter(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        if let LogicalOperator::Project(proj) = &child_expr.op {
                            return proj
                                .expressions
                                .iter()
                                .all(|e| matches!(&e.expr, BoundExpression::ColumnRef(_)));
                        }
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Project(project) = &child_expr.op else {
            return Ok(vec![]);
        };

        let mut mapping = Vec::new();
        for proj_expr in &project.expressions {
            if let BoundExpression::ColumnRef(col_ref) = &proj_expr.expr {
                mapping.push(col_ref.column_idx);
            } else {
                return Ok(vec![]);
            }
        }

        let new_pred = rewrite_with_mapping(&filter.predicate, &mapping);
        let new_filter = FilterOp::new(new_pred, project.input_schema.clone());

        let input_schema = project.input_schema.clone();
        let expressions = project.expressions.clone();
        let output_schema = project.output_schema.clone();
        let filter_group = memo.insert_logical_expr(LogicalExpr::new(
            LogicalOperator::Filter(new_filter),
            child_expr.children.clone(),
        ));
        let new_project = ProjectOp::new(expressions, input_schema, output_schema);
        Ok(vec![
            LogicalExpr::new(LogicalOperator::Project(new_project), vec![filter_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct PredicateSimplificationRule;

impl Rule for PredicateSimplificationRule {
    fn name(&self) -> &'static str {
        "PredicateSimplification"
    }
    fn promise(&self) -> u32 {
        40
    }
}

impl TransformationRule for PredicateSimplificationRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        if let LogicalOperator::Filter(filter) = &expr.op {
            return can_simplify(&filter.predicate);
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, _memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        let simplified = simplify_predicate(&filter.predicate);
        if predicate_equals(&simplified, &filter.predicate) || is_true_literal(&simplified) {
            return Ok(vec![]);
        }
        let new_filter = FilterOp::new(simplified, filter.input_schema.clone());
        Ok(vec![
            LogicalExpr::new(LogicalOperator::Filter(new_filter), expr.children.clone())
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct LimitPushdownRule;

impl Rule for LimitPushdownRule {
    fn name(&self) -> &'static str {
        "LimitPushdown"
    }
    fn promise(&self) -> u32 {
        75
    }
}

impl TransformationRule for LimitPushdownRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Limit(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(&child_expr.op, LogicalOperator::Project(_));
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Limit(limit) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Project(project) = &child_expr.op else {
            return Ok(vec![]);
        };

        let new_limit = LimitOp::new(limit.limit, limit.offset, project.input_schema.clone());
        let expressions = project.expressions.clone();
        let input_schema = project.input_schema.clone();
        let output_schema = project.output_schema.clone();

        let limit_group = memo.insert_logical_expr(LogicalExpr::new(
            LogicalOperator::Limit(new_limit),
            child_expr.children.clone(),
        ));
        let new_project = ProjectOp::new(expressions, input_schema, output_schema);
        Ok(vec![
            LogicalExpr::new(LogicalOperator::Project(new_project), vec![limit_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct DistinctEliminationRule;

impl Rule for DistinctEliminationRule {
    fn name(&self) -> &'static str {
        "DistinctElimination"
    }
    fn promise(&self) -> u32 {
        95
    }
}

impl TransformationRule for DistinctEliminationRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Distinct(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    return child.logical_props.unique;
                }
            }
        }
        false
    }

    fn apply(&self, _expr: &LogicalExpr, _memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        Ok(vec![])
    }
}

pub struct ProjectMergeRule;

impl Rule for ProjectMergeRule {
    fn name(&self) -> &'static str {
        "ProjectMerge"
    }
    fn promise(&self) -> u32 {
        70
    }
}

impl TransformationRule for ProjectMergeRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        if let LogicalOperator::Project(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(&child_expr.op, LogicalOperator::Project(_));
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Project(outer) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Project(inner) = &child_expr.op else {
            return Ok(vec![]);
        };

        let composed: Vec<ProjectExpr> = outer
            .expressions
            .iter()
            .map(|e| ProjectExpr {
                expr: compose_expressions(&e.expr, &inner.expressions),
                alias: e.alias.clone(),
            })
            .collect();
        let new_project = ProjectOp::new(
            composed,
            inner.input_schema.clone(),
            outer.output_schema.clone(),
        );
        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Project(new_project),
                child_expr.children.clone(),
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

pub struct ProjectRemoveRule;

impl Rule for ProjectRemoveRule {
    fn name(&self) -> &'static str {
        "ProjectRemove"
    }
    fn promise(&self) -> u32 {
        65
    }
}

impl TransformationRule for ProjectRemoveRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        if let LogicalOperator::Project(project) = &expr.op {
            return is_identity_project(project);
        }
        false
    }

    fn apply(&self, _expr: &LogicalExpr, _memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        Ok(vec![])
    }
}

fn swap_join_condition(cond: &BoundExpression) -> BoundExpression {
    match cond {
        BoundExpression::BinaryOp {
            left,
            op,
            right,
            result_type,
        } => {
            let swapped_op = match op {
                BinaryOperator::Eq => BinaryOperator::Eq,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::Le => BinaryOperator::Ge,
                BinaryOperator::Ge => BinaryOperator::Le,
                BinaryOperator::And => {
                    return BoundExpression::BinaryOp {
                        left: Box::new(swap_join_condition(left)),
                        op: BinaryOperator::And,
                        right: Box::new(swap_join_condition(right)),
                        result_type: *result_type,
                    };
                }
                BinaryOperator::Or => {
                    return BoundExpression::BinaryOp {
                        left: Box::new(swap_join_condition(left)),
                        op: BinaryOperator::Or,
                        right: Box::new(swap_join_condition(right)),
                        result_type: *result_type,
                    };
                }
                _ => *op,
            };
            BoundExpression::BinaryOp {
                left: right.clone(),
                op: swapped_op,
                right: left.clone(),
                result_type: *result_type,
            }
        }
        _ => cond.clone(),
    }
}

fn extract_bc_condition(
    outer: &Option<BoundExpression>,
    inner: &Option<BoundExpression>,
    a_cols: usize,
    b_cols: usize,
) -> Option<BoundExpression> {
    let mut preds = Vec::new();
    if let Some(c) = outer {
        collect_predicates_for_range(c, a_cols, &mut preds);
    }
    if let Some(c) = inner {
        if let Some(s) = shift_condition_for_bc(c, a_cols, b_cols) {
            preds.push(s);
        }
    }
    combine_predicates(preds)
}

fn extract_a_condition(
    outer: &Option<BoundExpression>,
    inner: &Option<BoundExpression>,
    a_cols: usize,
) -> Option<BoundExpression> {
    let mut preds = Vec::new();
    if let Some(c) = inner {
        collect_predicates_involving_range(c, 0, a_cols, &mut preds);
    }
    if let Some(c) = outer {
        collect_predicates_involving_range(c, 0, a_cols, &mut preds);
    }
    combine_predicates(preds)
}

fn collect_predicates_for_range(
    expr: &BoundExpression,
    offset: usize,
    out: &mut Vec<BoundExpression>,
) {
    match expr {
        BoundExpression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
            ..
        } => {
            collect_predicates_for_range(left, offset, out);
            collect_predicates_for_range(right, offset, out);
        }
        _ => {
            if all_columns_ge(expr, offset) {
                if let Some(s) = shift_columns(expr, -(offset as i32)) {
                    out.push(s);
                }
            }
        }
    }
}

fn collect_predicates_involving_range(
    expr: &BoundExpression,
    start: usize,
    end: usize,
    out: &mut Vec<BoundExpression>,
) {
    match expr {
        BoundExpression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
            ..
        } => {
            collect_predicates_involving_range(left, start, end, out);
            collect_predicates_involving_range(right, start, end, out);
        }
        _ => {
            if any_column_in_range(expr, start, end) {
                out.push(expr.clone());
            }
        }
    }
}

fn shift_condition_for_bc(
    expr: &BoundExpression,
    a_cols: usize,
    _b_cols: usize,
) -> Option<BoundExpression> {
    shift_columns(expr, -(a_cols as i32))
}

fn shift_columns(expr: &BoundExpression, offset: i32) -> Option<BoundExpression> {
    match expr {
        BoundExpression::ColumnRef(c) => {
            let n = c.column_idx as i32 + offset;
            if n < 0 {
                None
            } else {
                Some(BoundExpression::ColumnRef(BoundColumnRef {
                    column_idx: n as usize,
                    ..*c
                }))
            }
        }
        BoundExpression::BinaryOp {
            left,
            op,
            right,
            result_type,
        } => Some(BoundExpression::BinaryOp {
            left: Box::new(shift_columns(left, offset)?),
            op: *op,
            right: Box::new(shift_columns(right, offset)?),
            result_type: *result_type,
        }),
        BoundExpression::Literal { .. } => Some(expr.clone()),
        _ => Some(expr.clone()),
    }
}

fn all_columns_ge(expr: &BoundExpression, min: usize) -> bool {
    match expr {
        BoundExpression::ColumnRef(c) => c.column_idx >= min,
        BoundExpression::BinaryOp { left, right, .. } => {
            all_columns_ge(left, min) && all_columns_ge(right, min)
        }
        BoundExpression::Literal { .. } => true,
        _ => true,
    }
}

fn any_column_in_range(expr: &BoundExpression, start: usize, end: usize) -> bool {
    match expr {
        BoundExpression::ColumnRef(c) => c.column_idx >= start && c.column_idx < end,
        BoundExpression::BinaryOp { left, right, .. } => {
            any_column_in_range(left, start, end) || any_column_in_range(right, start, end)
        }
        _ => false,
    }
}

fn combine_predicates(preds: Vec<BoundExpression>) -> Option<BoundExpression> {
    preds.into_iter().reduce(|a, b| BoundExpression::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
        result_type: DataTypeKind::Boolean,
    })
}

fn classify_predicates(
    expr: &BoundExpression,
    left_cols: usize,
    left: &mut Vec<BoundExpression>,
    right: &mut Vec<BoundExpression>,
    join: &mut Vec<BoundExpression>,
) {
    match expr {
        BoundExpression::BinaryOp {
            op: BinaryOperator::And,
            left: l,
            right: r,
            ..
        } => {
            classify_predicates(l, left_cols, left, right, join);
            classify_predicates(r, left_cols, left, right, join);
        }
        _ => {
            let (ul, ur) = check_column_usage(expr, left_cols);
            if ul && !ur {
                left.push(expr.clone());
            } else if ur && !ul {
                right.push(expr.clone());
            } else {
                join.push(expr.clone());
            }
        }
    }
}

fn check_column_usage(expr: &BoundExpression, left_cols: usize) -> (bool, bool) {
    match expr {
        BoundExpression::ColumnRef(c) => {
            if c.column_idx < left_cols {
                (true, false)
            } else {
                (false, true)
            }
        }
        BoundExpression::BinaryOp { left, right, .. } => {
            let (l1, r1) = check_column_usage(left, left_cols);
            let (l2, r2) = check_column_usage(right, left_cols);
            (l1 || l2, r1 || r2)
        }
        BoundExpression::UnaryOp { expr, .. } | BoundExpression::IsNull { expr, .. } => {
            check_column_usage(expr, left_cols)
        }
        BoundExpression::InList { expr, list, .. } => {
            let (mut l, mut r) = check_column_usage(expr, left_cols);
            for i in list {
                let (l2, r2) = check_column_usage(i, left_cols);
                l = l || l2;
                r = r || r2;
            }
            (l, r)
        }
        BoundExpression::Between {
            expr, low, high, ..
        } => {
            let (l1, r1) = check_column_usage(expr, left_cols);
            let (l2, r2) = check_column_usage(low, left_cols);
            let (l3, r3) = check_column_usage(high, left_cols);
            (l1 || l2 || l3, r1 || r2 || r3)
        }
        _ => (false, false),
    }
}

fn rewrite_with_mapping(expr: &BoundExpression, mapping: &[usize]) -> BoundExpression {
    match expr {
        BoundExpression::ColumnRef(c) => BoundExpression::ColumnRef(BoundColumnRef {
            column_idx: mapping.get(c.column_idx).copied().unwrap_or(c.column_idx),
            ..*c
        }),
        BoundExpression::BinaryOp {
            left,
            op,
            right,
            result_type,
        } => BoundExpression::BinaryOp {
            left: Box::new(rewrite_with_mapping(left, mapping)),
            op: *op,
            right: Box::new(rewrite_with_mapping(right, mapping)),
            result_type: *result_type,
        },
        BoundExpression::UnaryOp {
            op,
            expr,
            result_type,
        } => BoundExpression::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_with_mapping(expr, mapping)),
            result_type: *result_type,
        },
        BoundExpression::IsNull { expr, negated } => BoundExpression::IsNull {
            expr: Box::new(rewrite_with_mapping(expr, mapping)),
            negated: *negated,
        },
        BoundExpression::InList {
            expr,
            list,
            negated,
        } => BoundExpression::InList {
            expr: Box::new(rewrite_with_mapping(expr, mapping)),
            list: list
                .iter()
                .map(|e| rewrite_with_mapping(e, mapping))
                .collect(),
            negated: *negated,
        },
        BoundExpression::Between {
            expr,
            low,
            high,
            negated,
        } => BoundExpression::Between {
            expr: Box::new(rewrite_with_mapping(expr, mapping)),
            low: Box::new(rewrite_with_mapping(low, mapping)),
            high: Box::new(rewrite_with_mapping(high, mapping)),
            negated: *negated,
        },
        _ => expr.clone(),
    }
}

fn can_simplify(expr: &BoundExpression) -> bool {
    match expr {
        BoundExpression::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            left,
            right,
            ..
        } => {
            is_bool_literal(left)
                || is_bool_literal(right)
                || can_simplify(left)
                || can_simplify(right)
        }
        _ => false,
    }
}

fn simplify_predicate(expr: &BoundExpression) -> BoundExpression {
    match expr {
        BoundExpression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
            result_type,
        } => {
            let l = simplify_predicate(left);
            let r = simplify_predicate(right);
            if is_true_literal(&l) {
                r
            } else if is_true_literal(&r) {
                l
            } else if is_false_literal(&l) || is_false_literal(&r) {
                make_bool_literal(false)
            } else {
                BoundExpression::BinaryOp {
                    left: Box::new(l),
                    op: BinaryOperator::And,
                    right: Box::new(r),
                    result_type: *result_type,
                }
            }
        }
        BoundExpression::BinaryOp {
            op: BinaryOperator::Or,
            left,
            right,
            result_type,
        } => {
            let l = simplify_predicate(left);
            let r = simplify_predicate(right);
            if is_true_literal(&l) || is_true_literal(&r) {
                make_bool_literal(true)
            } else if is_false_literal(&l) {
                r
            } else if is_false_literal(&r) {
                l
            } else {
                BoundExpression::BinaryOp {
                    left: Box::new(l),
                    op: BinaryOperator::Or,
                    right: Box::new(r),
                    result_type: *result_type,
                }
            }
        }
        _ => expr.clone(),
    }
}

fn is_bool_literal(expr: &BoundExpression) -> bool {
    matches!(expr, BoundExpression::Literal { value } if matches!(value, DataType::Boolean(_)))
}
fn is_true_literal(expr: &BoundExpression) -> bool {
    matches!(expr, BoundExpression::Literal { value: DataType::Boolean(b) } if b.0 != 0)
}
fn is_false_literal(expr: &BoundExpression) -> bool {
    matches!(expr, BoundExpression::Literal { value: DataType::Boolean(b) } if b.0 == 0)
}
fn make_bool_literal(val: bool) -> BoundExpression {
    BoundExpression::Literal {
        value: DataType::Boolean(UInt8::from(val)),
    }
}

fn predicate_equals(a: &BoundExpression, b: &BoundExpression) -> bool {
    match (a, b) {
        (BoundExpression::Literal { value: v1 }, BoundExpression::Literal { value: v2 }) => {
            std::mem::discriminant(v1) == std::mem::discriminant(v2)
        }
        (BoundExpression::ColumnRef(c1), BoundExpression::ColumnRef(c2)) => {
            c1.column_idx == c2.column_idx
        }
        (
            BoundExpression::BinaryOp {
                left: l1,
                op: o1,
                right: r1,
                ..
            },
            BoundExpression::BinaryOp {
                left: l2,
                op: o2,
                right: r2,
                ..
            },
        ) => o1 == o2 && predicate_equals(l1, l2) && predicate_equals(r1, r2),
        _ => false,
    }
}

fn compose_expressions(outer: &BoundExpression, inner: &[ProjectExpr]) -> BoundExpression {
    match outer {
        BoundExpression::ColumnRef(c) => inner
            .get(c.column_idx)
            .map(|e| e.expr.clone())
            .unwrap_or_else(|| outer.clone()),
        BoundExpression::BinaryOp {
            left,
            op,
            right,
            result_type,
        } => BoundExpression::BinaryOp {
            left: Box::new(compose_expressions(left, inner)),
            op: *op,
            right: Box::new(compose_expressions(right, inner)),
            result_type: *result_type,
        },
        BoundExpression::UnaryOp {
            op,
            expr,
            result_type,
        } => BoundExpression::UnaryOp {
            op: *op,
            expr: Box::new(compose_expressions(expr, inner)),
            result_type: *result_type,
        },
        _ => outer.clone(),
    }
}

fn is_identity_project(project: &ProjectOp) -> bool {
    let in_cols = project.input_schema.columns().len();
    let out_cols = project.expressions.len();
    if in_cols != out_cols {
        return false;
    }
    for (idx, e) in project.expressions.iter().enumerate() {
        match &e.expr {
            BoundExpression::ColumnRef(c) if c.column_idx == idx => continue,
            _ => return false,
        }
    }
    true
}

fn create_column_ref(column_idx: usize, schema: &Schema) -> BoundExpression {
    let dtype = schema
        .columns()
        .get(column_idx)
        .map(|c| c.dtype)
        .unwrap_or(DataTypeKind::Int);
    BoundExpression::ColumnRef(BoundColumnRef {
        table_id: OBJECT_ZERO,
        scope_table_index: 0,
        column_idx,
        data_type: dtype,
    })
}

pub struct TableScanRule;
impl Rule for TableScanRule {
    fn name(&self) -> &'static str {
        "TableScan"
    }
}
impl ImplementationRule for TableScanRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::TableScan(_))
    }
    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::TableScan(scan) = &expr.op else {
            return Ok(vec![]);
        };

        let mut seq_scan = SeqScanOp::new(scan.table_id, scan.table_schema.clone());

        if let Some(cols) = &scan.columns {
            seq_scan = seq_scan.with_columns(cols.clone());
        }

        if let Some(pred) = &scan.predicate {
            seq_scan = seq_scan.with_predicate(pred.clone());
        }

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::SeqScan(seq_scan),
            vec![],
        )])
    }
}

pub struct IndexScanRule;
impl Rule for IndexScanRule {
    fn name(&self) -> &'static str {
        "IndexScan"
    }
}
impl ImplementationRule for IndexScanRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::IndexScan(_))
    }
    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::IndexScan(scan) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::IndexScan(PhysIndexScanOp::from(scan.clone())),
            vec![],
        )])
    }
}

pub struct FilterRule;
impl Rule for FilterRule {
    fn name(&self) -> &'static str {
        "Filter"
    }
}
impl ImplementationRule for FilterRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Filter(_))
    }
    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Filter(PhysFilterOp::new(
                filter.predicate.clone(),
                filter.input_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct ProjectRule;
impl Rule for ProjectRule {
    fn name(&self) -> &'static str {
        "Project"
    }
}
impl ImplementationRule for ProjectRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Project(_))
    }
    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Project(project) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Project(PhysProjectOp::new(
                project.expressions.clone(),
                project.input_schema.clone(),
                project.output_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct JoinRule;

impl Rule for JoinRule {
    fn name(&self) -> &'static str {
        "Join"
    }
}

impl ImplementationRule for JoinRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Join(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Join(join) = &expr.op else {
            return Ok(vec![]);
        };

        let mut implementations = Vec::new();

        let nl_join = NestedLoopJoinOp::new(
            join.join_type,
            join.condition.clone(),
            join.output_schema.clone(),
        );
        implementations.push(PhysicalExpr::new(
            PhysicalOperator::NestedLoopJoin(nl_join),
            expr.children.clone(),
        ));

        if join.is_equi_join() {
            let keys = join.extract_equi_keys();
            if !keys.is_empty() {
                let (left_keys, right_keys): (Vec<_>, Vec<_>) = keys
                    .iter()
                    .map(|(l, r)| {
                        (
                            create_column_ref(*l, &join.left_schema),
                            create_column_ref(*r, &join.right_schema),
                        )
                    })
                    .unzip();

                let residual = join.condition.as_ref().and_then(|c| c.extract_non_equi());

                let hash_join = HashJoinOp::new(
                    join.join_type,
                    left_keys.clone(),
                    right_keys.clone(),
                    residual,
                    join.output_schema.clone(),
                );
                implementations.push(PhysicalExpr::new(
                    PhysicalOperator::HashJoin(hash_join),
                    expr.children.clone(),
                ));

                let left_key_indices: Vec<usize> = keys.iter().map(|(l, _)| *l).collect();
                let right_key_indices: Vec<usize> = keys.iter().map(|(_, r)| *r).collect();

                let left_ordering: Vec<OrderingSpec> = left_key_indices
                    .iter()
                    .map(|&col| OrderingSpec::asc(col))
                    .collect();
                let right_ordering: Vec<OrderingSpec> = right_key_indices
                    .iter()
                    .map(|&col| OrderingSpec::asc(col))
                    .collect();

                let output_ordering: Vec<(BoundExpression, bool)> =
                    left_keys.iter().map(|e| (e.clone(), true)).collect();

                let merge_join = MergeJoinOp {
                    join_type: join.join_type,
                    left_keys,
                    right_keys,
                    left_ordering: left_ordering.clone(),
                    right_ordering: right_ordering.clone(),
                    output_ordering: output_ordering.clone(),
                    output_schema: join.output_schema.clone(),
                };

                let mut merge_expr = PhysicalExpr::new(
                    PhysicalOperator::MergeJoin(merge_join),
                    expr.children.clone(),
                );
                merge_expr.properties = super::prop::PhysicalProperties {
                    ordering: output_ordering,
                    ..Default::default()
                };
                implementations.push(merge_expr);
            }
        }

        Ok(implementations)
    }
}

pub struct AggregateRule;

impl Rule for AggregateRule {
    fn name(&self) -> &'static str {
        "Aggregate"
    }
}

impl ImplementationRule for AggregateRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Aggregate(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Aggregate(agg) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::HashAggregate(HashAggregateOp::new(
                agg.group_by.clone(),
                agg.aggregates.clone(),
                agg.input_schema.clone(),
                agg.output_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct SortRule;

impl Rule for SortRule {
    fn name(&self) -> &'static str {
        "Sort"
    }
}

impl ImplementationRule for SortRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Sort(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Sort(sort) = &expr.op else {
            return Ok(vec![]);
        };

        let mut phys_expr = PhysicalExpr::new(
            PhysicalOperator::ExternalSort(ExternalSortOp::new(
                sort.order_by.clone(),
                sort.schema.clone(),
            )),
            expr.children.clone(),
        );

        phys_expr.properties = super::prop::PhysicalProperties {
            ordering: sort
                .order_by
                .iter()
                .map(|s| (s.expr.clone(), s.asc))
                .collect(),
            ..Default::default()
        };

        Ok(vec![phys_expr])
    }
}

pub struct LimitRule;

impl Rule for LimitRule {
    fn name(&self) -> &'static str {
        "Limit"
    }
}

impl ImplementationRule for LimitRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Limit(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Limit(limit) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Limit(PhysLimitOp::new(
                limit.limit,
                limit.offset,
                limit.schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct DistinctRule;

impl Rule for DistinctRule {
    fn name(&self) -> &'static str {
        "Distinct"
    }
}

impl ImplementationRule for DistinctRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Distinct(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Distinct(distinct) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![
            PhysicalExpr::new(
                PhysicalOperator::HashDistinct(HashDistinctOp {
                    schema: distinct.schema.clone(),
                }),
                expr.children.clone(),
            ),
            PhysicalExpr::new(
                PhysicalOperator::SortDistinct(SortDistinctOp {
                    schema: distinct.schema.clone(),
                }),
                expr.children.clone(),
            ),
        ])
    }
}

pub struct SetOperationRule;

impl Rule for SetOperationRule {
    fn name(&self) -> &'static str {
        "SetOperation"
    }
}

impl ImplementationRule for SetOperationRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(
            &expr.op,
            LogicalOperator::Union(_) | LogicalOperator::Intersect(_) | LogicalOperator::Except(_)
        )
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        match &expr.op {
            LogicalOperator::Union(union_op) => Ok(vec![PhysicalExpr::new(
                PhysicalOperator::HashUnion(HashUnionOp {
                    all: union_op.all,
                    schema: union_op.schema.clone(),
                }),
                expr.children.clone(),
            )]),
            LogicalOperator::Intersect(intersect_op) => Ok(vec![PhysicalExpr::new(
                PhysicalOperator::HashIntersect(HashIntersectOp {
                    all: intersect_op.all,
                    schema: intersect_op.schema.clone(),
                }),
                expr.children.clone(),
            )]),
            LogicalOperator::Except(except_op) => Ok(vec![PhysicalExpr::new(
                PhysicalOperator::HashExcept(HashExceptOp {
                    all: except_op.all,
                    schema: except_op.schema.clone(),
                }),
                expr.children.clone(),
            )]),
            _ => Ok(vec![]),
        }
    }
}

pub struct ValuesRule;

impl Rule for ValuesRule {
    fn name(&self) -> &'static str {
        "Values"
    }
}

impl ImplementationRule for ValuesRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Values(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Values(values) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Values(PhysValuesOp::new(
                values.rows.clone(),
                values.schema.clone(),
            )),
            vec![],
        )])
    }
}

pub struct EmptyRule;

impl Rule for EmptyRule {
    fn name(&self) -> &'static str {
        "Empty"
    }
}

impl ImplementationRule for EmptyRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Empty(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Empty(empty) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Empty(PhysEmptyOp::new(empty.schema.clone())),
            vec![],
        )])
    }
}

pub struct InsertRule;

impl Rule for InsertRule {
    fn name(&self) -> &'static str {
        "Insert"
    }
}

impl ImplementationRule for InsertRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Insert(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Insert(insert) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Insert(PhysInsertOp::new(
                insert.table_id,
                insert.columns.clone(),
                insert.table_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct UpdateRule;

impl Rule for UpdateRule {
    fn name(&self) -> &'static str {
        "Update"
    }
}

impl ImplementationRule for UpdateRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Update(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Update(update) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Update(PhysUpdateOp::new(
                update.table_id,
                update.assignments.clone(),
                update.table_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct DeleteRule;

impl Rule for DeleteRule {
    fn name(&self) -> &'static str {
        "Delete"
    }
}

impl ImplementationRule for DeleteRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Delete(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Delete(delete) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Delete(PhysDeleteOp::new(
                delete.table_id,
                delete.table_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

pub struct MaterializeRule;

impl Rule for MaterializeRule {
    fn name(&self) -> &'static str {
        "Materialize"
    }
}

impl ImplementationRule for MaterializeRule {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, LogicalOperator::Materialize(_))
    }

    fn implement(
        &self,
        expr: &LogicalExpr,
        _required: &RequiredProperties,
        _memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Materialize(mat) = &expr.op else {
            return Ok(vec![]);
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Materialize(PhysMaterializeOp::new(mat.schema.clone())),
            expr.children.clone(),
        )])
    }
}

/// Rule to merge a Project into an Aggregate when the Project just references
/// aggregate output columns. This avoids having a Project that tries to
/// re-evaluate aggregate expressions.
///
/// Transforms:
/// ```text
/// Project([name, COUNT(*)])
///   └─ Aggregate(group_by=[name], aggregates=[COUNT(*)])
/// ```
/// Into:
/// ```text
/// Project([ColumnRef(0), ColumnRef(1)])  // References aggregate output
///   └─ Aggregate(group_by=[name], aggregates=[COUNT(*)])
/// ```
///
/// Or eliminates the Project entirely if it's an identity projection.
pub struct AggregateProjectMergeRule;

impl Rule for AggregateProjectMergeRule {
    fn name(&self) -> &'static str {
        "AggregateProjectMerge"
    }
    fn promise(&self) -> u32 {
        95 // High priority to run before other rules
    }
}

impl TransformationRule for AggregateProjectMergeRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Match: Project -> Aggregate
        if let LogicalOperator::Project(project) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        if matches!(&child_expr.op, LogicalOperator::Aggregate(_)) {
                            // Only match if project contains aggregate expressions
                            return project
                                .expressions
                                .iter()
                                .any(|e| matches!(&e.expr, BoundExpression::Aggregate { .. }));
                        }
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Project(project) = &expr.op else {
            return Ok(vec![]);
        };

        let child_group = *expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No child".into()))?;
        let child = memo
            .get_group(child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child not found".into()))?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("No expressions".into()))?;
        let LogicalOperator::Aggregate(agg) = &child_expr.op else {
            return Ok(vec![]);
        };

        // Build a mapping: for each project expression, determine what aggregate output column it maps to
        let mut rewritten_exprs = Vec::new();

        for proj_expr in project.expressions.iter() {
            match &proj_expr.expr {
                // Direct column reference to aggregate output - keep as is
                BoundExpression::ColumnRef(_) => {
                    rewritten_exprs.push(proj_expr.clone());
                }
                // Aggregate expression - convert to column reference
                BoundExpression::Aggregate { func, arg, .. } => {
                    // Find which aggregate in the agg.aggregates this matches
                    let agg_idx = agg.aggregates.iter().position(|a| {
                        a.func == *func && Self::args_match(a.arg.as_ref(), arg.as_deref())
                    });

                    if let Some(idx) = agg_idx {
                        // The aggregate output columns come after group_by columns
                        let output_col_idx = agg.group_by.len() + idx;
                        let dtype = agg
                            .output_schema
                            .columns()
                            .get(output_col_idx)
                            .map(|c| c.dtype)
                            .unwrap_or(DataTypeKind::Null);

                        rewritten_exprs.push(ProjectExpr {
                            expr: BoundExpression::ColumnRef(BoundColumnRef {
                                table_id: OBJECT_ZERO,
                                scope_table_index: 0,
                                column_idx: output_col_idx,
                                data_type: dtype,
                            }),
                            alias: proj_expr.alias.clone(),
                        });
                    } else {
                        // Aggregate not found in child - can't rewrite
                        return Ok(vec![]);
                    }
                }
                // Other expressions - check if they reference group_by columns
                other => {
                    // Try to find matching group_by expression
                    let group_idx = agg
                        .group_by
                        .iter()
                        .position(|g| Self::exprs_equal(g, other));

                    if let Some(idx) = group_idx {
                        let dtype = agg
                            .output_schema
                            .columns()
                            .get(idx)
                            .map(|c| c.dtype)
                            .unwrap_or(DataTypeKind::Null);

                        rewritten_exprs.push(ProjectExpr {
                            expr: BoundExpression::ColumnRef(BoundColumnRef {
                                table_id: OBJECT_ZERO,
                                scope_table_index: 0,
                                column_idx: idx,
                                data_type: dtype,
                            }),
                            alias: proj_expr.alias.clone(),
                        });
                    } else {
                        // Can't rewrite this expression - keep as is but this might fail at runtime
                        // A better approach would be to reject this case
                        return Ok(vec![]);
                    }
                }
            }
        }

        // Check if it's an identity projection (just selecting all columns in order)
        let is_identity = rewritten_exprs.len() == agg.output_schema.columns().len()
            && rewritten_exprs.iter().enumerate().all(|(idx, e)| {
                if let BoundExpression::ColumnRef(cr) = &e.expr {
                    cr.column_idx == idx
                } else {
                    false
                }
            });

        // If it's an identity projection, eliminate it by not producing alternatives
        // The optimizer should pick the child directly
        if is_identity {
            return Ok(vec![]);
        }

        // Create new project with rewritten expressions
        let new_project = ProjectOp::new(
            rewritten_exprs,
            agg.output_schema.clone(), // Input is now the aggregate output
            project.output_schema.clone(),
        );

        Ok(vec![
            LogicalExpr::new(LogicalOperator::Project(new_project), vec![child_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

impl AggregateProjectMergeRule {
    /// Check if aggregate arguments match
    fn args_match(agg_arg: Option<&BoundExpression>, proj_arg: Option<&BoundExpression>) -> bool {
        match (agg_arg, proj_arg) {
            (None, None) => true,
            (Some(a), Some(b)) => Self::exprs_equal(a, b),
            _ => false,
        }
    }

    /// Simple expression equality check
    fn exprs_equal(a: &BoundExpression, b: &BoundExpression) -> bool {
        match (a, b) {
            (BoundExpression::Star, BoundExpression::Star) => true,
            (BoundExpression::ColumnRef(ca), BoundExpression::ColumnRef(cb)) => {
                ca.column_idx == cb.column_idx
            }
            (BoundExpression::Literal { value: va }, BoundExpression::Literal { value: vb }) => {
                va == vb
            }
            (
                BoundExpression::BinaryOp {
                    left: la,
                    op: opa,
                    right: ra,
                    ..
                },
                BoundExpression::BinaryOp {
                    left: lb,
                    op: opb,
                    right: rb,
                    ..
                },
            ) => opa == opb && Self::exprs_equal(la, lb) && Self::exprs_equal(ra, rb),
            (
                BoundExpression::UnaryOp {
                    op: opa, expr: ea, ..
                },
                BoundExpression::UnaryOp {
                    op: opb, expr: eb, ..
                },
            ) => opa == opb && Self::exprs_equal(ea, eb),
            _ => false,
        }
    }
}
