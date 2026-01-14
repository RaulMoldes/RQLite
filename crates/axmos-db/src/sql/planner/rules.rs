//! Transformation and implementation rules for the Cascades optimizer.
//!
//! Transformation rules generate logically equivalent plans.
//! Implementation rules convert logical operators to physical operators.

use crate::{
    ObjectId,
    schema::{Schema, base::IndexHandle},
    sql::{
        binder::bounds::*,
        parser::ast::{BinaryOperator, JoinType},
    },
    types::DataTypeKind,
};

use super::{
    PlannerError, PlannerResult, logical::*, memo::Memo, physical::*, prop::RequiredProperties,
};

/// Base trait for all rules.
pub trait Rule {
    fn name(&self) -> &'static str;
    fn promise(&self) -> u32 {
        50
    }
}

/// Transformation rules produce equivalent logical expressions.
pub trait TransformationRule: Rule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;
    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>>;
}

/// Implementation rules produce physical expressions.
pub trait ImplementationRule: Rule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;
    fn implement(
        &self,
        expr: &LogicalExpr,
        required: &RequiredProperties,
        memo: &Memo,
    ) -> PlannerResult<Vec<PhysicalExpr>>;
}

/// Returns all transformation rules.
pub fn transformation_rules() -> Vec<Box<dyn TransformationRule>> {
    vec![
        Box::new(JoinCommutativityRule),
        Box::new(JoinAssociativityRule),
        Box::new(FilterMergeRule),
        Box::new(FilterPushdownJoinRule),
        Box::new(FilterPushdownProjectRule),
        Box::new(FilterToIndexScanRule),
    ]
}

/// Returns all implementation rules.
pub fn implementation_rules() -> Vec<Box<dyn ImplementationRule>> {
    vec![
        Box::new(TableScanRule),
        Box::new(IndexScanRule),
        Box::new(FilterRule),
        Box::new(ProjectRule),
        Box::new(JoinRule),
        Box::new(AggregateRule),
        Box::new(SortRule),
        Box::new(LimitRule),
        Box::new(DistinctRule),
        Box::new(ValuesRule),
        Box::new(EmptyRule),
        Box::new(InsertRule),
        Box::new(UpdateRule),
        Box::new(DeleteRule),
        Box::new(MaterializeRule),
    ]
}

// Helper functions

/// Swaps left/right in a join condition.
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

/// Shifts column indices by offset.
fn shift_columns(expr: &BoundExpression, offset: i32) -> Option<BoundExpression> {
    match expr {
        BoundExpression::ColumnBinding(c) => {
            let new_idx = c.column_idx as i32 + offset;
            if new_idx < 0 {
                None
            } else {
                Some(BoundExpression::ColumnBinding(Binding {
                    column_idx: new_idx as usize,
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

/// Checks if all columns in expr have index >= min.
fn all_columns_ge(expr: &BoundExpression, min: usize) -> bool {
    match expr {
        BoundExpression::ColumnBinding(c) => c.column_idx >= min,
        BoundExpression::BinaryOp { left, right, .. } => {
            all_columns_ge(left, min) && all_columns_ge(right, min)
        }
        BoundExpression::Literal { .. } => true,
        _ => true,
    }
}

/// Checks if any column in expr is in range [start, end).
fn any_column_in_range(expr: &BoundExpression, start: usize, end: usize) -> bool {
    match expr {
        BoundExpression::ColumnBinding(c) => c.column_idx >= start && c.column_idx < end,
        BoundExpression::BinaryOp { left, right, .. } => {
            any_column_in_range(left, start, end) || any_column_in_range(right, start, end)
        }
        _ => false,
    }
}

/// Combines predicates with AND.
fn combine_predicates(preds: Vec<BoundExpression>) -> Option<BoundExpression> {
    preds.into_iter().reduce(|a, b| BoundExpression::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
        result_type: DataTypeKind::Bool,
    })
}

/// Collects predicates for columns >= offset.
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

/// Collects predicates involving columns in range [start, end).
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

/// Extracts the B⋈C condition for associativity transform.
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
        if let Some(s) = shift_columns(c, -(a_cols as i32)) {
            preds.push(s);
        }
    }
    combine_predicates(preds)
}

/// Extracts the A condition for associativity transform.
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

/// Classifies a predicate into left-only, right-only, or join predicates.
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

/// Checks if expression uses left/right columns.
fn check_column_usage(expr: &BoundExpression, left_cols: usize) -> (bool, bool) {
    match expr {
        BoundExpression::ColumnBinding(c) => {
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

/// Rewrites column references using a mapping.
fn rewrite_with_mapping(expr: &BoundExpression, mapping: &[usize]) -> BoundExpression {
    match expr {
        BoundExpression::ColumnBinding(c) => BoundExpression::ColumnBinding(Binding {
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
        _ => expr.clone(),
    }
}

/// Creates a column reference expression.
fn create_column_ref(column_idx: usize, schema: &Schema) -> BoundExpression {
    let dtype = schema
        .column(column_idx)
        .map(|c| c.datatype())
        .unwrap_or(DataTypeKind::Null);

    BoundExpression::ColumnBinding(Binding {
        table_id: None,
        scope_index: 0,
        column_idx,
        data_type: dtype,
    })
}

// Transformation Rules

/// Swaps join inputs for inner/cross joins.
#[derive(Clone, Copy)]
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

    fn apply(&self, expr: &LogicalExpr, _memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Join(join) = &expr.op else {
            return Ok(vec![]);
        };
        if expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let swapped_cond = join.condition.as_ref().map(swap_join_condition);
        let new_join = JoinOp::new(
            join.join_type,
            swapped_cond,
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

/// Transforms (A⋈B)⋈C into A⋈(B⋈C) for inner joins.
#[derive(Clone, Copy)]
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
        let LogicalOperator::Join(outer) = &expr.op else {
            return false;
        };
        if outer.join_type != JoinType::Inner || expr.children.len() != 2 {
            return false;
        }

        let left_group = expr.children[0];
        if let Some(left) = memo.get_group(left_group) {
            if let Some(left_expr) = left.logical_exprs.first() {
                if let LogicalOperator::Join(inner) = &left_expr.op {
                    return inner.join_type == JoinType::Inner;
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Join(outer) = &expr.op else {
            return Ok(vec![]);
        };
        let left_group = expr.children[0];
        let c_group = expr.children[1];

        let left = memo
            .get_group(left_group)
            .ok_or(PlannerError::InvalidState)?;
        let left_expr = left
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?;
        let LogicalOperator::Join(inner) = &left_expr.op else {
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

        let a_cols = a_props.schema.num_columns();
        let b_cols = b_props.schema.num_columns();

        // Build B⋈C
        let bc_expr = {
            let bc_cond = extract_bc_condition(&outer.condition, &inner.condition, a_cols, b_cols);
            let bc_join = JoinOp::new(
                JoinType::Inner,
                bc_cond,
                b_props.schema.clone(),
                c_props.schema.clone(),
            );
            LogicalExpr::new(LogicalOperator::Join(bc_join), vec![b_group, c_group])
        };

        let a_cond = extract_a_condition(&outer.condition, &inner.condition, a_cols);

        let bc_group = memo.insert_logical_expr(bc_expr);

        // Build A⋈(B⋈C)
        let bc_schema = memo
            .get_group(bc_group)
            .map(|g| g.logical_props.schema.clone())
            .ok_or(PlannerError::InvalidState)?;

        let a_bc_join = JoinOp::new(JoinType::Inner, a_cond, a_props.schema.clone(), bc_schema);

        Ok(vec![
            LogicalExpr::new(LogicalOperator::Join(a_bc_join), vec![a_group, bc_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

/// Merges stacked filters into one.
#[derive(Clone, Copy)]
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

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(outer) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = expr.children.first().ok_or(PlannerError::InvalidState)?;
        let child = memo
            .get_group(*child_group)
            .ok_or(PlannerError::InvalidState)?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?;
        let LogicalOperator::Filter(inner) = &child_expr.op else {
            return Ok(vec![]);
        };

        let merged = BoundExpression::BinaryOp {
            left: Box::new(outer.predicate.clone()),
            op: BinaryOperator::And,
            right: Box::new(inner.predicate.clone()),
            result_type: DataTypeKind::Bool,
        };

        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Filter(FilterOp::new(merged, inner.input_schema.clone())),
                child_expr.children.clone(),
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

/// Pushes filter predicates below joins.
#[derive(Clone, Copy)]
pub struct FilterPushdownJoinRule;

impl Rule for FilterPushdownJoinRule {
    fn name(&self) -> &'static str {
        "FilterPushdownJoin"
    }
    fn promise(&self) -> u32 {
        90
    }
}

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

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr.children.first().ok_or(PlannerError::InvalidState)?;
        let child = memo
            .get_group(child_group)
            .ok_or(PlannerError::InvalidState)?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?;
        let LogicalOperator::Join(join) = &child_expr.op else {
            return Ok(vec![]);
        };

        let join_type = join.join_type;
        let left_schema = join.left_schema.clone();
        let right_schema = join.right_schema.clone();

        if join_type != JoinType::Inner && join_type != JoinType::Cross {
            return Ok(vec![]);
        }
        if child_expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let left_group = child_expr.children[0];
        let right_group = child_expr.children[1];
        let left_cols = join.left_schema.num_columns();

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

        if left_preds.is_empty() && right_preds.is_empty() {
            return Ok(vec![]);
        }

        // New join condition
        let new_cond =
            combine_predicates(join.condition.iter().cloned().chain(join_preds).collect());

        // Push left predicates
        let new_left = if !left_preds.is_empty() {
            let pred = combine_predicates(left_preds).unwrap();
            let f = FilterOp::new(pred, left_schema.clone());
            memo.insert_logical_expr(LogicalExpr::new(
                LogicalOperator::Filter(f),
                vec![left_group],
            ))
        } else {
            left_group
        };

        // Push right predicates (shift indices)
        let new_right = if !right_preds.is_empty() {
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

/// Pushes filter below project (when project is column-refs only).
#[derive(Clone, Copy)]
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
                                .all(|e| matches!(&e.expr, BoundExpression::ColumnBinding(_)));
                        }
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        // Check if the current operator is a filter
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };
        let child_group = *expr.children.first().ok_or(PlannerError::InvalidState)?;
        let child = memo
            .get_group(child_group)
            .ok_or(PlannerError::InvalidState)?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?;

        // Check if the first child operator is a project
        let LogicalOperator::Project(project) = &child_expr.op else {
            return Ok(vec![]);
        };

        let project_exprs = project.expressions.clone();
        let project_input_schema = project.input_schema.clone();
        let project_output_schema = project.output_schema.clone();

        // Build mapping from output to input columns
        let mut mapping = Vec::new();
        for proj_expr in &project_exprs {
            if let BoundExpression::ColumnBinding(col_ref) = &proj_expr.expr {
                mapping.push(col_ref.column_idx);
            } else {
                return Ok(vec![]);
            }
        }

        let new_pred = rewrite_with_mapping(&filter.predicate, &mapping);
        let new_filter = FilterOp::new(new_pred, project_input_schema.clone());

        let filter_group = memo.insert_logical_expr(LogicalExpr::new(
            LogicalOperator::Filter(new_filter),
            child_expr.children.clone(),
        ));

        // Build the new project operator with the pushed down filter
        let new_project =
            ProjectOp::new(project_exprs, project_input_schema, project_output_schema);

        Ok(vec![
            LogicalExpr::new(LogicalOperator::Project(new_project), vec![filter_group])
                .with_properties(expr.properties.clone()),
        ])
    }
}

// Implementation Rules
#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::TableScan(scan) = &expr.op else {
            return Ok(vec![]);
        };

        let mut seq = SeqScanOp::new(scan.table_id, scan.table_schema.clone());
        if let Some(cols) = &scan.columns {
            seq = seq.with_columns(cols.clone());
        }
        if let Some(pred) = &scan.predicate {
            seq = seq.with_predicate(pred.clone());
        }

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::SeqScan(seq),
            vec![],
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::IndexScan(scan) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::IndexScan(PhysIndexScanOp::from(scan.clone())),
            vec![],
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
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

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
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

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Join(join) = &expr.op else {
            return Ok(vec![]);
        };

        let mut impls = Vec::new();

        // Nested loop join is always available
        impls.push(PhysicalExpr::new(
            PhysicalOperator::NestedLoopJoin(NestedLoopJoinOp::new(
                join.join_type,
                join.condition.clone(),
                join.output_schema.clone(),
            )),
            expr.children.clone(),
        ));

        // Hash join and Merge join are available for equi-joins
        if join.is_equi_join() {
            let keys = join.extract_equi_keys();
            if !keys.is_empty() {
                let left_cols = join.left_schema.num_columns();

                // Create key expressions for left and right sides
                // Note: extract_equi_keys returns indices in the combined output schema
                // Left keys are already correct (0..left_cols)
                // Right keys need to be adjusted by subtracting left_cols
                let (left_keys, right_keys): (Vec<_>, Vec<_>) = keys
                    .iter()
                    .map(|(l, r)| {
                        // r is the index in the combined schema, convert to right-side index
                        let right_idx = r.saturating_sub(left_cols);
                        (
                            create_column_ref(*l, &join.left_schema),
                            create_column_ref(right_idx, &join.right_schema),
                        )
                    })
                    .unzip();

                // Hash join
                impls.push(PhysicalExpr::new(
                    PhysicalOperator::HashJoin(HashJoinOp::new(
                        join.join_type,
                        left_keys.clone(),
                        right_keys.clone(),
                        None,
                        join.output_schema.clone(),
                    )),
                    expr.children.clone(),
                ));

                // Merge join
                // left_ordering uses indices relative to left schema (child 0)
                // right_ordering uses indices relative to right schema (child 1)
                let left_ordering: Vec<_> =
                    keys.iter().map(|(l, _)| OrderingSpec::asc(*l)).collect();
                let right_ordering: Vec<_> = keys
                    .iter()
                    .map(|(_, r)| {
                        // r is in combined schema, convert to right-side index
                        let right_idx = r.saturating_sub(left_cols);
                        OrderingSpec::asc(right_idx)
                    })
                    .collect();
                let output_ordering: Vec<_> = left_keys.iter().map(|e| (e.clone(), true)).collect();

                let mut merge_expr = PhysicalExpr::new(
                    PhysicalOperator::MergeJoin(MergeJoinOp {
                        join_type: join.join_type,
                        left_keys,
                        right_keys,
                        left_ordering: left_ordering.clone(),
                        right_ordering,
                        output_ordering: output_ordering.clone(),
                        output_schema: join.output_schema.clone(),
                    }),
                    expr.children.clone(),
                );
                merge_expr.properties.ordering = output_ordering;
                impls.push(merge_expr);
            }
        }

        Ok(impls)
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
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

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Sort(sort) = &expr.op else {
            return Ok(vec![]);
        };

        let mut phys =
            PhysicalExpr::new(PhysicalOperator::Sort(sort.clone()), expr.children.clone());
        phys.properties.ordering = sort
            .order_by
            .iter()
            .map(|s| (s.expr.clone(), s.asc))
            .collect();

        Ok(vec![phys])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
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

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Distinct(d) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Distinct(HashDistinctOp::new(d.schema.clone())),
            expr.children.clone(),
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Values(v) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Values(PhysValuesOp::new(v.rows.clone(), v.schema.clone())),
            vec![],
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Empty(e) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Empty(PhysEmptyOp::new(e.schema.clone())),
            vec![],
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Insert(i) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Insert(PhysInsertOp::new(
                i.table_id,
                i.columns.clone(),
                i.table_schema.clone(),
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Update(u) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Update(PhysUpdateOp::new(
                u.table_id,
                u.assignments.clone(),
                u.table_schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Delete(d) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Delete(PhysDeleteOp::new(d.table_id, d.table_schema.clone())),
            expr.children.clone(),
        )])
    }
}

#[derive(Clone, Copy)]
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
    ) -> PlannerResult<Vec<PhysicalExpr>> {
        let LogicalOperator::Materialize(m) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::Materialize(PhysMaterializeOp::new(m.schema.clone())),
            expr.children.clone(),
        )])
    }
}

/// Transforms Filter(predicate) -> TableScan into IndexScan when an applicable index exists.
///
/// This rule identifies filter predicates that can leverage an index for more efficient
/// execution. It analyzes the predicate to extract range bounds and matches them against
/// available indexes on the table.
#[derive(Clone, Copy)]
pub struct FilterToIndexScanRule;

impl Rule for FilterToIndexScanRule {
    fn name(&self) -> &'static str {
        "FilterToIndexScan"
    }
    fn promise(&self) -> u32 {
        100 // High priority since index scans can dramatically improve performance
    }
}

impl TransformationRule for FilterToIndexScanRule {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Match pattern: Filter follwed by TableScan
        if let LogicalOperator::Filter(_) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(&child_expr.op, LogicalOperator::TableScan(_));
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> PlannerResult<Vec<LogicalExpr>> {
        let LogicalOperator::Filter(filter) = &expr.op else {
            return Ok(vec![]);
        };

        let child_group = *expr.children.first().ok_or(PlannerError::InvalidState)?;
        let child = memo
            .get_group(child_group)
            .ok_or(PlannerError::InvalidState)?;
        let child_expr = child
            .logical_exprs
            .first()
            .ok_or(PlannerError::InvalidState)?;

        let LogicalOperator::TableScan(scan) = &child_expr.op else {
            return Ok(vec![]);
        };

        // Get available indexes from the schema
        let indexes = scan.schema.get_indexes();
        if indexes.is_empty() {
            return Ok(vec![]);
        }

        let mut alternatives = Vec::new();

        for index_handle in indexes {
            // Try to match the predicate against this index
            if let Some(index_scan) =
                self.try_create_index_scan(scan, &filter.predicate, &index_handle)
            {
                alternatives.push(
                    LogicalExpr::new(LogicalOperator::IndexScan(index_scan), vec![])
                        .with_properties(expr.properties.clone()),
                );
            }
        }

        Ok(alternatives)
    }
}

impl FilterToIndexScanRule {
    /// Attempts to create an IndexScan from a filter predicate and index handle.
    /// Returns None if the predicate cannot use this index.
    fn try_create_index_scan(
        &self,
        scan: &TableScanOp,
        predicate: &BoundExpression,
        index_handle: &IndexHandle,
    ) -> Option<IndexScanOp> {
        let indexed_columns = index_handle.indexed_column_ids();
        let index_id = index_handle.id();

        // Extract predicates that can be used as index bounds
        let (range_start, range_end, residual) =
            self.extract_index_bounds(index_id, predicate, indexed_columns);

        // Only create an index scan if we can use at least one bound
        if range_start.is_none() && range_end.is_none() {
            return None;
        }

        let mut index_scan = IndexScanOp::new(
            scan.table_id,
            index_handle.id(),
            scan.schema.clone(),
            indexed_columns.to_vec(),
        );

        index_scan.range_start = range_start;
        index_scan.range_end = range_end;
        index_scan.residual_predicate = residual;

        Some(index_scan)
    }

    /// Extracts index bounds from a predicate.
    /// Returns (range_start, range_end, residual_predicate)
    fn extract_index_bounds(
        &self,
        index_id: ObjectId,
        predicate: &BoundExpression,
        indexed_columns: &[usize],
    ) -> (
        Option<Vec<IndexRangeBound>>,
        Option<Vec<IndexRangeBound>>,
        Option<BoundExpression>,
    ) {
        let mut range_start = Vec::new();
        let mut range_end = Vec::new();
        let mut residual_parts = Vec::new();

        self.collect_bounds(
            index_id,
            predicate,
            indexed_columns,
            &mut range_start,
            &mut range_end,
            &mut residual_parts,
        );

        let range_end = Some(range_end).filter(|s| !s.is_empty());
        let range_start = Some(range_start).filter(|s| !s.is_empty());

        let residual = combine_predicates(residual_parts);

        (range_start, range_end, residual)
    }

    /// Recursively collects bounds from a predicate expression.
    fn collect_bounds(
        &self,
        index_id: ObjectId,
        expr: &BoundExpression,
        indexed_columns: &[usize],
        range_start: &mut Vec<IndexRangeBound>,
        range_end: &mut Vec<IndexRangeBound>,
        residual: &mut Vec<BoundExpression>,
    ) {
        match expr {
            BoundExpression::BinaryOp {
                op: BinaryOperator::And,
                left,
                right,
                ..
            } => {
                // Recursively process AND conditions
                self.collect_bounds(
                    index_id,
                    left,
                    indexed_columns,
                    range_start,
                    range_end,
                    residual,
                );
                self.collect_bounds(
                    index_id,
                    right,
                    indexed_columns,
                    range_start,
                    range_end,
                    residual,
                );
            }
            BoundExpression::BinaryOp {
                op,
                left,
                right,
                result_type,
            } => {
                // Check if this is a comparison on an indexed column
                if let Some((col_idx, datatype)) = Self::extract_column_info(left)
                    && let Some(position) = indexed_columns.iter().position(|&x| x == col_idx)
                    && let Some(value) = Self::extract_literal(right)
                {
                    match op {
                        BinaryOperator::Eq => {
                            let bound = IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            };

                            // Equality: both bounds are the same
                            range_start.push(bound.clone());
                            range_end.push(bound.clone());

                            return;
                        }
                        BinaryOperator::Gt => {
                            range_start.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: false,
                                col_idx: position,
                            });

                            return;
                        }
                        BinaryOperator::Ge => {
                            range_start.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            });
                            return;
                        }
                        BinaryOperator::Lt => {
                            range_end.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: false,
                                col_idx: position,
                            });
                            return;
                        }

                        BinaryOperator::Le => {
                            range_end.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            });
                            return;
                        }

                        _ => {}
                    }
                }

                // Check if this is a comparison on an indexed column
                if let Some((col_idx, datatype)) = Self::extract_column_info(right)
                    && let Some(position) = indexed_columns.iter().position(|&x| x == col_idx)
                    && let Some(value) = Self::extract_literal(left)
                {
                    match op {
                        BinaryOperator::Eq => {
                            let bound = IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            };

                            // Equality: both bounds are the same
                            range_start.push(bound.clone());
                            range_end.push(bound.clone());

                            return;
                        }
                        BinaryOperator::Lt => {
                            range_start.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: false,
                                col_idx: position,
                            });

                            return;
                        }
                        BinaryOperator::Le => {
                            range_start.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            });
                            return;
                        }
                        BinaryOperator::Gt => {
                            range_end.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: false,
                                col_idx: position,
                            });
                            return;
                        }

                        BinaryOperator::Ge => {
                            range_end.push(IndexRangeBound {
                                value: value.clone(),
                                inclusive: true,
                                col_idx: position,
                            });
                            return;
                        }

                        _ => {}
                    }
                }

                // Cannot use this predicate for index bounds
                residual.push(expr.clone());
            }
            _ => {
                // Any other expression goes to residual
                residual.push(expr.clone());
            }
        }
    }

    fn extract_column_info(expr: &BoundExpression) -> Option<(usize, DataTypeKind)> {
        match expr {
            BoundExpression::ColumnBinding(c) => Some((c.column_idx, c.data_type)),
            _ => None,
        }
    }

    fn extract_literal(expr: &BoundExpression) -> Option<crate::DataType> {
        match expr {
            BoundExpression::Literal { value } => Some(value.clone()),
            _ => None,
        }
    }
}
