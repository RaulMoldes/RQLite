//! Transformation and implementation rules for the Cascades optimizer.
//!
//! Transformation rules explore equivalent logical plans.
//! Implementation rules map logical operators to physical operators.

use crate::sql::{
    ast::{BinaryOperator, JoinType},
    binder::ast::*,
};
use std::sync::Arc;

use super::logical::*;
use super::memo::Memo;
use super::physical::*;
use super::prop::RequiredProperties;
use super::{OptimizerError, OptimizerResult};

/// Rule trait - base for all rules
pub trait Rule: Send + Sync {
    /// Rule name for debugging
    fn name(&self) -> &'static str;

    /// Promise value (higher = applied later in Cascades)
    fn promise(&self) -> u32 {
        50
    }
}

/// Transformation rule - produces equivalent logical expressions
pub trait TransformationRule: Rule {
    /// Check if this rule matches the expression
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;

    /// Apply the transformation
    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>>;
}

/// Implementation rule - maps logical to physical operators
pub trait ImplementationRule: Rule {
    /// Check if this rule matches the expression
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool;

    /// Generate physical implementations
    fn implement(
        &self,
        expr: &LogicalExpr,
        required: &RequiredProperties,
        memo: &Memo,
    ) -> OptimizerResult<Vec<PhysicalExpr>>;
}

/// Collection of rules
pub struct RuleSet {
    pub transformation_rules: Vec<Arc<dyn TransformationRule>>,
    pub implementation_rules: Vec<Arc<dyn ImplementationRule>>,
}

impl RuleSet {
    pub fn new() -> Self {
        Self {
            transformation_rules: Vec::new(),
            implementation_rules: Vec::new(),
        }
    }

    pub fn add_transformation(&mut self, rule: Arc<dyn TransformationRule>) {
        self.transformation_rules.push(rule);
    }

    pub fn add_implementation(&mut self, rule: Arc<dyn ImplementationRule>) {
        self.implementation_rules.push(rule);
    }
}

impl Default for RuleSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Get default transformation rules
pub fn default_transformation_rules() -> Vec<Arc<dyn TransformationRule>> {
    vec![
        Arc::new(PredicatePushdown),
        Arc::new(JoinCommutativity),
        Arc::new(JoinAssociativity),
        Arc::new(FilterMerge),
        Arc::new(ProjectionPushdown),
    ]
}

/// Get default implementation rules
pub fn default_implementation_rules() -> Vec<Arc<dyn ImplementationRule>> {
    vec![
        Arc::new(TableScanImplementation),
        Arc::new(FilterImplementation),
        Arc::new(ProjectImplementation),
        Arc::new(JoinImplementation),
        Arc::new(AggregateImplementation),
        Arc::new(SortImplementation),
        Arc::new(LimitImplementation),
        Arc::new(DistinctImplementation),
        Arc::new(ValuesImplementation),
        Arc::new(EmptyImplementation),
        Arc::new(InsertImplementation),
        Arc::new(UpdateImplementation),
        Arc::new(DeleteImplementation),
    ]
}

/// Push predicates down through joins and projects
pub struct PredicatePushdown;

impl Rule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }

    fn promise(&self) -> u32 {
        100 // High priority
    }
}

impl TransformationRule for PredicatePushdown {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Match Filter on top of Join or Filter on top of Project
        if let LogicalOperator::Filter(filter) = &expr.op {
            if let Some(child_group) = expr.children.first() {
                if let Some(child) = memo.get_group(*child_group) {
                    if let Some(child_expr) = child.logical_exprs.first() {
                        return matches!(
                            &child_expr.op,
                            LogicalOperator::Join(_)
                                | LogicalOperator::Project(_)
                                | LogicalOperator::TableScan(_)
                        );
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

        let child_group = expr
            .children
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("Filter has no child".into()))?;

        let child = memo
            .get_group(*child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child group not found".into()))?;

        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("Child has no expressions".into()))?;

        match &child_expr.op {
            LogicalOperator::TableScan(scan) => {
                // Push predicate into table scan
                let new_scan = TableScanOp {
                    predicate: Some(filter.predicate.clone()),
                    ..scan.clone()
                };
                Ok(vec![
                    LogicalExpr::new(LogicalOperator::TableScan(new_scan), vec![])
                        .with_properties(expr.properties.clone()),
                ])
            }
            LogicalOperator::Join(join) => {
                // Try to push predicate to left or right side of join
                // For now, keep filter above join but mark as explored
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }
}

/// Join commutativity: A JOIN B = B JOIN A (for inner joins)
pub struct JoinCommutativity;

impl Rule for JoinCommutativity {
    fn name(&self) -> &'static str {
        "JoinCommutativity"
    }
}

impl TransformationRule for JoinCommutativity {
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

        // Swap left and right
        let swapped_condition = join
            .condition
            .as_ref()
            .map(|cond| swap_join_condition(cond));

        let new_join = JoinOp::new(
            join.join_type,
            swapped_condition,
            join.right_schema.clone(),
            join.left_schema.clone(),
        );

        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Join(new_join),
                vec![expr.children[1], expr.children[0]], // Swap children
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

/// Swap operands in join condition
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

/// Join associativity: (A JOIN B) JOIN C = A JOIN (B JOIN C)
pub struct JoinAssociativity;

impl Rule for JoinAssociativity {
    fn name(&self) -> &'static str {
        "JoinAssociativity"
    }

    fn promise(&self) -> u32 {
        30 // Lower priority - expensive
    }
}

impl TransformationRule for JoinAssociativity {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Match Join(Join, X)
        if let LogicalOperator::Join(outer) = &expr.op {
            if outer.join_type == JoinType::Inner && expr.children.len() == 2 {
                if let Some(left_group) = memo.get_group(expr.children[0]) {
                    if let Some(left_expr) = left_group.logical_exprs.first() {
                        if let LogicalOperator::Join(inner) = &left_expr.op {
                            return inner.join_type == JoinType::Inner;
                        }
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &LogicalExpr, memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        // (A JOIN B) JOIN C -> A JOIN (B JOIN C)
        // This is complex - for now, skip
        Ok(vec![])
    }
}

/// Merge adjacent filters
pub struct FilterMerge;

impl Rule for FilterMerge {
    fn name(&self) -> &'static str {
        "FilterMerge"
    }
}

impl TransformationRule for FilterMerge {
    fn matches(&self, expr: &LogicalExpr, memo: &Memo) -> bool {
        // Match Filter on Filter
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
            .ok_or_else(|| OptimizerError::InvalidState("Filter has no child".into()))?;

        let child = memo
            .get_group(*child_group)
            .ok_or_else(|| OptimizerError::InvalidState("Child group not found".into()))?;

        let child_expr = child
            .logical_exprs
            .first()
            .ok_or_else(|| OptimizerError::InvalidState("Child has no expressions".into()))?;

        let LogicalOperator::Filter(inner_filter) = &child_expr.op else {
            return Ok(vec![]);
        };

        // Merge predicates with AND
        let merged_predicate = BoundExpression::BinaryOp {
            left: Box::new(outer_filter.predicate.clone()),
            op: BinaryOperator::And,
            right: Box::new(inner_filter.predicate.clone()),
            result_type: crate::types::DataTypeKind::Boolean,
        };

        let new_filter = FilterOp::new(merged_predicate, inner_filter.input_schema.clone());

        Ok(vec![
            LogicalExpr::new(
                LogicalOperator::Filter(new_filter),
                child_expr.children.clone(), // Skip the inner filter
            )
            .with_properties(expr.properties.clone()),
        ])
    }
}

/// Push projections down
pub struct ProjectionPushdown;

impl Rule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "ProjectionPushdown"
    }
}

impl TransformationRule for ProjectionPushdown {
    fn matches(&self, expr: &LogicalExpr, _memo: &Memo) -> bool {
        // Only apply to certain cases
        matches!(&expr.op, LogicalOperator::Project(_))
    }

    fn apply(&self, _expr: &LogicalExpr, _memo: &mut Memo) -> OptimizerResult<Vec<LogicalExpr>> {
        // TODO: Implement column pruning
        Ok(vec![])
    }
}

// ==================== Implementation Rules ====================

/// Implement table scan
pub struct TableScanImplementation;

impl Rule for TableScanImplementation {
    fn name(&self) -> &'static str {
        "TableScanImpl"
    }
}

impl ImplementationRule for TableScanImplementation {
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

        // Create sequential scan
        let seq_scan = SeqScanOp::new(scan.table_id, scan.table_name.clone(), scan.schema.clone());
        let seq_scan = if let Some(pred) = &scan.predicate {
            seq_scan.with_predicate(pred.clone())
        } else {
            seq_scan
        };
        let seq_scan = if let Some(cols) = &scan.columns {
            seq_scan.with_columns(cols.clone())
        } else {
            seq_scan
        };

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::SeqScan(seq_scan),
            vec![],
        )])
    }
}

/// Implement filter
pub struct FilterImplementation;

impl Rule for FilterImplementation {
    fn name(&self) -> &'static str {
        "FilterImpl"
    }
}

impl ImplementationRule for FilterImplementation {
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

/// Implement projection
pub struct ProjectImplementation;

impl Rule for ProjectImplementation {
    fn name(&self) -> &'static str {
        "ProjectImpl"
    }
}

impl ImplementationRule for ProjectImplementation {
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

/// Implement joins (nested loop, hash, merge)
pub struct JoinImplementation;

impl Rule for JoinImplementation {
    fn name(&self) -> &'static str {
        "JoinImpl"
    }
}

impl ImplementationRule for JoinImplementation {
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

        // Nested loop join (always applicable)
        implementations.push(PhysicalExpr::new(
            PhysicalOperator::NestedLoopJoin(NestedLoopJoinOp::new(
                join.join_type,
                join.condition.clone(),
                join.output_schema.clone(),
            )),
            expr.children.clone(),
        ));

        // Hash join (for equi-joins)
        if join.is_equi_join() {
            let keys = join.extract_equi_keys();
            if !keys.is_empty() {
                let left_keys: Vec<BoundExpression> = keys
                    .iter()
                    .map(|(l, _)| {
                        BoundExpression::ColumnRef(BoundColumnRef {
                            table_idx: 0,
                            column_idx: *l,
                            data_type: crate::types::DataTypeKind::Int, // Placeholder
                        })
                    })
                    .collect();

                let right_keys: Vec<BoundExpression> = keys
                    .iter()
                    .map(|(_, r)| {
                        BoundExpression::ColumnRef(BoundColumnRef {
                            table_idx: 1,
                            column_idx: *r,
                            data_type: crate::types::DataTypeKind::Int, // Placeholder
                        })
                    })
                    .collect();

                implementations.push(PhysicalExpr::new(
                    PhysicalOperator::HashJoin(HashJoinOp::new(
                        join.join_type,
                        left_keys,
                        right_keys,
                        None, // Residual conditions
                        join.output_schema.clone(),
                    )),
                    expr.children.clone(),
                ));
            }
        }

        Ok(implementations)
    }
}

/// Implement aggregation
pub struct AggregateImplementation;

impl Rule for AggregateImplementation {
    fn name(&self) -> &'static str {
        "AggregateImpl"
    }
}

impl ImplementationRule for AggregateImplementation {
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

        // Hash aggregate is the default
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

/// Implement sort
pub struct SortImplementation;

impl Rule for SortImplementation {
    fn name(&self) -> &'static str {
        "SortImpl"
    }
}

impl ImplementationRule for SortImplementation {
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

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::ExternalSort(ExternalSortOp::new(
                sort.order_by.clone(),
                sort.schema.clone(),
            )),
            expr.children.clone(),
        )])
    }
}

/// Implement limit
pub struct LimitImplementation;

impl Rule for LimitImplementation {
    fn name(&self) -> &'static str {
        "LimitImpl"
    }
}

impl ImplementationRule for LimitImplementation {
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

/// Implement distinct
pub struct DistinctImplementation;

impl Rule for DistinctImplementation {
    fn name(&self) -> &'static str {
        "DistinctImpl"
    }
}

impl ImplementationRule for DistinctImplementation {
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

        Ok(vec![PhysicalExpr::new(
            PhysicalOperator::HashDistinct(HashDistinctOp {
                schema: distinct.schema.clone(),
            }),
            expr.children.clone(),
        )])
    }
}

/// Implement VALUES
pub struct ValuesImplementation;

impl Rule for ValuesImplementation {
    fn name(&self) -> &'static str {
        "ValuesImpl"
    }
}

impl ImplementationRule for ValuesImplementation {
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

/// Implement Empty
pub struct EmptyImplementation;

impl Rule for EmptyImplementation {
    fn name(&self) -> &'static str {
        "EmptyImpl"
    }
}

impl ImplementationRule for EmptyImplementation {
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

/// Implement INSERT
pub struct InsertImplementation;

impl Rule for InsertImplementation {
    fn name(&self) -> &'static str {
        "InsertImpl"
    }
}

impl ImplementationRule for InsertImplementation {
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

/// Implement UPDATE
pub struct UpdateImplementation;

impl Rule for UpdateImplementation {
    fn name(&self) -> &'static str {
        "UpdateImpl"
    }
}

impl ImplementationRule for UpdateImplementation {
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

/// Implement DELETE
pub struct DeleteImplementation;

impl Rule for DeleteImplementation {
    fn name(&self) -> &'static str {
        "DeleteImpl"
    }
}

impl ImplementationRule for DeleteImplementation {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        database::schema::{Column, Schema},
        sql::planner::memo::GroupId,
        types::{DataType, DataTypeKind, ObjectId, UInt8},
    };

    fn test_schema() -> Schema {
        Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::Int, "id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
            ],
            1,
        )
    }

    #[test]
    fn test_join_commutativity_matches() {
        let schema = test_schema();
        let rule = JoinCommutativity;

        let expr = LogicalExpr::new(
            LogicalOperator::Join(JoinOp::new(
                JoinType::Inner,
                None,
                schema.clone(),
                schema.clone(),
            )),
            vec![GroupId(0), GroupId(1)],
        );

        let memo = Memo::new();
        assert!(rule.matches(&expr, &memo));
    }

    #[test]
    fn test_filter_merge_matches() {
        // Create a simple filter-on-filter scenario
        let mut memo = Memo::new();
        let schema = test_schema();

        // Inner filter
        let inner_pred = BoundExpression::Literal {
            value: DataType::Boolean(UInt8::TRUE),
        };
        let inner_filter = LogicalExpr::new(
            LogicalOperator::Filter(FilterOp::new(inner_pred, schema.clone())),
            vec![],
        );
        let inner_group = memo.insert_logical_expr(inner_filter);

        // Outer filter
        let outer_pred = BoundExpression::Literal {
            value: DataType::Boolean(UInt8::TRUE),
        };
        let outer_filter = LogicalExpr::new(
            LogicalOperator::Filter(FilterOp::new(outer_pred, schema)),
            vec![inner_group],
        );

        let rule = FilterMerge;
        assert!(rule.matches(&outer_filter, &memo));
    }

    #[test]
    fn test_table_scan_implementation() {
        let rule = TableScanImplementation;
        let schema = test_schema();

        let expr = LogicalExpr::new(
            LogicalOperator::TableScan(TableScanOp::new(
                ObjectId::new(),
                "test".to_string(),
                schema,
            )),
            vec![],
        );

        let memo = Memo::new();
        let required = RequiredProperties::default();

        let impls = rule.implement(&expr, &required, &memo).unwrap();
        assert_eq!(impls.len(), 1);
        assert!(matches!(impls[0].op, PhysicalOperator::SeqScan(_)));
    }

    #[test]
    fn test_join_implementation() {
        let rule = JoinImplementation;
        let schema = test_schema();

        // Create an equi-join
        let condition = BoundExpression::BinaryOp {
            left: Box::new(BoundExpression::ColumnRef(BoundColumnRef {
                table_idx: 0,
                column_idx: 0,
                data_type: DataTypeKind::Int,
            })),
            op: BinaryOperator::Eq,
            right: Box::new(BoundExpression::ColumnRef(BoundColumnRef {
                table_idx: 1,
                column_idx: 0,
                data_type: DataTypeKind::Int,
            })),
            result_type: DataTypeKind::Boolean,
        };

        let expr = LogicalExpr::new(
            LogicalOperator::Join(JoinOp::new(
                JoinType::Inner,
                Some(condition),
                schema.clone(),
                schema,
            )),
            vec![GroupId(0), GroupId(1)],
        );

        let memo = Memo::new();
        let required = RequiredProperties::default();

        let impls = rule.implement(&expr, &required, &memo).unwrap();
        // Should produce both nested loop and hash join
        assert!(impls.len() >= 2);
    }
}
