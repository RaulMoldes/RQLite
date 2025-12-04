//! Memo structure for the Cascades optimizer.
//!
//! The memo is a compact representation of the search space that avoids redundant work
//! by sharing common subexpressions. Each group in the memo represents an equivalence
//! class of logically equivalent expressions.

use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use super::{
    logical::LogicalExpr,
    physical::PhysicalExpr,
    prop::{LogicalProperties, RequiredProperties},
    stats::Statistics,
};

/// Unique identifier for a group in the memo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupId(pub usize);

impl GroupId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

/// Unique identifier for an expression within the memo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ExprId {
    pub group_id: GroupId,
    pub expr_idx: usize,
    pub is_physical: bool,
}

impl ExprId {
    pub fn logical(group_id: GroupId, expr_idx: usize) -> Self {
        Self {
            group_id,
            expr_idx,
            is_physical: false,
        }
    }

    pub fn physical(group_id: GroupId, expr_idx: usize) -> Self {
        Self {
            group_id,
            expr_idx,
            is_physical: true,
        }
    }
}

impl Default for GroupId {
    fn default() -> Self {
        Self(0)
    }
}

/// A group in the memo representing an equivalence class
#[derive(Debug, Clone)]
pub struct Group {
    pub id: GroupId,
    /// Logical properties shared by all expressions in the group
    pub logical_props: LogicalProperties,
    /// Logical expressions in this group
    pub logical_exprs: Vec<LogicalExpr>,
    /// Physical expressions indexed by required properties
    pub physical_exprs: Vec<PhysicalExpr>,
    /// Best physical expression for each set of required properties
    pub best_physical: HashMap<RequiredProperties, (f64, ExprId)>,
    /// Statistics for cost estimation
    pub stats: Statistics,
    /// Whether exploration is complete
    pub explored: bool,
    /// Whether implementation is complete for default properties
    pub implemented: bool,
}

impl Group {
    pub fn new(id: GroupId, logical_props: LogicalProperties) -> Self {
        Self {
            id,
            logical_props,
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            best_physical: HashMap::new(),
            stats: Statistics::default(),
            explored: false,
            implemented: false,
        }
    }

    /// Add a logical expression to this group
    pub fn add_logical_expr(&mut self, mut expr: LogicalExpr) -> ExprId {
        let expr_idx = self.logical_exprs.len();
        expr.id = ExprId::logical(self.id, expr_idx);
        self.logical_exprs.push(expr);
        ExprId::logical(self.id, expr_idx)
    }

    /// Add a physical expression to this group
    pub fn add_physical_expr(&mut self, mut expr: PhysicalExpr) -> ExprId {
        let expr_idx = self.physical_exprs.len();
        expr.id = ExprId::physical(self.id, expr_idx);
        self.physical_exprs.push(expr);
        ExprId::physical(self.id, expr_idx)
    }

    /// Get the best cost for given required properties
    pub fn best_cost(&self, required: &RequiredProperties) -> Option<f64> {
        self.best_physical.get(required).map(|(cost, _)| *cost)
    }

    /// Update the best physical expression for given properties
    pub fn update_best(
        &mut self,
        required: RequiredProperties,
        cost: f64,
        expr_id: ExprId,
    ) -> bool {
        match self.best_physical.get(&required) {
            Some((best_cost, _)) if *best_cost <= cost => false,
            _ => {
                self.best_physical.insert(required, (cost, expr_id));
                true
            }
        }
    }
}

/// The memo structure is the central data structure of the Volcano and Cascades frameworks for extensible optimizers.
/// Refer: https://www.microsoft.com/en-us/research/publication/extensible-query-optimizers-in-practice/
#[derive(Debug)]
pub struct Memo {
    /// All groups in the memo
    groups: HashMap<GroupId, Group>,
    /// Next available group ID
    next_group_id: usize,
    /// Hash map for deduplication of expressions
    expr_hash: HashMap<u64, GroupId>,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            next_group_id: 0,
            expr_hash: HashMap::new(),
        }
    }

    /// Create a new group with given logical properties
    pub fn new_group(&mut self, logical_props: LogicalProperties) -> GroupId {
        let id = GroupId(self.next_group_id);
        self.next_group_id += 1;
        self.groups.insert(id, Group::new(id, logical_props));
        id
    }

    /// Insert a logical expression, deduplicating if already exists
    pub fn insert_logical_expr(&mut self, expr: LogicalExpr) -> GroupId {
        // Compute hash for deduplication
        let hash = self.compute_expr_hash(&expr);

        if let Some(&group_id) = self.expr_hash.get(&hash) {
            return group_id;
        }

        // Create new group
        let group_id = self.new_group(expr.properties.clone());
        self.expr_hash.insert(hash, group_id);

        // Add expression to group
        if let Some(group) = self.groups.get_mut(&group_id) {
            group.add_logical_expr(expr);
        }

        group_id
    }

    /// Add a logical expression to an existing group
    pub fn add_logical_expr_to_group(
        &mut self,
        group_id: GroupId,
        expr: LogicalExpr,
    ) -> Option<ExprId> {
        // Check for duplicates within the group
        let hash = self.compute_expr_hash(&expr);
        if self.expr_hash.contains_key(&hash) {
            return None;
        }
        self.expr_hash.insert(hash, group_id);

        self.groups
            .get_mut(&group_id)
            .map(|g| g.add_logical_expr(expr))
    }

    /// Add a physical expression to a group
    pub fn add_physical_expr_to_group(&mut self, group_id: GroupId, expr: PhysicalExpr) -> ExprId {
        self.groups
            .get_mut(&group_id)
            .map(|g| g.add_physical_expr(expr))
            .unwrap_or_default()
    }

    /// Get a group by ID
    pub fn get_group(&self, id: GroupId) -> Option<&Group> {
        self.groups.get(&id)
    }

    /// Get a mutable group by ID
    pub fn get_group_mut(&mut self, id: GroupId) -> Option<&mut Group> {
        self.groups.get_mut(&id)
    }

    /// Get a logical expression by ID
    pub fn get_logical_expr(&self, id: ExprId) -> Option<LogicalExpr> {
        self.groups
            .get(&id.group_id)
            .and_then(|g| g.logical_exprs.get(id.expr_idx))
            .cloned()
    }

    /// Get a physical expression by ID
    pub fn get_physical_expr(&self, id: ExprId) -> Option<PhysicalExpr> {
        self.groups
            .get(&id.group_id)
            .and_then(|g| g.physical_exprs.get(id.expr_idx))
            .cloned()
    }

    /// Mark a group as explored
    pub fn mark_explored(&mut self, group_id: GroupId) {
        if let Some(group) = self.groups.get_mut(&group_id) {
            group.explored = true;
        }
    }

    /// Check if a group has been explored
    pub fn is_explored(&self, group_id: GroupId) -> bool {
        self.groups
            .get(&group_id)
            .map(|g| g.explored)
            .unwrap_or(false)
    }

    /// Get the number of groups
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Get the total number of logical expressions
    pub fn num_logical_exprs(&self) -> usize {
        self.groups.values().map(|g| g.logical_exprs.len()).sum()
    }

    /// Get the total number of physical expressions
    pub fn num_physical_exprs(&self) -> usize {
        self.groups.values().map(|g| g.physical_exprs.len()).sum()
    }

    /// Compute hash for expression deduplication
    fn compute_expr_hash(&self, expr: &LogicalExpr) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash the operator type
        std::mem::discriminant(&expr.op).hash(&mut hasher);

        // Hash operator-specific data
        self.hash_operator(&expr.op, &mut hasher);

        // Hash children (by group ID)
        for child in &expr.children {
            child.0.hash(&mut hasher);
        }

        hasher.finish()
    }

    fn hash_operator(&self, op: &super::logical::LogicalOperator, hasher: &mut DefaultHasher) {
        use super::logical::LogicalOperator;

        match op {
            LogicalOperator::TableScan(scan) => {
                scan.table_name.hash(hasher);
            }
            LogicalOperator::IndexScan(scan) => {
                scan.index_name.hash(hasher);
            }
            LogicalOperator::Filter(_) => {
                // Filter predicates are structural - would need deep comparison
                // For now, rely on default discrimination
            }
            LogicalOperator::Project(_) => {
                // Similar to filter
            }
            LogicalOperator::Join(join) => {
                std::mem::discriminant(&join.join_type).hash(hasher);
            }
            LogicalOperator::Aggregate(_) => {}
            LogicalOperator::Sort(_) => {}
            LogicalOperator::Limit(limit) => {
                limit.limit.hash(hasher);
            }
            LogicalOperator::Distinct(_) => {}
            LogicalOperator::Union(u) => {
                u.all.hash(hasher);
            }
            LogicalOperator::Intersect(i) => {
                i.all.hash(hasher);
            }
            LogicalOperator::Except(e) => {
                e.all.hash(hasher);
            }
            LogicalOperator::Insert(ins) => {
                ins.table_id.hash(hasher);
            }
            LogicalOperator::Update(upd) => {
                upd.table_id.hash(hasher);
            }
            LogicalOperator::Delete(del) => {
                del.table_id.hash(hasher);
            }
            LogicalOperator::Values(_) => {}
            LogicalOperator::Empty(_) => {}
        }
    }

    /// Get all groups
    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups.values()
    }

    /// Update statistics for a group
    pub fn update_stats(&mut self, group_id: GroupId, stats: Statistics) {
        if let Some(group) = self.groups.get_mut(&group_id) {
            group.stats = stats;
        }
    }

    /// Merge two groups (used when expressions are found to be equivalent)
    pub fn merge_groups(&mut self, group1: GroupId, group2: GroupId) -> GroupId {
        if group1 == group2 {
            return group1;
        }

        // Keep the lower-numbered group
        let (keep, remove) = if group1.0 < group2.0 {
            (group1, group2)
        } else {
            (group2, group1)
        };

        // Move expressions from removed group to kept group
        if let Some(removed) = self.groups.remove(&remove) {
            if let Some(kept) = self.groups.get_mut(&keep) {
                for expr in removed.logical_exprs {
                    kept.add_logical_expr(expr);
                }
                for expr in removed.physical_exprs {
                    kept.add_physical_expr(expr);
                }
            }
        }

        // Update hash map entries
        for (_, gid) in self.expr_hash.iter_mut() {
            if *gid == remove {
                *gid = keep;
            }
        }

        keep
    }
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::schema::{Column, Schema};
    use crate::sql::planner::logical::{LogicalOperator, TableScanOp};
    use crate::types::{DataTypeKind, ObjectId};

    fn test_schema() -> Schema {
        Schema::from_columns(&[Column::new_unindexed(DataTypeKind::Int, "id", None)], 1)
    }

    #[test]
    fn test_memo_create_group() {
        let mut memo = Memo::new();
        let props = LogicalProperties::default();
        let group_id = memo.new_group(props);

        assert_eq!(group_id.0, 0);
        assert!(memo.get_group(group_id).is_some());
    }

    #[test]
    fn test_memo_insert_expr() {
        let mut memo = Memo::new();
        let schema = test_schema();

        let expr = LogicalExpr::new(
            LogicalOperator::TableScan(TableScanOp::new(
                ObjectId::new(),
                "test".to_string(),
                schema,
            )),
            vec![],
        );

        let group_id = memo.insert_logical_expr(expr.clone());

        // Same expression should return same group
        let group_id2 = memo.insert_logical_expr(expr);
        assert_eq!(group_id, group_id2);
    }

    #[test]
    fn test_memo_deduplication() {
        let mut memo = Memo::new();
        let schema = test_schema();

        let expr1 = LogicalExpr::new(
            LogicalOperator::TableScan(TableScanOp::new(
                ObjectId::new(),
                "table1".to_string(),
                schema.clone(),
            )),
            vec![],
        );

        let expr2 = LogicalExpr::new(
            LogicalOperator::TableScan(TableScanOp::new(
                ObjectId::new(),
                "table2".to_string(),
                schema,
            )),
            vec![],
        );

        let group1 = memo.insert_logical_expr(expr1);
        let group2 = memo.insert_logical_expr(expr2);

        // Different tables should create different groups
        assert_ne!(group1, group2);
    }

    #[test]
    fn test_group_add_expressions() {
        let mut memo = Memo::new();
        let props = LogicalProperties::default();
        let group_id = memo.new_group(props);

        let schema = test_schema();
        let expr = LogicalExpr::new(
            LogicalOperator::TableScan(TableScanOp::new(
                ObjectId::new(),
                "test".to_string(),
                schema,
            )),
            vec![],
        );

        let expr_id = memo.add_logical_expr_to_group(group_id, expr);
        assert!(expr_id.is_some());

        let group = memo.get_group(group_id).unwrap();
        assert_eq!(group.logical_exprs.len(), 1);
    }

    #[test]
    fn test_memo_statistics() {
        let mut memo = Memo::new();
        let props = LogicalProperties::default();
        let group_id = memo.new_group(props);

        let stats = Statistics {
            row_count: 1000.0,
            ..Default::default()
        };

        memo.update_stats(group_id, stats.clone());

        let group = memo.get_group(group_id).unwrap();
        assert_eq!(group.stats.row_count, 1000.0);
    }
}
