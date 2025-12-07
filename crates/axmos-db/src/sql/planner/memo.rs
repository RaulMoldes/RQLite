//! Memo structure for the Cascades optimizer.

use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use super::{
    logical::{LogicalExpr, LogicalOperator},
    physical::PhysicalExpr,
    prop::{LogicalProperties, RequiredProperties},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct GroupId(pub usize);

impl GroupId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

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

#[derive(Debug, Clone)]
pub struct Winner {
    pub expr_id: ExprId,
    pub cost: f64,
}

#[derive(Debug, Clone)]
pub struct Group {
    pub id: GroupId,
    pub logical_props: LogicalProperties,
    pub logical_exprs: Vec<LogicalExpr>,
    pub physical_exprs: Vec<PhysicalExpr>,
    pub winners: HashMap<RequiredProperties, Option<Winner>>,
    pub explored: bool,
}

impl Group {
    pub fn new(id: GroupId, logical_props: LogicalProperties) -> Self {
        Self {
            id,
            logical_props,
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            winners: HashMap::new(),
            explored: false,
        }
    }

    pub fn add_logical_expr(&mut self, mut expr: LogicalExpr) -> ExprId {
        let expr_idx = self.logical_exprs.len();
        expr.id = ExprId::logical(self.id, expr_idx);
        self.logical_exprs.push(expr);
        ExprId::logical(self.id, expr_idx)
    }

    pub fn add_physical_expr(&mut self, mut expr: PhysicalExpr) -> ExprId {
        let expr_idx = self.physical_exprs.len();
        expr.id = ExprId::physical(self.id, expr_idx);
        self.physical_exprs.push(expr);
        ExprId::physical(self.id, expr_idx)
    }

    pub fn get_winner(&self, required: &RequiredProperties) -> Option<&Winner> {
        self.winners.get(required).and_then(|w| w.as_ref())
    }

    pub fn set_winner(&mut self, required: RequiredProperties, winner: Option<Winner>) {
        self.winners.insert(required, winner);
    }

    pub fn has_explored(&self, required: &RequiredProperties) -> bool {
        self.winners.contains_key(required)
    }
}

pub struct Memo {
    groups: Vec<Group>,
    expr_to_group: HashMap<u64, GroupId>,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            expr_to_group: HashMap::new(),
        }
    }

    pub fn new_group(&mut self, logical_props: LogicalProperties) -> GroupId {
        let id = GroupId(self.groups.len());
        self.groups.push(Group::new(id, logical_props));
        id
    }

    pub fn insert_logical_expr(&mut self, expr: LogicalExpr) -> GroupId {
        let hash = self.compute_expr_hash(&expr);

        if let Some(&group_id) = self.expr_to_group.get(&hash) {
            return group_id;
        }

        let group_id = self.new_group(expr.properties.clone());
        self.expr_to_group.insert(hash, group_id);
        self.groups[group_id.0].add_logical_expr(expr);
        group_id
    }

    pub fn add_logical_expr_to_group(
        &mut self,
        group_id: GroupId,
        expr: LogicalExpr,
    ) -> Option<ExprId> {
        let hash = self.compute_expr_hash(&expr);
        if self.expr_to_group.contains_key(&hash) {
            return None;
        }
        self.expr_to_group.insert(hash, group_id);
        Some(self.groups[group_id.0].add_logical_expr(expr))
    }

    pub fn add_physical_expr_to_group(&mut self, group_id: GroupId, expr: PhysicalExpr) -> ExprId {
        self.groups[group_id.0].add_physical_expr(expr)
    }

    pub fn get_group(&self, id: GroupId) -> Option<&Group> {
        self.groups.get(id.0)
    }

    pub fn get_group_mut(&mut self, id: GroupId) -> Option<&mut Group> {
        self.groups.get_mut(id.0)
    }

    pub fn get_logical_expr(&self, id: ExprId) -> Option<&LogicalExpr> {
        self.groups
            .get(id.group_id.0)?
            .logical_exprs
            .get(id.expr_idx)
    }

    pub fn get_physical_expr(&self, id: ExprId) -> Option<&PhysicalExpr> {
        self.groups
            .get(id.group_id.0)?
            .physical_exprs
            .get(id.expr_idx)
    }

    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    fn compute_expr_hash(&self, expr: &LogicalExpr) -> u64 {
        let mut hasher = DefaultHasher::new();
        std::mem::discriminant(&expr.op).hash(&mut hasher);
        self.hash_operator(&expr.op, &mut hasher);
        for child in &expr.children {
            child.0.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn hash_operator(&self, op: &LogicalOperator, hasher: &mut DefaultHasher) {
        match op {
            LogicalOperator::TableScan(scan) => {
                scan.table_id.hash(hasher);
                scan.table_name.hash(hasher);
            }
            LogicalOperator::IndexScan(scan) => {
                scan.table_id.hash(hasher);
                scan.index_id.hash(hasher);
            }
            LogicalOperator::Join(join) => {
                std::mem::discriminant(&join.join_type).hash(hasher);
            }
            LogicalOperator::Limit(limit) => {
                limit.limit.hash(hasher);
                limit.offset.hash(hasher);
            }
            LogicalOperator::Union(u) => u.all.hash(hasher),
            LogicalOperator::Intersect(i) => i.all.hash(hasher),
            LogicalOperator::Except(e) => e.all.hash(hasher),
            LogicalOperator::Insert(ins) => ins.table_id.hash(hasher),
            LogicalOperator::Update(upd) => upd.table_id.hash(hasher),
            LogicalOperator::Delete(del) => del.table_id.hash(hasher),
            _ => {}
        }
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups.iter()
    }
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}
