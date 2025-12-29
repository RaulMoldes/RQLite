//! Plan exporters
//!
//! This module provides text and Graphviz export for query plans.

use std::fmt::Write;

use crate::sql::{binder::bounds::*, parser::ast::BinaryOperator};

use super::logical::*;
use super::physical::*;
use super::prop::{LogicalProperties, PhysicalProperties};

const MAX_EXPR_LEN: usize = 30;

/// Summarizes a bound expression to a short string.
pub fn summarize_expr(expr: &BoundExpression, max_len: usize) -> String {
    let s = format_expr(expr);
    if s.len() <= max_len {
        s
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Formats a bound expression to a readable string.
fn format_expr(expr: &BoundExpression) -> String {
    match expr {
        BoundExpression::ColumnRef(c) => format!("col[{}]", c.column_idx),
        BoundExpression::Literal { value } => format!("{:?}", value),
        BoundExpression::BinaryOp {
            left, op, right, ..
        } => {
            format!(
                "{} {} {}",
                format_expr(left),
                format_op(op),
                format_expr(right)
            )
        }
        BoundExpression::UnaryOp { op, expr, .. } => {
            format!("{:?} {}", op, format_expr(expr))
        }
        BoundExpression::IsNull { expr, negated } => {
            if *negated {
                format!("{} IS NOT NULL", format_expr(expr))
            } else {
                format!("{} IS NULL", format_expr(expr))
            }
        }
        BoundExpression::InList {
            expr,
            list,
            negated,
        } => {
            let op = if *negated { "NOT IN" } else { "IN" };
            format!("{} {} ({} items)", format_expr(expr), op, list.len())
        }
        BoundExpression::Between { expr, negated, .. } => {
            let op = if *negated { "NOT BETWEEN" } else { "BETWEEN" };
            format!("{} {}", format_expr(expr), op)
        }
        BoundExpression::Aggregate { func, .. } => format!("{:?}(...)", func),
        BoundExpression::Function { func, .. } => format!("{:?}(...)", func),
        BoundExpression::Case { .. } => "CASE...END".to_string(),
        BoundExpression::Exists { negated, .. } => if *negated {
            "NOT EXISTS(...)"
        } else {
            "EXISTS(...)"
        }
        .to_string(),
        BoundExpression::Star => "*".to_string(),
        BoundExpression::Subquery { .. } => "SUBQUERY(...)".to_string(),
        BoundExpression::InSubquery { negated, .. } => if *negated {
            "NOT IN SUBQUERY(...)"
        } else {
            "IN SUBQUERY(...)"
        }
        .to_string(),
    }
}

fn format_op(op: &BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::Eq => "=",
        BinaryOperator::Neq => "!=",
        BinaryOperator::Lt => "<",
        BinaryOperator::Le => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::Ge => ">=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::Like => "LIKE",
        BinaryOperator::Concat => "CONCAT",
        BinaryOperator::IsNot => "IS NOT",
        BinaryOperator::Is => "IS",
        BinaryOperator::In => "IN",
        BinaryOperator::NotLike => "NOT LIKE",
        BinaryOperator::NotIn => "NOT IN",
    }
}

// Visitor traits

pub trait LogicalPlanVisitor {
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize);
    fn leave_plan(&mut self, plan: &LogicalPlan, depth: usize);
    fn visit_operator(&mut self, op: &LogicalOperator, props: &LogicalProperties, depth: usize);
}

pub trait PhysicalPlanVisitor {
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize);
    fn leave_plan(&mut self, plan: &PhysicalPlan, depth: usize);
    fn visit_operator(
        &mut self,
        op: &PhysicalOperator,
        props: &PhysicalProperties,
        cost: f64,
        depth: usize,
    );
}

pub trait AcceptLogicalVisitor {
    fn accept<V: LogicalPlanVisitor>(&self, visitor: &mut V);
    fn accept_with_depth<V: LogicalPlanVisitor>(&self, visitor: &mut V, depth: usize);
}

pub trait AcceptPhysicalVisitor {
    fn accept<V: PhysicalPlanVisitor>(&self, visitor: &mut V);
    fn accept_with_depth<V: PhysicalPlanVisitor>(&self, visitor: &mut V, depth: usize);
}

impl AcceptLogicalVisitor for LogicalPlan {
    fn accept<V: LogicalPlanVisitor>(&self, visitor: &mut V) {
        self.accept_with_depth(visitor, 0);
    }

    fn accept_with_depth<V: LogicalPlanVisitor>(&self, visitor: &mut V, depth: usize) {
        visitor.enter_plan(self, depth);
        visitor.visit_operator(&self.op, &self.properties, depth);
        for child in &self.children {
            child.accept_with_depth(visitor, depth + 1);
        }
        visitor.leave_plan(self, depth);
    }
}

impl AcceptPhysicalVisitor for PhysicalPlan {
    fn accept<V: PhysicalPlanVisitor>(&self, visitor: &mut V) {
        self.accept_with_depth(visitor, 0);
    }

    fn accept_with_depth<V: PhysicalPlanVisitor>(&self, visitor: &mut V, depth: usize) {
        visitor.enter_plan(self, depth);
        visitor.visit_operator(&self.op, &self.properties, self.cost, depth);
        for child in &self.children {
            child.accept_with_depth(visitor, depth + 1);
        }
        visitor.leave_plan(self, depth);
    }
}

// Text exporter
pub struct TextExporter {
    output: String,
    indent: String,
    show_cost: bool,
}

impl TextExporter {
    pub fn new() -> Self {
        Self {
            output: String::new(),
            indent: "  ".to_string(),
            show_cost: true,
        }
    }

    pub fn minimal() -> Self {
        Self {
            show_cost: false,
            ..Self::new()
        }
    }

    pub fn finish(self) -> String {
        self.output
    }

    fn indent(&self, depth: usize) -> String {
        self.indent.repeat(depth)
    }
}

impl Default for TextExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalPlanVisitor for TextExporter {
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize) {
        let indent = self.indent(depth);
        let _ = writeln!(self.output, "{}├─ {}", indent, format_logical_op(&plan.op));

        if plan.properties.cardinality > 0.0 {
            let _ = writeln!(
                self.output,
                "{}│  rows: {:.0}",
                indent, plan.properties.cardinality
            );
        }
    }

    fn leave_plan(&mut self, _plan: &LogicalPlan, _depth: usize) {}
    fn visit_operator(&mut self, _op: &LogicalOperator, _props: &LogicalProperties, _depth: usize) {
    }
}

impl PhysicalPlanVisitor for TextExporter {
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize) {
        let indent = self.indent(depth);

        if self.show_cost {
            let _ = writeln!(
                self.output,
                "{}├─ {} (cost: {:.2})",
                indent,
                format_physical_op(&plan.op),
                plan.cost
            );
        } else {
            let _ = writeln!(self.output, "{}├─ {}", indent, format_physical_op(&plan.op));
        }
    }

    fn leave_plan(&mut self, _plan: &PhysicalPlan, _depth: usize) {}
    fn visit_operator(
        &mut self,
        _op: &PhysicalOperator,
        _props: &PhysicalProperties,
        _cost: f64,
        _depth: usize,
    ) {
    }
}

fn format_logical_op(op: &LogicalOperator) -> String {
    match op {
        LogicalOperator::TableScan(s) => format!("TableScan({})", s.table_name),
        LogicalOperator::IndexScan(s) => format!("IndexScan(idx={})", s.index_id),
        LogicalOperator::Filter(f) => {
            format!("Filter({})", summarize_expr(&f.predicate, MAX_EXPR_LEN))
        }
        LogicalOperator::Project(p) => format!("Project({} exprs)", p.expressions.len()),
        LogicalOperator::Join(j) => format!("{:?}Join", j.join_type),
        LogicalOperator::Aggregate(a) => format!(
            "Aggregate({} groups, {} aggs)",
            a.group_by.len(),
            a.aggregates.len()
        ),
        LogicalOperator::Sort(s) => format!("Sort({} keys)", s.order_by.len()),
        LogicalOperator::Limit(l) => {
            if let Some(off) = l.offset {
                format!("Limit({}, offset {})", l.limit, off)
            } else {
                format!("Limit({})", l.limit)
            }
        }
        LogicalOperator::Distinct(_) => "Distinct".to_string(),
        LogicalOperator::Insert(i) => format!("Insert({} cols)", i.columns.len()),
        LogicalOperator::Update(u) => format!("Update({} assigns)", u.assignments.len()),
        LogicalOperator::Delete(_) => "Delete".to_string(),
        LogicalOperator::Values(v) => format!("Values({} rows)", v.rows.len()),
        LogicalOperator::Empty(_) => "Empty".to_string(),
        LogicalOperator::Materialize(_) => "Materialize".to_string(),
    }
}

fn format_physical_op(op: &PhysicalOperator) -> String {
    match op {
        PhysicalOperator::SeqScan(s) => format!("SeqScan({})", s.table_id),
        PhysicalOperator::IndexScan(s) => format!("IndexScan({}.{})", s.table_id, s.index_id),
        PhysicalOperator::Filter(f) => {
            format!("Filter({})", summarize_expr(&f.predicate, MAX_EXPR_LEN))
        }
        PhysicalOperator::Project(_) => "Project".to_string(),
        PhysicalOperator::NestedLoopJoin(j) => format!("NLJoin({:?})", j.join_type),
        PhysicalOperator::HashJoin(j) => format!("HashJoin({:?})", j.join_type),
        PhysicalOperator::MergeJoin(j) => format!("MergeJoin({:?})", j.join_type),
        PhysicalOperator::HashAggregate(_) => "HashAggregate".to_string(),
        PhysicalOperator::Sort(s) => format!("Sort({} keys)", s.order_by.len()),
        PhysicalOperator::Limit(l) => format!("Limit({})", l.limit),
        PhysicalOperator::Distinct(_) => "Distinct".to_string(),
        PhysicalOperator::Insert(_) => "Insert".to_string(),
        PhysicalOperator::Update(_) => "Update".to_string(),
        PhysicalOperator::Delete(_) => "Delete".to_string(),
        PhysicalOperator::Values(_) => "Values".to_string(),
        PhysicalOperator::Empty(_) => "Empty".to_string(),
        PhysicalOperator::Materialize(_) => "Materialize".to_string(),
    }
}

// Graphviz exporter

pub struct GraphvizExporter {
    output: String,
    node_counter: u32,
    parent_stack: Vec<u32>,
    title: String,
}

impl GraphvizExporter {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            output: String::new(),
            node_counter: 0,
            parent_stack: Vec::new(),
            title: title.into(),
        }
    }

    pub fn finish(mut self) -> String {
        self.write_footer();
        self.output
    }

    fn write_header(&mut self) {
        let _ = writeln!(self.output, "digraph QueryPlan {{");
        let _ = writeln!(self.output, "  rankdir=TB;");
        let _ = writeln!(
            self.output,
            "  node [shape=box, style=\"filled,rounded\", fontsize=10];"
        );
        let _ = writeln!(self.output, "  edge [color=\"#999999\", arrowsize=0.7];");
        if !self.title.is_empty() {
            let _ = writeln!(
                self.output,
                "  label=\"{}\"; labelloc=t;",
                escape_graphviz(&self.title)
            );
        }
        let _ = writeln!(self.output);
    }

    fn write_footer(&mut self) {
        let _ = writeln!(self.output, "}}");
    }

    fn alloc_node(&mut self) -> u32 {
        let id = self.node_counter;
        self.node_counter += 1;
        id
    }

    fn write_node(&mut self, id: u32, label: &str, color: &str) {
        let _ = writeln!(
            self.output,
            "  node{} [label=\"{}\", fillcolor=\"{}\"];",
            id,
            escape_graphviz(label),
            color
        );
    }

    fn write_edge(&mut self, from: u32, to: u32) {
        let _ = writeln!(self.output, "  node{} -> node{};", from, to);
    }
}

impl LogicalPlanVisitor for GraphvizExporter {
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize) {
        if depth == 0 {
            self.write_header();
        }

        let id = self.alloc_node();
        let label = format_logical_op(&plan.op);
        let color = logical_op_color(&plan.op);
        self.write_node(id, &label, color);

        if let Some(&parent) = self.parent_stack.last() {
            self.write_edge(parent, id);
        }
        self.parent_stack.push(id);
    }

    fn leave_plan(&mut self, _plan: &LogicalPlan, _depth: usize) {
        self.parent_stack.pop();
    }

    fn visit_operator(&mut self, _op: &LogicalOperator, _props: &LogicalProperties, _depth: usize) {
    }
}

impl PhysicalPlanVisitor for GraphvizExporter {
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize) {
        if depth == 0 {
            self.write_header();
        }

        let id = self.alloc_node();
        let label = format!("{}\ncost: {:.2}", format_physical_op(&plan.op), plan.cost);
        let color = physical_op_color(&plan.op);
        self.write_node(id, &label, color);

        if let Some(&parent) = self.parent_stack.last() {
            self.write_edge(parent, id);
        }
        self.parent_stack.push(id);
    }

    fn leave_plan(&mut self, _plan: &PhysicalPlan, _depth: usize) {
        self.parent_stack.pop();
    }

    fn visit_operator(
        &mut self,
        _op: &PhysicalOperator,
        _props: &PhysicalProperties,
        _cost: f64,
        _depth: usize,
    ) {
    }
}

fn logical_op_color(op: &LogicalOperator) -> &'static str {
    match op {
        LogicalOperator::TableScan(_) | LogicalOperator::IndexScan(_) => "#f0f0f0",
        LogicalOperator::Filter(_) | LogicalOperator::Project(_) => "#d4edda",
        LogicalOperator::Join(_) => "#ffe5cc",
        LogicalOperator::Aggregate(_) | LogicalOperator::Sort(_) => "#d4edda",
        LogicalOperator::Limit(_) | LogicalOperator::Distinct(_) => "#ffe5cc",
        _ => "#f0f0f0",
    }
}

fn physical_op_color(op: &PhysicalOperator) -> &'static str {
    match op {
        PhysicalOperator::SeqScan(_) | PhysicalOperator::IndexScan(_) => "#f0f0f0",
        PhysicalOperator::Filter(_) | PhysicalOperator::Project(_) => "#d4edda",
        PhysicalOperator::NestedLoopJoin(_)
        | PhysicalOperator::HashJoin(_)
        | PhysicalOperator::MergeJoin(_) => "#ffe5cc",
        PhysicalOperator::HashAggregate(_) | PhysicalOperator::Sort(_) => "#d4edda",
        PhysicalOperator::Limit(_) | PhysicalOperator::Distinct(_) => "#ffe5cc",
        _ => "#f0f0f0",
    }
}

fn escape_graphviz(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "")
        .replace('<', "\\<")
        .replace('>', "\\>")
}

// Convenience methods on plans

impl LogicalPlan {
    /// Exports to Graphviz DOT format.
    pub fn to_graphviz(&self, title: &str) -> String {
        let mut exporter = GraphvizExporter::new(title);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty prints the plan as a tree.
    pub fn explain(&self) -> String {
        let mut exporter = TextExporter::new();
        self.accept(&mut exporter);
        exporter.finish()
    }
}

impl PhysicalPlan {
    /// Exports to Graphviz DOT format.
    pub fn to_graphviz(&self, title: &str) -> String {
        let mut exporter = GraphvizExporter::new(title);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty prints the plan as a tree.
    pub fn explain(&self) -> String {
        let mut exporter = TextExporter::new();
        self.accept(&mut exporter);
        exporter.finish()
    }
}
