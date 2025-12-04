//! Plan exporters using the visitor pattern.
//!
//! This module provides a flexible way to export logical and physical plans
//! to various formats (Graphviz, JSON, text, etc.) using the visitor pattern.
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult, Write},
};

use crate::database::schema::Schema;

const MAX_EXPR_LEN: usize = 30;

use super::logical::*;
use super::physical::*;
use super::prop::{LogicalProperties, PhysicalProperties};

/// Visitor trait for exporting logical plans.
///
/// Implementors define how each logical operator type should be exported.
pub trait LogicalPlanVisitor {
    /// Called when entering a plan node (before visiting children).
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize);

    /// Called when leaving a plan node (after visiting children).
    fn leave_plan(&mut self, plan: &LogicalPlan, depth: usize);

    /// Visit a specific logical operator.
    fn visit_operator(&mut self, op: &LogicalOperator, props: &LogicalProperties, depth: usize);
}

/// Visitor trait for exporting physical plans.
///
/// Implementors define how each physical operator type should be exported.
pub trait PhysicalPlanVisitor {
    /// Called when entering a plan node (before visiting children).
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize);

    /// Called when leaving a plan node (after visiting children).
    fn leave_plan(&mut self, plan: &PhysicalPlan, depth: usize);

    /// Visit a specific physical operator.
    fn visit_operator(
        &mut self,
        op: &PhysicalOperator,
        props: &PhysicalProperties,
        cost: f64,
        depth: usize,
    );
}

/// Trait for types that can be visited by a logical plan visitor.
pub trait AcceptLogicalVisitor {
    fn accept<V: LogicalPlanVisitor>(&self, visitor: &mut V);
    fn accept_with_depth<V: LogicalPlanVisitor>(&self, visitor: &mut V, depth: usize);
}

/// Trait for types that can be visited by a physical plan visitor.
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

/// Plan exporter trait
pub trait PlanExporter: Sized {
    type Output;

    /// Finish exporting and return the final output.
    fn finish(self) -> Self::Output;
}

/// Configuration options for Graphviz export.
#[derive(Debug, Clone)]
pub struct GraphvizConfig {
    /// Title for the graph.
    pub title: String,
    /// Graph direction (TB, BT, LR, RL).
    pub rank_dir: RankDir,
    /// Whether to show schema information.
    pub show_schema: bool,
    /// Whether to show cardinality/cost estimates.
    pub show_estimates: bool,
    /// Whether to show detailed operator info.
    pub show_details: bool,
    /// Font name for labels.
    pub font_name: String,
    /// Font size for labels.
    pub font_size: u32,
    /// Node spacing (ranksep).
    pub rank_sep: f32,
    /// Sibling spacing (nodesep).
    pub node_sep: f32,
    /// Background color.
    pub bg_color: Option<String>,
    /// Edge style.
    pub edge_style: EdgeStyle,
}

/// Edge visual style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EdgeStyle {
    #[default]
    Straight,
    Orthogonal,
    Curved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankDir {
    TopToBottom,
    BottomToTop,
    LeftToRight,
    RightToLeft,
}

impl RankDir {
    fn as_str(&self) -> &'static str {
        match self {
            Self::TopToBottom => "TB",
            Self::BottomToTop => "BT",
            Self::LeftToRight => "LR",
            Self::RightToLeft => "RL",
        }
    }
}

impl Default for GraphvizConfig {
    fn default() -> Self {
        Self {
            title: String::new(),
            show_schema: true,
            show_estimates: true,
            show_details: true,
            font_name: "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif".to_string(),
            font_size: 12,
            rank_dir: RankDir::TopToBottom,
            rank_sep: 0.5,
            node_sep: 0.3,
            bg_color: None,
            edge_style: EdgeStyle::Straight,
        }
    }
}

/// Graphviz DOT format exporter for query plans.
pub struct GraphvizExporter {
    config: GraphvizConfig,
    output: String,
    node_counter: u32,
    /// Stack of parent node IDs for edge creation.
    parent_stack: Vec<u32>,
    /// Map from plan pointer to node ID (for deduplication).
    node_ids: HashMap<usize, u32>,
}

impl GraphvizExporter {
    pub fn new(title: impl Into<String>) -> Self {
        Self::with_config(GraphvizConfig {
            title: title.into(),
            ..Default::default()
        })
    }

    pub fn with_config(config: GraphvizConfig) -> Self {
        Self {
            config,
            output: String::new(),
            node_counter: 0,
            parent_stack: Vec::new(),
            node_ids: std::collections::HashMap::new(),
        }
    }

    fn write_header(&mut self) {
        let _ = writeln!(self.output, "digraph QueryPlan {{");
        let _ = writeln!(self.output, "  // Graph settings");
        let _ = writeln!(self.output, "  rankdir={};", self.config.rank_dir.as_str());
        let _ = writeln!(self.output, "  ranksep={};", self.config.rank_sep);
        let _ = writeln!(self.output, "  nodesep={};", self.config.node_sep);
        let _ = writeln!(
            self.output,
            "  splines={};",
            match self.config.edge_style {
                EdgeStyle::Straight => "line",
                EdgeStyle::Orthogonal => "ortho",
                EdgeStyle::Curved => "curved",
            }
        );

        if let Some(bg) = &self.config.bg_color {
            let _ = writeln!(self.output, "  bgcolor=\"{}\";", bg);
        }

        let _ = writeln!(
            self.output,
            "  node [shape=box, style=\"filled,rounded\", fontname=\"{}\", fontsize={}, margin=\"0.2,0.1\"];",
            self.config.font_name, self.config.font_size.saturating_sub(3)
        );

        let _ = writeln!(
            self.output,
            "  edge [fontname=\"{}\", fontsize={}, color=\"#999999\", arrowsize=0.7];",
            self.config.font_name,
            self.config.font_size.saturating_sub(2)
        );

        // Title
        if !self.config.title.is_empty() {
            let _ = writeln!(
                self.output,
                "  label=\"{}\";",
                escape_graphviz(&self.config.title)
            );
        }
        let _ = writeln!(self.output, "  labelloc=t;");
        let _ = writeln!(self.output, "  labeljust=c;");

        let _ = writeln!(self.output);
    }

    fn write_footer(&mut self) {
        let _ = writeln!(self.output, "}}");
    }

    fn allocate_node_id(&mut self) -> u32 {
        let id = self.node_counter;
        self.node_counter += 1;
        id
    }

    fn write_node(&mut self, id: u32, label: &str, shape: &str, color: &str) {
        let _ = writeln!(
            self.output,
            "  node{} [label=\"{}\", shape={}, fillcolor=\"{}\"];",
            id,
            escape_graphviz(label),
            shape,
            color
        );
    }

    fn write_edge(&mut self, from: u32, to: u32, label: Option<&str>) {
        if let Some(lbl) = label {
            let _ = writeln!(
                self.output,
                "  node{} -> node{} [label=\"{}\"];",
                from,
                to,
                escape_graphviz(lbl)
            );
        } else {
            let _ = writeln!(self.output, "  node{} -> node{};", from, to);
        }
    }

    fn draw_physical_node(&mut self, id: u32, plan: &PhysicalPlan) {
        let label = self.build_label(&plan.op);
        let shape = plan.op.shape();
        let color = plan.op.color();
        self.write_node(id, &label, shape, color);
    }

    fn draw_logical_node(&mut self, id: u32, plan: &LogicalPlan) {
        let label = self.build_label(&plan.op);
        let shape = plan.op.shape();
        let color = plan.op.color();
        self.write_node(id, &label, shape, color);
    }

    fn build_details<D: DrawableNode>(&self, op: &D) -> (String, Vec<(&'static str, String)>) {
        let op_name = format!("{op}");
        let mut details: Vec<(&'static str, String)> = Vec::new();

        // Output columns
        if self.config.show_schema {
            let schema = op.get_schema();
            let cols = schema.columns();
            let col_str = if cols.len() <= 3 {
                cols.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                format!("{} columns", cols.len())
            };
            details.push(("Output", col_str));
        }

        // Operator-specific
        if self.config.show_details {
            if let Some(detailed) = op.detail() {
                details.push(("Details:", format!("{detailed}")));
            }
        }

        (op_name, details)
    }

    fn build_label<D: DrawableNode>(&self, op: &D) -> String {
        let mut parts = Vec::new();

        // Append the marker to the beginning
        let icon = op.marker();

        parts.push(format!("{} {}", icon, op));

        if self.config.show_schema {
            let schema = op.get_schema();
            let cols = schema.columns();
            if cols.len() <= 4 {
                let col_names: Vec<_> = cols.iter().map(|c| c.name.as_str()).collect();
                parts.push(format!("cols: {}", col_names.join(", ")));
            } else {
                parts.push(format!("cols: {} columns", cols.len()));
            }
        }

        if self.config.show_details {
            if let Some(detail) = op.detail() {
                parts.push(detail);
            }
        }

        parts.join("\n")
    }
}

impl PlanExporter for GraphvizExporter {
    type Output = String;

    fn finish(mut self) -> String {
        self.write_footer();
        self.output
    }
}

impl LogicalPlanVisitor for GraphvizExporter {
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize) {
        if depth == 0 {
            self.write_header();
        }

        let node_id = self.allocate_node_id();
        let ptr = plan as *const _ as usize;
        self.node_ids.insert(ptr, node_id);

        self.draw_logical_node(node_id, plan);

        // Create edge from parent
        if let Some(&parent_id) = self.parent_stack.last() {
            self.write_edge(parent_id, node_id, None);
        }

        self.parent_stack.push(node_id);
    }

    fn leave_plan(&mut self, _plan: &LogicalPlan, _depth: usize) {
        self.parent_stack.pop();
    }

    fn visit_operator(&mut self, _op: &LogicalOperator, _props: &LogicalProperties, _depth: usize) {
        // Main work is done in enter_plan
    }
}

impl PhysicalPlanVisitor for GraphvizExporter {
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize) {
        if depth == 0 {
            self.write_header();
        }

        let node_id = self.allocate_node_id();
        let ptr = plan as *const _ as usize;
        self.node_ids.insert(ptr, node_id);

        self.draw_physical_node(node_id, plan);

        // Create edge from parent
        if let Some(&parent_id) = self.parent_stack.last() {
            self.write_edge(parent_id, node_id, None);
        }

        self.parent_stack.push(node_id);
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
        // Main work is done in enter_plan
    }
}

fn escape_graphviz(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "")
        .replace('<', "\\<")
        .replace('>', "\\>")
        .replace('{', "\\{")
        .replace('}', "\\}")
        .replace('|', "\\|")
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

impl Display for LogicalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            LogicalOperator::TableScan(scan) => write!(f, "TableScan({})", scan.table_name),
            LogicalOperator::IndexScan(scan) => write!(f, "IndexScan({})", scan.index_name),
            LogicalOperator::Filter(_) => write!(f, "Filter"),
            LogicalOperator::Project(p) => write!(f, "Project({} exprs)", p.expressions.len()),
            LogicalOperator::Join(j) => write!(f, "{:?} Join", j.join_type),
            LogicalOperator::Union(u) => {
                if u.all {
                    write!(f, "Union All")
                } else {
                    write!(f, "Union")
                }
            }
            LogicalOperator::Intersect(i) => {
                if i.all {
                    write!(f, "Intersect All")
                } else {
                    write!(f, "Intersect")
                }
            }
            LogicalOperator::Except(e) => {
                if e.all {
                    write!(f, "Except All")
                } else {
                    write!(f, "Except")
                }
            }
            LogicalOperator::Aggregate(a) => {
                write!(
                    f,
                    "Aggregate({} groups, {} aggs)",
                    a.group_by.len(),
                    a.aggregates.len()
                )
            }
            LogicalOperator::Sort(s) => write!(f, "Sort({} keys)", s.order_by.len()),
            LogicalOperator::Limit(l) => {
                if let Some(off) = l.offset {
                    write!(f, "Limit({}, offset {})", l.limit, off)
                } else {
                    write!(f, "Limit({})", l.limit)
                }
            }
            LogicalOperator::Distinct(_) => write!(f, "Distinct"),
            LogicalOperator::Insert(i) => write!(f, "Insert({} cols)", i.columns.len()),
            LogicalOperator::Update(u) => write!(f, "Update({} assigns)", u.assignments.len()),
            LogicalOperator::Delete(_) => write!(f, "Delete"),
            LogicalOperator::Values(v) => write!(f, "Values({} rows)", v.rows.len()),
            LogicalOperator::Empty(_) => write!(f, "Empty"),
        }
    }
}

impl Display for PhysicalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            PhysicalOperator::SeqScan(s) => write!(f, "SeqScan({})", s.table_name),
            PhysicalOperator::IndexScan(s) => {
                write!(f, "IndexScan({}.{})", s.table_name, s.index_name)
            }
            PhysicalOperator::IndexOnlyScan(s) => {
                write!(f, "IndexOnlyScan({}.{})", s.table_name, s.index_name)
            }
            PhysicalOperator::Filter(_) => write!(f, "Filter"),
            PhysicalOperator::Project(_) => write!(f, "Project"),
            PhysicalOperator::NestedLoopJoin(j) => write!(f, "NLJoin({:?})", j.join_type),
            PhysicalOperator::HashJoin(j) => write!(f, "HashJoin({:?})", j.join_type),
            PhysicalOperator::MergeJoin(j) => write!(f, "MergeJoin({:?})", j.join_type),
            PhysicalOperator::HashAggregate(_) => write!(f, "HashAggregate"),
            PhysicalOperator::SortAggregate(_) => write!(f, "SortAggregate"),
            PhysicalOperator::ExternalSort(_) => write!(f, "ExternalSort"),
            PhysicalOperator::TopN(t) => write!(f, "TopN({})", t.limit),
            PhysicalOperator::HashUnion(_) => write!(f, "HashUnion"),
            PhysicalOperator::HashIntersect(_) => write!(f, "HashIntersect"),
            PhysicalOperator::HashExcept(_) => write!(f, "HashExcept"),
            PhysicalOperator::HashDistinct(_) => write!(f, "HashDistinct"),
            PhysicalOperator::SortDistinct(_) => write!(f, "SortDistinct"),
            PhysicalOperator::Limit(l) => write!(f, "Limit({})", l.limit),
            PhysicalOperator::Insert(_) => write!(f, "Insert"),
            PhysicalOperator::Update(_) => write!(f, "Update"),
            PhysicalOperator::Delete(_) => write!(f, "Delete"),
            PhysicalOperator::Values(_) => write!(f, "Values"),
            PhysicalOperator::Empty(_) => write!(f, "Empty"),
            PhysicalOperator::Sort(_) => write!(f, "Sort"),
            PhysicalOperator::Exchange(e) => write!(f, "Exchange({:?})", e.distribution),
        }
    }
}

pub trait DrawableNode: Display {
    fn get_schema(&self) -> &Schema;
    fn shape(&self) -> &'static str;
    fn color(&self) -> &'static str;
    fn marker(&self) -> &'static str;
    fn detail(&self) -> Option<String>;
}

impl DrawableNode for PhysicalOperator {
    fn get_schema(&self) -> &Schema {
        self.output_schema()
    }

    fn marker(&self) -> &'static str {
        match self {
            PhysicalOperator::SeqScan(_) => "[S]",
            PhysicalOperator::IndexScan(_) | PhysicalOperator::IndexOnlyScan(_) => "[I]",
            PhysicalOperator::Filter(_) => "[F]",
            PhysicalOperator::Project(_) => "[P]",
            PhysicalOperator::NestedLoopJoin(_) => "[NLJ]",
            PhysicalOperator::HashJoin(_) => "[HJ]",
            PhysicalOperator::MergeJoin(_) => "[MJ]",
            PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_) => "[A]",
            PhysicalOperator::ExternalSort(_) | PhysicalOperator::Sort(_) => "[S]",
            PhysicalOperator::TopN(_) => "[TN]",
            PhysicalOperator::HashUnion(_)
            | PhysicalOperator::HashIntersect(_)
            | PhysicalOperator::HashExcept(_) => "[U]",
            PhysicalOperator::HashDistinct(_) | PhysicalOperator::SortDistinct(_) => "[D]",
            PhysicalOperator::Limit(_) => "[L]",
            PhysicalOperator::Insert(_) => "[+]",
            PhysicalOperator::Update(_) => "[M]",
            PhysicalOperator::Delete(_) => "[-]",
            PhysicalOperator::Exchange(_) => "[X]",
            PhysicalOperator::Values(_) => "[V]",
            PhysicalOperator::Empty(_) => "[ ]",
        }
    }
    fn detail(&self) -> Option<String> {
        match self {
            PhysicalOperator::Filter(f) => {
                Some(format!("pred: {}", f.predicate.summarize(MAX_EXPR_LEN)))
            }
            PhysicalOperator::HashJoin(j) => {
                if !j.left_keys.is_empty() {
                    Some(format!("{} join keys", j.left_keys.len()))
                } else {
                    None
                }
            }
            PhysicalOperator::MergeJoin(j) => {
                if !j.left_keys.is_empty() {
                    Some(format!("{} merge keys", j.left_keys.len()))
                } else {
                    None
                }
            }
            PhysicalOperator::SeqScan(s) => s
                .predicate
                .as_ref()
                .map(|p| format!("filter: {}", p.summarize(MAX_EXPR_LEN))),
            PhysicalOperator::HashAggregate(a) => {
                if !a.group_by.is_empty() {
                    Some(format!(
                        "{} groups, {} aggs",
                        a.group_by.len(),
                        a.aggregates.len()
                    ))
                } else {
                    Some(format!("{} aggs", a.aggregates.len()))
                }
            }
            _ => None,
        }
    }
    fn shape(&self) -> &'static str {
        match self {
            PhysicalOperator::SeqScan(_)
            | PhysicalOperator::IndexScan(_)
            | PhysicalOperator::IndexOnlyScan(_) => "cylinder",
            PhysicalOperator::NestedLoopJoin(_)
            | PhysicalOperator::HashJoin(_)
            | PhysicalOperator::MergeJoin(_) => "diamond",
            PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_) => "hexagon",
            PhysicalOperator::ExternalSort(_) | PhysicalOperator::Sort(_) => "trapezium",
            PhysicalOperator::Filter(_) => "parallelogram",
            PhysicalOperator::Project(_) => "box",
            PhysicalOperator::HashUnion(_)
            | PhysicalOperator::HashIntersect(_)
            | PhysicalOperator::HashExcept(_) => "triangle",
            PhysicalOperator::Limit(_) | PhysicalOperator::TopN(_) => "house",
            PhysicalOperator::HashDistinct(_) | PhysicalOperator::SortDistinct(_) => "invtrapezium",
            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => "doubleoctagon",
            PhysicalOperator::Values(_) => "note",
            PhysicalOperator::Empty(_) => "point",
            PhysicalOperator::Exchange(_) => "octagon",
        }
    }

    fn color(&self) -> &'static str {
        match self {
            PhysicalOperator::SeqScan(_) => "#f0f0f0",
            PhysicalOperator::IndexScan(_) | PhysicalOperator::IndexOnlyScan(_) => "#f0f0f0",
            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => "#f0f0f0",
            PhysicalOperator::Values(_) | PhysicalOperator::Empty(_) => "#f0f0f0",

            PhysicalOperator::Filter(_) | PhysicalOperator::Project(_) => "#d4edda",
            PhysicalOperator::HashDistinct(_) | PhysicalOperator::SortDistinct(_) => "#d4edda",
            PhysicalOperator::ExternalSort(_) | PhysicalOperator::Sort(_) => "#d4edda",

            PhysicalOperator::NestedLoopJoin(_)
            | PhysicalOperator::HashJoin(_)
            | PhysicalOperator::MergeJoin(_) => "#ffe5cc",
            PhysicalOperator::HashUnion(_)
            | PhysicalOperator::HashIntersect(_)
            | PhysicalOperator::HashExcept(_) => "#ffe5cc",

            PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_) => "#d4edda",

            PhysicalOperator::Limit(_) | PhysicalOperator::TopN(_) => "#ffe5cc",
            PhysicalOperator::Exchange(_) => "#ffe5cc",
        }
    }
}

impl DrawableNode for LogicalOperator {
    fn get_schema(&self) -> &Schema {
        self.output_schema()
    }

    fn marker(&self) -> &'static str {
        match self {
            LogicalOperator::TableScan(_) => "[T]",
            LogicalOperator::IndexScan(_) => "[I]",
            LogicalOperator::Filter(_) => "[F]",
            LogicalOperator::Project(_) => "[P]",
            LogicalOperator::Join(_) => "[J]",
            LogicalOperator::Union(_)
            | LogicalOperator::Intersect(_)
            | LogicalOperator::Except(_) => "[U]",
            LogicalOperator::Aggregate(_) => "[G]",
            LogicalOperator::Sort(_) => "[S]",
            LogicalOperator::Limit(_) => "[L]",
            LogicalOperator::Distinct(_) => "[D]",
            LogicalOperator::Insert(_) => "[+]",
            LogicalOperator::Update(_) => "[M]",
            LogicalOperator::Delete(_) => "[-]",
            LogicalOperator::Values(_) => "[V]",
            LogicalOperator::Empty(_) => "[ ]",
        }
    }

    fn detail(&self) -> Option<String> {
        match self {
            LogicalOperator::Filter(f) => {
                Some(format!("pred: {}", f.predicate.summarize(MAX_EXPR_LEN)))
            }
            LogicalOperator::Join(j) => j
                .condition
                .as_ref()
                .map(|c| format!("on: {}", c.summarize(MAX_EXPR_LEN))),
            LogicalOperator::TableScan(s) => s
                .predicate
                .as_ref()
                .map(|p| format!("filter: {}", p.summarize(MAX_EXPR_LEN))),
            LogicalOperator::Aggregate(a) => {
                if !a.group_by.is_empty() {
                    let groups: Vec<_> = a
                        .group_by
                        .iter()
                        .take(3)
                        .map(|e| e.summarize(MAX_EXPR_LEN))
                        .collect();
                    let suffix = if a.group_by.len() > 3 { ", ..." } else { "" };
                    Some(format!("by: {}{}", groups.join(", "), suffix))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    fn shape(&self) -> &'static str {
        match self {
            LogicalOperator::TableScan(_) | LogicalOperator::IndexScan(_) => "cylinder",
            LogicalOperator::Join(_) => "diamond",
            LogicalOperator::Aggregate(_) => "hexagon",
            LogicalOperator::Sort(_) => "trapezium",
            LogicalOperator::Filter(_) => "parallelogram",
            LogicalOperator::Project(_) => "box",
            LogicalOperator::Union(_)
            | LogicalOperator::Intersect(_)
            | LogicalOperator::Except(_) => "triangle",
            LogicalOperator::Limit(_) => "house",
            LogicalOperator::Distinct(_) => "invtrapezium",
            LogicalOperator::Insert(_)
            | LogicalOperator::Update(_)
            | LogicalOperator::Delete(_) => "doubleoctagon",
            LogicalOperator::Values(_) => "note",
            LogicalOperator::Empty(_) => "point",
        }
    }
    fn color(&self) -> &'static str {
        match self {
            LogicalOperator::TableScan(_) | LogicalOperator::IndexScan(_) => "#f0f0f0",
            LogicalOperator::Insert(_)
            | LogicalOperator::Update(_)
            | LogicalOperator::Delete(_) => "#f0f0f0",

            LogicalOperator::Filter(_) | LogicalOperator::Project(_) => "#d4edda",
            LogicalOperator::Distinct(_) | LogicalOperator::Sort(_) => "#d4edda",

            LogicalOperator::Join(_) => "#ffe5cc",
            LogicalOperator::Union(_)
            | LogicalOperator::Intersect(_)
            | LogicalOperator::Except(_) => "#ffe5cc",

            LogicalOperator::Aggregate(_) => "#d4edda",

            LogicalOperator::Limit(_) => "#ffe5cc",

            LogicalOperator::Values(_) => "#f0f0f0",
            LogicalOperator::Empty(_) => "#f0f0f0",
        }
    }
}

/// Configuration for text export.
#[derive(Debug, Clone)]
pub struct TextConfig {
    /// Show schema information (column count).
    pub show_schema: bool,
    /// Show cardinality estimates.
    pub show_cardinality: bool,
    /// Show cost estimates (physical plans only).
    pub show_cost: bool,
    /// Show unique property.
    pub show_unique: bool,
    /// Show operator-specific details.
    pub show_details: bool,
    /// Show ordering information (physical plans only).
    pub show_ordering: bool,
    /// Indentation string (default: two spaces).
    pub indent: String,
}

impl Default for TextConfig {
    fn default() -> Self {
        Self {
            show_schema: true,
            show_cardinality: true,
            show_cost: true,
            show_unique: true,
            show_details: true,
            show_ordering: true,
            indent: "  ".to_string(),
        }
    }
}

impl TextConfig {
    /// Minimal configuration - only show operator names.
    pub fn minimal() -> Self {
        Self {
            show_schema: false,
            show_cardinality: false,
            show_cost: false,
            show_unique: false,
            show_details: false,
            show_ordering: false,
            indent: "  ".to_string(),
        }
    }

    /// Verbose configuration - show everything.
    pub fn verbose() -> Self {
        Self::default()
    }
}

/// Text exporter for human-readable plan output.
///
/// This produces the same format as the original `explain()` methods,
/// allowing those to be replaced with visitor-based implementation.
pub struct TextExporter {
    config: TextConfig,
    output: String,
}

impl TextExporter {
    pub fn new() -> Self {
        Self::with_config(TextConfig::default())
    }

    pub fn with_config(config: TextConfig) -> Self {
        Self {
            config,
            output: String::new(),
        }
    }

    pub fn minimal() -> Self {
        Self::with_config(TextConfig::minimal())
    }

    fn indent(&self, depth: usize) -> String {
        self.config.indent.repeat(depth)
    }

    fn write_line(&mut self, depth: usize, prefix: &str, content: &str) {
        let indent = self.indent(depth);
        let _ = writeln!(self.output, "{}{}  {}", indent, prefix, content);
    }
}

impl Default for TextExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanExporter for TextExporter {
    type Output = String;

    fn finish(self) -> String {
        self.output
    }
}

impl LogicalPlanVisitor for TextExporter {
    fn enter_plan(&mut self, plan: &LogicalPlan, depth: usize) {
        let indent = self.indent(depth);

        // Operator line
        let _ = writeln!(self.output, "{}├─ {}", indent, plan.op);

        // Schema information
        if self.config.show_schema {
            let _ = writeln!(
                self.output,
                "{}│  schema: {} columns",
                indent,
                plan.properties.schema.columns().len()
            );
        }

        // Cardinality
        if self.config.show_cardinality && plan.properties.cardinality > 0.0 {
            let _ = writeln!(
                self.output,
                "{}│  cardinality: {:.2}",
                indent, plan.properties.cardinality
            );
        }

        // Unique property
        if self.config.show_unique && plan.properties.unique {
            let _ = writeln!(self.output, "{}│  unique: true", indent);
        }

        // Operator-specific details
        if self.config.show_details {
            match &plan.op {
                LogicalOperator::TableScan(op) => {
                    let _ = writeln!(self.output, "{}│  table: {}", indent, op.table_name);
                    if let Some(cols) = &op.columns {
                        let _ = writeln!(self.output, "{}│  columns: {:?}", indent, cols);
                    }
                }
                LogicalOperator::Join(op) => {
                    if op.condition.is_some() {
                        let _ = writeln!(self.output, "{}│  condition: present", indent);
                    }
                }
                LogicalOperator::Aggregate(op) => {
                    let _ = writeln!(
                        self.output,
                        "{}│  group_by: {} expressions",
                        indent,
                        op.group_by.len()
                    );
                    let _ = writeln!(
                        self.output,
                        "{}│  aggregates: {} functions",
                        indent,
                        op.aggregates.len()
                    );
                }
                _ => {}
            }
        }
    }

    fn leave_plan(&mut self, _plan: &LogicalPlan, _depth: usize) {
        // Nothing to do on exit
    }

    fn visit_operator(&mut self, _op: &LogicalOperator, _props: &LogicalProperties, _depth: usize) {
        // Main work done in enter_plan
    }
}

impl PhysicalPlanVisitor for TextExporter {
    fn enter_plan(&mut self, plan: &PhysicalPlan, depth: usize) {
        let indent = self.indent(depth);

        // Operator line with cost
        if self.config.show_cost {
            let _ = writeln!(
                self.output,
                "{}├─ {} (cost: {:.2})",
                indent, plan.op, plan.cost
            );
        } else {
            let _ = writeln!(self.output, "{}├─ {}", indent, plan.op);
        }

        // Ordering information
        if self.config.show_ordering && !plan.properties.ordering.is_empty() {
            let _ = writeln!(
                self.output,
                "{}│  ordering: {:?}",
                indent, plan.properties.ordering
            );
        }
    }

    fn leave_plan(&mut self, _plan: &PhysicalPlan, _depth: usize) {
        // Nothing to do on exit
    }

    fn visit_operator(
        &mut self,
        _op: &PhysicalOperator,
        _props: &PhysicalProperties,
        _cost: f64,
        _depth: usize,
    ) {
        // Main work done in enter_plan
    }
}

impl LogicalPlan {
    /// Export to Graphviz DOT format (simple style).
    pub fn to_graphviz(&self, title: &str) -> String {
        let mut exporter = GraphvizExporter::new(title);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Export to Graphviz with custom configuration.
    pub fn to_graphviz_with_config(&self, config: GraphvizConfig) -> String {
        let mut exporter = GraphvizExporter::with_config(config);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty print the plan as a tree (using visitor pattern).
    pub fn explain(&self) -> String {
        let mut exporter = TextExporter::new();
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty print with custom configuration.
    pub fn explain_with_config(&self, config: TextConfig) -> String {
        let mut exporter = TextExporter::with_config(config);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Get the output schema of this plan.
    pub fn output_schema(&self) -> &Schema {
        self.op.output_schema()
    }
}

impl PhysicalPlan {
    /// Export to Graphviz DOT format (simple style).
    pub fn to_graphviz(&self, title: &str) -> String {
        let mut exporter = GraphvizExporter::new(title);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Export to Graphviz with custom configuration.
    pub fn to_graphviz_with_config(&self, config: GraphvizConfig) -> String {
        let mut exporter = GraphvizExporter::with_config(config);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty print the plan as a tree
    pub fn explain(&self) -> String {
        let mut exporter = TextExporter::new();
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Pretty print with custom configuration.
    pub fn explain_with_config(&self, config: TextConfig) -> String {
        let mut exporter = TextExporter::with_config(config);
        self.accept(&mut exporter);
        exporter.finish()
    }

    /// Get the output schema of this plan.
    pub fn output_schema(&self) -> &Schema {
        self.op.output_schema()
    }
}
