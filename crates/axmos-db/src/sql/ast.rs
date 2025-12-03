use crate::types::DataTypeKind;
use std::fmt::{Display, Formatter, Result as FmtResult};

pub trait Simplify {
    fn simplify(&mut self);
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Number(f64),
    String(String),
    Boolean(bool),
    Null,

    // Identifiers and columns
    Identifier(String),
    QualifiedIdentifier {
        table: String,
        column: String,
    },
    Star,

    // Binary operations
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    // Unary operations
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },

    // Function call
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },

    // CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expr>>,
    },

    // Subquery
    Subquery(Box<SelectStatement>),

    // List (for IN operator)
    List(Vec<Expr>),

    // BETWEEN
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },

    // EXISTS
    Exists(Box<SelectStatement>),
}

impl Simplify for Expr {
    fn simplify(&mut self) {
        match self {
            // Simplify binary operations.
            Expr::BinaryOp { left, op, right } => {
                // First, recursively simplify the children
                left.simplify();
                right.simplify();

                // Attempt to simplify with basic algebraic rules.
                // For literals, we can apply a simple basic arithmetic to unnest the query and apply the operations in place.
                match (&**left, op, &**right) {
                    (Expr::Number(a), BinaryOperator::Plus, Expr::Number(b)) if *b == 0.0 => {
                        *self = Expr::Number(*a);
                    }
                    (Expr::Number(a), BinaryOperator::Plus, Expr::Number(b)) if *a == 0.0 => {
                        *self = Expr::Number(*b);
                    }
                    (Expr::Number(a), BinaryOperator::Minus, Expr::Number(b)) if *b == 0.0 => {
                        *self = Expr::Number(*a);
                    }
                    (Expr::Number(a), BinaryOperator::Minus, Expr::Number(b)) if *a == 0.0 => {
                        *self = Expr::Number(*b);
                    }
                    (Expr::Number(a), BinaryOperator::Multiply, Expr::Number(b))
                        if *b == 0.0 || *a == 0.0 =>
                    {
                        *self = Expr::Number(0.0);
                    }
                    (Expr::Number(a), BinaryOperator::Multiply, Expr::Number(b)) if *a == 1.0 => {
                        *self = Expr::Number(*b);
                    }
                    (Expr::Number(a), BinaryOperator::Multiply, Expr::Number(b)) if *b == 1.0 => {
                        *self = Expr::Number(*a);
                    }
                    (Expr::Number(a), BinaryOperator::Divide, Expr::Number(b)) if *b == 1.0 => {
                        *self = Expr::Number(*a);
                    }
                    (Expr::Number(a), BinaryOperator::Minus, Expr::Number(b)) if *b == 0.0 => {
                        *self = Expr::Number(*a);
                    }
                    (Expr::Number(a), BinaryOperator::Minus, Expr::Number(b)) if *a == 0.0 => {
                        *self = Expr::Number(*b);
                    }
                    (Expr::Number(a), BinaryOperator::Plus, Expr::Number(b)) => {
                        *self = Expr::Number(a + b);
                    }
                    (Expr::Number(a), BinaryOperator::Minus, Expr::Number(b)) => {
                        *self = Expr::Number(a - b);
                    }
                    (Expr::Number(a), BinaryOperator::Multiply, Expr::Number(b)) => {
                        *self = Expr::Number(a * b);
                    }
                    (Expr::Number(a), BinaryOperator::Divide, Expr::Number(b)) if *b != 0.0 => {
                        *self = Expr::Number(a / b);
                    }
                    (Expr::Number(a), BinaryOperator::Eq, Expr::Number(b)) => {
                        *self = Expr::Boolean(a == b)
                    }
                    (Expr::Number(a), BinaryOperator::Gt, Expr::Number(b)) => {
                        *self = Expr::Boolean(a > b)
                    }
                    (Expr::Number(a), BinaryOperator::Lt, Expr::Number(b)) => {
                        *self = Expr::Boolean(a < b)
                    }
                    (Expr::Number(a), BinaryOperator::Ge, Expr::Number(b)) => {
                        *self = Expr::Boolean(a >= b)
                    }
                    (Expr::Number(a), BinaryOperator::Le, Expr::Number(b)) => {
                        *self = Expr::Boolean(a <= b)
                    }
                    (Expr::Number(a), BinaryOperator::Modulo, Expr::Number(b)) => {
                        *self = Expr::Number(a % b)
                    }
                    (Expr::Boolean(a), BinaryOperator::And, Expr::Boolean(b)) => {
                        *self = Expr::Boolean(*a && *b)
                    }
                    (Expr::Boolean(a), BinaryOperator::Or, Expr::Boolean(b)) => {
                        *self = Expr::Boolean(*a || *b)
                    }
                    // TRUE AND x → x
                    (Expr::Boolean(true), BinaryOperator::And, right) => {
                        *self = (*right).clone();
                    }
                    // x AND TRUE → x
                    (left, BinaryOperator::And, Expr::Boolean(true)) => {
                        *self = (*left).clone();
                    }
                    // FALSE AND x → FALSE
                    (Expr::Boolean(false), BinaryOperator::And, _) => {
                        *self = Expr::Boolean(false);
                    }
                    // x AND FALSE → FALSE
                    (_, BinaryOperator::And, Expr::Boolean(false)) => {
                        *self = Expr::Boolean(false);
                    }

                    // TRUE OR x → TRUE
                    (Expr::Boolean(true), BinaryOperator::Or, _) => {
                        *self = Expr::Boolean(true);
                    }
                    // x OR TRUE → TRUE
                    (_, BinaryOperator::Or, Expr::Boolean(true)) => {
                        *self = Expr::Boolean(true);
                    }
                    // FALSE OR x → x
                    (Expr::Boolean(false), BinaryOperator::Or, right) => {
                        *self = (*right).clone();
                    }
                    // x OR FALSE → x
                    (left, BinaryOperator::Or, Expr::Boolean(false)) => {
                        *self = (*left).clone();
                    }
                    (Expr::String(a), BinaryOperator::Concat, Expr::String(b)) => {
                        *self = Expr::String(format!("{a}{b}"));
                    }
                    (Expr::String(a), BinaryOperator::Like, Expr::String(pattern)) => {
                        let regex = pattern.replace('%', ".*").replace('_', ".");
                        let re = regex::Regex::new(&format!("^{regex}$")).unwrap();
                        *self = Expr::Boolean(re.is_match(a));
                    }
                    (Expr::String(a), BinaryOperator::NotLike, Expr::String(pattern)) => {
                        let regex = pattern.replace('%', ".*").replace('_', ".");
                        let re = regex::Regex::new(&format!("^{regex}$")).unwrap();
                        *self = Expr::Boolean(!re.is_match(a));
                    }
                    // x IN (x, y, z) → TRUE
                    (Expr::Number(a), BinaryOperator::In, Expr::List(items)) => {
                        let result = items
                            .iter()
                            .any(|e| matches!(e, Expr::Number(b) if *a == *b));
                        *self = Expr::Boolean(result);
                    }
                    (Expr::String(a), BinaryOperator::In, Expr::List(items)) => {
                        let result = items
                            .iter()
                            .any(|e| matches!(e, Expr::String(b) if *a == *b));
                        *self = Expr::Boolean(result);
                    }
                    (Expr::String(a), BinaryOperator::NotIn, Expr::List(items)) => {
                        let result = !items.iter().any(|e| matches!(e, Expr::String(b) if a == b));
                        *self = Expr::Boolean(result);
                    }
                    (Expr::Number(a), BinaryOperator::NotIn, Expr::List(items)) => {
                        let result = !items.iter().any(|e| matches!(e, Expr::Number(b) if a == b));
                        *self = Expr::Boolean(result);
                    }

                    _ => {}
                }
            }

            // Simplify unary ops
            Expr::UnaryOp { op, expr } => match (op, expr.as_mut()) {
                (UnaryOperator::Minus, Expr::Number(a)) => {
                    *self = Expr::Number(*a * (-1.0));
                }
                (UnaryOperator::Plus, Expr::Number(a)) => {
                    *self = Expr::Number(*a);
                }
                (UnaryOperator::Not, Expr::Boolean(a)) => *self = Expr::Boolean(!*a),
                _ => {}
            },

            // Simplify lists
            Expr::List(items) => {
                for e in items {
                    e.simplify();
                }
            }

            // Simplificar CASE WHEN THEN ELSE
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    op.simplify();
                }
                for clause in when_clauses {
                    clause.condition.simplify();
                    clause.result.simplify();
                }
                if let Some(else_expr) = else_clause {
                    else_expr.simplify();
                }
            }

            // Simplify subqueries
            Expr::Subquery(subq) | Expr::Exists(subq) => {
                // Call the select statement simplifier.
                subq.as_mut().simplify();
            }

            // Simplify BETWEEN
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.simplify();
                low.simplify();
                high.simplify();
            }

            // In function calls we can simplify the arguments.
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    arg.simplify();
                }
            }

            // Literals and identifiers cannot be simplified.
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Eq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,

    // Logical
    And,
    Or,

    // String
    Like,
    NotLike,
    Concat,

    // Set
    In,
    NotIn,

    // NULL
    Is,
    IsNot,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectItem>,
    pub from: Option<TableReference>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

impl Simplify for SelectStatement {
    fn simplify(&mut self) {
        for col in self.columns.iter_mut() {
            col.simplify();
        }

        if let Some(clause) = self.from.as_mut() {
            clause.simplify();
        };

        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify();
        };

        for g_clause in self.group_by.iter_mut() {
            g_clause.simplify();
        }

        if let Some(having_clause) = self.having.as_mut() {
            having_clause.simplify();
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Star,
    ExprWithAlias { expr: Expr, alias: Option<String> },
}

impl Simplify for SelectItem {
    fn simplify(&mut self) {
        if let Self::ExprWithAlias { expr, .. } = self {
            expr.simplify();
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<TableReference>,
        join_type: JoinType,
        right: Box<TableReference>,
        on: Option<Expr>,
    },
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

impl Simplify for TableReference {
    fn simplify(&mut self) {
        match self {
            Self::Join {
                left, right, on, ..
            } => {
                left.as_mut().simplify();
                right.as_mut().simplify();

                if let Some(on_expr) = on {
                    on_expr.simplify();
                };
            }
            Self::Subquery { query, .. } => {
                query.as_mut().simplify();
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
}

impl Simplify for OrderByExpr {
    fn simplify(&mut self) {
        self.expr.simplify();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    With(WithStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    Transaction(TransactionStatement),
}

impl Simplify for Statement {
    fn simplify(&mut self) {
        match self {
            Self::With(s) => s.simplify(),
            Self::AlterTable(s) => s.simplify(),
            Self::Delete(s) => s.simplify(),
            Self::Insert(s) => s.simplify(),
            Self::Select(s) => s.simplify(),
            Self::Update(s) => s.simplify(),
            Self::CreateTable(s) => s.simplify(),
            _ => {}
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WithStatement {
    pub recursive: bool,
    pub ctes: Vec<(String, SelectStatement)>,
    pub body: Box<SelectStatement>,
}

impl Simplify for WithStatement {
    fn simplify(&mut self) {
        for (_, clause) in self.ctes.iter_mut() {
            clause.simplify();
        }

        self.body.as_mut().simplify();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Values,
}

impl Simplify for InsertStatement {
    fn simplify(&mut self) {
        self.values.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Values {
    Values(Vec<Vec<Expr>>),
    Query(Box<SelectStatement>),
}

impl Simplify for Values {
    fn simplify(&mut self) {
        match self {
            Self::Values(values) => {
                for vi in values.iter_mut() {
                    for vj in vi.iter_mut() {
                        vj.simplify();
                    }
                }
            }
            Self::Query(query) => {
                query.simplify();
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub set_clauses: Vec<SetClause>,
    pub where_clause: Option<Expr>,
}

impl Simplify for UpdateStatement {
    fn simplify(&mut self) {
        for item in self.set_clauses.iter_mut() {
            item.simplify();
        }

        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub column: String,
    pub value: Expr,
}

impl Simplify for SetClause {
    fn simplify(&mut self) {
        self.value.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

impl Simplify for DeleteStatement {
    fn simplify(&mut self) {
        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify();
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDefExpr>,
    pub constraints: Vec<TableConstraintExpr>,
}

impl Simplify for CreateTableStatement {
    fn simplify(&mut self) {
        for ct in self.columns.iter_mut() {
            ct.simplify();
        }
        for ct in self.constraints.iter_mut() {
            ct.simplify();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefExpr {
    pub name: String,
    pub data_type: DataTypeKind,
    pub constraints: Vec<ColumnConstraintExpr>,
}

impl Simplify for ColumnDefExpr {
    fn simplify(&mut self) {
        for ct in self.constraints.iter_mut() {
            ct.simplify();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintExpr {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey { table: String, column: String },
    Check(Expr),
    Default(Expr),
}

impl Simplify for ColumnConstraintExpr {
    fn simplify(&mut self) {
        match self {
            Self::Check(expr) | Self::Default(expr) => expr.simplify(),
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintExpr {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    Check(Expr),
}

impl Simplify for TableConstraintExpr {
    fn simplify(&mut self) {
        if let Self::Check(expr) = self {
            expr.simplify();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table: String,
    pub action: AlterAction,
}

impl Simplify for AlterTableStatement {
    fn simplify(&mut self) {
        self.action.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterColumnStatement {
    pub name: String,
    pub action: AlterColumnAction,
}

impl Simplify for AlterColumnStatement {
    fn simplify(&mut self) {
        self.action.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddColumn(ColumnDefExpr),
    DropColumn(String),
    AlterColumn(AlterColumnStatement),
    AddConstraint(TableConstraintExpr),
    DropConstraint(String),
}

impl Simplify for AlterAction {
    fn simplify(&mut self) {
        match self {
            Self::AddColumn(col) => col.simplify(),
            Self::AlterColumn(col) => col.simplify(),
            Self::AddConstraint(ct) => ct.simplify(),
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnAction {
    SetDataType(DataTypeKind),
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

impl Simplify for AlterColumnAction {
    fn simplify(&mut self) {
        if let Self::SetDefault(expr) = self {
            expr.simplify();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub table: String,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub order: Option<OrderDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Statement::Select(s) => write!(f, "{s}"),
            Statement::Insert(s) => write!(f, "{s}"),
            Statement::Update(s) => write!(f, "{s}"),
            Statement::Delete(s) => write!(f, "{s}"),
            Statement::CreateTable(s) => write!(f, "{s}"),
            Statement::AlterTable(s) => write!(f, "{s}"),
            Statement::DropTable(s) => write!(f, "{s}"),
            Statement::CreateIndex(s) => write!(f, "{s}"),
            Statement::Transaction(s) => write!(f, "{s}"),
            Statement::With(s) => write!(f, "{s}"),
        }
    }
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }

        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{}", cols.join(", "))?;

        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        if !self.group_by.is_empty() {
            let groups: Vec<String> = self.group_by.iter().map(|g| g.to_string()).collect();
            write!(f, " GROUP BY {}", groups.join(", "))?;
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        if !self.order_by.is_empty() {
            let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            write!(f, " ORDER BY {}", orders.join(", "))?;
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}

impl Display for SelectItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            SelectItem::Star => write!(f, "*"),
            SelectItem::ExprWithAlias { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(a) = alias {
                    write!(f, " AS {a}")?;
                }
                Ok(())
            }
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Expr::Number(n) => write!(f, "{n}"),
            Expr::String(s) => write!(f, "'{s}'"),
            Expr::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Expr::Null => write!(f, "NULL"),
            Expr::Identifier(id) => write!(f, "{id}"),
            Expr::QualifiedIdentifier { table, column } => write!(f, "{table}.{column}"),
            Expr::Star => write!(f, "*"),
            Expr::BinaryOp { left, op, right } => write!(f, "({left} {op} {right})"),
            Expr::UnaryOp { op, expr } => write!(f, "{op}{expr}"),
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                write!(f, "{name}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{})", args_str.join(", "))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for clause in when_clauses {
                    write!(f, " {clause}")?;
                }
                if let Some(else_expr) = else_clause {
                    write!(f, " ELSE {else_expr}")?;
                }
                write!(f, " END")
            }
            Expr::Subquery(s) => write!(f, "({s})"),
            Expr::List(items) => {
                let list: Vec<String> = items.iter().map(|i| i.to_string()).collect();
                write!(f, "({})", list.join(", "))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {low} AND {high}")
            }
            Expr::Exists(s) => write!(f, "EXISTS ({s})"),
        }
    }
}

impl Display for WhenClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "WHEN {} THEN {}", self.condition, self.result)
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "%"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::Neq => write!(f, "<>"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Le => write!(f, "<="),
            BinaryOperator::Ge => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::Like => write!(f, "LIKE"),
            BinaryOperator::NotLike => write!(f, "NOT LIKE"),
            BinaryOperator::Concat => write!(f, "||"),
            BinaryOperator::In => write!(f, "IN"),
            BinaryOperator::NotIn => write!(f, "NOT IN"),
            BinaryOperator::Is => write!(f, "IS"),
            BinaryOperator::IsNot => write!(f, "IS NOT"),
        }
    }
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            UnaryOperator::Plus => write!(f, "+"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Not => write!(f, "NOT "),
        }
    }
}

impl Display for TableReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TableReference::Table { name, alias } => {
                write!(f, "{name}")?;
                if let Some(a) = alias {
                    write!(f, " AS {a}")?;
                }
                Ok(())
            }
            TableReference::Join {
                left,
                join_type,
                right,
                on,
            } => {
                write!(f, "{left} {join_type} {right}")?;
                if let Some(on_expr) = on {
                    write!(f, " ON {on_expr}")?;
                }
                Ok(())
            }
            TableReference::Subquery { query, alias } => {
                write!(f, "({query}) AS {alias}")
            }
        }
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
            JoinType::Cross => write!(f, "CROSS JOIN"),
        }
    }
}

impl Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " ASC")
        } else {
            write!(f, " DESC")
        }
    }
}

impl Display for InsertStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "INSERT INTO {}", self.table)?;
        if let Some(cols) = &self.columns {
            write!(f, " ({})", cols.join(", "))?;
        }
        write!(f, " {}", self.values)
    }
}

impl Display for Values {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Values::Values(rows) => {
                write!(f, "VALUES ")?;
                let row_strs: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();
                write!(f, "{}", row_strs.join(", "))
            }
            Values::Query(q) => write!(f, "{q}"),
        }
    }
}

impl Display for UpdateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "UPDATE {} SET ", self.table)?;
        let sets: Vec<String> = self.set_clauses.iter().map(|s| s.to_string()).collect();
        write!(f, "{}", sets.join(", "))?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

impl Display for SetClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} = {}", self.column, self.value)
    }
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DELETE FROM {}", self.table)?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

impl Display for CreateTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "CREATE TABLE {} (", self.table)?;
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{}", cols.join(", "))?;
        if !self.constraints.is_empty() {
            let cts: Vec<String> = self.constraints.iter().map(|c| c.to_string()).collect();
            write!(f, ", {}", cts.join(", "))?;
        }
        write!(f, ")")
    }
}

impl Display for ColumnDefExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} {}", self.name, self.data_type)?;
        for ct in &self.constraints {
            write!(f, " {ct}")?;
        }
        Ok(())
    }
}

impl Display for ColumnConstraintExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ColumnConstraintExpr::NotNull => write!(f, "NOT NULL"),
            ColumnConstraintExpr::Unique => write!(f, "UNIQUE"),
            ColumnConstraintExpr::PrimaryKey => write!(f, "PRIMARY KEY"),
            ColumnConstraintExpr::ForeignKey { table, column } => {
                write!(f, "REFERENCES {table}({column})")
            }
            ColumnConstraintExpr::Check(expr) => write!(f, "CHECK ({expr})"),
            ColumnConstraintExpr::Default(expr) => write!(f, "DEFAULT {expr}"),
        }
    }
}

impl Display for TableConstraintExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TableConstraintExpr::PrimaryKey(cols) => {
                write!(f, "PRIMARY KEY ({})", cols.join(", "))
            }
            TableConstraintExpr::Unique(cols) => {
                write!(f, "UNIQUE ({})", cols.join(", "))
            }
            TableConstraintExpr::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                write!(
                    f,
                    "FOREIGN KEY ({}) REFERENCES {}({})",
                    columns.join(", "),
                    ref_table,
                    ref_columns.join(", ")
                )
            }
            TableConstraintExpr::Check(expr) => write!(f, "CHECK ({expr})"),
        }
    }
}

impl Display for AlterTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "ALTER TABLE {} {}", self.table, self.action)
    }
}

impl Display for AlterAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            AlterAction::AddColumn(col) => write!(f, "ADD COLUMN {col}"),
            AlterAction::DropColumn(name) => write!(f, "DROP COLUMN {name}"),
            AlterAction::AlterColumn(col) => write!(f, "ALTER COLUMN {col}"),
            AlterAction::AddConstraint(ct) => write!(f, "ADD {ct}"),
            AlterAction::DropConstraint(name) => write!(f, "DROP CONSTRAINT {name}"),
        }
    }
}

impl Display for AlterColumnStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} {}", self.name, self.action)
    }
}

impl Display for AlterColumnAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            AlterColumnAction::SetDataType(dt) => write!(f, "SET DATA TYPE {dt}"),
            AlterColumnAction::SetDefault(expr) => write!(f, "SET DEFAULT {expr}"),
            AlterColumnAction::DropDefault => write!(f, "DROP DEFAULT"),
            AlterColumnAction::SetNotNull => write!(f, "SET NOT NULL"),
            AlterColumnAction::DropNotNull => write!(f, "DROP NOT NULL"),
        }
    }
}

impl Display for DropTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DROP TABLE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.table)?;
        if self.cascade {
            write!(f, " CASCADE")?;
        }
        Ok(())
    }
}

impl Display for CreateIndexStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "CREATE")?;
        if self.unique {
            write!(f, " UNIQUE")?;
        }
        write!(f, " INDEX")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} ON {} (", self.name, self.table)?;
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", cols.join(", "))
    }
}

impl Display for IndexColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.name)?;
        if let Some(order) = &self.order {
            write!(f, " {order}")?;
        }
        Ok(())
    }
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

impl Display for TransactionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TransactionStatement::Begin => write!(f, "BEGIN"),
            TransactionStatement::Commit => write!(f, "COMMIT"),
            TransactionStatement::Rollback => write!(f, "ROLLBACK"),
        }
    }
}

impl Display for WithStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "WITH ")?;
        if self.recursive {
            write!(f, "RECURSIVE ")?;
        }
        let ctes: Vec<String> = self
            .ctes
            .iter()
            .map(|(name, query)| format!("{name} AS ({query})"))
            .collect();
        write!(f, "{} {}", ctes.join(", "), self.body)
    }
}
