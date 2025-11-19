use crate::types::DataTypeKind;

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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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
