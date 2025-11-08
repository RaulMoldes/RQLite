use crate::types::DataTypeKind;

pub(crate) trait Simplify {
    fn simplify(&mut self) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
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
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            // Simplify binary operations.
            Expr::BinaryOp { left, op, right } => {
                // First, recursively simplify the children
                left.simplify()?;
                right.simplify()?;

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
                    e.simplify()?;
                }
            }

            // Simplificar CASE WHEN THEN ELSE
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    op.simplify()?;
                }
                for clause in when_clauses {
                    clause.condition.simplify()?;
                    clause.result.simplify()?;
                }
                if let Some(else_expr) = else_clause {
                    else_expr.simplify()?;
                }
            }

            // Simplify subqueries
            Expr::Subquery(subq) | Expr::Exists(subq) => {
                // Call the select statement simplifier.
                subq.as_mut().simplify()?;
            }

            // Simplify BETWEEN
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.simplify()?;
                low.simplify()?;
                high.simplify()?;
            }

            // In function calls we can simplify the arguments.
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    arg.simplify()?;
                }
            }

            // Literals and identifiers cannot be simplified.
            _ => {}
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WhenClause {
    pub(crate) condition: Expr,
    pub(crate) result: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BinaryOperator {
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
pub(crate) enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SelectStatement {
    pub(crate) distinct: bool,
    pub(crate) columns: Vec<SelectItem>,
    pub(crate) from: Option<TableReference>,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) having: Option<Expr>,
    pub(crate) order_by: Vec<OrderByExpr>,
    pub(crate) limit: Option<usize>,
}

impl Simplify for SelectStatement {
    fn simplify(&mut self) -> Result<(), String> {
        for col in self.columns.iter_mut() {
            col.simplify()?;
        }

        if let Some(clause) = self.from.as_mut() {
            clause.simplify()?;
        };

        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify()?;
        };

        for g_clause in self.group_by.iter_mut() {
            g_clause.simplify()?;
        }

        if let Some(having_clause) = self.having.as_mut() {
            having_clause.simplify()?;
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectItem {
    Star,
    ExprWithAlias { expr: Expr, alias: Option<String> },
}

impl Simplify for SelectItem {
    fn simplify(&mut self) -> Result<(), String> {
        if let Self::ExprWithAlias { expr, .. } = self {
            expr.simplify()?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TableReference {
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
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            Self::Join {
                left, right, on, ..
            } => {
                left.as_mut().simplify()?;
                right.as_mut().simplify()?;

                if let Some(on_expr) = on {
                    on_expr.simplify()?;
                };
            }
            Self::Subquery { query, .. } => {
                query.as_mut().simplify()?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByExpr {
    pub(crate) expr: Expr,
    pub(crate) asc: bool,
}

impl Simplify for OrderByExpr {
    fn simplify(&mut self) -> Result<(), String> {
        self.expr.simplify()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Statement {
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
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            Self::With(s) => s.simplify(),
            Self::AlterTable(s) => s.simplify(),
            Self::Delete(s) => s.simplify(),
            Self::Insert(s) => s.simplify(),
            Self::Select(s) => s.simplify(),
            Self::Update(s) => s.simplify(),
            Self::CreateTable(s) => s.simplify(),
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WithStatement {
    pub(crate) recursive: bool,
    pub(crate) ctes: Vec<(String, SelectStatement)>,
    pub(crate) body: Box<SelectStatement>,
}

impl Simplify for WithStatement {
    fn simplify(&mut self) -> Result<(), String> {
        for (_, clause) in self.ctes.iter_mut() {
            clause.simplify()?;
        }

        self.body.as_mut().simplify()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertStatement {
    pub(crate) table: String,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) values: Values,
}

impl Simplify for InsertStatement {
    fn simplify(&mut self) -> Result<(), String> {
        self.values.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Values {
    Values(Vec<Vec<Expr>>),
    Query(Box<SelectStatement>),
}

impl Simplify for Values {
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            Self::Values(values) => {
                for vi in values.iter_mut() {
                    for vj in vi.iter_mut() {
                        vj.simplify()?;
                    }
                }
            }
            Self::Query(query) => {
                query.simplify()?;
            }
        }

        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateStatement {
    pub(crate) table: String,
    pub(crate) set_clauses: Vec<SetClause>,
    pub(crate) where_clause: Option<Expr>,
}

impl Simplify for UpdateStatement {
    fn simplify(&mut self) -> Result<(), String> {
        for item in self.set_clauses.iter_mut() {
            item.simplify()?;
        }

        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SetClause {
    pub(crate) column: String,
    pub(crate) value: Expr,
}

impl Simplify for SetClause {
    fn simplify(&mut self) -> Result<(), String> {
        self.value.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeleteStatement {
    pub(crate) table: String,
    pub(crate) where_clause: Option<Expr>,
}

impl Simplify for DeleteStatement {
    fn simplify(&mut self) -> Result<(), String> {
        if let Some(clause) = self.where_clause.as_mut() {
            clause.simplify()?;
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateTableStatement {
    pub(crate) table: String,
    pub(crate) columns: Vec<ColumnDefExpr>,
    pub(crate) constraints: Vec<TableConstraintExpr>,
}

impl Simplify for CreateTableStatement {
    fn simplify(&mut self) -> Result<(), String> {
        for ct in self.columns.iter_mut() {
            ct.simplify()?;
        }
        for ct in self.constraints.iter_mut() {
            ct.simplify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnDefExpr {
    pub(crate) name: String,
    pub(crate) data_type: DataTypeKind,
    pub(crate) constraints: Vec<ColumnConstraintExpr>,
}

impl Simplify for ColumnDefExpr {
    fn simplify(&mut self) -> Result<(), String> {
        for ct in self.constraints.iter_mut() {
            ct.simplify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColumnConstraintExpr {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey { table: String, column: String },
    Check(Expr),
    Default(Expr),
}

impl Simplify for ColumnConstraintExpr {
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            Self::Check(expr) | Self::Default(expr) => expr.simplify(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TableConstraintExpr {
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
    fn simplify(&mut self) -> Result<(), String> {
        if let Self::Check(expr) = self {
            expr.simplify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AlterTableStatement {
    pub(crate) table: String,
    pub(crate) action: AlterAction,
}

impl Simplify for AlterTableStatement {
    fn simplify(&mut self) -> Result<(), String> {
        self.action.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AlterColumnStatement {
    pub(crate) name: String,
    pub(crate) action: AlterColumnAction,
}

impl Simplify for AlterColumnStatement {
    fn simplify(&mut self) -> Result<(), String> {
        self.action.simplify()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlterAction {
    AddColumn(ColumnDefExpr),
    DropColumn(String),
    AlterColumn(AlterColumnStatement),
    AddConstraint(TableConstraintExpr),
    DropConstraint(String),
}

impl Simplify for AlterAction {
    fn simplify(&mut self) -> Result<(), String> {
        match self {
            Self::AddColumn(col) => col.simplify(),
            Self::AlterColumn(col) => col.simplify(),
            Self::AddConstraint(ct) => ct.simplify(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlterColumnAction {
    SetDataType(DataTypeKind),
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

impl Simplify for AlterColumnAction {
    fn simplify(&mut self) -> Result<(), String> {
        if let Self::SetDefault(expr) = self {
            expr.simplify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DropTableStatement {
    pub(crate) table: String,
    pub(crate) if_exists: bool,
    pub(crate) cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateIndexStatement {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<IndexColumn>,
    pub(crate) unique: bool,
    pub(crate) if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexColumn {
    pub(crate) name: String,
    pub(crate) order: Option<OrderDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}
