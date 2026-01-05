pub(crate) mod ast;
mod lexer;

use ast::*;
use lexer::Lexer;

pub use lexer::Token;

use crate::types::DataTypeKind;
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    mem,
};

/// Parser errors (Step 1: SQL text -> AST)
#[derive(Debug, PartialEq, Clone)]
pub enum ParserError {
    InvalidExpression(String),
    UnexpectedToken(String),
    UnexpectedEof,
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidExpression(s) => write!(f, "Invalid expression: {}", s),
            Self::UnexpectedToken(s) => write!(f, "Unexpected token: {}", s),
            Self::UnexpectedEof => write!(f, "Unexpected end of input"),
        }
    }
}

impl Error for ParserError {}
pub type ParseResult<T> = Result<T, ParserError>;

/// Main parser implementation.
/// Uses a pratt parsing approach to parse sql expressions into AST nodes.
pub struct Parser {
    lexer: Lexer,
    current_token: Token,
}

impl Parser {
    pub fn new(sql: &str) -> Self {
        let mut lexer = Lexer::new(sql);
        let current_token = lexer.next_token();
        Parser {
            lexer,
            current_token,
        }
    }

    fn next_token(&mut self) {
        self.current_token = self.lexer.next_token();
    }

    fn __peek_token(&mut self) -> Token {
        self.lexer.__peek_token()
    }

    fn expect(&mut self, expected: Token) -> ParseResult<()> {
        // docs on std::mem::discriminant: https://doc.rust-lang.org/std/mem/fn.discriminant.html
        // Returns a value uniquely identifying the enum variant in the calling token.
        if mem::discriminant(&self.current_token) == mem::discriminant(&expected) {
            self.next_token();
            Ok(())
        } else {
            Err(ParserError::UnexpectedToken(self.current_token.to_string()))
        }
    }

    /// Consumes the next token if the current token matches the expected one.
    fn consume_if(&mut self, token: &Token) -> bool {
        if mem::discriminant(&self.current_token) == mem::discriminant(token) {
            self.next_token();
            true
        } else {
            false
        }
    }

    /// Expression parsing with Pratt parsing
    pub(crate) fn parse_expression(&mut self) -> ParseResult<Expr> {
        self.parse_expr_bp(0)
    }

    /// Obtains the expression binding power using a Pratt Parsing approach.
    /// I recommend this read on Pratt Parsing: https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html
    fn parse_expr_bp(&mut self, min_bp: u8) -> ParseResult<Expr> {
        let mut lhs = self.parse_prefix()?;

        while let Some((l_bp, r_bp)) = self.infix_binding_power() {
            if l_bp < min_bp {
                break;
            }

            lhs = self.parse_infix(lhs, r_bp)?;
        }

        Ok(lhs)
    }

    /// Given the current token, obtains the parsed prefix of the expression.
    fn parse_prefix(&mut self) -> ParseResult<Expr> {
        match &self.current_token {
            Token::NumberLiteral(n) => {
                let num = *n;
                self.next_token();
                Ok(Expr::Number(num))
            }
            Token::StringLiteral(s) => {
                let string = s.clone();
                self.next_token();
                Ok(Expr::String(string))
            }
            Token::True => {
                self.next_token();
                Ok(Expr::Boolean(true))
            }
            Token::False => {
                self.next_token();
                Ok(Expr::Boolean(false))
            }
            Token::Null => {
                self.next_token();
                Ok(Expr::Null)
            }
            Token::Star => {
                self.next_token();
                Ok(Expr::Star)
            }
            // Consuming identifiers is tricky as we want to support qualified expressions like [TABLE].[COLUMN], or even [TABLE].* for selecting all columns in a table.
            Token::Identifier(name) => {
                let name = name.clone();
                self.next_token();

                // Check for qualified identifier (table.column)
                if self.consume_if(&Token::Dot) {
                    if let Token::Identifier(col) = &self.current_token {
                        let column = col.clone();
                        self.next_token();
                        Ok(Expr::QualifiedIdentifier {
                            table: name,
                            column,
                        })
                    } else if self.current_token == Token::Star {
                        self.next_token();
                        Ok(Expr::Star) // Actually should be QualifiedStar in SELECT
                    } else {
                        Err(ParserError::InvalidExpression(
                            "expected column name after '.'".to_string(),
                        ))
                    }
                }
                // CHECK FOR FUNCTION CALLS OR NESTED SUBQUERIES.
                else if self.current_token == Token::LParen {
                    self.next_token();

                    // Check if this is a subquery (SELECT inside parentheses)
                    // This should be handled at the parse_prefix level.
                    // Check for DISTINCT in aggregate functions
                    let distinct = self.consume_if(&Token::Distinct);

                    let mut args = Vec::new();
                    if self.current_token != Token::RParen {
                        loop {
                            // We need to recursively parse the inner expression.
                            // Args in function calls are separated by commas.
                            args.push(self.parse_expression()?);
                            if !self.consume_if(&Token::Comma) {
                                break;
                            }
                        }
                    }

                    self.expect(Token::RParen)?;
                    Ok(Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                    })
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            Token::LParen => {
                self.next_token();

                // Check if this is a subquery (SELECT inside parentheses)
                if self.current_token == Token::Select {
                    let subquery = self.parse_select_statement()?;
                    self.expect(Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(subquery)));
                }

                let mut exprs = Vec::new();
                loop {
                    exprs.push(self.parse_expression()?);
                    if self.consume_if(&Token::Comma) {
                        continue;
                    } else {
                        break;
                    }
                }

                self.expect(Token::RParen)?;

                if exprs.len() == 1 {
                    Ok(exprs.into_iter().next().unwrap()) // Single expression, normal case
                } else {
                    Ok(Expr::List(exprs)) // Tuple/list of expressions
                }
            }
            // Once we reach an operator, we need to parse the expression binding power using Pratt Parsing.
            // The operator precedence for summation is 9 (Took values from: https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html).
            // The Operator PRECEDENCE, determines the strength to which it binds to anything to the left of it.
            Token::Plus => {
                self.next_token();
                // Note that here we are parsing the UNARY plus operator.
                // UNary PLUS and MINUS operators must have higher precedence than addition but lower than multiplication in evaluation order.
                // This way, the expression: -1 * 2, is parsed as (* (-1) 2), instead of (- (1 * 2))
                // Which would happen if unary operators (minus, on this case)
                // had lower precedence.
                let expr = self.parse_expr_bp(9)?; // Unary plus precedence
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            Token::Minus => {
                self.next_token();
                // Special case: if next token is a number, combine into negative number
                if let Token::NumberLiteral(n) = self.current_token {
                    let num = -n;
                    self.next_token();
                    Ok(Expr::Number(num))
                } else {
                    // Regular unary minus for expressions
                    let expr = self.parse_expr_bp(9)?;
                    Ok(Expr::UnaryOp {
                        op: UnaryOperator::Minus,
                        expr: Box::new(expr),
                    })
                }
            }
            // If you take a look at the infix table:
            // ´´´rust
            // Token::Or => Some((1, 2)),
            // Token::And => Some((3, 4)),
            // ´´´
            // By giving here the NOT operator a precedence of three, we are making sur it binds tighter than  AND & OR,
            // but looselier than +, - , * an unary operators.
            // This way, the expression: [NOT a AND b OR c], will be parsed as: (OR (AND (NOT a) b) c)
            Token::Not => {
                self.next_token();
                let expr = self.parse_expr_bp(3)?; // NOT precedence
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            Token::Case => self.parse_case_expression(),
            Token::Exists => {
                // Here we are forcing the inner expression to be between parentheses.
                // The EXISTS SELECT ... without parentheses is not allowed.
                self.next_token();
                self.expect(Token::LParen)?;
                let subquery = self.parse_select_statement()?;
                self.expect(Token::RParen)?;
                Ok(Expr::Exists(Box::new(subquery)))
            }
            _ => Err(ParserError::InvalidExpression(
                self.current_token.to_string(),
            )),
        }
    }

    fn parse_infix(&mut self, left: Expr, r_bp: u8) -> ParseResult<Expr> {
        let op = match &self.current_token {
            Token::Plus => {
                self.next_token();
                BinaryOperator::Plus
            }
            Token::Minus => {
                self.next_token();
                BinaryOperator::Minus
            }
            Token::Star => {
                self.next_token();
                BinaryOperator::Multiply
            }
            Token::Slash => {
                self.next_token();
                BinaryOperator::Divide
            }
            Token::Percent => {
                self.next_token();
                BinaryOperator::Modulo
            }
            Token::Eq => {
                self.next_token();
                BinaryOperator::Eq
            }
            Token::Neq => {
                self.next_token();
                BinaryOperator::Neq
            }
            Token::Lt => {
                self.next_token();
                BinaryOperator::Lt
            }
            Token::Gt => {
                self.next_token();
                BinaryOperator::Gt
            }
            Token::Le => {
                self.next_token();
                BinaryOperator::Le
            }
            Token::Ge => {
                self.next_token();
                BinaryOperator::Ge
            }
            Token::And => {
                self.next_token();
                BinaryOperator::And
            }
            Token::Or => {
                self.next_token();
                BinaryOperator::Or
            }
            Token::Like => {
                self.next_token();
                BinaryOperator::Like
            }
            Token::Not => {
                // Handle combined infix forms: NOT IN / NOT BETWEEN / NOT LIKE
                self.next_token(); // consume NOT
                match &self.current_token {
                    Token::In => {
                        self.next_token();
                        self.expect(Token::LParen)?;

                        if self.current_token == Token::Select {
                            let subquery = self.parse_select_statement()?;
                            self.expect(Token::RParen)?;
                            return Ok(Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::NotIn,
                                right: Box::new(Expr::Subquery(Box::new(subquery))),
                            });
                        } else {
                            let mut values = Vec::new();
                            loop {
                                values.push(self.parse_expression()?);
                                if !self.consume_if(&Token::Comma) {
                                    break;
                                }
                            }
                            self.expect(Token::RParen)?;
                            return Ok(Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::NotIn,
                                right: Box::new(Expr::List(values)),
                            });
                        }
                    }
                    Token::Between => {
                        self.next_token();
                        let low = self.parse_expr_bp(4)?;
                        self.expect(Token::And)?;
                        let high = self.parse_expr_bp(4)?;
                        return Ok(Expr::Between {
                            expr: Box::new(left),
                            negated: true,
                            low: Box::new(low),
                            high: Box::new(high),
                        });
                    }
                    Token::Like => {
                        self.next_token();
                        let right = self.parse_expr_bp(r_bp)?;
                        return Ok(Expr::BinaryOp {
                            left: Box::new(left),
                            op: BinaryOperator::NotLike,
                            right: Box::new(right),
                        });
                    }
                    _ => {
                        return Err(ParserError::UnexpectedToken(self.current_token.to_string()));
                    }
                }
            }
            Token::In => {
                self.next_token();
                self.expect(Token::LParen)?;

                // Check if it's a subquery or a list
                if self.current_token == Token::Select {
                    let subquery = self.parse_select_statement()?;
                    self.expect(Token::RParen)?;
                    return Ok(Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::In,
                        right: Box::new(Expr::Subquery(Box::new(subquery))),
                    });
                } else {
                    let mut values = Vec::new();
                    loop {
                        values.push(self.parse_expression()?);
                        if !self.consume_if(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect(Token::RParen)?;
                    return Ok(Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::In,
                        right: Box::new(Expr::List(values)),
                    });
                }
            }
            Token::Between => {
                self.next_token();
                let low = self.parse_expr_bp(4)?;
                self.expect(Token::And)?;
                let high = self.parse_expr_bp(4)?;
                return Ok(Expr::Between {
                    expr: Box::new(left),
                    negated: false,
                    low: Box::new(low),
                    high: Box::new(high),
                });
            }
            Token::Is => {
                self.next_token();
                if self.consume_if(&Token::Not) {
                    BinaryOperator::IsNot
                } else {
                    BinaryOperator::Is
                }
            }
            Token::Concat => {
                self.next_token();
                BinaryOperator::Concat
            }
            _ => {
                return Err(ParserError::InvalidExpression(format!(
                    "unexpected infix operator: {:?}",
                    self.current_token
                )));
            }
        };

        let right = self.parse_expr_bp(r_bp)?;

        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    // In a Pratt parser, each operator (like +, *, =, AND) has a binding power that tells the parser how tightly it binds operands.
    // The parser uses these numbers to decide when to stop consuming operators and return the current expression.
    // Consider this expression: 1 + 2 * 3.
    // If operators + and * had the same bp, the interpreter would execute the instruction as (1 + 2) * 3, as it reads left to right, but this is wrong mathemathically.
    // By assigning higher binding power to * than +, the parser knows:
    // + has lbp of 7 and * has lbp of 9. Therefore, the expression is interpreted as follows: (+ 1 (* 2 3)).
    // The operand 2 binds to the operator * as it has higher lbp than +'s rbp.
    fn infix_binding_power(&mut self) -> Option<(u8, u8)> {
        match &self.current_token {
            Token::Or => Some((1, 2)),
            Token::And => Some((3, 4)),
            Token::Eq | Token::Neq | Token::Lt | Token::Gt | Token::Le | Token::Ge => Some((5, 6)),
            Token::Like | Token::In | Token::Between => Some((5, 6)),
            Token::Is => Some((5, 6)),
            Token::Plus | Token::Minus => Some((7, 8)),
            Token::Star | Token::Slash | Token::Percent => Some((9, 10)),
            Token::Concat => Some((7, 8)),
            Token::Not => {
                let next = self.__peek_token();
                match next {
                    Token::In | Token::Between | Token::Like => Some((5, 6)),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// Utilities for parsing complex expressions.
impl Parser {
    /// Parses a CASE expression.
    /// ```sql
    /// CASE enum
    /// WHEN a THEN X
    /// WHEN b THEN y
    /// ELSE ...
    /// [...]
    /// ```
    fn parse_case_expression(&mut self) -> ParseResult<Expr> {
        self.expect(Token::Case)?;

        let mut operand = None;
        let mut when_clauses = Vec::new();

        // Check if there's an operand before WHEN
        if self.current_token != Token::When {
            operand = Some(Box::new(self.parse_expression()?));
        }

        // Parse WHEN clauses
        while self.consume_if(&Token::When) {
            let condition = self.parse_expression()?;
            self.expect(Token::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push(WhenClause { condition, result });
        }

        // Parse optional ELSE clause
        let else_clause = if self.consume_if(&Token::Else) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(Token::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    /// Parses a list of identifier tokens: (col1, col2, col3 ...).
    fn parse_identifier_list(&mut self) -> ParseResult<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            if let Token::Identifier(name) = &self.current_token {
                identifiers.push(name.clone());
                self.next_token();
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected identifier".to_string(),
                ));
            }

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        Ok(identifiers)
    }

    pub fn parse(&mut self) -> ParseResult<Statement> {
        match &self.current_token {
            Token::With => Ok(Statement::With(self.parse_with_statement()?)),
            Token::Select => Ok(Statement::Select(self.parse_select_statement()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert_statement()?)),
            Token::Update => Ok(Statement::Update(self.parse_update_statement()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete_statement()?)),
            Token::Create => {
                let next_token = self.__peek_token();
                match next_token {
                    Token::Table => {
                        Ok(Statement::CreateTable(self.parse_create_table_statement()?))
                    }
                    Token::Index | Token::Unique => {
                        Ok(Statement::CreateIndex(self.parse_create_index_statement()?))
                    }

                    _ => Err(ParserError::UnexpectedToken(next_token.to_string())),
                }
            }
            Token::Alter => Ok(Statement::AlterTable(self.parse_alter_statement()?)),
            Token::Drop => Ok(Statement::DropTable(self.parse_drop_statement()?)),
            Token::Begin | Token::Commit | Token::Rollback => {
                Ok(Statement::Transaction(self.parse_transaction_statement()?))
            }
            _ => Err(ParserError::UnexpectedEof),
        }
    }

    /// Visits an ALTER TABLE statement.
    ///
    /// ```sql
    /// ALTER TABLE [table] ADD COLUMN
    /// ALTER TABLE [table] ADD CONSTRAINT
    /// ALTER TABLE [table] DROP CONSTRAINT
    /// ALTER TABLE [table] DROP COLUMN
    /// ALTER TABLE [table] ALTER COLUMN
    /// [...]
    /// ```
    fn parse_alter_statement(&mut self) -> ParseResult<AlterTableStatement> {
        self.expect(Token::Alter)?;
        self.expect(Token::Table)?;

        // Parse the table name as an identifier.
        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        let action = if self.consume_if(&Token::Add) {
            // Allows both ADD COLUMN and ADD (column identifier) to generate ADD COLUMN statements.
            if self.consume_if(&Token::Column) || matches!(self.current_token, Token::Identifier(_))
            {
                // Add column action requires a column definition
                let column_def = self.parse_column_def()?;
                AlterAction::AddColumn(column_def)
            } else if self.consume_if(&Token::Constraint) {
                // Add constraint action requires a constraint definition.
                let constraint = self.parse_table_constraint()?;
                AlterAction::AddConstraint(constraint)
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected COLUMN or CONSTRAINT after ADD".to_string(),
                ));
            }

        // Drop alter statements
        } else if self.consume_if(&Token::Drop) {
            // Drop column
            if self.consume_if(&Token::Column) {
                if let Token::Identifier(col_name) = &self.current_token {
                    let name = col_name.clone();
                    self.next_token();
                    AlterAction::DropColumn(name)
                } else {
                    return Err(ParserError::InvalidExpression(
                        "expected column name".to_string(),
                    ));
                }

            // Drop constraint
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected COLUMN or CONSTRAINT after DROP".to_string(),
                ));
            }

        // Alter statements to modify a column.
        // Supported ones are:
        //
        // ALTER COLUMN SET DEFAULT [VALUE]
        // ALTER COLUMN SET NOT NULL
        // ALTER COLUMN DROP DEFAULT
        // ALTER COLUMN DROP NOT NULL
        // ALTER COLUMN SET [DATATYPE]
        } else if self.consume_if(&Token::Alter) || self.consume_if(&Token::Modify) {
            self.consume_if(&Token::Column);
            if let Token::Identifier(col_name) = &self.current_token {
                let name = col_name.clone();
                self.next_token();

                let action = if self.consume_if(&Token::Set) {
                    if self.consume_if(&Token::Default) {
                        let expr = self.parse_expression()?;
                        AlterColumnAction::SetDefault(expr)
                    } else if self.consume_if(&Token::Not) {
                        self.expect(Token::Null)?;
                        AlterColumnAction::SetNotNull
                    } else {
                        return Err(ParserError::InvalidExpression(
                            "expected DEFAULT or NOT NULL after SET".to_string(),
                        ));
                    }
                } else if self.consume_if(&Token::Drop) {
                    if self.consume_if(&Token::Default) {
                        AlterColumnAction::DropDefault
                    } else if self.consume_if(&Token::Not) {
                        self.expect(Token::Null)?;
                        AlterColumnAction::DropNotNull
                    } else {
                        return Err(ParserError::InvalidExpression(
                            "expected DEFAULT or NOT NULL after DROP".to_string(),
                        ));
                    }
                } else {
                    // Assume it's a type change
                    let data_type = self.parse_data_type()?;
                    AlterColumnAction::SetDataType(data_type)
                };

                AlterAction::AlterColumn(AlterColumnStatement { name, action })
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected column name".to_string(),
                ));
            }
        } else {
            return Err(ParserError::InvalidExpression(
                "expected ADD, DROP, ALTER, or MODIFY".to_string(),
            ));
        };

        Ok(AlterTableStatement { table, action })
    }

    /// Parses a [CREATE TABLE] statement.
    /// ```sql
    /// CREATE TABLE [table] (
    ///    col_1 [Dtype] [CONSTRAINT],
    ///    col_2 [Dtype],
    ///    [...]
    /// );",
    ///```
    fn parse_create_table_statement(&mut self) -> ParseResult<CreateTableStatement> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;

        // Check for [if_not_exists]
        let if_not_exists = if self.current_token == Token::If {
            self.next_token();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            // Check if it's a table constraint
            if matches!(
                self.current_token,
                Token::Primary | Token::Unique | Token::Foreign | Token::Check | Token::Constraint
            ) {
                constraints.push(self.parse_table_constraint()?);
            } else if let Token::Identifier(name) = &self.current_token {
                let col_name = name.clone();

                self.next_token();

                let data_type = self.parse_data_type()?;
                let mut column_def = ColumnDefExpr::new(col_name, data_type);
                self.parse_column_constraints(&mut column_def)?;
                columns.push(column_def);
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected column definition or constraint".to_string(),
                ));
            }

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        self.expect(Token::RParen)?;

        Ok(CreateTableStatement {
            table,
            columns,
            constraints,
            if_not_exists,
        })
    }

    fn parse_create_index_statement(&mut self) -> ParseResult<CreateIndexStatement> {
        self.expect(Token::Create)?;

        self.expect(Token::Unique)?;
        self.expect(Token::Index)?;

        let if_not_exists = if self.current_token == Token::If {
            self.next_token();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let name = if let Token::Identifier(idx_name) = &self.current_token {
            let index_name = idx_name.clone();
            self.next_token();
            index_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected index name".to_string(),
            ));
        };

        self.expect(Token::On)?;

        let table = if let Token::Identifier(tbl_name) = &self.current_token {
            let table_name = tbl_name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            if let Token::Identifier(col_name) = &self.current_token {
                let name = col_name.clone();
                self.next_token();

                let order = if self.consume_if(&Token::Asc) {
                    Some(OrderDirection::Asc)
                } else if self.consume_if(&Token::Desc) {
                    Some(OrderDirection::Desc)
                } else {
                    None
                };

                columns.push(IndexColumn { name, order });
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected column name".to_string(),
                ));
            }

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        self.expect(Token::RParen)?;

        Ok(CreateIndexStatement {
            name,
            table,
            columns,
            if_not_exists,
        })
    }

    /// Parses an DELETE statement, with multiple optional where clauses.
    ///
    /// ```sql
    /// DELETE FROM [table]
    /// WHERE [expr]
    /// [...]
    /// ```
    fn parse_delete_statement(&mut self) -> ParseResult<DeleteStatement> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            table,
            where_clause,
        })
    }

    /// Parses (visits) a DROP TABLE statement
    ///
    /// ```sql
    /// DROP TABLE [table]
    /// ```
    fn parse_drop_statement(&mut self) -> ParseResult<DropTableStatement> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;

        // Check for IF EXISTS clause
        let if_exists = if let Token::If = &self.current_token {
            self.next_token();
            if self.current_token == Token::Exists {
                self.next_token();
                true
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected EXISTS after IF".to_string(),
                ));
            }
        } else {
            false
        };

        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        let cascade = if let Token::Identifier(s) = &self.current_token {
            if s.to_lowercase() == "cascade" {
                self.next_token();
                true
            } else {
                false
            }
        } else {
            false
        };

        Ok(DropTableStatement {
            table,
            if_exists,
            cascade,
        })
    }

    /// Parses an [INSERT] statement.
    ///  An insert statement consists of the list of columns to insert and an  optional values or query clause
    /// Example with values:
    /// ```sql
    /// INSERT INTO [table] (col1, col2, col3)
    /// VALUES ((value11, value12, value13), (value21, value22, value23));
    /// ```
    /// Example with query:
    /// ```sql
    /// INSERT INTO [table] (col1, col2, col3)
    /// SELECT FROM [other table];
    /// ```
    fn parse_insert_statement(&mut self) -> ParseResult<InsertStatement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        // Parse optional column list
        let columns = if self.current_token == Token::LParen {
            self.next_token();
            let mut cols = Vec::new();
            loop {
                if let Token::Identifier(col) = &self.current_token {
                    cols.push(col.clone());
                    self.next_token();
                } else {
                    return Err(ParserError::InvalidExpression(
                        "expected column name".to_string(),
                    ));
                }
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
            self.expect(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        // Parse [VALUES] or [SELECT]
        let values = if self.consume_if(&Token::Values) {
            let mut value_lists = Vec::new();
            loop {
                self.expect(Token::LParen)?;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_expression()?);
                    if !self.consume_if(&Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::RParen)?;
                value_lists.push(values);

                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
            Values::Values(value_lists)
        } else if self.current_token == Token::Select {
            Values::Query(Box::new(self.parse_select_statement()?))
        } else {
            return Err(ParserError::InvalidExpression(
                "expected VALUES or SELECT".to_string(),
            ));
        };

        Ok(InsertStatement {
            table,
            columns,
            values,
        })
    }

    /// Parses transaction-management statements.
    ///
    /// ```sql
    /// BEGIN / BEGIN TRANSACTION.
    /// COMMIT
    /// ROLLBACK
    /// END TRANSACTION
    /// ```
    fn parse_transaction_statement(&mut self) -> ParseResult<TransactionStatement> {
        match &self.current_token {
            Token::Begin => {
                self.next_token();
                self.consume_if(&Token::Transaction);
                Ok(TransactionStatement::Begin)
            }
            Token::Commit => {
                self.next_token();
                Ok(TransactionStatement::Commit)
            }
            Token::Rollback => {
                self.next_token();
                Ok(TransactionStatement::Rollback)
            }
            _ => Err(ParserError::InvalidExpression(
                "expected BEGIN, COMMIT, or ROLLBACK".to_string(),
            )),
        }
    }

    /// Parses an UPDATE statement, with multiple set and where clauses.
    ///
    /// ```sql
    /// UPDATE [table]
    /// SET [col] = value
    /// WHERE [expr]
    /// [...]
    /// ```
    fn parse_update_statement(&mut self) -> ParseResult<UpdateStatement> {
        self.expect(Token::Update)?;

        let table = if let Token::Identifier(name) = &self.current_token {
            let table_name = name.clone();
            self.next_token();
            table_name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected table name".to_string(),
            ));
        };

        self.expect(Token::Set)?;

        let mut set_clauses = Vec::new();
        loop {
            if let Token::Identifier(col) = &self.current_token {
                let column = col.clone();
                self.next_token();
                self.expect(Token::Eq)?;
                let value = self.parse_expression()?;
                set_clauses.push(SetClause { column, value });
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected column name".to_string(),
                ));
            }

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            table,
            set_clauses,
            where_clause,
        })
    }

    /// Parses a WITH statement, with an optional RECURSIVE clause.
    /// ```sql
    /// WITH
    /// [RECURSIVE]
    /// ALIAS AS
    /// ([CTE])
    ///[...] (Supports up to N ctes)
    /// SELECT [...]
    fn parse_with_statement(&mut self) -> ParseResult<WithStatement> {
        self.expect(Token::With)?;
        let recursive = if matches!(self.current_token, Token::Recursive) {
            self.next_token();
            true
        } else {
            false
        };

        let mut ctes = Vec::new();

        loop {
            let name = if let Token::Identifier(id) = &self.current_token {
                let id = id.clone();
                self.next_token();
                id
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected CTE name".to_string(),
                ));
            };

            self.expect(Token::As)?;
            self.expect(Token::LParen)?;
            let select_stmt = self.parse_select_statement()?;
            self.expect(Token::RParen)?;

            ctes.push((name, select_stmt));

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        let body = Box::new(self.parse_select_statement()?);

        Ok(WithStatement {
            recursive,
            ctes,
            body,
        })
    }

    /// PARSES SELECT STATEMENTS
    /// ```sql
    /// SELECT [Projection]
    /// FROM [Table Reference]
    /// WHERE [Cond]
    /// GROUP BY [Expr]
    /// HAVING [Cond]
    /// ORDER BY [item] [ASC/DESC]
    /// LIMIT n;
    /// ```
    fn parse_select_statement(&mut self) -> ParseResult<SelectStatement> {
        self.expect(Token::Select)?;

        let distinct = self.consume_if(&Token::Distinct);

        // Parse SELECT list
        let columns = self.parse_select_list()?;

        // Parse FROM clause
        let from = if self.consume_if(&Token::From) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY clause
        let mut group_by = Vec::new();
        if self.consume_if(&Token::Group) {
            self.expect(Token::By)?;
            loop {
                group_by.push(self.parse_expression()?);
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
        }

        // Parse HAVING clause
        let having = if self.consume_if(&Token::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY clause
        let mut order_by = Vec::new();
        if self.consume_if(&Token::Order) {
            self.expect(Token::By)?;
            loop {
                let expr = self.parse_expression()?;
                let asc = if self.consume_if(&Token::Desc) {
                    false
                } else {
                    self.consume_if(&Token::Asc);
                    true
                };
                order_by.push(OrderByExpr { expr, asc });
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
        }

        // Parse LIMIT clause
        let limit = if self.consume_if(&Token::Limit) {
            if let Token::NumberLiteral(n) = self.current_token {
                let limit_val = n as usize;
                self.next_token();
                Some(limit_val)
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected number after LIMIT".to_string(),
                ));
            }
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if self.consume_if(&Token::Offset) {
            if let Token::NumberLiteral(n) = self.current_token {
                let offset_val = n as usize;
                self.next_token();
                Some(offset_val)
            } else {
                return Err(ParserError::InvalidExpression(
                    "expected number after OFFSET".to_string(),
                ));
            }
        } else {
            None
        };

        Ok(SelectStatement {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        let mut items = Vec::new();

        loop {
            if self.current_token == Token::Star {
                self.next_token();
                items.push(SelectItem::Star);
            } else {
                let expr = self.parse_expression()?;

                // Check for alias
                let alias = if self.consume_if(&Token::As) {
                    if let Token::Identifier(alias) = &self.current_token {
                        let alias_str = alias.clone();
                        self.next_token();
                        Some(alias_str)
                    } else {
                        return Err(ParserError::InvalidExpression(
                            "expected identifier after AS".to_string(),
                        ));
                    }
                } else if let Token::Identifier(_) = &self.current_token {
                    // Implicit alias without AS
                    if let Token::Identifier(alias) = &self.current_token {
                        let alias_str = alias.clone();
                        self.next_token();
                        Some(alias_str)
                    } else {
                        None
                    }
                } else {
                    None
                };

                items.push(SelectItem::ExprWithAlias { expr, alias });
            }

            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    /// Parses a table reference in the FROM clause, including joins and subqueries.
    /// ```sql
    /// FROM
    /// table1
    /// JOIN table2 ON table1.col1 = table2.col2
    /// [...]
    /// JOIN tablen ON .[..]
    /// ```
    /// Also supports comma-separated tables (implicit cross join):
    /// ```sql
    /// FROM table1, table2, table3
    /// ```
    fn parse_table_ref(&mut self) -> ParseResult<TableReference> {
        let mut table_ref = self.parse_single_table_ref()?;

        // Parse optional JOINs and comma-separated tables
        loop {
            // Check for comma first (implicit cross join)
            if self.consume_if(&Token::Comma) {
                let right = self.parse_single_table_ref()?;
                table_ref = TableReference::Join {
                    left: Box::new(table_ref),
                    join_type: JoinType::Cross,
                    right: Box::new(right),
                    on: None,
                };
                continue;
            }

            let join_type = match &self.current_token {
                Token::Join => {
                    self.next_token();
                    JoinType::Inner
                }
                Token::Inner => {
                    self.next_token();
                    self.expect(Token::Join)?;
                    JoinType::Inner
                }
                Token::Left => {
                    self.next_token();
                    self.expect(Token::Join)?;
                    JoinType::Left
                }
                Token::Right => {
                    self.next_token();
                    self.expect(Token::Join)?;
                    JoinType::Right
                }
                Token::Full => {
                    self.next_token();
                    self.expect(Token::Join)?;
                    JoinType::Full
                }
                Token::Cross => {
                    self.next_token();
                    self.expect(Token::Join)?;
                    JoinType::Cross
                }
                _ => break, // No more joins
            };

            // Parse the right-hand side of the join (single table, not recursive)
            let right = self.parse_single_table_ref()?;

            // Optional ON clause
            let on = if self.consume_if(&Token::On) {
                Some(self.parse_expression()?)
            } else {
                None
            };

            table_ref = TableReference::Join {
                left: Box::new(table_ref),
                join_type,
                right: Box::new(right),
                on,
            };
        }

        Ok(table_ref)
    }

    /// Parses a single table reference (table name or subquery) without joins.
    /// This is a helper for parse_table_ref to handle the base case.
    fn parse_single_table_ref(&mut self) -> ParseResult<TableReference> {
        match &self.current_token {
            Token::Identifier(name) => {
                let table_name = name.clone();
                self.next_token();

                // Parse optional alias (with or without AS keyword)
                let alias = if self.consume_if(&Token::As) {
                    if let Token::Identifier(alias_name) = &self.current_token {
                        let alias_name = alias_name.clone();
                        self.next_token();
                        Some(alias_name)
                    } else {
                        return Err(ParserError::InvalidExpression(
                            "expected alias after AS".to_string(),
                        ));
                    }
                } else if let Token::Identifier(alias_name) = &self.current_token {
                    // Implicit alias (without AS keyword)
                    let alias_name = alias_name.clone();
                    self.next_token();
                    Some(alias_name)
                } else {
                    None
                };

                Ok(TableReference::Table {
                    name: table_name,
                    alias,
                })
            }
            Token::LParen => {
                // Subquery in FROM
                self.next_token();
                let subquery = self.parse_select_statement()?;
                self.expect(Token::RParen)?;

                // Optional AS keyword
                self.consume_if(&Token::As);

                // Alias required for subquery
                let alias = if let Token::Identifier(alias_name) = &self.current_token {
                    let alias_name = alias_name.clone();
                    self.next_token();
                    alias_name
                } else {
                    return Err(ParserError::InvalidExpression(
                        "expected alias for subquery".to_string(),
                    ));
                };

                Ok(TableReference::Subquery {
                    query: Box::new(subquery),
                    alias,
                })
            }
            _ => Err(ParserError::UnexpectedToken(self.current_token.to_string())),
        }
    }
    /// Parses data types.
    ///
    /// Supports both SQL standard data types and AxmosDB specific types (VARINT, BLOB and TEXT).
    fn parse_data_type(&mut self) -> ParseResult<DataTypeKind> {
        let data_type = if let Token::Identifier(type_name) = &self.current_token {
            let name = type_name.to_uppercase();
            self.next_token();

            match name.as_str() {
                "INT" | "INTEGER" => DataTypeKind::Int,
                "BIGINT" => DataTypeKind::BigInt,
                "UINT" | "UNSIGNED INTEGER" => DataTypeKind::UInt,
                "BIGUINT" => DataTypeKind::BigUInt,
                "FLOAT" => DataTypeKind::Float,
                "DOUBLE" => DataTypeKind::Double,
                "TEXT" => DataTypeKind::Blob,
                "BOOLEAN" | "BOOL" => DataTypeKind::Bool,
                "BLOB" => DataTypeKind::Blob,
                _ => {
                    return Err(ParserError::UnexpectedToken(name.to_string()));
                }
            }
        } else {
            return Err(ParserError::InvalidExpression(
                "expected data type".to_string(),
            ));
        };

        Ok(data_type)
    }

    /// Parses a column definition statement.
    /// [COL_NAME] [DATA TYPE] [CONSTRAINTS]
    fn parse_column_def(&mut self) -> ParseResult<ColumnDefExpr> {
        let name = if let Token::Identifier(col_name) = &self.current_token {
            let name = col_name.clone();
            self.next_token();
            name
        } else {
            return Err(ParserError::InvalidExpression(
                "expected column name".to_string(),
            ));
        };

        let data_type = self.parse_data_type()?;
        let mut column_def = ColumnDefExpr::new(name, data_type);
        self.parse_column_constraints(&mut column_def)?;

        Ok(column_def)
    }

    /// Parses all types of column constraints, modifying the passed [ColumnDefExpr] in place in order to set the proper bits.
    fn parse_column_constraints(
        &mut self,
        column_def: &mut ColumnDefExpr,
    ) -> Result<(), ParserError> {
        loop {
            match &self.current_token {
                Token::Not => {
                    self.next_token();
                    self.expect(Token::Null)?;
                    column_def.is_non_null = true;
                }
                Token::Default => {
                    self.next_token();
                    let expr = self.parse_expression()?;
                    column_def.default = Some(expr);
                }
                _ => break,
            }
        }

        Ok(())
    }

    /// Parses constraints applied at table level.
    /// Ex:
    ///
    /// ```sql
    /// ALTER TABLE foo ADD CONSTRAINT [body];
    /// ```
    fn parse_table_constraint(&mut self) -> ParseResult<TableConstraintExpr> {
        // Skip optional CONSTRAINT name
        if self.consume_if(&Token::Constraint)
            && let Token::Identifier(_) = self.current_token
        {
            self.next_token();
        }

        match &self.current_token {
            Token::Primary => {
                self.next_token();
                self.expect(Token::Key)?;
                self.expect(Token::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(Token::RParen)?;
                Ok(TableConstraintExpr::PrimaryKey(columns))
            }
            Token::Unique => {
                self.next_token();
                self.expect(Token::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(Token::RParen)?;
                Ok(TableConstraintExpr::Unique(columns))
            }
            Token::Foreign => {
                self.next_token();
                self.expect(Token::Key)?;
                self.expect(Token::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(Token::RParen)?;

                self.expect(Token::References)?;
                let ref_table = if let Token::Identifier(name) = &self.current_token {
                    let table = name.clone();
                    self.next_token();
                    table
                } else {
                    return Err(ParserError::InvalidExpression(
                        "expected referenced table name".to_string(),
                    ));
                };

                self.expect(Token::LParen)?;
                let ref_columns = self.parse_identifier_list()?;
                self.expect(Token::RParen)?;

                Ok(TableConstraintExpr::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                })
            }
            _ => Err(ParserError::UnexpectedToken(self.current_token.to_string())),
        }
    }
}
