mod analyzer;
pub mod ast;
pub mod lexer;
mod parser;
mod preparator;

use crate::database::{Database, errors::SQLError};

use crate::sql::ast::Simplify;
use crate::sql::preparator::Preparator;

use analyzer::Analyzer;

pub(crate) use ast::Statement;
use lexer::Lexer;
use parser::Parser;

#[cfg(test)]
mod tests;

/// Parse a SQL query string into an AST
pub(crate) fn parse_sql(sql: &str) -> Result<Statement, SQLError> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    Ok(stmt)
}

/// Parse a SQL query string into an AST
pub(crate) fn parsing_pipeline(sql: &str, db: &Database) -> Result<Statement, SQLError> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    let mut analyzer = Analyzer::new(db);
    let mut preparator = Preparator::new(db);
    analyzer.analyze(&stmt)?;
    preparator.prepare_stmt(&mut stmt);
    Ok(stmt)
}
