mod analyzer;
pub mod ast;
mod lexer;
mod parser;
mod preparator;

use crate::database::Database;
use crate::sql::ast::Simplify;
use crate::sql::parser::ParserError;
use crate::sql::preparator::Preparator;
use analyzer::{Analyzer, AnalyzerError};
pub(crate) use ast::Statement;
use lexer::Lexer;
use parser::Parser;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq)]
pub enum SQLError {
    ParserError(ParserError),
    AnalyzerError(AnalyzerError),
}

impl std::fmt::Display for SQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnalyzerError(e) => write!(f, "analyzer error {e}"),
            Self::ParserError(e) => write!(f, "parser error {e}"),
        }
    }
}

impl From<ParserError> for SQLError {
    fn from(value: ParserError) -> Self {
        Self::ParserError(value)
    }
}

impl From<AnalyzerError> for SQLError {
    fn from(value: AnalyzerError) -> Self {
        Self::AnalyzerError(value)
    }
}

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
