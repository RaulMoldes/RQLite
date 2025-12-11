pub mod analyzer;
pub mod ast;
pub mod binder;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod planner;

use crate::{
    database::{Database, errors::SQLResult},
    sql::{
        analyzer::Analyzer,
        ast::{Simplify, Statement},
        binder::Binder,
        lexer::Lexer,
        parser::Parser,
    },
};

#[cfg(test)]
mod tests;

/// Parse a SQL query string into an AST
pub(crate) fn parse_sql(sql: &str) -> SQLResult<Statement> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    Ok(stmt)
}

