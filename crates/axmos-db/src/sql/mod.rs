pub mod analyzer;
pub mod ast;
pub mod binder;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod planner;

use crate::{
    database::errors::ExecutionResult,
    sql::{
        ast::{Simplify, Statement},
        lexer::Lexer,
        parser::Parser,
    },
};

#[cfg(test)]
mod tests;

/// Parse a SQL query string into an AST
pub(crate) fn parse_sql(sql: &str) -> ExecutionResult<Statement> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    Ok(stmt)
}
