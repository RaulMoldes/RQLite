mod analyzer;
pub mod ast;
mod binder;
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
pub fn parse_sql(sql: &str) -> SQLResult<Statement> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    Ok(stmt)
}

/// Parse a SQL query string into an AST
pub(crate) fn parsing_pipeline(sql: &str, db: &Database) -> SQLResult<Statement> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    let mut analyzer = Analyzer::new(db.catalog(), db.main_worker_cloned());
    let mut preparator = Binder::new(db.catalog(), db.main_worker_cloned());
    analyzer.analyze(&stmt)?;
    preparator.bind(&mut stmt)?;
    Ok(stmt)
}
