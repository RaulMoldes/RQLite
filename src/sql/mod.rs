mod ast;
mod parser;
mod lexer;
mod analyzer;



use ast::Statement;
use lexer::Lexer;
use parser::Parser;

use crate::sql::ast::Simplify;

#[cfg(test)]
mod tests;


/// Parse a SQL query string into an AST
pub(crate) fn parse_sql(sql: &str) -> Result<Statement, String> {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut stmt = parser.parse()?;
    stmt.simplify()?;
    Ok(stmt)

}
