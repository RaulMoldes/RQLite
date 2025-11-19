use crate::database::Database;
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::sql::ast::OrderDirection;

pub struct OrderBy {
    columns: Vec<String>,
    direction: OrderDirection,
}
impl OrderBy {
    fn new(columns: Vec<String>, direction: OrderDirection) -> Self {
        Self { columns, direction }
    }
}
impl ExecutionPlanStep for OrderBy {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        // Would sort the result set based on order expressions
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None // TODO!
    }
}

// Limit step
pub struct Limit {
    limit: usize,
}

impl Limit {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl ExecutionPlanStep for Limit {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        // Would truncate result set to limit
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None
    }
}
