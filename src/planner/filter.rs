use crate::database::Database;
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::sql::ast::Expr;



pub struct Filter {
    predicate: Expr,

}

impl Filter {
    pub fn new(predicate: Expr) -> Self {
        Self { predicate }
    }
}



impl ExecutionPlanStep for Filter {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None
    }
}
