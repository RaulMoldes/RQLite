use crate::database::schema::{Database, Schema};
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::sql::ast::{Expr, JoinType};


pub struct GroupBy {
    group_by: Vec<Expr>,
    having: Option<Expr>,

}



impl GroupBy {
    pub fn new(group_by: Vec<Expr>, having: Option<Expr>) -> Self {
        Self {
            group_by,
            having
        }
    }
}

impl ExecutionPlanStep for GroupBy {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
       None
    }
}
