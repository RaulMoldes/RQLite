use crate::database::schema::Schema;
use crate::database::Database;
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::sql::ast::{Expr, JoinType};

pub struct NestedLoopJoin {
    join_type: JoinType,
    on_condition: Option<Expr>,
    left_schema: Schema,
    right_schema: Schema,
}

impl ExecutionPlanStep for NestedLoopJoin {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None
    }
}
