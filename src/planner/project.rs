use crate::database::schema::{Database, Schema};
use crate::planner::{ExecutionPlanStep, ResultSet};

// Project step
pub struct Project {
    columns: Vec<String>,
    schema: Schema,
}

impl Project {
    pub fn new(columns: Vec<String>, schema: Schema) -> Self {
        Self {
            columns, schema
        }
    }
}
impl ExecutionPlanStep for Project {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None // TODO!
    }
}
