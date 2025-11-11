use crate::database::schema::{Database, Schema};
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::sql::ast::{
    BinaryOperator, Expr, JoinType, OrderByExpr, SelectItem, SelectStatement, TableReference,
};
use crate::structures::bplustree::{BPlusTree, Comparator};
use std::collections::VecDeque;

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
