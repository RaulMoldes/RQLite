use crate::database::Database;
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::types::ObjectId;

pub(crate) enum Scan {
    SeqScan {
        table_id: ObjectId,
        cached_resultset: Option<ResultSet>,
    },
    IndexScan {
        table_id: ObjectId,
        index_id: ObjectId,
        start_key: Option<Box<[u8]>>,
        end_key: Option<Box<[u8]>>,
        cached_resultset: Option<ResultSet>,
    },
}

impl ExecutionPlanStep for Scan {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        // todo: implement scans
        Ok(())
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        None
    }
}
