use crate::database::schema::Database;
use crate::database::schema::Schema;
use crate::planner::{ExecutionPlanStep, ResultSet};
use crate::storage::tuple::TupleRef;
use crate::structures::bplustree::{
    IterDirection, NodeAccessMode,
};
use crate::types::OId;

pub enum Scan {
    SeqScan {
        table_oid: OId,
        schema: Schema,
        cached_resultset: Option<ResultSet>,
    },
    IndexScan {
        table_oid: OId,
        index_oid: OId,
        table_schema: Schema,
        index_schema: Schema,
        start_key: Option<Box<[u8]>>,
        end_key: Option<Box<[u8]>>,
        cached_resultset: Option<ResultSet>,
    },
}

impl ExecutionPlanStep for Scan {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()> {
        match self {
            Scan::SeqScan {
                table_oid,
                schema,
                cached_resultset,
                ..
            } => {
                let table_obj = db.get_obj(*table_oid)?;
                let table = db.build_tree(table_obj)?;

                let mut collected = ResultSet::new();
                for item in table.iter()? {
                    match item {
                        Ok(payload) => collected.push(Box::from(payload)),
                        Err(e) => return Err(e),
                    }
                }
                *cached_resultset = Some(collected);
                Ok(())
            }

            Scan::IndexScan {
                table_oid,
                index_oid,
                table_schema,
                index_schema,
                start_key,
                end_key,
                cached_resultset,
                ..
            } => {
                let table_obj = db.get_obj(*table_oid)?;
                let table = db.build_tree(table_obj)?;

                let index_obj = db.get_obj(*index_oid)?;

                let index = db.build_tree(index_obj)?;

                let mut collected = ResultSet::new();

                // Perform index scan with range if provided
                let start = start_key.as_deref().unwrap_or(&[]);
                let end = end_key.as_deref().unwrap_or(&[]);

                for item in index.iter_from(start, IterDirection::Forward)? {
                    match item {
                        Ok(payload) => {
                            // Check if we've reached the end of range
                            if !end.is_empty()
                                && index.compare(payload.as_ref(), end)?
                                    == std::cmp::Ordering::Greater
                            {
                                break;
                            }

                            // Extract rowid from index tuple
                            let tuple = TupleRef::read(payload.as_ref(), index_schema)?;
                            let rowid = tuple.value(index_schema, 1)?;

                            let table_result =
                                table.search_from_root(rowid.as_ref(), NodeAccessMode::Read)?;
                            if let Some(table_payload) = table.get_content_from_result(table_result)
                            {
                                collected.push(Box::from(table_payload));
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }

                *cached_resultset = Some(collected);

                Ok(())
            }
        }
    }

    fn take_result_set(&mut self) -> Option<ResultSet> {
        match self {
            Scan::SeqScan {
                cached_resultset,
                ..
            } => cached_resultset.take(),
            Scan::IndexScan {
                cached_resultset,
                ..
            } => cached_resultset.take(),
        }
    }
}
