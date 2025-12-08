// src/sql/executor/mod.rs
pub mod context;
pub mod eval;
pub mod ops;

use std::io;

use crate::database::schema::Schema;
use crate::sql::executor::context::ExecutionContext;
use crate::sql::executor::ops::{filter::Filter, project::Project, seq_scan::SeqScanExecutor};
use crate::sql::planner::physical::{PhysicalOperator, PhysicalPlan};
use crate::types::DataType;

use std::io::{Error as IoError, ErrorKind};

/// Result of a single row fetch
pub(crate) type Row = Vec<DataType>;

/// The Volcano/Iterator model trait
pub(crate) trait Executor {
    /// Initialize the operator and its children
    fn open(&mut self) -> io::Result<()>;

    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> io::Result<Option<Row>>;

    /// Clean up resources
    fn close(&mut self) -> io::Result<()>;

    /// Get the output schema for this operator
    fn schema(&self) -> &Schema;
}

/// Execution statistics for profiling
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_produced: u64,
    pub rows_scanned: u64,
    pub pages_read: u64,
}

/// Builds an executor tree from a physical plan.
pub(crate) struct ExecutorBuilder {
    ctx: ExecutionContext,
}

impl ExecutorBuilder {
    pub(crate) fn new(ctx: ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Build an executor tree from a physical plan.
    pub(crate) fn build(&self, plan: &PhysicalPlan) -> io::Result<Box<dyn Executor>> {
        self.build_operator(&plan.op, &plan.children)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> io::Result<Box<dyn Executor>> {
        match op {
            PhysicalOperator::SeqScan(scan_op) => {
                let executor = SeqScanExecutor::new(scan_op.clone(), self.ctx.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Filter(filter_op) => {
                if children.len() != 1 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Filter requires exactly 1 child",
                    ));
                }
                let child = self.build(&children[0])?;
                let executor = Filter::new(child, filter_op.predicate.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Project(project_op) => {
                if children.len() != 1 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Project requires exactly 1 child",
                    ));
                }
                let child = self.build(&children[0])?;
                let expressions: Vec<_> = project_op
                    .expressions
                    .iter()
                    .map(|pe| pe.expr.clone())
                    .collect();

                let executor = Project::new(child, expressions, project_op.output_schema.clone());
                Ok(Box::new(executor))
            }

            _ => Err(IoError::new(
                ErrorKind::Unsupported,
                format!("{} executor not yet implemented", op.name()),
            )),
        }
    }
}

/// Pretty prints a row of data types
fn pretty_print_row(row: &[DataType]) -> String {
    let values: Vec<String> = row
        .iter()
        .map(|dt| match dt {
            DataType::Null => "NULL".to_string(),
            DataType::Char(u) => char::from(u.0).to_string(),

            DataType::Boolean(b) => if b.0 != 0 { "true" } else { "false" }.to_string(),
            DataType::SmallInt(v) => v.0.to_string(),
            DataType::HalfInt(v) => v.0.to_string(),
            DataType::Int(v) => v.0.to_string(),
            DataType::BigInt(v) => v.0.to_string(),
            DataType::SmallUInt(v) | DataType::Byte(v) => v.0.to_string(),
            DataType::HalfUInt(v) => v.0.to_string(),
            DataType::UInt(v) => v.0.to_string(),
            DataType::BigUInt(v) => v.0.to_string(),
            DataType::Float(v) => v.0.to_string(),
            DataType::Double(v) => v.0.to_string(),
            DataType::Text(blob) => {
                format!("'{}'", blob.as_str(crate::TextEncoding::Utf8))
            }
            DataType::Blob(blob) => format!("<blob {} bytes>", blob.len()),
            DataType::Date(d) => format!("DATE({})", d.0),
            DataType::DateTime(dt) => format!("DATETIME({})", dt.0),
        })
        .collect();

    format!("({})", values.join(", "))
}

/// Pretty prints multiple rows as a table
fn pretty_print_results(rows: &[Vec<DataType>]) {
    println!("┌─────────────────────────────────────┐");
    println!("│ Results: {} row(s)                  │", rows.len());
    println!("├─────────────────────────────────────┤");
    for (i, row) in rows.iter().enumerate() {
        println!("│ [{}] {}", i, pretty_print_row(row));
    }
    println!("└─────────────────────────────────────┘");
}

#[cfg(test)]
mod query_execution_tests {
    use super::*;
    use serial_test::serial;
    use std::path::Path;

    use crate::database::Database;
    use crate::database::schema::{Column, Schema};
    use crate::database::stats::CatalogStatsProvider;
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::binder::Binder;
    // use crate::sql::executor::builder::ExecutorBuilder;
    use crate::sql::executor::Executor;
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::sql::planner::model::AxmosCostModel;
    use crate::sql::planner::{CascadesOptimizer, OptimizerConfig};
    use crate::transactions::worker::WorkerPool;
    use crate::types::{Blob, DataType, DataTypeKind, UInt64};
    use crate::{AxmosDBConfig, IncrementalVaccum, TextEncoding};

    fn setup_db(path: impl AsRef<Path>) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        // Create users table
        let schema = Schema::from_columns(
            &[
                Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
                Column::new_unindexed(DataTypeKind::Text, "name", None),
                Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
            ],
            1,
        );
        db.catalog()
            .create_table("users", schema, db.main_worker_cloned())?;

        // Insert test data
        let handle = db.coordinator().begin().unwrap();
        let pool = WorkerPool::from(handle);
        pool.begin().unwrap();

        for (name, age) in [
            ("Alice", 25u64),
            ("Bob", 30),
            ("Charlie", 35),
            ("Diana", 28),
            ("Eve", 22),
        ] {
            pool.insert_one(
                "users",
                &[
                    DataType::Text(Blob::from(name)),
                    DataType::BigUInt(UInt64::from(age)),
                ],
            )
            .unwrap();
        }

        pool.try_commit().unwrap();

        Ok(db)
    }

    /// Execute a SQL query and return all result rows.
    fn execute_query(db: &Database, sql: &str) -> std::io::Result<Vec<Vec<DataType>>> {
        // 1. Parse
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

        // 2. Bind
        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        let bound_stmt = binder
            .bind(&stmt)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

        // 3. Optimize
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();
        let cost_model = AxmosCostModel::new(4096);
        let stats_provider = CatalogStatsProvider::new(catalog.clone(), worker.clone());

        let mut optimizer = CascadesOptimizer::new(
            catalog.clone(),
            worker.clone(),
            OptimizerConfig::default(),
            cost_model,
            stats_provider,
        );

        let physical_plan = optimizer
            .optimize(&bound_stmt)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        // 4. Build executor from physical plan
        let handle = db
            .coordinator()
            .begin()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        let pool = WorkerPool::from(handle);
        pool.begin()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let ctx = pool.execution_context();
        let builder = ExecutorBuilder::new(ctx);
        println!("EXPLAIN {physical_plan}");
        let mut executor = builder.build(&physical_plan)?;

        // 5. Execute and collect results
        executor.open()?;

        let mut results = Vec::new();
        while let Some(row) = executor.next()? {
            results.push(row);
        }

        executor.close()?;
        pool.try_commit()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(results)
    }

    #[test]
    #[serial]
    fn test_select_all() {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users").unwrap();
        pretty_print_results(&results);
        assert_eq!(results.len(), 5, "Should return all 5 users");
    }

    #[test]
    #[serial]
    fn test_select_with_where_greater_than() {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age > 28").unwrap();
        pretty_print_results(&results);

        //Bob(30), Charlie(35) pass the filter
        assert_eq!(results.len(), 2, "Should return 2 users with age > 28");

        for row in &results {
            if let DataType::BigUInt(UInt64(age)) = &row[2] {
                assert!(*age > 28, "All returned rows should have age > 28");
            }
        }
    }

    #[test]
    #[serial]
    fn test_select_with_where_equality() {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age = 25").unwrap();

        // Only Alice(25)
        assert_eq!(results.len(), 1, "Should return 1 user with age = 25");
    }

    #[test]
    #[serial]
    fn test_select_with_where_no_matches() {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age > 100").unwrap();

        assert_eq!(results.len(), 0, "Should return no users");
    }

    #[test]
    #[serial]
    fn test_select_with_compound_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(dir.path().join("test.db")).unwrap();

        let results =
            execute_query(&db, "SELECT * FROM users WHERE age >= 25 AND age <= 30").unwrap();

        // Alice(25), Bob(30), Diana(28)
        assert_eq!(
            results.len(),
            3,
            "Should return 3 users with 25 <= age <= 30"
        );
    }
}
