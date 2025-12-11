// src/sql/executor/mod.rs
pub mod build;
pub mod context;
pub mod ddl;
pub mod eval;
pub mod ops;

use crate::{
    database::{errors::ExecutionResult, schema::Schema},
    types::DataType,
    vector,
};

use std::fmt::{Display, Formatter, Result as FmtResult};

vector!(Row, DataType);

impl From<&[DataType]> for Row {
    fn from(value: &[DataType]) -> Self {
        Self(value.to_vec())
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        for (i, value) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, " | ")?;
            }
            write!(f, "{}", value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ExecutionState {
    Open,
    Closed,
    Running,
}

impl Display for ExecutionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Open => f.write_str("OPEN"),
            Self::Closed => f.write_str("CLOSED"),
            Self::Running => f.write_str("RUNNING"),
        }
    }
}

/// The Volcano/Iterator model trait
pub(crate) trait Executor {
    /// Initialize the operator and its children
    fn open(&mut self) -> ExecutionResult<()>;

    /// Fetch the next row, returns None when exhausted
    fn next(&mut self) -> ExecutionResult<Option<Row>>;

    /// Clean up resources
    fn close(&mut self) -> ExecutionResult<()>;

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

/*
#[cfg(test)]
mod query_execution_tests {
    use super::*;
    use serial_test::serial;
    use std::path::Path;

    use crate::{
        AxmosDBConfig, IncrementalVaccum, TextEncoding,
        database::{
            Database,
            errors::TransactionResult,
            schema::{Column, Schema},
            stats::CatalogStatsProvider,
        },
        io::pager::{Pager, SharedPager},
        sql::{
            binder::Binder,
            executor::build::ExecutorBuilder,
            lexer::Lexer,
            parser::Parser,
            planner::{
                CascadesOptimizer, OptimizerConfig, ProcessedStatement, model::AxmosCostModel,
            },
        },
        transactions::accessor::RcPageAccessorPool,
        types::{Blob, DataType, DataTypeKind, UInt64},
    };

    fn create_db(
        page_size: usize,
        cache_size: usize,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: page_size as u32,
            cache_size: Some(cache_size as u16),
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
            .create_table("users", schema, db.main_accessor.clone()d())?;

        // Insert test data
        let handle = db.coordinator().begin().unwrap();
        let pool = RcPageAccessorPool::from(handle);
        pool.begin().unwrap();

        for (name, age) in [
            ("Alice", 25u64),
            ("Bob", 30),
            ("Charlie", 35),
            ("Diana", 28),
            ("Diana", 22),
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

    fn execute_query(db: &Database, sql: &str) -> TransactionResult<Vec<Row>> {
        // 1. Parse
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse()?;

        // 2. Bind
        let mut binder = Binder::new(db.catalog(), db.main_accessor.clone()d());
        let bound_stmt = binder.bind(&stmt)?;

        // 3. Process (handles both DDL and queries)
        let catalog = db.catalog();
        let accessor = db.main_accessor.clone()d();
        let cost_model = AxmosCostModel::new(4096);
        let stats_provider = CatalogStatsProvider::new(catalog.clone(), accessor.clone());

        let mut optimizer = CascadesOptimizer::new(
            catalog.clone(),
            accessor.clone(),
            OptimizerConfig::default(),
            cost_model,
            stats_provider,
        );

        match optimizer.process(&bound_stmt)? {
            ProcessedStatement::DdlExecuted(result) => Ok(vec![result.to_row()]),
            ProcessedStatement::Plan(physical_plan) => {
                // 4. Build and execute the plan
                let handle = db.coordinator().begin()?;
                let pool = RcPageAccessorPool::from(handle);
                pool.begin()?;

                let ctx = pool.execution_context();
                let builder = ExecutorBuilder::new(ctx);
                let mut executor = builder.build(&physical_plan)?;

                executor.open()?;
                let mut results = Vec::new();
                while let Some(row) = executor.next()? {
                    results.push(row);
                }
                executor.close()?;

                pool.try_commit()?;
                Ok(results)
            }
        }
    }

    #[test]
    #[serial]
    fn test_select_all() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users").unwrap();
        for row in &results {
            println!("{row}");
        }
        assert_eq!(results.len(), 5, "Should return all 5 users");
    }

    #[test]
    #[serial]
    fn test_select_order_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users ORDER BY name DESC LIMIT 1").unwrap();
        for row in &results {
            println!("{row}");
        }
        assert_eq!(results.len(), 1, "Should return all 5 users");
    }

    #[test]
    #[serial]
    fn test_select_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT name, COUNT(*) FROM users GROUP BY name").unwrap();
        for row in &results {
            println!("{row}");
        }
        assert_eq!(results.len(), 4, "Should return all 5 users");
    }

    #[test]
    #[serial]
    fn test_select_with_where_greater_than() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age > 28").unwrap();
        for row in &results {
            println!("{row}");
        }

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
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age = 25").unwrap();

        // Only Alice(25)
        assert_eq!(results.len(), 1, "Should return 1 user with age = 25");
    }

    #[test]
    #[serial]
    fn test_select_with_where_no_matches() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results = execute_query(&db, "SELECT * FROM users WHERE age > 100").unwrap();

        assert_eq!(results.len(), 0, "Should return no users");
    }

    #[test]
    #[serial]
    fn test_select_with_compound_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        let results =
            execute_query(&db, "SELECT * FROM users WHERE age >= 25 AND age <= 30").unwrap();

        // Alice(25), Bob(30), Diana(28)
        assert_eq!(
            results.len(),
            3,
            "Should return 3 users with 25 <= age <= 30"
        );
    }

    #[test]
    #[serial]
    fn test_insert_single_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Insert a new user
        let results =
            execute_query(&db, "INSERT INTO users (name, age) VALUES ('Frank', 40)").unwrap();

        // Should return 1 row with affected count
        assert_eq!(results.len(), 1, "Insert should return one result row");
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 1, "Should have inserted 1 row");
        } else {
            panic!("Expected BigUInt for affected row count");
        }

        // Verify the row was inserted
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age = 40").unwrap();
        assert_eq!(select_results.len(), 1, "Should find the inserted user");
    }

    #[test]
    #[serial]
    fn test_insert_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Insert multiple users
        let results = execute_query(
            &db,
            "INSERT INTO users (name, age) VALUES ('Frank', 40), ('Grace', 45), ('Henry', 50)",
        )
        .unwrap();

        // Should return affected count of 3
        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 3, "Should have inserted 3 rows");
        }

        // Verify all rows were inserted
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age >= 40").unwrap();
        assert_eq!(select_results.len(), 3, "Should find all 3 inserted users");
    }

    #[test]
    #[serial]
    fn test_update_single_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Update Alice's age from 25 to 26
        let results = execute_query(&db, "UPDATE users SET age = 26 WHERE age = 25").unwrap();

        // Should return affected count
        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 1, "Should have updated 1 row");
        }

        // Verify the update
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age = 26").unwrap();
        assert_eq!(select_results.len(), 1, "Should find user with updated age");

        // Verify old value is gone
        let old_results = execute_query(&db, "SELECT * FROM users WHERE age = 25").unwrap();
        assert_eq!(old_results.len(), 0, "Should not find user with old age");
    }

    #[test]
    #[serial]
    fn test_update_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Update all users with age < 30 to age = 29
        let results = execute_query(&db, "UPDATE users SET age = 29 WHERE age < 30").unwrap();

        // Alice(25), Diana(28), Eve(22) should be updated
        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 3, "Should have updated 3 rows");
        }

        // Verify the updates
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age = 29").unwrap();
        assert_eq!(select_results.len(), 3, "Should find 3 users with age 29");
    }

    #[test]
    #[serial]
    fn test_update_no_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Try to update non-existent rows
        let results = execute_query(&db, "UPDATE users SET age = 100 WHERE age > 1000").unwrap();

        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 0, "Should have updated 0 rows");
        }
    }

    #[test]
    #[serial]
    fn test_delete_single_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Delete Alice (age = 25)
        let results = execute_query(&db, "DELETE FROM users WHERE age = 25").unwrap();

        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 1, "Should have deleted 1 row");
        }

        // Verify deletion
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age = 25").unwrap();
        assert_eq!(select_results.len(), 0, "Should not find deleted user");

        // Verify others remain
        let all_results = execute_query(&db, "SELECT * FROM users").unwrap();
        assert_eq!(all_results.len(), 4, "Should have 4 remaining users");
    }

    #[test]
    #[serial]
    fn test_delete_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Delete all users with age < 30
        let results = execute_query(&db, "DELETE FROM users WHERE age < 30").unwrap();

        // Alice(25), Diana(28), Eve(22) should be deleted
        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 3, "Should have deleted 3 rows");
        }

        // Verify deletions
        let select_results = execute_query(&db, "SELECT * FROM users").unwrap();
        assert_eq!(select_results.len(), 2, "Should have 2 remaining users");

        // Verify remaining users are Bob(30) and Charlie(35)
        for row in &select_results {
            if let DataType::BigUInt(UInt64(age)) = &row[2] {
                assert!(*age >= 30, "Remaining users should have age >= 30");
            }
        }
    }

    #[test]
    #[serial]
    fn test_delete_no_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Try to delete non-existent rows
        let results = execute_query(&db, "DELETE FROM users WHERE age > 1000").unwrap();

        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 0, "Should have deleted 0 rows");
        }

        // Verify all users remain
        let all_results = execute_query(&db, "SELECT * FROM users").unwrap();
        assert_eq!(all_results.len(), 5, "Should still have all 5 users");
    }

    #[test]
    #[serial]
    fn test_delete_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Delete all users
        let results = execute_query(&db, "DELETE FROM users").unwrap();

        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 5, "Should have deleted 5 rows");
        }

        // Verify table is empty
        let select_results = execute_query(&db, "SELECT * FROM users").unwrap();
        assert_eq!(select_results.len(), 0, "Table should be empty");
    }

    #[test]
    #[serial]
    fn test_insert_then_update_then_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Insert a new user
        execute_query(&db, "INSERT INTO users (name, age) VALUES ('Zara', 99)").unwrap();

        // Verify insert
        let results = execute_query(&db, "SELECT * FROM users WHERE age = 99").unwrap();
        assert_eq!(results.len(), 1, "Should find inserted user");

        // Update the user
        execute_query(&db, "UPDATE users SET age = 100 WHERE age = 99").unwrap();

        // Verify update
        let results = execute_query(&db, "SELECT * FROM users WHERE age = 100").unwrap();
        assert_eq!(results.len(), 1, "Should find updated user");

        // Delete the user
        execute_query(&db, "DELETE FROM users WHERE age = 100").unwrap();

        // Verify delete
        let results = execute_query(&db, "SELECT * FROM users WHERE age = 100").unwrap();
        assert_eq!(results.len(), 0, "Should not find deleted user");

        // Original users should still be there
        let all_results = execute_query(&db, "SELECT * FROM users").unwrap();
        assert_eq!(all_results.len(), 5, "Should still have original 5 users");
    }

    #[test]
    #[serial]
    fn test_update_with_expression() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(4096, 100, dir.path().join("test.db")).unwrap();

        // Update age to age + 1 for all users
        let results = execute_query(&db, "UPDATE users SET age = age + 1").unwrap();

        assert_eq!(results.len(), 1);
        if let DataType::BigUInt(UInt64(count)) = &results[0][0] {
            assert_eq!(*count, 5, "Should have updated 5 rows");
        }

        // Verify: Alice should now be 26 (was 25)
        let select_results = execute_query(&db, "SELECT * FROM users WHERE age = 26").unwrap();
        assert_eq!(select_results.len(), 1, "Alice should now have age 26");

        // Verify: No one should have age 25 anymore
        let old_results = execute_query(&db, "SELECT * FROM users WHERE age = 25").unwrap();
        assert_eq!(old_results.len(), 0, "No one should have age 25");
    }
}
*/
