mod tests;
mod utils;

use crate::{
    DBConfig, Database, DatabaseError, QueryResult, matrix_tests, param_tests, param2_tests,
};
use tests::*;
use utils::*;

// Database creation tests
#[test]
fn test_create_database() {
    let db = TestDb::new();
    assert!(db.path().exists());
}

#[test]
fn test_create_existing_fails() {
    let db = TestDb::new();
    let path = db.path();
    let result = Database::create(&path, DBConfig::default());
    assert!(matches!(result, Err(DatabaseError::AlreadyExists(_))));
}

#[test]
fn test_open_nonexistent_fails() {
    let (dir, path) = temp_db_path();
    let result = Database::open(&path, DBConfig::default());
    assert!(matches!(result, Err(DatabaseError::NotFound(_))));
    drop(dir);
}

#[test]
fn test_open_or_create() {
    let (dir, path) = temp_db_path();
    let db1 = Database::open_or_create(&path, DBConfig::default()).unwrap();
    drop(db1);
    let db2 = Database::open_or_create(&path, DBConfig::default()).unwrap();
    drop(db2);
    drop(dir);
}

// Recovery tests
param_tests!(test_create_and_open, flush => [true, false]);

param_tests!(test_recovery_insert_count, count => [1, 10, 50, 100, 200]);

param2_tests!(
    test_recovery_with_updates, initial, updates => [
        (10, 5),
        (20, 10),
        (50, 25)
    ]
);

param2_tests!(
    test_recovery_with_deletes, initial, deletes => [
        (10, 3),
        (20, 10),
        (50, 20)
    ]
);

matrix_tests!(
    test_multiple_tables_recovery,
    tables => [1, 2, 3],
    rows => [5, 10, 20]
);

// Batch execution tests
param_tests!(test_batch_execution, count => [1, 5, 10, 20]);

#[test]
fn test_batch_rollback_on_failure() {
    let db = TestDb::new();
    setup_items_table(&db);
    db.execute_ok("INSERT INTO items VALUES (1, 100)");

    let result = db.db.execute_batch(&[
        "INSERT INTO items VALUES (2, 200)",
        "INSERT INTO items VALUES (3, 300)",
        "INSERT INTO nonexistent VALUES (4, 400)",
    ]);

    assert!(result.is_err());
    assert_eq!(db.query_count("items"), 1);
}

// Session transaction tests
param_tests!(test_session_transaction_commit, count => [1, 5, 10]);
param_tests!(test_session_transaction_rollback, count => [1, 5, 10]);

#[test]
fn test_session_error_allows_rollback() {
    let db = TestDb::new();
    setup_test_table(&db);
    db.execute_ok("INSERT INTO test VALUES (1, 100)");

    let mut session = db.session().unwrap();
    session.execute("INSERT INTO test VALUES (2, 200)").unwrap();

    let result = session.execute("INSERT INTO nonexistent VALUES (3, 300)");
    assert!(result.is_err());

    session.abort_transaction().unwrap();
    assert_eq!(db.query_count("test"), 1);
}

#[test]
fn test_session_rollback_updates() {
    let db = TestDb::new();
    setup_test_table(&db);
    db.execute_ok("INSERT INTO test VALUES (1, 100)");

    let mut session = db.session().unwrap();

    session
        .execute("UPDATE test SET value = 999 WHERE id = 1")
        .unwrap();
    session.abort_transaction().unwrap();

    assert_eq!(
        db.query_single_int("SELECT value FROM test WHERE id = 1"),
        100
    );
}

#[test]
fn test_session_rollback_deletes() {
    let db = TestDb::new();
    setup_test_table(&db);
    db.execute_ok("INSERT INTO test VALUES (1, 100)");
    db.execute_ok("INSERT INTO test VALUES (2, 200)");

    let mut session = db.session().unwrap();

    session.execute("DELETE FROM test WHERE id = 1").unwrap();
    session.abort_transaction().unwrap();

    assert_eq!(db.query_count("test"), 2);
}

// Vacuum tests
param_tests!(test_vacuum_after_updates, updates => [5, 10, 20]);

#[test]
fn test_vacuum_empty_database() {
    let db = TestDb::new();
    let stats = db.vacuum().unwrap();
    assert_eq!(stats.tables_vacuumed, 0);
}

#[test]
fn test_vacuum_preserves_data() {
    let db = TestDb::new();
    setup_test_with_data(&db, 20);
    db.execute_ok("UPDATE test SET value = value + 1 WHERE id < 10");
    db.execute_ok("DELETE FROM test WHERE id >= 15");

    db.vacuum().unwrap();

    assert_eq!(db.query_count("test"), 15);
}

#[test]
fn test_vacuum_multiple_tables() {
    let db = TestDb::new();
    setup_users_table(&db);
    setup_orders_table(&db);

    db.execute_ok("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute_ok("UPDATE users SET name = 'Alice Updated' WHERE id = 1");
    db.execute_ok("INSERT INTO orders VALUES (100, 1, 99.99)");
    db.execute_ok("UPDATE orders SET amount = 109.99 WHERE id = 100");

    let stats = db.vacuum().unwrap();
    assert!(stats.tables_vacuumed >= 2);
}

// Analyze tests
param2_tests!(
    test_analyze_with_sampling, rate, rows => [
        sr_1: 1.0, rows_10: 10;
        sr_1_2: 0.5, rows_50: 50;
        sr_0_1: 0.1, rows_100: 100;
    ]
);

#[test]
fn test_analyze_empty() {
    let db = TestDb::new();
    let result = db.analyze(1.0, 10000);
    assert!(result.is_ok());
}

// Constraint tests
param2_tests!(
    test_unique_constraint_violation, first, second => [
        ( "ABC", "ABC"),
        ("ABC", "DEF")
    ]
);

#[test]
fn test_unique_on_create_table() {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE users (id BIGINT, email TEXT, name TEXT, UNIQUE(email))");
    db.execute_ok("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice')");

    let result = db.execute("INSERT INTO users VALUES (2, 'alice@example.com', 'Bob')");
    assert!(result.is_err());

    db.execute_ok("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob')");
    assert_eq!(db.query_count("users"), 2);
}

#[test]
fn test_unique_allows_update_same_row() {
    let db = TestDb::new();
    db.execute_ok(
        "CREATE TABLE accounts (id BIGINT, username TEXT, status TEXT, UNIQUE(username))",
    );
    db.execute_ok("INSERT INTO accounts VALUES (1, 'john_doe', 'active')");
    db.execute_ok("INSERT INTO accounts VALUES (2, 'jane_doe', 'active')");

    db.execute_ok("UPDATE accounts SET username = 'john_doe' WHERE id = 1");

    let result = db.execute("UPDATE accounts SET username = 'jane_doe' WHERE id = 1");
    assert!(result.is_err());

    db.execute_ok("UPDATE accounts SET username = 'johnny' WHERE id = 1");
}

#[test]
fn test_constraint_rollback() {
    test_constraint_rollback_on_violation();
}

#[test]
fn test_unique_delete_reinsert() {
    test_unique_after_delete_reinsert();
}

#[test]
fn test_unique_index() {
    test_unique_via_create_index();
}

#[test]
fn test_not_null_constraint() {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE products (id BIGINT, name TEXT NOT NULL, price DOUBLE)");
    db.execute_ok("INSERT INTO products VALUES (1, 'Widget', 9.99)");
    assert_eq!(db.query_count("products"), 1);
}

// Complex recovery scenarios
#[test]
fn test_recovery_multiple_sessions() {
    let (dir, path) = temp_db_path();

    {
        let db = Database::create(&path, DBConfig::default()).unwrap();
        db.execute("CREATE TABLE counter (id BIGINT, value INT)")
            .unwrap();
        db.execute("INSERT INTO counter VALUES (1, 100)").unwrap();
        db.flush().unwrap();
    }

    {
        let db = Database::open(&path, DBConfig::default()).unwrap();
        db.execute("UPDATE counter SET value = 200 WHERE id = 1")
            .unwrap();
        db.execute("INSERT INTO counter VALUES (2, 300)").unwrap();
        db.flush().unwrap();
    }

    {
        let db = Database::open(&path, DBConfig::default()).unwrap();
        db.execute("DELETE FROM counter WHERE id = 2").unwrap();
        db.flush().unwrap();
    }

    {
        let db = Database::open(&path, DBConfig::default()).unwrap();
        let result = db.execute("SELECT COUNT(*) FROM counter").unwrap();
        let rows = result.into_rows().unwrap();
        let count = rows.first().unwrap()[0].as_big_int().unwrap().value();
        assert_eq!(count, 1);
    }

    drop(dir);
}

/// Integration test to validate correct execution under the following scenario:
///
/// Transaction A creates a table.
///
/// Transaction B inserts data and commits.
///
/// Transaction C inserts more data and aborts.
///
/// The DB vaccums.
///
/// Only non aborted data should be read by new transactions.
#[test]
#[cfg_attr(miri, ignore)]
fn test_rollback_reopen_vacuum() {
    use std::fs;
    use tempfile::tempdir;

    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("rollback_test.axm");
    let config = DBConfig::default();

    let initial_size: u64;
    let size_after_rollback: u64;
    let size_after_vacuum: u64;

    // Phase 1: Create database, table, insert data in transaction, then rollback
    {
        let db = Database::create(&db_path, config).expect("Failed to create database");

        db.execute("CREATE TABLE test_table (id INT, name TEXT, value INT)")
            .expect("Failed to create table");

        let mut session = db.session().expect("Failed to create session");

        for i in 0..10 {
            let sql = format!(
                "INSERT INTO test_table (id, name, value) VALUES ({}, 'item_{}', {})",
                i,
                i,
                i * 10
            );
            session.execute(&sql).expect("Failed to insert");
        }

        session.commit_transaction().expect("Failed to commit");

        let mut session = db.session().expect("Failed to create session");
        for i in 10..20 {
            let sql = format!(
                "INSERT INTO test_table (id, name, value) VALUES ({}, 'item_{}', {})",
                i,
                i,
                i * 10
            );
            session.execute(&sql).expect("Failed to insert");
        }

        session.abort_transaction().expect("Failed to abort");

        db.flush().expect("Failed to flush");

        initial_size = fs::metadata(&db_path)
            .expect("Failed to get file metadata")
            .len();

        println!("Initial size after rollback: {} bytes", initial_size);
    }

    // Phase 2: Reopen database and verify data is not visible
    {
        let db = Database::open(&db_path, config).expect("Failed to open database");

        let result = db
            .execute("SELECT COUNT(*) FROM test_table")
            .expect("Failed to execute select");

        let count: i64 = match &result {
            QueryResult::Rows(rows) => rows.first().unwrap()[0].as_big_int().unwrap().value(),
            _ => panic!("Expected Select result"),
        };

        assert_eq!(count, 10, "Aborted transaction data should not be visible");

        size_after_rollback = fs::metadata(&db_path)
            .expect("Failed to get file metadata")
            .len();

        println!("Size after reopen: {} bytes", size_after_rollback);
    }

    // Phase 3: Run vacuum and verify space is recovered
    {
        let db = Database::open(&db_path, config).expect("Failed to open database");

        let stats = db.vacuum().expect("Failed to vacuum");

        println!("Vacuum stats: {:?}", stats);
        assert!(
            stats.total_freed() > 0,
            "Vacuum should have freed some space from aborted transaction tuples"
        );

        db.flush().expect("Failed to flush after vacuum");

        size_after_vacuum = fs::metadata(&db_path)
            .expect("Failed to get file metadata")
            .len();

        println!("Size after vacuum: {} bytes", size_after_vacuum);
    }

    // Phase 4: Final verification
    {
        let db = Database::open(&db_path, config).expect("Failed to open database");

        let result = db
            .execute("SELECT COUNT(*) FROM test_table")
            .expect("Failed to execute select");

        let count: i64 = match &result {
            QueryResult::Rows(rows) => rows.first().unwrap()[0].as_big_int().unwrap().value(),
            _ => panic!("Expected Select result"),
        };

        assert_eq!(count, 10, "Data should still not be visible after vacuum");
    }

}



/// Recovery tests on failure scenarios.
///
/// This test suite intends to validate that via the wal we are able to recover the database to a usable consistent state after a disk failover.
#[test]
#[cfg_attr(miri, ignore)]
fn test_recovery_without_flush() {
    use tempfile::tempdir;

    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("ddl_no_flush.axm");
    let config = DBConfig::default();

    // Create table and insert data WITHOUT calling flush (simulating crash)
    {
        let db = Database::create(&db_path, config).expect("Failed to create database");

        db.execute("CREATE TABLE crash_test (id BIGINT, data TEXT)")
            .expect("Failed to create table");

        db.execute("INSERT INTO crash_test VALUES (1, 'before_crash')")
            .expect("Failed to insert");
    }

    // Reopen and verify recovery worked
    {
        let db = Database::open(&db_path, config).expect("Failed to open database");

        let result = db.execute("SELECT COUNT(*) FROM crash_test");
        println!("{:?}", result);
        assert!(result.is_ok(), "Transaction was committed so it must be recovered by the wal");

        // If table exists, verify data integrity
        if let Ok(result) = result {
            let rows = result.into_rows().expect("Expected rows");
            let count = rows.first().unwrap()[0].as_big_int().unwrap().value();
            assert!(count == 1, "Data should be consistent");
        }

    }

    drop(dir);
}
