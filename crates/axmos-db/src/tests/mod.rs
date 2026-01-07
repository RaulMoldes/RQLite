mod tests;
mod utils;

use crate::{DBConfig, Database, DatabaseError, matrix_tests, param_tests, param2_tests};
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
