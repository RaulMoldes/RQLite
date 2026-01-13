use super::utils::*;
use tempfile::TempDir;
use std::path::PathBuf;
use crate::{DBConfig, Database};

pub fn test_create_and_open(flush_before_close: bool) {
    let (dir, path) = temp_db_path();

    {
        let db = Database::create(&path, DBConfig::default()).unwrap();
        db.execute("CREATE TABLE test (id BIGINT, value INT)")
            .unwrap();
        db.execute("INSERT INTO test VALUES (1, 100)").unwrap();

        if flush_before_close {
            db.flush().unwrap();
        }
    }

    let reopened = reopen_db(path);
    let result = reopened.execute("SELECT COUNT(*) FROM test").unwrap();
    let rows = result.into_rows().unwrap();
    assert_eq!(rows.len(), 1);
}

pub fn test_recovery_insert_count(count: i64) {
    let (dir, path) = temp_db_path();

    {
        let db = Database::create(&path, DBConfig::default()).unwrap();
        db.execute("CREATE TABLE items (id BIGINT, value INT)")
            .unwrap();
        for i in 0..count {
            db.execute(&format!("INSERT INTO items VALUES ({}, {})", i, i * 10))
                .unwrap();
        }
        let result = db.execute("SELECT COUNT(*) FROM items").unwrap();
        let rows = result.into_rows().unwrap();
        let actual = rows.first().unwrap()[0].as_big_int().unwrap().value();
        assert_eq!(actual, count);
        db.flush().unwrap();
    }

    let reopened = reopen_db(path);
    let result = reopened.execute("SELECT COUNT(*) FROM items").unwrap();
    let rows = result.into_rows().unwrap();
    let actual = rows.first().unwrap()[0].as_big_int().unwrap().value();
    assert_eq!(actual, count);

    drop(dir);
}

pub fn test_recovery_with_updates(initial: i64, update_count: i64) {
    let (dir, path) = temp_db_path();
    let db = TestDb::from_temp_dir(dir, path);
    let path = db.path();
    setup_test_with_data(&db, initial);

    for i in 0..update_count {
        db.execute_ok(&format!(
            "UPDATE test SET value = {} WHERE id = {}",
            i * 100,
            i % initial
        ));
    }
    db.flush();
    let dir = db.into_dir();

    let reopened = reopen_db(path);
    let result = reopened.execute("SELECT COUNT(*) FROM test").unwrap();
    let rows = result.into_rows().unwrap();
    let actual = rows.first().unwrap()[0].as_big_int().unwrap().value();
    assert_eq!(actual, initial);
}

pub fn test_recovery_with_deletes(initial: i64, delete_count: i64) {
    let db = TestDb::new();
    let path = db.path();

    setup_test_with_data(&db, initial);

    for i in 0..delete_count {
        db.execute_ok(&format!("DELETE FROM test WHERE id = {}", i));
    }
    db.flush();
    let dir = db.into_dir();

    let reopened = reopen_db(path);
    let result = reopened.execute("SELECT COUNT(*) FROM test").unwrap();
    let rows = result.into_rows().unwrap();
    let actual = rows.first().unwrap()[0].as_big_int().unwrap().value();
    assert_eq!(actual, initial - delete_count);
}

pub fn test_batch_execution(statement_count: usize) {
    let db = TestDb::new();
    setup_test_table(&db);

    let statements: Vec<String> = (0..statement_count)
        .map(|i| format!("INSERT INTO test VALUES ({}, {})", i, i * 10))
        .collect();

    let stmt_refs: Vec<&str> = statements.iter().map(|s| s.as_str()).collect();
    db.db.execute_batch(&stmt_refs).expect("Batch failed");

    assert_eq!(db.query_count("test"), statement_count as i64);
}

pub fn test_session_transaction_commit(insert_count: i64) {
    let db = TestDb::new();
    setup_test_table(&db);

    let mut session = db.session().expect("failed to create session");

    for i in 0..insert_count {
        session
            .execute(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    session.commit_transaction().unwrap();

    assert_eq!(db.query_count("test"), insert_count);
}

pub fn test_session_transaction_rollback(insert_count: i64) {
    let db = TestDb::new();
    setup_test_table(&db);
    db.execute_ok("INSERT INTO test VALUES (999, 999)");

    let mut session = db.session().expect("failed to create session");

    for i in 0..insert_count {
        session
            .execute(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    session.abort_transaction().unwrap();

    assert_eq!(db.query_count("test"), 1);
}

pub fn test_vacuum_after_updates(update_count: i64) {
    let db = TestDb::new();
    setup_test_table(&db);
    db.execute_ok("INSERT INTO test VALUES (1, 100)");

    for i in 0..update_count {
        db.execute_ok(&format!("UPDATE test SET value = {} WHERE id = 1", 100 + i));
    }

    let stats = db.vacuum().unwrap();
    assert!(stats.tables_vacuumed >= 1);
}

pub fn test_unique_constraint_violation(first_value: &str, duplicate_value: &str) {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE codes (id BIGINT, code TEXT, UNIQUE(code))");
    db.execute_ok(&format!("INSERT INTO codes VALUES (1, '{}')", first_value));

    let result = db.execute(&format!(
        "INSERT INTO codes VALUES (2, '{}')",
        duplicate_value
    ));

    if first_value == duplicate_value {
        assert!(result.is_err(), "Duplicate should fail");
    } else {
        assert!(result.is_ok(), "Different value should succeed");
    }
}

pub fn test_multiple_tables_recovery(table_count: usize, rows_per_table: i64) {
    let db = TestDb::new();
    let path = db.path();

    for t in 0..table_count {
        db.execute_ok(&format!("CREATE TABLE table_{} (id BIGINT, value INT)", t));
        for r in 0..rows_per_table {
            db.execute_ok(&format!(
                "INSERT INTO table_{} VALUES ({}, {})",
                t,
                r,
                r * 10
            ));
        }
    }
    db.flush();
    let dir = db.into_dir();

    let reopened = reopen_db(path);
    for t in 0..table_count {
        let result = reopened
            .execute(&format!("SELECT COUNT(*) FROM table_{}", t))
            .unwrap();
        let rows = result.into_rows().unwrap();
        let actual = rows.first().unwrap()[0].as_big_int().unwrap().value();
        assert_eq!(actual, rows_per_table);
    }
}

pub fn test_analyze_with_sampling(sample_rate: f64, row_count: i64) {
    let db = TestDb::new();
    setup_items_table(&db);

    for i in 0..row_count {
        db.execute_ok(&format!("INSERT INTO items VALUES ({}, {})", i, i * 10));
    }

    let result = db.analyze(sample_rate, 10000);
    assert!(result.is_ok(), "Analyze failed: {:?}", result.err());
}

pub fn test_constraint_rollback_on_violation() {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE test (id BIGINT, code TEXT, UNIQUE(code))");
    db.execute_ok("INSERT INTO test VALUES (1, 'A')");

    let mut session = db.session().expect("failed to create session");

    session.execute("INSERT INTO test VALUES (2, 'B')").unwrap();

    let result = session.execute("INSERT INTO test VALUES (3, 'A')");
    assert!(result.is_err());

    session.abort_transaction().unwrap();

    assert_eq!(db.query_count("test"), 1);
}

pub fn test_unique_after_delete_reinsert() {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE codes (id BIGINT, code TEXT, UNIQUE(code))");
    db.execute_ok("INSERT INTO codes VALUES (1, 'ABC')");
    db.execute_ok("DELETE FROM codes WHERE id = 1");
    db.execute_ok("INSERT INTO codes VALUES (2, 'ABC')");

    assert_eq!(db.query_count("codes"), 1);
}

pub fn test_unique_via_create_index() {
    let db = TestDb::new();
    db.execute_ok("CREATE TABLE items (id BIGINT, sku TEXT, name TEXT)");
    db.execute_ok("INSERT INTO items VALUES (1, 'SKU001', 'Item 1')");
    db.execute_ok("CREATE UNIQUE INDEX idx_sku ON items (sku)");

    let result = db.execute("INSERT INTO items VALUES (2, 'SKU001', 'Item 2')");
    assert!(result.is_err());

    db.execute_ok("INSERT INTO items VALUES (2, 'SKU002', 'Item 2')");
    assert_eq!(db.query_count("items"), 2);
}


pub fn setup_db_no_flush<F>(workload: F) -> (TempDir, PathBuf)
where
    F: FnOnce(&Database),
{
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("ddl_no_flush.axm");
    let config = DBConfig::default();

    {
        let db = Database::create(&db_path, config)
            .expect("Failed to create database");

        db.execute("CREATE TABLE crash_test (id BIGINT, data TEXT)")
            .expect("Failed to create table");

        workload(&db);

    }

    (dir, db_path)
}


pub fn reopen_and_assert<F>(db_path: &PathBuf, assertions: F)
where
    F: FnOnce(&Database),
{
    let config = DBConfig::default();

    let db = Database::open(db_path, config)
        .expect("Failed to open database");

    assertions(&db);
}
