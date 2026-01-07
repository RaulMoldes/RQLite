use crate::{DBConfig, Database, DatabaseResult, QueryResult};
use std::path::PathBuf;
use tempfile::TempDir;

pub struct TestDb {
    pub db: Database,
    _temp_dir: TempDir,
}

impl TestDb {
    pub fn new() -> Self {
        let (temp_dir, path) = temp_db_path();
        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");
        Self {
            db,
            _temp_dir: temp_dir,
        }
    }

    pub fn from_temp_dir(dir: TempDir, path: PathBuf) -> Self {
        let db = Database::create(&path, DBConfig::default()).expect("Failed to create database");
        Self { db, _temp_dir: dir }
    }

    pub fn with_config(config: DBConfig) -> Self {
        let (temp_dir, path) = temp_db_path();
        let db = Database::create(&path, config).expect("Failed to create database");
        Self {
            db,
            _temp_dir: temp_dir,
        }
    }

    pub fn path(&self) -> PathBuf {
        self.db.path().to_path_buf()
    }

    pub fn into_dir(self) -> TempDir {
        self._temp_dir
    }

    pub fn execute(&self, sql: &str) -> DatabaseResult<QueryResult> {
        self.db.execute(sql)
    }

    pub fn execute_ok(&self, sql: &str) -> QueryResult {
        self.db.execute(sql).expect(&format!("Failed: {}", sql))
    }

    pub fn execute_err(&self, sql: &str) -> crate::DatabaseError {
        self.db
            .execute(sql)
            .expect_err(&format!("Expected error: {}", sql))
    }

    pub fn query_count(&self, table: &str) -> i64 {
        let result = self.execute_ok(&format!("SELECT COUNT(*) FROM {}", table));
        let rows = result.into_rows().expect("Expected rows");
        rows.first().unwrap()[0].as_big_int().unwrap().value()
    }

    pub fn query_single_int(&self, sql: &str) -> i32 {
        let result = self.execute_ok(sql);
        let rows = result.into_rows().expect("Expected rows");
        rows.first().unwrap()[0].as_int().unwrap().value()
    }

    pub fn query_single_bigint(&self, sql: &str) -> i64 {
        let result = self.execute_ok(sql);
        let rows = result.into_rows().expect("Expected rows");
        rows.first().unwrap()[0].as_big_int().unwrap().value()
    }

    pub fn query_single_double(&self, sql: &str) -> f64 {
        let result = self.execute_ok(sql);
        let rows = result.into_rows().expect("Expected rows");
        rows.first().unwrap()[0].as_double().unwrap().value()
    }

    pub fn flush(&self) {
        self.db.flush().expect("Failed to flush");
    }

    pub fn sync(&self) {
        self.db.sync().expect("Failed to sync");
    }
}

impl std::ops::Deref for TestDb {
    type Target = Database;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

pub fn temp_db_path() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let path = dir.path().join("test.db");
    (dir, path)
}

pub fn reopen_db(path: PathBuf) -> Database {
    Database::open(&path, DBConfig::default()).expect("Failed to reopen database")
}

pub fn setup_users_table(db: &TestDb) {
    db.execute_ok("CREATE TABLE users (id BIGINT, name TEXT, age INT)");
}

pub fn setup_users_with_data(db: &TestDb, count: i64) {
    setup_users_table(db);
    for i in 1..=count {
        db.execute_ok(&format!(
            "INSERT INTO users VALUES ({}, 'User{}', {})",
            i,
            i,
            20 + i
        ));
    }
}

pub fn setup_orders_table(db: &TestDb) {
    db.execute_ok("CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DOUBLE)");
}

pub fn setup_items_table(db: &TestDb) {
    db.execute_ok("CREATE TABLE items (id BIGINT, value INT)");
}

pub fn setup_test_table(db: &TestDb) {
    db.execute_ok("CREATE TABLE test (id BIGINT, value INT)");
}

pub fn setup_test_with_data(db: &TestDb, count: i64) {
    setup_test_table(db);
    for i in 0..count {
        db.execute_ok(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10));
    }
}
