use super::utils::*;
use crate::types::{DataType, UInt64};

pub fn test_scan_rows(num_rows: u64) {
    let harness = TestHarness::new();
    harness.setup_users_table(num_rows);
    harness.assert_count("SELECT * FROM users", num_rows as usize);
}

pub fn test_filter_predicate(predicate: &str, expected: usize) {
    let harness = TestHarness::new();
    harness.setup_users_table(10);
    harness.assert_count(
        &format!("SELECT * FROM users WHERE {}", predicate),
        expected,
    );
}

pub fn test_limit(num_rows: u64, limit: usize, expected: usize) {
    let harness = TestHarness::new();
    harness.setup_users_table(num_rows);
    harness.assert_count(&format!("SELECT * FROM users LIMIT {}", limit), expected);
}

pub fn test_limit_offset(limit: usize, offset: usize, expected: usize) {
    let harness = TestHarness::new();
    harness.setup_users_table(10);
    harness.assert_count(
        &format!("SELECT * FROM users LIMIT {} OFFSET {}", limit, offset),
        expected,
    );
}

pub fn test_project_columns(num_columns: usize) {
    let harness = TestHarness::new();
    harness.setup_users_table(1);

    let columns = match num_columns {
        1 => "name",
        2 => "name, age",
        3 => "name, age, email",
        4 => "id, name, age, email",
        _ => panic!("Unsupported column count"),
    };

    let results = harness.execute_sql(&format!("SELECT {} FROM users", columns));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), num_columns);
}

pub fn test_aggregate_empty_table(agg_func: &str, expect_null: bool) {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    let results = harness.execute_sql(&format!("SELECT {}(age) FROM users", agg_func));
    assert_eq!(results.len(), 1);

    if expect_null {
        assert!(matches!(&results[0][0], DataType::Null));
    }
}

pub fn test_sort_order(asc: bool) {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let tid = harness.create_table("users", users_columns(), handle.id());
    for i in [3, 1, 4, 5, 9, 2, 6] {
        harness.insert_row_direct(
            tid,
            vec![
                DataType::BigUInt(UInt64(i as u64 * 10)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int(i.into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let order = if asc { "ASC" } else { "DESC" };
    let results = harness.execute_sql(&format!("SELECT age FROM users ORDER BY age {}", order));

    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();

    if asc {
        assert!(ages.windows(2).all(|w| w[0] <= w[1]), "Should be ascending");
    } else {
        assert!(
            ages.windows(2).all(|w| w[0] >= w[1]),
            "Should be descending"
        );
    }
}

pub fn test_insert_row_count(num_rows: usize) {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    let values: Vec<String> = (1..=num_rows)
        .map(|i| format!("('User{}', 'user{}@test.com', {})", i, i, 20 + i))
        .collect();

    let sql = format!("INSERT INTO users VALUES {}", values.join(", "));
    let results = harness.execute_sql(&sql);

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), num_rows as u64),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), num_rows as u64);
}

pub fn test_update_with_filter(predicate: &str, expected_affected: u64) {
    let harness = TestHarness::new();
    harness.setup_users_table(5);

    let sql = format!("UPDATE users SET age = 100 WHERE {}", predicate);
    let results = harness.execute_sql(&sql);

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), expected_affected),
        other => panic!("Expected BigUInt, got {:?}", other),
    }
}

pub fn test_delete_with_filter(predicate: &str, expected_affected: u64, expected_remaining: u64) {
    let harness = TestHarness::new();
    harness.setup_users_table(5);

    let sql = format!("DELETE FROM users WHERE {}", predicate);
    let results = harness.execute_sql(&sql);

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), expected_affected),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), expected_remaining);
}

pub fn test_cross_join(left_rows: u64, right_rows: u64) {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let users = harness.create_table("users", users_columns(), handle.id());
    let orders = harness.create_table("orders", orders_columns(), handle.id());

    for i in 1..=left_rows {
        harness.insert_row_direct(
            users,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int(25.into()),
            ],
            &handle,
        );
    }

    for i in 1..=right_rows {
        harness.insert_row_direct(
            orders,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::BigUInt(UInt64(1)),
                DataType::Double(100.0.into()),
                DataType::Blob("completed".into()),
            ],
            &handle,
        );
    }

    handle.commit().expect("Failed to commit");

    let expected = (left_rows * right_rows) as usize;
    harness.assert_count("SELECT * FROM users, orders", expected);
}

pub fn test_group_by_count(num_groups: usize) {
    let harness = TestHarness::new();

    let orders: Vec<(u64, u64, f64, &str)> = (1..=10)
        .map(|i| {
            let user_id = ((i - 1) % num_groups as u64) + 1;
            (i, user_id, 100.0, "completed")
        })
        .collect();

    harness.setup_orders_table(&orders);

    let results = harness.execute_sql("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id");
    assert_eq!(results.len(), num_groups);
}

pub fn test_index_range_query(start: i32, end: i32, expected: usize) {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    for i in 1..=10 {
        harness.execute_sql(&format!(
            "INSERT INTO products VALUES ('Product{}', {})",
            i,
            i * 10
        ));
    }

    harness.assert_count(
        &format!(
            "SELECT name FROM products WHERE price >= {} AND price <= {}",
            start, end
        ),
        expected,
    );
}
