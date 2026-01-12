mod tests;
mod utils;

use crate::types::{DataType, UInt64};
use crate::{matrix_tests, param_tests, param2_tests, param3_tests};
use tests::*;
use utils::*;

// Scan tests
param_tests!(test_scan_rows, rows => [0, 1, 5, 10, 100]);

param2_tests!(
    test_filter_predicate, predicate, expected => [
        id_eq_3: "id = 3", expected_1: 1;
        age_gt_25: "age > 25", expected_5: 5;
        age_gt_100: "age > 100", expected_0: 0;
        age_gt_23_lt_28: "age > 23 AND age < 28", expected_4: 4;
        age_lt_25: "age < 25", expected_4: 4;
        age_gte_25: "age >= 25", expected_6: 6;
        age_lte_25: "age <= 25", expected_5: 5
    ]
);

// Limit tests
param3_tests!(
    test_limit, rows, limit, expected => [
        (10, 3, 3),
        (3, 100, 3),
        (5, 0, 0),
        (10, 5, 5)
    ]
);

param3_tests!(
    test_limit_offset, limit, offset, expected => [
        (3, 5, 3),
        (5, 8, 2),
        (10, 0, 10)
    ]
);

// Projection tests
param_tests!(test_project_columns, cols => [1, 2, 3, 4]);

// Sort tests
param_tests!(test_sort_order, asc => [true, false]);

// Insert tests
param_tests!(test_insert_row_count, rows => [1, 3, 5, 10]);

// Update tests
param2_tests!(
    test_update_with_filter, predicate, affected => [
        age_gt_23: "age > 23", affected_2: 2;
        age_gt_100: "age > 100", affected_0:  0;
        id_eq_1: "id = 1", affected_1: 1
    ]
);

// Delete tests
param3_tests!(
    test_delete_with_filter, predicate, affected, remaining => [
        id_eq_2: "id = 2", affected_1: 1,  remaining_4: 4;
        age_gt_23: "age > 23", affected_2:  2, remaining_3: 3;
        age_gt_100: "age > 100", affected_0: 0, remaining_5: 5;
    ]
);

// Cross join tests
matrix_tests!(
    test_cross_join,
    left => [1, 2, 3],
    right => [1, 2, 3]
);

// Group by tests
param_tests!(test_group_by_count, groups => [1, 2, 3, 5]);

// Index range query tests
param3_tests!(
    test_index_range_query, start, end, expected => [
        (50, 80, 4),
        (10, 30, 3),
        (100, 200, 1)
    ]
);

// Non-parameterized tests
#[test]
fn test_project_with_expression() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);
    harness.assert_single_int("SELECT age + 10 FROM users", 31);
}

#[test]
fn test_sort_with_limit() {
    let harness = TestHarness::new();
    harness.setup_users_table(10);

    let results = harness.execute_sql("SELECT age FROM users ORDER BY age DESC LIMIT 3");
    assert_eq!(results.len(), 3);

    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();
    assert_eq!(ages, vec![30, 29, 28]);
}

#[test]
fn test_count_star() {
    let harness = TestHarness::new();
    harness.setup_users_table(5);
    assert_eq!(harness.query_count("users"), 5);
}

#[test]
fn test_sum_aggregate() {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let tid = harness.create_table("users", users_columns(), handle.id());
    for i in 1..=5 {
        harness.insert_row_direct(
            tid,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32 * 10).into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    harness.assert_single_double("SELECT SUM(age) FROM users", 150.0, 0.001);
}

#[test]
fn test_avg_aggregate() {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let tid = harness.create_table("users", users_columns(), handle.id());
    for i in 1..=3 {
        harness.insert_row_direct(
            tid,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32 * 10).into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    harness.assert_single_double("SELECT AVG(age) FROM users", 20.0, 0.001);
}

#[test]
fn test_min_max_aggregate() {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let tid = harness.create_table("users", users_columns(), handle.id());
    for i in [30, 10, 50, 20, 40] {
        harness.insert_row_direct(
            tid,
            vec![
                DataType::BigUInt(UInt64(i as u64)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int(i.into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT MIN(age), MAX(age) FROM users");
    assert_eq!(results.len(), 1);

    match (&results[0][0], &results[0][1]) {
        (DataType::Int(min), DataType::Int(max)) => {
            assert_eq!(min.value(), 10);
            assert_eq!(max.value(), 50);
        }
        _ => panic!("Expected Int values"),
    }
}

#[test]
fn test_empty_result_aggregation() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    let results = harness.execute_sql("SELECT SUM(age) FROM users");
    assert!(matches!(&results[0][0], DataType::Null));
}

#[test]
fn test_group_by_with_sum() {
    let harness = TestHarness::new();
    harness.setup_orders_table(&[
        (1, 1, 100.0, "completed"),
        (2, 1, 200.0, "completed"),
        (3, 2, 150.0, "completed"),
        (4, 2, 50.0, "completed"),
    ]);

    let results = harness.execute_sql("SELECT user_id, SUM(amount) FROM orders GROUP BY user_id");
    assert_eq!(results.len(), 2);
}

#[test]
fn test_inner_join() {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let users = harness.create_table("users", users_columns(), handle.id());
    let orders = harness.create_table("orders", orders_columns(), handle.id());

    harness.insert_row_direct(
        users,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::Blob("Alice".into()),
            DataType::Blob("alice@test.com".into()),
            DataType::Int(25.into()),
        ],
        &handle,
    );

    harness.insert_row_direct(
        users,
        vec![
            DataType::BigUInt(UInt64(2)),
            DataType::Blob("Bob".into()),
            DataType::Blob("bob@test.com".into()),
            DataType::Int(30.into()),
        ],
        &handle,
    );

    harness.insert_row_direct(
        orders,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::BigUInt(UInt64(1)),
            DataType::Double(100.0.into()),
            DataType::Blob("completed".into()),
        ],
        &handle,
    );

    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert_eq!(results.len(), 1);
}

#[test]
fn test_insert_select() {
    let harness = TestHarness::new();
    harness.setup_users_table(3);

    let (mut handle, last_lsn) = harness.begin_transaction();
    harness.create_table("users2", users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    harness.execute_sql("INSERT INTO users2 SELECT * FROM users WHERE age > 21");
    assert_eq!(harness.query_count("users2"), 2);
}

#[test]
fn test_update_with_expression() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);

    harness.execute_sql("UPDATE users SET age = age + 5");
    harness.assert_single_int("SELECT age FROM users", 26);
}

#[test]
fn test_delete_all_rows() {
    let harness = TestHarness::new();
    harness.setup_users_table(3);

    harness.execute_sql("DELETE FROM users");
    assert_eq!(harness.query_count("users"), 0);
}

#[test]
fn test_complex_query_with_join_filter_sort_limit() {
    let harness = TestHarness::new();

    let (mut handle, last_lsn) = harness.begin_transaction();
    let users = harness.create_table("users", users_columns(), handle.id());
    let orders = harness.create_table("orders", orders_columns(), handle.id());

    for i in 1..=5 {
        harness.insert_row_direct(
            users,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((20 + i as i32).into()),
            ],
            &handle,
        );
    }

    for i in 1..=10 {
        harness.insert_row_direct(
            orders,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::BigUInt(UInt64((i % 5) + 1)),
                DataType::Double((i as f64 * 10.0).into()),
                DataType::Blob("completed".into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql(
        "SELECT users.name, orders.amount
         FROM users
         JOIN orders ON users.id = orders.user_id
         WHERE users.age > 22
         ORDER BY orders.amount DESC
         LIMIT 5",
    );

    assert_eq!(results.len(), 5);
}

#[test]
fn test_create_index_via_sql() {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    let (mut handle, last_lsn) = harness.begin_transaction();
    let snapshot = handle.snapshot();
    let builder = harness.tree_builder();

    let index =
        harness
            .catalog()
            .get_relation_by_name("idx_products_price", &builder, &snapshot);
    assert!(index.is_ok(), "Index should exist in catalog");
    handle.commit().expect("Failed to commit");
}

#[test]
fn test_index_maintained_on_insert() {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("INSERT INTO products VALUES ('Gadget', 200)");
    harness.execute_sql("INSERT INTO products VALUES ('Gizmo', 150)");

    harness.assert_count("SELECT name FROM products WHERE price = 150", 1);
}

#[test]
fn test_index_maintained_on_delete() {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("INSERT INTO products VALUES ('Gadget', 200)");
    harness.execute_sql("INSERT INTO products VALUES ('Gizmo', 150)");

    harness.execute_sql("DELETE FROM products WHERE name = 'Widget'");

    let results = harness.execute_sql("SELECT name FROM products WHERE price = 150");
    assert_eq!(results.len(), 1);

    match &results[0][0] {
        DataType::Blob(b) => assert_eq!(b.to_string(), "Gizmo"),
        other => panic!("Expected Blob, got {:?}", other),
    }
}

#[test]
fn test_index_maintained_on_update() {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("UPDATE products SET price = 150 WHERE name = 'Widget'");

    harness.assert_count("SELECT name FROM products WHERE price = 100", 0);
    harness.assert_count("SELECT name FROM products WHERE price = 150", 1);
}

#[test]
fn test_multiple_indexes_on_table() {
    let harness = TestHarness::new();
    harness.setup_employees_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_emp_dept ON employees(department)")
        .expect("Failed to create department index");
    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_emp_salary ON employees(salary)")
        .expect("Failed to create salary index");

    harness.execute_sql("INSERT INTO employees VALUES ('Alice', 'Engineering', 100000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Bob', 'Sales', 80000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Charlie', 'Product', 90000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Diana', 'Tools', 85000)");

    harness.assert_count(
        "SELECT name FROM employees WHERE department = 'Engineering'",
        1,
    );
    harness.assert_count("SELECT name FROM employees WHERE salary > 85000", 2);
}

#[test]
fn test_create_index_if_not_exists() {
    let harness = TestHarness::new();
    harness.setup_products_table();

    harness
        .execute_ddl("CREATE UNIQUE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    let result = harness
        .execute_ddl("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_price ON products(price)");
    assert!(result.is_ok());
}
