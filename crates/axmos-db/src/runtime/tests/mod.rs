//! Runtime execution tests.
use crate::{
    DBConfig,
    io::pager::{BtreeBuilder, Pager, SharedPager},
    multithreading::coordinator::{Snapshot, TransactionCoordinator, TransactionHandle},
    runtime::{RuntimeResult, builder::MutableExecutorBuilder, context::TransactionContext},
    schema::{
        base::{Column, Relation},
        catalog::{Catalog, CatalogTrait, SharedCatalog},
    },
    sql::{
        binder::binder::Binder,
        parser::Parser,
        planner::{CascadesOptimizer, PhysicalPlan},
    },
    storage::{
        page::BtreePage,
        tuple::{Row, TupleBuilder},
    },
    tree::accessor::{Accessor, BtreeWriteAccessor},
    types::{DataType, DataTypeKind, ObjectId, PageId, UInt64},
};
use tempfile::TempDir;

/// Test harness for runtime execution tests.
struct TestHarness {
    _temp_dir: TempDir,
    pager: SharedPager,
    catalog: SharedCatalog,
    coordinator: TransactionCoordinator,
}

impl TestHarness {
    /// Create a new test harness with an empty database.
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        let config = DBConfig::default();
        let pager = Pager::from_config(config, &db_path).expect("Failed to create pager");
        let pager = SharedPager::from(pager);

        let (meta_table_page, meta_index_page) = {
            let mut p = pager.write();
            let meta_table = p
                .allocate_page::<BtreePage>()
                .expect("Failed to allocate meta table page");
            let meta_index = p
                .allocate_page::<BtreePage>()
                .expect("Failed to allocate meta index page");
            (meta_table, meta_index)
        };

        let catalog = Catalog::new(meta_table_page, meta_index_page);
        let catalog = SharedCatalog::from(catalog);
        let coordinator = TransactionCoordinator::new(pager.clone());

        Self {
            _temp_dir: temp_dir,
            pager,
            catalog,
            coordinator,
        }
    }

    fn begin_transaction(&self) -> TransactionHandle {
        self.coordinator
            .begin()
            .expect("Failed to begin transaction")
    }

    fn tree_builder(&self) -> BtreeBuilder {
        let pager = self.pager.read();
        BtreeBuilder::new(pager.min_keys_per_page(), pager.num_siblings_per_side())
            .with_pager(self.pager.clone())
    }

    fn create_write_context(
        &self,
        handle: TransactionHandle,
    ) -> RuntimeResult<TransactionContext<BtreeWriteAccessor>> {
        TransactionContext::new(
            BtreeWriteAccessor::new(),
            self.pager.clone(),
            self.catalog.clone(),
            handle,
        )
    }

    fn create_table(&self, name: &str, columns: Vec<Column>, txid: u64) -> PageId {
        let object_id = self.catalog.read().get_next_object_id();
        let root_page = self
            .pager
            .write()
            .allocate_page::<BtreePage>()
            .expect("Failed to allocate table page");

        let relation = Relation::table(object_id, name, root_page, columns);
        self.catalog
            .write()
            .store_relation(relation, &self.tree_builder(), txid)
            .expect("Failed to store relation");

        root_page
    }

    fn optimize_sql(&self, sql: &str, snapshot: Snapshot) -> PhysicalPlan {
        let builder = self.tree_builder();
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Parse failed");
        let mut binder = Binder::new(self.catalog.clone(), builder.clone(), snapshot.clone());
        let bound = binder.bind(&stmt).expect("Bind failed");
        let mut optimizer =
            CascadesOptimizer::with_defaults(self.catalog.clone(), builder, snapshot);
        optimizer.optimize(&bound).expect("Optimize failed")
    }

    fn execute_plan(&self, plan: &PhysicalPlan, handle: TransactionHandle) -> Vec<Row> {
        let ctx = self
            .create_write_context(handle)
            .expect("Failed to create context");
        let builder = MutableExecutorBuilder::new(ctx.clone());
        let mut executor = builder.build(plan).expect("Failed to build executor");

        let mut results = Vec::new();
        while let Some(row) = executor.next().expect("Execution failed") {
            results.push(row);
        }
        ctx.pre_commit().expect("Failed to pre commit");
        ctx.handle().commit().expect("Failed to commit");
        ctx.end().expect("Failed to end");
        results
    }

    fn execute_sql(&self, sql: &str) -> Vec<Row> {
        let handle = self.begin_transaction();
        let snapshot = handle.snapshot();
        let plan = self.optimize_sql(sql, snapshot);
        self.execute_plan(&plan, handle)
    }

    fn execute_ddl(&self, sql: &str) -> RuntimeResult<()> {
        use crate::runtime::ddl::DdlExecutor;

        let handle = self.begin_transaction();
        let snapshot = handle.snapshot();
        let builder = self.tree_builder();

        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Parse failed");

        let mut binder = Binder::new(self.catalog.clone(), builder.clone(), snapshot.clone());
        let bound = binder.bind(&stmt).expect("Bind failed");

        let ctx = self
            .create_write_context(handle.clone())
            .expect("Failed to create context");
        let mut ddl_executor = DdlExecutor::new(ctx.clone());
        ddl_executor.execute(&bound)?;

        ctx.pre_commit().expect("Failed to pre commit");
        handle.commit().expect("Failed to commit");
        ctx.end().expect("Failed to end");

        Ok(())
    }

    fn insert_row_direct(
        &self,
        table_id: ObjectId,
        values: Vec<DataType>,
        handle: &TransactionHandle,
    ) {
        let builder = self.tree_builder();
        let snapshot = handle.snapshot();
        let tid = handle.id();

        let relation = self
            .catalog
            .read()
            .get_relation(table_id, &builder, &snapshot)
            .expect("Table not found");

        let schema = relation.schema();
        let root = relation.root();

        let row = Row::new(values.into_boxed_slice());
        let tuple_builder = TupleBuilder::from_schema(schema);
        let tuple = tuple_builder
            .build(&row, tid)
            .expect("Failed to build tuple");

        let mut tree = builder.build_tree_mut(root);
        tree.insert(root, tuple, schema)
            .expect("Failed to insert tuple");
        tree.accessor_mut().expect("Uninitialized tree").clear();
    }
}

impl TestHarness {
    /// Execute SQL and assert result count
    fn assert_count(&self, sql: &str, expected: usize) {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), expected, "SQL: {}", sql);
    }

    /// Execute COUNT(*) query and return the count
    fn query_count(&self, table: &str) -> u64 {
        let results = self.execute_sql(&format!("SELECT COUNT(*) FROM {}", table));
        assert_eq!(results.len(), 1, "Expected single row for COUNT(*)");
        match &results[0][0] {
            DataType::BigUInt(v) => v.value(),
            DataType::BigInt(v) => v.value() as u64,
            other => panic!("Expected numeric result, got {:?}", other),
        }
    }

    /// Assert a single numeric result equals expected value
    fn assert_single_int(&self, sql: &str, expected: i32) {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), 1);
        match &results[0][0] {
            DataType::Int(v) => assert_eq!(v.value(), expected, "SQL: {}", sql),
            other => panic!("Expected Int, got {:?}", other),
        }
    }

    /// Assert a single double result equals expected value (with tolerance)
    fn assert_single_double(&self, sql: &str, expected: f64, tolerance: f64) {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), 1);
        match &results[0][0] {
            DataType::Double(v) => {
                assert!(
                    (v.value() - expected).abs() < tolerance,
                    "SQL: {}, expected: {}, got: {}",
                    sql,
                    expected,
                    v.value()
                );
            }
            other => panic!("Expected Double, got {:?}", other),
        }
    }

    /// Create users table with standard columns and optionally populate with data
    fn setup_users_table(&self, num_rows: u64) {
        let handle = self.begin_transaction();
        self.create_table("users", users_columns(), handle.id());

        for i in 1..=num_rows {
            self.insert_row_direct(
                1,
                vec![
                    DataType::BigUInt(UInt64(i)),
                    DataType::Blob(format!("User{}", i).into()),
                    DataType::Blob(format!("user{}@test.com", i).into()),
                    DataType::Int((20 + i as i32).into()),
                ],
                &handle,
            );
        }
        handle.commit().expect("Failed to commit");
    }

    /// Create orders table with standard columns and optionally populate with data
    fn setup_orders_table(&self, orders: &[(u64, u64, f64, &str)]) {
        let handle = self.begin_transaction();
        self.create_table("orders", orders_columns(), handle.id());

        for &(id, user_id, amount, status) in orders {
            self.insert_row_direct(
                2,
                vec![
                    DataType::BigUInt(UInt64(id)),
                    DataType::BigUInt(UInt64(user_id)),
                    DataType::Double(amount.into()),
                    DataType::Blob(status.into()),
                ],
                &handle,
            );
        }
        handle.commit().expect("Failed to commit");
    }
}

/// Standard users table columns
fn users_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Blob, "email"),
        Column::new_with_defaults(DataTypeKind::Int, "age"),
    ]
}

/// Standard orders table columns
fn orders_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::BigUInt, "user_id"),
        Column::new_with_defaults(DataTypeKind::Double, "amount"),
        Column::new_with_defaults(DataTypeKind::Blob, "status"),
    ]
}

/// Generate a test that sets up users table, executes SQL, and asserts row count
macro_rules! sql_count_test {
    ($name:ident, rows: $rows:expr, sql: $sql:expr, expected: $expected:expr) => {
        #[test]
        fn $name() {
            let harness = TestHarness::new();
            harness.setup_users_table($rows);
            harness.assert_count($sql, $expected);
        }
    };
}

/// Generate multiple tests for filtering with different predicates
macro_rules! filter_tests {
    ($($name:ident: $predicate:expr => $expected:expr),+ $(,)?) => {
        $(
            sql_count_test!($name, rows: 10,
                sql: concat!("SELECT * FROM users WHERE ", $predicate),
                expected: $expected
            );
        )+
    };
}

#[test]
fn test_empty_table_scan() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);
    harness.assert_count("SELECT * FROM users", 0);
}

sql_count_test!(test_single_row_scan, rows: 1, sql: "SELECT * FROM users", expected: 1);
sql_count_test!(test_multiple_rows_scan, rows: 5, sql: "SELECT * FROM users", expected: 5);

filter_tests! {
    test_filter_equality: "id = 3" => 1,
    test_filter_greater_than: "age > 25" => 5,
    test_filter_no_match: "age > 100" => 0,
    test_filter_and: "age > 23 AND age < 28" => 4,
    test_filter_less_than: "age < 25" => 4,
    test_filter_gte: "age >= 25" => 6,
    test_filter_lte: "age <= 25" => 5,
}

#[test]
fn test_project_single_column() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);
    let results = harness.execute_sql("SELECT name FROM users");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 1);
}

#[test]
fn test_project_multiple_columns() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);
    let results = harness.execute_sql("SELECT name, age FROM users");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 2);
}

#[test]
fn test_project_with_expression() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);
    harness.assert_single_int("SELECT age + 10 FROM users", 31); // 21 + 10
}

sql_count_test!(test_limit_basic, rows: 10, sql: "SELECT * FROM users LIMIT 3", expected: 3);
sql_count_test!(test_limit_larger_than_table, rows: 3, sql: "SELECT * FROM users LIMIT 100", expected: 3);
sql_count_test!(test_limit_zero, rows: 1, sql: "SELECT * FROM users LIMIT 0", expected: 0);
sql_count_test!(test_limit_with_offset, rows: 10, sql: "SELECT * FROM users LIMIT 3 OFFSET 5", expected: 3);

#[test]
fn test_sort_ascending() {
    let harness = TestHarness::new();

    // Insert in random order
    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    for i in [3, 1, 4, 5, 9, 2, 6] {
        harness.insert_row_direct(
            1,
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

    let results = harness.execute_sql("SELECT age FROM users ORDER BY age ASC");
    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();

    assert!(
        ages.windows(2).all(|w| w[0] <= w[1]),
        "Ages should be sorted ascending"
    );
}

#[test]
fn test_sort_descending() {
    let harness = TestHarness::new();
    harness.setup_users_table(5);

    let results = harness.execute_sql("SELECT age FROM users ORDER BY age DESC");
    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();

    assert!(
        ages.windows(2).all(|w| w[0] >= w[1]),
        "Ages should be sorted descending"
    );
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
fn test_count_empty_table() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);
    assert_eq!(harness.query_count("users"), 0);
}

#[test]
fn test_sum_aggregate() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    for i in 1..=5 {
        harness.insert_row_direct(
            1,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32 * 10).into()), // ages: 10, 20, 30, 40, 50
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

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    for i in 1..=3 {
        harness.insert_row_direct(
            1,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32 * 10).into()), // ages: 10, 20, 30
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

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    for i in [30, 10, 50, 20, 40] {
        harness.insert_row_direct(
            1,
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

// ============================================================================
// Group By Tests
// ============================================================================

#[test]
fn test_group_by() {
    let harness = TestHarness::new();
    harness.setup_orders_table(&[
        (1, 1, 100.0, "completed"),
        (2, 1, 200.0, "completed"),
        (3, 2, 150.0, "pending"),
        (4, 2, 50.0, "completed"),
        (5, 3, 300.0, "pending"),
    ]);

    let results = harness.execute_sql("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id");
    assert_eq!(results.len(), 3); // 3 distinct user_ids
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
    assert_eq!(results.len(), 2); // 2 distinct user_ids
}

#[test]
fn test_inner_join() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    harness.create_table("orders", orders_columns(), handle.id());

    harness.insert_row_direct(
        1,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::Blob("Alice".into()),
            DataType::Blob("alice@test.com".into()),
            DataType::Int(25.into()),
        ],
        &handle,
    );

    harness.insert_row_direct(
        1,
        vec![
            DataType::BigUInt(UInt64(2)),
            DataType::Blob("Bob".into()),
            DataType::Blob("bob@test.com".into()),
            DataType::Int(30.into()),
        ],
        &handle,
    );

    harness.insert_row_direct(
        2,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::BigUInt(UInt64(1)), // user_id = 1
            DataType::Double(100.0.into()),
            DataType::Blob("completed".into()),
        ],
        &handle,
    );

    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert_eq!(results.len(), 1); // Only Alice has an order
}

#[test]
fn test_cross_join() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    harness.create_table("orders", orders_columns(), handle.id());

    for i in 1..=2 {
        harness.insert_row_direct(
            1,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int(25.into()),
            ],
            &handle,
        );
    }

    for i in 1..=3 {
        harness.insert_row_direct(
            2,
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

    harness.assert_count("SELECT * FROM users, orders", 6); // 2 * 3 = 6
}

#[test]
fn test_insert_single_row() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    let results = harness.execute_sql("INSERT INTO users VALUES ('Alice', 'alice@test.com', 25)");
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), 1);
}

#[test]
fn test_insert_multiple_statements() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    harness.execute_sql("INSERT INTO users VALUES ('Alice', 'alice@test.com', 25)");
    harness.execute_sql("INSERT INTO users VALUES ('Bob', 'bob@test.com', 30)");
    harness.execute_sql("INSERT INTO users VALUES ('Charlie', 'charlie@test.com', 35)");

    assert_eq!(harness.query_count("users"), 3);
}

#[test]
fn test_insert_multiple_rows_single_statement() {
    let harness = TestHarness::new();
    harness.setup_users_table(0);

    let results = harness.execute_sql(
        "INSERT INTO users VALUES
            ('Alice', 'alice@test.com', 25),
            ('Bob', 'bob@test.com', 30),
            ('Charlie', 'charlie@test.com', 35)",
    );

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 3),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), 3);
}

#[test]
fn test_insert_select() {
    let harness = TestHarness::new();
    harness.setup_users_table(3);

    // Create second table
    let handle = harness.begin_transaction();
    harness.create_table("users2", users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    harness.execute_sql("INSERT INTO users2 SELECT * FROM users WHERE age > 21");
    assert_eq!(harness.query_count("users2"), 2);
}

#[test]
fn test_update_single_row() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);

    let results = harness.execute_sql("UPDATE users SET age = 30 WHERE id = 1");
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    harness.assert_single_int("SELECT age FROM users WHERE id = 1", 30);
}

#[test]
fn test_update_multiple_rows() {
    let harness = TestHarness::new();
    harness.setup_users_table(5);

    let results = harness.execute_sql("UPDATE users SET age = 100 WHERE age > 23");
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 2), // ages 24, 25
        other => panic!("Expected BigUInt, got {:?}", other),
    }
}

#[test]
fn test_update_with_expression() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);

    harness.execute_sql("UPDATE users SET age = age + 5");
    harness.assert_single_int("SELECT age FROM users", 26); // 21 + 5
}

#[test]
fn test_delete_single_row() {
    let harness = TestHarness::new();
    harness.setup_users_table(3);

    let results = harness.execute_sql("DELETE FROM users WHERE id = 2");
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), 2);
}

#[test]
fn test_delete_multiple_rows() {
    let harness = TestHarness::new();
    harness.setup_users_table(5);

    let results = harness.execute_sql("DELETE FROM users WHERE age > 23");
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 2),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    assert_eq!(harness.query_count("users"), 3);
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

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());
    harness.create_table("orders", orders_columns(), handle.id());

    for i in 1..=5 {
        harness.insert_row_direct(
            1,
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
            2,
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
fn test_transaction_isolation_read_committed() {
    let harness = TestHarness::new();
    harness.setup_users_table(1);

    let handle2 = harness.begin_transaction();
    let snapshot2 = handle2.snapshot();
    let plan = harness.optimize_sql("SELECT COUNT(*) FROM users", snapshot2);
    let results = harness.execute_plan(&plan, handle2);

    match &results[0][0] {
        DataType::BigInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigInt, got {:?}", other),
    }
}

#[test]
fn test_null_handling_in_aggregates() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", users_columns(), handle.id());

    harness.insert_row_direct(
        1,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::Blob("Alice".into()),
            DataType::Blob("alice@test.com".into()),
            DataType::Null,
        ],
        &handle,
    );

    harness.insert_row_direct(
        1,
        vec![
            DataType::BigUInt(UInt64(2)),
            DataType::Blob("Bob".into()),
            DataType::Blob("bob@test.com".into()),
            DataType::Int(30.into()),
        ],
        &handle,
    );
    handle.commit().expect("Failed to commit");

    assert_eq!(harness.query_count("users"), 2);
    harness.assert_single_double("SELECT SUM(age) FROM users", 30.0, 0.001);
}

#[test]
fn test_create_index_via_sql() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    // Verify index exists
    let handle = harness.begin_transaction();
    let snapshot = handle.snapshot();
    let builder = harness.tree_builder();

    let index =
        harness
            .catalog
            .read()
            .get_relation_by_name("idx_products_price", &builder, &snapshot);
    assert!(index.is_ok(), "Index should exist in catalog");
    handle.commit().expect("Failed to commit");
}

#[test]
fn test_index_maintained_on_insert() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("INSERT INTO products VALUES ('Gadget', 200)");
    harness.execute_sql("INSERT INTO products VALUES ('Gizmo', 150)");

    harness.assert_count("SELECT name FROM products WHERE price = 150", 1);
}

#[test]
fn test_index_range_query() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    for i in 1..=10 {
        harness.execute_sql(&format!(
            "INSERT INTO products VALUES ('Product{}', {})",
            i,
            i * 10
        ));
    }

    // Range query: prices 50, 60, 70, 80
    harness.assert_count(
        "SELECT name FROM products WHERE price >= 50 AND price <= 80",
        4,
    );
}

#[test]
fn test_index_maintained_on_delete() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("INSERT INTO products VALUES ('Gadget', 200)");

    // Indexes are unique by default, so duplicate index keys are not allowed.
    harness.execute_sql("INSERT INTO products VALUES ('Gizmo', 150)");

    harness.execute_sql("DELETE FROM products WHERE name = 'Widget'");

    let results = harness.execute_sql("SELECT name FROM products WHERE price = 150");
    assert_eq!(results.len(), 1);

    match &results[0][0] {
        DataType::Blob(b) => assert_eq!(b.to_string(), "Blob (Gizmo)"),
        other => panic!("Expected Blob, got {:?}", other),
    }
}

#[test]
fn test_index_maintained_on_update() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    harness.execute_sql("INSERT INTO products VALUES ('Widget', 100)");
    harness.execute_sql("UPDATE products SET price = 150 WHERE name = 'Widget'");

    harness.assert_count("SELECT name FROM products WHERE price = 100", 0);
    harness.assert_count("SELECT name FROM products WHERE price = 150", 1);
}

#[test]
fn test_multiple_indexes_on_table() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "employees",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Blob, "department"),
            Column::new_with_defaults(DataTypeKind::Int, "salary"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_emp_dept ON employees(department)")
        .expect("Failed to create department index");
    harness
        .execute_ddl("CREATE INDEX idx_emp_salary ON employees(salary)")
        .expect("Failed to create salary index");

    harness.execute_sql("INSERT INTO employees VALUES ('Alice', 'Engineering', 100000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Bob', 'Sales', 80000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Charlie', 'Product', 90000)");
    harness.execute_sql("INSERT INTO employees VALUES ('Diana', 'Tools', 85000)");

    // Indexes must be unique
    harness.assert_count(
        "SELECT name FROM employees WHERE department = 'Engineering'",
        1,
    );
    harness.assert_count("SELECT name FROM employees WHERE salary > 85000", 2);
}

#[test]
fn test_create_index_if_not_exists() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table(
        "products",
        vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Int, "price"),
        ],
        handle.id(),
    );
    handle.commit().expect("Failed to commit");

    harness
        .execute_ddl("CREATE INDEX idx_products_price ON products(price)")
        .expect("Failed to create index");

    // Should not error with IF NOT EXISTS
    let result =
        harness.execute_ddl("CREATE INDEX IF NOT EXISTS idx_products_price ON products(price)");
    assert!(result.is_ok());
}
