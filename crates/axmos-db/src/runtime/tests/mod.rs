//! Runtime execution tests.
//!
//! Integration tests for the query execution pipeline.
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

        // Allocate meta table and meta index pages
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

    /// Begin a new transaction.
    fn begin_transaction(&self) -> TransactionHandle {
        self.coordinator
            .begin()
            .expect("Failed to begin transaction")
    }

    /// Create a BtreeBuilder.
    fn tree_builder(&self) -> BtreeBuilder {
        let pager = self.pager.read();
        BtreeBuilder::new(pager.min_keys_per_page(), pager.num_siblings_per_side())
            .with_pager(self.pager.clone())
    }

    /// Create a transaction context with write accessor.
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

    /// Create and store a table in the catalog.
    fn create_table(
        &self,
        name: &str,
        object_id: ObjectId,
        columns: Vec<Column>,
        txid: u64,
    ) -> PageId {
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

    /// Parse, bind, and optimize SQL.
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

    /// Execute a plan and collect all rows.
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

        ctx.commit().expect("Failed to commit");
        results
    }

    /// Execute SQL and return results.
    fn execute_sql(&self, sql: &str) -> Vec<Row> {
        let handle = self.begin_transaction();
        let snapshot = handle.snapshot();
        let plan = self.optimize_sql(sql, snapshot);
        self.execute_plan(&plan, handle)
    }

    /// Execute SQL expecting a single count result.
    fn execute_sql_count(&self, sql: &str) -> u64 {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), 1, "Expected single row result");

        match &results[0][0] {
            DataType::BigUInt(v) => v.value(),
            DataType::BigInt(v) => v.value() as u64,
            other => panic!("Expected numeric result, got {:?}", other),
        }
    }

    /// Insert a row directly into a table (bypassing SQL).
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
            .build(row, tid)
            .expect("Failed to build tuple");

        let mut tree = builder.build_tree_mut(root);
        tree.insert(root, tuple, schema)
            .expect("Failed to insert tuple");
        tree.accessor_mut().expect("Uninitialized tree").clear();
    }
}

/// Helper to create standard test columns for users table.
fn users_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Blob, "email"),
        Column::new_with_defaults(DataTypeKind::Int, "age"),
    ]
}

/// Helper to create standard test columns for orders table.
fn orders_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::BigUInt, "user_id"),
        Column::new_with_defaults(DataTypeKind::Double, "amount"),
        Column::new_with_defaults(DataTypeKind::Blob, "status"),
    ]
}

#[test]
fn test_empty_table_scan() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users");
    assert!(results.is_empty());
}

#[test]
fn test_single_row_scan() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users");
    assert_eq!(results.len(), 1);
}

#[test]
fn test_multiple_rows_scan() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users");
    assert_eq!(results.len(), 5);
}

#[test]
fn test_filter_equality() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users WHERE id = 3");
    assert_eq!(results.len(), 1);
}

#[test]
fn test_filter_comparison() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=10 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users WHERE age > 25");
    assert_eq!(results.len(), 5); // ages 26-30
}

#[test]
fn test_filter_no_match() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users WHERE age > 100");
    assert!(results.is_empty());
}

#[test]
fn test_filter_and_condition() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=10 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users WHERE age > 23 AND age < 28");
    assert_eq!(results.len(), 4); // ages 24, 25, 26, 27
}

#[test]
fn test_project_single_column() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT name FROM users");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 1);
}

#[test]
fn test_project_multiple_columns() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT name, age FROM users");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 2);
}

#[test]
fn test_project_with_expression() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT age + 10 FROM users");
    assert_eq!(results.len(), 1);

    match &results[0][0] {
        DataType::Int(v) => assert_eq!(v.value(), 35),
        other => panic!("Expected Int, got {:?}", other),
    }
}

#[test]
fn test_limit_basic() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=10 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users LIMIT 3");
    assert_eq!(results.len(), 3);
}

#[test]
fn test_limit_larger_than_table() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=3 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users LIMIT 100");
    assert_eq!(results.len(), 3);
}

#[test]
fn test_limit_zero() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users LIMIT 0");
    assert!(results.is_empty());
}

#[test]
fn test_limit_with_offset() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=10 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT * FROM users LIMIT 3 OFFSET 5");
    assert_eq!(results.len(), 3);
}

#[test]
fn test_sort_ascending() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
            _ => panic!("Expected Int, found {}", r[0]),
        })
        .collect();

    let mut sorted = ages.clone();
    sorted.sort();
    assert_eq!(ages, sorted);
}

#[test]
fn test_sort_descending() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=5 {
        harness.insert_row_direct(
            1,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32).into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT age FROM users ORDER BY age DESC");

    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();

    assert_eq!(ages, vec![5, 4, 3, 2, 1]);
}

#[test]
fn test_sort_with_limit() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=10 {
        harness.insert_row_direct(
            1,
            vec![
                DataType::BigUInt(UInt64(i)),
                DataType::Blob(format!("User{}", i).into()),
                DataType::Blob(format!("user{}@test.com", i).into()),
                DataType::Int((i as i32).into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT age FROM users ORDER BY age DESC LIMIT 3");
    assert_eq!(results.len(), 3);

    let ages: Vec<i32> = results
        .iter()
        .map(|r| match &r[0] {
            DataType::Int(v) => v.value(),
            _ => panic!("Expected Int"),
        })
        .collect();

    assert_eq!(ages, vec![10, 9, 8]);
}

#[test]
fn test_count_star() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 5);
}

#[test]
fn test_count_empty_table() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 0);
}

#[test]
fn test_sum_aggregate() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=5 {
        harness.insert_row_direct(
            1,
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

    let results = harness.execute_sql("SELECT SUM(age) FROM users");
    assert_eq!(results.len(), 1);

    match &results[0][0] {
        DataType::Double(v) => assert_eq!(v.value(), 150.0),
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_avg_aggregate() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=3 {
        harness.insert_row_direct(
            1,
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

    let results = harness.execute_sql("SELECT AVG(age) FROM users");
    assert_eq!(results.len(), 1);

    match &results[0][0] {
        DataType::Double(v) => assert!((v.value() - 20.0).abs() < 0.001),
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_min_max_aggregate() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    assert_eq!(results[0].len(), 2);

    match (&results[0][0], &results[0][1]) {
        (DataType::Int(min), DataType::Int(max)) => {
            assert_eq!(min.value(), 10);
            assert_eq!(max.value(), 50);
        }
        _ => panic!("Expected Int values"),
    }
}

#[test]
fn test_group_by() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("orders", 2, orders_columns(), handle.id());

    let orders = [
        (1, 1, 100.0, "completed"),
        (2, 1, 200.0, "completed"),
        (3, 2, 150.0, "pending"),
        (4, 2, 50.0, "completed"),
        (5, 3, 300.0, "pending"),
    ];

    for (id, user_id, amount, status) in orders {
        harness.insert_row_direct(
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

    let results = harness.execute_sql("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id");
    assert_eq!(results.len(), 3);
}

#[test]
fn test_group_by_with_sum() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("orders", 2, orders_columns(), handle.id());

    let orders = [(1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0), (4, 2, 50.0)];

    for (id, user_id, amount) in orders {
        harness.insert_row_direct(
            2,
            vec![
                DataType::BigUInt(UInt64(id)),
                DataType::BigUInt(UInt64(user_id)),
                DataType::Double(amount.into()),
                DataType::Blob("completed".into()),
            ],
            &handle,
        );
    }
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("SELECT user_id, SUM(amount) FROM orders GROUP BY user_id");
    assert_eq!(results.len(), 2);
}

#[test]
fn test_inner_join() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    harness.create_table("orders", 2, orders_columns(), handle.id());

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
fn test_cross_join() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    harness.create_table("orders", 2, orders_columns(), handle.id());

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

    let results = harness.execute_sql("SELECT * FROM users, orders");
    assert_eq!(results.len(), 6);
}

#[test]
fn test_insert_single_row() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    let results =
        harness.execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 25)");

    assert_eq!(results.len(), 1);
    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 1);
}

#[test]
fn test_insert_multiple_rows() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    harness.execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 25)");
    harness.execute_sql("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com', 30)");
    harness.execute_sql("INSERT INTO users VALUES (3, 'Charlie', 'charlie@test.com', 35)");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 3);
}

#[test]
fn test_insert_select() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    harness.create_table("users2", 2, users_columns(), handle.id());

    for i in 1..=3 {
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
    handle.commit().expect("Failed to commit");

    harness.execute_sql("INSERT INTO users2 SELECT * FROM users WHERE age > 21");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users2");
    assert_eq!(count, 2);
}

#[test]
fn test_update_single_row() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("UPDATE users SET age = 30 WHERE id = 1");

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    let rows = harness.execute_sql("SELECT age FROM users WHERE id = 1");
    match &rows[0][0] {
        DataType::Int(v) => assert_eq!(v.value(), 30),
        other => panic!("Expected Int, got {:?}", other),
    }
}

#[test]
fn test_update_multiple_rows() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("UPDATE users SET age = 100 WHERE age > 23");

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 2),
        other => panic!("Expected BigUInt, got {:?}", other),
    }
}

#[test]
fn test_update_with_expression() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    harness.execute_sql("UPDATE users SET age = age + 5");

    let rows = harness.execute_sql("SELECT age FROM users");
    match &rows[0][0] {
        DataType::Int(v) => assert_eq!(v.value(), 30),
        other => panic!("Expected Int, got {:?}", other),
    }
}

#[test]
fn test_delete_single_row() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=3 {
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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("DELETE FROM users WHERE id = 2");

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 1),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 2);
}

#[test]
fn test_delete_multiple_rows() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

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
    handle.commit().expect("Failed to commit");

    let results = harness.execute_sql("DELETE FROM users WHERE age > 23");

    match &results[0][0] {
        DataType::BigUInt(v) => assert_eq!(v.value(), 2),
        other => panic!("Expected BigUInt, got {:?}", other),
    }

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 3);
}

#[test]
fn test_delete_all_rows() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());

    for i in 1..=3 {
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
    handle.commit().expect("Failed to commit");

    harness.execute_sql("DELETE FROM users");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 0);
}

#[test]
fn test_complex_query_with_join_filter_sort_limit() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    harness.create_table("orders", 2, orders_columns(), handle.id());

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

    let handle1 = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle1.id());

    harness.insert_row_direct(
        1,
        vec![
            DataType::BigUInt(UInt64(1)),
            DataType::Blob("Alice".into()),
            DataType::Blob("alice@test.com".into()),
            DataType::Int(25.into()),
        ],
        &handle1,
    );
    handle1.commit().expect("Failed to commit");

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
    harness.create_table("users", 1, users_columns(), handle.id());

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

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 2);

    let results = harness.execute_sql("SELECT SUM(age) FROM users");
    match &results[0][0] {
        DataType::Double(v) => assert_eq!(v.value(), 30.0),
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_empty_result_aggregation() {
    let harness = TestHarness::new();

    let handle = harness.begin_transaction();
    harness.create_table("users", 1, users_columns(), handle.id());
    handle.commit().expect("Failed to commit");

    let count = harness.execute_sql_count("SELECT COUNT(*) FROM users");
    assert_eq!(count, 0);

    let results = harness.execute_sql("SELECT SUM(age) FROM users");
    assert!(matches!(&results[0][0], DataType::Null));
}
