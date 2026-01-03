use crate::{
    io::pager::BtreeBuilder,
    multithreading::coordinator::Snapshot,
    schema::{
        base::{Column, Relation},
        catalog::{CatalogTrait, MemCatalog},
    },
    sql::{
        binder::{
            DatabaseItem, ScopeError,
            analyzer::{Analyzer, AnalyzerError, AnalyzerResult},
            binder::{Binder, BinderError, BinderResult},
            bounds::*,
        },
        parser::Parser,
        planner::{CascadesOptimizer, PhysicalPlan},
    },
    types::DataTypeKind,
};

/// Helper: Creates a test catalog with predefined tables
fn create_test_catalog(builder: &BtreeBuilder) -> MemCatalog {
    let mut catalog = MemCatalog::new();

    // Table: users (id INT, name TEXT, email TEXT, age INT)
    let users = Relation::table(
        1,
        "users",
        100,
        vec![
            Column::new_with_defaults(DataTypeKind::Int, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Blob, "email"),
            Column::new_with_defaults(DataTypeKind::Int, "age"),
        ],
    );
    catalog.store_relation(users, builder, 0).unwrap();

    // Table: orders (id INT, user_id INT, amount DOUBLE, status TEXT)
    let orders = Relation::table(
        2,
        "orders",
        200,
        vec![
            Column::new_with_defaults(DataTypeKind::Int, "id"),
            Column::new_with_defaults(DataTypeKind::Int, "user_id"),
            Column::new_with_defaults(DataTypeKind::Double, "amount"),
            Column::new_with_defaults(DataTypeKind::Blob, "status"),
        ],
    );
    catalog.store_relation(orders, builder, 0).unwrap();

    // Table: products (id INT, name TEXT, price DOUBLE, category TEXT)
    let products = Relation::table(
        3,
        "products",
        300,
        vec![
            Column::new_with_defaults(DataTypeKind::Int, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::Double, "price"),
            Column::new_with_defaults(DataTypeKind::Blob, "category"),
        ],
    );
    catalog.store_relation(products, builder, 0).unwrap();

    catalog
}

/// Helper: Parses and binds SQL
fn bind_sql(sql: &str) -> BinderResult<BoundStatement> {
    let snapshot = Snapshot::default();
    let builder = BtreeBuilder::default();
    let catalog = create_test_catalog(&builder);

    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse()
        .map_err(|e| BinderError::Other(e.to_string()))?;
    let mut binder = Binder::new(catalog, builder, snapshot);
    binder.bind(&stmt)
}

/// Helper: Parses and analyzes SQL, returns result
fn analyze_sql<C: CatalogTrait>(
    sql: &str,
    catalog: C,
    builder: BtreeBuilder,
    snapshot: Snapshot,
) -> AnalyzerResult<()> {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse().map_err(AnalyzerError::Parser)?;
    let mut analyzer = Analyzer::new(catalog, builder, snapshot);
    analyzer.analyze(&stmt)
}

fn analyzer_test(sql: &str) -> AnalyzerResult<()> {
    let snapshot = Snapshot::default();
    let builder = BtreeBuilder::default();
    let catalog = create_test_catalog(&builder);

    analyze_sql(sql, catalog, builder, snapshot)
}

/// Helper: Optimizes SQL and returns the physical plan
fn optimize_sql(sql: &str) -> PhysicalPlan {
    let snapshot = Snapshot::default();
    let builder = BtreeBuilder::default();
    let catalog = create_test_catalog(&builder);

    let mut parser = Parser::new(sql);
    let stmt = parser.parse().expect("parse failed");

    let mut binder = Binder::new(catalog.clone(), builder.clone(), snapshot.clone());
    let bound = binder.bind(&stmt).expect("bind failed");

    let mut optimizer = CascadesOptimizer::with_defaults(catalog, builder, snapshot);
    optimizer.optimize(&bound).expect("optimize failed")
}

/// Helper: Optimizes SQL and returns Result for error testing
fn try_optimize_sql(sql: &str) -> Result<PhysicalPlan, String> {
    let snapshot = Snapshot::default();
    let builder = BtreeBuilder::default();
    let catalog = create_test_catalog(&builder);

    let mut parser = Parser::new(sql);
    let stmt = parser.parse().map_err(|e| e.to_string())?;

    let mut binder = Binder::new(catalog.clone(), builder.clone(), snapshot.clone());
    let bound = binder.bind(&stmt).map_err(|e| e.to_string())?;

    let mut optimizer = CascadesOptimizer::with_defaults(catalog, builder, snapshot);
    optimizer.optimize(&bound).map_err(|e| e.to_string())
}

/// Helper: Returns the root operator name
fn root_op_name(plan: &PhysicalPlan) -> &'static str {
    plan.op.name()
}

/// Helper: Counts operators of a given type in the plan
fn count_operators(plan: &PhysicalPlan, name: &str) -> usize {
    let mut count = if plan.op.name() == name { 1 } else { 0 };
    for child in &plan.children {
        count += count_operators(child, name);
    }
    count
}

/// Helper: Checks if plan contains an operator
fn has_operator(plan: &PhysicalPlan, name: &str) -> bool {
    count_operators(plan, name) > 0
}

/// Helper: Gets the leaf operators (scans)
fn get_leaf_ops(plan: &PhysicalPlan) -> Vec<&'static str> {
    if plan.children.is_empty() {
        vec![plan.op.name()]
    } else {
        plan.children.iter().flat_map(get_leaf_ops).collect()
    }
}

#[test]
fn test_analyzer_select_simple_columns() {
    let result = analyzer_test("SELECT id, name FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_select_star() {
    let result = analyzer_test("SELECT * FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_select_star_without_from_fails() {
    let result = analyzer_test("SELECT *");
    assert!(result.is_err());
}

#[test]
fn test_analyzer_select_nonexistent_column_fails() {
    let result = analyzer_test("SELECT nonexistent FROM users");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_select_nonexistent_table_fails() {
    let result = analyzer_test("SELECT id FROM nonexistent");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_select_qualified_column() {
    let result = analyzer_test("SELECT users.name FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_select_with_alias() {
    let result = analyzer_test("SELECT u.name FROM users u");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_select_wrong_alias_fails() {
    let result = analyzer_test("SELECT users.name FROM users u");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_join_basic() {
    let result = analyzer_test(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_join_ambiguous_column_fails() {
    // Both tables have 'id' column
    let result = analyzer_test("SELECT id FROM users JOIN orders ON users.id = orders.user_id");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::AmbiguousColumn(_)))
    ));
}

#[test]
fn test_analyzer_join_qualified_resolves_ambiguity() {
    let result = analyzer_test(
        "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_cross_join() {
    let result = analyzer_test("SELECT users.name, products.name FROM users, products");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_duplicate_alias_fails() {
    let result = analyzer_test("SELECT * FROM users t, orders t");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::DuplicateAlias(_)))
    ));
}

#[test]
fn test_analyzer_where_clause_valid() {
    let result = analyzer_test("SELECT name FROM users WHERE age > 18");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_where_clause_invalid_column() {
    let result = analyzer_test("SELECT name FROM users WHERE invalid_col > 18");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_where_with_subquery() {
    let result = analyzer_test("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_where_exists_subquery() {
    let result = analyzer_test(
        "SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_group_by_valid() {
    let result = analyzer_test("SELECT status, COUNT(*) FROM orders GROUP BY status");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_having_valid() {
    let result = analyzer_test(
        "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id HAVING SUM(amount) > 100",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_order_by_valid() {
    let result = analyzer_test("SELECT name, age FROM users ORDER BY age DESC");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_order_by_invalid_column() {
    let result = analyzer_test("SELECT name FROM users ORDER BY invalid_col");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_subquery_in_from() {
    let result = analyzer_test("SELECT sub.name FROM (SELECT name FROM users) AS sub");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_correlated_subquery() {
    let result = analyzer_test(
        "SELECT * FROM users u WHERE u.id = (SELECT MAX(user_id) FROM orders WHERE user_id = u.id)",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_cte_basic() {
    let result = analyzer_test(
        "WITH active_users AS (SELECT id, name FROM users WHERE age > 18) SELECT * FROM active_users",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_cte_multiple() {
    let result = analyzer_test(
        "WITH
                adults AS (SELECT id, name FROM users WHERE age >= 18),
                big_orders AS (SELECT user_id, amount FROM orders WHERE amount > 100)
            SELECT adults.name, big_orders.amount
            FROM adults JOIN big_orders ON adults.id = big_orders.user_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_insert_all_columns() {
    let result = analyzer_test("INSERT INTO users VALUES (1, 'John', 'john@test.com', 25)");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_insert_specific_columns() {
    let result = analyzer_test("INSERT INTO users (id, name) VALUES (1, 'John')");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_insert_invalid_column_fails() {
    let result = analyzer_test("INSERT INTO users (id, invalid_col) VALUES (1, 'test')");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Column(_, _)))
    ));
}

#[test]
fn test_analyzer_insert_column_count_mismatch_fails() {
    let result = analyzer_test("INSERT INTO users (id, name) VALUES (1, 'John', 'extra')");
    assert!(result.is_err());
}

#[test]
fn test_analyzer_insert_from_select() {
    let result =
        analyzer_test("INSERT INTO users (id, name) SELECT id, name FROM users WHERE age > 18");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_update_simple() {
    let result = analyzer_test("UPDATE users SET age = 30 WHERE id = 1");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_update_with_expression() {
    let result = analyzer_test("UPDATE users SET age = age + 1");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_update_invalid_column_fails() {
    let result = analyzer_test("UPDATE users SET invalid_col = 1");

    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_update_invalid_table_fails() {
    let result = analyzer_test("UPDATE nonexistent SET id = 1");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_delete_simple() {
    let result = analyzer_test("DELETE FROM users WHERE id = 1");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_delete_invalid_column_in_where() {
    let result = analyzer_test("DELETE FROM users WHERE invalid_col = 1");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::NotFound(_)))
    ));
}

#[test]
fn test_analyzer_create_table_simple() {
    let result = analyzer_test("CREATE TABLE new_table (id INT, name TEXT)");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_create_table_already_exists_fails() {
    let result = analyzer_test("CREATE TABLE users (id INT)");
    assert!(matches!(
        result,
        Err(AnalyzerError::AlreadyExists(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_create_table_duplicate_column_fails() {
    let result = analyzer_test("CREATE TABLE new_table (id INT, id INT)");
    assert!(matches!(
        result,
        Err(AnalyzerError::Scope(ScopeError::AmbiguousColumn(_)))
    ));
}

#[test]
fn test_analyzer_create_table_with_foreign_key() {
    let result = analyzer_test(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES orders(id))",
    );
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_create_table_fk_invalid_ref_table_fails() {
    let result = analyzer_test(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES nonexistent(id))",
    );
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_create_table_fk_invalid_ref_column_fails() {
    let result = analyzer_test(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES orders(invalid_col))",
    );
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Column(_, _)))
    ));
}

#[test]
fn test_analyzer_alter_add_column() {
    let result = analyzer_test("ALTER TABLE users ADD COLUMN status TEXT");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_alter_add_column_already_exists_fails() {
    let result = analyzer_test("ALTER TABLE users ADD COLUMN name TEXT");
    assert!(matches!(
        result,
        Err(AnalyzerError::AlreadyExists(DatabaseItem::Column(_, _)))
    ));
}

#[test]
fn test_analyzer_alter_drop_column() {
    let result = analyzer_test("ALTER TABLE users DROP COLUMN email");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_alter_drop_column_not_found_fails() {
    let result = analyzer_test("ALTER TABLE users DROP COLUMN nonexistent");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Column(_, _)))
    ));
}

#[test]
fn test_analyzer_alter_table_not_found_fails() {
    let result = analyzer_test("ALTER TABLE nonexistent ADD COLUMN id INT");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_drop_table() {
    let result = analyzer_test("DROP TABLE users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_drop_table_not_found_fails() {
    let result = analyzer_test("DROP TABLE nonexistent");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_drop_table_if_exists() {
    let result = analyzer_test("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_create_index() {
    let result = analyzer_test("CREATE INDEX idx_name ON users(name)");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_create_index_invalid_column_fails() {
    let result = analyzer_test("CREATE INDEX idx_invalid ON users(nonexistent)");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Column(_, _)))
    ));
}

#[test]
fn test_analyzer_create_index_invalid_table_fails() {
    let result = analyzer_test("CREATE INDEX idx_test ON nonexistent(id)");
    assert!(matches!(
        result,
        Err(AnalyzerError::NotFound(DatabaseItem::Table(_)))
    ));
}

#[test]
fn test_analyzer_function_call() {
    let result = analyzer_test("SELECT UPPER(name), ABS(age) FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_case_expression() {
    let result =
        analyzer_test("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_between_expression() {
    let result = analyzer_test("SELECT * FROM users WHERE age BETWEEN 18 AND 65");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_in_list_expression() {
    let result = analyzer_test("SELECT * FROM users WHERE age IN (18, 21, 30)");
    assert!(result.is_ok());
}

#[test]
fn test_analyzer_complex_query_with_joins_subqueries_and_aggregates() {
    let result = analyzer_test(
        "SELECT
                u.name,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.age >= 18
            GROUP BY u.id, u.name
            HAVING SUM(o.amount) > 100
            ORDER BY total_spent DESC
            LIMIT 10",
    );

    assert!(result.is_ok());
}

#[test]
fn test_binder_select_simple_columns() {
    let result = bind_sql("SELECT id, name FROM users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        assert_eq!(select.columns.len(), 2);
        // id is column 0, name is column 1
        if let BoundExpression::ColumnRef(cr) = &select.columns[0].expr {
            assert_eq!(cr.column_idx, 0);
            assert_eq!(cr.data_type, DataTypeKind::Int);
        }
        if let BoundExpression::ColumnRef(cr) = &select.columns[1].expr {
            assert_eq!(cr.column_idx, 1);
            assert_eq!(cr.data_type, DataTypeKind::Blob);
        }
    }
}

#[test]
fn test_binder_select_star() {
    let result = bind_sql("SELECT * FROM users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        assert_eq!(select.columns.len(), 4); // id, name, email, age
    }
}

#[test]
fn test_binder_select_nonexistent_column_fails() {
    let result = bind_sql("SELECT nonexistent FROM users");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_select_nonexistent_table_fails() {
    let result = bind_sql("SELECT id FROM nonexistent");
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_select_qualified_column() {
    let result = bind_sql("SELECT users.name FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_binder_select_with_alias() {
    let result = bind_sql("SELECT u.name FROM users u");
    assert!(result.is_ok());
}

#[test]
fn test_binder_select_wrong_alias_fails() {
    let result = bind_sql("SELECT users.name FROM users u");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_join_basic() {
    let result = bind_sql(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_join_ambiguous_column_fails() {
    let result = bind_sql("SELECT id FROM users JOIN orders ON users.id = orders.user_id");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_join_qualified_resolves_ambiguity() {
    let result =
        bind_sql("SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        // First id from users (scope_index 0), second from orders (scope_index 1)
        if let BoundExpression::ColumnRef(cr) = &select.columns[0].expr {
            assert_eq!(cr.scope_index, 0);
            assert_eq!(cr.column_idx, 0);
        }
        if let BoundExpression::ColumnRef(cr) = &select.columns[1].expr {
            assert_eq!(cr.scope_index, 1);
            assert_eq!(cr.column_idx, 0);
        }
    }
}

#[test]
fn test_binder_cross_join() {
    let result = bind_sql("SELECT users.name, products.name FROM users, products");
    assert!(result.is_ok());
}

#[test]
fn test_binder_duplicate_alias_fails() {
    let result = bind_sql("SELECT * FROM users t, orders t");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_where_clause_valid() {
    let result = bind_sql("SELECT name FROM users WHERE age > 18");
    assert!(result.is_ok());
}

#[test]
fn test_binder_where_clause_invalid_column() {
    let result = bind_sql("SELECT name FROM users WHERE invalid_col > 18");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_group_by_valid() {
    let result = bind_sql("SELECT status, COUNT(*) FROM orders GROUP BY status");
    assert!(result.is_ok());
}

#[test]
fn test_binder_having_valid() {
    let result = bind_sql(
        "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id HAVING SUM(amount) > 100",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_order_by_valid() {
    let result = bind_sql("SELECT name, age FROM users ORDER BY age DESC");
    assert!(result.is_ok());
}

#[test]
fn test_binder_order_by_alias() {
    let result = bind_sql(
        "SELECT name, SUM(amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY name ORDER BY total DESC",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_order_by_invalid_column() {
    let result = bind_sql("SELECT name FROM users ORDER BY invalid_col");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_subquery_in_from() {
    let result = bind_sql("SELECT sub.name FROM (SELECT name FROM users) AS sub");
    assert!(result.is_ok());
}

#[test]
fn test_binder_where_with_subquery() {
    let result = bind_sql("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)");
    assert!(result.is_ok());
}

#[test]
fn test_binder_where_exists_subquery() {
    let result = bind_sql(
        "SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_correlated_subquery() {
    let result = bind_sql(
        "SELECT * FROM users u WHERE u.id = (SELECT MAX(user_id) FROM orders WHERE user_id = u.id)",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_cte_basic() {
    let result = bind_sql(
        "WITH active_users AS (SELECT id, name FROM users WHERE age > 18) SELECT * FROM active_users",
    );
    assert!(result.is_ok());

    if let Ok(BoundStatement::With(with_stmt)) = result {
        assert_eq!(with_stmt.ctes.len(), 1);
        if let Some(BoundTableRef::Cte { cte_idx, .. }) = &with_stmt.body.from {
            assert_eq!(*cte_idx, 0);
        }
    }
}

#[test]
fn test_binder_cte_multiple() {
    let result = bind_sql(
        "WITH
                adults AS (SELECT id, name FROM users WHERE age >= 18),
                big_orders AS (SELECT user_id, amount FROM orders WHERE amount > 100)
            SELECT adults.name, big_orders.amount
            FROM adults JOIN big_orders ON adults.id = big_orders.user_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_insert_all_columns() {
    let result = bind_sql("INSERT INTO users VALUES (1, 'John', 'john@test.com', 25)");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Insert(insert)) = result {
        assert_eq!(insert.columns, vec![0, 1, 2, 3]);
    }
}

#[test]
fn test_binder_insert_specific_columns() {
    let result = bind_sql("INSERT INTO users (id, name) VALUES (1, 'John')");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Insert(insert)) = result {
        assert_eq!(insert.columns, vec![0, 1]); // id=0, name=1
    }
}

#[test]
fn test_binder_insert_invalid_column_fails() {
    let result = bind_sql("INSERT INTO users (id, invalid_col) VALUES (1, 'test')");
    assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
}

#[test]
fn test_binder_insert_column_count_mismatch_fails() {
    let result = bind_sql("INSERT INTO users (id, name) VALUES (1, 'John', 'extra')");
    assert!(matches!(
        result,
        Err(BinderError::ColumnCountMismatch { .. })
    ));
}

#[test]
fn test_binder_insert_from_select() {
    let result = bind_sql("INSERT INTO users (id, name) SELECT id, name FROM users WHERE age > 18");
    assert!(result.is_ok());
}

#[test]
fn test_binder_update_simple() {
    let result = bind_sql("UPDATE users SET age = 30 WHERE id = 1");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Update(update)) = result {
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].column_idx, 3); // age is index 3
    }
}

#[test]
fn test_binder_update_with_expression() {
    let result = bind_sql("UPDATE users SET age = age + 1");
    assert!(result.is_ok());
}

#[test]
fn test_binder_update_invalid_column_fails() {
    let result = bind_sql("UPDATE users SET invalid_col = 1");
    assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
}

#[test]
fn test_binder_update_invalid_table_fails() {
    let result = bind_sql("UPDATE nonexistent SET id = 1");
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_delete_simple() {
    let result = bind_sql("DELETE FROM users WHERE id = 1");
    assert!(result.is_ok());
}

#[test]
fn test_binder_delete_invalid_column_in_where() {
    let result = bind_sql("DELETE FROM users WHERE invalid_col = 1");
    assert!(matches!(result, Err(BinderError::Scope(_))));
}

#[test]
fn test_binder_create_table_simple() {
    let result = bind_sql("CREATE TABLE new_table (id INT, name TEXT)");
    assert!(result.is_ok());

    if let Ok(BoundStatement::CreateTable(create)) = result {
        assert_eq!(create.table_name, "new_table");
        assert_eq!(create.columns.len(), 2);
    }
}

#[test]
fn test_binder_create_table_with_foreign_key() {
    let result = bind_sql(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES orders(id))",
    );
    assert!(result.is_ok());

    if let Ok(BoundStatement::CreateTable(create)) = result {
        assert_eq!(create.constraints.len(), 1);
        if let BoundTableConstraint::ForeignKey {
            ref_table_id,
            ref_columns,
            ..
        } = &create.constraints[0]
        {
            assert_eq!(*ref_table_id, 2); // orders table_id
            assert_eq!(ref_columns, &vec![0]); // id column
        }
    }
}

#[test]
fn test_binder_create_table_fk_invalid_ref_table_fails() {
    let result = bind_sql(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES nonexistent(id))",
    );
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_create_table_fk_invalid_ref_column_fails() {
    let result = bind_sql(
        "CREATE TABLE order_items (id INT, order_id INT, FOREIGN KEY (order_id) REFERENCES orders(invalid_col))",
    );
    assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
}

#[test]
fn test_binder_alter_add_column() {
    let result = bind_sql("ALTER TABLE users ADD COLUMN status TEXT");
    assert!(result.is_ok());
}

#[test]
fn test_binder_alter_drop_column() {
    let result = bind_sql("ALTER TABLE users DROP COLUMN email");
    assert!(result.is_ok());

    if let Ok(BoundStatement::AlterTable(alter)) = result {
        if let BoundAlterAction::DropColumn(idx) = alter.action {
            assert_eq!(idx, 2); // email is index 2
        }
    }
}

#[test]
fn test_binder_alter_drop_column_not_found_fails() {
    let result = bind_sql("ALTER TABLE users DROP COLUMN nonexistent");
    assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
}

#[test]
fn test_binder_alter_table_not_found_fails() {
    let result = bind_sql("ALTER TABLE nonexistent ADD COLUMN id INT");
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_drop_table() {
    let result = bind_sql("DROP TABLE users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::DropTable(drop)) = result {
        assert_eq!(drop.table_id, Some(1)); // users table_id
    }
}

#[test]
fn test_binder_drop_table_not_found_fails() {
    let result = bind_sql("DROP TABLE nonexistent");
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_drop_table_if_exists() {
    let result = bind_sql("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_binder_create_index() {
    let result = bind_sql("CREATE INDEX idx_name ON users(name)");
    assert!(result.is_ok());

    if let Ok(BoundStatement::CreateIndex(idx)) = result {
        assert_eq!(idx.table_id, 1);
        assert_eq!(idx.columns.len(), 1);
        assert_eq!(idx.columns[0].column_idx, 1); // name is index 1
    }
}

#[test]
fn test_binder_create_index_invalid_column_fails() {
    let result = bind_sql("CREATE INDEX idx_invalid ON users(nonexistent)");
    assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
}

#[test]
fn test_binder_create_index_invalid_table_fails() {
    let result = bind_sql("CREATE INDEX idx_test ON nonexistent(id)");
    assert!(matches!(result, Err(BinderError::TableNotFound(_))));
}

#[test]
fn test_binder_function_call() {
    let result = bind_sql("SELECT UPPER(name), ABS(age) FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_binder_aggregate_functions() {
    let result = bind_sql("SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        assert!(matches!(
            &select.columns[0].expr,
            BoundExpression::Aggregate {
                func: AggregateFunction::Count,
                ..
            }
        ));
        assert!(matches!(
            &select.columns[1].expr,
            BoundExpression::Aggregate {
                func: AggregateFunction::Sum,
                ..
            }
        ));
    }
}

#[test]
fn test_binder_case_expression() {
    let result = bind_sql("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_binder_between_expression() {
    let result = bind_sql("SELECT * FROM users WHERE age BETWEEN 18 AND 65");
    assert!(result.is_ok());
}

#[test]
fn test_binder_in_list_expression() {
    let result = bind_sql("SELECT * FROM users WHERE age IN (18, 21, 30)");
    assert!(result.is_ok());
}

#[test]
fn test_binder_complex_query_with_joins_subqueries_and_aggregates() {
    let result = bind_sql(
        "SELECT
                u.name,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.age >= 18
            GROUP BY u.id, u.name
            HAVING SUM(o.amount) > 100
            ORDER BY total_spent DESC
            LIMIT 10",
    );
    assert!(result.is_ok());
}

#[test]
fn test_binder_expression_type_inference() {
    let result = bind_sql("SELECT age + 1, age > 18, COUNT(*), UPPER(name) FROM users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        // age + 1 -> numeric
        assert_eq!(select.columns[0].expr.data_type(), DataTypeKind::Int);
        // age > 18 -> Bool
        assert_eq!(select.columns[1].expr.data_type(), DataTypeKind::Bool);
        // COUNT(*) -> BigInt
        assert_eq!(select.columns[2].expr.data_type(), DataTypeKind::BigInt);
        // UPPER(name) -> Blob
        assert_eq!(select.columns[3].expr.data_type(), DataTypeKind::Blob);
    }
}

#[test]
fn test_binder_schema_generation() {
    let result = bind_sql("SELECT name, age * 2 as double_age FROM users");
    assert!(result.is_ok());

    if let Ok(BoundStatement::Select(select)) = result {
        assert_eq!(select.schema.num_columns(), 2);
        assert_eq!(select.schema.iter_columns().next().unwrap().name(), "name");
    }
}

#[test]
fn test_optimize_simple_select() {
    let plan = optimize_sql("SELECT id, name FROM users");

    assert!(plan.cost > 0.0);
    assert!(has_operator(&plan, "SeqScan"));
}

#[test]
fn test_optimize_select_star() {
    let plan = optimize_sql("SELECT * FROM users");

    assert!(has_operator(&plan, "SeqScan"));
    assert!(plan.cost > 0.0);
}

#[test]
fn test_optimize_select_with_filter() {
    let plan = optimize_sql("SELECT name FROM users WHERE age > 18");

    assert!(has_operator(&plan, "Filter"));
    assert!(has_operator(&plan, "SeqScan"));
}

#[test]
fn test_optimize_select_with_projection() {
    let plan = optimize_sql("SELECT name, age FROM users");

    // Should have a scan, may or may not have explicit project
    assert!(has_operator(&plan, "SeqScan"));
}

#[test]
fn test_optimize_inner_join() {
    let plan = optimize_sql(
        "SELECT users.name, orders.amount
         FROM users
         JOIN orders ON users.id = orders.user_id",
    );

    // Should produce some join operator
    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join, "Expected a join operator");

    // Should have two scans
    let leaves = get_leaf_ops(&plan);
    assert_eq!(leaves.len(), 2);
}

#[test]
fn test_optimize_left_join() {
    let plan = optimize_sql(
        "SELECT users.name, orders.amount
         FROM users
         LEFT JOIN orders ON users.id = orders.user_id",
    );

    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join);
}

#[test]
fn test_optimize_cross_join() {
    let plan = optimize_sql("SELECT users.name, products.name FROM users, products");

    // Cross join typically becomes nested loop
    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join);
}

#[test]
fn test_optimize_three_way_join() {
    let plan = optimize_sql(
        "SELECT u.name, o.amount, p.price
         FROM users u
         JOIN orders o ON u.id = o.user_id
         JOIN products p ON p.id = o.id",
    );

    // Should have 2 joins and 3 scans
    let join_count = count_operators(&plan, "HashJoin")
        + count_operators(&plan, "NestedLoopJoin")
        + count_operators(&plan, "MergeJoin");
    assert_eq!(join_count, 2, "Expected 2 join operators");

    let leaves = get_leaf_ops(&plan);
    assert_eq!(leaves.len(), 3);
}

#[test]
fn test_optimize_count_star() {
    let plan = optimize_sql("SELECT COUNT(*) FROM users");

    assert!(has_operator(&plan, "HashAggregate"));
}

#[test]
fn test_optimize_group_by() {
    let plan = optimize_sql("SELECT status, COUNT(*) FROM orders GROUP BY status");

    assert!(has_operator(&plan, "HashAggregate"));
}

#[test]
fn test_optimize_group_by_with_having() {
    let plan = optimize_sql(
        "SELECT user_id, SUM(amount)
         FROM orders
         GROUP BY user_id
         HAVING SUM(amount) > 100",
    );

    assert!(has_operator(&plan, "HashAggregate"));
    assert!(has_operator(&plan, "Filter"));
}

#[test]
fn test_optimize_multiple_aggregates() {
    let plan = optimize_sql(
        "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
    );

    assert!(has_operator(&plan, "HashAggregate"));
}

#[test]
fn test_optimize_order_by() {
    let plan = optimize_sql("SELECT name, age FROM users ORDER BY age DESC");

    assert!(has_operator(&plan, "Sort"));
}

#[test]
fn test_optimize_limit() {
    let plan = optimize_sql("SELECT * FROM users LIMIT 10");

    assert!(has_operator(&plan, "Limit"));
}

#[test]
fn test_optimize_limit_offset() {
    let plan = optimize_sql("SELECT * FROM users LIMIT 10 OFFSET 5");

    assert!(has_operator(&plan, "Limit"));
}

#[test]
fn test_optimize_order_by_limit() {
    let plan = optimize_sql("SELECT name FROM users ORDER BY age LIMIT 5");
    println!("{}", plan.explain());
    assert!(has_operator(&plan, "Sort"));
    assert!(has_operator(&plan, "Limit"));
}

#[test]
fn test_optimize_distinct() {
    let plan = optimize_sql("SELECT DISTINCT name FROM users");

    assert!(has_operator(&plan, "Distinct"));
}

#[test]
fn test_optimize_distinct_multiple_columns() {
    let plan = optimize_sql("SELECT DISTINCT name, age FROM users");

    assert!(has_operator(&plan, "Distinct"));
}

#[test]
fn test_optimize_subquery_in_from() {
    let plan = optimize_sql("SELECT sub.name FROM (SELECT name FROM users WHERE age > 18) AS sub");

    assert!(has_operator(&plan, "Filter"));
}

#[test]
fn test_optimize_in_subquery() {
    let plan = optimize_sql("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)");

    // Should optimize to some form of semi-join or filter
    assert!(plan.cost > 0.0);
}

#[test]
fn test_optimize_exists_subquery() {
    let plan = optimize_sql(
        "SELECT name FROM users u
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );

    assert!(plan.cost > 0.0);
}

#[test]
fn test_optimize_simple_cte() {
    let plan = optimize_sql(
        "WITH adults AS (SELECT id, name FROM users WHERE age >= 18)
         SELECT * FROM adults",
    );

    assert!(has_operator(&plan, "Filter"));
}

#[test]
fn test_optimize_multiple_ctes() {
    let plan = optimize_sql(
        "WITH
            adults AS (SELECT id, name FROM users WHERE age >= 18),
            big_orders AS (SELECT user_id, amount FROM orders WHERE amount > 100)
         SELECT adults.name, big_orders.amount
         FROM adults JOIN big_orders ON adults.id = big_orders.user_id",
    );

    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join);
}

#[test]
fn test_optimize_insert_values() {
    let plan = optimize_sql("INSERT INTO users VALUES (1, 'John', 'john@test.com', 25)");

    assert!(has_operator(&plan, "Insert"));
    assert!(has_operator(&plan, "Values"));
}

#[test]
fn test_optimize_insert_select() {
    let plan =
        optimize_sql("INSERT INTO users (id, name) SELECT id, name FROM users WHERE age > 18");

    assert!(has_operator(&plan, "Insert"));
    assert!(has_operator(&plan, "Filter"));
}

#[test]
fn test_optimize_update() {
    let plan = optimize_sql("UPDATE users SET age = 30 WHERE id = 1");

    assert!(has_operator(&plan, "Update"));
    assert!(has_operator(&plan, "Materialize")); // Halloween protection
}

#[test]
fn test_optimize_update_no_where() {
    let plan = optimize_sql("UPDATE users SET age = age + 1");

    assert!(has_operator(&plan, "Update"));
    assert!(has_operator(&plan, "Materialize"));
}

#[test]
fn test_optimize_delete() {
    let plan = optimize_sql("DELETE FROM users WHERE id = 1");

    assert!(has_operator(&plan, "Delete"));
    assert!(has_operator(&plan, "Materialize")); // Halloween protection
}

#[test]
fn test_optimize_delete_no_where() {
    let plan = optimize_sql("DELETE FROM users");

    assert!(has_operator(&plan, "Delete"));
}

#[test]
fn test_optimize_complex_query() {
    let plan = optimize_sql(
        "SELECT
            u.name,
            COUNT(o.id) as order_count,
            SUM(o.amount) as total_spent
         FROM users u
         LEFT JOIN orders o ON u.id = o.user_id
         WHERE u.age >= 18
         GROUP BY u.id, u.name
         HAVING SUM(o.amount) > 100
         ORDER BY total_spent DESC
         LIMIT 10",
    );
    println!("{}", plan.explain());
    // Should have join, filter, aggregate, sort, limit
    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join);
    assert!(has_operator(&plan, "HashAggregate"));
    assert!(has_operator(&plan, "Sort"));
    assert!(has_operator(&plan, "Limit"));
}

#[test]
fn test_optimize_nested_subqueries() {
    let plan = optimize_sql(
        "SELECT name FROM users
         WHERE age > (SELECT AVG(age) FROM users WHERE id IN (SELECT user_id FROM orders))",
    );

    assert!(plan.cost > 0.0);
}

#[test]
fn test_cost_increases_with_joins() {
    let simple_plan = optimize_sql("SELECT * FROM users");
    let join_plan = optimize_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id");

    // Join should be more expensive
    assert!(join_plan.cost > simple_plan.cost);
}

#[test]
fn test_cost_reasonable_for_simple_scan() {
    let plan = optimize_sql("SELECT * FROM users");

    // Cost should be positive and finite
    assert!(plan.cost > 0.0);
    assert!(plan.cost.is_finite());
}

#[test]
fn test_hash_join_preferred_for_equijoin() {
    let plan = optimize_sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

    // For equi-joins, hash join should typically be preferred
    // (unless tables are very small)
    let has_hash_join = has_operator(&plan, "HashJoin");
    let has_nl_join = has_operator(&plan, "NestedLoopJoin");

    // At least one join type should be present
    assert!(has_hash_join || has_nl_join);
}

#[test]
fn test_filter_pushdown_through_join() {
    // Filter on left table should be pushed below join
    let plan = optimize_sql(
        "SELECT u.name, o.amount
         FROM users u
         JOIN orders o ON u.id = o.user_id
         WHERE u.age > 18",
    );

    // The filter should exist somewhere in the plan
    assert!(has_operator(&plan, "Filter"));
}

#[test]
fn test_filter_merge() {
    // Multiple stacked filters should be merged
    let plan = optimize_sql(
        "SELECT * FROM users WHERE age > 18 AND name = 'John' AND email LIKE '%@test.com'",
    );

    // Should have at most 1 filter (merged)
    let filter_count = count_operators(&plan, "Filter");
    assert!(filter_count <= 1, "Expected filters to be merged");
}

#[test]
fn test_optimize_empty_result_limit_zero() {
    let plan = optimize_sql("SELECT * FROM users LIMIT 0");

    assert!(has_operator(&plan, "Limit"));
}

#[test]
fn test_optimize_single_value_insert() {
    let plan = optimize_sql("INSERT INTO users (id) VALUES (1)");

    assert!(has_operator(&plan, "Insert"));
}

#[test]
fn test_optimize_self_join() {
    let plan = optimize_sql(
        "SELECT a.name, b.name
         FROM users a
         JOIN users b ON a.id = b.id",
    );

    let has_join = has_operator(&plan, "HashJoin")
        || has_operator(&plan, "NestedLoopJoin")
        || has_operator(&plan, "MergeJoin");
    assert!(has_join);

    // Should have two scans of the same table
    let scan_count = count_operators(&plan, "SeqScan");
    assert_eq!(scan_count, 2);
}

#[test]
fn test_plan_has_valid_schema() {
    let plan = optimize_sql("SELECT name, age FROM users");

    // The root operator should have a valid schema
    let schema = plan.op.output_schema();
    assert!(schema.num_columns() >= 2);
}

#[test]
fn test_join_output_schema_combined() {
    let plan = optimize_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id");

    // Output should have columns from both tables
    let schema = plan.op.output_schema();
    // users has 4 cols, orders has 4 cols
    assert!(schema.num_columns() >= 4);
}
