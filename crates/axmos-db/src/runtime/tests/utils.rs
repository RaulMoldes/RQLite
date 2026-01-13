use crate::{
    DBConfig, TransactionId,
    io::logger::Begin,
    io::pager::{BtreeBuilder, Pager, SharedPager},
    multithreading::coordinator::{Snapshot, TransactionCoordinator, TransactionHandle},
    runtime::{
        RuntimeResult,
        builder::ExecutorBuilder,
        context::{TransactionContext, TransactionLogger},
        ddl::DdlExecutor,
    },
    schema::{
        base::{Column, Relation},
        catalog::{Catalog, SharedCatalog},
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
    tree::accessor::Accessor,
    types::{DataType, DataTypeKind, ObjectId, UInt64},
};
use tempfile::TempDir;

pub struct TestHarness {
    _temp_dir: TempDir,
    pager: SharedPager,
    catalog: SharedCatalog,
    coordinator: TransactionCoordinator,
}

impl TestHarness {
    pub fn new() -> Self {
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

        let catalog = Catalog::new(pager.clone(), meta_table_page, meta_index_page);
        let catalog = SharedCatalog::from(catalog);
        let coordinator = TransactionCoordinator::new(pager.clone());

        Self {
            _temp_dir: temp_dir,
            pager,
            catalog,
            coordinator,
        }
    }

    pub fn catalog(&self) -> SharedCatalog {
        self.catalog.clone()
    }

    pub fn begin_transaction(&self) -> (TransactionHandle, u64) {
        let handle = self
            .coordinator
            .begin()
            .expect("Failed to begin transaction");
        let last_lsn = self
            .pager
            .write()
            .push_to_log(Begin, handle.id(), None)
            .unwrap();
        (handle, last_lsn)
    }

    pub fn tree_builder(&self) -> BtreeBuilder {
        let pager = self.pager.read();
        BtreeBuilder::new(pager.min_keys_per_page(), pager.num_siblings_per_side())
            .with_pager(self.pager.clone())
    }

    pub fn create_context(&self, handle: TransactionHandle) -> RuntimeResult<TransactionContext> {
        TransactionContext::new(self.pager.clone(), self.catalog.clone(), handle)
    }

    pub fn create_logger(&self, id: TransactionId, last_lsn: u64) -> TransactionLogger {
        TransactionLogger::new(id, self.pager.clone(), last_lsn)
    }

    pub fn create_table(&self, name: &str, columns: Vec<Column>, txid: u64) -> ObjectId {
        let object_id = self.catalog.get_next_object_id();

        let root_page = self
            .pager
            .write()
            .allocate_page::<BtreePage>()
            .expect("Failed to allocate table page");

        let relation = Relation::table(object_id, name, root_page, columns);
        self.catalog
            .store_relation(relation, &self.tree_builder(), txid)
            .expect("Failed to store relation");

        object_id
    }

    pub fn optimize_sql(&self, sql: &str, snapshot: &Snapshot) -> PhysicalPlan {
        let builder = self.tree_builder();
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Parse failed");
        let mut binder = Binder::new(self.catalog.clone(), builder.clone(), snapshot);
        let bound = binder.bind(&stmt).expect("Bind failed");
        let mut optimizer =
            CascadesOptimizer::with_defaults(self.catalog.clone(), builder, &snapshot);
        optimizer.optimize(&bound).expect("Optimize failed")
    }

    pub fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        handle: TransactionHandle,
        last_lsn: u64,
    ) -> Vec<Row> {
        println!("{}", plan.explain());

        let tid = handle.id();
        let ctx = self
            .create_context(handle)
            .expect("Failed to create context");

        let logger = self.create_logger(tid, last_lsn);

        let builder = ExecutorBuilder::new(ctx.create_child().unwrap(), logger);
        let mut executor = builder.build(plan).expect("Failed to build executor");

        let mut results = Vec::new();
        while let Some(row) = executor.next().expect("Execution failed") {
            results.push(row);
        }
        ctx.commit_transaction().unwrap();
        results
    }

    pub fn execute_sql(&self, sql: &str) -> Vec<Row> {
        let (handle, last_lsn) = self.begin_transaction();
        let snapshot = handle.snapshot();
        let plan = self.optimize_sql(sql, snapshot);
        dbg!(&plan);
        self.execute_plan(&plan, handle, last_lsn)
    }

    pub fn execute_ddl(&self, sql: &str) -> RuntimeResult<()> {
        let (handle, last_lsn) = self.begin_transaction();
        let snapshot = handle.snapshot();
        let builder = self.tree_builder();

        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Parse failed");

        let mut binder = Binder::new(self.catalog.clone(), builder.clone(), snapshot);
        let bound = binder.bind(&stmt).expect("Bind failed");
        let tid = handle.id();
        let ctx = self
            .create_context(handle)
            .expect("Failed to create context");

        let logger = self.create_logger(tid, last_lsn);
        let mut ddl_executor = DdlExecutor::new(ctx.create_child().unwrap(), logger);
        ddl_executor.execute(&bound)?;

        ctx.commit_transaction().unwrap();
        Ok(())
    }

    pub fn insert_row_direct(
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
    pub fn assert_count(&self, sql: &str, expected: usize) {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), expected, "SQL: {}", sql);
    }

    pub fn query_count(&self, table: &str) -> u64 {
        let results = self.execute_sql(&format!("SELECT COUNT(*) FROM {}", table));
        assert_eq!(results.len(), 1, "Expected single row for COUNT(*)");
        match &results[0][0] {
            DataType::BigUInt(v) => v.value(),
            DataType::BigInt(v) => v.value() as u64,
            other => panic!("Expected numeric result, got {:?}", other),
        }
    }

    pub fn assert_single_int(&self, sql: &str, expected: i32) {
        let results = self.execute_sql(sql);
        assert_eq!(results.len(), 1);
        match &results[0][0] {
            DataType::Int(v) => assert_eq!(v.value(), expected, "SQL: {}", sql),
            other => panic!("Expected Int, got {:?}", other),
        }
    }

    pub fn assert_single_double(&self, sql: &str, expected: f64, tolerance: f64) {
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

    pub fn setup_users_table(&self, num_rows: u64) {
        let (mut handle, last_lsn) = self.begin_transaction();
        let table_id = self.create_table("users", users_columns(), handle.id());

        for i in 1..=num_rows {
            self.insert_row_direct(
                table_id,
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

    pub fn setup_orders_table(&self, orders: &[(u64, u64, f64, &str)]) {
        let (mut handle, last_lsn) = self.begin_transaction();
        let tid = self.create_table("orders", orders_columns(), handle.id());

        for &(id, user_id, amount, status) in orders {
            self.insert_row_direct(
                tid,
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

    pub fn setup_products_table(&self) {
        let (mut handle, last_lsn) = self.begin_transaction();
        let id = self.create_table("products", products_columns(), handle.id());
        println!("Created products table {id}");
        handle.commit().expect("Failed to commit");
    }

    pub fn setup_employees_table(&self) {
        let (mut handle, last_lsn) = self.begin_transaction();
        self.create_table("employees", employees_columns(), handle.id());
        handle.commit().expect("Failed to commit");
    }
}

pub fn users_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Blob, "email"),
        Column::new_with_defaults(DataTypeKind::Int, "age"),
    ]
}

pub fn orders_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::BigUInt, "user_id"),
        Column::new_with_defaults(DataTypeKind::Double, "amount"),
        Column::new_with_defaults(DataTypeKind::Blob, "status"),
    ]
}

pub fn products_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Int, "price"),
    ]
}

pub fn employees_columns() -> Vec<Column> {
    vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Blob, "department"),
        Column::new_with_defaults(DataTypeKind::Int, "salary"),
    ]
}
