pub(crate) mod errors;
pub(crate) mod runner;
pub(crate) mod schema;
pub(crate) mod stats;

use std::{
    collections::HashMap,
    io::{self, Error as IoError, ErrorKind},
    ops::Deref,
    sync::Arc,
};

use crate::{
    AxmosDBConfig, DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, DEFAULT_CACHE_SIZE,
    DEFAULT_PAGE_SIZE, DEFAULT_POOL_SIZE, IncrementalVaccum, TRANSACTION_ZERO, TextEncoding,
    database::{
        errors::{BoxError, IntoBoxError, TaskResult, TransactionResult},
        runner::{TaskContext, TaskRunner},
        schema::{Column, Constraint, Index, Relation, Schema, Table},
        stats::CatalogStatsProvider,
    },
    io::{
        frames::FrameAccessMode,
        pager::{Pager, SharedPager},
    },
    sql::{
        binder::Binder,
        executor::{Row, build::ExecutorBuilder, context::ExecutionContext},
        lexer::Lexer,
        parser::Parser,
        planner::{CascadesOptimizer, OptimizerConfig, ProcessedStatement, model::AxmosCostModel},
    },
    storage::{
        page::BtreePage,
        tuple::{OwnedTuple, Tuple, TupleRef},
    },
    structures::{
        bplustree::{BPlusTree, SearchResult},
        comparator::{DynComparator, NumericComparator},
    },
    transactions::{TransactionCoordinator, accessor::RcPageAccessor},
    types::{
        Blob, DataType, DataTypeKind, ObjectId, PAGE_ZERO, PageId, UInt64, get_next_object,
        initialize_atomics,
    },
};

use std::path::Path;

pub(crate) const META_TABLE: &str = "rqcatalog";
pub(crate) const META_INDEX: &str = "rqindex";

/// META TABLES AND META INDEXES ARE SPECIAL.
/// THIS IS WHY TO MANAGE THE PRIMARY KEY OF THIS TABLES WE USE THE OBJECT ID ATOMIC INSTEAD OF THE STANDARD ROW-ID. THIS ALLOWS FOR FASTER ACCESS, AND ENSURES THE IDENTIFIER OF EACH TABLE ON EACH OF THE METAS IS THE SAME.
pub(crate) fn meta_idx_schema() -> Schema {
    Schema::from_columns(
        [
            Column::new_unindexed(DataTypeKind::Text, "o_name", None),
            Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
        ]
        .as_ref(),
        1,
    )
}
pub(crate) fn meta_table_schema() -> Schema {
    let mut key_constraints = HashMap::new();
    key_constraints.insert("name_pkey".to_string(), Constraint::PrimaryKey);

    Schema::from_columns(
        [
            Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
            Column::new_unindexed(DataTypeKind::BigUInt, "o_root", None),
            Column::new_unindexed(DataTypeKind::Byte, "o_type", None),
            Column::new_unindexed(DataTypeKind::Blob, "o_metadata", None),
            Column::new(
                DataTypeKind::Text,
                "o_name",
                Some(META_INDEX.to_string()),
                Some(key_constraints),
            ),
        ]
        .as_ref(),
        1,
    )
}

pub struct Database {
    pager: SharedPager,
    controller: TransactionCoordinator,
    task_runner: TaskRunner,
    catalog: SharedCatalog,
}

impl Database {
    pub fn with_defaults(path: impl AsRef<Path>) -> io::Result<Self> {
        let config = AxmosDBConfig {
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: Some(DEFAULT_CACHE_SIZE),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, path)?;
        Database::new(
            SharedPager::from(pager),
            DEFAULT_BTREE_MIN_KEYS as usize,
            DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE as usize,
            DEFAULT_POOL_SIZE as usize,
        )
    }
}

#[derive(Debug)]
pub(crate) struct Catalog {
    meta_table: PageId,
    meta_index: PageId,
    btree_min_keys: usize,
    btree_num_siblings_per_side: usize,
}

#[derive(Debug)]
pub(crate) struct SharedCatalog(Arc<Catalog>);

impl Clone for SharedCatalog {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Deref for SharedCatalog {
    type Target = Catalog;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Catalog> for SharedCatalog {
    fn from(value: Catalog) -> Self {
        Self(Arc::new(value))
    }
}

impl Catalog {
    pub(crate) fn new_uninit(min_keys: usize, siblings_per_side: usize) -> Self {
        Self {
            meta_table: PAGE_ZERO,
            meta_index: PAGE_ZERO,
            btree_min_keys: min_keys,
            btree_num_siblings_per_side: siblings_per_side,
        }
    }

    pub(crate) fn new_init(
        min_keys: usize,
        siblings_per_side: usize,
        accessor: RcPageAccessor,
    ) -> io::Result<Self> {
        let (meta_table_root, meta_index_root) = {
            let mut accessor = accessor.borrow_mut();
            (
                accessor.alloc_page::<BtreePage>()?,
                accessor.alloc_page::<BtreePage>()?,
            )
        };

        let meta_table = Self::create_meta_table(meta_table_root)?;
        let meta_idx = Self::create_meta_index(meta_index_root)?;

        let catalog = Catalog {
            meta_table: meta_table_root,
            meta_index: meta_index_root,
            btree_min_keys: min_keys,
            btree_num_siblings_per_side: siblings_per_side,
        };

        catalog.create_relation(Relation::TableRel(meta_table), accessor.clone())?;
        catalog.create_relation(Relation::IndexRel(meta_idx), accessor)?;

        Ok(catalog)
    }

    pub(crate) fn create_meta_table(root: PageId) -> io::Result<Table> {
        let meta_schema = meta_table_schema();
        let mut meta_table = Table::new(META_TABLE, root, meta_schema);
        let next = get_next_object();
        meta_table.set_next(next);

        Ok(meta_table)
    }

    pub(crate) fn create_meta_index(root: PageId) -> io::Result<Index> {
        let meta_schema = meta_idx_schema();
        Ok(Index::new(META_INDEX, root, meta_schema))
    }

    pub(crate) fn index_btree(
        &self,
        root: PageId,
        schema: &Schema,
        tx: RcPageAccessor,
    ) -> io::Result<BPlusTree<DynComparator>> {
        let comparator = schema.comparator();

        Ok(BPlusTree::from_existent(
            root,
            tx,
            self.btree_min_keys,
            self.btree_num_siblings_per_side,
            comparator,
        ))
    }

    pub(crate) fn table_btree(
        &self,
        root: PageId,
        tx: RcPageAccessor,
    ) -> io::Result<BPlusTree<NumericComparator>> {
        let comparator = NumericComparator::with_type::<u64>();

        Ok(BPlusTree::from_existent(
            root,
            tx,
            self.btree_min_keys,
            self.btree_num_siblings_per_side,
            comparator,
        ))
    }

    pub(crate) fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        accessor: RcPageAccessor,
    ) -> io::Result<ObjectId> {
        let root = accessor.borrow_mut().alloc_page::<BtreePage>()?;
        debug_assert!(
            root.is_valid(),
            "Cannot allocate a table pointing to page zero"
        );
        self.create_relation(
            Relation::TableRel(Table::new(table_name, root, schema)),
            accessor,
        )
    }

    pub(crate) fn create_index(
        &self,
        index_name: &str,
        schema: Schema,
        accessor: RcPageAccessor,
    ) -> io::Result<ObjectId> {
        let root = accessor.borrow_mut().alloc_page::<BtreePage>()?;
        debug_assert!(
            root.is_valid(),
            "Cannot allocate a table pointing to page zero"
        );
        self.create_relation(
            Relation::IndexRel(Index::new(index_name, root, schema)),
            accessor,
        )
    }

    /// Stores a new relation in the meta table using the provided RcPageAccessor.
    pub(crate) fn store_relation(
        &self,
        mut obj: Relation,
        accessor: RcPageAccessor,
    ) -> io::Result<ObjectId> {
        if !obj.is_allocated() {
            let id = accessor.borrow_mut().alloc_page::<BtreePage>()?;
            obj.set_root(id);
        };

        let mut btree = self.table_btree(self.meta_table, accessor.clone())?; // Increments the RcPageAccessor's ref count
        let obj_id = obj.id();
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_accessor_stack();

        // Will replace existing relation if it exists.
        btree.upsert(self.meta_table, tuple.as_ref())?;

        Ok(obj_id)
    }

    /// Updates the metadata of a given relation using the provided RcPageAccessor.
    pub(crate) fn update_relation(
        &self,
        obj: Relation,
        accessor: RcPageAccessor,
    ) -> io::Result<()> {
        let mut btree = self.table_btree(self.meta_table, accessor.clone())?;
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_accessor_stack();
        btree.update(self.meta_table, tuple.as_ref())?;
        Ok(())
    }

    /// Removes a given relation using the provided RcPageAccessor.
    /// Cascades the removal if required.
    pub(crate) fn remove_relation(
        &self,
        rel: Relation,
        cascade: bool,
        accessor: RcPageAccessor,
    ) -> io::Result<()> {
        if cascade {
            let schema = rel.schema();
            let dependants = schema.get_dependants();

            // Recursively remove object dependants
            for dep in dependants {
                let relation = self.get_relation(&dep, accessor.clone())?;
                self.remove_relation(relation, true, accessor.clone())?;
            }
        };

        let mut meta_table = self.table_btree(self.meta_table, accessor.clone())?;
        let mut meta_index = self.index_btree(self.meta_index, &meta_idx_schema(), accessor)?;

        let blob = Blob::from(rel.name());
        meta_index.clear_accessor_stack();
        meta_index.remove(self.meta_index, blob.as_ref())?;
        meta_table.clear_accessor_stack();
        meta_table.remove(self.meta_table, rel.id().as_ref())?;

        Ok(())
    }

    /// Stores a relation in the meta - index using the provided RcPageAccessor
    pub(crate) fn index_relation(
        &self,
        obj: &Relation,
        accessor: RcPageAccessor,
    ) -> io::Result<()> {
        let mut btree = self.index_btree(self.meta_index, &meta_idx_schema(), accessor.clone())?;

        let schema = meta_idx_schema();
        let tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(obj.name())),
                DataType::BigUInt(UInt64::from(obj.id())),
            ],
            &schema,
            TRANSACTION_ZERO, // PLaceholder. MUST DO THIS INSIDE A TRANSACTION
        )?;

        btree.clear_accessor_stack();
        let boxed: OwnedTuple = tuple.into();
        btree.insert(self.meta_index, boxed.as_ref())?;

        Ok(())
    }

    /// Stores a relation in the meta index and the meta table using the provided RcPageAccessor.
    pub(crate) fn create_relation(
        &self,
        relation: Relation,
        accessor: RcPageAccessor,
    ) -> io::Result<ObjectId> {
        let id = relation.id();
        self.index_relation(&relation, accessor.clone())?;
        self.store_relation(relation, accessor)?;
        Ok(id)
    }

    /// Checks if a given relation exists in the meta-index
    pub(crate) fn lookup_relation(
        &self,
        name: &str,
        accessor: RcPageAccessor,
    ) -> io::Result<SearchResult> {
        let meta_idx = self.index_btree(self.meta_index, &meta_idx_schema(), accessor)?;
        let blob = Blob::from(name);
        meta_idx.search_from_root(blob.as_ref(), FrameAccessMode::Read)
    }

    /// Obtains a relation from the meta table if it exists, using the provided RcPageAccessor.
    pub(crate) fn get_relation(
        &self,
        name: &str,
        accessor: RcPageAccessor,
    ) -> io::Result<Relation> {
        let meta_idx = self.index_btree(self.meta_index, &meta_idx_schema(), accessor.clone())?;

        let blob = Blob::from(name);
        let bytes: &[u8] = blob.as_ref();

        let result = meta_idx.search_from_root(bytes, FrameAccessMode::Read)?;

        let payload = match result {
            SearchResult::Found(_) => meta_idx.get_payload(result)?.ok_or_else(|| {
                IoError::new(ErrorKind::NotFound, "Object not found in meta table")
            })?,
            SearchResult::NotFound(_) => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        meta_idx.clear_accessor_stack();
        let schema = meta_idx_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;

        let id = match tuple.value(0)? {
            crate::types::DataTypeRef::BigUInt(v) => ObjectId::from(v.to_owned()),
            _ => {
                // DEVUELVE NULL. PARECE UN ERROR EN REINTERPRET CAST.

                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "Invalid ID type in meta index",
                ));
            }
        };
        // Go to the meta table to obtain therelation
        self.get_relation_unchecked(id, accessor)
    }

    /// Directly gets a relation from the meta table
    pub(crate) fn get_relation_unchecked(
        &self,
        id: ObjectId,
        accessor: RcPageAccessor,
    ) -> io::Result<Relation> {
        let meta_table = self.table_btree(self.meta_table, accessor.clone())?;
        let bytes: &[u8] = id.as_ref();
        let result = meta_table.search_from_root(id.as_ref(), FrameAccessMode::Read)?;
        let payload = match result {
            SearchResult::Found(_) => meta_table.get_payload(result)?.ok_or_else(|| {
                IoError::new(ErrorKind::NotFound, "Object not found in meta table")
            })?,
            SearchResult::NotFound(_) => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        meta_table.clear_accessor_stack();
        let schema = meta_table_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;

        Relation::try_from(tuple)
    }

    pub(crate) fn meta_table_root(&self) -> PageId {
        self.meta_table
    }

    pub(crate) fn meta_index_root(&self) -> PageId {
        self.meta_index
    }
}

impl Database {
    pub(crate) fn new(
        pager: SharedPager,
        min_keys: usize,
        siblings_per_side: usize,
        pool_size: usize,
    ) -> io::Result<Self> {
        initialize_atomics();

        let init_accessor = RcPageAccessor::new(pager.clone());
        let catalog = SharedCatalog::from(Catalog::new_init(
            min_keys,
            siblings_per_side,
            init_accessor,
        )?);

        let controller = TransactionCoordinator::new(pager.clone(), catalog.clone());
        let task_runner = TaskRunner::new(
            pool_size,
            pager.clone(),
            catalog.clone(),
            controller.clone(),
        );

        Ok(Self {
            pager,
            controller,
            task_runner,
            catalog,
        })
    }

    pub(crate) fn catalog(&self) -> SharedCatalog {
        self.catalog.clone()
    }

    pub(crate) fn coordinator(&self) -> TransactionCoordinator {
        self.controller.clone()
    }

    pub(crate) fn task_runner(&self) -> &TaskRunner {
        &self.task_runner
    }

    pub(crate) fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    fn meta_table(&self) -> PageId {
        self.catalog.meta_table
    }

    fn meta_index(&self) -> PageId {
        self.catalog.meta_index
    }

    pub(crate) fn run_transaction<F, T>(&self, f: F) -> TransactionResult<T>
    where
        F: FnOnce(&ExecutionContext) -> TransactionResult<T> + Send + 'static,
        T: Send + 'static,
    {
        use crate::sql::executor::context::ExecutionContext;

        let result = self.task_runner.run_with_result(move |ctx| {
            // Begin transaction
            let handle = ctx.coordinator().begin().box_err()?;

            // Create execution context
            let exec_ctx = ExecutionContext::new(
                ctx.accessor(),
                ctx.pager(),
                ctx.catalog().clone(),
                handle.snapshot(),
                handle.id(),
                handle.logger(),
            );

            // Execute the user's function
            match f(&exec_ctx) {
                Ok(value) => {
                    // Commit on success
                    handle.commit().box_err()?;
                    Ok(value)
                }
                Err(e) => {
                    // Abort on error
                    let _ = handle.abort();
                    Err(Box::new(e) as BoxError)
                }
            }
        })?;

        Ok(result)
    }

    /// Runs a read-only operation without transaction overhead.
    ///
    /// Use this for simple reads that don't need ACID guarantees.
    pub(crate) fn run_read<F, T>(&self, f: F) -> TaskResult<T>
    where
        F: FnOnce(&TaskContext) -> Result<T, BoxError> + Send + 'static,
        T: Send + 'static,
    {
        self.task_runner.run_with_result(f)
    }

    pub fn run_query(&self, sql: String) -> TransactionResult<Vec<Row>> {
        let results = self.task_runner.run_with_result(move |ctx| {
            // 1. Parse
            let lexer = Lexer::new(&sql);
            let mut parser = Parser::new(lexer);
            let stmt = parser.parse().box_err()?;

            // 2. Bind
            let mut binder = Binder::new(ctx.catalog(), ctx.accessor());
            let bound_stmt = binder.bind(&stmt).box_err()?;

            // 3. Optimize
            let catalog = ctx.catalog().clone();
            let accessor = ctx.accessor();
            let cost_model = AxmosCostModel::new(4096);
            let stats_provider = CatalogStatsProvider::new(catalog.clone(), accessor.clone());

            let mut optimizer = CascadesOptimizer::new(
                catalog.clone(),
                accessor.clone(),
                OptimizerConfig::default(),
                cost_model,
                stats_provider,
            );

            match optimizer.process(&bound_stmt).box_err()? {
                ProcessedStatement::DdlExecuted(result) => Ok(vec![result.to_row()]),
                ProcessedStatement::Plan(physical_plan) => {
                    // 4. Begin transaction and execute
                    let handle = ctx.coordinator().begin().box_err()?;

                    let exec_ctx = ExecutionContext::new(
                        ctx.accessor(),
                        ctx.pager(),
                        ctx.catalog().clone(),
                        handle.snapshot(),
                        handle.id(),
                        handle.logger(),
                    );

                    let builder = ExecutorBuilder::new(exec_ctx);
                    let mut executor = builder.build(&physical_plan).box_err()?;

                    executor.open().box_err()?;
                    let mut results = Vec::new();
                    while let Some(row) = executor.next().box_err()? {
                        results.push(row);
                    }
                    executor.close().box_err()?;

                    // Commit transaction
                    handle.commit().box_err()?;

                    Ok(results)
                }
            }
        })?;

        Ok(results)
    }

    /// Runs a SQL query and returns results as formatted strings.
    pub fn run_query_formatted(&self, sql: String) -> TransactionResult<String> {
        let rows = self.run_query(sql)?;

        if rows.is_empty() {
            return Ok("(0 rows)".to_string());
        }

        let mut output = String::new();
        for row in &rows {
            output.push_str(&format!("{}\n", row));
        }
        output.push_str(&format!("({} rows)", rows.len()));

        Ok(output)
    }
}
