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
    DBConfig, TRANSACTION_ZERO,
    database::{
        errors::{BoxError, IntoBoxError, TaskResult, TransactionResult},
        runner::{TaskContext, TaskRunner},
        schema::{Index, Relation, Schema, Table},
        stats::CatalogStatsProvider,
    },
    io::{
        frames::FrameAccessMode,
        pager::{Pager, SharedPager},
    },
    schema,
    sql::{
        binder::Binder,
        executor::{Row, build::ExecutorBuilder, context::ExecutionContext},
        parser::Parser,
        planner::{CascadesOptimizer, OptimizerConfig, ProcessedStatement, model::AxmosCostModel},
    },
    storage::{
        page::BtreePage,
        tuple::{Tuple, TupleRef},
    },
    structures::{
        bplustree::{BPlusTree, SearchResult},
        builder::AsKeyBytes,
        comparator::{DynComparator, NumericComparator},
    },
    transactions::{TransactionCoordinator, accessor::RcPageAccessor},
    types::{
        Blob, DataType, DataTypeRef, ObjectId, PAGE_ZERO, PageId, UInt8, UInt64, get_next_object,
        initialize_atomics,
    },
};

use std::path::Path;

pub(crate) const META_TABLE: &str = "rqcatalog";
pub(crate) const META_INDEX: &str = "rqindex";

/// META TABLES AND META INDEXES ARE SPECIAL.
/// THIS IS WHY TO MANAGE THE PRIMARY KEY OF THIS TABLES WE USE THE OBJECT ID ATOMIC INSTEAD OF THE STANDARD ROW-ID. THIS ALLOWS FOR FASTER ACCESS, AND ENSURES THE IDENTIFIER OF EACH TABLE ON EACH OF THE METAS IS THE SAME.
pub(crate) fn meta_idx_schema() -> Schema {
    schema!(keys: 1,
        o_name: Text,
        row_id: BigUInt
    )
}

pub(crate) fn meta_table_schema() -> Schema {
    schema!(keys: 1,
        row_id: BigUInt,
        o_root: BigUInt,
        o_type: Byte,
        o_metadata: Blob,
        o_name: Text { index: META_INDEX, primary_key: "name_pkey" }
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
        let config = DBConfig::default();

        Database::new(config, path)
    }
}

#[derive(Debug)]
pub(crate) struct Catalog {
    meta_table: PageId,
    meta_index: PageId,
    meta_table_schema: Schema,
    meta_index_schema: Schema,
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
            meta_table_schema: meta_table_schema(),
            meta_index_schema: meta_idx_schema(),
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
            meta_table_schema: meta_table_schema(),
            meta_index_schema: meta_idx_schema(),
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

    pub(crate) fn meta_table_schema(&self) -> &Schema {
        &self.meta_table_schema
    }

    pub(crate) fn meta_index_schema(&self) -> &Schema {
        &self.meta_index_schema
    }

    /// Converts a relation into a tuple for storage.
    pub(crate) fn relation_as_tuple(&self, relation: &Relation) -> io::Result<Tuple<'_>> {
        let root_page = if relation.root() != PAGE_ZERO {
            DataType::BigUInt(UInt64::from(relation.root()))
        } else {
            DataType::Null
        };

        // Serialize metadata in-place
        let metadata = DataType::Blob(relation.metadata_as_blob()?);

        let t = Tuple::new(
            &[
                DataType::BigUInt(UInt64::from(relation.id())),
                root_page,
                DataType::Byte(UInt8::from(relation.object_type() as u8)),
                metadata,
                DataType::Text(Blob::from(relation.name())),
            ],
            self.meta_table_schema(),
            TRANSACTION_ZERO, // PLACEHOLDER. MUST DO THIS INSIDE A TRANSACTION IDEALLY
        )?;

        Ok(t)
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
        let tuple = self.relation_as_tuple(&obj)?;
        btree.clear_accessor_stack();

        // Will replace existing relation if it exists.
        btree.upsert(self.meta_table, tuple)?;

        Ok(obj_id)
    }

    /// Updates the metadata of a given relation using the provided RcPageAccessor.
    pub(crate) fn update_relation(
        &self,
        obj: Relation,
        accessor: RcPageAccessor,
    ) -> io::Result<()> {
        let mut btree = self.table_btree(self.meta_table, accessor.clone())?;
        let tuple = self.relation_as_tuple(&obj)?;
        btree.clear_accessor_stack();
        btree.update(self.meta_table, tuple)?;
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
        meta_index.remove(self.meta_index, blob.as_key_bytes())?;
        meta_table.clear_accessor_stack();
        meta_table.remove(self.meta_table, rel.id().as_key_bytes())?;

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

        btree.insert(self.meta_index, tuple)?;

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
        meta_idx.search_from_root(blob.as_key_bytes(), FrameAccessMode::Read)
    }

    /// Obtains a relation from the meta table if it exists, using the provided RcPageAccessor.
    pub(crate) fn get_relation(
        &self,
        name: &str,
        accessor: RcPageAccessor,
    ) -> io::Result<Relation> {
        let meta_idx = self.index_btree(self.meta_index, &meta_idx_schema(), accessor.clone())?;

        let blob = Blob::from(name);
        let bytes = blob.as_key_bytes();

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
            DataTypeRef::BigUInt(v) => ObjectId::from(v.to_owned()),
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
        let result = meta_table.search_from_root(id.as_key_bytes(), FrameAccessMode::Read)?;
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
    pub(crate) fn new(config: DBConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        initialize_atomics();
        let pager: SharedPager = SharedPager::from(Pager::from_config(config, path)?);

        let init_accessor = RcPageAccessor::new(pager.clone());
        let catalog = SharedCatalog::from(Catalog::new_init(
            config.min_keys_per_page as usize,
            config.num_siblings_per_side as usize,
            init_accessor,
        )?);

        let controller = TransactionCoordinator::new(pager.clone(), catalog.clone());
        let task_runner = TaskRunner::new(
            config.pool_size as usize,
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
            let mut parser = Parser::new(&sql);
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
