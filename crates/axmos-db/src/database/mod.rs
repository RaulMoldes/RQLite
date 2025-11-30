pub mod errors;
pub mod schema;

use std::{
    cell::{Ref, RefMut},
    collections::HashMap,
    io::{self, Error as IoError, ErrorKind},
    ops::Deref,
    sync::Arc,
};

use crate::{
    TRANSACTION_ZERO,
    database::schema::{Column, Constraint, Index, Relation, Schema, Table},
    io::{frames::FrameAccessMode, pager::SharedPager},
    storage::{
        page::BtreePage,
        tuple::{OwnedTuple, Tuple, TupleRef},
    },
    structures::bplustree::{
        BPlusTree, DynComparator, FixedSizeBytesComparator, NumericComparator, SearchResult,
        VarlenComparator,
    },
    transactions::{
        TransactionCoordinator,
        worker::{ThreadWorker, Worker},
    },
    types::{
        Blob, DataType, DataTypeKind, ObjectId, PAGE_ZERO, PageId, UInt64, get_next_object,
        initialize_atomics,
    },
};

pub const META_TABLE: &str = "rqcatalog";
pub const META_INDEX: &str = "rqindex";

pub fn meta_idx_schema() -> Schema {
    Schema::from_columns(
        [
            Column::new_unindexed(DataTypeKind::Text, "o_name", None),
            Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None),
        ]
        .as_ref(),
        1,
    )
}
pub fn meta_table_schema() -> Schema {
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
    main_worker: Worker,
    catalog: SharedCatalog,
}

#[derive(Debug)]
pub struct Catalog {
    meta_table: PageId,
    meta_index: PageId,
    btree_min_keys: usize,
    btree_num_siblings_per_side: usize,
}

#[derive(Debug)]
pub struct SharedCatalog(Arc<Catalog>);

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
    pub fn new_uninit(min_keys: usize, siblings_per_side: usize) -> Self {
        Self {
            meta_table: PAGE_ZERO,
            meta_index: PAGE_ZERO,
            btree_min_keys: min_keys,
            btree_num_siblings_per_side: siblings_per_side,
        }
    }

    pub fn new_init(min_keys: usize, siblings_per_side: usize, worker: Worker) -> io::Result<Self> {
        let (meta_table_root, meta_index_root) = {
            let mut worker = worker.borrow_mut();
            (
                worker.alloc_page::<BtreePage>()?,
                worker.alloc_page::<BtreePage>()?,
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

        catalog.create_relation(Relation::TableRel(meta_table), worker.clone())?;
        catalog.create_relation(Relation::IndexRel(meta_idx), worker)?;
        Ok(catalog)
    }

    pub fn create_meta_table(root: PageId) -> io::Result<Table> {
        let meta_schema = meta_table_schema();
        let mut meta_table = Table::new(META_TABLE, root, meta_schema);
        let next = get_next_object();
        meta_table.set_next(next);

        Ok(meta_table)
    }

    pub fn create_meta_index(root: PageId) -> io::Result<Index> {
        let meta_schema = meta_idx_schema();
        Ok(Index::new(META_INDEX, root, meta_schema))
    }

    pub fn index_btree(
        &self,
        root: PageId,
        dtype: DataTypeKind,
        tx: Worker,
    ) -> io::Result<BPlusTree<DynComparator>> {
        let comparator = if dtype.is_numeric() {
            DynComparator::StrictNumeric(NumericComparator::for_size(dtype.size().unwrap()))
        } else if dtype.is_fixed_size() {
            DynComparator::FixedSizeBytes(FixedSizeBytesComparator::for_size(dtype.size().unwrap()))
        } else {
            DynComparator::Variable(VarlenComparator)
        };

        Ok(BPlusTree::from_existent(
            root,
            tx,
            self.btree_min_keys,
            self.btree_num_siblings_per_side,
            comparator,
        ))
    }

    pub fn table_btree(
        &self,
        root: PageId,
        tx: Worker,
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

    pub fn create_table(&self, table_name: &str, schema: Schema, worker: Worker) -> io::Result<()> {
        let root = worker.borrow_mut().alloc_page::<BtreePage>()?;
        debug_assert!(
            root.is_valid(),
            "Cannot allocate a table pointing to page zero"
        );
        self.create_relation(
            Relation::TableRel(Table::new(table_name, root, schema)),
            worker,
        )
    }

    pub fn create_index(&self, index_name: &str, schema: Schema, worker: Worker) -> io::Result<()> {
        let root = worker.borrow_mut().alloc_page::<BtreePage>()?;
        debug_assert!(
            root.is_valid(),
            "Cannot allocate a table pointing to page zero"
        );
        self.create_relation(
            Relation::IndexRel(Index::new(index_name, root, schema)),
            worker,
        )
    }

    /// Stores a new relation in the meta table using the provided worker.
    pub fn store_relation(&self, mut obj: Relation, worker: Worker) -> io::Result<ObjectId> {
        if !obj.is_allocated() {
            let id = worker.borrow_mut().alloc_page::<BtreePage>()?;
            obj.set_root(id);
        };

        let mut btree = self.table_btree(self.meta_table, worker.clone())?; // Increments the worker's ref count
        let obj_id = obj.id();
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_worker_stack();

        // Will replace existing relation if it exists.
        btree.upsert(self.meta_table, tuple.as_ref())?;

        Ok(obj_id)
    }

    /// Updates the metadata of a given relation using the provided worker.
    pub fn update_relation(&self, obj: Relation, worker: Worker) -> io::Result<()> {
        let mut btree = self.table_btree(self.meta_table, worker.clone())?;
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_worker_stack();
        btree.update(self.meta_table, tuple.as_ref())?;
        Ok(())
    }

    /// Removes a given relation using the provided worker.
    /// Cascades the removal if required.
    pub fn remove_relation(&self, rel: Relation, cascade: bool, worker: Worker) -> io::Result<()> {
        if cascade {
            let schema = rel.schema();
            let dependants = schema.get_dependants();

            // Recursively remove object dependants
            for dep in dependants {
                let relation = self.get_relation(&dep, worker.clone())?;
                self.remove_relation(relation, true, worker.clone())?;
            }
        };

        let mut meta_table = self.table_btree(self.meta_table, worker.clone())?;
        let mut meta_index = self.index_btree(self.meta_index, DataTypeKind::Text, worker)?;

        let blob = Blob::from(rel.name());
        meta_index.clear_worker_stack();
        meta_index.remove(self.meta_index, blob.as_ref())?;
        meta_table.clear_worker_stack();
        meta_table.remove(self.meta_table, rel.id().as_ref())?;

        Ok(())
    }

    /// Stores a relation in the meta - index using the provided Worker
    pub fn index_relation(&self, obj: &Relation, worker: Worker) -> io::Result<()> {
        let mut btree = self.index_btree(self.meta_index, DataTypeKind::Text, worker.clone())?;

        let schema = meta_idx_schema();
        let tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(obj.name())),
                DataType::BigUInt(UInt64::from(obj.id())),
            ],
            &schema,
            TRANSACTION_ZERO, // PLaceholder. MUST DO THIS INSIDE A TRANSACTION
        )?;

        btree.clear_worker_stack();
        let boxed: OwnedTuple = tuple.into();
        btree.insert(self.meta_index, boxed.as_ref())?;

        Ok(())
    }

    /// Stores a relation in the meta index and the meta table using the provided worker.
    pub fn create_relation(&self, relation: Relation, worker: Worker) -> io::Result<()> {
        self.index_relation(&relation, worker.clone())?;
        self.store_relation(relation, worker)?;
        Ok(())
    }

    /// Checks if a given relation exists in the meta-index
    pub fn lookup_relation(&self, name: &str, worker: Worker) -> io::Result<SearchResult> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text, worker)?;
        let blob = Blob::from(name);
        meta_idx.search_from_root(blob.as_ref(), FrameAccessMode::Read)
    }

    /// Obtains a relation from the meta table if it exists, using the provided worker.
    pub fn get_relation(&self, name: &str, worker: Worker) -> io::Result<Relation> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text, worker.clone())?;

        let blob = Blob::from(name);

        let result = meta_idx.search_from_root(blob.as_ref(), FrameAccessMode::Read)?;

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

        meta_idx.clear_worker_stack();
        let schema = meta_idx_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;
        println!("{tuple}");
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
        self.get_relation_unchecked(id, worker)
    }

    /// Directly gets a relation from the meta table
    pub fn get_relation_unchecked(&self, id: ObjectId, worker: Worker) -> io::Result<Relation> {
        let meta_table = self.table_btree(self.meta_table, worker.clone())?;
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

        meta_table.clear_worker_stack();
        let schema = meta_table_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;
        //  println!("Read: {tuple}");
        Relation::try_from(tuple)
    }
}

impl Database {
    pub fn new(pager: SharedPager, min_keys: usize, siblings_per_side: usize) -> io::Result<Self> {
        initialize_atomics();

        let main_worker = Worker::new(pager.clone());
        let catalog = SharedCatalog::from(Catalog::new_init(
            min_keys,
            siblings_per_side,
            main_worker.clone(),
        )?);
        let controller = TransactionCoordinator::new(pager.clone(), catalog.clone());

        Ok(Self {
            pager,
            controller,
            main_worker,
            catalog,
        })
    }

    pub fn main_worker_ref(&self) -> Ref<'_, ThreadWorker> {
        self.main_worker.borrow()
    }

    pub fn catalog(&self) -> SharedCatalog {
        self.catalog.clone()
    }

    pub fn main_worker_ref_mut(&self) -> RefMut<'_, ThreadWorker> {
        self.main_worker.borrow_mut()
    }

    pub fn main_worker_cloned(&self) -> Worker {
        self.main_worker.clone()
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    fn meta_table(&self) -> PageId {
        self.catalog.meta_table
    }

    fn meta_index(&self) -> PageId {
        self.catalog.meta_index
    }
}
