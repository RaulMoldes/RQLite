pub mod errors;
pub mod schema;

use std::{
    collections::HashMap,
    io::{self, Error as IoError, ErrorKind},
    cell::{Ref, RefMut}
};

use crate::{
    database::schema::{Constraint, Index, Table},
    io::pager::{Transaction, Worker},
    storage::tuple::{OwnedTuple, Tuple, TupleRef},
    structures::bplustree::NumericComparator,
};

use crate::io::frames::FrameAccessMode;
use crate::io::pager::SharedPager;
use crate::storage::page::BtreePage;
use crate::structures::bplustree::{
    BPlusTree, DynComparator, FixedSizeBytesComparator, SearchResult, VarlenComparator,
};

use crate::types::initialize_atomics;
use crate::types::{Blob, DataType, DataTypeKind, OId, PAGE_ZERO, PageId, UInt64, get_next_object};
use schema::{Column, Relation, Schema};

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
    main_worker: Worker,
    meta_table: PageId,
    meta_index: PageId,
    btree_min_keys: usize,
    btree_num_siblings_per_side: usize,
}

impl Database {
    pub fn new(pager: SharedPager, min_keys: usize, siblings_per_side: usize) -> Self {
        let main_worker = Worker::new(pager.clone());
        Self {
            pager,
            main_worker,
            meta_table: PAGE_ZERO,
            meta_index: PAGE_ZERO,
            btree_min_keys: min_keys,
            btree_num_siblings_per_side: siblings_per_side,
        }
    }

    pub fn spawn_worker(&self) -> Worker {
        Worker::new(self.pager())
    }

    pub fn main_worker(&self) -> Ref<'_, Transaction> {
        self.main_worker.borrow()
    }


    pub fn main_worker_mut(&self) -> RefMut<'_, Transaction> {
        self.main_worker.borrow_mut()
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    pub fn init_meta_table(&mut self, root: PageId) -> io::Result<Table> {
        let meta_schema = meta_table_schema();
        let mut meta_table = Table::new(META_TABLE, root, meta_schema);
        let next = get_next_object();
        meta_table.set_next(next);
        self.meta_table = root;
        Ok(meta_table)
    }

    pub fn init_meta_index(&mut self, root: PageId) -> io::Result<Index> {
        let meta_schema = meta_idx_schema();
        self.meta_index = root;
        Ok(Index::new(META_INDEX, root, meta_schema))
    }

    pub fn create_index(&mut self, index_name: &str, schema: Schema) -> io::Result<()> {
        let root_page = self.main_worker_mut().alloc_page::<BtreePage>()?;
        let index = Index::new(index_name, root_page, schema);
        self.create_relation(Relation::IndexRel(index))
    }

    pub fn create_table(&mut self, table_name: &str, schema: Schema) -> io::Result<()> {
        let root_page = self.main_worker_mut().alloc_page::<BtreePage>()?;
        let table = Table::new(table_name, root_page, schema);
        self.create_relation(Relation::TableRel(table))
    }

    pub fn create_relation(&mut self, relation: Relation) -> io::Result<()> {
        self.index(&relation)?;
        self.store_relation(relation)?;
        Ok(())
    }

    // Lazy initialization.
    pub fn init(&mut self) -> io::Result<()> {
        initialize_atomics();

        let (meta_tbl_root, meta_index_root) = {
                let mut worker = self.main_worker_mut();
                (worker.alloc_page::<BtreePage>()?, worker.alloc_page::<BtreePage>()?)
        };

        let meta_table = self.init_meta_table(meta_tbl_root)?;
        let meta_idx = self.init_meta_index(meta_index_root)?;



        self.create_relation(Relation::TableRel(meta_table))?;
        self.create_relation(Relation::IndexRel(meta_idx))?;

        Ok(())
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

    pub fn store_relation(&mut self, mut obj: Relation) -> io::Result<OId> {
        let oid = obj.id();

        if !obj.is_allocated() {
            let id = self.main_worker_mut().alloc_page::<BtreePage>()?;
            obj.set_root(id);
        };

        let mut btree = self.table_btree(self.meta_table, self.main_worker.clone())?; // Increments the worker's ref count

        let tuple = obj.into_boxed_tuple()?;
        self.main_worker_mut().clear_stack();


        // Will replace existing relation if it exists.
        btree.upsert(self.meta_table, tuple.as_ref())?;

        Ok(oid)
    }

    pub fn update_relation(&mut self, obj: Relation) -> io::Result<()> {
        let mut btree = self.table_btree(self.meta_table, self.main_worker.clone())?;
        let tuple = obj.into_boxed_tuple()?;
        self.main_worker_mut().clear_stack();
        btree.update(self.meta_table, tuple.as_ref())?;
        Ok(())
    }

    pub fn remove_relation(&mut self, rel: Relation, cascade: bool) -> io::Result<()> {
        if cascade {
            let schema = rel.schema();
            let dependants = schema.get_dependants();

            // Recursively remove object dependants
            for dep in dependants {
                let relation = self.relation(&dep)?;
                self.remove_relation(relation, true)?;
            }
        };

        let mut meta_table = self.table_btree(self.meta_table, self.main_worker.clone())?;
        let mut meta_index =
            self.index_btree(self.meta_index, DataTypeKind::Text, self.main_worker.clone())?;

        let blob = Blob::from(rel.name());
        self.main_worker_mut().clear_stack();
        meta_index.remove(self.meta_index, blob.as_ref())?;
        self.main_worker_mut().clear_stack();
        meta_table.remove(self.meta_table, rel.id().as_ref())?;

        Ok(())
    }

    pub fn index(&mut self, obj: &Relation) -> io::Result<()> {
        let mut btree = self.index_btree(self.meta_index, DataTypeKind::Text, self.main_worker.clone())?;

        let schema = meta_idx_schema();
        let tuple = Tuple::new(
            &[
                DataType::Text(Blob::from(obj.name())),
                DataType::BigUInt(UInt64::from(obj.id())),
            ],
            &schema,
        )?;

        self.main_worker_mut().clear_stack();
        let boxed: OwnedTuple = tuple.into();
        btree.insert(self.meta_index, boxed.as_ref())?;

        Ok(())
    }

    pub fn lookup(&self, name: &str) -> io::Result<SearchResult> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text, self.main_worker.clone())?;
        let blob = Blob::from(name);
        meta_idx.search_from_root(blob.as_ref(), FrameAccessMode::Read)
    }




    pub fn relation(&self, name: &str) -> io::Result<Relation> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text, self.main_worker.clone())?;

        let blob = Blob::from(name);

        let result = meta_idx.search_from_root(blob.as_ref(), FrameAccessMode::Read)?;

        let payload = match result {
            SearchResult::Found(_) => {
                meta_idx.get_payload(result)?.ok_or_else(|| {
                    IoError::new(
                        ErrorKind::NotFound,
                        "Object not found in meta table",
                    )
                })?
            }
            SearchResult::NotFound(_) => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        self.main_worker_mut().clear_stack();
        let schema = meta_idx_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;
        println!("{tuple}");
        let id = match tuple.value(0)? {
            crate::types::DataTypeRef::BigUInt(v) => OId::from(v.to_owned()),
            _ => {
                // DEVUELVE NULL. PARECE UN ERROR EN REINTERPRET CAST.

                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "Invalid ID type in meta index",
                ));
            }
        };

        self.relation_direct(id)
    }

    pub fn relation_direct(&self, id: OId) -> io::Result<Relation> {
        let meta_table = self.table_btree(self.meta_table, self.main_worker.clone())?;
        let bytes: &[u8] = id.as_ref();
        let result = meta_table.search_from_root(id.as_ref(), FrameAccessMode::Read)?;
        let payload = match  result {
            SearchResult::Found(_) =>  meta_table.get_payload(result)?.ok_or_else(|| {
                    IoError::new(
                        ErrorKind::NotFound,
                        "Object not found in meta table",
                    )
                })?,
            SearchResult::NotFound(_) => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        self.main_worker_mut().clear_stack();
        let schema = meta_table_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;
        println!("Read: {tuple}");
        Relation::try_from(tuple)
    }
}

#[cfg(test)]
mod db_tests {

    use super::*;
    use crate::database::{Database, schema::Schema};
    use crate::io::pager::{Pager, SharedPager};

    use crate::types::{DataTypeKind, PAGE_ZERO};

    use crate::{AxmosDBConfig, IncrementalVaccum,  TextEncoding};
    use serial_test::serial;
    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> io::Result<Database> {
        let config = AxmosDBConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        let mut db = Database::new(SharedPager::from(pager), 3, 2);
        db.init()?;

        // Create a users table
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, true);
        users_schema.add_column("created_at", DataTypeKind::DateTime, false, false, false);

        db.create_table("users", users_schema)?;

        let mut posts_schema = Schema::new();
        posts_schema.add_column("id", DataTypeKind::Int, true, true, false);
        posts_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        posts_schema.add_column("title", DataTypeKind::Text, false, false, false);
        posts_schema.add_column("content", DataTypeKind::Blob, false, false, true);
        posts_schema.add_column("published", DataTypeKind::Boolean, false, false, false);

        db.create_table("posts", posts_schema)?;

        Ok(db)
    }

    #[test]
    #[serial]
    fn test_db_init() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();

        // Verify meta table and index are created
        assert_ne!(db.meta_table, PAGE_ZERO);
        assert_ne!(db.meta_index, PAGE_ZERO);

        // Try to retrieve the meta catalog
        let meta_table_rel = db.relation(META_TABLE);
        assert!(meta_table_rel.is_ok(), "Should find meta table");

        let meta_index_rel = db.relation(META_INDEX);
        assert!(meta_index_rel.is_ok(), "Should find meta index");
    }
}
