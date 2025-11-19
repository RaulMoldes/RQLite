pub mod schema;
pub mod errors;

use std::collections::HashMap;

use crate::{
    database::schema::{Constraint, Index, Table},
    storage::tuple::{Tuple, TupleRef},
    structures::bplustree::NumericComparator,
};

use crate::io::pager::SharedPager;
use crate::structures::bplustree::{
    BPlusTree, DynComparator, FixedSizeBytesComparator, NodeAccessMode, SearchResult,
    VarlenComparator,
};

use crate::storage::page::BtreePage;

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
            Column::new_unindexed(DataTypeKind::BigUInt, "o_last_lsn", None),
        ]
        .as_ref(),
        1,
    )
}

pub struct Database {
    pager: SharedPager,
    meta_table: PageId,
    meta_index: PageId,
    btree_min_keys: usize,
    btree_num_siblings_per_side: usize,
}

impl Database {
    pub fn new(pager: SharedPager, min_keys: usize, siblings_per_side: usize) -> Self {
        Self {
            pager,
            meta_table: PAGE_ZERO,
            meta_index: PAGE_ZERO,
            btree_min_keys: min_keys,
            btree_num_siblings_per_side: siblings_per_side,
        }
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    pub fn init_meta_table(&self, root: PageId) -> Table {
        let meta_schema = meta_table_schema();
        let mut meta_table = Table::new(META_TABLE, root, meta_schema);
        let next = get_next_object();
        meta_table.set_next(next);
        meta_table
    }

    pub fn init_meta_index(&self, root: PageId) -> Index {
        let meta_schema = meta_idx_schema();
        Index::new(META_INDEX, root, meta_schema)
    }

    pub fn create_index(&mut self, index_name: &str, schema: Schema) -> std::io::Result<()> {
        let root_page = self.pager().write().alloc_page::<BtreePage>()?;
        let index = Index::new(index_name, root_page, schema);
        self.create_relation(Relation::IndexRel(index))
    }

    pub fn create_table(&mut self, table_name: &str, schema: Schema) -> std::io::Result<()> {
        let root_page = self.pager().write().alloc_page::<BtreePage>()?;
        let table = Table::new(table_name, root_page, schema);
        self.create_relation(Relation::TableRel(table))
    }

    pub fn create_relation(&mut self, relation: Relation) -> std::io::Result<()> {
        self.index(&relation)?;
        self.store_relation(relation)?;
        Ok(())
    }

    // Lazy initialization.
    pub fn init(&mut self) -> std::io::Result<()> {
        initialize_atomics();

        self.meta_table = self.pager.write().alloc_page::<BtreePage>()?;
        self.meta_index = self.pager.write().alloc_page::<BtreePage>()?;

        let meta_table = self.init_meta_table(self.meta_table);
        let meta_idx = self.init_meta_index(self.meta_index);

        self.create_relation(Relation::TableRel(meta_table))?;
        self.create_relation(Relation::IndexRel(meta_idx))?;

        Ok(())
    }

    pub fn index_btree(
        &self,
        root: PageId,
        dtype: DataTypeKind,
    ) -> std::io::Result<BPlusTree<DynComparator>> {
        let comparator = if dtype.is_numeric() {
            DynComparator::StrictNumeric(NumericComparator::for_size(dtype.size().unwrap()))
        } else if dtype.is_fixed_size() {
            DynComparator::FixedSizeBytes(FixedSizeBytesComparator::for_size(dtype.size().unwrap()))
        } else {
            DynComparator::Variable(VarlenComparator)
        };

        Ok(BPlusTree::from_existent(
            self.pager(),
            root,
            self.btree_min_keys,
            self.btree_num_siblings_per_side,
            comparator,
        ))
    }

    pub fn table_btree(&self, root: PageId) -> std::io::Result<BPlusTree<NumericComparator>> {
        let comparator = NumericComparator::with_type::<u64>();

        Ok(BPlusTree::from_existent(
            self.pager(),
            root,
            self.btree_min_keys,
            self.btree_num_siblings_per_side,
            comparator,
        ))
    }

    pub fn store_relation(&mut self, mut obj: Relation) -> std::io::Result<OId> {
        let oid = obj.id();

        if !obj.is_allocated() {
            obj.alloc(&mut self.pager)?;
        };

        let mut btree = self.table_btree(self.meta_table)?;
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_stack();
        // Will replace existing relation if it exists.
        btree.upsert(self.meta_table, &tuple)?;

        Ok(oid)
    }

    pub fn update_relation(&mut self, obj: Relation) -> std::io::Result<()> {
        let mut btree = self.table_btree(self.meta_table)?;
        let tuple = obj.into_boxed_tuple()?;
        btree.clear_stack();
        btree.update(self.meta_table, &tuple)?;
        Ok(())
    }

    pub fn remove_relation(&mut self, rel: Relation, cascade: bool) -> std::io::Result<()> {
        if cascade {
            let schema = rel.schema();
            let dependants = schema.get_dependants();

            // Recursively remove object dependants
            for dep in dependants {
                let relation = self.relation(&dep)?;
                self.remove_relation(relation, true)?;
            }
        };

        let mut meta_table = self.table_btree(self.meta_table)?;
        let mut meta_index = self.index_btree(self.meta_index, DataTypeKind::Text)?;

        let blob = Blob::from(rel.name());
        meta_index.clear_stack();
        meta_index.remove(self.meta_index, blob.as_ref())?;
        meta_table.clear_stack();
        meta_table.remove(self.meta_table, rel.id().as_ref())?;

        Ok(())
    }

    pub fn index(&mut self, obj: &Relation) -> std::io::Result<()> {
        let mut btree = self.index_btree(self.meta_index, DataTypeKind::Text)?;

        let schema = meta_idx_schema();
        let tuple = Tuple::from_data(
            &[
                DataType::Text(Blob::from(obj.name())),
                DataType::BigUInt(UInt64::from(obj.id())),
            ],
            &schema,
        )?;

        btree.clear_stack();
        btree.insert(self.meta_index, tuple.as_ref())?;

        Ok(())
    }

    pub fn lookup(&self, name: &str) -> std::io::Result<SearchResult> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text)?;
        let blob = Blob::from(name);
        meta_idx.search_from_root(blob.as_ref(), NodeAccessMode::Read)
    }

    pub fn relation(&self, name: &str) -> std::io::Result<Relation> {
        let meta_idx = self.index_btree(self.meta_index, DataTypeKind::Text)?;

        let blob = Blob::from(name);

        let result = meta_idx.search_from_root(blob.as_ref(), NodeAccessMode::Read)?;

        let payload = match result {
            SearchResult::Found(position) => meta_idx.get_content_from_result(result).unwrap(),
            SearchResult::NotFound(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        meta_idx.clear_stack();
        let schema = meta_idx_schema();
        let tuple = TupleRef::read(payload.as_ref(), &schema)?;
        println!("{tuple}");
        let id = match tuple.value(0)? {
            crate::types::DataTypeRef::BigUInt(v) => OId::from(v.to_owned()),
            _ => {
                // DEVUELVE NULL. PARECE UN ERROR EN REINTERPRET CAST.

                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid ID type in meta index",
                ));
            }
        };

        self.relation_direct(id)
    }

    pub fn relation_direct(&self, id: OId) -> std::io::Result<Relation> {
        let meta_table = self.table_btree(self.meta_table)?;
        let bytes: &[u8] = id.as_ref();

        let payload = match meta_table.search_from_root(id.as_ref(), NodeAccessMode::Read)? {
            SearchResult::Found(position) => meta_table
                .get_content_from_result(SearchResult::Found(position))
                .unwrap(),
            SearchResult::NotFound(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Object not found in meta table",
                ));
            }
        };

        meta_table.clear_stack();
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

    use crate::{AxmosDBConfig, IncrementalVaccum, ReadWriteVersion, TextEncoding};
    use serial_test::serial;
    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
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
