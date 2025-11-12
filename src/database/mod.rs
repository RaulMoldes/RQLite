pub mod schema;
use crate::storage::{
    cell::Slot,
    tuple::{TupleRef, tuple}
};

use crate::io::pager::SharedPager;
use crate::structures::bplustree::{
    BPlusTree,  DynComparator, FixedSizeComparator, NodeAccessMode, SearchResult,
    VarlenComparator,
};

use schema::{Schema,  Column,  DBObject, ObjectType, AsBytes};
use crate::types::initialize_atomics;
use crate::types::{
   OId, Blob,
    UInt64, META_INDEX_ROOT, META_TABLE_ROOT, DataTypeKind, DataType, DataTypeRef
};



pub const META_TABLE: &str = "rqcatalog";
pub const META_INDEX: &str = "rqindex";


pub fn meta_idx_schema() -> Schema {
    Schema::from(
        [
            Column::new_unindexed(DataTypeKind::Text, "o_name", None),
            Column::new_unindexed(DataTypeKind::BigUInt, "o_id", None),
        ]
        .as_ref(),
    )
}
pub fn meta_table_schema() -> Schema {
    Schema::from(
        [
            Column::new_unindexed(DataTypeKind::BigUInt, "o_id", None),
            Column::new_unindexed(DataTypeKind::BigUInt, "o_root", None),
            Column::new_unindexed(DataTypeKind::Byte, "o_type", None),
            Column::new_unindexed(DataTypeKind::Blob, "o_metadata", None),
            Column::new(
                DataTypeKind::Text,
                "o_name",
                Some(META_INDEX.to_string()),
                None,
            ),
        ]
        .as_ref(),
    )
}

pub struct Database {
    pager: SharedPager,
}

impl Database {
    pub fn new(pager: SharedPager) -> Self {
        Self { pager }
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }
    // Lazy initialization.
    pub fn init(&mut self) -> std::io::Result<()> {
        initialize_atomics();
        let meta_schema = meta_table_schema();
        let schema_bytes = meta_schema.write_to()?;

        let mut meta_table_obj = DBObject::new(ObjectType::Table, META_TABLE, Some(&schema_bytes));

        meta_table_obj.alloc(&mut self.pager)?;

        let meta_idx_schema = meta_idx_schema();
        let schema_bytes = meta_idx_schema.write_to()?;
        let mut meta_idx_obj = DBObject::new(ObjectType::Index, META_INDEX, Some(&schema_bytes));

        meta_idx_obj.alloc(&mut self.pager)?;

        self.index_obj(&meta_table_obj)?;
        self.create_obj(meta_table_obj)?;

        self.index_obj(&meta_idx_obj)?;
        self.create_obj(meta_idx_obj)?;

        Ok(())
    }

    pub fn build_tree(&self, value: DBObject) -> std::io::Result<BPlusTree<DynComparator>> {
        let dtype = value.get_schema().columns[0].dtype;

        let comparator = if dtype.is_fixed_size() {
            DynComparator::Fixed(FixedSizeComparator::for_size(dtype.size().unwrap()))
        } else {
            DynComparator::Variable(VarlenComparator)
        };

        Ok(BPlusTree::from_existent(
            self.pager(),
            value.root(),
            3,
            3,
            comparator,
        ))
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> std::io::Result<()> {
        let schema_bytes = schema.write_to()?;
        let obj_creat = DBObject::new(ObjectType::Table, name, Some(&schema_bytes));
        self.index_obj(&obj_creat)?;
        self.create_obj(obj_creat)?;
        Ok(())
    }

    pub fn create_obj(&mut self, mut obj: DBObject) -> std::io::Result<OId> {
        let oid = obj.id();

        if !obj.is_allocated() {
            obj.alloc(&mut self.pager)?;
        };

        let mut meta_table = BPlusTree::from_existent(
            self.pager.clone(),
            META_TABLE_ROOT,
            3,
            2,
            FixedSizeComparator::with_type::<u64>(),
        );

        let tuple = obj.into_boxed_tuple()?;
        meta_table.clear_stack();
        meta_table.insert(META_TABLE_ROOT, &tuple)?;
        Ok(oid)
    }

    pub fn update_obj(&mut self, obj: DBObject) -> std::io::Result<OId> {
        let oid = obj.id();

        let mut meta_table = BPlusTree::from_existent(
            self.pager.clone(),
            META_TABLE_ROOT,
            3,
            2,
            FixedSizeComparator::with_type::<u64>(),
        );

        let tuple = obj.into_boxed_tuple()?;
        meta_table.clear_stack();
        meta_table.update(META_TABLE_ROOT, &tuple)?;
        Ok(oid)
    }

    pub fn remove_obj(&mut self, oid: OId, obj_name: &str, cascade: bool) -> std::io::Result<()> {
        if cascade {
            let obj = self.get_obj(oid)?;
            let schema = obj.get_schema();
            let dependants = schema.get_dependants();
            for dep in dependants {
                let oid = self.search_obj(&dep)?;
                self.remove_obj(oid, &dep, true)?;
            }
        };

        let mut meta_table = BPlusTree::from_existent(
            self.pager.clone(),
            META_TABLE_ROOT,
            3,
            2,
            FixedSizeComparator::with_type::<u64>(),
        );

        let mut meta_index =
            BPlusTree::from_existent(self.pager.clone(), META_INDEX_ROOT, 3, 2, VarlenComparator);

        meta_table.clear_stack();
        meta_index.clear_stack();
        let blob = Blob::from(obj_name);
        meta_index.remove(META_INDEX_ROOT, blob.as_ref())?;
        meta_table.remove(META_TABLE_ROOT, oid.as_ref())?;

        Ok(())
    }

    pub fn index_obj(&mut self, obj: &DBObject) -> std::io::Result<()> {
        let mut meta_idx =
            BPlusTree::from_existent(self.pager.clone(), META_INDEX_ROOT, 3, 2, VarlenComparator);

        let schema = meta_idx_schema();
        let tuple = tuple(
            &[
                DataType::Text(Blob::from(obj.name().as_str())),
                DataType::BigUInt(UInt64::from(obj.id())),
            ],
            &schema,
        )?;
        meta_idx.clear_stack();
        meta_idx.insert(META_INDEX_ROOT, &tuple)?;

        Ok(())
    }

    pub fn search_obj(&self, obj_name: &str) -> std::io::Result<OId> {
        let meta_idx =
            BPlusTree::from_existent(self.pager.clone(), META_INDEX_ROOT, 3, 2, VarlenComparator);
        let blob = Blob::from(obj_name);
        let result = meta_idx.search(
            &(META_INDEX_ROOT, Slot(0)),
            blob.as_ref(),
            NodeAccessMode::Read,
        )?;

        let payload = match result {
            SearchResult::Found(position) => meta_idx.get_content_from_result(result),
            SearchResult::NotFound(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Object not found in meta table",
                ))
            }
        };

        meta_idx.clear_stack();

        if let Some(p) = payload {
            let schema = meta_idx_schema();
            let tuple = TupleRef::read(p.as_ref(), &schema)?;

            if let DataTypeRef::BigUInt(u) = tuple.value(&schema, 1)? {
                return Ok(OId::from(u.to_owned()));
            };
        };

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Object not found in meta table",
        ))
    }

    pub fn get_obj(&self, obj_id: OId) -> std::io::Result<DBObject> {
        let meta_table = BPlusTree::from_existent(
            self.pager.clone(),
            META_TABLE_ROOT,
            3,
            2,
            FixedSizeComparator::with_type::<u64>(),
        );

        let result = meta_table.search(
            &(META_TABLE_ROOT, Slot(0)),
            obj_id.as_ref(),
            NodeAccessMode::Read,
        )?;

        let payload = match result {
            SearchResult::Found(position) => meta_table.get_content_from_result(result),
            SearchResult::NotFound(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Object not found in meta table",
                ))
            }
        };

        meta_table.clear_stack();

        if let Some(p) = payload {
            let schema = meta_table_schema();
            let tuple = TupleRef::read(p.as_ref(), &schema)?;

            Ok(tuple.try_into().unwrap())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Object not found in meta table",
            ))
        }
    }
}





#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::pager::Pager;
    use crate::{IncrementalVaccum, RQLiteConfig, ReadWriteVersion, TextEncoding};
    use schema::{Schema,  Column, Constraint,  Relation, Table, Index, DBObject, ObjectType, AsBytes};
    use serial_test::serial;
    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = RQLiteConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        let mut db = Database::new(SharedPager::from(pager));
        db.init()?;
        Ok(db)
    }

    #[test]
    #[serial]
    fn test_meta_table() -> std::io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 100, path)?;

        let oid = db.search_obj(META_TABLE)?;
        assert_ne!(oid, OId::from(0));
        let obj = db.get_obj(oid)?;
        assert_eq!(obj.root(), META_TABLE_ROOT);
        let (meta_schema, _) = Schema::read_from(obj.metadata().unwrap())?;
        assert_eq!(meta_schema, meta_table_schema());

        let oid = db.search_obj(META_INDEX)?;
        assert_ne!(oid, OId::from(0));
        let obj = db.get_obj(oid)?;
        assert_eq!(obj.root(), META_INDEX_ROOT);
        let (meta_schema, _) = Schema::read_from(obj.metadata().unwrap())?;
        assert_eq!(meta_schema, meta_idx_schema());
        Ok(())
    }

    #[test]
    #[serial]
    fn test_create_table() -> std::io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 100, path)?;
        let mut pk = Column::new_unindexed(DataTypeKind::BigUInt, "id", None);
        pk.add_constraint("primary".to_string(), Constraint::PrimaryKey);
        let schema = Schema::from(
            [
                pk.clone(),
                Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
                Column::new_unindexed(DataTypeKind::Text, "description", None),
            ]
            .as_ref(),
        );

        let schema_bytes = schema.write_to()?;
        let obj_creat = DBObject::new(ObjectType::Table, "table_a", Some(&schema_bytes));
        assert_eq!(obj_creat.metadata().unwrap(), schema_bytes);
        db.index_obj(&obj_creat)?;
        db.create_obj(obj_creat)?;

        let id = db.search_obj("table_a")?;
        let obj_ret = db.get_obj(id)?;
        assert_eq!(obj_ret.name(), "table_a");
        assert!(obj_ret.metadata().is_some());
        assert!(obj_ret.root().is_valid());
        assert_eq!(obj_ret.id(), id, "Objects do not match");
        assert_eq!(obj_ret.metadata().unwrap(), schema_bytes);
        let (des, _) = Schema::read_from(obj_ret.metadata().unwrap())?;

        assert_eq!(
            des,
            Schema::from(
                [
                    pk,
                    Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
                    Column::new_unindexed(DataTypeKind::Text, "description", None),
                ]
                .as_ref(),
            )
        );

        Ok(())
    }



    #[test]
    fn test_table_conversion() -> std::io::Result<()> {
        let schema = Schema::from([
            Column::new_unindexed(DataTypeKind::BigUInt, "id", None),
            Column::new_unindexed(DataTypeKind::Text, "name", None),
        ].as_ref());

        let table = Table::new(
            "test_table",
            schema);

        // Convert Table to db object
        let db_obj = DBObject::try_from(Relation::TableRel(table.clone()))?;

        assert_eq!(db_obj.object_type(), ObjectType::Table);
        assert_eq!(db_obj.name(), "test_table");
        assert!(db_obj.metadata().is_some());

        // Convert DBObject back to table
        let relation = Relation::try_from(db_obj)?;

        match relation {
            Relation::TableRel(converted_table) => {
                assert_eq!(converted_table.id(), table.id());
                assert_eq!(converted_table.name(), table.name());
                assert_eq!(converted_table.schema(), table.schema());
                assert_eq!(converted_table.next(), table.next());
            },
            _ => panic!("Expected TableRel, got IndexRel"),
        }

        Ok(())
    }



    #[test]
    fn test_index_conversion() -> std::io::Result<()> {
        let schema = Schema::from([
            Column::new_unindexed(DataTypeKind::BigUInt, "id", None),
        ].as_ref());

        let index = Index::new("test_index", schema, DataType::BigUInt(UInt64(100)), DataType::BigUInt(UInt64(999)));



        // Convert Index to [DBObject]
        let db_obj = DBObject::try_from(Relation::IndexRel(index.clone()))?;

        assert_eq!(db_obj.object_type(), ObjectType::Index);
        assert_eq!(db_obj.name(), "test_index");
        assert!(db_obj.metadata().is_some());

        // Convert DBObject back to [Index] and check fields.
        let relation = Relation::try_from(db_obj)?;

        match relation {
            Relation::IndexRel(converted_index) => {
                assert_eq!(converted_index.id(), index.id());
                assert_eq!(converted_index.name(), index.name());
                assert_eq!(converted_index.num_keys(), index.num_keys());
                assert_eq!(converted_index.range(), index.range());
                assert_eq!(converted_index.schema(), index.schema());
            },
            _ => panic!("Expected IndexRel, got TableRel"),
        }

        Ok(())
    }
}
