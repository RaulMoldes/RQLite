use crate::io::pager::SharedPager;
use crate::repr_enum;
use crate::storage::cell::Slot;
use crate::storage::page::BtreePage;
use crate::storage::tuple::{ColumnDef,  Schema, Tuple, TupleRef, tuple};
use crate::structures::bplustree::NodeAccessMode;
use crate::structures::bplustree::{BPlusTree, FixedSizeComparator, Payload, SearchResult};
use crate::types::{Blob, DataType, DataTypeRef, OId, PageId, UInt64, UInt8};
use crate::types::{DataTypeKind, Key, META_TABLE_ROOT, PAGE_ZERO};
use crate::TextEncoding;
pub const META_TABLE: &str = "rqcatalog";

pub fn meta_table_schema() -> Schema {
    Schema::from(
        [
            ColumnDef::new(DataTypeKind::BigUInt, "o_id"),
            ColumnDef::new(DataTypeKind::BigUInt, "o_root"),
            ColumnDef::new(DataTypeKind::Byte, "o_type"),
            ColumnDef::new(DataTypeKind::Blob, "o_metadata"),
            ColumnDef::new(DataTypeKind::Text, "o_name"),
        ]
        .as_ref(),
    )
}

repr_enum!(
    enum ObjectType: u8 {
        Table = 0,
        Index = 1,
        Column = 2,
        Constraint = 3,
        View = 4,
    }
);

#[derive(Debug, PartialEq)]
struct DBObject {
    o_id: OId,
    root: PageId,
    o_type: ObjectType,
    name: String,
    metadata: Option<Box<[u8]>>,
}

impl<'a> TryFrom<TupleRef<'a>> for DBObject {
    type Error = std::io::Error;

    fn try_from(tuple: TupleRef<'a>) -> Result<Self, Self::Error> {
        let schema = meta_table_schema();

        if tuple.num_fields() != schema.columns.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid tuple: expected {} columns, got {}",
                    schema.columns.len(),
                    tuple.num_fields()
                ),
            ));
        }

        let o_id = match tuple.value(&schema, 0)? {
            DataTypeRef::BigUInt(v) => OId::from(v.to_owned()),
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_id cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_id",
                ))
            }
        };

        let root = match tuple.value(&schema, 1)? {
            DataTypeRef::BigUInt(v) => PageId::from(v.to_owned()),
            DataTypeRef::Null => PAGE_ZERO,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_root",
                ))
            }
        };

        let o_type = match tuple.value(&schema, 2)? {
            DataTypeRef::Byte(v) => {
                let type_val = v.to_owned();
                ObjectType::try_from(type_val)?
            }
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_type cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_type",
                ))
            }
        };

        let metadata = match tuple.value(&schema, 3)? {
            DataTypeRef::Blob(v) => Some(v.data().to_vec().into_boxed_slice()),
            DataTypeRef::Null => None,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for metadata",
                ))
            }
        };

        let name = match tuple.value(&schema, 4)? {
            DataTypeRef::Text(blob) => blob.as_str(TextEncoding::Utf8).to_string(),
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_name cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_name",
                ))
            }
        };

        Ok(DBObject {
            o_id,
            root,
            o_type,
            name,
            metadata,
        })
    }
}

impl<'a> TryFrom<Tuple<'a>> for DBObject {
    type Error = std::io::Error;

    fn try_from(tuple: Tuple<'a>) -> Result<Self, Self::Error> {
        let schema = meta_table_schema();

        if tuple.num_fields() != schema.columns.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid tuple: expected {} columns, got {}",
                    schema.columns.len(),
                    tuple.num_fields()
                ),
            ));
        }

        let o_id = match tuple.value(&schema, 0)? {
            DataTypeRef::BigUInt(v) => OId::from(v.to_owned()),
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_id cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_id",
                ))
            }
        };

        let root = match tuple.value(&schema, 1)? {
            DataTypeRef::BigUInt(v) => PageId::from(v.to_owned()),
            DataTypeRef::Null => PAGE_ZERO,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_root",
                ))
            }
        };

        let o_type = match tuple.value(&schema, 2)? {
            DataTypeRef::Byte(v) => {
                let type_val = v.to_owned();
                ObjectType::try_from(type_val)?
            }
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_type cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_type",
                ))
            }
        };

        let metadata = match tuple.value(&schema, 3)? {
            DataTypeRef::Blob(v) => Some(v.data().to_vec().into_boxed_slice()),
            DataTypeRef::Null => None,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for metadata",
                ))
            }
        };

        let name = match tuple.value(&schema, 4)? {
            DataTypeRef::Text(blob) => blob.as_str(TextEncoding::Utf8).to_string(),
            DataTypeRef::Null => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "o_name cannot be NULL",
                ))
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid type for o_name",
                ))
            }
        };

        Ok(DBObject {
            o_id,
            root,
            o_type,
            name,
            metadata,
        })
    }
}

impl TryFrom<UInt8> for ObjectType {
    type Error = std::io::Error;

    fn try_from(value: UInt8) -> Result<Self, Self::Error> {
        match value.0 {
            0 => Ok(ObjectType::Table),
            1 => Ok(ObjectType::Index),
            2 => Ok(ObjectType::Column),
            3 => Ok(ObjectType::Constraint),
            4 => Ok(ObjectType::View),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid ObjectType value: {}", value.0),
            )),
        }
    }
}

impl DBObject {
    fn new(o_type: ObjectType, name: &str, metadata: Option<&[u8]>) -> Self {
        Self {
            o_id: OId::new_key(),
            root: PAGE_ZERO,
            o_type,
            name: name.to_string(),
            metadata: metadata.map(|v| v.to_vec().into_boxed_slice()),
        }
    }

    fn metadata(&self) -> Option<&[u8]> {
        if let Some(meta) = &self.metadata {
            Some(meta.as_ref())
        } else {
            None
        }
    }

    fn is_allocated(&self) -> bool {
        !matches!(self.o_type, ObjectType::Table | ObjectType::Index) || self.root.is_valid()
    }

    fn alloc(&mut self, pager: &mut SharedPager) -> std::io::Result<()> {
        self.root = if matches!(self.o_type, ObjectType::Index | ObjectType::Table) {
            pager.write().alloc_page::<BtreePage>()?
        } else {
            PAGE_ZERO
        };
        Ok(())
    }

    fn into_boxed_tuple(self) -> std::io::Result<Box<[u8]>> {
        let root_page = if self.root != PAGE_ZERO {
            DataType::BigUInt(UInt64::from(self.root))
        } else {
            DataType::Null
        };

        let parent = if let Some(obj) = self.metadata {
            DataType::Blob(Blob::from(obj.as_ref()))
        } else {
            DataType::Null
        };





        let schema = meta_table_schema();
        tuple(
            &[
                DataType::BigUInt(UInt64::from(self.o_id)),
                root_page,
                DataType::Byte(UInt8::from(self.o_type as u8)),
                parent,
                DataType::Text(Blob::from(self.name.as_str())),
            ],
            &schema,
        )
    }
}





struct Database {
    pager: SharedPager,
}

impl Database {
    fn new(pager: SharedPager) -> Self {
        Self { pager }
    }

    fn create_obj(&mut self, mut obj: DBObject) -> std::io::Result<OId> {
        let oid = obj.o_id;

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

        meta_table.insert(META_TABLE_ROOT, &tuple)?;

        Ok(oid)
    }

    fn get_obj(&self, obj_id: OId) -> std::io::Result<DBObject> {
        let mut meta_table = BPlusTree::from_existent(
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

        std::fs::write("meta_table.json", meta_table.json()?)?;

        let payload = match result {
            SearchResult::Found(position) => meta_table.get_content_from_result(result),
            SearchResult::NotFound(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Object not found in meta table",
                ))
            }
        };



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
    use std::path::Path;

    fn create_db(page_size: u32, capacity: u16, path: impl AsRef<Path>) -> Database {
        let config = RQLiteConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
        };

        let mut pager = Pager::from_config(config, &path).unwrap();
        let id = pager.alloc_page::<BtreePage>().unwrap(); // Allocate the meta table
        debug_assert_eq!(id, META_TABLE_ROOT);
        Database::new(SharedPager::from(pager))
    }

    #[test]
    fn test_create_table() -> std::io::Result<()> {
        let mut db = create_db(4096, 100, "test.db");
        let schema =  Schema::from(
        [
            ColumnDef::new(DataTypeKind::BigUInt, "id"),
            ColumnDef::new(DataTypeKind::BigUInt, "age"),
            ColumnDef::new(DataTypeKind::Text, "description"),
        ]
        .as_ref(),
    );


        let schema_bytes = schema.write_to()?;
        let obj_creat = DBObject::new(ObjectType::Table, "table_a", Some(&schema_bytes));
        assert_eq!(obj_creat.metadata().unwrap(), schema_bytes);
        let id = db.create_obj(obj_creat)?;
        let obj_ret = db.get_obj(id)?;
        assert_eq!(obj_ret.name, "table_a");
        assert!(obj_ret.metadata().is_some());
        assert!(obj_ret.root.is_valid());
        assert_eq!(obj_ret.o_id, id, "Objects do not match");
        assert_eq!(obj_ret.metadata().unwrap(), schema_bytes);
        let des = Schema::read_from(obj_ret.metadata().unwrap())?;

        assert_eq!(des, Schema::from(
        [
            ColumnDef::new(DataTypeKind::BigUInt, "id"),
            ColumnDef::new(DataTypeKind::BigUInt, "age"),
            ColumnDef::new(DataTypeKind::Text, "description"),
        ]
        .as_ref(),
    ));
        Ok(())
    }
}
