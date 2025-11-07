use crate::io::pager::SharedPager;
use crate::storage::{
    cell::Slot,
    page::BtreePage,
    tuple::{tuple,  Tuple, TupleRef}
};

use crate::structures::bplustree::{BPlusTree, FixedSizeComparator,NodeAccessMode, VarlenComparator,  SearchResult};
use crate::types::{Blob, DataType, DataTypeRef, OId, PageId, UInt64, UInt8, VarInt, DataTypeKind, Key, META_INDEX_ROOT, META_TABLE_ROOT, PAGE_ZERO, varint::MAX_VARINT_LEN,
reinterpret_cast
};

use crate::{TextEncoding, repr_enum};

pub const META_TABLE: &str = "rqcatalog";
pub const META_INDEX: &str = "rqindex";



#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Schema {
    pub(crate) columns: Vec<Column>,
}

pub trait AsBytes {
    fn write_to(&self) -> std::io::Result<Vec<u8>>;
    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)>
    where Self: Sized;

}

impl AsBytes for Schema {
    fn write_to(&self) -> std::io::Result<Vec<u8>> {
        let mut out_buffer = Vec::new();
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        let buffer = VarInt::encode(self.columns.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);


        for column in &self.columns {
            let column_bytes = column.write_to()?;
            let buffer = VarInt::encode(column_bytes.len() as i64, &mut varint_buf);
            out_buffer.extend_from_slice(buffer);
            out_buffer.extend_from_slice(&column_bytes);
        }

        Ok(out_buffer)
    }

    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)> {
        let (column_count, mut cursor) = VarInt::from_encoded_bytes(bytes)?;
        let column_count_usize: usize = column_count.try_into()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut columns = Vec::with_capacity(column_count_usize);

        for _ in 0..column_count_usize {

            let (column_len, column_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let column_len_usize: usize = column_len.try_into()?;
            cursor += column_len_offset;

            if cursor + column_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for column",
                ));
            }


            let (column, _) = Column::read_from(&bytes[cursor..cursor + column_len_usize])?;
            cursor += column_len_usize;

            columns.push(column);
        }

        Ok((Schema { columns }, cursor))
    }
}



impl Schema {
    fn alignment(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.dtype.alignment())
            .max()
            .unwrap_or(1)
    }
}

impl From<&[Column]> for Schema {
    fn from(value: &[Column]) -> Self {
        Self {
            columns: value.to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub(crate) dtype: DataTypeKind,
    pub(crate) name: String,
    is_primary_key: bool,
    non_null: bool,
    unique: bool,
    foreign_key: Option<ForeignKey>,
    default: Option<DataType>
}

impl Column {
    pub fn new(dtype: DataTypeKind, name: &str, is_primary_key: bool, non_null: bool, unique: bool) -> Self {

        Self {
            dtype,
            name: name.to_string(),
            is_primary_key,
            non_null,
            unique,
            foreign_key: None,
            default: None
        }
    }

    pub fn is_not_null(&self) -> bool {
        self.non_null
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary_key
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn default(&self) -> Option<&DataType> {
        self.default.as_ref()
    }

     pub fn foreign_key(&self) -> Option<&ForeignKey> {
        self.foreign_key.as_ref()
    }

    pub fn set_unique(&mut self, unique: bool) {
        self.unique = unique;
    }

    pub fn set_default(&mut self, default: Option<DataType>) {
        self.default = default;
    }

    pub fn set_primary(&mut self, primary: bool) {
        self.is_primary_key = primary;
    }


    pub fn set_foreign_key(&mut self, fk: Option<ForeignKey>) {
        self.foreign_key = fk;
    }

    pub fn set_non_null(&mut self, non_null: bool) {
        self.non_null = non_null;
    }

    pub fn rename(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_datatype(&mut self, datatype: DataTypeKind) {
        debug_assert!(self.dtype.can_cast_to(datatype), "Error: cannot cast between types: {}, and {}", self.dtype, datatype);
        self.dtype = datatype;
    }
}




impl AsBytes for Column {
    fn write_to(&self) -> std::io::Result<Vec<u8>> {
        let mut out_buffer = Vec::new();
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        out_buffer.push(self.dtype as u8);


        let name_bytes = self.name.as_bytes();
        let buffer = VarInt::encode(name_bytes.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);
        out_buffer.extend_from_slice(name_bytes);
        out_buffer.push(self.is_primary_key as u8);
        out_buffer.push(self.non_null as u8);
        out_buffer.push(self.unique as u8);

        match &self.foreign_key {
            Some(fk) => {
                out_buffer.push(1u8);

                let table_bytes = fk.table().as_bytes();
                let buffer = VarInt::encode(table_bytes.len() as i64, &mut varint_buf);
                out_buffer.extend_from_slice(buffer);
                out_buffer.extend_from_slice(table_bytes);


                let column_bytes = fk.column().as_bytes();
                let buffer = VarInt::encode(column_bytes.len() as i64, &mut varint_buf);
                out_buffer.extend_from_slice(buffer);
                out_buffer.extend_from_slice(column_bytes);
            }
            None => {
                out_buffer.push(0u8);
            }
        }


        match &self.default {
            Some(default_val) => {
                out_buffer.push(1u8);
                out_buffer.extend_from_slice(default_val.as_ref());
            }
            None => {
                out_buffer.push(0u8);
            }
        }

        Ok(out_buffer)
    }

    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)> {
        let mut cursor = 0;


        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for dtype",
            ));
        }
        let dtype = DataTypeKind::from_repr(bytes[cursor])?;
        cursor += 1;


        let (name_len, name_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
        let name_len_usize: usize = name_len.try_into()?;
        cursor += name_len_offset;

        if cursor + name_len_usize > bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for column name",
            ));
        }

        let name = String::from_utf8(bytes[cursor..cursor + name_len_usize].to_vec())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        cursor += name_len_usize;

        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for is_primary_key",
            ));
        }
        let is_primary_key = bytes[cursor] != 0;
        cursor += 1;


        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for non_null",
            ));
        }
        let non_null = bytes[cursor] != 0;
        cursor += 1;


        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for unique",
            ));
        }
        let unique = bytes[cursor] != 0;
        cursor += 1;


        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for foreign_key flag",
            ));
        }
        let foreign_key = if bytes[cursor] != 0 {
            cursor += 1;


            let (table_len, table_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let table_len_usize: usize = table_len.try_into()?;
            cursor += table_len_offset;

            if cursor + table_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for foreign key table",
                ));
            }

            let table = String::from_utf8(bytes[cursor..cursor + table_len_usize].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += table_len_usize;


            let (column_len, column_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let column_len_usize: usize = column_len.try_into()?;
            cursor += column_len_offset;

            if cursor + column_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for foreign key column",
                ));
            }

            let column = String::from_utf8(bytes[cursor..cursor + column_len_usize].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += column_len_usize;

            Some(ForeignKey { ref_table: table, ref_col: column })
        } else {
            cursor += 1;
            None
        };

  
        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for default flag",
            ));
        }
        let default = if bytes[cursor] != 0 {
            cursor += 1;


            let (default_val, bytes_read) = reinterpret_cast(dtype, bytes)?;
            cursor += bytes_read;

            Some(default_val.to_owned())
        } else {
            cursor += 1;
            None
        };

        Ok((
            Column {
                dtype,
                name,
                is_primary_key,
                non_null,
                unique,
                foreign_key,
                default,
            },
            cursor,
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    ref_table: String,
    ref_col: String
}

impl ForeignKey {
    pub fn table(&self) -> &str {
        &self.ref_table
    }

    pub fn column(&self) -> &str {
        &self.ref_col
    }
}

pub fn meta_idx_schema() -> Schema {
    Schema::from(
        [
            Column::new(DataTypeKind::Text, "o_name", false, true, true),
            Column::new(DataTypeKind::BigUInt, "o_id", false, true, true),
        ]
        .as_ref(),
    )
}
pub fn meta_table_schema() -> Schema {
    Schema::from(
        [
            Column::new(DataTypeKind::BigUInt, "o_id", true, true, true),
            Column::new(DataTypeKind::BigUInt, "o_root",false, true, true),
            Column::new(DataTypeKind::Byte, "o_type", false, true, false),
            Column::new(DataTypeKind::Blob, "o_metadata", false, false, false),
            Column::new(DataTypeKind::Text, "o_name", false, true, true),
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

    // Lazy initialization.
    fn init(&mut self) -> std::io::Result<()> {
        let meta_schema = meta_table_schema();
        let schema_bytes = meta_schema.write_to()?;
        let mut meta_table_obj = DBObject::new(ObjectType::Table, META_TABLE, Some(&schema_bytes));
        if !meta_table_obj.is_allocated() {
            meta_table_obj.alloc(&mut self.pager)?;
        };

        let meta_idx_schema = meta_idx_schema();
        let schema_bytes = meta_idx_schema.write_to()?;
        let mut meta_idx_obj = DBObject::new(ObjectType::Index, META_INDEX, Some(&schema_bytes));
        if !meta_idx_obj.is_allocated() {
            meta_idx_obj.alloc(&mut self.pager)?;
        };

        self.index_obj(&meta_table_obj)?;
        self.create_obj(meta_table_obj)?;

        self.index_obj(&meta_idx_obj)?;
        self.create_obj(meta_idx_obj)?;

        Ok(())
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

    fn index_obj(&mut self, obj: &DBObject) -> std::io::Result<()> {
        let mut meta_idx =
            BPlusTree::from_existent(self.pager.clone(), META_INDEX_ROOT, 3, 2, VarlenComparator);

        let schema = meta_idx_schema();
        let tuple = tuple(
            &[
                DataType::Text(Blob::from(obj.name.as_str())),
                DataType::BigUInt(UInt64::from(obj.o_id)),
            ],
            &schema,
        )?;

        meta_idx.insert(META_INDEX_ROOT, &tuple)?;

        Ok(())
    }

    fn search_obj(&self, obj_name: &str) -> std::io::Result<OId> {
        let mut meta_idx =
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
        let db = create_db(4096, 100, "test.db")?;

        let oid = db.search_obj(META_TABLE)?;
        assert_ne!(oid, OId::from(0));
        let obj = db.get_obj(oid)?;
        assert_eq!(obj.root, META_TABLE_ROOT);
        let (meta_schema, _) = Schema::read_from(obj.metadata().unwrap())?;
        assert_eq!(meta_schema, meta_table_schema());

        let oid = db.search_obj(META_INDEX)?;
        assert_ne!(oid, OId::from(0));
        let obj = db.get_obj(oid)?;
        assert_eq!(obj.root, META_INDEX_ROOT);
        let (meta_schema, _) = Schema::read_from(obj.metadata().unwrap())?;
        assert_eq!(meta_schema, meta_idx_schema());
        Ok(())
    }

    #[test]
    #[serial]
    fn test_create_table() -> std::io::Result<()> {
        let mut db = create_db(4096, 100, "test.db")?;
        let schema = Schema::from(
            [
                Column::new(DataTypeKind::BigUInt, "id", true, true, true),
                Column::new(DataTypeKind::BigUInt, "age", false, false, false),
                Column::new(DataTypeKind::Text, "description",false, false, false),
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
        assert_eq!(obj_ret.name, "table_a");
        assert!(obj_ret.metadata().is_some());
        assert!(obj_ret.root.is_valid());
        assert_eq!(obj_ret.o_id, id, "Objects do not match");
        assert_eq!(obj_ret.metadata().unwrap(), schema_bytes);
        let (des, _) = Schema::read_from(obj_ret.metadata().unwrap())?;

        assert_eq!(
            des,
            Schema::from(
                [
                    Column::new(DataTypeKind::BigUInt, "id", true, true, true),
                    Column::new(DataTypeKind::BigUInt, "age",false, false, false),
                    Column::new(DataTypeKind::Text, "description",false, false, false),
                ]
                .as_ref(),
            )
        );
        Ok(())
    }
}
