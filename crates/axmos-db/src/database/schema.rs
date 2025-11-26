use crate::storage::{
    tuple::{OwnedTuple, Tuple, TupleRef},
};
use crate::structures::bplustree::Comparator;
use crate::types::{
    Blob, DataType, DataTypeKind, DataTypeRef, OId, PAGE_ZERO, PageId, UInt8, UInt64, VarInt,
    varint::MAX_VARINT_LEN,
};
use std::io::{Read, Seek, Write};

use super::meta_table_schema;
use crate::io::{
    AsBytes, read_string_unchecked, read_type_from_buf, read_variable_length,
    write_string_unchecked,
};
use crate::{TextEncoding, repr_enum};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Schema {
    pub(crate) columns: Vec<Column>,
    pub(crate) num_keys: u8,
    pub(crate) constraints: HashMap<String, TableConstraint>,
    column_index: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TableConstraint {
    PrimaryKey(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    Unique(Vec<String>),
}

impl Schema {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            num_keys: 0,
            constraints: HashMap::new(),
            column_index: HashMap::new(),
        }
    }

    pub fn set_num_keys(&mut self, num_keys: u8) {
        self.num_keys = num_keys;
    }

    pub fn keys(&self) -> Vec<&Column> {
        self.columns.iter().take(self.num_keys as usize).collect()
    }

    pub fn keys_mut(&mut self) -> Vec<&mut Column> {
        self.columns
            .iter_mut()
            .take(self.num_keys as usize)
            .collect()
    }

    pub fn key(&self, name: &str) -> Option<&Column> {
        if let Some(idx) = self.column_index.get(name)
            && *idx < self.num_keys as usize
        {
            return self.columns.get(*idx);
        }
        None
    }

    pub fn key_mut(&mut self, name: &str) -> Option<&mut Column> {
        if let Some(idx) = self.column_index.get(name)
            && *idx < self.num_keys as usize
        {
            return self.columns.get_mut(*idx);
        }
        None
    }
    pub fn column(&self, name: &str) -> Option<&Column> {
        if let Some(idx) = self.column_index.get(name) {
            return self.columns.get(*idx);
        }
        None
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    pub fn values(&self) -> &[Column] {
        &self.columns[self.num_keys as usize..]
    }

    pub fn values_mut(&mut self) -> &mut [Column] {
        &mut self.columns[self.num_keys as usize..]
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().take(self.num_keys as usize)
    }

    pub fn iter_keys_mut(&mut self) -> impl Iterator<Item = &mut Column> {
        self.columns.iter_mut().take(self.num_keys as usize)
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().skip(self.num_keys as usize)
    }

    pub fn iter_values_mut(&mut self) -> impl Iterator<Item = &mut Column> {
        self.columns.iter_mut().skip(self.num_keys as usize)
    }

    pub fn iter_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    pub fn iter_columns_mut(&mut self) -> impl Iterator<Item = &mut Column> {
        self.columns.iter_mut()
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut Column> {
        if let Some(idx) = self.column_index.get(name) {
            return self.columns.get_mut(*idx);
        }
        None
    }

    pub fn push_column(&mut self, col: Column) {
        self.column_index
            .insert(col.name.clone(), self.columns.len());
        self.columns.push(col);
    }

    pub fn add_column(
        &mut self,
        name: &str,
        dtype: DataTypeKind,
        primary: bool,
        non_null: bool,
        unique: bool,
    ) {
        let mut col = Column::new_unindexed(dtype, name, None);
        if primary {
            col.add_constraint(format!("{name}_pkey"), Constraint::PrimaryKey);
        };

        if non_null {
            col.add_constraint(format!("{name}_non_null"), Constraint::NonNull);
        };

        if unique {
            col.add_constraint(format!("{name}_unique"), Constraint::Unique);
        };

        self.push_column(col);
    }

    pub fn has_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|p| p.name == column_name)
    }

    pub fn has_pk(&self) -> bool {
        self.columns.iter().any(|p| p.is_pk())
            || self
                .constraints
                .values()
                .any(|c| matches!(c, TableConstraint::PrimaryKey(_)))
    }

    pub fn drop_constraint(&mut self, constraint_name: &str) {
        self.constraints.remove(constraint_name);
    }

    pub fn add_constraint(&mut self, constraint: TableConstraint) {
        let name = match &constraint {
            TableConstraint::PrimaryKey(columns) => {
                if self.has_pk() {
                    panic!("Cannot add a duplicated primary key constraint");
                };
                format!("{}_pkey", columns.join("_"))
            }
            TableConstraint::Unique(columns) => {
                format!("{}_unique", columns.join("_"))
            }
            TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                format!("{}_{}_fkey", ref_table, columns.join("_"))
            }
        };

        self.constraints.insert(name, constraint);
    }

    pub fn get_dependants(&self) -> Vec<String> {
        let mut deps: Vec<String> = self
            .columns
            .iter()
            .filter_map(|col| col.fk().map(|fk| fk.ref_table.clone()))
            .collect();

        for ct in self.constraints.values() {
            if let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } = ct
            {
                deps.push(ref_table.to_string());
            };
        }

        deps.extend(
            self.columns
                .iter()
                .filter(|col| col.has_index())
                .map(|c| c.index.as_ref().unwrap().clone()),
        );
        deps
    }

    pub fn has_constraint(&self, ct_name: &str) -> bool {
        self.constraints.contains_key(ct_name)
            || self.columns.iter().any(|p| p.has_constraint(ct_name))
    }
}

repr_enum!(
    pub enum ConstraintType: u8 {
        PrimaryKey = 0x00,
        ForeignKey = 0x01,
        Unique = 0x02,
        NonNull = 0x03,
        Default = 0x04

}
);
impl AsBytes for Schema {
    fn write_to<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        buffer.write_all(&[self.num_keys])?;
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        let vbuffer = VarInt::encode(self.columns.len() as i64, &mut varint_buf);
        buffer.write_all(vbuffer)?;

        for column in &self.columns {
            column.write_to(buffer)?;
        }

        let vbuffer = VarInt::encode(self.constraints.len() as i64, &mut varint_buf);
        buffer.write_all(vbuffer)?;

        for (name, constraint) in &self.constraints {
            write_string_unchecked(buffer, name)?;

            match constraint {
                TableConstraint::PrimaryKey(cols) => {
                    buffer.write_all(&[ConstraintType::PrimaryKey.as_repr()])?;
                    let vbuffer = VarInt::encode(cols.len() as i64, &mut varint_buf);
                    buffer.write_all(vbuffer)?;
                    for col in cols {
                        write_string_unchecked(buffer, col)?;
                    }
                }
                TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    buffer.write_all(&[ConstraintType::ForeignKey.as_repr()])?;
                    write_string_unchecked(buffer, ref_table)?;

                    let vbuffer = VarInt::encode(columns.len() as i64, &mut varint_buf);
                    buffer.write_all(vbuffer)?;
                    for col in columns {
                        write_string_unchecked(buffer, col)?;
                    }

                    let vbuffer = VarInt::encode(ref_columns.len() as i64, &mut varint_buf);
                    buffer.write_all(vbuffer)?;
                    for col in ref_columns {
                        write_string_unchecked(buffer, col)?;
                    }
                }
                TableConstraint::Unique(cols) => {
                    buffer.write_all(&[ConstraintType::Unique.as_repr()])?;
                    let vbuffer = VarInt::encode(cols.len() as i64, &mut varint_buf);
                    buffer.write_all(vbuffer)?;
                    for col in cols {
                        write_string_unchecked(buffer, col)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> std::io::Result<Self> {
        let mut tmp_buf = [0u8; 1];
        bytes.read_exact(&mut tmp_buf)?;
        let num_keys = tmp_buf[0];

        let varint = VarInt::read_buf(bytes)?;
        let (column_count, offset) = VarInt::from_encoded_bytes(&varint)?;

        let column_count_usize: usize = column_count.try_into()?;

        let mut columns = Vec::with_capacity(column_count_usize);
        let mut column_index = HashMap::new();

        for i in 0..column_count_usize {
            let column = Column::read_from(bytes)?;
            column_index.insert(column.name.clone(), i);
            columns.push(column);
        }

        let varint = VarInt::read_buf(bytes)?;
        let (constraint_count, offset) = VarInt::from_encoded_bytes(&varint)?;

        let constraint_count_usize: usize = constraint_count.try_into()?;

        let mut constraints = HashMap::with_capacity(constraint_count_usize);

        for _ in 0..constraint_count_usize {
            let name = read_string_unchecked(bytes)?;
            let mut cbuf = [0u8];
            bytes.read_exact(&mut cbuf)?;
            let constraint_type = ConstraintType::from_repr(cbuf[0])?;

            let constraint = match constraint_type {
                ConstraintType::PrimaryKey => {
                    let varint = VarInt::read_buf(bytes)?;
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&varint)?;
                    let cols_count_usize: usize = cols_count.try_into()?;
                    let mut cols = Vec::with_capacity(cols_count_usize);

                    for _ in 0..cols_count_usize {
                        let name = read_string_unchecked(bytes)?;
                        cols.push(name);
                    }
                    TableConstraint::PrimaryKey(cols)
                }
                ConstraintType::ForeignKey => {
                    let ref_table = read_string_unchecked(bytes)?;
                    let varint = VarInt::read_buf(bytes)?;
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&varint)?;
                    let cols_count_usize: usize = cols_count.try_into()?;

                    let mut columns = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let name = read_string_unchecked(bytes)?;
                        columns.push(name);
                    }

                    let varint = VarInt::read_buf(bytes)?;
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&varint)?;
                    let cols_count_usize: usize = cols_count.try_into()?;

                    let mut ref_columns = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let name = read_string_unchecked(bytes)?;
                        ref_columns.push(name);
                    }

                    TableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    }
                }
                ConstraintType::Unique => {
                    let varint = VarInt::read_buf(bytes)?;
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&varint)?;
                    let cols_count_usize: usize = cols_count.try_into()?;
                    let mut cols = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let col_name = read_string_unchecked(bytes)?;
                        cols.push(col_name);
                    }
                    TableConstraint::Unique(cols)
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unknown constraint type: {constraint_type}"),
                    ));
                }
            };

            constraints.insert(name, constraint);
        }

        Ok(Schema {
            columns,
            num_keys,
            constraints,
            column_index,
        })
    }
}

impl Schema {
    pub fn from_columns(columns: &[Column], num_keys: u8) -> Self {
        let mut column_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            column_index.insert(col.name.clone(), i);
        }

        Self {
            columns: columns.to_vec(),
            num_keys,
            constraints: HashMap::new(),
            column_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    object_id: OId,
    root_page: PageId,
    next_row: UInt64,
    name: String,
    schema: Schema,
}

impl Table {
    pub fn new(name: &str, root_page: PageId, schema: Schema) -> Self {
        Self {
            object_id: OId::new(),
            root_page,
            next_row: UInt64(0),
            name: name.to_string(),
            schema,
        }
    }

    pub fn id(&self) -> OId {
        self.object_id
    }

    pub fn root(&self) -> PageId {
        self.root_page
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn next(&self) -> UInt64 {
        self.next_row
    }

    pub fn build(id: OId, name: &str, root_page: PageId, schema: Schema, next_row: UInt64) -> Self {
        Self {
            object_id: id,
            root_page,
            name: name.to_string(),
            schema,
            next_row,
        }
    }

    pub fn add_row(&mut self) {
        self.next_row += 1usize;
    }

    pub fn set_next(&mut self, value: u64) {
        self.next_row = UInt64(value);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Index {
    object_id: OId,
    root_page: PageId,

    name: String,
    min_val: Option<Box<[u8]>>,
    max_val: Option<Box<[u8]>>,
    schema: Schema,
}

impl Index {
    pub fn new(name: &str, root_page: PageId, schema: Schema) -> Self {
        Self {
            object_id: OId::new(),
            root_page,

            name: name.to_string(),
            min_val: None,
            max_val: None,
            schema,
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        id: OId,
        name: &str,
        root_page: PageId,
        schema: Schema,
        min_val: Box<[u8]>,
        max_val: Box<[u8]>,
        num_keys: u8,
    ) -> Self {
        Self {
            object_id: id,
            root_page,

            name: name.to_string(),
            min_val: Some(min_val),
            max_val: Some(max_val),
            schema,
        }
    }

    pub fn id(&self) -> OId {
        self.object_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn root(&self) -> PageId {
        self.root_page
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn range(&self) -> Option<(&[u8], &[u8])> {
        if let Some(xmin) = self.min_val.as_ref()
            && let Some(xmax) = self.max_val.as_ref()
        {
            return Some((xmin.as_ref(), xmax.as_ref()));
        }
        None
    }

    pub fn add_item(&mut self, key: &[u8], comparator: impl Comparator) {
        if let Some(max) = self.max_val.as_ref() {
            if let Ok(Ordering::Greater) = comparator.compare(max.as_ref(), key) {
                self.max_val = Some(key.to_vec().into_boxed_slice())
            }
        } else {
            self.max_val = Some(key.to_vec().into_boxed_slice())
        };

        if let Some(min) = self.min_val.as_ref() {
            if let Ok(Ordering::Less) = comparator.compare(min.as_ref(), key) {
                self.min_val = Some(key.to_vec().into_boxed_slice())
            }
        } else {
            self.min_val = Some(key.to_vec().into_boxed_slice())
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Relation {
    TableRel(Table),
    IndexRel(Index),
}

impl Relation {
    pub fn id(&self) -> OId {
        match self {
            Relation::TableRel(t) => t.object_id,
            Relation::IndexRel(i) => i.object_id,
        }
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.schema().columns
    }

    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.schema_mut().columns
    }

    pub fn keys(&self) -> Vec<&Column> {
        self.schema().keys()
    }

    pub fn keys_mut(&mut self) -> Vec<&mut Column> {
        self.schema_mut().keys_mut()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.schema().column(name)
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.schema_mut().column_mut(name)
    }

    pub fn column_unchecked(&self, idx: usize) -> &Column {
        &self.columns()[idx]
    }

    pub fn column_mut_unchecked(&mut self, idx: usize) -> &mut Column {
        &mut self.columns_mut()[idx]
    }

    pub fn key(&self, name: &str) -> Option<&Column> {
        self.schema().key(name)
    }

    pub fn key_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.schema_mut().key_mut(name)
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        match self {
            Relation::TableRel(t) => &mut t.schema,
            Relation::IndexRel(i) => &mut i.schema,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Relation::TableRel(t) => &t.name,
            Relation::IndexRel(i) => &i.name,
        }
    }

    pub fn root(&self) -> PageId {
        match self {
            Relation::TableRel(t) => t.root_page,
            Relation::IndexRel(i) => i.root_page,
        }
    }

    pub fn set_root(&mut self, root: PageId) {
        match self {
            Relation::TableRel(t) => t.root_page = root,
            Relation::IndexRel(i) => i.root_page = root,
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Relation::TableRel(t) => &t.schema,
            Relation::IndexRel(i) => &i.schema,
        }
    }

    pub fn object_type(&self) -> ObjectType {
        match self {
            Relation::TableRel(_) => ObjectType::Table,
            Relation::IndexRel(_) => ObjectType::Index,
        }
    }

    pub fn is_allocated(&self) -> bool {
        self.root().is_valid()
    }



    pub fn into_boxed_tuple(self) -> std::io::Result<OwnedTuple> {
        let root_page = if self.root() != PAGE_ZERO {
            DataType::BigUInt(UInt64::from(self.root()))
        } else {
            DataType::Null
        };

        // Serialize metadata in-place
        let metadata = DataType::Blob(self.metadata_as_blob()?);

        let schema = meta_table_schema();
        let t = Tuple::new(
            &[
                DataType::BigUInt(UInt64::from(self.id())),
                root_page,
                DataType::Byte(UInt8::from(self.object_type() as u8)),
                metadata,
                DataType::Text(Blob::from(self.name())),
            ],
            &schema,
        )?;

        Ok(t.into())
    }

    fn metadata_as_blob(&self) -> std::io::Result<Blob> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        self.schema().write_to(&mut buffer)?;

        match self {
            Relation::TableRel(t) => {
                buffer.write_all(t.next_row.as_ref())?;
            }
            Relation::IndexRel(i) => {
                if let Some(val) = &i.min_val {
                    buffer.write_all(&[1u8])?;
                    let mut vbuffer = [0u8; MAX_VARINT_LEN];
                    let len_buffer = VarInt::encode(val.len() as i64, &mut vbuffer);
                    buffer.write_all(len_buffer)?;
                    buffer.write_all(val.as_ref())?;
                } else {
                    buffer.write_all(&[0u8])?;
                };

                if let Some(val) = &i.max_val {
                    buffer.write_all(&[1u8])?;
                    let mut vbuffer = [0u8; MAX_VARINT_LEN];
                    let len_buffer = VarInt::encode(val.len() as i64, &mut vbuffer);
                    buffer.write_all(len_buffer)?;
                    buffer.write_all(val.as_ref())?;
                } else {
                    buffer.write_all(&[0u8])?;
                };
            }
        }

        let buf = buffer.into_inner();
        let bytes: &[u8] = buf.as_ref();

        Ok(Blob::from(bytes))
    }
}

impl<'a, 'b> TryFrom<TupleRef<'a, 'b>> for Relation {
    type Error = std::io::Error;

    fn try_from(tuple: TupleRef<'a, 'b>) -> Result<Self, Self::Error> {
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

        let o_id = match tuple.key(0)? {
            DataTypeRef::BigUInt(v) => OId::from(v.to_owned()),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid o_id",
                ));
            }
        };

        let root = match tuple.value(0)? {
            DataTypeRef::BigUInt(v) => PageId::from(v.to_owned()),
            DataTypeRef::Null => PAGE_ZERO,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid root",
                ));
            }
        };

        let o_type = match tuple.value(1)? {
            DataTypeRef::Byte(v) => ObjectType::try_from(v.to_owned().0)?,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid o_type",
                ));
            }
        };

        let name = match tuple.value(3)? {
            DataTypeRef::Text(blob) => blob.as_str(TextEncoding::Utf8).to_string(),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid name",
                ));
            }
        };

        match tuple.value(2)? {
            DataTypeRef::Blob(metadata) => {
                let mut metadata_buf = std::io::Cursor::new(metadata.content());
                let schema = Schema::read_from(&mut metadata_buf)?;

                match o_type {
                    ObjectType::Table => {
                        let mut tmp_buf = [0u8; 8];
                        metadata_buf.read_exact(&mut tmp_buf)?;
                        let next_row = UInt64::try_from(tmp_buf.as_slice())?;

                        Ok(Relation::TableRel(Table {
                            object_id: o_id,
                            root_page: root,

                            name: name.to_string(),
                            schema,
                            next_row,
                        }))
                    }
                    ObjectType::Index => {
                        let dtype = schema
                            .columns
                            .first()
                            .ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Index schema must have at least one column",
                                )
                            })?
                            .dtype;

                        let mut tmp_buf = [0u8; 1];
                        metadata_buf.read_exact(&mut tmp_buf)?;
                        let min_val = if tmp_buf[0] == 1 {
                            Some(read_variable_length(&mut metadata_buf)?)
                        } else {
                            None
                        };

                        let mut tmp_buf = [0u8; 1];
                        metadata_buf.read_exact(&mut tmp_buf)?;
                        let max_val = if tmp_buf[0] == 1 {
                            Some(read_variable_length(&mut metadata_buf)?)
                        } else {
                            None
                        };

                        Ok(Relation::IndexRel(Index {
                            object_id: o_id,
                            root_page: root,
                            name: name.to_string(),

                            min_val,
                            max_val,
                            schema,
                        }))
                    }
                }
            }
            DataTypeRef::Null => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing metadata",
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid metadata type",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub(crate) dtype: DataTypeKind,
    pub(crate) name: String,
    index: Option<String>,
    cts: HashMap<String, Constraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NonNull,
    Unique,
    ForeignKey(ForeignKey),
    Default(DataType),
}

impl Column {
    pub fn new(
        dtype: DataTypeKind,
        name: &str,
        index: Option<String>,
        constraints: Option<HashMap<String, Constraint>>,
    ) -> Self {
        Self {
            dtype,
            index,
            name: name.to_string(),
            cts: constraints.unwrap_or_default(),
        }
    }

    pub fn new_unindexed(
        dtype: DataTypeKind,
        name: &str,
        constraints: Option<HashMap<String, Constraint>>,
    ) -> Self {
        Self::new(dtype, name, None, constraints)
    }

    pub fn has_constraint(&self, ct_name: &str) -> bool {
        self.cts.contains_key(ct_name)
    }

    pub fn has_index(&self) -> bool {
        self.index.is_some()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> Option<&String> {
        self.index.as_ref()
    }

    pub fn set_index(&mut self, index: String) {
        self.index = Some(index);
    }

    pub fn constraints(&self) -> &HashMap<String, Constraint> {
        &self.cts
    }

    pub fn drop_constraint(&mut self, ct_name: &str) {
        self.cts.remove(ct_name);
    }

    pub fn add_constraint(&mut self, ct_name: String, ct: Constraint) {
        self.cts.insert(ct_name, ct);
    }

    pub fn rename(&mut self, name: String) {
        self.name = name;
    }

    pub fn default(&self) -> Option<&DataType> {
        self.cts.iter().find_map(|(_, p)| {
            if let Constraint::Default(dtype) = p {
                Some(dtype)
            } else {
                None
            }
        })
    }

    pub fn fk(&self) -> Option<&ForeignKey> {
        self.cts.iter().find_map(|(_, p)| {
            if let Constraint::ForeignKey(key) = p {
                Some(key)
            } else {
                None
            }
        })
    }

    pub fn is_pk(&self) -> bool {
        self.cts
            .iter()
            .any(|(_, p)| matches!(p, Constraint::PrimaryKey))
    }

    pub fn is_unique(&self) -> bool {
        self.cts
            .iter()
            .any(|(_, p)| matches!(p, Constraint::Unique | Constraint::PrimaryKey))
    }

    pub fn is_non_null(&self) -> bool {
        self.cts
            .iter()
            .any(|(_, p)| matches!(p, Constraint::NonNull | Constraint::PrimaryKey))
    }

    pub fn set_datatype(&mut self, datatype: DataTypeKind) {
        self.dtype = datatype;
    }
}

impl AsBytes for Column {
    fn write_to<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        // Write dtype
        buffer.write_all(&[self.dtype as u8])?;

        // Write index
        if let Some(idx) = &self.index {
            buffer.write_all(&[1u8])?;
            write_string_unchecked(buffer, idx)?;
        } else {
            buffer.write_all(&[0u8])?;
        };

        // Write name
        write_string_unchecked(buffer, &self.name)?;

        // Write constraints count
        let vbuffer = VarInt::encode(self.cts.len() as i64, &mut varint_buf);
        buffer.write_all(vbuffer)?;

        // Write each constraint
        for (ct_name, constraint) in &self.cts {
            // Write constraint name
            write_string_unchecked(buffer, ct_name)?;
            // Write constraint type and data
            match constraint {
                Constraint::PrimaryKey => {
                    buffer.write_all(&[ConstraintType::PrimaryKey.as_repr()])?;
                }
                Constraint::NonNull => {
                    buffer.write_all(&[ConstraintType::NonNull.as_repr()])?;
                }
                Constraint::Unique => {
                    buffer.write_all(&[ConstraintType::Unique.as_repr()])?;
                }
                Constraint::ForeignKey(fk) => {
                    buffer.write_all(&[ConstraintType::ForeignKey.as_repr()])?;
                    fk.write_to(buffer)?;
                }
                Constraint::Default(dt) => {
                    buffer.write_all(&[ConstraintType::Default.as_repr()])?;
                    buffer.write_all(dt.as_ref())?;
                }
            }
        }

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> std::io::Result<Self> {
        let mut dtype_buf = [0u8; 2];
        bytes.read_exact(&mut dtype_buf)?;
        let dtype = DataTypeKind::from_repr(dtype_buf[0])?;
        let has_index = dtype_buf[1] != 0u8;

        // HAS INDEX
        let index = if has_index {
            let index = read_string_unchecked(bytes)?;
            Some(index)
        } else {
            None
        };

        // Read name
        let name = read_string_unchecked(bytes)?;

        // Read constraints count
        let varint = VarInt::read_buf(bytes)?;
        let (cts_count, cts_count_offset) = VarInt::from_encoded_bytes(&varint)?;
        let cts_count_usize: usize = cts_count.try_into().unwrap();
        let mut cts = HashMap::new();

        // Read each constraint
        for _ in 0..cts_count_usize {
            // Read constraint name
            let ct_name = read_string_unchecked(bytes)?;

            let mut ctype_buf = [0u8; 1];
            bytes.read_exact(&mut ctype_buf)?;
            let ctype = ConstraintType::from_repr(ctype_buf[0])?;

            let constraint = match ctype {
                ConstraintType::PrimaryKey => Constraint::PrimaryKey,
                ConstraintType::NonNull => Constraint::NonNull,
                ConstraintType::Unique => Constraint::Unique,
                ConstraintType::ForeignKey => Constraint::ForeignKey(ForeignKey::read_from(bytes)?),
                ConstraintType::Default => {
                    // Read Default DataType

                    let dt = read_type_from_buf(dtype, bytes)?;
                    Constraint::Default(dt)
                }
            };

            cts.insert(ct_name, constraint);
        }

        Ok(Column {
            dtype,
            name,
            index,
            cts,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    ref_table: String,
    ref_col: String,
}

impl ForeignKey {
    pub fn new(ref_table: String, ref_col: String) -> Self {
        Self { ref_table, ref_col }
    }
}

impl AsBytes for ForeignKey {
    fn write_to<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        // Reference table
        write_string_unchecked(buffer, &self.ref_table)?;

        // Reference column
        write_string_unchecked(buffer, &self.ref_col)?;
        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let ref_table = read_string_unchecked(bytes)?;
        let ref_col = read_string_unchecked(bytes)?;

        Ok(Self { ref_table, ref_col })
    }
}

impl ForeignKey {
    pub fn table(&self) -> &str {
        &self.ref_table
    }

    pub fn column(&self) -> &str {
        &self.ref_col
    }
}

repr_enum!(
    pub enum ObjectType: u8 {
        Table = 0,
        Index = 1,
    }
);

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use crate::serialize_test;
    use crate::types::{Int32, Int64};

    serialize_test!(
        test_serialization_1,
        Column::new_unindexed(DataTypeKind::Int, "test_column", None),
        "Column serialization failed"
    );

    serialize_test!(
        test_serialization_2,
        {
            let mut col = Column::new_unindexed(DataTypeKind::Text, "user_name", None);
            col.add_constraint("pk".to_string(), Constraint::PrimaryKey);
            col.add_constraint("nn".to_string(), Constraint::NonNull);
            col.add_constraint("uniq".to_string(), Constraint::Unique);
            col
        },
        "Column with constraints serialization failed"
    );

    serialize_test!(
        test_serialization_3,
        {
            let mut col = Column::new_unindexed(DataTypeKind::BigInt, "user_id", None);
            let fk = ForeignKey::new("users".to_string(), "id".to_string());
            col.add_constraint("fk_user".to_string(), Constraint::ForeignKey(fk));
            col
        },
        "Column with FK serialization failed"
    );

    serialize_test!(
        test_serialization_4,
        {
            let mut col = Column::new_unindexed(DataTypeKind::Int, "status", None);
            col.add_constraint(
                "default".to_string(),
                Constraint::Default(DataType::Int(Int32(42))),
            );
            col
        },
        "Column with default value serialization failed"
    );

    serialize_test!(
        test_serialization_5,
        Column::new(
            DataTypeKind::Text,
            "email",
            Some("idx_email".to_string()),
            None
        ),
        "Column with index serialization failed"
    );

    serialize_test!(
        test_serialization_6,
        Schema::new(),
        "Empty schema serialization failed"
    );

    serialize_test!(
        test_serialization_7,
        {
            let mut schema = Schema::new();
            schema.add_column("id", DataTypeKind::BigInt, true, true, false);
            schema.add_column("name", DataTypeKind::Text, false, true, false);
            schema.add_column("email", DataTypeKind::Text, false, false, true);
            schema.add_column("age", DataTypeKind::Int, false, false, false);
            schema
        },
        "Schema with columns serialization failed"
    );

    serialize_test!(
        test_serialization_8,
        {
            let mut schema = Schema::new();
            schema.add_column("id", DataTypeKind::BigInt, false, true, false);
            schema.add_column("email", DataTypeKind::Text, false, true, false);
            schema.add_constraint(TableConstraint::PrimaryKey(vec!["id".to_string()]));
            schema.add_constraint(TableConstraint::Unique(vec!["email".to_string()]));
            schema
        },
        "Schema with table constraints serialization failed"
    );

    serialize_test!(
        test_serialization_9,
        {
            let mut schema = Schema::new();
            schema.add_column("user_id", DataTypeKind::BigInt, false, true, false);
            schema.add_column("product_id", DataTypeKind::BigInt, false, true, false);
            schema.add_column("quantity", DataTypeKind::Int, false, true, false);
            schema.add_constraint(TableConstraint::PrimaryKey(vec![
                "user_id".to_string(),
                "product_id".to_string(),
            ]));
            schema
        },
        "Schema with composite PK serialization failed"
    );

    serialize_test!(
        test_serialization_10,
        {
            let mut schema = Schema::new();
            schema.add_column("id", DataTypeKind::BigInt, true, true, false);
            schema.add_column("user_id", DataTypeKind::BigInt, false, true, false);
            schema.add_column("product_id", DataTypeKind::BigInt, false, true, false);

            schema.add_constraint(TableConstraint::ForeignKey {
                columns: vec!["user_id".to_string()],
                ref_table: "users".to_string(),
                ref_columns: vec!["id".to_string()],
            });

            schema.add_constraint(TableConstraint::ForeignKey {
                columns: vec!["product_id".to_string()],
                ref_table: "products".to_string(),
                ref_columns: vec!["id".to_string()],
            });
            schema
        },
        "Schema with foreign keys serialization failed"
    );

    serialize_test!(
        test_serialization_11,
        {
            let mut schema = Schema::new();

            let mut col1 = Column::new_unindexed(DataTypeKind::BigInt, "id", None);
            col1.add_constraint("pk".to_string(), Constraint::PrimaryKey);
            col1.set_index("idx_id".to_string());
            schema.push_column(col1);

            let mut col2 = Column::new_unindexed(DataTypeKind::Text, "email", None);
            col2.add_constraint("unique_email".to_string(), Constraint::Unique);
            col2.add_constraint("nn_email".to_string(), Constraint::NonNull);
            schema.push_column(col2);

            let mut col3 = Column::new_unindexed(DataTypeKind::Int, "status", None);
            col3.add_constraint(
                "default_status".to_string(),
                Constraint::Default(DataType::Int(Int32(1))),
            );
            schema.push_column(col3);

            let mut col4 = Column::new_unindexed(DataTypeKind::BigInt, "org_id", None);
            let fk = ForeignKey::new("organizations".to_string(), "id".to_string());
            col4.add_constraint("fk_org".to_string(), Constraint::ForeignKey(fk));
            schema.push_column(col4);

            schema.add_constraint(TableConstraint::Unique(vec![
                "email".to_string(),
                "org_id".to_string(),
            ]));

            schema
        },
        "Complex schema serialization failed"
    );

    serialize_test!(
        test_serialization_12,
        ForeignKey::new("parent_table".to_string(), "parent_column".to_string()),
        "ForeignKey serialization failed"
    );

    serialize_test!(
        test_serialization_13,
        Column::new_unindexed(DataTypeKind::Text, "", None),
        "Empty string column name serialization failed"
    );

    serialize_test!(
        test_serialization_14,
        {
            let long_name = "a".repeat(1000);
            Column::new_unindexed(DataTypeKind::Text, &long_name, None)
        },
        "Long string column name serialization failed"
    );

    serialize_test!(
        test_serialization_15,
        Column::new_unindexed(DataTypeKind::Text, "用户名_", None),
        "Unicode column name serialization failed"
    );

    serialize_test!(
        test_serialization_16,
        {
            let mut schema = Schema::new();
            let mut col = Column::new(
                DataTypeKind::Text,
                "test_col",
                Some("idx_test".to_string()),
                None,
            );
            col.add_constraint("pk".to_string(), Constraint::PrimaryKey);
            col.add_constraint("nn".to_string(), Constraint::NonNull);
            col.add_constraint("uniq".to_string(), Constraint::Unique);
            col.add_constraint(
                "def".to_string(),
                Constraint::Default(DataType::Text(Blob::from("default_value"))),
            );
            col.add_constraint(
                "fk".to_string(),
                Constraint::ForeignKey(ForeignKey::new(
                    "ref_table".to_string(),
                    "ref_col".to_string(),
                )),
            );
            schema.push_column(col);
            schema
        },
        "Schema with all constraint types failed"
    );

    serialize_test!(
        test_serialization_18,
        {
            let mut schema = Schema::new();
            for i in 0..127 {
                schema.add_column(&format!("col_{i}"), DataTypeKind::Int, false, false, false);
            }
            schema
        },
        "Schema with 127 columns failed"
    );

    serialize_test!(
        test_serialization_19,
        {
            let mut schema = Schema::new();
            for i in 0..128 {
                schema.add_column(&format!("col_{i}"), DataTypeKind::Int, false, false, false);
            }
            schema
        },
        "Schema with 128 columns failed"
    );

    serialize_test!(
        test_serialization_20,
        {
            let mut schema = Schema::new();

            let mut col_int = Column::new_unindexed(DataTypeKind::Int, "int_col", None);
            col_int.add_constraint(
                "def_int".to_string(),
                Constraint::Default(DataType::Int(Int32(42))),
            );
            schema.push_column(col_int);

            let mut col_text = Column::new_unindexed(DataTypeKind::Text, "text_col", None);
            col_text.add_constraint(
                "def_text".to_string(),
                Constraint::Default(DataType::Text(Blob::from("hello"))),
            );
            schema.push_column(col_text);

            let mut col_bigint = Column::new_unindexed(DataTypeKind::BigInt, "bigint_col", None);
            col_bigint.add_constraint(
                "def_bigint".to_string(),
                Constraint::Default(DataType::BigInt(Int64(9999999))),
            );
            schema.push_column(col_bigint);

            let mut col_null = Column::new_unindexed(DataTypeKind::Null, "null_col", None);
            col_null.add_constraint("def_null".to_string(), Constraint::Default(DataType::Null));
            schema.push_column(col_null);

            schema
        },
        "Schema with different default types failed"
    );
}
