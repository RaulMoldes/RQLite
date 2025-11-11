use crate::io::pager::SharedPager;
use crate::storage::{
    cell::Slot,
    page::BtreePage,
    tuple::{tuple, Tuple, TupleRef},
};

use crate::structures::bplustree::{
    BPlusTree, FixedSizeComparator, NodeAccessMode, SearchResult, VarlenComparator,
};
use crate::types::initialize_atomics;
use crate::types::{
    reinterpret_cast, varint::MAX_VARINT_LEN, Blob, DataType, DataTypeKind, DataTypeRef, Key, OId,
    PageId, UInt64, UInt8, VarInt, META_INDEX_ROOT, META_TABLE_ROOT, PAGE_ZERO,
};

use crate::{repr_enum, TextEncoding};
use std::collections::HashMap;

pub const META_TABLE: &str = "rqcatalog";
pub const META_INDEX: &str = "rqindex";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Schema {
    pub(crate) columns: Vec<Column>,
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
            constraints: HashMap::new(),
            column_index: HashMap::new(),
        }
    }

    pub fn find_col(&self, name: &str) -> Option<&Column> {
        if let Some(idx) = self.column_index.get(name) {
            return self.columns.get(*idx);
        }
        None
    }

    pub fn find_column_mut(&mut self, name: &str) -> Option<&mut Column> {
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

pub trait AsBytes {
    fn write_to(&self) -> std::io::Result<Vec<u8>>;
    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)>
    where
        Self: Sized;
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

        let buffer = VarInt::encode(self.constraints.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);

        for (name, constraint) in &self.constraints {
            let name_bytes = name.as_bytes();
            let buffer = VarInt::encode(name_bytes.len() as i64, &mut varint_buf);
            out_buffer.extend_from_slice(buffer);
            out_buffer.extend_from_slice(name_bytes);

            match constraint {
                TableConstraint::PrimaryKey(cols) => {
                    out_buffer.push(0);
                    let buffer = VarInt::encode(cols.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    for col in cols {
                        let col_bytes = col.as_bytes();
                        let buffer = VarInt::encode(col_bytes.len() as i64, &mut varint_buf);
                        out_buffer.extend_from_slice(buffer);
                        out_buffer.extend_from_slice(col_bytes);
                    }
                }
                TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    out_buffer.push(1);

                    let buffer = VarInt::encode(ref_table.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    out_buffer.extend_from_slice(ref_table.as_bytes());

                    let buffer = VarInt::encode(columns.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    for col in columns {
                        let col_bytes = col.as_bytes();
                        let buffer = VarInt::encode(col_bytes.len() as i64, &mut varint_buf);
                        out_buffer.extend_from_slice(buffer);
                        out_buffer.extend_from_slice(col_bytes);
                    }

                    let buffer = VarInt::encode(ref_columns.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    for col in ref_columns {
                        let col_bytes = col.as_bytes();
                        let buffer = VarInt::encode(col_bytes.len() as i64, &mut varint_buf);
                        out_buffer.extend_from_slice(buffer);
                        out_buffer.extend_from_slice(col_bytes);
                    }
                }
                TableConstraint::Unique(cols) => {
                    out_buffer.push(2);
                    let buffer = VarInt::encode(cols.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    for col in cols {
                        let col_bytes = col.as_bytes();
                        let buffer = VarInt::encode(col_bytes.len() as i64, &mut varint_buf);
                        out_buffer.extend_from_slice(buffer);
                        out_buffer.extend_from_slice(col_bytes);
                    }
                }
            }
        }

        Ok(out_buffer)
    }

    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)> {
        let mut cursor = 0;

        let (column_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
        cursor += offset;
        let column_count_usize: usize = column_count
            .try_into()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut columns = Vec::with_capacity(column_count_usize);
        let mut column_index = HashMap::new();

        for i in 0..column_count_usize {
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

            column_index.insert(column.name.clone(), i);
            columns.push(column);
        }

        let (constraint_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
        cursor += offset;
        let constraint_count_usize: usize = constraint_count
            .try_into()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut constraints = HashMap::with_capacity(constraint_count_usize);

        for _ in 0..constraint_count_usize {
            let (name_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            cursor += offset;
            let name_len_usize: usize = name_len.try_into()?;

            if cursor + name_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for constraint name",
                ));
            }

            let name = String::from_utf8(bytes[cursor..cursor + name_len_usize].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += name_len_usize;

            if cursor >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for constraint type",
                ));
            }

            let constraint_type = bytes[cursor];
            cursor += 1;

            let constraint = match constraint_type {
                0 => {
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    cursor += offset;
                    let cols_count_usize: usize = cols_count.try_into()?;

                    let mut cols = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let (col_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                        cursor += offset;
                        let col_len_usize: usize = col_len.try_into()?;

                        if cursor + col_len_usize > bytes.len() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Insufficient bytes for column name in constraint",
                            ));
                        }

                        let col_name =
                            String::from_utf8(bytes[cursor..cursor + col_len_usize].to_vec())
                                .map_err(|e| {
                                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                                })?;
                        cursor += col_len_usize;
                        cols.push(col_name);
                    }
                    TableConstraint::PrimaryKey(cols)
                }
                1 => {
                    let (ref_table_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    cursor += offset;
                    let ref_table_len_usize: usize = ref_table_len.try_into()?;

                    if cursor + ref_table_len_usize > bytes.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Insufficient bytes for reference table",
                        ));
                    }

                    let ref_table =
                        String::from_utf8(bytes[cursor..cursor + ref_table_len_usize].to_vec())
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    cursor += ref_table_len_usize;

                    let (cols_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    cursor += offset;
                    let cols_count_usize: usize = cols_count.try_into()?;

                    let mut columns = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let (col_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                        cursor += offset;
                        let col_len_usize: usize = col_len.try_into()?;

                        if cursor + col_len_usize > bytes.len() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Insufficient bytes for column name",
                            ));
                        }

                        let col_name =
                            String::from_utf8(bytes[cursor..cursor + col_len_usize].to_vec())
                                .map_err(|e| {
                                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                                })?;
                        cursor += col_len_usize;
                        columns.push(col_name);
                    }

                    let (ref_cols_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    cursor += offset;
                    let ref_cols_count_usize: usize = ref_cols_count.try_into()?;

                    let mut ref_columns = Vec::with_capacity(ref_cols_count_usize);
                    for _ in 0..ref_cols_count_usize {
                        let (col_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                        cursor += offset;
                        let col_len_usize: usize = col_len.try_into()?;

                        if cursor + col_len_usize > bytes.len() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Insufficient bytes for reference column name",
                            ));
                        }

                        let col_name =
                            String::from_utf8(bytes[cursor..cursor + col_len_usize].to_vec())
                                .map_err(|e| {
                                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                                })?;
                        cursor += col_len_usize;
                        ref_columns.push(col_name);
                    }

                    TableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    }
                }
                2 => {
                    let (cols_count, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    cursor += offset;
                    let cols_count_usize: usize = cols_count.try_into()?;

                    let mut cols = Vec::with_capacity(cols_count_usize);
                    for _ in 0..cols_count_usize {
                        let (col_len, offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                        cursor += offset;
                        let col_len_usize: usize = col_len.try_into()?;

                        if cursor + col_len_usize > bytes.len() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Insufficient bytes for column name in constraint",
                            ));
                        }

                        let col_name =
                            String::from_utf8(bytes[cursor..cursor + col_len_usize].to_vec())
                                .map_err(|e| {
                                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                                })?;
                        cursor += col_len_usize;
                        cols.push(col_name);
                    }
                    TableConstraint::Unique(cols)
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unknown constraint type: {constraint_type}"),
                    ))
                }
            };

            constraints.insert(name, constraint);
        }

        Ok((
            Schema {
                columns,
                constraints,
                column_index,
            },
            cursor,
        ))
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
        let mut column_index = HashMap::new();
        for (i, col) in value.iter().enumerate() {
            column_index.insert(col.name.clone(), i);
        }

        Self {
            columns: value.to_vec(),
            constraints: HashMap::new(),
            column_index,
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
    fn write_to(&self) -> std::io::Result<Vec<u8>> {
        let mut out_buffer = Vec::new();
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        // Write dtype
        out_buffer.push(self.dtype as u8);

        // Write index
        if let Some(idx) = &self.index {
            out_buffer.push(1); // INDEX MARKER
            let buffer = VarInt::encode(idx.len() as i64, &mut varint_buf);
            out_buffer.extend_from_slice(buffer);
            out_buffer.extend_from_slice(idx.as_ref());
        } else {
            out_buffer.push(0);
        };

        // Write name
        let name_bytes = self.name.as_bytes();
        let buffer = VarInt::encode(name_bytes.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);
        out_buffer.extend_from_slice(name_bytes);

        // Write constraints count
        let buffer = VarInt::encode(self.cts.len() as i64, &mut varint_buf);
        out_buffer.extend_from_slice(buffer);

        // Write each constraint
        for (ct_name, constraint) in &self.cts {
            // Write constraint name
            let ct_name_bytes = ct_name.as_bytes();
            let buffer = VarInt::encode(ct_name_bytes.len() as i64, &mut varint_buf);
            out_buffer.extend_from_slice(buffer);
            out_buffer.extend_from_slice(ct_name_bytes);

            // Write constraint type and data
            match constraint {
                Constraint::PrimaryKey => {
                    out_buffer.push(0); // Type tag for PrimaryKey
                }
                Constraint::NonNull => {
                    out_buffer.push(1); // Type tag for NonNull
                }
                Constraint::Unique => {
                    out_buffer.push(2); // Type tag for Unique
                }
                Constraint::ForeignKey(fk) => {
                    out_buffer.push(3); // Type tag for ForeignKey
                                        // Serialize ForeignKey
                    let fk_bytes = fk.write_to()?;
                    let buffer = VarInt::encode(fk_bytes.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    out_buffer.extend_from_slice(&fk_bytes);
                }
                Constraint::Default(dt) => {
                    out_buffer.push(4); // Type tag for Default

                    let dt_bytes = dt.as_ref();
                    let buffer = VarInt::encode(dt_bytes.len() as i64, &mut varint_buf);
                    out_buffer.extend_from_slice(buffer);
                    out_buffer.extend_from_slice(dt_bytes);
                }
            }
        }

        Ok(out_buffer)
    }

    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)> {
        let mut cursor = 0;

        // Read dtype
        if cursor >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Insufficient bytes for dtype",
            ));
        }
        let dtype = DataTypeKind::from_repr(bytes[cursor])?;
        cursor += 1;
        let next = bytes[cursor];
        cursor += 1;

        // HAS INDEX
        let index = if next == 1 {
            let (index_len, index_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let index_len_usize: usize = index_len.try_into()?;
            cursor += index_len_offset;

            if cursor + index_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for column index",
                ));
            }
            let index = String::from_utf8(bytes[cursor..cursor + index_len_usize].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += index_len_usize;
            Some(index)
        } else {
            None
        };

        // Read name
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

        // Read constraints count
        let (cts_count, cts_count_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
        let cts_count_usize: usize = cts_count.try_into().unwrap();
        cursor += cts_count_offset;

        let mut cts = HashMap::new();

        // Read each constraint
        for _ in 0..cts_count_usize {
            // Read constraint name
            let (ct_name_len, ct_name_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
            let ct_name_len_usize: usize = ct_name_len.try_into()?;
            cursor += ct_name_len_offset;

            if cursor + ct_name_len_usize > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for constraint name",
                ));
            }
            let ct_name = String::from_utf8(bytes[cursor..cursor + ct_name_len_usize].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            cursor += ct_name_len_usize;

            // Read constraint type and data
            if cursor >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Insufficient bytes for constraint type",
                ));
            }
            let constraint_type = bytes[cursor];
            cursor += 1;

            let constraint = match constraint_type {
                0 => Constraint::PrimaryKey,
                1 => Constraint::NonNull,
                2 => Constraint::Unique,
                3 => {
                    // Read ForeignKey
                    let (fk_len, fk_len_offset) = VarInt::from_encoded_bytes(&bytes[cursor..])?;
                    let fk_len_usize: usize = fk_len.try_into()?;
                    cursor += fk_len_offset;

                    if cursor + fk_len_usize > bytes.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Insufficient bytes for ForeignKey",
                        ));
                    }
                    let (fk, _) = ForeignKey::read_from(&bytes[cursor..cursor + fk_len_usize])?;
                    cursor += fk_len_usize;
                    Constraint::ForeignKey(fk)
                }
                4 => {
                    // Read Default DataType
                    let (dt, bytes_read) = reinterpret_cast(dtype, &bytes[cursor..])?;
                    cursor += bytes_read;
                    Constraint::Default(dt.to_owned())
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unknown constraint type: {constraint_type}"),
                    ));
                }
            };

            cts.insert(ct_name, constraint);
        }

        Ok((
            Column {
                dtype,
                name,
                index,
                cts,
            },
            cursor,
        ))
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
    fn write_to(&self) -> std::io::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.ref_table.len() + self.ref_col.len());

        // Reference table
        let mut varint_buf = [0u8; MAX_VARINT_LEN];
        let len_bytes = VarInt::encode(self.ref_table.len() as i64, &mut varint_buf);
        buffer.extend_from_slice(len_bytes);
        buffer.extend_from_slice(self.ref_table.as_bytes());

        // Reference column
        let mut varint_buf = [0u8; MAX_VARINT_LEN];
        let len_bytes = VarInt::encode(self.ref_col.len() as i64, &mut varint_buf);
        buffer.extend_from_slice(len_bytes);
        buffer.extend_from_slice(self.ref_col.as_bytes());
        Ok(buffer)
    }

    fn read_from(bytes: &[u8]) -> std::io::Result<(Self, usize)>
    where
        Self: Sized,
    {
        let mut cursor = 0;
        let (len, offset) = VarInt::from_encoded_bytes(bytes)?;
        let len_usize: usize = len.try_into()?;
        cursor += offset;

        let ref_table = match std::str::from_utf8(&bytes[cursor..cursor + len_usize]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        };

        cursor += len_usize;

        let (len, offset) = VarInt::from_encoded_bytes(bytes)?;
        let len_usize: usize = len.try_into()?;
        cursor += offset;

        let ref_col = match std::str::from_utf8(&bytes[cursor..cursor + len_usize]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        };

        cursor += len_usize;

        Ok((Self { ref_table, ref_col }, cursor))
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

repr_enum!(
    pub enum ObjectType: u8 {
        Table = 0,
        Index = 1,
        Column = 2,
        Constraint = 3,
        View = 4,
    }
);

#[derive(Debug, PartialEq, Clone)]
pub struct DBObject {
    o_id: OId,
    root: PageId,
    o_type: ObjectType,
    name: String,
    metadata: Option<Box<[u8]>>,
}

impl DBObject {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn root(&self) -> PageId {
        self.root
    }

    pub fn set_meta(&mut self, new_meta: &[u8]) {
        self.metadata = Some(new_meta.to_vec().into_boxed_slice());
    }

    pub fn object_type(&self) -> ObjectType {
        self.o_type
    }

    pub fn get_schema(&self) -> Schema {
        if let Some(metadata) = self.metadata() {
            if let Ok((schema, _)) = Schema::read_from(metadata) {
                return schema;
            }
        };
        panic!("Object does not have schema");
    }
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
    pub fn new(o_type: ObjectType, name: &str, metadata: Option<&[u8]>) -> Self {
        Self {
            o_id: OId::new_key(),
            root: PAGE_ZERO,
            o_type,
            name: name.to_string(),
            metadata: metadata.map(|v| v.to_vec().into_boxed_slice()),
        }
    }

    pub fn metadata(&self) -> Option<&[u8]> {
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

    pub fn create_table(&mut self, name: &str, schema: Schema) -> std::io::Result<()> {
        let schema_bytes = schema.write_to()?;
        let obj_creat = DBObject::new(ObjectType::Table, name, Some(&schema_bytes));
        self.index_obj(&obj_creat)?;
        self.create_obj(obj_creat)?;
        Ok(())
    }

    pub fn create_obj(&mut self, mut obj: DBObject) -> std::io::Result<OId> {
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
        meta_table.clear_stack();
        meta_table.insert(META_TABLE_ROOT, &tuple)?;
        Ok(oid)
    }

    pub fn update_obj(&mut self, obj: DBObject) -> std::io::Result<OId> {
        let oid = obj.o_id;

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
                DataType::Text(Blob::from(obj.name.as_str())),
                DataType::BigUInt(UInt64::from(obj.o_id)),
            ],
            &schema,
        )?;
        meta_idx.clear_stack();
        meta_idx.insert(META_INDEX_ROOT, &tuple)?;

        Ok(())
    }

    pub fn search_obj(&self, obj_name: &str) -> std::io::Result<OId> {
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
                    pk,
                    Column::new_unindexed(DataTypeKind::BigUInt, "age", None),
                    Column::new_unindexed(DataTypeKind::Text, "description", None),
                ]
                .as_ref(),
            )
        );

        dbg!(des);
        Ok(())
    }
}
