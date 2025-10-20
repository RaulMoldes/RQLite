use crate::types::{DataTypeMarker, PageId, RowId};
use std::collections::HashMap;

/// SQL constraints.
#[derive(Debug, PartialEq, Clone)]

pub(crate) enum SQLConstraint {
    PrimaryKey,
    Unique,
    ForeignKey,
    Default,
    NotNull,
    Check,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataTypeMarker,
    pub constraints: Vec<SQLConstraint>,
}

/// In-memory representation of a table schema.
#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    /// Column definitions.
    pub columns: Vec<Column>,
    /// Quick index to find column defs based on their name.
    pub index: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexMeta {
    /// Root page of the index-
    pub root: PageId,
    /// Index name.
    pub name: String,
    /// Column on which the index was created.
    pub column: Column,
    /// Schema of the index. Always key -> primary key.
    pub schema: Schema,
    /// Always `true` because non-unique indexes are not implemented.
    pub unique: bool,
}

/// Data that we need to know about tables at runtime.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableMeta {
    /// Root page of the table.
    pub root: PageId,
    /// Table name.
    pub name: String,
    /// Schema of the table as defined by the `CREATE TABLE` statement.
    pub schema: Schema,
    /// All the indexes associated to this table.
    pub indexes: Vec<IndexMeta>,
    /// Next [`RowId`] for this table.
    row_id: RowId,
}

/// Dynamic dispatch for relation types.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Relation {
    Index(IndexMeta),
    Table(TableMeta),
}
