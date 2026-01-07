pub(crate) mod base;
pub(crate) mod catalog;
pub(crate) mod stats;

pub use catalog::SharedCatalog;

#[cfg(test)]
mod tests;

use crate::DataType;

use crate::types::DataTypeKind;
pub(crate) use base::{Column, Schema};
pub(crate) use stats::Stats;

use std::fmt::{Display, Formatter, Result as FmtResult};

pub(crate) fn meta_table_schema() -> Schema {
    Schema::new_table(vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"),
        Column::new_with_defaults(DataTypeKind::BigUInt, "root_page").with_non_null_constraint(),
        Column::new_with_defaults(DataTypeKind::BigUInt, "next_row_id")
            .with_default(DataType::Null)
            .expect("Invalid  meta table schema"), // Set to DataTypeKind::Null for indexes
        Column::new_with_defaults(DataTypeKind::Blob, "name").with_non_null_constraint(),
        Column::new_with_defaults(DataTypeKind::Blob, "schema").with_non_null_constraint(), // Will have to serialize and serialize the schema as a blob
        Column::new_with_defaults(DataTypeKind::Blob, "stats"), // Will have to serialize and serialize the stats as a blob. Stats will be set to null when not set.
    ])
}

pub(crate) fn meta_index_schema() -> Schema {
    Schema::new_index(
        vec![
            Column::new_with_defaults(DataTypeKind::Blob, "name").with_non_null_constraint(),
            Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"),
        ],
        1,
    )
}

/// Object already exists errors
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseItem {
    Table(String),
    Index(String),
    Constraint(String),
    Column(String, String),
}

impl Display for DatabaseItem {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Index(index) => write!(f, "Index '{}'", index),
            Self::Table(table) => write!(f, "Table '{}' ", table),
            Self::Constraint(ct) => write!(f, "Constraint '{}'", ct),
            Self::Column(table, column) => {
                write!(f, "Column '{}' on table '{}'", column, table)
            }
        }
    }
}
