pub(crate) mod base;
pub(crate) mod catalog;
pub(crate) mod stats;

use crate::bytemuck_slice;
use crate::types::DataTypeKind;
pub(crate) use base::{Column, Schema};
use rkyv::{
    Archive, Deserialize, Serialize, from_bytes, rancor::Error as RkyvError, to_bytes,
    util::AlignedVec,
};
pub(crate) use stats::{ColumnStats, Stats};

pub(crate) fn meta_table_schema() -> Schema {
    Schema::new_table(vec![
        Column::new_with_defaults(DataTypeKind::BigInt, "row_id"),
        Column::new_with_defaults(DataTypeKind::BigInt, "root_page"),
        Column::new_with_defaults(DataTypeKind::BigInt, "next_row_id"), // Set to DataTypeKind::Null for indexes
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
        Column::new_with_defaults(DataTypeKind::Blob, "schema"), // Will have to serialize and serialize the schema as a blob
        Column::new_with_defaults(DataTypeKind::Blob, "stats"), // Will have to serialize and serialize the stats as a blob. Stats will be set to null when not set.
    ])
}
