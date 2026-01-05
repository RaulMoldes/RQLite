//! Constraint validation module for DML operations.
//!
//! This module provides validation of database constraints (UNIQUE, NOT NULL, etc.)
//! before INSERT and UPDATE operations are committed.

use std::collections::HashSet;

use crate::{
    SerializationError,
    runtime::context::TransactionContext,
    schema::{
        DatabaseItem, Schema,
        catalog::{CatalogError, CatalogTrait},
    },
    storage::tuple::{TupleError, TupleReader, TupleRef},
    tree::{
        accessor::TreeReader,
        bplustree::{BtreeError, SearchResult},
    },
    types::{DataType, RowId},
};

use std::fmt;

#[derive(Debug)]
pub enum ValidationError {
    NonNullConstraintViolated(DatabaseItem),
    UniqueConstraintViolated(DatabaseItem),
    TupleError(TupleError),
    SerializationError(SerializationError),
    CatalogError(CatalogError),
    Btree(BtreeError),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::NonNullConstraintViolated(item) => {
                write!(f, "NOT NULL constraint violated on {item}")
            }
            ValidationError::UniqueConstraintViolated(item) => {
                write!(f, "UNIQUE constraint violated on {item}")
            }
            ValidationError::TupleError(err) => {
                write!(f, "tuple error: {err}")
            }
            ValidationError::SerializationError(err) => {
                write!(f, "serialization error: {err}")
            }
            ValidationError::CatalogError(err) => {
                write!(f, "catalog error: {err}")
            }
            ValidationError::Btree(err) => {
                write!(f, "btree error: {err}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<TupleError> for ValidationError {
    fn from(err: TupleError) -> Self {
        ValidationError::TupleError(err)
    }
}

impl From<SerializationError> for ValidationError {
    fn from(err: SerializationError) -> Self {
        ValidationError::SerializationError(err)
    }
}

impl From<CatalogError> for ValidationError {
    fn from(err: CatalogError) -> Self {
        ValidationError::CatalogError(err)
    }
}

impl From<BtreeError> for ValidationError {
    fn from(err: BtreeError) -> Self {
        ValidationError::Btree(err)
    }
}

pub type ValidationResult<T> = Result<T, ValidationError>;

pub(crate) struct ConstraintValidator<'a, Acc: TreeReader> {
    table_name: String,
    schema: &'a Schema,
    ctx: TransactionContext<Acc>,
}

impl<'a, Acc: TreeReader + Clone> ConstraintValidator<'a, Acc> {
    pub(crate) fn new(
        schema: &'a Schema,
        table_name: String,
        ctx: TransactionContext<Acc>,
    ) -> Self {
        Self {
            table_name,
            schema,
            ctx,
        }
    }
    /// Validates NOT NULL constraints.
    pub(crate) fn validate_not_null_constraints(
        &self,
        values: &[DataType],
    ) -> ValidationResult<()> {
        for (idx, col) in self.schema.iter_columns().enumerate() {
            if col.is_non_null && idx < values.len() && values[idx].is_null() {
                return Err(ValidationError::NonNullConstraintViolated(
                    DatabaseItem::Column(self.table_name.clone(), col.name().to_string()),
                ));
            }
        }
        Ok(())
    }

    /// Validates UNIQUE constraints by checking indexes.
    pub(crate) fn validate_unique_constraints(
        &self,
        values: &[DataType],
        skip_nulls: bool,
        excluded_set: HashSet<RowId>,
    ) -> ValidationResult<()> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let indexes = self.schema.get_indexes();

        // For each of the indexes, check if we can build a new entry with the
        for index in indexes {
            let index_relation =
                self.ctx
                    .catalog()
                    .read()
                    .get_relation(index.id(), &tree_builder, &snapshot)?;

            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            // Build the key from the indexed columns
            let mut key_bytes: Vec<u8> = Vec::new();
            let mut has_null = false;

            for &col_idx in index.indexed_column_ids() {
                if col_idx < values.len() {
                    // In SQL, NULL values don't violate UNIQUE constraints
                    // (NULL != NULL, so multiple NULLs are allowed)
                    if values[col_idx].is_null() && skip_nulls {
                        has_null = true;
                        break;
                    }
                    let serialized = values[col_idx].serialize()?;
                    key_bytes.extend_from_slice(&serialized);
                }
            }

            // Skip uniqueness check if any indexed column is NULL
            if has_null {
                continue;
            }

            // Search in the index
            let mut index_btree = self.ctx.build_tree(index_root);

            if let SearchResult::Found(pos) = index_btree.search(&key_bytes, index_schema)? {
                // Found a matching key - need to check if it's VISIBLE to our snapshot
                // If the tuple was deleted and the delete is visible, it's not a conflict
                let tuple_reader = TupleReader::from_schema(index_schema);

                let is_conflict = index_btree.with_cell_at(pos, |bytes| {
                    // Parse the tuple for our snapshot - this respects MVCC visibility
                    match tuple_reader.parse_for_snapshot(bytes, &snapshot)? {
                        Some(layout) => {
                            // Tuple is visible! Check if it's the row we're excluding (for updates)
                            let tuple_ref = TupleRef::new(bytes, layout);

                            // row_id is the first value in the index
                            // Index layout: [key1, key2, ...] [row_id]
                            //               ^-- keys --^      ^-- values --^
                            if let Ok(row_id_ref) = tuple_ref.value_with(0, index_schema) {
                                if let Some(found_row_id) = row_id_ref.to_owned() {
                                    if let Some(uid) = found_row_id.as_big_u_int() {
                                        if excluded_set.contains(&uid.value()) {
                                            // This is the same row we're updating, not a conflict
                                            return Ok::<bool, TupleError>(false);
                                        }
                                    }
                                }
                            }

                            // Different row or no exclusion (conflict found)
                            Ok::<bool, TupleError>(true)
                        }
                        None => {
                            // Tuple is NOT visible (deleted or not yet committed)
                            // No conflict
                            Ok::<bool, TupleError>(false)
                        }
                    }
                })??;

                if is_conflict {
                    let col_names: Vec<&str> = index
                        .indexed_column_ids()
                        .iter()
                        .filter_map(|&idx| self.schema.column(idx).map(|c| c.name()))
                        .collect();
                    return Err(ValidationError::UniqueConstraintViolated(
                        DatabaseItem::Column(col_names.join(","), self.table_name.to_string()),
                    ));
                }
            }
        }

        Ok(())
    }
}
