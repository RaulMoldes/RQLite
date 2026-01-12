//! Constraint validation module for DML operations.
//!
//! This module provides validation of database constraints (UNIQUE, NOT NULL, etc.)
//! before INSERT and UPDATE operations are committed.

use std::collections::HashSet;

use crate::{
    ObjectId, SerializationError, UInt64,
    runtime::context::ThreadContext,
    schema::{DatabaseItem, Schema, base::IndexHandle, catalog::CatalogError},
    storage::tuple::{TupleError, TupleReader, TupleRef},
    tree::bplustree::{BtreeError, SearchResult},
    types::{DataType, RowId},
};

use std::fmt;

#[derive(Debug)]
pub enum ValidationError {
    NonNullConstraintViolated(DatabaseItem),
    UniqueConstraintViolated(DatabaseItem),
    ForeignKeyViolation(DatabaseItem),
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
            ValidationError::ForeignKeyViolation(item) => {
                write!(f, "FOREIGN KEY constraint violated on {item}")
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

pub(crate) struct ConstraintValidator<'a> {
    table_name: String,
    schema: &'a Schema,
    ctx: ThreadContext,
}

impl<'a> ConstraintValidator<'a> {
    pub(crate) fn new(schema: &'a Schema, table_name: String, ctx: ThreadContext) -> Self {
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

    pub(crate) fn validate_foreign_key_constraints(
        &self,
        table_id: ObjectId,
        values: &[DataType],
    ) -> ValidationResult<()> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let ref_table_relation =
            self.ctx
                .catalog()
                .get_relation(table_id, &tree_builder, &snapshot)?;
        let schema = ref_table_relation.schema();
        let infos = schema.get_foreign_keys_info();
        for info in infos {
            let found =
                if let Some(handle) = schema.get_index_for_column_list(info.referenced_columns()) {
                    self.search_index(&handle, values, true, &HashSet::new())?
                } else {
                    self.search_table(
                        info.referenced_table(),
                        info.referenced_columns(),
                        values,
                        true,
                        &HashSet::new(),
                    )?
                };

            if !found {
                return Err(ValidationError::ForeignKeyViolation(DatabaseItem::Table(
                    self.table_name.to_string(),
                )));
            }
        }

        Ok(())
    }

    /// Seq scan over a table to see if there is any row with a specific value list.
    pub(crate) fn search_table(
        &self,
        table_id: ObjectId,
        target_columns: &[usize],
        values: &[DataType],
        skip_nulls: bool,
        excluded_set: &HashSet<RowId>,
    ) -> ValidationResult<bool> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let table_relation =
            self.ctx
                .catalog()
                .get_relation(table_id, &tree_builder, &snapshot)?;
        let table_root = table_relation.root();

        // Search in the table
        let mut table_btree = self.ctx.build_tree(table_root);
        if table_btree.is_empty()? {
            return Ok(false);
        }

        for position in table_btree.iter_forward()? {
            if let Ok(pos) = position {
                if let Some(row) = table_btree.get_row_at(pos, self.schema, &snapshot)? {
                    let Some(DataType::BigUInt(UInt64(value))) = row.first() else {
                        return Err(ValidationError::Btree(BtreeError::Other(
                            "tables must start with row id".to_string(),
                        )));
                    };

                    if excluded_set.contains(&value) {
                        continue;
                    };

                    let all_match = target_columns.iter().enumerate().all(|(i, &col_idx)| {
                        let row_value = &row[col_idx];
                        let target_value = &values[i];

                        if skip_nulls && (row_value.is_null() || target_value.is_null()) {
                            false
                        } else {
                            row_value == target_value
                        }
                    });

                    if all_match {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Searches for an entry on an index. Returns true if the entry is found.
    pub(crate) fn search_index(
        &self,
        index: &IndexHandle,
        values: &[DataType],
        skip_nulls: bool,
        excluded_set: &HashSet<RowId>,
    ) -> ValidationResult<bool> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let index_relation =
            self.ctx
                .catalog()
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
        if has_null && skip_nulls {
            return Ok(false);
        }

        // Search in the index
        let mut index_btree = self.ctx.build_tree(index_root);

        if let SearchResult::Found(pos) = index_btree.search(&key_bytes, index_schema)? {
            // Found a matching key - need to check if it's VISIBLE to our snapshot
            // If the tuple was deleted and the delete is visible, it's not a conflict
            let tuple_reader = TupleReader::from_schema(index_schema);

            let is_found = index_btree.with_cell_at(pos, |bytes| {
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

            return Ok(is_found);
        }

        Ok(false)
    }

    /// Validates UNIQUE constraints by checking indexes.
    pub(crate) fn validate_unique_constraints(
        &self,
        values: &[DataType],
        skip_nulls: bool,
        excluded_set: HashSet<RowId>,
    ) -> ValidationResult<()> {
        let indexes = self.schema.get_indexes();

        // For each of the indexes, check if we can build a new entry with the
        for index in indexes {
            let is_conflict = self.search_index(&index, values, skip_nulls, &excluded_set)?;
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

        Ok(())
    }
}
