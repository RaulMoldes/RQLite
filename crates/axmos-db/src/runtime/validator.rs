//! Constraint validation module for DML operations.
//!
//! This module provides validation of database constraints (UNIQUE, NOT NULL, etc.)
//! before INSERT and UPDATE operations are committed.

use std::collections::HashMap;

use crate::{
    core::SerializableType,
    runtime::{RuntimeError, RuntimeResult, context::TransactionContext},
    schema::{Schema, base::IndexHandle, catalog::CatalogTrait},
    storage::tuple::TupleReader,
    tree::{accessor::TreeWriter, bplustree::SearchResult},
    types::{DataType, RowId},
};

/// Validates all constraints before an insert operation.
///
/// This function checks:
/// - NOT NULL constraints on all columns
/// - UNIQUE constraints via index lookup
pub fn validate_insert_constraints<Acc>(
    ctx: &mut TransactionContext<Acc>,
    schema: &Schema,
    values: &[DataType],
    indexes: &[IndexHandle],
) -> RuntimeResult<()>
where
    Acc: TreeWriter + Clone + Default,
{
    // Validate NOT NULL constraints
    validate_not_null_constraints(schema, values)?;

    // Validate UNIQUE constraints via index lookup
    validate_unique_constraints(ctx, schema, values, indexes, None)?;

    Ok(())
}

/// Validates NOT NULL constraints.
fn validate_not_null_constraints(schema: &Schema, values: &[DataType]) -> RuntimeResult<()> {
    for (idx, col) in schema.iter_columns().enumerate() {
        if col.is_non_null && idx < values.len() && values[idx].is_null() {
            return Err(RuntimeError::Other(format!(
                "NOT NULL constraint violated for column '{}'",
                col.name()
            )));
        }
    }
    Ok(())
}

/// Validates UNIQUE constraints by checking indexes.
///
/// If `exclude_row_id` is Some, that row is excluded from the check (for updates).
fn validate_unique_constraints<Acc>(
    ctx: &mut TransactionContext<Acc>,
    schema: &Schema,
    values: &[DataType],
    indexes: &[IndexHandle],
    exclude_row_id: Option<RowId>,
) -> RuntimeResult<()>
where
    Acc: TreeWriter + Clone + Default,
{
    let tree_builder = ctx.tree_builder();
    let snapshot = ctx.snapshot();

    for index in indexes {
        let index_relation =
            ctx.catalog()
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
                if values[col_idx].is_null() {
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
        let mut index_btree = ctx.build_tree(index_root);

        if let SearchResult::Found(pos) = index_btree.search(&key_bytes, index_schema)? {
            // Found a matching key - need to check if it's VISIBLE to our snapshot
            // If the tuple was deleted and the delete is visible, it's not a conflict
            let tuple_reader = TupleReader::from_schema(index_schema);

            let is_conflict = index_btree.with_cell_at(pos, |bytes| {
                // Parse the tuple for our snapshot - this respects MVCC visibility
                match tuple_reader.parse_for_snapshot(bytes, &snapshot) {
                    Ok(Some(layout)) => {
                        // Tuple is visible! Check if it's the row we're excluding (for updates)
                        if let Some(exclude_id) = exclude_row_id {
                            // The row_id is the last column in the index
                            // In index schema: keys are the indexed columns, values contain row_id
                            let tuple_ref = crate::storage::tuple::TupleRef::new(bytes, layout);

                            // row_id is the first (and usually only) value in the index
                            // Index layout: [key1, key2, ...] [row_id]
                            //               ^-- keys --^      ^-- values --^
                            if let Ok(row_id_ref) = tuple_ref.value_with(0, index_schema) {
                                if let Some(found_row_id) = row_id_ref.to_owned() {
                                    if let Some(uid) = found_row_id.as_big_u_int() {
                                        if uid.value() == exclude_id {
                                            // This is the same row we're updating, not a conflict
                                            return false;
                                        }
                                    }
                                }
                            }
                        }
                        // Different row or no exclusion - this is a conflict
                        true
                    }
                    Ok(None) => {
                        // Tuple is NOT visible (deleted or not yet committed)
                        // No conflict!
                        false
                    }
                    Err(_) => {
                        // Parse error - treat as no conflict to be safe
                        // (better to allow than to block incorrectly)
                        false
                    }
                }
            })?;

            if is_conflict {
                let col_names: Vec<&str> = index
                    .indexed_column_ids()
                    .iter()
                    .filter_map(|&idx| schema.column(idx).map(|c| c.name()))
                    .collect();
                return Err(RuntimeError::Other(format!(
                    "UNIQUE constraint violated for column(s): {}",
                    col_names.join(", ")
                )));
            }
        }
    }

    ctx.accessor_mut().clear();
    Ok(())
}

/// Validates constraints for an update operation.
///
/// This function checks:
/// - NOT NULL constraints on updated columns
/// - UNIQUE constraints (excluding the current row) via index lookup
pub fn validate_update_constraints<Acc>(
    ctx: &mut TransactionContext<Acc>,
    schema: &Schema,
    old_values: &[DataType],
    assignments: &HashMap<usize, DataType>,
    indexes: &[IndexHandle],
    row_id: RowId,
) -> RuntimeResult<()>
where
    Acc: TreeWriter + Clone + Default,
{
    // Build the new values array by applying assignments to old values
    let mut new_values: Vec<DataType> = old_values.to_vec();
    for (&value_idx, value) in assignments {
        // value_idx is relative to values (after keys), convert to column index
        let col_idx = value_idx + schema.num_keys();
        if col_idx < new_values.len() {
            new_values[col_idx] = value.clone();
        }
    }

    // Validate NOT NULL
    validate_not_null_constraints(schema, &new_values)?;

    // Validate UNIQUE (but exclude the current row)
    validate_unique_constraints(ctx, schema, &new_values, indexes, Some(row_id))?;

    Ok(())
}
