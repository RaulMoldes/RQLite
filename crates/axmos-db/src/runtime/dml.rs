//! DML (Data Manipulation Language) executor for single-row operations.
//!
//! This module provides a unified interface for Insert, Update, and Delete operations
//! on individual rows, including automatic maintenance of secondary indexes.

use std::collections::{HashMap, HashSet};

use crate::{
    core::SerializableType,
    runtime::{
        RuntimeError, RuntimeResult, TypeSystemError, context::TransactionContext,
        eval::ExpressionEvaluator,
    },
    schema::{Schema, base::IndexHandle, catalog::CatalogTrait},
    sql::binder::bounds::BoundExpression,
    storage::tuple::{Row, Tuple, TupleBuilder, TupleReader},
    tree::{accessor::TreeWriter, bplustree::SearchResult},
    types::{DataType, ObjectId, RowId, UInt64},
};

/// Result of an insert operation.
pub struct InsertResult {
    /// The row ID assigned to the inserted row.
    pub row_id: RowId,
}

/// Result of an update operation.
pub struct UpdateResult {
    /// Whether the row was successfully updated.
    pub updated: bool,
}

/// Result of a delete operation.
pub struct DeleteResult {
    /// Whether the row was successfully deleted.
    pub deleted: bool,
}

/// Executes single-row DML operations with automatic index maintenance.
///
/// This component provides a clean interface for Insert, Update, and Delete
/// operations.
pub(crate) struct DmlExecutor<'ctx, Acc>
where
    Acc: TreeWriter + Clone + Default,
{
    ctx: &'ctx mut TransactionContext<Acc>,
}

impl<'ctx, Acc> DmlExecutor<'ctx, Acc>
where
    Acc: TreeWriter + Clone + Default,
{
    /// Creates a new DML executor with the given transaction context.
    pub(crate) fn new(ctx: &'ctx mut TransactionContext<Acc>) -> Self {
        Self { ctx }
    }

    // Public API

    /// Inserts a new row into the specified table.
    ///
    /// # Arguments
    /// * `table_id` - The ID of the target table
    /// * `columns` - The column indices being inserted (maps input position to schema position)
    /// * `values` - The row of values to insert
    ///
    /// # Returns
    /// The row ID assigned to the inserted row.
    pub(crate) fn insert(
        &mut self,
        table_id: ObjectId,
        columns: &[usize],
        values: &Row,
    ) -> RuntimeResult<InsertResult> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let tid = self.ctx.tid();

        // Get the relation and allocate a new row ID
        let mut relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let row_id = relation.next_row_id();
        relation.increment_row_id();

        let schema = relation.schema().clone();
        let root = relation.root();

        // Build the full row with the assigned row ID
        let full_row = self.build_full_row(&schema, columns, values, row_id)?;
        let full_values: Vec<DataType> = full_row.iter().cloned().collect();

        // Create and insert the tuple
        let tuple = TupleBuilder::from_schema(&schema).build(full_row, tid)?;

        // Log the insert operation
        self.ctx
            .log_insert(table_id, row_id.value(), Box::from(&tuple))?;

        // Insert into main table
        let mut btree = self.ctx.build_tree(root);
        btree.insert(root, tuple, &schema)?;
        btree.accessor_mut()?.clear();

        // Maintain secondary indexes
        self.insert_index_entries(&relation.get_indexes(), &full_values, row_id.value())?;

        // Update relation metadata
        self.ctx.catalog().write().update_relation(
            relation.object_id(),
            Some(relation.next_row_id().value()),
            None,
            None,
            &tree_builder,
            &snapshot,
        )?;

        Ok(InsertResult {
            row_id: row_id.value(),
        })
    }

    /// Updates a row identified by its row ID.
    ///
    /// # Arguments
    /// * `table_id` - The ID of the target table
    /// * `row_id` - The row ID to update (extracted from the scanned row)
    /// * `assignments` - Map of column indices to new values (uses value_idx, not column_idx)
    ///
    /// # Returns
    /// Whether the update was successful.
    pub fn update(
        &mut self,
        table_id: ObjectId,
        row_id: &UInt64,
        assignments: &HashMap<usize, DataType>,
    ) -> RuntimeResult<UpdateResult> {
        let row_id_bytes = row_id.serialize()?;
        let tid = self.ctx.tid();
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get relation and schema
        let relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;
        let schema = relation.schema().clone();
        let root = relation.root();

        // Search for the tuple
        let mut btree = self.ctx.build_tree(root);
        let position = match btree.search(&row_id_bytes, &schema)? {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(UpdateResult { updated: false }),
        };

        // Read the existing tuple
        let tuple_reader = TupleReader::from_schema(&schema);
        let existing_tuple = btree.with_cell_at(position, |bytes| {
            tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
            Tuple::from_slice_unchecked(bytes).ok()
        })?;

        let Some(tuple) = existing_tuple else {
            self.ctx.accessor_mut().clear();
            return Ok(UpdateResult { updated: false });
        };

        // Get the old row for index maintenance
        let old_row = btree
            .get_row_at(position, &schema, &snapshot)?
            .expect("Tuple should exist");

        // Apply the update
        let mut updated_tuple = tuple.clone();
        updated_tuple.add_version_with_schema(assignments, tid, &schema)?;
        updated_tuple.vacuum_with_schema(snapshot.xmin(), &schema)?;

        // Get the new row for index maintenance
        let new_row = updated_tuple
            .as_tuple_ref(&schema, &snapshot)
            .expect("Updated tuple should be visible")
            .to_row_with_schema(&schema)?;

        // Log the update
        self.ctx.log_update(
            table_id,
            row_id.value(),
            Box::from(&tuple),
            Box::from(&updated_tuple),
        )?;

        // Update the main table
        btree.update(root, updated_tuple, &schema)?;

        // Maintain secondary indexes
        self.update_index_entries(
            &relation.get_indexes(),
            &old_row.clone().into_inner(),
            &new_row.into_inner(),
            assignments,
            &schema,
            row_id.value(),
        )?;

        Ok(UpdateResult { updated: true })
    }

    /// Deletes a row identified by its row ID.
    ///
    /// # Arguments
    /// * `table_id` - The ID of the target table
    /// * `row_id` - The row ID to delete (extracted from the scanned row)
    ///
    /// # Returns
    /// Whether the delete was successful.
    pub fn delete(&mut self, table_id: ObjectId, row_id: &UInt64) -> RuntimeResult<DeleteResult> {
        let row_id_bytes = row_id.serialize()?;
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get relation and schema
        let relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;
        let schema = relation.schema().clone();
        let root = relation.root();

        // Search for the tuple
        let mut btree = self.ctx.build_tree(root);
        let position = match btree.search(&row_id_bytes, &schema)? {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(DeleteResult { deleted: false }),
        };

        // Read the existing tuple
        let tuple_reader = TupleReader::from_schema(&schema);
        let existing_tuple = btree.with_cell_at(position, |bytes| {
            tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
            Tuple::from_slice_unchecked(bytes).ok()
        })?;

        let Some(tuple) = existing_tuple else {
            self.ctx.accessor_mut().clear();
            return Ok(DeleteResult { deleted: false });
        };

        // Get the row for index maintenance
        let old_row = btree
            .get_row_at(position, &schema, &snapshot)?
            .expect("Tuple should exist");

        // Mark as deleted
        let mut deleted_tuple = tuple.clone();
        deleted_tuple.delete(snapshot.xid())?;
        deleted_tuple.vacuum_with_schema(snapshot.xmin(), &schema)?;

        // Log the delete
        self.ctx
            .log_delete(table_id, row_id.value(), Box::from(&tuple))?;

        // Update the main table
        btree.update(root, deleted_tuple, &schema)?;

        // Maintain secondary indexes
        self.delete_index_entries(
            &relation.get_indexes(),
            &old_row.into_inner(),
            row_id.value(),
        )?;

        Ok(DeleteResult { deleted: true })
    }

    /// Builds a full row with default values, mapping input columns to schema positions.
    fn build_full_row(
        &self,
        schema: &Schema,
        columns: &[usize],
        values: &Row,
        row_id: UInt64,
    ) -> RuntimeResult<Row> {
        let num_cols = schema.num_columns();
        let mut full_values: Vec<DataType> = vec![DataType::Null; num_cols];

        // First column is always the row ID (primary key)
        full_values[0] = DataType::BigUInt(row_id);

        // Map input columns to schema positions with type casting
        for (input_idx, &schema_col_idx) in columns.iter().enumerate() {
            if input_idx < values.len() && schema_col_idx < num_cols {
                let expected_type = schema
                    .column(schema_col_idx)
                    .ok_or(RuntimeError::ColumnNotFound(schema_col_idx))?
                    .datatype();
                let casted = values[input_idx].try_cast(expected_type)?;
                full_values[schema_col_idx] = casted;
            }
        }

        Ok(Row::new(full_values.into_boxed_slice()))
    }

    /// Builds an index entry row from table values.
    fn build_index_entry(
        values: &[DataType],
        index: &IndexHandle,
        index_schema: &Schema,
        row_id: RowId,
    ) -> RuntimeResult<Row> {
        let mut entry: Vec<DataType> = Vec::with_capacity(index_schema.num_keys() + 1);

        for col_idx in index.indexed_column_ids() {
            let value = values
                .get(*col_idx)
                .ok_or(RuntimeError::ColumnNotFound(*col_idx))?;
            entry.push(value.clone());
        }

        // Append row ID as the last column
        entry.push(DataType::BigUInt(UInt64(row_id)));

        Ok(Row::new(entry.into_boxed_slice()))
    }

    /// Inserts entries into all secondary indexes for a new row.
    fn insert_index_entries(
        &mut self,
        indexes: &[IndexHandle],
        values: &[DataType],
        row_id: RowId,
    ) -> RuntimeResult<()> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let tid = self.ctx.tid();

        for index in indexes {
            let index_relation =
                self.ctx
                    .catalog()
                    .read()
                    .get_relation(index.id(), &tree_builder, &snapshot)?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            // Build and insert the index entry
            let index_row = Self::build_index_entry(values, index, index_schema, row_id)?;
            let index_tuple = TupleBuilder::from_schema(index_schema).build(index_row, tid)?;

            let mut index_btree = self.ctx.build_tree(index_root);
            index_btree.insert(index_root, index_tuple, index_schema)?;
            index_btree.accessor_mut()?.clear();
        }

        Ok(())
    }

    /// Updates entries in secondary indexes affected by the update.
    fn update_index_entries(
        &mut self,
        indexes: &[IndexHandle],
        old_values: &[DataType],
        new_values: &[DataType],
        assignments: &HashMap<usize, DataType>,
        table_schema: &Schema,
        row_id: RowId,
    ) -> RuntimeResult<()> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let tid = self.ctx.tid();

        // Determine which columns were modified
        let modified_columns: HashSet<usize> = assignments.keys().copied().collect();

        for index in indexes {
            // Skip indexes not affected by this update
            let index_affected = index
                .indexed_column_ids()
                .iter()
                .any(|col| modified_columns.contains(col));

            if !index_affected {
                continue;
            }

            let index_relation =
                self.ctx
                    .catalog()
                    .read()
                    .get_relation(index.id(), &tree_builder, &snapshot)?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            // Build the old index key for searching
            let old_index_row = Self::build_index_entry(old_values, index, index_schema, row_id)?;
            let old_index_tuple =
                TupleBuilder::from_schema(index_schema).build(old_index_row, tid)?;

            // Build new assignments for the index
            let index_assignments =
                self.build_index_assignments(old_values, assignments, index, index_schema)?;

            // Search and update the index entry
            let mut index_btree = self.ctx.build_tree(index_root);
            let search_result = index_btree.search_tuple(&old_index_tuple, index_schema)?;

            if let SearchResult::Found(pos) = search_result {
                let tuple_reader = TupleReader::from_schema(index_schema);
                let updated = index_btree.with_cell_at(pos, |bytes| {
                    tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
                    let mut tuple = Tuple::from_slice_unchecked(bytes).ok()?;
                    tuple
                        .add_version_with_schema(&index_assignments, snapshot.xid(), index_schema)
                        .ok()?;
                    let _ = tuple.vacuum_with_schema(snapshot.xmin(), index_schema);
                    Some(tuple)
                })?;

                if let Some(tuple) = updated {
                    index_btree.update(index_root, tuple, index_schema)?;
                }
            }
        }

        self.ctx.accessor_mut().clear();
        Ok(())
    }

    /// Builds assignment map for an index based on table assignments.
    fn build_index_assignments(
        &self,
        old_values: &[DataType],
        table_assignments: &HashMap<usize, DataType>,
        index: &IndexHandle,
        index_schema: &Schema,
    ) -> RuntimeResult<HashMap<usize, DataType>> {
        let mut index_assignments: HashMap<usize, DataType> = HashMap::new();

        for (index_col_idx, table_col_idx) in index.indexed_column_ids().iter().enumerate() {
            // Check if this table column was modified
            let Some(new_value) = table_assignments.get(table_col_idx) else {
                continue;
            };

            // Validate type compatibility
            let index_col_type = index_schema
                .column(index_col_idx)
                .ok_or(RuntimeError::ColumnNotFound(index_col_idx))?
                .datatype();

            if index_col_type != new_value.kind() {
                return Err(RuntimeError::TypeError(
                    TypeSystemError::UnexpectedDataType(new_value.kind()),
                ));
            }

            index_assignments.insert(index_col_idx, new_value.clone());
        }

        Ok(index_assignments)
    }

    /// Marks entries as deleted in all secondary indexes for a deleted row.
    fn delete_index_entries(
        &mut self,
        indexes: &[IndexHandle],
        values: &[DataType],
        row_id: RowId,
    ) -> RuntimeResult<()> {
        let tree_builder = self.ctx.tree_builder();
        let snapshot = self.ctx.snapshot();
        let tid = self.ctx.tid();

        for index in indexes {
            let index_relation =
                self.ctx
                    .catalog()
                    .read()
                    .get_relation(index.id(), &tree_builder, &snapshot)?;
            let index_schema = index_relation.schema();
            let index_root = index_relation.root();

            // Build the index key for searching
            let index_row = Self::build_index_entry(values, index, index_schema, row_id)?;
            let index_tuple = TupleBuilder::from_schema(index_schema).build(index_row, tid)?;

            // Search and mark as deleted
            let mut index_btree = self.ctx.build_tree(index_root);
            let search_result = index_btree.search_tuple(&index_tuple, index_schema)?;

            if let SearchResult::Found(pos) = search_result {
                let tuple_reader = TupleReader::from_schema(index_schema);
                let deleted = index_btree.with_cell_at(pos, |bytes| {
                    tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
                    let mut tuple = Tuple::from_slice_unchecked(bytes).ok()?;
                    tuple.delete(tid).ok()?;
                    let _ = tuple.vacuum_with_schema(snapshot.xmin(), index_schema);
                    Some(tuple)
                })?;

                if let Some(tuple) = deleted {
                    index_btree.update(index_root, tuple, index_schema)?;
                }
            }
        }

        self.ctx.accessor_mut().clear();
        Ok(())
    }
}

/// Extracts the row ID from a row, assuming it's in the first column.
pub(crate) fn extract_row_id(row: &Row) -> RuntimeResult<&UInt64> {
    row[0]
        .as_big_u_int()
        .ok_or_else(|| RuntimeError::TypeError(TypeSystemError::UnexpectedDataType(row[0].kind())))
}

/// Evaluates assignment expressions and converts column indices to value indices.
///
/// # Arguments
/// * `assignments` - Iterator of (column_idx, expression) pairs
/// * `row` - The current row to evaluate expressions against
/// * `schema` - The table schema
///
/// # Returns
/// A map of value_idx -> evaluated_value suitable for `add_version_with_schema`.
pub(crate) fn evaluate_assignments<'a>(
    assignments: impl Iterator<Item = &'a (usize, BoundExpression)>,
    row: &Row,
    schema: &Schema,
) -> RuntimeResult<HashMap<usize, DataType>> {
    let mut result = HashMap::new();
    let evaluator = ExpressionEvaluator::new(row, schema);
    let num_keys = schema.num_keys();

    for (column_idx, expr) in assignments {
        // Validate we're not updating a key column
        if *column_idx < num_keys {
            return Err(RuntimeError::CannotUpdateKeyColumn);
        }

        // Evaluate and cast
        let evaluated = evaluator.evaluate(expr)?;
        let expected_type = schema
            .column(*column_idx)
            .ok_or(RuntimeError::ColumnNotFound(*column_idx))?
            .datatype();
        let casted = evaluated.try_cast(expected_type)?;

        // Convert to value index
        let value_idx = column_idx - num_keys;
        result.insert(value_idx, casted);
    }

    Ok(result)
}
