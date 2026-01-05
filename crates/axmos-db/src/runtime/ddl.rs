//! DDL (Data Definition Language) executor.
//!
//! This module provides execution of DDL statements (CREATE, ALTER, DROP)
//! that modify the database schema. DDL statements bypass the query optimizer
//! and are executed directly against the catalog.

use crate::{
    PageId,
    core::SerializableType,
    runtime::context::TransactionContext,
    schema::{
        base::{Column, ForeignKeyInfo, Relation, Schema, SchemaError, TableConstraint},
        catalog::{CatalogError, CatalogTrait},
    },
    sql::binder::{DatabaseItem, bounds::*},
    storage::{
        page::BtreePage,
        tuple::{Row, TupleBuilder, TupleReader},
    },
    tree::accessor::BtreeWriteAccessor,
    types::{Blob, DataType, DataTypeKind, ObjectId, UInt64},
};

use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
};

/// Errors that can occur during DDL execution.
#[derive(Debug)]
pub enum DdlError {
    /// Object already exists
    AlreadyExists(DatabaseItem),
    /// Object not found
    NotFound(DatabaseItem),

    /// Catalog error
    CatalogError(CatalogError),
    /// Schema error
    SchemaError(SchemaError),
    IoError(IoError),
    /// Other error
    Other(String),
}

impl From<IoError> for DdlError {
    fn from(value: IoError) -> Self {
        Self::IoError(value)
    }
}

impl Display for DdlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyExists(ae) => write!(f, "{} already exists", ae),
            Self::NotFound(nf) => write!(f, "{} not found", nf),
            Self::IoError(t) => write!(f, "io error: {}", t),
            Self::CatalogError(e) => write!(f, "catalog error: {}", e),
            Self::SchemaError(e) => write!(f, "schema error: {}", e),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for DdlError {}

impl From<CatalogError> for DdlError {
    fn from(e: CatalogError) -> Self {
        Self::CatalogError(e)
    }
}

impl From<SchemaError> for DdlError {
    fn from(e: SchemaError) -> Self {
        Self::SchemaError(e)
    }
}

pub type DdlResult<T> = Result<T, DdlError>;

/// Result of a DDL operation.
#[derive(Debug, Clone)]
pub enum DdlOutcome {
    /// Table was created
    TableCreated {
        name: String,
        object_id: ObjectId,
    },
    /// Table was altered
    TableAltered {
        name: String,
        action: String,
    },
    /// Table was dropped
    TableDropped {
        name: String,
    },
    /// Index was created
    IndexCreated {
        name: String,
        object_id: ObjectId,
    },
    /// Index was dropped
    IndexDropped {
        name: String,
    },
    /// Transaction control
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    /// No operation performed (e.g., IF NOT EXISTS when object exists)
    NoOp,
}

impl DdlOutcome {
    /// Convert outcome to a result row for consistency with DML operations.
    pub fn to_row(&self) -> Vec<DataType> {
        let message = match self {
            Self::TableCreated { name, .. } => format!("CREATE TABLE {}", name),
            Self::TableAltered { name, action } => format!("ALTER TABLE {} {}", name, action),
            Self::TableDropped { name } => format!("DROP TABLE {}", name),
            Self::IndexCreated { name, .. } => format!("CREATE INDEX {}", name),
            Self::IndexDropped { name } => format!("DROP INDEX {}", name),
            Self::TransactionStarted => "BEGIN".to_string(),
            Self::TransactionCommitted => "COMMIT".to_string(),
            Self::TransactionRolledBack => "ROLLBACK".to_string(),
            Self::NoOp => "OK".to_string(),
        };
        vec![DataType::Blob(Blob::from(message.as_str()))]
    }
}

/// Executes DDL statements directly against the catalog.
///
/// DDL statements modify the database schema and don't go through
/// the query optimizer. They are executed immediately and affect
/// the catalog metadata.
pub struct DdlExecutor {
    ctx: TransactionContext<BtreeWriteAccessor>,
}

impl DdlExecutor {
    /// Creates a new DDL executor.
    pub(crate) fn new(ctx: TransactionContext<BtreeWriteAccessor>) -> Self {
        Self { ctx }
    }

    /// Check if a statement is a DDL statement.
    pub fn is_ddl(stmt: &BoundStatement) -> bool {
        matches!(
            stmt,
            BoundStatement::CreateTable(_)
                | BoundStatement::CreateIndex(_)
                | BoundStatement::AlterTable(_)
                | BoundStatement::DropTable(_)
                | BoundStatement::Transaction(_)
        )
    }

    /// Execute a DDL statement.
    pub fn execute(&mut self, stmt: &BoundStatement) -> DdlResult<DdlOutcome> {
        match stmt {
            BoundStatement::CreateTable(create) => self.execute_create_table(create),
            BoundStatement::CreateIndex(create) => self.execute_create_index(create),
            BoundStatement::AlterTable(alter) => self.execute_alter_table(alter),
            BoundStatement::DropTable(drop) => self.execute_drop_table(drop),
            BoundStatement::Transaction(tx) => self.execute_transaction(tx),
            _ => Err(DdlError::Other("Not a DDL statement".to_string())),
        }
    }

    fn execute_create_table(&mut self, stmt: &BoundCreateTable) -> DdlResult<DdlOutcome> {
        // Check if table already exists
        if self.relation_exists(&stmt.table_name) {
            if stmt.if_not_exists {
                return Ok(DdlOutcome::NoOp);
            }
            return Err(DdlError::AlreadyExists(DatabaseItem::Table(
                stmt.table_name.clone(),
            )));
        }

        // Build columns from bound definitions, prepending the internal row_id column
        let mut columns: Vec<Column> = Vec::with_capacity(stmt.columns.len() + 1);

        // Add internal row_id column as the first column (primary key)
        columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        // Add user-defined columns
        columns.extend(stmt.columns.iter().map(|c| self.bound_column_to_column(c)));

        // Allocate resources
        let object_id = self.ctx.catalog().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        // Create the relation
        let mut relation = Relation::table(object_id, &stmt.table_name, root_page, columns);

        // Collect column-level unique constraints that need indexes
        // These are columns marked with UNIQUE but not part of a table-level constraint
        let column_level_unique_indices: Vec<(usize, String)> = stmt
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_unique)
            .map(|(idx, col)| (idx + 1, col.name.clone())) // +1 to account for row_id column
            .filter(|(col_idx, _)| {
                // Check if this column is already covered by a table-level constraint
                !stmt.constraints.iter().any(|c| match c {
                    BoundTableConstraint::Unique(indices) => indices.contains(&(col_idx - 1)),
                    BoundTableConstraint::PrimaryKey(indices) => indices.contains(&(col_idx - 1)),
                    _ => false,
                })
            })
            .collect();

        // Apply table-level constraints (indices need to be adjusted by +1 for row_id)
        for constraint in &stmt.constraints {
            let adjusted = self.adjust_constraint_indices(constraint);
            self.apply_table_constraint(
                relation.schema_mut(),
                object_id,
                &stmt.table_name,
                &adjusted,
            )?;
        }

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Store in catalog
        self.ctx
            .catalog()
            .write()
            .store_relation(relation, &tree_builder, snapshot.xid())?;

        // Now create indexes for column-level unique constraints
        // This must happen AFTER storing the relation since create_index_for_constraint
        // needs to look up the table
        for (col_idx, col_name) in column_level_unique_indices {
            let index_name = format!("{}_{}_unique_idx", stmt.table_name, col_name);
            self.create_index_for_constraint(
                object_id,
                &index_name,
                &[col_idx],
                true, // unique
            )?;
        }

        Ok(DdlOutcome::TableCreated {
            name: stmt.table_name.clone(),
            object_id,
        })
    }

    /// Adjusts constraint column indices by +1 to account for the internal row_id column.
    fn adjust_constraint_indices(&self, constraint: &BoundTableConstraint) -> BoundTableConstraint {
        match constraint {
            BoundTableConstraint::PrimaryKey(indices) => {
                BoundTableConstraint::PrimaryKey(indices.iter().map(|i| i + 1).collect())
            }
            BoundTableConstraint::Unique(indices) => {
                BoundTableConstraint::Unique(indices.iter().map(|i| i + 1).collect())
            }
            BoundTableConstraint::ForeignKey {
                columns,
                ref_table_id,
                ref_columns,
            } => BoundTableConstraint::ForeignKey {
                columns: columns.iter().map(|i| i + 1).collect(),
                ref_table_id: *ref_table_id,
                ref_columns: ref_columns.clone(), // ref_columns don't need adjustment
            },
        }
    }

    fn execute_create_index(&mut self, stmt: &BoundCreateIndex) -> DdlResult<DdlOutcome> {
        // Check if index already exists
        if self.relation_exists(&stmt.index_name) {
            if stmt.if_not_exists {
                return Ok(DdlOutcome::NoOp);
            }
            return Err(DdlError::AlreadyExists(DatabaseItem::Index(
                stmt.index_name.clone(),
            )));
        }

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get the table to find column types
        let table_relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(stmt.table_id, &tree_builder, &snapshot)?;

        let table_schema = table_relation.schema().clone();
        let table_root = table_relation.root();

        if stmt.columns.is_empty() {
            return Err(DdlError::Other(
                "Index must have at least one column".to_string(),
            ));
        }

        // Build index columns: indexed columns as keys + row_id as value
        let mut index_columns: Vec<Column> = Vec::with_capacity(stmt.columns.len() + 1);

        for col_ref in &stmt.columns {
            if let Some(table_col) = table_schema.column(col_ref.column_idx) {
                index_columns.push(Column::new_with_defaults(
                    table_col.datatype(),
                    table_col.name(),
                ));
            }
        }

        // Add row_id as the value column
        index_columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        // Allocate resources
        let object_id = self.ctx.catalog().read().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        // Create the index relation
        let index_relation = Relation::index(
            object_id,
            &stmt.index_name,
            root_page,
            index_columns,
            stmt.columns.len(), // num_keys = number of indexed columns
        );

        let index_schema = index_relation.schema().clone();
        let index_root = index_relation.root();

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Store in catalog
        self.ctx
            .catalog()
            .write()
            .store_relation(index_relation, &tree_builder, snapshot.xid())?;

        // Update the table schema to reference this index
        // It is important that the order of the columns is kept.
        let indexed_column_ids: Vec<usize> = stmt.columns.iter().map(|c| c.column_idx).collect();
        self.add_unique_constraint_for_index(stmt.table_id, &indexed_column_ids)?;
        self.add_index_to_table(stmt.table_id, object_id, indexed_column_ids.clone())?;

        // *** POPULATE THE INDEX WITH EXISTING DATA ***
        self.populate_index(
            table_root,
            &table_schema,
            index_root,
            &index_schema,
            &indexed_column_ids,
        )?;

        Ok(DdlOutcome::IndexCreated {
            name: stmt.index_name.clone(),
            object_id,
        })
    }

    /// Populates a newly created index with existing table data.
    ///
    /// This scans all visible rows in the table and inserts corresponding
    /// entries into the index.
    fn populate_index(
        &mut self,
        table_root: PageId,
        table_schema: &Schema,
        index_root: PageId,
        index_schema: &Schema,
        indexed_column_ids: &[usize],
    ) -> DdlResult<()> {
        let snapshot = self.ctx.snapshot();
        let tid = snapshot.xid();
        // Collect rows to insert into index
        let mut index_entries: Vec<Row> = Vec::new();

        {
            // Scan the table and collect all visible rows
            let mut table_btree = self.ctx.build_tree(table_root);

            if table_btree.is_empty().map_err(|_| DdlError::Other("Failed to check tree emptyness".to_string()))? {
                return Ok(())
            };
            let positions: Vec<crate::tree::accessor::BtreePagePosition> = table_btree
                .iter_forward()
                .map_err(|_| DdlError::Other("Failed to create tree iterator".to_string()))?
                .filter(|p| p.is_ok())
                .map(|f| f.unwrap())
                .collect();

            for pos in positions {
                let row = table_btree
                    .get_row_at(pos, table_schema, &snapshot)
                    .map_err(|_| DdlError::Other(format!("failed to get row at position {pos}")))?;

                let Some(row) = row else {
                    continue;
                };

                let row_id = row.first();

                let Some(DataType::BigUInt(UInt64(value))) = row_id else {
                    return Err(DdlError::Other(
                        "Tables should have row id type for the first column!".to_string(),
                    ));
                };

                // Extract indexed column values + row_id
                let mut entry_values: Vec<DataType> =
                    Vec::with_capacity(indexed_column_ids.len() + 1);
                for &col_idx in indexed_column_ids {
                    if col_idx < row.len() {
                        entry_values.push(row[col_idx].clone());
                    }
                }
                entry_values.push(DataType::BigUInt(UInt64(*value)));

                index_entries.push(Row::new(entry_values.into_boxed_slice()));
            }
        }

        // Now insert all entries into the index
        let mut index_btree = self.ctx.build_tree(index_root);
        for entry in index_entries {
            let tuple = TupleBuilder::from_schema(index_schema)
                .build(entry, tid)
                .map_err(|e| DdlError::Other(format!("Failed to build index tuple: {}", e)))?;

            index_btree
                .insert(index_root, tuple, index_schema)
                .map_err(|e| DdlError::Other(format!("Failed to insert into index: {}", e)))?;
        }

        Ok(())
    }

    /// Creates an index to back a constraint (PRIMARY KEY or UNIQUE).
    fn create_index_for_constraint(
        &mut self,
        table_id: ObjectId,
        index_name: &str,
        col_indices: &[usize],
        _unique: bool,
    ) -> DdlResult<ObjectId> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get table schema to build index columns
        let table_relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let table_schema = table_relation.schema().clone();
        let table_root = table_relation.root();

        // Build index columns from the constraint columns + row_id
        let mut index_columns: Vec<Column> = Vec::with_capacity(col_indices.len() + 1);

        for &col_idx in col_indices {
            if let Some(table_col) = table_schema.column(col_idx) {
                index_columns.push(Column::new_with_defaults(
                    table_col.datatype(),
                    table_col.name(),
                ));
            }
        }

        // Add row_id as the value column
        index_columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        // Allocate resources
        let object_id = self.ctx.catalog().read().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        // Create the index relation
        let index_relation = Relation::index(
            object_id,
            index_name,
            root_page,
            index_columns,
            col_indices.len(), // num_keys
        );

        let index_schema = index_relation.schema().clone();
        let index_root = index_relation.root();

        // Store in catalog
        self.ctx
            .catalog()
            .write()
            .store_relation(index_relation, &tree_builder, snapshot.xid())?;

        // Register the index with the table
        self.add_index_to_table(table_id, object_id, col_indices.to_vec())?;

        // Populate the index with existing data
        self.populate_index(
            table_root,
            &table_schema,
            index_root,
            &index_schema,
            col_indices,
        )?;

        Ok(object_id)
    }

    /// Adds an index reference to a table's schema.
    fn add_index_to_table(
        &mut self,
        table_id: ObjectId,
        index_id: ObjectId,
        columns: Vec<usize>,
    ) -> DdlResult<()> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        let mut relation =
            self.ctx
                .catalog()
                .write()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let schema = relation.schema_mut();

        // Add the index to the schema's index map
        if let Some(indexes) = schema.table_indexes.as_mut() {
            indexes.insert(index_id, columns);
        }
        let id = relation.object_id();

        self.ctx.catalog().write().update_relation(
            id,
            None,
            Some(relation.into_schema()),
            None,
            &tree_builder,
            &snapshot,
        )?;

        Ok(())
    }

    /// Adds a unique constraint to a table for the given columns.
    /// Called when creating a UNIQUE index.
    fn add_unique_constraint_for_index(
        &mut self,
        table_id: ObjectId,
        columns: &[usize],
    ) -> DdlResult<()> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let mut relation =
            self.ctx
                .catalog()
                .write()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let schema = relation.schema_mut();

        // Mark individual columns as unique
        for &col_idx in columns {
            if let Some(col) = schema.columns.get_mut(col_idx) {
                col.is_unique = true;
            }
        }

        // Add table-level unique constraint
        if let Some(constraints) = schema.table_constraints.as_mut() {
            // Check if this constraint already exists
            let already_exists = constraints
                .iter()
                .any(|c| matches!(c, TableConstraint::Unique(cols) if cols == columns));

            if !already_exists {
                constraints.push(TableConstraint::Unique(columns.to_vec()));
            }
        }

        let id = relation.object_id();

        self.ctx.catalog().write().update_relation(
            id,
            None,
            Some(relation.into_schema()),
            None,
            &tree_builder,
            &snapshot,
        )?;

        Ok(())
    }

    fn execute_alter_table(&mut self, stmt: &BoundAlterTable) -> DdlResult<DdlOutcome> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        let mut relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(stmt.table_id, &tree_builder, &snapshot)?;

        let table_name = relation.name().to_string();

        if !relation.is_table() {
            return Err(DdlError::CatalogError(CatalogError::TableNotFound(
                stmt.table_id,
            )));
        }

        let action_desc = match &stmt.action {
            BoundAlterAction::AddColumn(col_def) => {
                self.alter_add_column(&mut relation, col_def, &table_name)?
            }
            BoundAlterAction::DropColumn(col_idx) => {
                self.alter_drop_column(&mut relation, *col_idx)?
            }
            BoundAlterAction::AlterColumn {
                column_idx,
                new_type,
                set_default,
                drop_default,
                set_not_null,
                drop_not_null,
            } => self.alter_modify_column(
                &mut relation,
                *column_idx,
                new_type,
                set_default,
                *drop_default,
                *set_not_null,
                *drop_not_null,
            )?,
            BoundAlterAction::AddConstraint(constraint) => {
                self.alter_add_constraint(&mut relation, constraint)?
            }
        };

        let id = relation.object_id();
        // Update the relation in catalog
        self.ctx.catalog().write().update_relation(
            id,
            None,
            Some(relation.into_schema()),
            None,
            &tree_builder,
            &snapshot,
        )?;

        Ok(DdlOutcome::TableAltered {
            name: table_name,
            action: action_desc,
        })
    }

    fn alter_add_column(
        &self,
        relation: &mut Relation,
        col_def: &BoundColumnDef,
        table_name: &str,
    ) -> DdlResult<String> {
        let schema = relation.schema_mut();

        // Check column doesn't already exist
        if schema.column_index.contains_key(&col_def.name) {
            return Err(DdlError::AlreadyExists(DatabaseItem::Column(
                table_name.to_string(),
                col_def.name.clone(),
            )));
        }

        let column = self.bound_column_to_column(col_def);

        // Add to columns vector and update index
        let new_idx = schema.columns.len();
        schema.columns.push(column);
        schema.column_index.insert(col_def.name.clone(), new_idx);

        Ok(format!("ADD COLUMN {}", col_def.name))
    }

    fn alter_drop_column(&self, relation: &mut Relation, col_idx: usize) -> DdlResult<String> {
        let schema = relation.schema_mut();

        let column_name = schema
            .column(col_idx)
            .ok_or_else(|| DdlError::Other(format!("Column index {} out of bounds", col_idx)))?
            .name()
            .to_string();

        // Cannot drop key columns
        if col_idx < schema.num_keys() {
            return Err(DdlError::Other(format!(
                "Cannot drop key column '{}'",
                column_name
            )));
        }

        // Remove from columns vector
        schema.columns.remove(col_idx);

        // Rebuild column index
        schema.column_index = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name().to_string(), i))
            .collect();

        Ok(format!("DROP COLUMN {}", column_name))
    }

    fn alter_modify_column(
        &self,
        relation: &mut Relation,
        column_idx: usize,
        new_type: &Option<DataTypeKind>,
        set_default: &Option<BoundExpression>,
        drop_default: bool,
        set_not_null: bool,
        drop_not_null: bool,
    ) -> DdlResult<String> {
        let schema = relation.schema_mut();

        let column = schema
            .columns
            .get_mut(column_idx)
            .ok_or_else(|| DdlError::Other(format!("Column index {} out of bounds", column_idx)))?;

        let column_name = column.name().to_string();

        if let Some(dtype) = new_type {
            column.dtype = *dtype as u8;
        }

        if let Some(default_expr) = set_default {
            if let Some(default_value) = self.eval_literal_expr(default_expr) {
                // Serialize the default value
                let required_size = default_value.runtime_size();
                let mut writer = vec![0u8; required_size];
                if default_value.write_to(&mut writer, 0).is_ok() {
                    column.default = Some(writer.into_boxed_slice());
                }
            }
        }

        if drop_default {
            column.default = None;
        }

        if set_not_null {
            column.is_non_null = true;
        }

        if drop_not_null {
            column.is_non_null = false;
        }

        Ok(format!("ALTER COLUMN {}", column_name))
    }

    fn alter_add_constraint(
        &mut self,
        relation: &mut Relation,
        constraint: &BoundTableConstraint,
    ) -> DdlResult<String> {
        let id = relation.object_id();
        let name = relation.name().to_string();
        let schema = relation.schema_mut();

        self.apply_table_constraint(schema, id, &name, constraint)
    }

    /// Applies a bound table constraint to the schema.
    fn apply_table_constraint(
        &mut self,
        schema: &mut Schema,
        table_id: ObjectId,
        table_name: &str,
        constraint: &BoundTableConstraint,
    ) -> DdlResult<String> {
        match constraint {
            BoundTableConstraint::PrimaryKey(col_indices) => {
                // Check if already has PK
                if schema.has_primary_key() {
                    return Err(DdlError::SchemaError(SchemaError::DuplicatePrimaryKey));
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| schema.column(idx).map(|c| c.name().to_string()))
                    .collect();

                let constraint_name = format!("{}_pkey", col_names.join("_"));

                // Add the constraint
                if let Some(constraints) = schema.table_constraints.as_mut() {
                    constraints.push(TableConstraint::PrimaryKey(col_indices.clone()));
                }

                // Update num_keys if needed
                if let Some(&first_pk) = col_indices.first() {
                    schema.num_keys = first_pk + 1;
                }

                let index_name = format!("{}_pkey_idx", table_name);
                self.create_index_for_constraint(
                    table_id,
                    &index_name,
                    col_indices,
                    true, // unique
                )?;

                Ok(format!("ADD CONSTRAINT {}", constraint_name))
            }

            BoundTableConstraint::Unique(col_indices) => {
                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| schema.column(idx).map(|c| c.name().to_string()))
                    .collect();

                let constraint_name = format!("{}_unique", col_names.join("_"));

                // Mark columns as unique
                for &idx in col_indices {
                    if let Some(col) = schema.columns.get_mut(idx) {
                        col.is_unique = true;
                    }
                }

                // Add the constraint
                if let Some(constraints) = schema.table_constraints.as_mut() {
                    constraints.push(TableConstraint::Unique(col_indices.clone()));
                }

                let index_name = format!("{}_uniq_idx", table_name);
                self.create_index_for_constraint(
                    table_id,
                    &index_name,
                    col_indices,
                    true, // unique
                )?;

                Ok(format!("ADD CONSTRAINT {}", constraint_name))
            }

            BoundTableConstraint::ForeignKey {
                columns,
                ref_table_id,
                ref_columns,
            } => {
                let snapshot = self.ctx.snapshot();
                let tree_builder = self.ctx.tree_builder();
                // Get referenced table name for constraint naming
                let ref_table_name = self
                    .ctx
                    .catalog()
                    .read()
                    .get_relation(*ref_table_id, &tree_builder, &snapshot)
                    .map(|r| r.name().to_string())
                    .unwrap_or_else(|_| "unknown".to_string());

                let col_names: Vec<String> = columns
                    .iter()
                    .filter_map(|&idx| schema.column(idx).map(|c| c.name().to_string()))
                    .collect();

                let constraint_name = format!("{}_{}_fkey", ref_table_name, col_names.join("_"));

                // Create FK info
                let fk_info =
                    ForeignKeyInfo::for_table_and_columns(*ref_table_id, ref_columns.clone());

                // Add the constraint
                if let Some(constraints) = schema.table_constraints.as_mut() {
                    constraints.push(TableConstraint::ForeignKey {
                        columns: columns.clone(),
                        info: fk_info,
                    });
                }

                Ok(format!("ADD CONSTRAINT {}", constraint_name))
            }
        }
    }

    fn execute_drop_table(&mut self, stmt: &BoundDropTable) -> DdlResult<DdlOutcome> {
        let Some(table_id) = stmt.table_id else {
            if stmt.if_exists {
                return Ok(DdlOutcome::NoOp);
            }
            return Err(DdlError::NotFound(DatabaseItem::Table(
                stmt.table_name.clone(),
            )));
        };

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get the relation
        let relation = self
            .ctx
            .catalog()
            .read()
            .get_relation(table_id, &tree_builder, &snapshot)
            .map_err(|_| DdlError::NotFound(DatabaseItem::Table(stmt.table_name.clone())))?;

        let table_name = relation.name().to_string();

        // Remove the relation (cascade handled by catalog)
        self.ctx.catalog().write().remove_relation(
            relation,
            &tree_builder,
            &snapshot,
            stmt.cascade,
        )?;

        Ok(DdlOutcome::TableDropped { name: table_name })
    }

    fn execute_transaction(&self, stmt: &BoundTransaction) -> DdlResult<DdlOutcome> {
        // Transaction control is typically handled at a higher level
        // (TransactionCoordinator), but we return appropriate outcomes
        match stmt {
            BoundTransaction::Begin => Ok(DdlOutcome::TransactionStarted),
            BoundTransaction::Commit => Ok(DdlOutcome::TransactionCommitted),
            BoundTransaction::Rollback => Ok(DdlOutcome::TransactionRolledBack),
        }
    }

    /// Check if any relation exists by name.
    fn relation_exists(&self, name: &str) -> bool {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        self.ctx
            .catalog()
            .read()
            .get_relation_by_name(name, &tree_builder, &snapshot)
            .is_ok()
    }

    /// Convert a bound column definition to a schema column.
    fn bound_column_to_column(&self, col_def: &BoundColumnDef) -> Column {
        let mut col = Column::new_with_defaults(col_def.data_type, &col_def.name);

        col.is_non_null = col_def.is_non_null;
        col.is_unique = col_def.is_unique;

        if let Some(ref default_expr) = col_def.default {
            if let Some(default_value) = self.eval_literal_expr(default_expr) {
                // Serialize the default value
                let required_size = default_value.runtime_size();
                let mut writer = vec![0u8; required_size];
                if default_value.write_to(&mut writer, 0).is_ok() {
                    col.default = Some(writer.into_boxed_slice());
                }
            }
        }

        col
    }

    /// Evaluate a literal expression to get a DataType value.
    /// Only supports literal values for DEFAULT constraints.
    fn eval_literal_expr(&self, expr: &BoundExpression) -> Option<DataType> {
        match expr {
            BoundExpression::Literal { value } => Some(value.clone()),
            _ => None, // Non-literal defaults are not supported yet
        }
    }
}
