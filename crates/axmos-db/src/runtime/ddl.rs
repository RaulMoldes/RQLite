//! DDL (Data Definition Language) executor.
//!
//! This module provides execution of DDL statements (CREATE, ALTER, DROP)
//! that modify the database schema. DDL statements bypass the query optimizer
//! and are executed directly against the catalog.

use crate::{
    io::pager::{BtreeBuilder, SharedPager},
    multithreading::coordinator::Snapshot,
    schema::{
        base::{Column, ForeignKeyInfo, Relation, Schema, SchemaError, TableConstraint},
        catalog::{CatalogError, CatalogTrait},
    },
    sql::binder::{DatabaseItem, bounds::*},
    storage::page::BtreePage,
    types::{Blob, DataType, DataTypeKind, ObjectId},
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
pub struct DdlExecutor<'a, C: CatalogTrait> {
    catalog: &'a mut C,
    tree_builder: &'a BtreeBuilder,
    snapshot: &'a Snapshot,
    pager: SharedPager,
}

impl<'a, C: CatalogTrait> DdlExecutor<'a, C> {
    /// Creates a new DDL executor.
    pub(crate) fn new(
        catalog: &'a mut C,
        tree_builder: &'a BtreeBuilder,
        snapshot: &'a Snapshot,
        pager: SharedPager,
    ) -> Self {
        Self {
            catalog,
            tree_builder,
            snapshot,
            pager,
        }
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

        // Build columns from bound definitions
        let columns: Vec<Column> = stmt
            .columns
            .iter()
            .map(|c| self.bound_column_to_column(c))
            .collect();

        // Allocate resources
        let object_id = self.catalog.get_next_object_id();
        let root_page = self.pager.write().allocate_page::<BtreePage>()?;

        // Create the relation
        let mut relation = Relation::table(object_id, &stmt.table_name, root_page, columns);

        // Apply table-level constraints
        for constraint in &stmt.constraints {
            self.apply_table_constraint(relation.schema_mut(), constraint)?;
        }

        // Store in catalog
        self.catalog
            .store_relation(relation, self.tree_builder, self.snapshot.xid())?;

        Ok(DdlOutcome::TableCreated {
            name: stmt.table_name.clone(),
            object_id,
        })
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

        // Get the table to find column types
        let table_relation =
            self.catalog
                .get_relation(stmt.table_id, self.tree_builder, self.snapshot)?;

        let table_schema = table_relation.schema();

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
        let object_id = self.catalog.get_next_object_id();
        let root_page = self.pager.write().allocate_page::<BtreePage>()?;

        // Create the index relation
        let relation = Relation::index(
            object_id,
            &stmt.index_name,
            root_page,
            index_columns,
            stmt.columns.len(), // num_keys = number of indexed columns
        );

        // Store in catalog
        self.catalog
            .store_relation(relation, self.tree_builder, self.snapshot.xid())?;

        // Update the table schema to reference this index
        // It is important that the order of the columns is kept.
        let indexed_column_ids: Vec<usize> = stmt.columns.iter().map(|c| c.column_idx).collect();
        self.add_index_to_table(stmt.table_id, object_id, indexed_column_ids)?;

        Ok(DdlOutcome::IndexCreated {
            name: stmt.index_name.clone(),
            object_id,
        })
    }

    /// Adds an index reference to a table's schema.
    fn add_index_to_table(
        &mut self,
        table_id: ObjectId,
        index_id: ObjectId,
        columns: Vec<usize>,
    ) -> DdlResult<()> {
        let mut relation = self
            .catalog
            .get_relation(table_id, self.tree_builder, self.snapshot)?;

        let schema = relation.schema_mut();

        // Add the index to the schema's index map
        if let Some(indexes) = schema.table_indexes.as_mut() {
            indexes.insert(index_id, columns);
        }

        self.catalog
            .update_relation(relation, self.tree_builder, self.snapshot)?;

        Ok(())
    }

    fn execute_alter_table(&mut self, stmt: &BoundAlterTable) -> DdlResult<DdlOutcome> {
        let mut relation =
            self.catalog
                .get_relation(stmt.table_id, self.tree_builder, self.snapshot)?;

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

        // Update the relation in catalog
        self.catalog
            .update_relation(relation, self.tree_builder, self.snapshot)?;

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
        &self,
        relation: &mut Relation,
        constraint: &BoundTableConstraint,
    ) -> DdlResult<String> {
        let schema = relation.schema_mut();
        self.apply_table_constraint(schema, constraint)
    }

    /// Applies a bound table constraint to the schema.
    fn apply_table_constraint(
        &self,
        schema: &mut Schema,
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

                Ok(format!("ADD CONSTRAINT {}", constraint_name))
            }

            BoundTableConstraint::ForeignKey {
                columns,
                ref_table_id,
                ref_columns,
            } => {
                // Get referenced table name for constraint naming
                let ref_table_name = self
                    .catalog
                    .get_relation(*ref_table_id, self.tree_builder, self.snapshot)
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

        // Get the relation
        let relation = self
            .catalog
            .get_relation(table_id, self.tree_builder, self.snapshot)
            .map_err(|_| DdlError::NotFound(DatabaseItem::Table(stmt.table_name.clone())))?;

        let table_name = relation.name().to_string();

        // Remove the relation (cascade handled by catalog)
        self.catalog
            .remove_relation(relation, self.tree_builder, self.snapshot, stmt.cascade)?;

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
        self.catalog
            .get_relation_by_name(name, self.tree_builder, self.snapshot)
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
