//! DDL (Data Definition Language) executor.
//!
//! This module provides execution of DDL statements (CREATE, ALTER, DROP)
//! that modify the database schema. DDL statements bypass the query optimizer
//! and are executed directly against the catalog.
use crate::{
    PageId,
    runtime::{RuntimeError, RuntimeResult, context::ThreadContext, eval::eval_literal_expr},
    schema::{
        DatabaseItem,
        base::{Column, ForeignKeyInfo, Relation, Schema, SchemaError, TableConstraint},
        catalog::{CatalogError, CatalogTrait},
    },
    sql::binder::bounds::*,
    storage::{
        page::BtreePage,
        tuple::{Row, TupleBuilder},
    },
    types::{Blob, DataType, DataTypeKind, ObjectId, UInt64},
};

use std::fmt::{Display, Formatter, Result as FmtResult};

/// Result of a DDL operation.
#[derive(Debug, Clone)]
pub enum DdlResult {
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

    /// Transaction management,
    TransactionCommitted,
    TransactionStarted,
    TransactionRolledBack,

    /// No operation performed
    NoOp,
}

impl Display for DdlResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::TableCreated { name, .. } => write!(f, "CREATE TABLE {}", name),
            Self::TableAltered { name, action } => write!(f, "ALTER TABLE {} {}", name, action),
            Self::TableDropped { name } => write!(f, "DROP TABLE {}", name),
            Self::IndexCreated { name, .. } => write!(f, "CREATE INDEX {}", name),
            Self::IndexDropped { name } => write!(f, "DROP INDEX {}", name),
            Self::TransactionCommitted => f.write_str("COMMIT"),
            Self::TransactionStarted => f.write_str("BEGIN"),
            Self::TransactionRolledBack => f.write_str("ROLLBACK"),
            Self::NoOp => f.write_str("OK"),
        }
    }
}

impl DdlResult {
    /// Convert outcome to a result row for consistency with DML operations.
    pub fn to_row(&self) -> Row {
        Row::from(vec![DataType::Blob(Blob::from(self.to_string()))])
    }
}

fn index_name(col_names: &Vec<String>, table_name: &str) -> String {
    format!("{}_{}_unique_idx", table_name, col_names.join("_"))
}

/// Executes DDL statements directly against the catalog.
///
/// DDL statements modify the database schema and don't go through
/// the query optimizer. They are executed immediately and affect
/// the catalog metadata.
pub struct DdlExecutor {
    ctx: ThreadContext,
}

impl DdlExecutor {
    /// Creates a new DDL executor.
    pub(crate) fn new(ctx: ThreadContext) -> Self {
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
    pub fn execute(&mut self, stmt: &BoundStatement) -> RuntimeResult<DdlResult> {
        match stmt {
            BoundStatement::CreateTable(create) => self.execute_create_table(create),
            BoundStatement::CreateIndex(create) => self.execute_create_index(create),
            BoundStatement::AlterTable(alter) => self.execute_alter_table(alter),
            BoundStatement::DropTable(drop) => self.execute_drop_table(drop),
            _ => Err(RuntimeError::InvalidStatement),
        }
    }

    /// Executes [CREATE TABLE] statements.
    fn execute_create_table(&mut self, stmt: &BoundCreateTable) -> RuntimeResult<DdlResult> {
        // Check if table already exists
        if self.relation_exists(&stmt.table_name) {
            if stmt.if_not_exists {
                return Ok(DdlResult::NoOp);
            }
            return Err(RuntimeError::AlreadyExists(DatabaseItem::Table(
                stmt.table_name.clone(),
            )));
        }

        // Build columns from bound definitions, prepending the internal row_id column
        let mut columns: Vec<Column> = Vec::with_capacity(stmt.columns.len() + 1);

        // Add internal row_id column as the first column
        columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        // Add user-defined columns
        columns.extend(stmt.columns.iter().map(|c| Column::from(c)));

        // Allocate resources
        let object_id = self.ctx.catalog().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        // Create the relation
        let relation = Relation::table(object_id, &stmt.table_name, root_page, columns);

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        let schema = relation.schema().clone();
        // Store in catalog
        self.ctx
            .catalog()
            .write()
            .store_relation(relation, &tree_builder, snapshot.xid())?;

        let mut relation = self
            .ctx
            .catalog()
            .get_relation(object_id, &tree_builder, &snapshot)?;

        // Now create indexes for column-level unique constraints
        // This must happen AFTER storing the relation since [create_unique_index] needs to look it up in the catalog.
        for constraint in stmt.constraints.iter() {
            self.add_constraint(&mut relation, constraint)?;
        }

        Ok(DdlResult::TableCreated {
            name: stmt.table_name.clone(),
            object_id,
        })
    }

    fn execute_create_index(&mut self, stmt: &BoundCreateIndex) -> RuntimeResult<DdlResult> {
        // Check if index already exists
        if self.relation_exists(&stmt.index_name) {
            if stmt.if_not_exists {
                return Ok(DdlResult::NoOp);
            }
            return Err(RuntimeError::AlreadyExists(DatabaseItem::Index(
                stmt.index_name.clone(),
            )));
        }

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        if stmt.columns.is_empty() {
            return Err(RuntimeError::Other(
                "Index must have at least one column".to_string(),
            ));
        }

        let index_id = self.create_unique_index(stmt.table_id, &stmt.index_name, &stmt.columns)?;

        Ok(DdlResult::IndexCreated {
            name: stmt.index_name.to_string(),
            object_id: index_id,
        })
    }

    /// Creates a unique index on a given table and applies unique constraints for the indexed columns.
    fn create_unique_index(
        &mut self,
        table_id: ObjectId,
        index_name: &str,
        indexed_column_ids: &[usize],
    ) -> RuntimeResult<ObjectId> {
        let num_keys = indexed_column_ids.len();
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get the table to find column types
        let mut table_relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let table_root = table_relation.root();

        // Build indexed column list: indexed columns as keys + row_id as value
        let mut index_columns: Vec<Column> = Vec::with_capacity(indexed_column_ids.len() + 1);

        // Populate the list of indexed columns.
        {
            let table_schema = table_relation.schema();
            for idx in indexed_column_ids {
                if let Some(column) = table_schema.column(*idx) {
                    index_columns.push(Column::new_with_defaults(column.datatype(), column.name()));
                }
            }
        }

        // Add row_id as the value column
        index_columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        // Allocate resources
        let object_id = self.ctx.catalog().read().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        // Create the index relation
        let index_relation =
            Relation::index(object_id, &index_name, root_page, index_columns, num_keys);

        let index_root = index_relation.root();

        // Clone the schema as we need it to populate the index later.
        let index_schema = index_relation.schema().clone();
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Store in catalog
        self.ctx
            .catalog()
            .write()
            .store_relation(index_relation, &tree_builder, snapshot.xid())?;

        // Update the table schema to reference this index
        // It is important that the order of the columns is kept.
        {
            let table_schema = table_relation.schema_mut();

            // Add table-level unique constraint
            if let Some(constraints) = table_schema.table_constraints.as_mut() {
                // Check if this constraint already exists
                let already_exists = constraints.iter().any(
                    |c| matches!(c, TableConstraint::Unique(cols) if cols == indexed_column_ids),
                );

                if !already_exists {
                    constraints.push(TableConstraint::Unique(indexed_column_ids.to_vec()));
                }
            }

            // Add the index to the schema's index map
            if let Some(indexes) = table_schema.table_indexes.as_mut() {
                indexes.insert(object_id, indexed_column_ids.to_vec());
            }
        }

        self.ctx.catalog().write().update_relation(
            table_id,
            None,
            Some(table_relation.schema().clone()),
            None,
            &tree_builder,
            &snapshot,
        )?;

        // Populate the index with existing data
        self.populate_index(
            table_root,
            table_relation.schema(),
            index_root,
            &index_schema,
            &indexed_column_ids,
        )?;

        Ok(object_id)
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
    ) -> RuntimeResult<()> {
        let snapshot = self.ctx.snapshot();
        let tid = snapshot.xid();
        // Collect rows to insert into index
        let mut index_entries: Vec<Row> = Vec::new();

        {
            // Scan the table and collect all visible rows
            let mut table_btree = self.ctx.build_tree(table_root);

            if table_btree.is_empty()? {
                return Ok(());
            };
            let positions: Vec<crate::tree::accessor::BtreePagePosition> = table_btree
                .iter_forward()?
                .filter(|p| p.is_ok())
                .map(|f| f.unwrap())
                .collect();

            for pos in positions {
                let row = table_btree.get_row_at(pos, table_schema, &snapshot)?;

                let Some(row) = row else {
                    continue;
                };

                let row_id = row.first();

                let Some(DataType::BigUInt(UInt64(value))) = row_id else {
                    return Err(RuntimeError::Other(
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
        let mut index_btree = self.ctx.build_tree_mut(index_root);
        for entry in index_entries {
            let tuple = TupleBuilder::from_schema(index_schema).build(&entry, tid)?;
            index_btree.insert(index_root, tuple, index_schema)?;
        }

        Ok(())
    }

    fn execute_alter_table(&mut self, stmt: &BoundAlterTable) -> RuntimeResult<DdlResult> {
        let mut relation = {
            let snapshot = self.ctx.snapshot();
            let tree_builder = self.ctx.tree_builder();
            self.ctx
                .catalog()
                .read()
                .get_relation(stmt.table_id, &tree_builder, &snapshot)?
        };

        let table_name = relation.name().to_string();

        if !relation.is_table() {
            return Err(RuntimeError::Catalog(CatalogError::TableNotFound(
                stmt.table_id,
            )));
        }

        let action_desc = match &stmt.action {
            BoundAlterAction::AddColumn(col_def) => {
                let col = Column::from(col_def);
                let name = col.name().to_string();
                relation.add_column(col)?;

                Ok(format!("ADD COLUMN {}", name))
            }
            BoundAlterAction::DropColumn(col_idx) => {
                let schema = relation.schema_mut();

                // Cannot drop key columns
                if *col_idx < schema.num_keys() {
                    return Err(RuntimeError::CannotUpdateKeyColumn);
                }

                let name = schema
                    .column(*col_idx)
                    .ok_or(SchemaError::ColumnIndexOutOfBounds(*col_idx))?
                    .name()
                    .to_string();

                // Remove from columns vector
                schema.remove_column_unchecked(*col_idx);

                Ok(format!("DROP COLUMN {}", name))
            }
            BoundAlterAction::AlterColumn {
                column_idx,
                action_type,
            } => self.alter_column(&mut relation, *column_idx, action_type),
            BoundAlterAction::AddConstraint(constraint) => {
                self.add_constraint(&mut relation, constraint)
            }
        }?;

        let id = relation.object_id();
        let tree_builder = self.ctx.tree_builder();
        // Update the relation in catalog
        self.ctx.catalog().write().update_relation(
            id,
            None,
            Some(relation.into_schema()),
            None,
            &tree_builder,
            self.ctx.snapshot(),
        )?;

        Ok(DdlResult::TableAltered {
            name: table_name,
            action: action_desc,
        })
    }

    fn alter_column(
        &self,
        relation: &mut Relation,
        column_idx: usize,
        action_type: &BoundAlterColumnAction,
    ) -> RuntimeResult<String> {
        let schema = relation.schema_mut();

        let column = schema
            .columns
            .get_mut(column_idx)
            .ok_or(SchemaError::ColumnIndexOutOfBounds(column_idx))?;

        let column_name = column.name().to_string();

        match action_type {
            BoundAlterColumnAction::DropDefault => column.default = None,
            BoundAlterColumnAction::SetNotNull => column.is_non_null = true,
            BoundAlterColumnAction::DropNotNull => column.is_non_null = false,
            BoundAlterColumnAction::SetDataType(dt) => column.dtype = *dt as u8,
            BoundAlterColumnAction::SetDefault(set_default) => {
                if let Some(default_value) = eval_literal_expr(set_default) {
                    column.default = Some(default_value.serialize()?);
                }
            }
        }

        Ok(format!("ALTER COLUMN {}", column_name))
    }

    fn add_constraint(
        &mut self,
        relation: &mut Relation,
        constraint: &BoundTableConstraint,
    ) -> RuntimeResult<String> {
        match constraint {
            BoundTableConstraint::PrimaryKey(col_indices) => {
                // Check if already has PK
                if relation.schema().has_primary_key() {
                    return Err(RuntimeError::Schema(SchemaError::DuplicatePrimaryKey));
                }

                {
                    let schema_mut = relation.schema_mut();
                    // Add the Constraint
                    if let Some(constraints) = schema_mut.table_constraints.as_mut() {
                        constraints.push(TableConstraint::PrimaryKey(col_indices.clone()));
                    }

                    // All columns in the primary key cannot be null.
                    for index in col_indices {
                        let col = schema_mut
                            .column_mut(*index)
                            .ok_or(SchemaError::ColumnIndexOutOfBounds(*index))?;
                        col.is_non_null = true;
                    }
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect::<Vec<String>>();

                let index_name = index_name(&col_names, relation.name());
                self.create_unique_index(relation.object_id(), &index_name, col_indices)?;

                Ok(format!(
                    "ADDED PRIMARY KEY CONSTRAINT ON COLUMNS: {}",
                    col_names.join(" ,")
                ))
            }

            BoundTableConstraint::Unique(col_indices) => {
                // Add the constraint
                if let Some(constraints) = relation.schema_mut().table_constraints.as_mut() {
                    constraints.push(TableConstraint::Unique(col_indices.clone()));
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect::<Vec<String>>();

                let index_name = index_name(&col_names, relation.name());
                self.create_unique_index(relation.object_id(), &index_name, col_indices)?;

                Ok(format!(
                    "ADDED UNIQUE CONSTRAINT ON COLUMNS: {}",
                    col_names.join(",")
                ))
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
                    .map(|r| r.name().to_string())?;

                let col_names: Vec<String> = columns
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect();

                // Create FK info
                let fk_info =
                    ForeignKeyInfo::for_table_and_columns(*ref_table_id, ref_columns.clone());

                // Add the constraint
                if let Some(constraints) = relation.schema_mut().table_constraints.as_mut() {
                    constraints.push(TableConstraint::ForeignKey {
                        columns: columns.clone(),
                        info: fk_info,
                    });
                }

                Ok(format!(
                    "ADDED FOREIGN KEY CONSTRAINT WITH TABLE {} ON {}",
                    ref_table_name,
                    col_names.join(",")
                ))
            }
        }
    }

    fn execute_drop_table(&mut self, stmt: &BoundDropTable) -> RuntimeResult<DdlResult> {
        let Some(table_id) = stmt.table_id else {
            if stmt.if_exists {
                return Ok(DdlResult::NoOp);
            }
            return Err(RuntimeError::Schema(SchemaError::NotFound(
                DatabaseItem::Table(stmt.table_name.clone()),
            )));
        };

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        // Get the relation
        let relation =
            self.ctx
                .catalog()
                .read()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let table_name = relation.name().to_string();

        // Remove the relation
        self.ctx.catalog().write().remove_relation(
            relation,
            &tree_builder,
            &snapshot,
            stmt.cascade,
        )?;

        Ok(DdlResult::TableDropped { name: table_name })
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
}
