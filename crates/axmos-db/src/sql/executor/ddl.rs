use crate::{
    database::{
        SharedCatalog,
        errors::{AlreadyExists, DdlError, DdlResult},
        schema::{Column, Constraint, ForeignKey, ObjectType, Relation, Schema, TableConstraint},
    },
    sql::binder::ast::*,
    structures::bplustree::SearchResult,
    transactions::worker::Worker,
    types::{Blob, DataType, DataTypeKind, ObjectId},
};

use super::Row;

/// Executes DDL (Data Definition Language) statements directly.
/// These operations modify schema/catalog and don't go through the optimizer.
pub(crate) struct DdlExecutor {
    catalog: SharedCatalog,
    worker: Worker,
}

impl DdlExecutor {
    pub(crate) fn new(catalog: SharedCatalog, worker: Worker) -> Self {
        Self { catalog, worker }
    }

    /// Check if a statement is DDL
    pub(crate) fn is_ddl(stmt: &BoundStatement) -> bool {
        matches!(
            stmt,
            BoundStatement::CreateTable(_)
                | BoundStatement::CreateIndex(_)
                | BoundStatement::AlterTable(_)
                | BoundStatement::DropTable(_)
                | BoundStatement::Transaction(_)
        )
    }

    /// Execute a DDL statement and return result
    pub(crate) fn execute(&self, stmt: &BoundStatement) -> DdlResult<DdlOutcome> {
        match stmt {
            BoundStatement::CreateTable(create) => self.execute_create_table(create),
            BoundStatement::CreateIndex(create) => self.execute_create_index(create),
            BoundStatement::AlterTable(alter) => self.execute_alter_table(alter),
            BoundStatement::DropTable(drop) => self.execute_drop_table(drop),
            BoundStatement::Transaction(tx) => self.execute_transaction(tx),
            _ => Err(DdlError::Other("Not a DDL statement".to_string())),
        }
    }

    fn execute_create_table(&self, stmt: &BoundCreateTable) -> DdlResult<DdlOutcome> {
        // Check if table already exists
        if let Ok(SearchResult::Found(_)) = self
            .catalog
            .lookup_relation(&stmt.table_name, self.worker.clone())
        {
            if stmt.if_not_exists {
                return Ok(DdlOutcome::NoOp);
            }
            return Err(DdlError::AlreadyExists(AlreadyExists::Table(
                stmt.table_name.clone(),
            )));
        }

        let schema = self.build_schema_from_bound(&stmt.columns, &stmt.constraints);
        let object_id = self
            .catalog
            .create_table(&stmt.table_name, schema, self.worker.clone())?;

        Ok(DdlOutcome::TableCreated {
            name: stmt.table_name.clone(),
            object_id,
        })
    }
    fn execute_create_index(&self, stmt: &BoundCreateIndex) -> DdlResult<DdlOutcome> {
        // Check if index already exists
        if let Ok(SearchResult::Found(_)) = self
            .catalog
            .lookup_relation(&stmt.index_name, self.worker.clone())
        {
            if stmt.if_not_exists {
                return Ok(DdlOutcome::NoOp);
            }
            return Err(DdlError::AlreadyExists(AlreadyExists::Index(
                stmt.index_name.clone(),
            )));
        }

        // Get the table to find column types
        let mut table_relation = self
            .catalog
            .get_relation_unchecked(stmt.table_id, self.worker.clone())?;

        if !matches!(table_relation, Relation::TableRel(_)) {
            return Err(DdlError::InvalidObjectType(ObjectType::Table));
        }

        if stmt.columns.is_empty() {
            return Err(DdlError::Other(
                "Index must have at least one column".to_string(),
            ));
        }

        // Build index schema: all indexed columns as keys + row_id as value
        let num_index_keys = stmt.columns.len();
        let mut index_columns: Vec<Column> = Vec::with_capacity(num_index_keys + 1);
        let mut column_names: Vec<String> = Vec::with_capacity(num_index_keys);

        for col_ref in &stmt.columns {
            let table_col = &table_relation.schema().columns()[col_ref.column_idx];
            column_names.push(table_col.name().to_string());

            index_columns.push(Column::new_unindexed(
                table_col.dtype,
                table_col.name(),
                None,
            ));
        }

        // row_id is the value column
        index_columns.push(Column::new_unindexed(DataTypeKind::BigUInt, "row_id", None));

        let index_schema = Schema::from_columns(&index_columns, num_index_keys as u8);

        // Create the index
        let object_id =
            self.catalog
                .create_index(&stmt.index_name, index_schema, self.worker.clone())?;

        // Update table's schema (all participating columns reference this index)
        for col_name in &column_names {
            if let Some(col) = table_relation.schema_mut().column_mut(col_name) {
                if col.index().is_none() {
                    col.set_index(stmt.index_name.clone());
                }
            }
        }

        self.catalog
            .update_relation(table_relation, self.worker.clone())?;

        Ok(DdlOutcome::IndexCreated {
            name: stmt.index_name.clone(),
            object_id,
        })
    }

    fn execute_alter_table(&self, stmt: &BoundAlterTable) -> DdlResult<DdlOutcome> {
        let mut relation = self
            .catalog
            .get_relation_unchecked(stmt.table_id, self.worker.clone())?;

        let table_name = relation.name().to_string();

        if !matches!(relation, Relation::TableRel(_)) {
            return Err(DdlError::InvalidObjectType(
                crate::database::schema::ObjectType::Table,
            ));
        }

        let action_desc = match &stmt.action {
            BoundAlterAction::AddColumn(col_def) => {
                // Check column doesn't already exist
                if relation.schema().has_column(&col_def.name) {
                    return Err(DdlError::AlreadyExists(AlreadyExists::Column(
                        table_name.clone(),
                        col_def.name.clone(),
                    )));
                }

                let column = self.bound_column_to_column(col_def);
                relation.schema_mut().push_column(column);
                "ADD COLUMN".to_string()
            }

            BoundAlterAction::DropColumn(col_idx) => {
                let column_name = relation.schema().columns()[*col_idx].name().to_string();

                // Check if column has an index - if so, we need to drop it first or error
                if let Some(index_name) = relation.schema().columns()[*col_idx].index().cloned() {
                    // Drop the index first
                    let index_relation = self
                        .catalog
                        .get_relation(&index_name, self.worker.clone())?;
                    self.catalog
                        .remove_relation(index_relation, false, self.worker.clone())?;
                }

                // Remove the column
                relation.schema_mut().columns_mut().remove(*col_idx);
                format!("DROP COLUMN {}", column_name)
            }

            BoundAlterAction::AlterColumn {
                column_idx,
                new_type,
                set_default,
                drop_default,
                set_not_null,
                drop_not_null,
            } => {
                let column = &mut relation.schema_mut().columns_mut()[*column_idx];
                let column_name = column.name().to_string();

                if let Some(dtype) = new_type {
                    column.set_datatype(*dtype);
                }

                if let Some(default_expr) = set_default {
                    // Evaluate the default expression to get a DataType value
                    // For now, we only support literal defaults
                    if let Some(default_value) = self.eval_literal_expr(default_expr) {
                        column.add_constraint(
                            format!("{}_default", column_name),
                            Constraint::Default(default_value),
                        );
                    }
                }

                if *drop_default {
                    // Find and remove default constraint
                    let default_key = column
                        .constraints()
                        .iter()
                        .find(|(_, c)| matches!(c, Constraint::Default(_)))
                        .map(|(k, _)| k.clone());

                    if let Some(key) = default_key {
                        column.drop_constraint(&key);
                    }
                }

                if *set_not_null {
                    column.add_constraint(format!("{}_not_null", column_name), Constraint::NonNull);
                }

                if *drop_not_null {
                    // Find and remove not null constraint
                    let nn_key = column
                        .constraints()
                        .iter()
                        .find(|(_, c)| matches!(c, Constraint::NonNull))
                        .map(|(k, _)| k.clone());

                    if let Some(key) = nn_key {
                        column.drop_constraint(&key);
                    }
                }

                format!("ALTER COLUMN {}", column_name)
            }

            BoundAlterAction::AddConstraint(constraint) => {
                let constraint_name =
                    self.add_table_constraint(relation.schema_mut(), constraint)?;
                format!("ADD CONSTRAINT {}", constraint_name)
            }

            BoundAlterAction::DropConstraint(constraint_name) => {
                if !relation.schema().has_constraint(constraint_name) {
                    return Err(DdlError::Other(format!(
                        "Constraint {} not found",
                        constraint_name
                    )));
                }

                // Try to drop from table-level constraints first
                relation.schema_mut().drop_constraint(constraint_name);

                // Also check column-level constraints
                for col in relation.schema_mut().columns_mut() {
                    if col.has_constraint(constraint_name) {
                        col.drop_constraint(constraint_name);
                        break;
                    }
                }

                format!("DROP CONSTRAINT {}", constraint_name)
            }
        };

        // Update the relation in catalog
        self.catalog
            .update_relation(relation, self.worker.clone())?;

        Ok(DdlOutcome::TableAltered {
            name: table_name,
            action: action_desc,
        })
    }

    fn execute_drop_table(&self, stmt: &BoundDropTable) -> DdlResult<DdlOutcome> {
        // Try to get the relation
        let relation = match self
            .catalog
            .get_relation_unchecked(stmt.table_id, self.worker.clone())
        {
            Ok(rel) => rel,
            Err(_) => {
                if stmt.if_exists {
                    return Ok(DdlOutcome::NoOp);
                }
                return Err(DdlError::Other("Table not found".to_string()));
            }
        };

        let table_name = relation.name().to_string();

        // Remove the relation (cascade handled by catalog.remove_relation)
        self.catalog
            .remove_relation(relation, stmt.cascade, self.worker.clone())?;

        Ok(DdlOutcome::TableDropped { name: table_name })
    }

    fn execute_transaction(&self, stmt: &BoundTransaction) -> DdlResult<DdlOutcome> {
        // TODO. Implement complete transaction management and integrate with workerpools
        match stmt {
            BoundTransaction::Begin => Ok(DdlOutcome::TransactionStarted),
            BoundTransaction::Commit => Ok(DdlOutcome::TransactionCommitted),
            BoundTransaction::Rollback => Ok(DdlOutcome::TransactionRolledBack),
        }
    }

    // Helper methods

    fn build_schema_from_bound(
        &self,
        columns: &[BoundColumnDef],
        constraints: &[BoundTableConstraint],
    ) -> Schema {
        let mut schema = Schema::new();

        // Add columns
        for col_def in columns {
            let column = self.bound_column_to_column(col_def);
            schema.push_column(column);
        }

        // Determine num_keys (first PK column or 0)
        let mut pk_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            if col.is_pk() {
                pk_idx = Some(i);
                break;
            }
        }

        // Apply table-level constraints
        for constraint in constraints {
            match constraint {
                BoundTableConstraint::PrimaryKey(col_indices) => {
                    // Mark columns as primary key
                    for &idx in col_indices {
                        if let Some(col) = schema.columns_mut().get_mut(idx) {
                            col.add_constraint(
                                format!("{}_pkey", col.name()),
                                Constraint::PrimaryKey,
                            );
                        }
                    }

                    // Set num_keys to first PK column index + 1
                    if let Some(&first_pk) = col_indices.first() {
                        schema.set_num_keys((first_pk + 1) as u8);
                    }

                    // Add table-level constraint
                    let col_names: Vec<String> = col_indices
                        .iter()
                        .filter_map(|&idx| schema.columns().get(idx).map(|c| c.name().to_string()))
                        .collect();
                    schema.add_constraint(TableConstraint::PrimaryKey(col_names));
                }

                BoundTableConstraint::Unique(col_indices) => {
                    let col_names: Vec<String> = col_indices
                        .iter()
                        .filter_map(|&idx| schema.columns().get(idx).map(|c| c.name().to_string()))
                        .collect();
                    schema.add_constraint(TableConstraint::Unique(col_names));
                }

                BoundTableConstraint::ForeignKey {
                    columns,
                    ref_table_id,
                    ref_columns,
                } => {
                    // We need to resolve table name from ID
                    if let Ok(ref_relation) = self
                        .catalog
                        .get_relation_unchecked(*ref_table_id, self.worker.clone())
                    {
                        let ref_table_name = ref_relation.name().to_string();

                        let col_names: Vec<String> = columns
                            .iter()
                            .filter_map(|&idx| {
                                schema.columns().get(idx).map(|c| c.name().to_string())
                            })
                            .collect();

                        let ref_col_names: Vec<String> = ref_columns
                            .iter()
                            .filter_map(|&idx| {
                                ref_relation
                                    .schema()
                                    .columns()
                                    .get(idx)
                                    .map(|c| c.name().to_string())
                            })
                            .collect();

                        schema.add_constraint(TableConstraint::ForeignKey {
                            columns: col_names,
                            ref_table: ref_table_name,
                            ref_columns: ref_col_names,
                        });
                    }
                }
            }
        }

        // If no explicit PK was set but a column has PK constraint, set num_keys
        if schema.num_keys == 0 {
            if let Some(idx) = pk_idx {
                schema.set_num_keys((idx + 1) as u8);
            }
        }

        schema
    }

    fn bound_column_to_column(&self, col_def: &BoundColumnDef) -> Column {
        let mut col = Column::new_unindexed(col_def.data_type, &col_def.name, None);

        for constraint in &col_def.constraints {
            match constraint {
                BoundColumnConstraint::PrimaryKey => {
                    col.add_constraint(format!("{}_pkey", col_def.name), Constraint::PrimaryKey);
                }
                BoundColumnConstraint::NotNull => {
                    col.add_constraint(format!("{}_not_null", col_def.name), Constraint::NonNull);
                }
                BoundColumnConstraint::Unique => {
                    col.add_constraint(format!("{}_unique", col_def.name), Constraint::Unique);
                }
                BoundColumnConstraint::ForeignKey {
                    ref_table_id,
                    ref_column_idx,
                } => {
                    // Resolve table and column names
                    if let Ok(ref_relation) = self
                        .catalog
                        .get_relation_unchecked(*ref_table_id, self.worker.clone())
                    {
                        let ref_table_name = ref_relation.name().to_string();
                        let ref_col_name = ref_relation
                            .schema()
                            .columns()
                            .get(*ref_column_idx)
                            .map(|c| c.name().to_string())
                            .unwrap_or_default();

                        let fk = ForeignKey::new(ref_table_name, ref_col_name);
                        col.add_constraint(
                            format!("{}_fkey", col_def.name),
                            Constraint::ForeignKey(fk),
                        );
                    }
                }
                BoundColumnConstraint::Default(expr) => {
                    if let Some(default_value) = self.eval_literal_expr(expr) {
                        col.add_constraint(
                            format!("{}_default", col_def.name),
                            Constraint::Default(default_value),
                        );
                    }
                }
            }
        }

        col
    }

    fn add_table_constraint(
        &self,
        schema: &mut Schema,
        constraint: &BoundTableConstraint,
    ) -> DdlResult<String> {
        match constraint {
            BoundTableConstraint::PrimaryKey(col_indices) => {
                if schema.has_pk() {
                    return Err(DdlError::Other(
                        "Table already has a primary key".to_string(),
                    ));
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| schema.columns().get(idx).map(|c| c.name().to_string()))
                    .collect();

                let constraint_name = format!("{}_pkey", col_names.join("_"));

                // Mark columns as PK
                for &idx in col_indices {
                    if let Some(col) = schema.columns_mut().get_mut(idx) {
                        col.add_constraint(format!("{}_pkey", col.name()), Constraint::PrimaryKey);
                    }
                }

                schema.add_constraint(TableConstraint::PrimaryKey(col_names));
                Ok(constraint_name)
            }

            BoundTableConstraint::Unique(col_indices) => {
                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| schema.columns().get(idx).map(|c| c.name().to_string()))
                    .collect();

                let constraint_name = format!("{}_unique", col_names.join("_"));
                schema.add_constraint(TableConstraint::Unique(col_names));
                Ok(constraint_name)
            }

            BoundTableConstraint::ForeignKey {
                columns,
                ref_table_id,
                ref_columns,
            } => {
                if let Ok(ref_relation) = self
                    .catalog
                    .get_relation_unchecked(*ref_table_id, self.worker.clone())
                {
                    let ref_table_name = ref_relation.name().to_string();

                    let col_names: Vec<String> = columns
                        .iter()
                        .filter_map(|&idx| schema.columns().get(idx).map(|c| c.name().to_string()))
                        .collect();

                    let ref_col_names: Vec<String> = ref_columns
                        .iter()
                        .filter_map(|&idx| {
                            ref_relation
                                .schema()
                                .columns()
                                .get(idx)
                                .map(|c| c.name().to_string())
                        })
                        .collect();

                    let constraint_name =
                        format!("{}_{}_fkey", ref_table_name, col_names.join("_"));

                    schema.add_constraint(TableConstraint::ForeignKey {
                        columns: col_names,
                        ref_table: ref_table_name,
                        ref_columns: ref_col_names,
                    });

                    Ok(constraint_name)
                } else {
                    Err(DdlError::Other("Referenced table not found".to_string()))
                }
            }
        }
    }

    /// Evaluate a literal expression to get a DataType value
    /// Only supports literal values for DEFAULT constraints
    fn eval_literal_expr(&self, expr: &BoundExpression) -> Option<DataType> {
        match expr {
            BoundExpression::Literal { value } => Some(value.clone()),
            _ => None, // Non-literal defaults are not supported yet
        }
    }
}

/// Result of a DDL operation
#[derive(Debug, Clone)]
pub(crate) enum DdlOutcome {
    TableCreated { name: String, object_id: ObjectId },
    TableAltered { name: String, action: String },
    TableDropped { name: String },
    IndexCreated { name: String, object_id: ObjectId },
    IndexDropped { name: String },
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    NoOp,
}

impl DdlOutcome {
    /// Convert to a result row for consistency with DML operations
    pub(crate) fn to_row(&self) -> Row {
        match self {
            Self::TableCreated { name, .. } => Row::from(
                &[DataType::Text(Blob::from(
                    format!("CREATE TABLE {}", name).as_str(),
                ))][..],
            ),
            Self::TableAltered { name, action } => Row::from(
                &[DataType::Text(Blob::from(
                    format!("ALTER TABLE {} {}", name, action).as_str(),
                ))][..],
            ),
            Self::TableDropped { name } => Row::from(
                &[DataType::Text(Blob::from(
                    format!("DROP TABLE {}", name).as_str(),
                ))][..],
            ),
            Self::IndexCreated { name, .. } => Row::from(
                &[DataType::Text(Blob::from(
                    format!("CREATE INDEX {}", name).as_str(),
                ))][..],
            ),
            Self::IndexDropped { name } => Row::from(
                &[DataType::Text(Blob::from(
                    format!("DROP INDEX {}", name).as_str(),
                ))][..],
            ),
            Self::TransactionStarted => Row::from(&[DataType::Text(Blob::from("BEGIN"))][..]),
            Self::TransactionCommitted => Row::from(&[DataType::Text(Blob::from("COMMIT"))][..]),
            Self::TransactionRolledBack => Row::from(&[DataType::Text(Blob::from("ROLLBACK"))][..]),
            Self::NoOp => Row::from(&[DataType::Text(Blob::from("OK"))][..]),
        }
    }
}
