use crate::database::{
    Database,
    schema::{Column, Constraint, ForeignKey, Schema, TableConstraint},
    errors::DatabaseError,
};

use crate::sql::ast::{AlterAction, AlterColumnAction, ColumnConstraintExpr, TableConstraintExpr};

use crate::sql::{Statement, parsing_pipeline};
use crate::structures::bplustree::SearchResult;

use crate::types::{DataType};
use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind};

mod filter;
mod groupby;
mod index_selection;
mod join;
mod orderby;
mod project;
mod scan;

pub struct ResultSet {
    inner: Vec<Box<[u8]>>,
}

impl ResultSet {
    fn new() -> Self {
        Self { inner: Vec::new() }
    }

    fn push(&mut self, payload: Box<[u8]>) {
        self.inner.push(payload);
    }
}

pub trait ExecutionPlanStep {
    fn exec(&mut self, db: &mut Database) -> std::io::Result<()>;
    fn take_result_set(&mut self) -> Option<ResultSet> {
        None
    }
}

struct ExecutionPlan {
    steps: VecDeque<Box<dyn ExecutionPlanStep>>,
}

impl ExecutionPlan {
    fn empty() -> Self {
        Self {
            steps: VecDeque::new(),
        }
    }

    fn push_step(&mut self, step: Box<dyn ExecutionPlanStep>) {
        self.steps.push_back(step);
    }
}



fn planify(stmt: Statement, db: &mut Database) -> Result<ExecutionPlan, DatabaseError> {
    match stmt {
        // For statements that do not need a plan, we just execute them
        Statement::CreateTable(create) => {
            // Prepend the row_id:
            let mut table_columns = Vec::with_capacity(create.columns.len() + 1);
            let mut row_id_constraints = HashMap::new();
            row_id_constraints.insert("RowId_unique".to_string(), Constraint::Unique);
            row_id_constraints.insert("RowId_non_null".to_string(), Constraint::NonNull);
            let row_id = Column::new_unindexed(
                crate::types::DataTypeKind::BigUInt,
                "RowId",
                Some(row_id_constraints),
            );
            table_columns.push(row_id);
            for new_column in create.columns {
                let mut constraints = HashMap::with_capacity(new_column.constraints.len());

                for col_ct_expr in new_column.constraints {
                    match col_ct_expr {
                        ColumnConstraintExpr::ForeignKey { table, column } => {
                            let ct = Constraint::ForeignKey(ForeignKey::new(table, column));
                            constraints.insert(format!("{}_fkey", new_column.name), ct);
                        }
                        ColumnConstraintExpr::Default(expr) => {
                            let value = DataType::try_from((new_column.data_type, expr))?;
                            let ct = Constraint::Default(value);
                            constraints.insert(format!("{}_default", new_column.name), ct);
                        }
                        ColumnConstraintExpr::NotNull => {
                            let ct = Constraint::NonNull;
                            constraints.insert(format!("{}_non_null", new_column.name), ct);
                        }
                        ColumnConstraintExpr::Unique => {
                            let ct = Constraint::Unique;
                            constraints.insert(format!("{}_unique", new_column.name), ct);
                        }
                        ColumnConstraintExpr::PrimaryKey => {
                            let ct = Constraint::PrimaryKey;
                            constraints.insert(format!("{}_pkey", new_column.name), ct);
                        }
                        _ => {}
                    }
                }

                let col = Column::new_unindexed(
                    new_column.data_type,
                    &new_column.name,
                    Some(constraints),
                );
                table_columns.push(col);
            }

            let mut schema = Schema::from_columns(table_columns.as_slice(), 1); // Tables always have one key, which is the RowId.

            for table_constraint in create.constraints {
                match table_constraint {
                    TableConstraintExpr::PrimaryKey(pk_list) => {
                        schema.add_constraint(TableConstraint::PrimaryKey(pk_list));
                    }
                    TableConstraintExpr::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    } => {
                        schema.add_constraint(TableConstraint::ForeignKey {
                            columns,
                            ref_table,
                            ref_columns,
                        });
                    }
                    TableConstraintExpr::Unique(items) => {
                        schema.add_constraint(TableConstraint::Unique(items));
                    }
                    _ => {}
                }
            }

            db.create_table(&create.table, schema)?;
            Ok(ExecutionPlan::empty())
        }
        Statement::AlterTable(alter) => {
            // Get the table object ID
            let mut table = db.relation(&alter.table)?;
            match alter.action {
                AlterAction::AddColumn(col_def) => {
                    // Create the new column with its constraints
                    let mut constraints = HashMap::new();

                    for col_ct_expr in col_def.constraints {
                        match col_ct_expr {
                            ColumnConstraintExpr::ForeignKey { table, column } => {
                                let ct = Constraint::ForeignKey(ForeignKey::new(table, column));
                                constraints.insert(format!("{}_fkey", col_def.name), ct);
                            }
                            ColumnConstraintExpr::Default(expr) => {
                                let value = DataType::try_from((col_def.data_type, expr))?;
                                let ct = Constraint::Default(value);
                                constraints.insert(format!("{}_default", col_def.name), ct);
                            }
                            ColumnConstraintExpr::NotNull => {
                                let ct = Constraint::NonNull;
                                constraints.insert(format!("{}_non_null", col_def.name), ct);
                            }
                            ColumnConstraintExpr::Unique => {
                                let ct = Constraint::Unique;
                                constraints.insert(format!("{}_unique", col_def.name), ct);
                            }
                            ColumnConstraintExpr::PrimaryKey => {
                                let ct = Constraint::PrimaryKey;
                                constraints.insert(format!("{}_pkey", col_def.name), ct);
                            }
                            _ => {}
                        }
                    }

                    let column = Column::new_unindexed(
                        col_def.data_type,
                        &col_def.name,
                        if constraints.is_empty() {
                            None
                        } else {
                            Some(constraints)
                        },
                    );

                    let schema = table.schema_mut();
                    schema.push_column(column);
                    db.update_relation(table)?;
                }
                AlterAction::AlterColumn(alter_col) => {
                    let mut table = db.relation(&alter.table)?;

                    // Find the column to alter
                    let column = table
                        .column_mut(&alter_col.name)
                        .ok_or(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Column {} not found", alter_col.name),
                        ))?;

                    // Handle column alterations
                    match alter_col.action {
                        AlterColumnAction::SetDataType(dtype) => {
                            column.set_datatype(dtype);
                        }

                        AlterColumnAction::SetDefault(expr) => {
                            let value = DataType::try_from((column.dtype, expr))?;
                            let constraint_name = format!("{}_default", alter_col.name);
                            column.add_constraint(constraint_name, Constraint::Default(value));
                        }
                        AlterColumnAction::DropDefault => {
                            // Find and remove any default constraint
                            let constraints_to_remove: Vec<String> = column
                                .constraints()
                                .iter()
                                .filter(|(k, v)| matches!(v, Constraint::Default(_)))
                                .map(|(k, v)| k)
                                .cloned()
                                .collect();

                            for ct_name in constraints_to_remove {
                                column.drop_constraint(&ct_name);
                            }
                        }

                        AlterColumnAction::SetNotNull => {
                            let constraint_name = format!("{}_non_null", alter_col.name);
                            column.add_constraint(constraint_name, Constraint::NonNull);
                        }

                        AlterColumnAction::DropNotNull => {
                            // Find and remove any non-null constraint
                            let constraints_to_remove: Vec<String> = column
                                .constraints()
                                .iter()
                                .filter(|(k, v)| matches!(v, Constraint::NonNull))
                                .map(|(k, v)| k)
                                .cloned()
                                .collect();

                            for ct_name in constraints_to_remove {
                                column.drop_constraint(&ct_name);
                            }
                        }
                    }

                    // Write the updated schema back to the table object
                    db.update_relation(table)?;
                }
                AlterAction::DropColumn(column_name) => {
                    let mut table = db.relation(&alter.table)?;

                    // Find and remove the column
                    let initial_len = table.columns().len();
                    table.columns_mut().retain(|c| c.name != column_name);

                    if table.columns().len() == initial_len {
                        return Err(DatabaseError::IOError(io::Error::new(
                            ErrorKind::NotFound,
                            format!("Column '{column_name}' not found"),
                        )));
                    }
                    // Write the updated schema back
                    db.update_relation(table)?;
                }

                AlterAction::DropConstraint(constraint_name) => {
                    let mut table = db.relation(&alter.table)?;
                    table.schema_mut().drop_constraint(&constraint_name);
                    // Write the updated schema back
                    db.update_relation(table)?;
                }

                AlterAction::AddConstraint(constraint) => {
                    let mut table = db.relation(&alter.table)?;
                    match constraint {
                        TableConstraintExpr::PrimaryKey(columns) => {
                            let ct = TableConstraint::PrimaryKey(columns);
                            table.schema_mut().add_constraint(ct);
                        }
                        TableConstraintExpr::ForeignKey {
                            columns,
                            ref_table,
                            ref_columns,
                        } => {
                            let ct = TableConstraint::ForeignKey {
                                columns,
                                ref_table,
                                ref_columns,
                            };
                            table.schema_mut().add_constraint(ct);
                        }
                        TableConstraintExpr::Unique(columns) => {
                            let ct = TableConstraint::Unique(columns);
                            table.schema_mut().add_constraint(ct);
                        }
                        _ => {
                            return Err(DatabaseError::Other("Unsupported table constraint type".to_string()));
                        }
                    }

                    db.update_relation(table)?;
                }
            }

            Ok(ExecutionPlan::empty())
        }
        Statement::DropTable(drop) => {
            // Check if table exists only if IF EXISTS is not specified
            if let Ok(table) = db.relation(&drop.table) {
                // Drop the table
                db.remove_relation(table, drop.cascade)?;
            } else if drop.if_exists {
                return Err(DatabaseError::from(io::Error::new(
                    ErrorKind::NotFound,
                    format!("Table {} not found", drop.table),
                )))?
            };

            Ok(ExecutionPlan::empty())
        }

        Statement::CreateIndex(create_idx) => {
            // Check if index already exists
            if matches!(db.lookup(&create_idx.name)?, SearchResult::Found(_)) {
                if !create_idx.if_not_exists {
                    return Err(DatabaseError::from(io::Error::new(
                        ErrorKind::AlreadyExists,
                        format!("Index '{}' already exists", create_idx.name),
                    )));
                }
                // If IF NOT EXISTS is specified, just return success
                return Ok(ExecutionPlan::empty());
            }

            // Verify the table exists
            let mut table = db.relation(&create_idx.table).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Table '{}' not found", create_idx.table),
                )
            })?;

            let index_set: std::collections::HashSet<String> =
                create_idx.columns.iter().map(|c| c.name.clone()).collect();

            // Get the table to verify columns exist
            let row_id = table.keys()[0].clone();
            let mut index_columns: Vec<Column> = table
                .columns()
                .iter()
                .filter(|c| index_set.contains(c.name()))
                .cloned()
                .collect();
            debug_assert!(
                !index_set.contains(row_id.name()),
                "Cannot create an index over Row id column"
            );
            let num_keys = index_columns.len() as u8;
            index_columns.push(row_id);
            let index_schema = Schema::from_columns(index_columns.as_slice(), num_keys);
            let joined = index_set.iter().cloned().collect::<Vec<String>>().join("_");
            let index_name = format!("{}_{}_idx", create_idx.table, joined);

            // Verify columns exist in the table
            db.create_index(index_name.as_str(), index_schema)?;
            table.keys_mut()[0].set_index(index_name);
            db.update_relation(table)?;

            Ok(ExecutionPlan::empty())
        }
        Statement::Select(select) => Ok(ExecutionPlan::empty()),
        _ => Ok(ExecutionPlan::empty()),
    }
}

fn exec(sql: &str, db: &mut Database) -> Result<(), DatabaseError> {
    let stmt = parsing_pipeline(sql, db)?;
    let plan = planify(stmt, db)?;
    Ok(())
}

#[cfg(test)]
mod planner_tests {
    use super::*;
    use crate::database::schema::Schema;
    use crate::io::pager::{Pager, SharedPager};

    use crate::types::DataTypeKind;

    use crate::{AxmosDBConfig, IncrementalVaccum, ReadWriteVersion, TextEncoding};

    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        let mut db = Database::new(SharedPager::from(pager), 3, 2);
        db.init()?;

        // Create a users table
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, true);
        users_schema.add_column("created_at", DataTypeKind::DateTime, false, false, false);

        db.create_table("users", users_schema)?;

        let mut posts_schema = Schema::new();
        posts_schema.add_column("id", DataTypeKind::Int, true, true, false);
        posts_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        posts_schema.add_column("title", DataTypeKind::Text, false, false, false);
        posts_schema.add_column("content", DataTypeKind::Blob, false, false, true);
        posts_schema.add_column("published", DataTypeKind::Boolean, false, false, false);

        db.create_table("posts", posts_schema)?;

        Ok(db)
    }
}
