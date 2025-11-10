use crate::database::schema::Schema;
use crate::database::schema::{
    AsBytes, Column, Constraint, DBObject, Database, ForeignKey, ObjectType,
};
use crate::sql::ast::{AlterAction, AlterColumnAction, ColumnConstraintExpr, TableConstraintExpr};
use crate::sql::{parsing_pipeline, SQLError, Statement};
use crate::types::{DataType, DataTypeKind};
use std::collections::{HashMap, VecDeque};

enum Scan {
    SeqScan,
    IndexScan,
}

enum ExecutionPlanStep {
    SeqScan,
    IndexScan,
}

struct ExecutionPlan {
    steps: VecDeque<ExecutionPlanStep>,
}

impl ExecutionPlan {
    fn empty() -> Self {
        Self {
            steps: VecDeque::new(),
        }
    }
}

fn planify(stmt: Statement, db: &mut Database) -> std::io::Result<ExecutionPlan> {
    match stmt {
        // For statements that do not need a plan, we just execute them
        Statement::CreateTable(create) => {
            let mut table_columns = Vec::with_capacity(create.columns.len());
            for new_column in create.columns {
                let mut constraints = HashMap::with_capacity(new_column.constraints.len());

                for col_ct_expr in new_column.constraints {
                    match col_ct_expr {
                        ColumnConstraintExpr::ForeignKey { table, column } => {
                            let ct = Constraint::ForeignKey(ForeignKey::new(table, column));
                            constraints.insert(format!("{}_fkey", new_column.name), ct);
                        }
                        ColumnConstraintExpr::Default(expr) => {
                            let value = DataType::from_expr(expr, new_column.data_type);
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

                let col = Column::new(new_column.data_type, &new_column.name, Some(constraints));
                table_columns.push(col);
            }

            for table_constraint in create.constraints {
                match table_constraint {
                    TableConstraintExpr::PrimaryKey(pk_list) => {}
                    TableConstraintExpr::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    } => {}
                    TableConstraintExpr::Unique(items) => {}
                    _ => {} // TODO !
                }
            }

            let schema = Schema::from(table_columns.as_slice());

            db.create_table(&create.table, schema)?;
            Ok(ExecutionPlan::empty())
        }
        Statement::AlterTable(alter) => {
            // Get the table object ID
            let table_oid = db
                .search_obj(&alter.table)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotFound, e))?;
            let mut table_obj = db.get_obj(table_oid)?;

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
                                let value = DataType::from_expr(expr, col_def.data_type);
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

                    let column = Column::new(
                        col_def.data_type,
                        &col_def.name,
                        if constraints.is_empty() {
                            None
                        } else {
                            Some(constraints)
                        },
                    );

                    let mut schema = table_obj.get_schema();
                    schema.push_column(column);
                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }
                AlterAction::AlterColumn(alter_col) => {
                    let mut table_obj = db.get_obj(table_oid)?;
                    let mut schema = table_obj.get_schema();

                    // Find the column to alter
                    let column = schema
                        .columns
                        .iter_mut()
                        .find(|c| c.name == alter_col.name)
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                format!("Column '{}' not found", alter_col.name),
                            )
                        })?;

                    // Handle column alterations
                    match alter_col.action {
                        AlterColumnAction::SetDataType(dtype) => {
                            column.set_datatype(dtype);
                        }

                        AlterColumnAction::SetDefault(expr) => {
                            let value = DataType::from_expr(expr, column.dtype);
                            let constraint_name = format!("{}_default", alter_col.name);
                            column.add_constraint(constraint_name, Constraint::Default(value));
                        }

                        AlterColumnAction::DropDefault => {
                            // Find and remove any default constraint
                            let constraints_to_remove: Vec<String> = column
                                .constraints()
                                .keys()
                                .filter(|k| k.ends_with("_default"))
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
                                .keys()
                                .filter(|k| k.ends_with("_non_null"))
                                .cloned()
                                .collect();

                            for ct_name in constraints_to_remove {
                                column.drop_constraint(&ct_name);
                            }
                        }
                    }

                    // Write the updated schema back to the table object
                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }
                AlterAction::DropColumn(column_name) => {
                    let mut table_obj = db.get_obj(table_oid)?;
                    let mut schema = table_obj.get_schema();

                    // Find and remove the column
                    let initial_len = schema.columns.len();
                    schema.columns.retain(|c| c.name != column_name);

                    if schema.columns.len() == initial_len {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Column '{column_name}' not found"),
                        ));
                    }

                    // Write the updated schema back
                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }

                AlterAction::DropConstraint(constraint_name) => {
                    let mut table_obj = db.get_obj(table_oid)?;
                    let mut schema = table_obj.get_schema();

                    // Find the column with this constraint and remove it
                    let mut found = false;
                    for column in &mut schema.columns {
                        if column.has_constraint(&constraint_name) {
                            column.drop_constraint(&constraint_name);
                            found = true;
                            break;
                        }
                    }

                    if !found {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Constraint '{constraint_name}' not found"),
                        ));
                    }

                    // Write the updated schema back
                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }

                AlterAction::AddConstraint(constraint) => {
                    // Table constraints are not supported yet
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "Table constraints are not yet supported",
                    ));
                }
            }

            Ok(ExecutionPlan::empty())
        }
        Statement::DropTable(drop) => {
            // Check if table exists only if IF EXISTS is not specified
            if let Ok(oid) = db.search_obj(&drop.table) {
                // Drop the table
                // TODO: The cascade option should handle dependent objects if supported
                db.remove_obj(oid, &drop.table)?;
            } else if drop.if_exists {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Table {} not found", drop.table),
                ))?;
            };

            Ok(ExecutionPlan::empty())
        }

        Statement::CreateIndex(create_idx) => {
            // Check if index already exists
            if let Ok(existing_oid) = db.search_obj(&create_idx.name) {
                if !create_idx.if_not_exists {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("Index '{}' already exists", create_idx.name),
                    ));
                }
                // If IF NOT EXISTS is specified, just return success
                return Ok(ExecutionPlan::empty());
            }

            // Verify the table exists
            let table_oid = db.search_obj(&create_idx.table).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Table '{}' not found", create_idx.table),
                )
            })?;

            // Get the table to verify columns exist
            let table_obj = db.get_obj(table_oid)?;
            let table_schema = table_obj.get_schema();

            // Verify all columns exist in the table
            for idx_col in &create_idx.columns {
                if !table_schema.has_column(&idx_col.name) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "Column '{}' not found in table '{}'",
                            idx_col.name, create_idx.table
                        ),
                    ));
                }
            }

            // TODO: COMPLETE CREATE INDEX STATEMENT PLAN
            Ok(ExecutionPlan::empty())
        }

        _ => Ok(ExecutionPlan::empty()),
    }
}

fn exec(sql: &str, db: &mut Database) -> Result<(), SQLError> {
    let stmt = parsing_pipeline(sql, db)?;

    dbg!(&stmt);
    let plan = planify(stmt, db);
    Ok(())
}

#[cfg(test)]
mod planner_tests {
    use super::*;
    use crate::database::schema::{Database, Schema};
    use crate::io::pager::{Pager, SharedPager};

    use crate::types::DataTypeKind;

    use crate::{IncrementalVaccum, RQLiteConfig, ReadWriteVersion, TextEncoding};
    use serial_test::serial;
    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = RQLiteConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            read_write_version: ReadWriteVersion::Legacy,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        let mut db = Database::new(SharedPager::from(pager));
        db.init()?;

        // Create a users table
        let users_schema = Schema::new()
            .add_column("id", DataTypeKind::Int, true, true, false)
            .add_column("name", DataTypeKind::Text, false, false, false)
            .add_column("email", DataTypeKind::Text, false, true, false)
            .add_column("age", DataTypeKind::Int, false, false, true)
            .add_column("created_at", DataTypeKind::DateTime, false, false, false);

        db.create_table("users", users_schema)?;

        let posts_schema = Schema::new()
            .add_column("id", DataTypeKind::Int, true, true, false)
            .add_column("user_id", DataTypeKind::Int, false, false, false)
            .add_column("title", DataTypeKind::Text, false, false, false)
            .add_column("content", DataTypeKind::Blob, false, false, true)
            .add_column("published", DataTypeKind::Boolean, false, false, false);

        db.create_table("posts", posts_schema)?;

        Ok(db)
    }

    #[test]
    #[serial]
    fn test_planner_create_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE TABLE test_table (
            id INT PRIMARY KEY,
            email TEXT
        )";

        exec(sql, &mut db).unwrap();

        let obj = db.search_obj("test_table");
        assert!(obj.is_ok());

        let oid = obj.unwrap();
        let obj = db.get_obj(oid);
        assert!(obj.is_ok());
        let obj = obj.unwrap();
        assert_eq!(obj.name(), "test_table");
        assert!(matches!(obj.object_type(), ObjectType::Table));
        assert!(obj.metadata().is_some());
        let (schema, bytes) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let mut id = Column::new(DataTypeKind::Int, "id", None);
        let email = Column::new(DataTypeKind::Text, "email", None);
        id.add_constraint("id_pkey".to_string(), Constraint::PrimaryKey);

        assert_eq!(schema, Schema::from([id, email].as_slice()));
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_add_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        // First create a table
        let sql = "CREATE TABLE test_table (id INT PRIMARY KEY)";
        exec(sql, &mut db).unwrap();

        // Add a column
        let sql = "ALTER TABLE test_table ADD COLUMN email TEXT NOT NULL";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[1].name, "email");
        assert_eq!(schema.columns[1].dtype, DataTypeKind::Text);
        assert!(schema.columns[1].is_non_null());
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_drop_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        // Create table with multiple columns
        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        email TEXT,
        age INT
    )";
        exec(sql, &mut db).unwrap();

        // Drop a column
        let sql = "ALTER TABLE test_table DROP COLUMN email";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        assert_eq!(schema.columns.len(), 2);
        assert!(!schema.has_column("email"));
        assert!(schema.has_column("id"));
        assert!(schema.has_column("age"));
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_set_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        status TEXT
    )";
        exec(sql, &mut db).unwrap();

        // Set default value
        let sql = "ALTER TABLE test_table ALTER COLUMN status SET DEFAULT 'active'";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let status_col = schema.find_col("status").unwrap();
        assert!(status_col.has_constraint("status_default"));
        assert!(status_col.default().is_some());
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_drop_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        status TEXT DEFAULT 'pending'
    )";
        exec(sql, &mut db).unwrap();

        // Drop default
        let sql = "ALTER TABLE test_table ALTER COLUMN status DROP DEFAULT";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let status_col = schema.find_col("status").unwrap();
        assert!(!status_col.has_constraint("status_default"));
        assert!(status_col.default().is_none());
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_set_not_null() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        email TEXT
    )";
        exec(sql, &mut db).unwrap();

        // Set NOT NULL
        let sql = "ALTER TABLE test_table ALTER COLUMN email SET NOT NULL";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let email_col = schema.find_col("email").unwrap();
        assert!(email_col.is_non_null());
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_drop_not_null() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        email TEXT NOT NULL
    )";
        exec(sql, &mut db).unwrap();

        // Drop NOT NULL
        let sql = "ALTER TABLE test_table ALTER COLUMN email DROP NOT NULL";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let email_col = schema.find_col("email").unwrap();
        assert!(!email_col.is_non_null());
    }

    #[test]
    #[serial]
    fn test_planner_alter_table_change_data_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (
        id INT PRIMARY KEY,
        count INT
    )";
        exec(sql, &mut db).unwrap();

        // Change data type
        let sql = "ALTER TABLE test_table ALTER COLUMN count BIGINT";
        exec(sql, &mut db).unwrap();

        let oid = db.search_obj("test_table").unwrap();
        let obj = db.get_obj(oid).unwrap();
        let (schema, _) = Schema::read_from(obj.metadata().unwrap()).unwrap();

        let count_col = schema.find_col("count").unwrap();
        assert_eq!(count_col.dtype, DataTypeKind::BigInt);
    }

    #[test]
    #[serial]
    fn test_planner_drop_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        let sql = "CREATE TABLE test_table (id INT PRIMARY KEY)";
        exec(sql, &mut db).unwrap();

        // Verify table exists
        assert!(db.search_obj("test_table").is_ok());

        // Drop the table
        let sql = "DROP TABLE test_table";
        exec(sql, &mut db).unwrap();

        // Verify table no longer exists
        assert!(db.search_obj("test_table").is_err());
    }

    #[test]
    #[serial]
    fn test_planner_drop_table_if_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        // Drop non-existent table with IF EXISTS - should succeed
        let sql = "DROP TABLE IF EXISTS non_existent";
        assert!(exec(sql, &mut db).is_ok());
    }

    #[test]
    #[serial]
    fn test_planner_drop_table_not_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = create_db(4096, 10, &path).unwrap();

        // Drop non-existent table without IF EXISTS should fail
        let sql = "DROP TABLE non_existent";
        assert!(exec(sql, &mut db).is_err());
    }

}
