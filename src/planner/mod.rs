use crate::database::schema::{
    AsBytes, Column, Constraint, DBObject,  ForeignKey, ObjectType,
};
use crate::database::Database;
use crate::database::schema::{Schema, TableConstraint};
use crate::sql::ast::{
    AlterAction, AlterColumnAction, ColumnConstraintExpr,  TableConstraintExpr, TableReference
};
use crate::sql::{parsing_pipeline, SQLError, Statement, ast::{SelectStatement, Expr}};

use crate::types::DataType;
use std::collections::{HashMap, VecDeque};

mod groupby;
mod join;
mod orderby;
mod project;
mod scan;
mod index_selection;
mod filter;


use groupby::GroupBy;
use orderby::Limit;
use filter::Filter;

use scan::Scan;
use index_selection::find_applicable_indexes;

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




fn build_scan_steps(
    table_ref: &TableReference,
    where_clause: Option<&Expr>,
    db: &Database,
    plan: &mut ExecutionPlan,
) -> std::io::Result<()> {
    match table_ref {
        TableReference::Table { name, alias } => {
            let table_oid = db.search_obj(name).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::NotFound, format!("Table '{name}' not found"))
            })?;

            let table_obj = db.get_obj(table_oid)?;
            let schema = table_obj.get_schema();

            // Try to find applicable indexes
            let index_candidates = find_applicable_indexes(name, where_clause, db);

            let scan: Box<dyn ExecutionPlanStep> = if let Some(best_index) = index_candidates.first() {
                // Use index scan
                let index_obj = db.get_obj(best_index.get())?;
                let index_schema = index_obj.get_schema();

                let (start_key, end_key) = if let Some(where_expr) = where_clause {
                    // TODO: This should extract the actual bounds for an index. Currently I am not maintaining an stats table like postgres does so returns (None, None) for convenience.
                    (None, None)
                } else {
                    (None, None)
                };

                Box::new(Scan::IndexScan {
                    table_oid,
                    index_oid: best_index.get(),
                    table_schema: schema,
                    index_schema,
                    start_key,
                    end_key,
                    cached_resultset: Some(ResultSet::new()),
                })
            } else {
                // Use sequential scan
                Box::new(Scan::SeqScan {
                    table_oid,
                    schema,
                    cached_resultset: Some(ResultSet::new()),
                })
            };

            plan.push_step(scan);
        }

        TableReference::Join { left, join_type, right, on } => {
            // Build scans for both sides
            build_scan_steps(left, None, db, plan)?;
            build_scan_steps(right, None, db, plan)?;

            // TODO! Implement joins
        }

        TableReference::Subquery { query, alias } => {
            build_select_plan(query, db, plan)?;
        }
    }

    Ok(())
}


// Main function to build SELECT execution plan
fn build_select_plan(
    select: &SelectStatement,
    db: &Database,
    plan: &mut ExecutionPlan,
) -> std::io::Result<()> {

    if let Some(from) = &select.from {
        build_scan_steps(from, select.where_clause.as_ref(), db, plan)?;
    }


    if let Some(where_clause) = &select.where_clause {
        // Check if we need additional filtering beyond what index provides
        let needs_filter = true; // TODO: Determine based on index usage
        if needs_filter {

            let filter = Filter::new(where_clause.clone());
            plan.push_step(Box::new(filter));
        }
    }

    if !select.group_by.is_empty() {
        let group = GroupBy::new(select.group_by.clone(), select.having.clone());
        plan.push_step(Box::new(group));
    }

    // TODO: MUST TRANSFORM SELECT ITEMS INTO A HASHMAP OF TABLE -> COLUMNS
       // let project = Project::new(select.columns.iter().map(|c| c.column.to_string()),Schema::new());
       // plan.push_step(Box::new(project));

    // Step 5: Add order by operator (ORDER BY clause)
    if !select.order_by.is_empty() {
      //  let order = OrderBy::new(select.order_by.iter().map(|o| o.));
      //  plan.push_step(Box::new(order));
    }

    if let Some(limit) = select.limit {
        let limit_step = Limit::new(limit);
        plan.push_step(Box::new(limit_step));
    }

    Ok(())
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

                let col = Column::new_unindexed(
                    new_column.data_type,
                    &new_column.name,
                    Some(constraints),
                );
                table_columns.push(col);
            }

            let mut schema = Schema::from(table_columns.as_slice());

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

                    let column = Column::new_unindexed(
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
                    schema.drop_constraint(&constraint_name);
                    // Write the updated schema back
                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }

                AlterAction::AddConstraint(constraint) => {
                    let mut table_obj = db.get_obj(table_oid)?;
                    let mut schema = table_obj.get_schema();
                    match constraint {
                        TableConstraintExpr::PrimaryKey(columns) => {
                            let ct = TableConstraint::PrimaryKey(columns);
                            schema.add_constraint(ct);
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
                            schema.add_constraint(ct);
                        }
                        TableConstraintExpr::Unique(columns) => {
                            let ct = TableConstraint::Unique(columns);
                            schema.add_constraint(ct);
                        }
                        _ => {
                            return Err(std::io::Error::other("Unsupported table constraint type"))
                        }
                    }

                    table_obj.set_meta(&schema.write_to()?);
                    db.update_obj(table_obj)?;
                }
            }

            Ok(ExecutionPlan::empty())
        }
        Statement::DropTable(drop) => {
            // Check if table exists only if IF EXISTS is not specified
            if let Ok(oid) = db.search_obj(&drop.table) {
                // Drop the table
                db.remove_obj(oid, &drop.table, drop.cascade)?;
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
            let mut table_schema = table_obj.get_schema();
            let row_id = table_schema.columns.first().unwrap().clone();

            debug_assert!(
                create_idx.columns.len() == 1,
                "Multicolumn indexes are not supported yet!"
            );

            let index_schema = Schema::from(
                [
                    table_schema
                        .find_col(&create_idx.columns[0].name)
                        .ok_or(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Column not Found",
                        ))?
                        .clone(),
                    row_id,
                ]
                .as_slice(),
            );
            let index_name = format!("{}_{}_idx", create_idx.table, create_idx.columns[0].name);

            // Verify columns exist in the table
            if let Some(column) = table_schema.find_column_mut(&create_idx.columns[0].name) {
                let obj = DBObject::new(
                    ObjectType::Index,
                    &index_name,
                    Some(&index_schema.write_to()?),
                );
                db.index_obj(&obj)?;
                let index_id = db.create_obj(obj)?;
                column.set_index(index_name);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Column {} does not exist", &create_idx.columns[0].name),
                ));
            };

            Ok(ExecutionPlan::empty())
        },
        Statement::Select(select) => {
          Ok(ExecutionPlan::empty())
        },
        _ => Ok(ExecutionPlan::empty())
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
    use crate::database::schema::Schema;
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

        let mut id = Column::new_unindexed(DataTypeKind::Int, "id", None);
        let email = Column::new_unindexed(DataTypeKind::Text, "email", None);
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
