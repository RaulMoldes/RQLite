///! SQL Analyzer is in charge of performing validation before resolution.
///! Mainly, it validates column existence, type compatibility validation for literals,
///! constraint violation detection, duplicate definition detection and transaction state management.
use crate::{
    database::{
        SharedCatalog,
        errors::{AlreadyExists, AnalyzerError, AnalyzerResult},
        schema::{Column, Relation},
    },
    sql::ast::{
        AlterAction, AlterColumnAction, ColumnConstraintExpr, Expr, SelectItem, SelectStatement,
        Statement, TableConstraintExpr, TableReference, TransactionStatement, Values,
    },
    structures::bplustree::SearchResult,
    transactions::worker::Worker,
    types::DataTypeKind,
};

use regex::Regex;
use std::collections::HashSet;

pub const AGGREGATE_FUNCTORS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

fn is_date_iso(s: &str) -> bool {
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    re.is_match(s)
}

fn is_datetime_iso(s: &str) -> bool {
    let re = Regex::new(r"(?i)^\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}$").unwrap();
    re.is_match(s)
}

/// Analyzer context
struct AnalyzerCtx {
    on_transaction: bool,
}

impl AnalyzerCtx {
    fn new() -> Self {
        Self {
            on_transaction: false,
        }
    }
}

pub struct Analyzer {
    catalog: SharedCatalog,
    worker: Worker,
    ctx: AnalyzerCtx,
}

impl Analyzer {
    pub fn new(catalog: SharedCatalog, worker: Worker) -> Self {
        Self {
            catalog,
            worker,
            ctx: AnalyzerCtx::new(),
        }
    }

    pub fn analyze(&mut self, stmt: &Statement) -> AnalyzerResult<()> {
        match stmt {
            Statement::Transaction(t) => self.analyze_transaction(t),
            Statement::Select(s) => self.analyze_select(s),
            Statement::With(s) => self.analyze_with(s),
            Statement::Insert(s) => self.analyze_insert(s),
            Statement::CreateTable(s) => self.analyze_create_table(s),
            Statement::AlterTable(s) => self.analyze_alter_table(s),
            Statement::Delete(s) => self.analyze_delete(s),
            Statement::Update(s) => self.analyze_update(s),
            Statement::DropTable(s) => self.analyze_drop_table(s),
            Statement::CreateIndex(s) => self.analyze_create_index(s),
        }
    }

    fn analyze_transaction(&mut self, t: &TransactionStatement) -> AnalyzerResult<()> {
        match t {
            TransactionStatement::Begin => {
                if self.ctx.on_transaction {
                    return Err(AnalyzerError::AlreadyStarted);
                }
                self.ctx.on_transaction = true;
            }
            TransactionStatement::Commit | TransactionStatement::Rollback => {
                if !self.ctx.on_transaction {
                    return Err(AnalyzerError::NotStarted);
                }
                self.ctx.on_transaction = false;
            }
        }
        Ok(())
    }

    fn analyze_select(&self, stmt: &SelectStatement) -> AnalyzerResult<()> {
        // Validate FROM clause - tables must exist
        if let Some(table_ref) = &stmt.from {
            self.validate_table_ref(table_ref)?;
        }

        // Validate expressions (recursively handles subqueries)
        for item in &stmt.columns {
            if let SelectItem::ExprWithAlias { expr, .. } = item {
                self.validate_expr(expr)?;
            }
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.validate_expr(where_clause)?;
        }

        if let Some(having) = &stmt.having {
            self.validate_expr(having)?;
        }

        for group_by in &stmt.group_by {
            self.validate_expr(group_by)?;
        }

        for order_by in &stmt.order_by {
            self.validate_expr(&order_by.expr)?;
        }

        Ok(())
    }

    fn analyze_with(&self, stmt: &crate::sql::ast::WithStatement) -> AnalyzerResult<()> {
        // Validate each CTE query
        for (_, cte_query) in &stmt.ctes {
            self.analyze_select(cte_query)?;
        }

        // Validate main body
        self.analyze_select(&stmt.body)
    }

    fn analyze_insert(&self, stmt: &crate::sql::ast::InsertStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema();

        // Validate specified columns exist
        let columns: Vec<&Column> = if let Some(cols) = &stmt.columns {
            cols.iter()
                .map(|c| {
                    schema
                        .column(c)
                        .ok_or_else(|| AnalyzerError::NotFound(c.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            schema.columns.iter().collect()
        };

        match &stmt.values {
            Values::Query(select_expr) => {
                self.analyze_select(select_expr)?;
            }
            Values::Values(expr_list) => {
                for (i, row) in expr_list.iter().enumerate() {
                    if columns.len() != row.len() {
                        return Err(AnalyzerError::ColumnValueCountMismatch(i));
                    }

                    for (j, value) in row.iter().enumerate() {
                        // NOT NULL constraint check
                        if columns[j].is_non_null() && matches!(value, Expr::Null) {
                            return Err(AnalyzerError::ConstraintViolation(
                                "Non null constraint".to_string(),
                                columns[j].name.to_owned(),
                            ));
                        }

                        // Type validation for literals
                        self.validate_literal_value(columns[j].dtype, value)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn analyze_create_table(
        &self,
        stmt: &crate::sql::ast::CreateTableStatement,
    ) -> AnalyzerResult<()> {
        // Check table doesn't already exist
        if let Ok(SearchResult::Found(_)) = self
            .catalog
            .lookup_relation(&stmt.table, self.worker.clone())
        {
            return Err(AnalyzerError::AlreadyExists(AlreadyExists::Table(
                stmt.table.clone(),
            )));
        }

        let mut has_primary = false;
        let mut column_set = HashSet::with_capacity(stmt.columns.len());

        for col in &stmt.columns {
            if column_set.contains(&col.name) {
                return Err(AnalyzerError::DuplicatedColumn(col.name.to_string()));
            }
            column_set.insert(col.name.to_string());

            let mut has_default = false;
            let mut has_not_null = false;
            let mut has_unique = false;

            for constraint in &col.constraints {
                match constraint {
                    ColumnConstraintExpr::Default(_) => {
                        if has_default {
                            return Err(AnalyzerError::DuplicatedConstraint(col.name.to_string()));
                        }
                        has_default = true;
                    }
                    ColumnConstraintExpr::NotNull => {
                        if has_not_null {
                            return Err(AnalyzerError::DuplicatedConstraint(col.name.to_string()));
                        }
                        has_not_null = true;
                    }
                    ColumnConstraintExpr::Unique => {
                        if has_unique {
                            return Err(AnalyzerError::DuplicatedConstraint(col.name.to_string()));
                        }
                        has_unique = true;
                    }
                    ColumnConstraintExpr::PrimaryKey => {
                        if has_primary {
                            return Err(AnalyzerError::MultiplePrimaryKeys);
                        }
                        has_primary = true;
                    }
                    ColumnConstraintExpr::ForeignKey { table, column } => {
                        let other_relation = self.get_relation(table)?;
                        if let Some(schema_col) = other_relation.schema().column(column) {
                            if !schema_col.dtype.can_be_coerced(col.data_type)
                                && !col.data_type.can_be_coerced(schema_col.dtype)
                            {
                                return Err(AnalyzerError::InvalidDataType(col.data_type));
                            }
                        } else {
                            return Err(AnalyzerError::NotFound(column.to_string()));
                        }
                    }
                    _ => {}
                }
            }
        }

        for constraint in &stmt.constraints {
            match constraint {
                TableConstraintExpr::PrimaryKey(columns) => {
                    if !columns.iter().all(|c| column_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                    if has_primary {
                        return Err(AnalyzerError::MultiplePrimaryKeys);
                    }
                    has_primary = true;
                }
                TableConstraintExpr::Unique(columns) => {
                    if !columns.iter().all(|c| column_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                }
                TableConstraintExpr::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    if !columns.iter().all(|c| column_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }

                    let other_relation = self.get_relation(ref_table)?;
                    let other_set: HashSet<String> = other_relation
                        .columns()
                        .iter()
                        .map(|c| c.name().to_string())
                        .collect();

                    if ref_columns.iter().any(|c| !other_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn analyze_alter_table(
        &self,
        stmt: &crate::sql::ast::AlterTableStatement,
    ) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let obj_set: HashSet<String> = relation
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        match &stmt.action {
            AlterAction::AddColumn(col) => {
                if obj_set.contains(&col.name) {
                    return Err(AnalyzerError::AlreadyExists(AlreadyExists::Column(
                        relation.name().to_string(),
                        col.name.to_string(),
                    )));
                }
            }
            AlterAction::AlterColumn(alter_col) => {
                if !obj_set.contains(&alter_col.name) {
                    return Err(AnalyzerError::MissingColumns);
                }

                let schema = relation.schema();
                let column = schema
                    .column(&alter_col.name)
                    .ok_or_else(|| AnalyzerError::NotFound(alter_col.name.to_string()))?;

                match &alter_col.action {
                    AlterColumnAction::SetDefault(value) => {
                        if matches!(value, Expr::Null) && column.is_non_null() {
                            return Err(AnalyzerError::ConstraintViolation(
                                "Non null constraint".to_string(),
                                column.name.to_string(),
                            ));
                        }
                    }
                    AlterColumnAction::SetDataType(value) => {
                        if matches!(value, DataTypeKind::Null) && column.is_non_null() {
                            return Err(AnalyzerError::ConstraintViolation(
                                "Non null constraint".to_string(),
                                column.name.to_string(),
                            ));
                        }
                    }
                    _ => {}
                }
            }
            AlterAction::AddConstraint(ct) => match ct {
                TableConstraintExpr::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    if columns.iter().any(|c| !obj_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                    let other_relation = self.get_relation(ref_table)?;
                    let other_set: HashSet<String> = other_relation
                        .columns()
                        .iter()
                        .map(|c| c.name().to_string())
                        .collect();
                    if ref_columns.iter().any(|c| !other_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                }
                TableConstraintExpr::PrimaryKey(cols) => {
                    let schema = relation.schema();
                    if schema.has_pk() {
                        return Err(AnalyzerError::MultiplePrimaryKeys);
                    }
                    if cols.iter().any(|c| !obj_set.contains(c)) {
                        return Err(AnalyzerError::MissingColumns);
                    }
                }
                _ => {}
            },
            AlterAction::DropColumn(col) => {
                if !obj_set.contains(col) {
                    return Err(AnalyzerError::MissingColumns);
                }
            }
            AlterAction::DropConstraint(name) => {
                let schema = relation.schema();
                if !schema.has_constraint(name) {
                    return Err(AnalyzerError::NotFound(name.to_string()));
                }
            }
        }

        Ok(())
    }

    fn analyze_delete(&self, stmt: &crate::sql::ast::DeleteStatement) -> AnalyzerResult<()> {
        self.get_relation(&stmt.table)?;

        if let Some(clause) = &stmt.where_clause {
            self.validate_expr(clause)?;
        }

        Ok(())
    }

    fn analyze_update(&self, stmt: &crate::sql::ast::UpdateStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema();

        if let Some(clause) = &stmt.where_clause {
            self.validate_expr(clause)?;
        }

        for set_clause in &stmt.set_clauses {
            let col = schema
                .column(&set_clause.column)
                .ok_or_else(|| AnalyzerError::NotFound(set_clause.column.to_string()))?;

            if matches!(set_clause.value, Expr::Null) && col.is_non_null() {
                return Err(AnalyzerError::ConstraintViolation(
                    "Non null constraint".to_string(),
                    col.name.to_string(),
                ));
            }

            self.validate_expr(&set_clause.value)?;
        }

        Ok(())
    }

    fn analyze_drop_table(&self, stmt: &crate::sql::ast::DropTableStatement) -> AnalyzerResult<()> {
        if !stmt.if_exists {
            self.get_relation(&stmt.table)?;
        }
        Ok(())
    }

    fn analyze_create_index(
        &self,
        stmt: &crate::sql::ast::CreateIndexStatement,
    ) -> AnalyzerResult<()> {
        if self.get_relation(&stmt.name).is_ok() && !stmt.if_not_exists {
            return Err(AnalyzerError::AlreadyExists(AlreadyExists::Index(
                stmt.name.clone(),
            )));
        }

        let relation = self.get_relation(&stmt.table)?;
        let obj_set: HashSet<String> = relation
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        let columns: Vec<String> = stmt.columns.iter().map(|c| c.name.to_string()).collect();
        if columns.iter().any(|c| !obj_set.contains(c)) {
            return Err(AnalyzerError::MissingColumns);
        }

        Ok(())
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn validate_table_ref(&self, table_ref: &TableReference) -> AnalyzerResult<()> {
        match table_ref {
            TableReference::Table { name, .. } => {
                // Table must exist (CTEs are handled by resolver)
                // We do a soft check here - if not found in catalog, it might be a CTE
                let _ = self.get_relation(name); // Ignore error - might be CTE
            }
            TableReference::Join {
                left, right, on, ..
            } => {
                self.validate_table_ref(left)?;
                self.validate_table_ref(right)?;
                if let Some(on_expr) = on {
                    self.validate_expr(on_expr)?;
                }
            }
            TableReference::Subquery { query, .. } => {
                self.analyze_select(query)?;
            }
        }
        Ok(())
    }

    /// Validates expressions recursively, including nested subqueries
    fn validate_expr(&self, expr: &Expr) -> AnalyzerResult<()> {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.validate_expr(expr)?;
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.validate_expr(arg)?;
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.validate_expr(op)?;
                }
                for clause in when_clauses {
                    self.validate_expr(&clause.condition)?;
                    self.validate_expr(&clause.result)?;
                }
                if let Some(else_expr) = else_clause {
                    self.validate_expr(else_expr)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(low)?;
                self.validate_expr(high)?;
            }
            Expr::Subquery(select) => {
                // Recursively analyze subquery
                self.analyze_select(select)?;
            }
            Expr::Exists(select) => {
                self.analyze_select(select)?;
            }
            Expr::List(items) => {
                for item in items {
                    self.validate_expr(item)?;
                }
            }
            // Literals and identifiers don't need validation at analyzer level
            _ => {}
        }
        Ok(())
    }

    /// Validates literal values against expected column types
    fn validate_literal_value(&self, dtype: DataTypeKind, value: &Expr) -> AnalyzerResult<()> {
        match value {
            Expr::Number(f) => match dtype {
                DataTypeKind::Boolean => {
                    if *f != 0.0 && *f != 1.0 {
                        return Err(AnalyzerError::InvalidValue(dtype));
                    }
                }
                DataTypeKind::Byte | DataTypeKind::SmallInt => {
                    if *f < i8::MIN as f64 || *f > i8::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::SmallUInt => {
                    if *f < 0.0 || *f > u8::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::HalfInt => {
                    if *f < i16::MIN as f64 || *f > i16::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::HalfUInt => {
                    if *f < 0.0 || *f > u16::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::Int => {
                    if *f < i32::MIN as f64 || *f > i32::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::UInt | DataTypeKind::Date => {
                    if *f < 0.0 || *f > u32::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::BigInt => {
                    if *f < i64::MIN as f64 || *f > i64::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                DataTypeKind::BigUInt => {
                    if *f < 0.0 || *f > u64::MAX as f64 {
                        return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                    }
                }
                _ => {}
            },
            Expr::String(item) => {
                let numeric = item.parse::<f64>().ok();

                match dtype {
                    DataTypeKind::Int => {
                        if let Some(num) = numeric {
                            if num < i32::MIN as f64 || num > i32::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else if dtype.is_numeric() {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::Char => {
                        if item.len() > 1 {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Char));
                        }
                    }
                    DataTypeKind::Boolean => {
                        let upper = item.to_uppercase();
                        if upper != "TRUE" && upper != "FALSE" && upper != "1" && upper != "0" {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Boolean));
                        }
                    }
                    DataTypeKind::Date => {
                        if !is_date_iso(item) {
                            return Err(AnalyzerError::InvalidFormat(
                                item.to_string(),
                                "YYYY-MM-DD".to_string(),
                            ));
                        }
                    }
                    DataTypeKind::DateTime => {
                        if !is_datetime_iso(item) {
                            return Err(AnalyzerError::InvalidFormat(
                                item.to_string(),
                                "YYYY-MM-DDTHH:MM:SS".to_string(),
                            ));
                        }
                    }
                    _ => {}
                }
            }
            Expr::Boolean(_) => {
                if !matches!(dtype, DataTypeKind::Boolean) && !dtype.is_numeric() {
                    return Err(AnalyzerError::InvalidValue(dtype));
                }
            }
            // For complex expressions, validation happens recursively
            _ => {
                self.validate_expr(value)?;
            }
        }
        Ok(())
    }

    fn get_relation(&self, name: &str) -> AnalyzerResult<Relation> {
        self.catalog
            .get_relation(name, self.worker.clone())
            .map_err(|_| AnalyzerError::NotFound(name.to_string()))
    }
}

#[cfg(test)]
mod analyzer_tests {
    use super::*;
    use crate::{
        AxmosDBConfig, IncrementalVaccum, TextEncoding,
        database::{Database, schema::Schema},
        io::pager::{Pager, SharedPager},
        sql::{lexer::Lexer, parser::Parser},
    };
    use serial_test::serial;
    use std::path::Path;

    fn create_db(path: impl AsRef<Path>) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(100),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false); // NOT NULL
        users_schema.add_column("age", DataTypeKind::Int, false, false, false);
        users_schema.add_column("created_at", DataTypeKind::DateTime, false, false, false);
        let worker = db.main_worker_cloned();
        db.catalog()
            .create_table("users", users_schema, worker.clone())?;

        let mut orders_schema = Schema::new();
        orders_schema.add_column("id", DataTypeKind::Int, true, true, false);
        orders_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        orders_schema.add_column("amount", DataTypeKind::Double, false, false, false);
        db.catalog().create_table("orders", orders_schema, worker)?;

        Ok(db)
    }

    fn analyze_sql(sql: &str, db: &Database) -> AnalyzerResult<()> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().expect("Parse failed");
        let mut analyzer = Analyzer::new(db.catalog(), db.main_worker_cloned());
        analyzer.analyze(&stmt)
    }

    #[test]
    #[serial]
    fn test_insert_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("INSERT INTO users (id, unknown) VALUES (1, 'x')", &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_update_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("UPDATE users SET unknown = 'x'", &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_not_null_violation_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name, email) VALUES (1, 'John', NULL)",
            &db,
        );
        assert!(matches!(
            result,
            Err(AnalyzerError::ConstraintViolation(_, col)) if col == "email"
        ));
    }

    #[test]
    #[serial]
    fn test_not_null_violation_update() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("UPDATE users SET email = NULL WHERE id = 1", &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::ConstraintViolation(_, col)) if col == "email"
        ));
    }

    #[test]
    #[serial]
    fn test_column_value_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name) VALUES (1, 'John', 'extra')",
            &db,
        );
        assert!(matches!(
            result,
            Err(AnalyzerError::ColumnValueCountMismatch(_))
        ));
    }

    #[test]
    #[serial]
    fn test_invalid_int_value() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'John', 'j@t.com', 'not_int')",
            &db,
        );
        assert!(matches!(result, Err(AnalyzerError::InvalidValue(_))));
    }

    #[test]
    #[serial]
    fn test_arithmetic_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name, email, age) VALUES (9999999999999, 'John', 'j@t.com', 25)",
            &db,
        );
        assert!(matches!(
            result,
            Err(AnalyzerError::ArithmeticOverflow(_, _))
        ));
    }

    #[test]
    #[serial]
    fn test_invalid_datetime_format() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name, email, age, created_at) VALUES (1, 'John', 'j@t.com', 25, 'bad-date')",
            &db,
        );
        assert!(matches!(result, Err(AnalyzerError::InvalidFormat(_, _))));
    }

    #[test]
    #[serial]
    fn test_valid_datetime_format() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "INSERT INTO users (id, name, email, age, created_at) VALUES (1, 'John', 'j@t.com', 25, '2024-01-15T10:30:00')",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_table_already_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("CREATE TABLE users (id INT)", &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::AlreadyExists(AlreadyExists::Table(name))) if name == "users"
        ));
    }

    #[test]
    #[serial]
    fn test_duplicate_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("CREATE TABLE test (id INT, id TEXT)", &db);
        assert!(matches!(result, Err(AnalyzerError::DuplicatedColumn(_))));
    }

    #[test]
    #[serial]
    fn test_multiple_primary_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "CREATE TABLE test (id INT PRIMARY KEY, code INT PRIMARY KEY)",
            &db,
        );
        assert!(matches!(result, Err(AnalyzerError::MultiplePrimaryKeys)));
    }

    #[test]
    #[serial]
    fn test_foreign_key_invalid_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "CREATE TABLE test (id INT, user_id INT REFERENCES users(nonexistent))",
            &db,
        );
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_alter_add_existing_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("ALTER TABLE users ADD COLUMN name TEXT", &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::AlreadyExists(AlreadyExists::Column(_, col))) if col == "name"
        ));
    }

    #[test]
    #[serial]
    fn test_alter_drop_nonexistent_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("ALTER TABLE users DROP COLUMN nonexistent", &db);
        assert!(matches!(result, Err(AnalyzerError::MissingColumns)));
    }

    #[test]
    #[serial]
    fn test_nested_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        let mut analyzer = Analyzer::new(db.catalog(), db.main_worker_cloned());

        let lexer = Lexer::new("BEGIN");
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().unwrap();
        assert!(analyzer.analyze(&stmt).is_ok());

        let lexer = Lexer::new("BEGIN");
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().unwrap();
        assert!(matches!(
            analyzer.analyze(&stmt),
            Err(AnalyzerError::AlreadyStarted)
        ));
    }

    #[test]
    #[serial]
    fn test_commit_without_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql("COMMIT", &db);
        assert!(matches!(result, Err(AnalyzerError::NotStarted)));
    }

    #[test]
    #[serial]
    fn test_subquery_in_where_validated() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        // Valid nested subquery
        let result = analyze_sql(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_exists_subquery_validated() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]

    fn test_subquery_in_select_validated() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = analyze_sql(
            "SELECT name,(SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users",
            &db,
        );
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_select() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(analyze_sql("SELECT * FROM users WHERE age > 18", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(
            analyze_sql(
                "INSERT INTO users (id, name, email, age) VALUES (1, 'John', 'j@t.com', 25)",
                &db
            )
            .is_ok()
        );
    }

    #[test]
    #[serial]
    fn test_valid_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(
            analyze_sql(
                "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
                &db
            )
            .is_ok()
        );
    }

    #[test]
    #[serial]
    fn test_valid_update() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(analyze_sql("UPDATE users SET name = 'Ramon'", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(analyze_sql("DELETE FROM users WHERE name = 'Ramon'", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_drop_table() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(analyze_sql("DROP TABLE users", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_alter() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        assert!(analyze_sql("ALTER TABLE users ALTER COLUMN name SET NOT NULL", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_valid_subqueries() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();
        // FAILS IF YOU ADD THE COMMA. WE ARE USING CURRENTLY THE COMMA AS A CONTINUATION MARKER FOR SUBQUERIES.
        assert!(
            analyze_sql(
                "WITH adults as (SELECT * FROM users WHERE age > 10) SELECT * FROM adults;",
                &db
            )
            .is_ok()
        );
    }
}
