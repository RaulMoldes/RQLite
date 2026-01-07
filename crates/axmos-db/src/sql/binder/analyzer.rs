use crate::{
    io::pager::BtreeBuilder,
    multithreading::coordinator::Snapshot,
    schema::{
        base::{Column, Relation, Schema},
        catalog::CatalogTrait,
    },
    sql::{
        binder::{DatabaseItem, ScopeEntry, ScopeError, ScopeStack},
        parser::{
            ParserError,
            ast::{
                AlterAction, AlterColumnAction, AlterTableStatement, BinaryOperator,
                CreateIndexStatement, CreateTableStatement, DeleteStatement, DropTableStatement,
                Expr, InsertStatement, SelectItem, SelectStatement, Statement, TableConstraintExpr,
                TableReference, UnaryOperator, UpdateStatement, Values, WithStatement,
            },
        },
    },
    types::DataTypeKind,
};

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

pub(crate) const AGGREGATE_FUNCTORS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

/// Analyzer errors (Step 2: AST validation)
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzerError {
    Parser(ParserError),
    Scope(ScopeError),
    MultiplePrimaryKeys,
    AlreadyExists(DatabaseItem),
    InvalidColumnCount((usize, usize)),
    DuplicatedConstraint(String),
    ArithmeticOverflow(f64, f64),
    ArithmeticUnderflow(f64, f64),
    NotFound(DatabaseItem),
    InvalidDataType(DataTypeKind),
    Other(String),
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Parser(e) => write!(f, "parser error: {}", e),
            Self::Scope(e) => write!(f, "scope error: {}", e),

            Self::AlreadyExists(e) => write!(f, "{} already exists", e),
            Self::NotFound(obj) => write!(f, "{} not found", obj),
            Self::MultiplePrimaryKeys => write!(f, "Only one primary key per table is allowed"),

            Self::DuplicatedConstraint(s) => write!(f, "duplicated constraint on column '{}'", s),
            Self::InvalidDataType(data) => write!(f, "invalid data type {data}"),
            Self::ArithmeticOverflow(value, max_value) => write!(
                f,
                "arithmetic overflow on type with value {}. max value is {}",
                value, max_value
            ),
            Self::ArithmeticUnderflow(value, min_value) => write!(
                f,
                "arithmetic overflow on type with value {}. min value is {}",
                value, min_value
            ),
            Self::InvalidColumnCount((num_cols, num_values)) => write!(
                f,
                "column count mismatch. Number of columns is: {}. But number of values is: {}",
                num_cols, num_values
            ),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for AnalyzerError {}

impl From<ParserError> for AnalyzerError {
    fn from(value: ParserError) -> Self {
        Self::Parser(value)
    }
}

impl From<ScopeError> for AnalyzerError {
    fn from(value: ScopeError) -> Self {
        Self::Scope(value)
    }
}

pub type AnalyzerResult<T> = Result<T, AnalyzerError>;

pub(crate) struct Analyzer<C: CatalogTrait> {
    catalog: C,
    tree_builder: BtreeBuilder,
    snapshot: Snapshot,
    ctes: HashMap<String, Schema>,
    scopes: ScopeStack,
}

impl<C> Analyzer<C>
where
    C: CatalogTrait,
{
    pub(crate) fn new(catalog: C, builder: BtreeBuilder, snapshot: Snapshot) -> Self {
        Self {
            catalog,
            tree_builder: builder,
            snapshot: snapshot,
            ctes: HashMap::new(),
            scopes: ScopeStack::new(),
        }
    }

    fn analyze_select(&mut self, stmt: &SelectStatement) -> AnalyzerResult<()> {
        // Push a new scope for this SELECT
        self.scopes.push();

        // First, build the scope from FROM clause (tables must be known before validating columns)
        if let Some(table_ref) = &stmt.from {
            self.build_scope(table_ref)?;
        }

        // Afterwards validate columns
        for item in &stmt.columns {
            match item {
                SelectItem::Star => {
                    // Star is valid only if we have at least one table in scope
                    if self.scopes.current().map_or(true, |s| s.is_empty()) {
                        return Err(AnalyzerError::Other(
                            "SELECT * requires a FROM clause".to_string(),
                        ));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    self.validate_expr(expr)?;

                    if let Some(alias_name) = alias {
                        let dtype = self.infer_expr_type(expr)?;
                        if let Some(scope) = self.scopes.current_mut() {
                            scope.add_output_alias(alias_name.clone(), dtype);
                        }
                    }
                }
            }
        }

        // Validate WHERE clause
        if let Some(where_clause) = &stmt.where_clause {
            self.validate_expr(where_clause)?;
        }

        // Validate GROUP BY expressions
        for group_expr in &stmt.group_by {
            self.validate_expr(group_expr)?;
        }

        // Validate HAVING clause
        if let Some(having) = &stmt.having {
            self.validate_expr(having)?;
        }

        // Validate ORDER BY expressions
        for order_by in &stmt.order_by {
            self.validate_expr(&order_by.expr)?;
        }

        // Pop the scope when done
        self.scopes.pop();

        Ok(())
    }

    /// Builds a scope from an existing table reference in the AST
    fn build_scope(&mut self, table_ref: &TableReference) -> AnalyzerResult<()> {
        match table_ref {
            TableReference::Table { name, alias } => {
                let (schema, table_id, cte_idx) = if let Some(cte_schema) = self.ctes.get(name) {
                    (cte_schema.clone(), None, Some(0)) // TODO: track CTE index properly
                } else {
                    let relation = self.get_relation(name)?;
                    (relation.schema().clone(), Some(relation.object_id()), None)
                };

                let ref_name = alias.clone().unwrap_or_else(|| name.clone());
                let scope_index = self.scopes.current().map_or(0, |s| s.entry_order.len());

                let entry = ScopeEntry {
                    ref_name,
                    scope_index,
                    schema,
                    table_id,
                    cte_idx,
                };

                if let Some(scope) = self.scopes.current_mut() {
                    scope.add_entry(entry)?;
                }
                Ok(())
            }
            TableReference::Join {
                left, right, on, ..
            } => {
                self.build_scope(left)?;
                self.build_scope(right)?;

                if let Some(on_expr) = on {
                    self.validate_expr(on_expr)?;
                }
                Ok(())
            }
            TableReference::Subquery { query, alias } => {
                self.analyze_select(query)?;
                let schema = self.infer_select_schema(query)?;
                let scope_index = self.scopes.current().map_or(0, |s| s.entry_order.len());

                let entry = ScopeEntry {
                    ref_name: alias.clone(),
                    scope_index,
                    schema,
                    table_id: None,
                    cte_idx: None,
                };

                if let Some(scope) = self.scopes.current_mut() {
                    scope.add_entry(entry)?;
                }
                Ok(())
            }
        }
    }

    /// Validates expressions recursively, resolving column references against the current scope
    fn validate_expr(&mut self, expr: &Expr) -> AnalyzerResult<()> {
        match expr {
            Expr::Identifier(name) => {
                // Resolve unqualified column
                self.scopes.resolve_column(name, None)?;
                Ok(())
            }
            Expr::QualifiedIdentifier { table, column } => {
                // Resolve qualified column (table.column)
                self.scopes.resolve_column(column, Some(table))?;
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
                Ok(())
            }
            Expr::UnaryOp { expr, .. } => self.validate_expr(expr),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.validate_expr(arg)?;
                }
                Ok(())
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
                Ok(())
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(low)?;
                self.validate_expr(high)?;
                Ok(())
            }
            Expr::Subquery(select) => {
                // Subqueries have their own scope but can reference outer columns
                self.analyze_select(select)
            }
            Expr::Exists(select) => self.analyze_select(select),
            Expr::List(items) => {
                for item in items {
                    self.validate_expr(item)?;
                }
                Ok(())
            }
            // Literals don't need validation
            Expr::Number(_) | Expr::String(_) | Expr::Boolean(_) | Expr::Null | Expr::Star => {
                Ok(())
            }
        }
    }

    pub(crate) fn analyze(&mut self, stmt: &Statement) -> AnalyzerResult<()> {
        // Reset state for new analysis
        self.ctes.clear();

        match stmt {
            Statement::Transaction(_) => Ok(()),
            Statement::Select(s) => self.analyze_select(s),
            Statement::With(s) => self.analyze_with(s),
            Statement::Insert(s) => self.analyze_insert(s),
            Statement::Update(s) => self.analyze_update(s),
            Statement::Delete(s) => self.analyze_delete(s),
            Statement::CreateTable(s) => self.analyze_create_table(s),
            Statement::AlterTable(s) => self.analyze_alter_table(s),
            Statement::DropTable(s) => self.analyze_drop_table(s),
            Statement::CreateIndex(s) => self.analyze_create_index(s),
        }
    }

    /// Validates that a numeric value is within bounds.
    fn validate_bounds(value: f64, min: f64, max: f64) -> AnalyzerResult<()> {
        if value > max {
            return Err(AnalyzerError::ArithmeticOverflow(value, max));
        }
        if value < min {
            return Err(AnalyzerError::ArithmeticUnderflow(value, min));
        }

        Ok(())
    }

    /// Validates literal values against expected column types
    fn validate_return_type(&mut self, dtype: DataTypeKind, value: &Expr) -> AnalyzerResult<()> {
        match value {
            Expr::Number(f) => match dtype {
                DataTypeKind::Int => {
                    Self::validate_bounds(*f, i32::MIN as f64, i32::MAX as f64)?;
                }
                DataTypeKind::UInt => {
                    Self::validate_bounds(*f, u32::MIN as f64, u32::MAX as f64)?;
                }
                DataTypeKind::BigInt => {
                    Self::validate_bounds(*f, i64::MIN as f64, i64::MAX as f64)?;
                }
                DataTypeKind::BigUInt => {
                    Self::validate_bounds(*f, u64::MIN as f64, u64::MAX as f64)?;
                }
                _ => return Err(AnalyzerError::InvalidDataType(dtype)),
            }, // Currently we do not allow converting strings to numbers.
            Expr::String(item) => {
                if !matches!(dtype, DataTypeKind::Blob) {
                    return Err(AnalyzerError::InvalidDataType(dtype));
                }
            }
            Expr::Boolean(_) => {
                if !matches!(dtype, DataTypeKind::Bool) {
                    return Err(AnalyzerError::InvalidDataType(dtype));
                }
            }
            // For complex expressions, validation happens recursively
            _ => {
                self.validate_expr(value)?;
            }
        }
        Ok(())
    }

    /// Infers the output schema of a SELECT statement.
    /// Used for subqueries in FROM clause to know what columns they expose.
    /// Infers the output schema of a SELECT statement.
    fn infer_select_schema(&mut self, stmt: &SelectStatement) -> AnalyzerResult<Schema> {
        self.scopes.push();

        if let Some(from) = &stmt.from {
            self.build_scope(from)?;
        }

        let mut columns = Vec::new();

        for item in &stmt.columns {
            match item {
                SelectItem::Star => {
                    //  All columns selected, then we push all columns from all scopes to the schema.
                    if let Some(scope) = self.scopes.current() {
                        for entry in scope.iter() {
                            for col in entry.schema.iter_columns() {
                                columns.push(col.clone());
                            }
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let col = self.infer_column_from_expr(expr, alias.clone())?;
                    columns.push(col);
                }
            }
        }

        self.scopes.pop();
        Ok(Schema::new_table(columns))
    }

    /// Infers the data type of an expression.
    fn infer_expr_type(&self, expr: &Expr) -> AnalyzerResult<DataTypeKind> {
        match expr {
            Expr::Number(_) => Ok(DataTypeKind::Double),
            Expr::String(_) => Ok(DataTypeKind::Blob),
            Expr::Boolean(_) => Ok(DataTypeKind::Bool),
            Expr::Null => Ok(DataTypeKind::Null),

            Expr::Identifier(name) => {
                let resolved = self.scopes.resolve_column(name, None)?;
                Ok(resolved.data_type)
            }
            Expr::QualifiedIdentifier { table, column } => {
                let resolved = self.scopes.resolve_column(column, Some(table))?;
                Ok(resolved.data_type)
            }

            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => Ok(DataTypeKind::Double),

                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::Gt
                | BinaryOperator::Le
                | BinaryOperator::Ge
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Like
                | BinaryOperator::NotLike
                | BinaryOperator::In
                | BinaryOperator::NotIn
                | BinaryOperator::Is
                | BinaryOperator::IsNot => Ok(DataTypeKind::Bool),

                BinaryOperator::Concat => Ok(DataTypeKind::Blob),
            },

            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(DataTypeKind::Bool),
                UnaryOperator::Plus | UnaryOperator::Minus => self.infer_expr_type(expr),
            },

            Expr::FunctionCall { name, args, .. } => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    "COUNT" => Ok(DataTypeKind::BigInt),
                    "SUM" | "AVG" => Ok(DataTypeKind::Double),
                    "MIN" | "MAX" => args
                        .first()
                        .map(|arg| self.infer_expr_type(arg))
                        .unwrap_or(Ok(DataTypeKind::Null)),
                    "COALESCE" => {
                        for arg in args {
                            let dtype = self.infer_expr_type(arg)?;
                            if dtype != DataTypeKind::Null {
                                return Ok(dtype);
                            }
                        }
                        Ok(DataTypeKind::Null)
                    }
                    "UPPER" | "LOWER" | "TRIM" | "CONCAT" => Ok(DataTypeKind::Blob),
                    "ABS" | "ROUND" | "CEIL" | "FLOOR" => Ok(DataTypeKind::Double),
                    _ => Ok(DataTypeKind::Blob),
                }
            }

            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                if let Some(clause) = when_clauses.first() {
                    return self.infer_expr_type(&clause.result);
                }
                if let Some(else_expr) = else_clause {
                    return self.infer_expr_type(else_expr);
                }
                Ok(DataTypeKind::Null)
            }

            Expr::Between { .. } | Expr::Exists(_) => Ok(DataTypeKind::Bool),

            Expr::Subquery(select) => {
                if let Some(SelectItem::ExprWithAlias { expr, .. }) = select.columns.first() {
                    self.infer_expr_type(expr)
                } else {
                    Ok(DataTypeKind::Null)
                }
            }

            Expr::List(_) | Expr::Star => Ok(DataTypeKind::Null),
        }
    }

    /// Infers a column definition from an expression.
    /// Returns a Column with the inferred name and data type.
    fn infer_column_from_expr(&self, expr: &Expr, alias: Option<String>) -> AnalyzerResult<Column> {
        let (name, dtype) = match expr {
            Expr::Identifier(col_name) => {
                let name = alias.unwrap_or_else(|| col_name.clone());
                let dtype = self.infer_expr_type(expr)?;
                (name, dtype)
            }
            Expr::QualifiedIdentifier { column, .. } => {
                let name = alias.unwrap_or_else(|| column.clone());
                let dtype = self.infer_expr_type(expr)?;
                (name, dtype)
            }
            _ => {
                // For complex expressions, use alias or generate placeholder
                let name = alias.unwrap_or_else(|| "?column?".to_string());
                let dtype = self.infer_expr_type(expr)?;
                (name, dtype)
            }
        };

        Ok(Column::new_with_defaults(dtype, &name))
    }

    /// Utility that will raise the appropiate error when the searched table is not found in the catalog.
    fn check_relation_exists(&self, name: &str) -> AnalyzerResult<()> {
        self.catalog
            .bind_relation(name, &self.tree_builder, &self.snapshot)
            .map_err(|_| AnalyzerError::NotFound(DatabaseItem::Table(name.to_string())))?;

        Ok(())
    }

    /// Fetches a relation from the catalog by name
    fn get_relation(&self, name: &str) -> AnalyzerResult<Relation> {
        self.catalog
            .get_relation_by_name(name, &self.tree_builder, &self.snapshot)
            .map_err(|_| AnalyzerError::NotFound(DatabaseItem::Table(name.to_string())))
    }

    fn analyze_with(&mut self, stmt: &WithStatement) -> AnalyzerResult<()> {
        // Process each CTE and register its schema
        for (name, cte_query) in &stmt.ctes {
            // Analyze the CTE query
            self.analyze_select(cte_query)?;

            // Infer and store the CTE's schema for later reference
            let schema = self.infer_select_schema(cte_query)?;
            self.ctes.insert(name.clone(), schema);
        }

        // Analyze the main body with CTEs available
        self.analyze_select(&stmt.body)?;

        // Clean up CTEs after WITH statement
        for (name, _) in &stmt.ctes {
            self.ctes.remove(name);
        }

        Ok(())
    }

    fn analyze_insert(&mut self, stmt: &InsertStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema();

        // Validate specified columns exist
        let target_columns: Vec<usize> = if let Some(cols) = &stmt.columns {
            cols.iter()
                .map(|col_name| {
                    schema.column_index.get(col_name).copied().ok_or_else(|| {
                        AnalyzerError::NotFound(DatabaseItem::Column(
                            stmt.table.clone(),
                            col_name.clone(),
                        ))
                    })
                })
                .collect::<AnalyzerResult<Vec<_>>>()?
        } else {
            (1..schema.num_columns()).collect()
        };

        match &stmt.values {
            Values::Query(select) => {
                self.analyze_select(select)?;

                // Verify column count matches
                let select_col_count = self.count_select_columns(select);
                if select_col_count != target_columns.len() {
                    return Err(AnalyzerError::InvalidColumnCount((
                        target_columns.len(),
                        select_col_count,
                    )));
                }
            }
            Values::Values(rows) => {
                for (row_idx, row) in rows.iter().enumerate() {
                    if row.len() != target_columns.len() {
                        return Err(AnalyzerError::Other(format!(
                            "Row {} has {} values but {} columns specified",
                            row_idx,
                            row.len(),
                            target_columns.len()
                        )));
                    }

                    // Validate each value expression
                    for value in row {
                        self.validate_literal_expr(value)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn analyze_update(&mut self, stmt: &UpdateStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema().clone();

        // Build scope with the target table
        self.scopes.push();
        let entry = ScopeEntry {
            ref_name: stmt.table.clone(),
            scope_index: 0,
            schema,
            table_id: Some(relation.object_id()),
            cte_idx: None,
        };
        if let Some(scope) = self.scopes.current_mut() {
            scope.add_entry(entry)?;
        }

        // Validate SET clauses
        for set_clause in &stmt.set_clauses {
            // Verify column exists
            self.scopes.resolve_column(&set_clause.column, None)?;

            // Validate the value expression
            self.validate_expr(&set_clause.value)?;
        }

        // Validate WHERE clause
        if let Some(where_clause) = &stmt.where_clause {
            self.validate_expr(where_clause)?;
        }

        self.scopes.pop();
        Ok(())
    }

    fn analyze_delete(&mut self, stmt: &DeleteStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema().clone();

        // Build scope with the target table
        self.scopes.push();
        let entry = ScopeEntry {
            ref_name: stmt.table.clone(),
            scope_index: 0,
            schema,
            table_id: Some(relation.object_id()),
            cte_idx: None,
        };
        if let Some(scope) = self.scopes.current_mut() {
            scope.add_entry(entry)?;
        }

        // Validate WHERE clause
        if let Some(where_clause) = &stmt.where_clause {
            self.validate_expr(where_clause)?;
        }

        self.scopes.pop();
        Ok(())
    }

    fn analyze_create_table(&mut self, stmt: &CreateTableStatement) -> AnalyzerResult<()> {
        // Check table doesn't already exist
        if self.check_relation_exists(&stmt.table).is_ok() {
            return Err(AnalyzerError::AlreadyExists(DatabaseItem::Table(
                stmt.table.clone(),
            )));
        }

        // Check for duplicate column names
        let mut column_names = HashSet::new();
        let mut has_primary_key = false;

        for col in &stmt.columns {
            if !column_names.insert(&col.name) {
                return Err(AnalyzerError::Scope(ScopeError::AmbiguousColumn(
                    col.name.to_string(),
                )));
            }

            // Validate default expression if present
            if let Some(default_expr) = &col.default {
                self.validate_literal_expr(default_expr)?;
            }
        }

        // Validate table-level constraints
        for constraint in &stmt.constraints {
            match constraint {
                TableConstraintExpr::PrimaryKey(cols) => {
                    if has_primary_key {
                        return Err(AnalyzerError::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;

                    // Verify all columns exist
                    for col_name in cols {
                        if !column_names.contains(col_name) {
                            return Err(AnalyzerError::NotFound(DatabaseItem::Column(
                                stmt.table.clone(),
                                col_name.clone(),
                            )));
                        }
                    }
                }
                TableConstraintExpr::Unique(cols) => {
                    for col_name in cols {
                        if !column_names.contains(col_name) {
                            return Err(AnalyzerError::NotFound(DatabaseItem::Column(
                                stmt.table.clone(),
                                col_name.clone(),
                            )));
                        }
                    }
                }
                TableConstraintExpr::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    // Verify local columns exist
                    for col_name in columns {
                        if !column_names.contains(col_name) {
                            return Err(AnalyzerError::NotFound(DatabaseItem::Column(
                                stmt.table.clone(),
                                col_name.clone(),
                            )));
                        }
                    }

                    // Verify referenced table and columns exist
                    let ref_relation = self.get_relation(ref_table)?;
                    let ref_schema = ref_relation.schema();
                    for ref_col in ref_columns {
                        Self::check_column_exists(ref_col, ref_table, ref_schema)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Runs the analysis over an [ALTER TABLE] type statement
    fn analyze_alter_table(&mut self, stmt: &AlterTableStatement) -> AnalyzerResult<()> {
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema();

        match &stmt.action {
            AlterAction::AddColumn(col_def) => {
                // Check column doesn't already exist
                if Self::check_column_exists(&col_def.name, &stmt.table, schema).is_ok() {
                    return Err(AnalyzerError::AlreadyExists(DatabaseItem::Column(
                        stmt.table.to_string(),
                        col_def.name.to_string(),
                    )));
                }

                // Validate default expression
                if let Some(default_expr) = &col_def.default {
                    self.validate_literal_expr(default_expr)?;
                }
            }
            AlterAction::DropColumn(col_name) => {
                Self::check_column_exists(&col_name, &stmt.table, schema)?;
            }
            AlterAction::AlterColumn(alter_col) => {
                Self::check_column_exists(&alter_col.name, &stmt.table, schema)?;

                // Validate SET DEFAULT expression
                if let AlterColumnAction::SetDefault(expr) = &alter_col.action {
                    self.validate_literal_expr(expr)?;
                }
            }
            AlterAction::AddConstraint(constraint) => match constraint {
                TableConstraintExpr::PrimaryKey(cols) => {
                    for col_name in cols {
                        Self::check_column_exists(col_name, &stmt.table, schema)?;
                    }
                }
                TableConstraintExpr::Unique(cols) => {
                    for col_name in cols {
                        Self::check_column_exists(col_name, &stmt.table, schema)?;
                    }
                }
                TableConstraintExpr::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    for col_name in columns {
                        Self::check_column_exists(col_name, &stmt.table, schema)?;
                    }

                    let ref_relation = self.get_relation(ref_table)?;
                    let ref_schema = ref_relation.schema();
                    for ref_col in ref_columns {
                        Self::check_column_exists(ref_col, ref_table, ref_schema)?;
                    }
                }
            },
        }

        Ok(())
    }

    fn check_column_exists(
        column_name: &str,
        table_name: &str,
        schema: &Schema,
    ) -> AnalyzerResult<()> {
        if schema.bind_column(column_name).is_err() {
            return Err(AnalyzerError::NotFound(DatabaseItem::Column(
                table_name.to_string(),
                column_name.to_string(),
            )));
        }

        Ok(())
    }

    fn analyze_drop_table(&mut self, stmt: &DropTableStatement) -> AnalyzerResult<()> {
        if !stmt.if_exists {
            self.check_relation_exists(&stmt.table)?;
        }
        Ok(())
    }

    fn analyze_create_index(&mut self, stmt: &CreateIndexStatement) -> AnalyzerResult<()> {
        // Check index name doesn't already exist (indexes are stored as relations)
        if !stmt.if_not_exists && self.check_relation_exists(&stmt.name).is_ok() {
            return Err(AnalyzerError::AlreadyExists(DatabaseItem::Index(
                stmt.name.clone(),
            )));
        }

        // Verify table exists
        let relation = self.get_relation(&stmt.table)?;
        let schema = relation.schema();

        // Verify all indexed columns exist
        for idx_col in &stmt.columns {
            if schema.column_index.get(&idx_col.name).is_none() {
                return Err(AnalyzerError::NotFound(DatabaseItem::Column(
                    stmt.table.clone(),
                    idx_col.name.clone(),
                )));
            }
        }

        Ok(())
    }

    /// Validates expressions that don't require table scope (literals, simple expressions)
    fn validate_literal_expr(&self, expr: &Expr) -> AnalyzerResult<()> {
        match expr {
            Expr::Number(_) | Expr::String(_) | Expr::Boolean(_) | Expr::Null => Ok(()),
            Expr::UnaryOp { expr, .. } => self.validate_literal_expr(expr),
            Expr::BinaryOp { left, right, .. } => {
                self.validate_literal_expr(left)?;
                self.validate_literal_expr(right)
            }
            Expr::List(items) => {
                for item in items {
                    self.validate_literal_expr(item)?;
                }
                Ok(())
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.validate_literal_expr(arg)?;
                }
                Ok(())
            }
            // Column references not allowed in literal expressions
            Expr::Identifier(name) => Err(AnalyzerError::Other(format!(
                "Column reference '{}' not allowed in this context",
                name
            ))),
            Expr::QualifiedIdentifier { table, column } => Err(AnalyzerError::Other(format!(
                "Column reference '{}.{}' not allowed in this context",
                table, column
            ))),
            _ => Err(AnalyzerError::Other(
                "Complex expression not allowed in this context".to_string(),
            )),
        }
    }

    /// Counts the number of output columns in a SELECT statement
    fn count_select_columns(&self, stmt: &SelectStatement) -> usize {
        let mut count = 0;
        for item in &stmt.columns {
            match item {
                SelectItem::Star => {
                    // Would need to expand star - for now approximate
                    count += 1;
                }
                SelectItem::ExprWithAlias { .. } => {
                    count += 1;
                }
            }
        }
        count
    }
}
