use crate::database::schema::Relation;
use crate::database::schema::Table;
use crate::database::schema::{Column, Schema};
use crate::database::Database;
use crate::sql::ast::{
    AlterAction, AlterColumnAction, BinaryOperator, ColumnConstraintExpr, Expr, SelectItem,
    SelectStatement, Statement, TableConstraintExpr, TableReference, TransactionStatement, Values,
};

use crate::structures::bplustree::SearchResult;
use crate::types::DataTypeKind;
use regex::Regex;
use std::collections::HashSet;
use std::{collections::HashMap, fmt::Display};

// Common SQL aggregate functions
pub const AGGREGATE_FUNCTORS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
pub const STRING_FUNCTORS: [&str; 5] = ["LENGTH", "UPPER", "LOWER", "TRIM", "SUBSTR"];
pub const MATH_FUNCTORS: [&str; 4] = ["ABS", "ROUND", "CEIL", "FLOOR"];

fn is_date_iso(s: &str) -> bool {
    // Format: YYYY-MM-DD
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    re.is_match(s)
}

fn is_datetime_iso(s: &str) -> bool {
    // Format: YYYY-MM-DDTHH:MM:SS
    let re = Regex::new(r"(?i)^\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}$").unwrap();
    re.is_match(s)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AnalyzerError {
    ConstraintViolation(String, String),
    ColumnValueCountMismatch(usize),
    MissingColumns,
    NotFound(String),
    DuplicatedColumn(String),
    MultiplePrimaryKeys,
    AlreadyExists(AlreadyExists),
    ArithmeticOverflow(f64, DataTypeKind),
    InvalidFormat(String, String),
    InvalidExpression,
    AlreadyStarted,
    NotStarted,
    InvalidDataType(DataTypeKind),
    AliasNotProvided,
    DuplicatedConstraint(String),
    InvalidValue(DataTypeKind),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlreadyExists {
    Table(String),
    Index(String),
    Constraint(String),
    Column(String, String),
}

impl Display for AlreadyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Index(index) => write!(f, "index {index} already exists"),
            Self::Table(table) => write!(f, "table {table} already exists"),
            Self::Constraint(ct) => write!(f, "constraint {ct} already exists"),
            Self::Column(table, column) => {
                write!(f, "column {column} already exists on table {table}")
            }
        }
    }
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ColumnValueCountMismatch(row) => write!(f,"number of columns doesn't match values on row {row}"),
            Self::NotFound(obj) => write!(f, "Object not found in the database {obj}"),
            Self::MultiplePrimaryKeys => f.write_str("only one primary key per table is allowed"),
            Self::MissingColumns => {
                f.write_str("default values are not supported, all columns must be specified")
            }
            Self::DuplicatedColumn(col) => write!(f, "column '{col}' specified more than once"),
            Self::AlreadyExists(already_exists) => write!(f, "{already_exists}"),

            Self::ArithmeticOverflow(num, data_type) => {
                write!(f, "integer {num} out of range for data type {data_type}")
            }
            Self::ConstraintViolation(ct, column) => {
                write!(f, "constraint {ct} violated for column {column}")
            }
            Self::InvalidExpression => {
                f.write_str( "Invalid expression found!")
            }
            Self::AlreadyStarted => {
                f.write_str("There is already an opened transaction on this session. Cannot start another transaction on the same session until the current one finishes")
            },
            Self::NotStarted => {
                f.write_str("Transaction has not started. Must be on a transaction in order to call [ROLLBACK] or [COMMIT].")
            },
            Self::InvalidDataType(dtype) => {
                write!(f, "Invalid datatype for expression: {dtype}")
            },
            Self::InvalidValue(dtype) => {
                write!(f, "Invalid value for datatype: {dtype}")
            },
            Self::InvalidFormat(s, o) => {
                write!(f, "Invalid formatting for string: {s}, expected: {o}")
            },
            Self::AliasNotProvided => {
                write!(f, "An alias is required in order to reference columns when referencing multiple tables in the same statement.")
            },
            Self::DuplicatedConstraint(s) => {
                write!(f, "Duplicated constraint on column {s}")
            }
        }
    }
}

struct AnalyzerCtx {
    table_aliases: HashMap<String, String>,
    subqueries: HashMap<String, Relation>,
    current_table: Option<String>,
    on_transaction: bool,
}

impl AnalyzerCtx {
    fn new() -> Self {
        Self {
            table_aliases: HashMap::new(),
            subqueries: HashMap::new(),
            on_transaction: false,
            current_table: None,
        }
    }

    fn clear_query_context(&mut self) {
        self.table_aliases.clear();
        self.subqueries.clear();
        self.current_table = None;
    }

    fn save_context(
        &self,
    ) -> (
        HashMap<String, String>,
        HashMap<String, Relation>,
        Option<String>,
    ) {
        (
            self.table_aliases.clone(),
            self.subqueries.clone(),
            self.current_table.clone(),
        )
    }

    fn restore_context(
        &mut self,
        context: (
            HashMap<String, String>,
            HashMap<String, Relation>,
            Option<String>,
        ),
    ) {
        self.table_aliases = context.0;
        self.subqueries = context.1;
        self.current_table = context.2;
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ExprResult {
    Table(Vec<DataTypeKind>),
    Value(DataTypeKind),
}

pub(crate) struct Analyzer<'a> {
    db: &'a Database,
    ctx: AnalyzerCtx,
}

impl<'a> Analyzer<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self {
            db,
            ctx: AnalyzerCtx::new(),
        }
    }

    fn table_aliases(&self) -> &HashMap<String, String> {
        &self.ctx.table_aliases
    }

    fn analyze_identifier(&mut self, ident: &str) -> Result<DataTypeKind, AnalyzerError> {
        // Try to find the column in current table first
        if let Some(current) = &self.ctx.current_table {
            let obj = self
                .get_relation(current)
                .map_err(|e| AnalyzerError::NotFound(current.to_string()))?;
            let schema = obj.schema();
            if let Some(column) = schema.column(ident) {
                return Ok(column.dtype);
            }
        }

        // If not found and we have multiple tables, search in all of them
        if self.ctx.table_aliases.len() > 1 {
            for table in self.ctx.table_aliases.values() {
                let obj = self.get_relation(table)?;
                let schema = obj.schema();
                if let Some(column) = schema.column(ident) {
                    return Ok(column.dtype);
                }
            }

            // Check subqueries too
            for subquery in self.ctx.subqueries.values() {
                let schema = subquery.schema();
                if let Some(column) = schema.column(ident) {
                    return Ok(column.dtype);
                }
            }

            Err(AnalyzerError::NotFound(ident.to_string()))
        } else if self.ctx.current_table.is_none() && self.ctx.table_aliases.is_empty() {
            Err(AnalyzerError::AliasNotProvided)
        } else {
            Err(AnalyzerError::NotFound(ident.to_string()))
        }
    }

    fn analyze_func(
        &mut self,
        name: &str,
        args_dtypes: Vec<DataTypeKind>,
    ) -> Result<ExprResult, AnalyzerError> {
        let name_upper = name.to_uppercase();

        if AGGREGATE_FUNCTORS.contains(&name_upper.as_str()) {
            match name_upper.as_str() {
                "COUNT" => Ok(ExprResult::Value(DataTypeKind::BigInt)),
                "SUM" | "AVG" => {
                    if args_dtypes.is_empty() || !args_dtypes[0].is_numeric() {
                        Ok(ExprResult::Value(DataTypeKind::Double))
                    } else {
                        Ok(ExprResult::Value(args_dtypes[0]))
                    }
                }
                "MIN" | "MAX" => {
                    if args_dtypes.is_empty() {
                        Ok(ExprResult::Value(DataTypeKind::Null))
                    } else {
                        Ok(ExprResult::Value(args_dtypes[0]))
                    }
                }
                _ => Ok(ExprResult::Value(DataTypeKind::Null)),
            }
        } else if STRING_FUNCTORS.contains(&name_upper.as_str()) {
            match name_upper.as_str() {
                "LENGTH" => Ok(ExprResult::Value(DataTypeKind::Int)),
                "UPPER" | "LOWER" | "TRIM" | "SUBSTR" => Ok(ExprResult::Value(DataTypeKind::Text)),
                _ => Ok(ExprResult::Value(DataTypeKind::Text)),
            }
        } else if MATH_FUNCTORS.contains(&name_upper.as_str()) {
            if args_dtypes.is_empty() {
                Ok(ExprResult::Value(DataTypeKind::Double))
            } else {
                Ok(ExprResult::Value(args_dtypes[0]))
            }
        } else {
            // Unknown function, return NULL for now
            Ok(ExprResult::Value(DataTypeKind::Null))
        }
    }

    fn analyze_expr(&mut self, expr: &Expr) -> Result<ExprResult, AnalyzerError> {
        match expr {
            Expr::QualifiedIdentifier { table, column } => {
                // First check if it's an alias
                let real_table = self
                    .ctx
                    .table_aliases
                    .get(table)
                    .or_else(|| self.ctx.subqueries.get(table).map(|_| table))
                    .ok_or_else(|| AnalyzerError::NotFound(table.clone()))?;

                // Check in subqueries first
                if let Some(subquery) = self.ctx.subqueries.get(table) {
                    let schema = subquery.schema();
                    if let Some(column) = schema.column(column) {
                        return Ok(ExprResult::Value(column.dtype));
                    }
                }

                // Then check in regular tables
                let obj = self
                    .get_relation(real_table)
                    .map_err(|e| AnalyzerError::NotFound(real_table.to_string()))?;
                let schema = obj.schema();

                schema
                    .column(column)
                    .map(|c| ExprResult::Value(c.dtype))
                    .ok_or_else(|| AnalyzerError::NotFound(format!("{table}.{column}")))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_result = self.analyze_expr(left)?;

                if matches!(
                    right.as_ref(),
                    &Expr::Number(_) | &Expr::String(_) | &Expr::Boolean(_)
                ) {
                    if let ExprResult::Value(dt) = left_result {
                        self.analyze_value(dt, right)?;
                    }
                };

                let right_result = self.analyze_expr(right)?;

                match (left_result, right_result) {
                    (ExprResult::Value(left_dt), ExprResult::Value(right_dt)) => {
                        // Special handling for NULL
                        if matches!(left_dt, DataTypeKind::Null) {
                            return Ok(ExprResult::Value(right_dt));
                        }
                        if matches!(right_dt, DataTypeKind::Null) {
                            return Ok(ExprResult::Value(left_dt));
                        }

                        // Type compatibility check
                        if !left_dt.can_be_coerced(right_dt) && !right_dt.can_be_coerced(left_dt) {
                            return Err(AnalyzerError::InvalidDataType(right_dt));
                        }

                        // Arithmetic operators
                        if matches!(
                            op,
                            BinaryOperator::Plus
                                | BinaryOperator::Minus
                                | BinaryOperator::Multiply
                                | BinaryOperator::Divide
                                | BinaryOperator::Modulo
                        ) {
                            if !left_dt.is_numeric() {
                                return Err(AnalyzerError::InvalidDataType(left_dt));
                            }
                            if !right_dt.is_numeric() {
                                return Err(AnalyzerError::InvalidDataType(right_dt));
                            }

                            // Return the wider type for arithmetic
                            let result_type = match (left_dt, right_dt) {
                                (DataTypeKind::Double, _) | (_, DataTypeKind::Double) => {
                                    DataTypeKind::Double
                                }
                                (DataTypeKind::Float, _) | (_, DataTypeKind::Float) => {
                                    DataTypeKind::Float
                                }
                                (DataTypeKind::BigInt, _) | (_, DataTypeKind::BigInt) => {
                                    DataTypeKind::BigInt
                                }
                                _ => left_dt,
                            };
                            Ok(ExprResult::Value(result_type))
                        } else {
                            // Comparison operators return boolean
                            Ok(ExprResult::Value(DataTypeKind::Boolean))
                        }
                    }
                    (ExprResult::Value(left_dt), ExprResult::Table(right_dts)) => {
                        // For IN operator with list
                        if right_dts.is_empty() {
                            return Ok(ExprResult::Value(DataTypeKind::Boolean));
                        }

                        // All elements in the list should be compatible
                        for dtype in &right_dts {
                            if !left_dt.can_be_coerced(*dtype) && !dtype.can_be_coerced(left_dt) {
                                return Err(AnalyzerError::InvalidDataType(*dtype));
                            }
                        }

                        Ok(ExprResult::Value(DataTypeKind::Boolean))
                    }
                    _ => Err(AnalyzerError::InvalidExpression),
                }
            }
            Expr::Identifier(str) => {
                let datatype = self.analyze_identifier(str)?;
                Ok(ExprResult::Value(datatype))
            }
            Expr::Exists(stmt) => {
                // Save and restore context for subquery
                let saved_context = self.ctx.save_context();
                self.analyze_select(stmt.as_ref())?;
                self.ctx.restore_context(saved_context);
                Ok(ExprResult::Value(DataTypeKind::Boolean))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct: _,
            } => {
                let mut args_dtypes = Vec::with_capacity(args.len());
                for arg in args {
                    if let ExprResult::Value(item) = self.analyze_expr(arg)? {
                        args_dtypes.push(item);
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }
                self.analyze_func(name, args_dtypes)
            }
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                if let ExprResult::Value(expr_dtype) = self.analyze_expr(expr)? {
                    if let ExprResult::Value(left_dtype) = self.analyze_expr(low)? {
                        if !expr_dtype.can_be_coerced(left_dtype)
                            && !left_dtype.can_be_coerced(expr_dtype)
                            && !matches!(left_dtype, DataTypeKind::Null)
                        {
                            return Err(AnalyzerError::InvalidDataType(left_dtype));
                        }

                        if let ExprResult::Value(right_dtype) = self.analyze_expr(high)? {
                            if !expr_dtype.can_be_coerced(right_dtype)
                                && !right_dtype.can_be_coerced(expr_dtype)
                                && !matches!(right_dtype, DataTypeKind::Null)
                            {
                                return Err(AnalyzerError::InvalidDataType(right_dtype));
                            }

                            return Ok(ExprResult::Value(DataTypeKind::Boolean));
                        }
                    }
                }
                Err(AnalyzerError::InvalidExpression)
            }
            Expr::List(items) => {
                let mut dtypes = Vec::new();
                for item in items {
                    match self.analyze_expr(item)? {
                        ExprResult::Table(values) => dtypes.extend(values),
                        ExprResult::Value(value) => dtypes.push(value),
                    }
                }
                Ok(ExprResult::Table(dtypes))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut out_dtypes = Vec::new();

                let op_dtype = if let Some(expr) = operand {
                    if let ExprResult::Value(item) = self.analyze_expr(expr)? {
                        Some(item)
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                } else {
                    None
                };

                for item in when_clauses {
                    if let ExprResult::Value(cond_dtype) = self.analyze_expr(&item.condition)? {
                        if let Some(dtype) = op_dtype {
                            if !dtype.can_be_coerced(cond_dtype)
                                && !cond_dtype.can_be_coerced(dtype)
                                && !matches!(cond_dtype, DataTypeKind::Null)
                            {
                                return Err(AnalyzerError::InvalidDataType(cond_dtype));
                            };
                        } else {
                            // Simple WHEN without operand should be boolean
                            if !matches!(cond_dtype, DataTypeKind::Boolean | DataTypeKind::Null) {
                                return Err(AnalyzerError::InvalidDataType(cond_dtype));
                            }
                        }
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    };

                    if let ExprResult::Value(out_dtype) = self.analyze_expr(&item.result)? {
                        out_dtypes.push(out_dtype);
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }

                if let Some(expr) = else_clause {
                    if let ExprResult::Value(else_dtype) = self.analyze_expr(expr)? {
                        out_dtypes.push(else_dtype);
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }

                // Find the common type that all branches can be coerced to
                if out_dtypes.is_empty() {
                    return Ok(ExprResult::Value(DataTypeKind::Null));
                }

                let mut result_type = out_dtypes[0];
                for dtype in &out_dtypes[1..] {
                    if matches!(*dtype, DataTypeKind::Null) {
                        continue;
                    }
                    if matches!(result_type, DataTypeKind::Null) {
                        result_type = *dtype;
                        continue;
                    }
                    if !result_type.can_be_coerced(*dtype) && !dtype.can_be_coerced(result_type) {
                        return Err(AnalyzerError::InvalidDataType(*dtype));
                    }
                    // Choose the wider type
                    if matches!(*dtype, DataTypeKind::Double) {
                        result_type = DataTypeKind::Double;
                    } else if matches!(*dtype, DataTypeKind::Float)
                        && !matches!(result_type, DataTypeKind::Double)
                    {
                        result_type = DataTypeKind::Float;
                    }
                }

                Ok(ExprResult::Value(result_type))
            }
            Expr::UnaryOp { op: _, expr } => self.analyze_expr(expr),
            Expr::Null => Ok(ExprResult::Value(DataTypeKind::Null)),
            Expr::Star => {
                let mut out = Vec::new();

                // Collect columns from all tables
                let mut processed = HashSet::new();
                for (alias, table) in self.ctx.table_aliases.iter() {
                    if processed.insert(table.clone()) {
                        let obj = self
                            .get_relation(table)
                            .map_err(|e| AnalyzerError::NotFound(table.to_string()))?;
                        let schema = obj.schema();
                        let types: Vec<DataTypeKind> =
                            schema.columns.iter().map(|c| c.dtype).collect();
                        out.extend(types);
                    }
                }

                // Add columns from subqueries
                for subq in self.ctx.subqueries.values() {
                    let schema = subq.schema();
                    let types: Vec<DataTypeKind> = schema.columns.iter().map(|c| c.dtype).collect();
                    out.extend(types);
                }

                if out.is_empty() {
                    return Err(AnalyzerError::InvalidExpression);
                }

                Ok(ExprResult::Table(out))
            }
            Expr::Number(_) => Ok(ExprResult::Value(DataTypeKind::Double)),
            Expr::String(_) => Ok(ExprResult::Value(DataTypeKind::Text)),
            Expr::Boolean(_) => Ok(ExprResult::Value(DataTypeKind::Boolean)),
            Expr::Subquery(item) => {
                // Save and restore context for subquery
                let saved_context = self.ctx.save_context();
                let schema = self.analyze_select(item)?;
                self.ctx.restore_context(saved_context);

                Ok(ExprResult::Table(
                    schema.columns.iter().map(|d| d.dtype).collect(),
                ))
            }
        }
    }

    fn analyze_table_ref(&mut self, reference: &TableReference) -> Result<(), AnalyzerError> {
        match reference {
            TableReference::Table { name, alias } => {

                let obj = self
                    .get_relation(name)
                    .map_err(|e| AnalyzerError::NotFound(name.to_string()))?;
                self.ctx.current_table = Some(name.to_string());

                if let Some(aliased) = alias {
                    self.ctx.table_aliases.insert(aliased.clone(), name.clone());
                } else {
                    self.ctx.table_aliases.insert(name.clone(), name.clone());
                };
            }
            TableReference::Join {
                left, right, on, ..
            } => {
                self.analyze_table_ref(left)?;
                self.analyze_table_ref(right)?;
                self.ctx.current_table = None;

                if let Some(on_expr) = on {
                    if let ExprResult::Value(dtype) = self.analyze_expr(on_expr)? {
                        if !matches!(dtype, DataTypeKind::Boolean | DataTypeKind::Null) {
                            return Err(AnalyzerError::InvalidExpression);
                        }
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }
            }
            TableReference::Subquery { query, alias } => {
                // Save current context
                let saved_context = self.ctx.save_context();

                // Analyze subquery in clean context
                self.ctx.clear_query_context();
                let schema = self.analyze_select(query.as_ref())?;

                // Restore context
                self.ctx.restore_context(saved_context);

                // Add subquery to context
                let obj = Relation::TableRel(Table::new(alias, crate::types::PAGE_ZERO, schema));

                self.ctx.subqueries.insert(alias.to_string(), obj);
                self.ctx.table_aliases.insert(alias.clone(), alias.clone());
            }
        };
        Ok(())
    }

    fn analyze_select(&mut self, stmt: &SelectStatement) -> Result<Schema, AnalyzerError> {
        let mut out_columns = Vec::with_capacity(stmt.columns.len());

        if let Some(table_ref) = &stmt.from {
            self.analyze_table_ref(table_ref)?;
        }

        for item in stmt.columns.iter() {
            match item {
                SelectItem::Star => {
                    // Collect all columns maintaining order
                    let mut processed_tables = HashSet::new();

                    // Process main table first if exists
                    if let Some(current) = &self.ctx.current_table {
                        if processed_tables.insert(current.clone()) {
                            let obj = self
                                .get_relation(current)
                                .map_err(|e| AnalyzerError::NotFound(current.to_string()))?;
                            let schema = obj.schema();
                            out_columns.extend(schema.columns.clone());
                        }
                    }

                    // Process aliased tables
                    for (alias, table) in self.ctx.table_aliases.iter() {
                        if processed_tables.insert(table.clone()) {
                            let obj = self
                                .get_relation(table)
                                .map_err(|e| AnalyzerError::NotFound(table.to_string()))?;
                            let schema = obj.schema();
                            out_columns.extend(schema.columns.clone());
                        }
                    }

                    // Process subqueries
                    for subq in self.ctx.subqueries.values() {
                        let schema = subq.schema();
                        out_columns.extend(schema.columns.clone());
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let cname = if let Some(aliased) = alias {
                        aliased.clone()
                    } else if let Expr::Identifier(value) = expr {
                        value.clone()
                    } else if let Expr::QualifiedIdentifier { table: _, column } = expr {
                        column.clone()
                    } else {
                        "?".to_string()
                    };

                    let result = self.analyze_expr(expr)?;
                    match result {
                        ExprResult::Value(dtype) => {
                            let column = Column::new_unindexed(dtype, &cname, None);
                            out_columns.push(column);
                        }
                        ExprResult::Table(dtypes) => {
                            // For subqueries returning multiple columns
                            if dtypes.len() == 1 {
                                let column = Column::new_unindexed(dtypes[0], &cname, None);
                                out_columns.push(column);
                            } else {
                                return Err(AnalyzerError::InvalidExpression);
                            }
                        }
                    }
                }
            }
        }

        // Validate WHERE clause
        if let Some(clause) = &stmt.where_clause {
            if let ExprResult::Value(dt) = self.analyze_expr(clause)? {
                if !matches!(dt, DataTypeKind::Boolean | DataTypeKind::Null) {
                    return Err(AnalyzerError::InvalidExpression);
                }
            } else {
                return Err(AnalyzerError::InvalidExpression);
            }
        }

        // Validate ORDER BY
        for order_by in stmt.order_by.iter() {
            self.analyze_expr(&order_by.expr)?;
        }

        // Validate GROUP BY
        for group_by in stmt.group_by.iter() {
            self.analyze_expr(group_by)?;
        }

        Ok(Schema::from_columns(out_columns.as_slice(), 0))
    }

    pub fn analyze(&mut self, stmt: &Statement) -> Result<(), AnalyzerError> {
        // Clear query context for each new statement
        self.ctx.clear_query_context();

        match stmt {
            Statement::Transaction(t) => match t {
                TransactionStatement::Begin => {
                    if self.ctx.on_transaction {
                        return Err(AnalyzerError::AlreadyStarted);
                    };
                    self.ctx.on_transaction = true;
                }
                TransactionStatement::Commit | TransactionStatement::Rollback => {
                    if !self.ctx.on_transaction {
                        return Err(AnalyzerError::NotStarted);
                    };
                    self.ctx.on_transaction = false;
                }
            },
            Statement::Select(s) => {
                self.analyze_select(s)?;
            }
            Statement::With(s) => {
                // Analyze CTEs
                for (alias, item) in s.ctes.iter() {
                    // Save context before analyzing CTE
                    let saved_context = self.ctx.save_context();
                    self.ctx.clear_query_context();

                    let schema = self.analyze_select(item)?;

                    // Restore context and add CTE
                    self.ctx.restore_context(saved_context);
                    let obj =
                        Relation::TableRel(Table::new(alias, crate::types::PAGE_ZERO, schema));
                    self.ctx.subqueries.insert(alias.to_string(), obj);
                }

                // Analyze main query with CTEs available
                self.analyze_select(&s.body)?;
            }
            Statement::Insert(s) => {
                let obj = self
                    .get_relation(&s.table)
                    .map_err(|e| AnalyzerError::NotFound(s.table.to_string()))?;
                let schema = obj.schema();

                let columns: Vec<&Column> = if let Some(cols) = &s.columns {
                    cols.iter()
                        .map(|c| {
                            schema
                                .column(c)
                                .ok_or_else(|| AnalyzerError::NotFound(c.clone()))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    schema.columns.iter().collect::<Vec<_>>()
                };

                match &s.values {
                    Values::Query(select_expr) => {
                        let select_schema = self.analyze_select(select_expr)?;
                        if columns.len() != select_schema.columns.len() {
                            return Err(AnalyzerError::ColumnValueCountMismatch(0));
                        };

                        for (i, column) in select_schema.columns.iter().enumerate() {
                            if columns[i].is_non_null()
                                && matches!(column.dtype, DataTypeKind::Null)
                            {
                                return Err(AnalyzerError::ConstraintViolation(
                                    "Non null constraint".to_string(),
                                    columns[i].name.clone(),
                                ));
                            }

                            if !column.dtype.can_be_coerced(columns[i].dtype)
                                && !columns[i].dtype.can_be_coerced(column.dtype)
                                && !matches!(column.dtype, DataTypeKind::Null)
                            {
                                return Err(AnalyzerError::InvalidDataType(column.dtype));
                            }
                        }
                    }
                    Values::Values(expr_list) => {
                        for (i, row) in expr_list.iter().enumerate() {
                            if columns.len() != row.len() {
                                return Err(AnalyzerError::ColumnValueCountMismatch(i));
                            };

                            for (j, value) in row.iter().enumerate() {
                                if columns[j].is_non_null() && matches!(value, Expr::Null) {
                                    return Err(AnalyzerError::ConstraintViolation(
                                        "Non null constraint".to_string(),
                                        columns[j].name.to_owned(),
                                    ));
                                };

                                self.analyze_value(columns[j].dtype, value)?;
                            }
                        }
                    }
                }
            }
            Statement::CreateTable(stmt) => {
                if let Ok(SearchResult::Found(_)) = self.db.lookup(&stmt.table) {
                    return Err(AnalyzerError::AlreadyExists(AlreadyExists::Table(
                        stmt.table.clone(),
                    )));
                }

                let mut has_primary = false;
                let mut column_set = HashSet::with_capacity(stmt.columns.len());

                for col in stmt.columns.iter() {
                    let mut has_default = false;
                    let mut has_not_null = false;
                    let mut has_unique = false;

                    if column_set.contains(&col.name) {
                        return Err(AnalyzerError::DuplicatedColumn(col.name.to_string()));
                    } else {
                        column_set.insert(col.name.to_string());
                    };

                    for constraint in col.constraints.iter() {
                        match constraint {
                            ColumnConstraintExpr::Default(_) => {
                                if has_default {
                                    return Err(AnalyzerError::DuplicatedConstraint(
                                        col.name.to_string(),
                                    ));
                                } else {
                                    has_default = true;
                                }
                            }
                            ColumnConstraintExpr::NotNull => {
                                if has_not_null {
                                    return Err(AnalyzerError::DuplicatedConstraint(
                                        col.name.to_string(),
                                    ));
                                } else {
                                    has_not_null = true;
                                }
                            }
                            ColumnConstraintExpr::Unique => {
                                if has_unique {
                                    return Err(AnalyzerError::DuplicatedConstraint(
                                        col.name.to_string(),
                                    ));
                                } else {
                                    has_unique = true;
                                }
                            }
                            ColumnConstraintExpr::PrimaryKey => {
                                if has_primary {
                                    return Err(AnalyzerError::MultiplePrimaryKeys);
                                } else {
                                    has_primary = true;
                                }
                            }
                            ColumnConstraintExpr::ForeignKey { table, column } => {
                                let other_obj = self
                                    .get_relation(table)
                                    .map_err(|e| AnalyzerError::NotFound(table.to_string()))?;
                                if let Some(schema_col) = other_obj.schema().column(column) {
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
                            };

                            if has_primary {
                                return Err(AnalyzerError::MultiplePrimaryKeys);
                            } else {
                                has_primary = true;
                            }
                        }
                        TableConstraintExpr::Unique(columns) => {
                            if !columns.iter().all(|c| column_set.contains(c)) {
                                return Err(AnalyzerError::MissingColumns);
                            };
                        }
                        TableConstraintExpr::ForeignKey {
                            columns,
                            ref_table,
                            ref_columns,
                        } => {
                            if !columns.iter().all(|c| column_set.contains(c)) {
                                return Err(AnalyzerError::MissingColumns);
                            };

                            let other_obj = self
                                .get_relation(ref_table)
                                .map_err(|e| AnalyzerError::NotFound(ref_table.to_string()))?;

                            let other_set: HashSet<String> = other_obj
                                .columns()
                                .iter()
                                .map(|c| c.name().to_string())
                                .collect();

                            if ref_columns.iter().any(|c| !other_set.contains(c)) {
                                return Err(AnalyzerError::MissingColumns);
                            };

                            // TODO: MUST VERIFY TYPE COMPATIBILTY BETWEEN COLUMNS
                        }
                        _ => {}
                    }
                }
            }
            Statement::AlterTable(stmt) => {
                let obj = self
                    .get_relation(&stmt.table)
                    .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                let obj_set: HashSet<String> =
                    obj.columns().iter().map(|c| c.name().to_string()).collect();

                match &stmt.action {
                    AlterAction::AddColumn(col) => {
                        if obj_set.contains(&col.name) {
                            return Err(AnalyzerError::AlreadyExists(AlreadyExists::Column(
                                obj.name().to_string(),
                                col.name.to_string(),
                            )));
                        }
                    }
                    AlterAction::AlterColumn(alter_col) => {
                        if !obj_set.contains(&alter_col.name) {
                            return Err(AnalyzerError::MissingColumns);
                        };

                        let schema = obj.schema();

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
                            };
                            let other_obj = self
                                .get_relation(ref_table)
                                .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                            let other_set: HashSet<String> = other_obj
                                .columns()
                                .iter()
                                .map(|c| c.name().to_string())
                                .collect();
                            if ref_columns.iter().any(|c| !other_set.contains(c)) {
                                return Err(AnalyzerError::MissingColumns);
                            };
                        }
                        TableConstraintExpr::PrimaryKey(cols) => {
                            let schema = obj.schema();
                            if schema.has_pk() {
                                return Err(AnalyzerError::MultiplePrimaryKeys);
                            };
                            if cols.iter().any(|c| !obj_set.contains(c)) {
                                return Err(AnalyzerError::MissingColumns);
                            };
                        }
                        TableConstraintExpr::Unique(cols) => {}
                        _ => {}
                    },
                    AlterAction::DropColumn(col) => {
                        if !obj_set.contains(col) {
                            return Err(AnalyzerError::MissingColumns);
                        };
                    }
                    AlterAction::DropConstraint(name) => {
                        let schema = obj.schema();
                        // Note: The original logic seems reversed - should check if constraint exists
                        if !schema.has_constraint(name) {
                            return Err(AnalyzerError::NotFound(name.to_string()));
                        }
                    }
                }
            }
            Statement::Delete(stmt) => {
                self.get_relation(&stmt.table)
                    .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                self.ctx
                    .table_aliases
                    .insert(stmt.table.to_string(), stmt.table.to_string());
                self.ctx.current_table = Some(stmt.table.to_string());

                if let Some(clause) = &stmt.where_clause {
                    self.analyze_expr(clause)?;
                }
            }
            Statement::Update(stmt) => {
                let obj = self
                    .get_relation(&stmt.table)
                    .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                self.ctx
                    .table_aliases
                    .insert(stmt.table.to_string(), stmt.table.to_string());
                self.ctx.current_table = Some(stmt.table.to_string());
                let schema = obj.schema();

                if let Some(clause) = &stmt.where_clause {
                    self.analyze_expr(clause)?;
                }

                for column in stmt.set_clauses.iter() {
                    let col = schema
                        .column(&column.column)
                        .ok_or_else(|| AnalyzerError::NotFound(column.column.to_string()))?;

                    if matches!(column.value, Expr::Null) && col.is_non_null() {
                        return Err(AnalyzerError::ConstraintViolation(
                            "Non null constraint".to_string(),
                            col.name.to_string(),
                        ));
                    }

                    // For expressions, analyze them in context
                    match &column.value {
                        Expr::Identifier(_)
                        | Expr::QualifiedIdentifier { .. }
                        | Expr::BinaryOp { .. }
                        | Expr::FunctionCall { .. } => {
                            let result = self.analyze_expr(&column.value)?;
                            if let ExprResult::Value(value_type) = result {
                                if !value_type.can_be_coerced(col.dtype)
                                    && !col.dtype.can_be_coerced(value_type)
                                    && !matches!(value_type, DataTypeKind::Null)
                                {
                                    return Err(AnalyzerError::InvalidDataType(value_type));
                                }
                            }
                        }
                        _ => {
                            self.analyze_value(col.dtype, &column.value)?;
                        }
                    }
                }
            }
            Statement::DropTable(stmt) => {
                if !stmt.if_exists {
                    self.get_relation(&stmt.table)
                        .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                };
            }
            Statement::CreateIndex(stmt) => {
                if self.get_relation(&stmt.name).is_ok() && !stmt.if_not_exists {
                    return Err(AnalyzerError::AlreadyExists(AlreadyExists::Index(
                        stmt.name.clone(),
                    )));
                }
                let obj = self
                    .get_relation(&stmt.table)
                    .map_err(|e| AnalyzerError::NotFound(stmt.table.to_string()))?;
                let obj_set: HashSet<String> =
                    obj.columns().iter().map(|c| c.name().to_string()).collect();
                let columns: Vec<String> =
                    stmt.columns.iter().map(|c| c.name.to_string()).collect();

                if columns.iter().any(|c| !obj_set.contains(c)) {
                    return Err(AnalyzerError::MissingColumns);
                };
            }
        }
        Ok(())
    }

    fn analyze_value(
        &self,
        dtype: DataTypeKind,
        value: &Expr,
    ) -> Result<DataTypeKind, AnalyzerError> {
        match value {
            Expr::Number(f) => {
                // Check overflow for integer types
                match dtype {
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
                    _ => {} // Float, Double, etc. - no overflow check needed
                }

                // Return appropriate type based on target
                if dtype.is_numeric() {
                    Ok(dtype)
                } else {
                    Ok(DataTypeKind::Double)
                }
            }
            Expr::String(item) => {
                let len = item.len();
                let numeric = item.parse::<f64>().ok();

                match dtype {
                    DataTypeKind::Byte | DataTypeKind::SmallInt => {
                        if let Some(num) = numeric {
                            if num < i8::MIN as f64 || num > i8::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::SmallUInt => {
                        if let Some(num) = numeric {
                            if num < 0.0 || num > u8::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::HalfInt => {
                        if let Some(num) = numeric {
                            if num < i16::MIN as f64 || num > i16::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::HalfUInt => {
                        if let Some(num) = numeric {
                            if num < 0.0 || num > u16::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::Int => {
                        if let Some(num) = numeric {
                            if num < i32::MIN as f64 || num > i32::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::UInt => {
                        if let Some(num) = numeric {
                            if num < 0.0 || num > u32::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::BigInt => {
                        if let Some(num) = numeric {
                            if num < i64::MIN as f64 || num > i64::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::BigUInt => {
                        if let Some(num) = numeric {
                            if num < 0.0 || num > u64::MAX as f64 {
                                return Err(AnalyzerError::ArithmeticOverflow(num, dtype));
                            }
                        } else {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::Float | DataTypeKind::Double => {
                        if numeric.is_none() && dtype.is_numeric() {
                            return Err(AnalyzerError::InvalidValue(dtype));
                        }
                    }
                    DataTypeKind::Char => {
                        if len > 1 {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Char));
                        };
                    }
                    DataTypeKind::Boolean => {
                        let upper = item.to_uppercase();
                        if upper != "TRUE" && upper != "FALSE" && upper != "1" && upper != "0" {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Boolean));
                        };
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
                    _ => {} // Text, Blob, etc.
                }

                // Return appropriate type
                if dtype.is_numeric() && numeric.is_some() {
                    Ok(dtype)
                } else {
                    Ok(DataTypeKind::Text)
                }
            }
            Expr::Boolean(_) => {
                if !matches!(dtype, DataTypeKind::Boolean) && dtype.is_numeric() {
                    // Boolean can be coerced to numeric (0 or 1)
                    Ok(dtype)
                } else if !matches!(dtype, DataTypeKind::Boolean) {
                    Err(AnalyzerError::InvalidValue(dtype))
                } else {
                    Ok(DataTypeKind::Boolean)
                }
            }
            Expr::Null => Ok(DataTypeKind::Null),
            _ => {
                // For other expressions, we'd need to analyze them
                Ok(DataTypeKind::Null)
            }
        }
    }

    fn get_relation(&self, name: &str) -> Result<Relation, AnalyzerError> {
        if let Some(subquery) = self.ctx.subqueries.get(name) {
            return Ok(subquery.clone());
        };

        self.db
            .relation(name)
            .map_err(|e| AnalyzerError::NotFound(name.to_string()))
    }
}

#[cfg(test)]
mod sql_analyzer_tests {
    use crate::database::{schema::Schema, Database};
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::analyzer::{AlreadyExists, Analyzer, AnalyzerError};
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
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

    fn parse_and_analyze(sql: &str, db: &Database) -> Result<(), AnalyzerError> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let statement = parser.parse().expect("Failed to parse SQL");

        let mut analyzer = Analyzer::new(db);
        analyzer.analyze(&statement)
    }

    #[test]
    #[serial]
    fn test_analyzer_1() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE TABLE products (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            price DOUBLE,
            in_stock BOOLEAN DEFAULT TRUE
        )";
        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_2() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE TABLE users (id INT)";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::AlreadyExists(AlreadyExists::Table(name))) if name == "users"
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_3() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE TABLE test (
            id INT PRIMARY KEY,
            email TEXT PRIMARY KEY
        )";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::MultiplePrimaryKeys)));
    }

    #[test]
    #[serial]
    fn test_analyzer_4() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_5() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'John', 'extra@email.com')";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::ColumnValueCountMismatch(_))
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_6() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, email, age) VALUES (1, 'John', NULL, 34)";

        let result = parse_and_analyze(sql, &db);

        assert!(matches!(
            result,
            Err(AnalyzerError::ConstraintViolation(_, column)) if column == "email"
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_7() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 'not_a_number')";

        let result = parse_and_analyze(sql, &db);

        assert!(matches!(result, Err(AnalyzerError::InvalidValue(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, email, age) VALUES
                   (1, 'John', 'john@example.com', 25),
                   (2, 'Jane', 'jane@example.com', 30)";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_9() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_10() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT name, email FROM users WHERE age > 18";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_11() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT u.name, p.title
                   FROM users u
                   JOIN posts p ON u.id = p.user_id";
        let result = parse_and_analyze(sql, &db);

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_12() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT invalid_column FROM users";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_13() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM non_existent_table";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_14() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT name FROM users WHERE id IN (SELECT user_id FROM posts)";
        let result = parse_and_analyze(sql, &db);

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_15() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT name,
                        CASE
                            WHEN age < 18 THEN 'Minor'
                            WHEN age >= 18 AND age < 65 THEN 'Adult'
                            ELSE 'Senior'
                        END as category
                   FROM users";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_16() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT name FROM users u
                   WHERE EXISTS (SELECT 1 FROM posts p WHERE p.user_id = u.id)";
        let result = parse_and_analyze(sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_17() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "UPDATE users SET name = 'Updated Name' WHERE id = 1";
        let result = parse_and_analyze(sql, &db);

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_18() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "UPDATE users SET name = 'New Name', age = 30 WHERE id = 1";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_19() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "UPDATE users SET email = NULL WHERE id = 1";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::ConstraintViolation(_, column)) if column == "email"
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_20() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "UPDATE users SET invalid_column = 'value'";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_21() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DELETE FROM users WHERE age < 18";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_22() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DELETE FROM users";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_23() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DELETE FROM non_existent";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_24() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "ALTER TABLE users ADD COLUMN phone TEXT";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_25() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "ALTER TABLE users ADD COLUMN name TEXT";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::AlreadyExists(AlreadyExists::Column(_, column))) if column == "name"
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_26() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "ALTER TABLE users DROP COLUMN email";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_27() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "ALTER TABLE users DROP COLUMN phone";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::MissingColumns)));
    }

    #[test]
    #[serial]
    fn test_analyzer_28() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        // Create a table without primary key first
        let sql = "ALTER TABLE posts ADD CONSTRAINT PRIMARY KEY (id)";

        // Note: This might fail if posts already has a PK
        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::MultiplePrimaryKeys)));
    }

    #[test]
    #[serial]
    fn test_analyzer_29() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "ALTER TABLE posts ADD CONSTRAINT FOREIGN KEY (user_id) REFERENCES users(id)";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_30() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DROP TABLE users";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_31() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DROP TABLE IF EXISTS non_existent";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_32() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "DROP TABLE non_existent";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_33() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();

        assert!(parse_and_analyze("BEGIN", &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_34() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let mut analyzer = Analyzer::new(&db);

        // Start a transaction
        let lexer = Lexer::new("BEGIN");
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().unwrap();
        assert!(analyzer.analyze(&stmt).is_ok());

        // Try to start another one
        let lexer = Lexer::new("BEGIN");
        let mut parser = Parser::new(lexer);
        let stmt = parser.parse().unwrap();
        let result = analyzer.analyze(&stmt);

        assert!(matches!(result, Err(AnalyzerError::AlreadyStarted)));
    }

    #[test]
    #[serial]
    fn test_analyzer_35() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "WITH adult_users AS (
                      SELECT * FROM users WHERE age >= 18
                   )
                   SELECT name FROM adult_users";

        let result = parse_and_analyze(sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_36() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "WITH
                      adults AS (SELECT * FROM users WHERE age >= 18),
                      active_posts AS (SELECT * FROM posts WHERE published = TRUE)
                   SELECT a.name, p.title
                   FROM adults a
                   JOIN active_posts p ON a.id = p.user_id";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_37() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE INDEX idx_users_email ON users(email)";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_38() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE UNIQUE INDEX idx_users_email_unique ON users(email)";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_39() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE INDEX IF NOT EXISTS idx_test ON users(name)";
        let result = parse_and_analyze(sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_40() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "CREATE INDEX idx_invalid ON users(invalid_column)";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::MissingColumns)));
    }

    #[test]
    #[serial]
    fn test_analyzer_41() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, email, age)
                   VALUES (999999999999, 'John', 'john@example.com', 25)";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(
            result,
            Err(AnalyzerError::ArithmeticOverflow(_, _))
        ));
    }

    #[test]
    #[serial]
    fn test_analyzer_42() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();

        let sql = "INSERT INTO users (id, name, email, age, created_at)
                   VALUES (1, 'John', 'john@example.com', 25, '2024-01-15T10:30:00')";

        let result = parse_and_analyze(sql, &db);

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_43() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (id, name, email, age, created_at)
                   VALUES (1, 'John', 'john@example.com', 25, 'not-a-date')";

        let result = parse_and_analyze(sql, &db);
        assert!(matches!(result, Err(AnalyzerError::InvalidFormat(..))));
    }

    #[test]
    #[serial]
    fn test_analyzer_44() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users WHERE age = 'not_a_number'";

        let result = parse_and_analyze(sql, &db);

        assert!(matches!(result, Err(AnalyzerError::InvalidValue(_))));
    }

    #[test]
    #[serial]
    fn test_analyzer_45() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let result = parse_and_analyze(sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_46() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3, 4)";
        let result = parse_and_analyze(sql, &db);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_analyzer_47() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users WHERE name LIKE 'John%'";

        assert!(parse_and_analyze(sql, &db).is_ok());
    }
}
