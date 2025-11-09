use crate::database::schema::ObjectType;
use crate::database::schema::{AsBytes, DBObject, Database};
use crate::database::schema::{Column, Schema};
use crate::sql::ast::{
    AlterAction, AlterColumnAction, BinaryOperator, ColumnConstraintExpr, Expr, SelectItem,
    SelectStatement, Statement, TableConstraintExpr, TableReference, TransactionStatement, Values,
};
use crate::types::DataTypeKind;
use regex::Regex;
use std::collections::HashSet;
use std::{collections::HashMap, fmt::Display};

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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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
    subqueries: HashMap<String, DBObject>,
    current_table: Option<String>,
    on_transaction: bool,
}

#[derive(Debug, PartialEq, Clone)]
enum ExprResult {
    Table(Vec<DataTypeKind>),
    Value(DataTypeKind),
}
struct Analyzer<'a> {
    db: &'a Database,
    ctx: AnalyzerCtx,
}

impl<'a> Analyzer<'a> {
    fn new(db: &'a Database) -> Self {
        Self {
            db,
            ctx: AnalyzerCtx {
                table_aliases: HashMap::new(),
                subqueries: HashMap::new(),
                on_transaction: false,
                current_table: None,
            },
        }
    }
    fn table_aliases(&self) -> &HashMap<String, String> {
        &self.ctx.table_aliases
    }

    fn analyze_identifier(&mut self, ident: &str) -> Result<DataTypeKind, AnalyzerError> {
        if self.ctx.current_table.is_none() {
            return Err(AnalyzerError::AliasNotProvided);
        }

        let obj = self.obj_exists(self.ctx.current_table.as_ref().unwrap())?;
        let schema = obj.get_schema();
        if let Some((_, dtype, _)) = schema.get_dtype(ident) {
            return Ok(dtype);
        }

        Err(AnalyzerError::NotFound(ident.to_string()))
    }

    fn analyze_func(
        &mut self,
        ident: &str,
        args_dtypes: Vec<DataTypeKind>,
    ) -> Result<ExprResult, AnalyzerError> {
        Ok(ExprResult::Value(DataTypeKind::Null)) // TODO. SHOULD VALIDATE THAT THE FUNCTION EXISTS.
    }

    fn analyze_expr(&mut self, expr: &Expr) -> Result<ExprResult, AnalyzerError> {
        match expr {
            // Qualified idents only appear on select clauses.
            // This can be either on a select clause inside an outer insert or with clause, or on a main select clause.
            Expr::QualifiedIdentifier { table, column } => {
                if let Some(obj_name) = self.table_aliases().get(table) {
                    let obj = self.obj_exists(obj_name)?;
                    if let Some(metadata) = obj.metadata() {
                        if let Ok((schema, _)) = Schema::read_from(metadata) {
                            if schema.has_column(column) {
                                let (_, dtype, _) = schema
                                    .get_dtype(column)
                                    .ok_or(AnalyzerError::NotFound(column.to_string()))?;

                                return Ok(ExprResult::Value(dtype)); // Return the datatype of the column
                            } else {
                                return Err(AnalyzerError::MissingColumns);
                            };
                        };
                    };
                };
                Err(AnalyzerError::NotFound(table.clone()))
            }
            // Binary operators typically appear inside either where or join clauses.
            // What we need to validate here is that the
            Expr::BinaryOp { left, op, right } => {
                let left_result = self.analyze_expr(left)?;

                if let ExprResult::Value(left_dt) = left_result {
                    self.analyze_value(left_dt, right)?;
                }

                match (left_result, self.analyze_expr(right)?) {
                    (ExprResult::Value(left_dt), ExprResult::Value(right_dt)) => {
                        if !left_dt.can_be_coerced(right_dt) && !right_dt.can_be_coerced(left_dt) {
                            return Err(AnalyzerError::InvalidDataType(right_dt));
                        }

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

                            Ok(ExprResult::Value(left_dt))

                        } else {
                            Ok(ExprResult::Value(DataTypeKind::Boolean))
                        }
                    }
                    (ExprResult::Value(left_dt), ExprResult::Table(right_dts)) => {

                        if !right_dts.iter().all(|c| *c == right_dts[0])  {

                            return Err(AnalyzerError::InvalidExpression);
                        }

                        if !left_dt.can_be_coerced(right_dts[0])
                            && !right_dts[0].can_be_coerced(left_dt)
                        {
                            return Err(AnalyzerError::InvalidDataType(right_dts[0]));
                        }

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

                            if !right_dts[0].is_numeric() {
                                return Err(AnalyzerError::InvalidDataType(right_dts[0]));
                            }

                            Ok(ExprResult::Value(left_dt))
                        } else {
                            Ok(ExprResult::Value(DataTypeKind::Boolean))
                        }
                    }
                    _ => Err(AnalyzerError::InvalidExpression),
                }
            }
            Expr::Identifier(str) => {
                let datatype = self.analyze_identifier(str)?;
                Ok(ExprResult::Value(datatype))
            }
            Expr::Exists(stmt) => {
                self.analyze_select(stmt.as_ref())?;
                Ok(ExprResult::Value(DataTypeKind::Boolean))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let mut args_dtypes = Vec::with_capacity(args.len());
                for arg in args {
                    if let ExprResult::Value(item) = self.analyze_expr(arg)? {
                        args_dtypes.push(item);
                    } else {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }
                let return_dtype = self.analyze_func(name, args_dtypes)?;
                Ok(return_dtype)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if let ExprResult::Value(expr_dtype) = self.analyze_expr(expr)? {
                    if let ExprResult::Value(left_dtype) = self.analyze_expr(low)? {
                        if !expr_dtype.can_be_coerced(left_dtype)
                            && !left_dtype.can_be_coerced(expr_dtype)
                        {
                            return Err(AnalyzerError::InvalidDataType(left_dtype));
                        }

                        if let ExprResult::Value(right_dtype) = self.analyze_expr(high)? {
                            if !expr_dtype.can_be_coerced(right_dtype)
                                && !right_dtype.can_be_coerced(expr_dtype)
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
                            {
                                return Err(AnalyzerError::InvalidDataType(dtype));
                            };
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

                if !out_dtypes.iter().all(|&x| x == out_dtypes[0]) {
                    return Err(AnalyzerError::InvalidExpression);
                }
                Ok(ExprResult::Value(out_dtypes[0]))
            }
            Expr::UnaryOp { op, expr } => self.analyze_expr(expr),

            Expr::Null => Ok(ExprResult::Value(DataTypeKind::Null)),
            Expr::Star => {
                let mut out = Vec::new();
                for table in self.table_aliases().values() {
                    let obj = self.obj_exists(table)?;
                    let schema = obj.get_schema();
                    let types: Vec<DataTypeKind> = schema.columns.iter().map(|c| c.dtype).collect();
                    out.extend(types);
                }
                for subq in self.ctx.subqueries.values() {
                    let schema = subq.get_schema();
                    let types: Vec<DataTypeKind> = schema.columns.iter().map(|c| c.dtype).collect();
                    out.extend(types);
                }
                Ok(ExprResult::Table(out))
            }
            Expr::Number(_) => Ok(ExprResult::Value(DataTypeKind::Double)),
            Expr::String(_) => Ok(ExprResult::Value(DataTypeKind::Blob)),
            Expr::Boolean(_) => Ok(ExprResult::Value(DataTypeKind::Boolean)),
            Expr::Subquery(item) => {
                let schema = self.analyze_select(item)?;
                Ok(ExprResult::Table(
                    schema.columns.iter().map(|d| d.dtype).collect(),
                ))
            }
        }
    }

    fn analyze_table_ref(&mut self, reference: &TableReference) -> Result<(), AnalyzerError> {
        match reference {
            TableReference::Table { name, alias } => {
                let obj = self.obj_exists(name)?;
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
                    self.analyze_expr(on_expr)?;
                }
            }
            TableReference::Subquery { query, alias } => {
                let schema = self.analyze_select(query.as_ref())?;
                let obj =
                    DBObject::new(ObjectType::Table, alias, Some(&schema.write_to().unwrap()));
                self.ctx.subqueries.insert(alias.to_string(), obj);
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
                    // Push all the columns from all cached tables
                    for table in self.table_aliases().values() {
                        let obj = self.obj_exists(table)?;
                        let schema = obj.get_schema();
                        out_columns.extend(schema.columns);
                    }
                    for subq in self.ctx.subqueries.values() {
                        let schema = subq.get_schema();
                        out_columns.extend(schema.columns);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let cname = if let Some(aliased) = alias {
                        aliased
                    } else if let Expr::Identifier(value) = expr {
                        value
                    } else if let Expr::QualifiedIdentifier { table, column } = expr {
                        column
                    } else {
                        "Unknown"
                    };

                    let result = self.analyze_expr(expr)?;
                    if let ExprResult::Value(item) = result {
                        let column = Column::new(item, cname, None);
                        out_columns.push(column);
                    };
                }
            }
        }

        if let Some(clause) = &stmt.where_clause {
            if let ExprResult::Value(dt) = self.analyze_expr(clause)? {
                if !matches!(dt, DataTypeKind::Boolean) {
                    return Err(AnalyzerError::InvalidExpression);
                }
            } else {
                return Err(AnalyzerError::InvalidExpression);
            }
        }

        for order_by in stmt.order_by.iter() {
            self.analyze_expr(&order_by.expr)?;
        }

        for group_by in stmt.group_by.iter() {
            self.analyze_expr(group_by)?;
        }

        Ok(Schema {
            columns: out_columns,
        })
    }

    fn analyze(&mut self, stmt: &Statement) -> Result<(), AnalyzerError> {
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
                for (alias, item) in s.ctes.iter() {
                    let schema = self.analyze_select(item)?;
                    let obj =
                        DBObject::new(ObjectType::Table, alias, Some(&schema.write_to().unwrap()));
                    self.ctx.subqueries.insert(alias.to_string(), obj);
                }
                self.analyze_select(&s.body)?;
            }
            Statement::Insert(s) => {
                let obj = self.obj_exists(&s.table)?;
                let schema = obj.get_schema();

                let column_dtypes: Vec<(String, DataTypeKind, bool)> =
                    if let Some(cols) = &s.columns {
                        cols.iter()
                            .map(|c| {
                                schema
                                    .get_dtype(c)
                                    .ok_or_else(|| AnalyzerError::NotFound(c.clone()))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    } else {
                        schema
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.dtype, c.is_non_null()))
                            .collect::<Vec<_>>()
                    };

                match &s.values {
                    Values::Query(expr) => {
                        let schema = self.analyze_select(expr)?;
                        if column_dtypes.len() != schema.columns.len() {
                            return Err(AnalyzerError::ColumnValueCountMismatch(0));
                        };

                        for (i, column) in schema.columns.iter().enumerate() {
                            if !column_dtypes[i].2 && matches!(column.dtype, DataTypeKind::Null) {
                                return Err(AnalyzerError::ConstraintViolation(
                                    "Non null constraint".to_string(),
                                    column_dtypes[i].0.to_string(),
                                ));
                            }

                            if !column.dtype.can_be_coerced(column_dtypes[i].1)
                                && !column_dtypes[i].1.can_be_coerced(column.dtype)
                            {
                                return Err(AnalyzerError::InvalidDataType(column.dtype));
                            }
                        }
                    }
                    Values::Values(expr_list) => {
                        for (i, row) in expr_list.iter().enumerate() {
                            if column_dtypes.len() != row.len() {
                                return Err(AnalyzerError::ColumnValueCountMismatch(i));
                            };

                            for (i, value) in row.iter().enumerate() {
                                let (name, cdtype, is_non_null) = &column_dtypes[i];

                                if *is_non_null && matches!(value, Expr::Null) {
                                    return Err(AnalyzerError::ConstraintViolation(
                                        "Non null constraint".to_string(),
                                        name.to_owned(),
                                    ));
                                };

                                self.analyze_value(*cdtype, value)?;
                            }
                        }
                    }
                }
            }
            Statement::CreateTable(stmt) => {
                if self.obj_exists(&stmt.table).is_ok() {
                    return Err(AnalyzerError::AlreadyExists(AlreadyExists::Table(
                        stmt.table.clone(),
                    )));
                }
                let mut has_primary = false;
                let mut column_set = HashSet::with_capacity(stmt.columns.len());
                for col in stmt.columns.iter() {
                    let mut has_default = false;

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
                            ColumnConstraintExpr::PrimaryKey => {
                                if has_primary {
                                    return Err(AnalyzerError::MultiplePrimaryKeys);
                                } else {
                                    has_primary = true;
                                }
                            }
                            ColumnConstraintExpr::ForeignKey { table, column } => {
                                let other_obj = self.obj_exists(table)?;
                                if let Some((_, dtype, _)) =
                                    other_obj.get_schema().get_dtype(column)
                                {
                                    if !dtype.can_be_coerced(col.data_type)
                                        && !col.data_type.can_be_coerced(dtype)
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

                            let other_obj = self.obj_exists(ref_table)?;
                            self.obj_has_columns(&other_obj, ref_columns)?;
                        }
                        _ => {}
                    }
                }
            }
            Statement::AlterTable(stmt) => {
                let obj = self.obj_exists(&stmt.table)?;

                match &stmt.action {
                    AlterAction::AddColumn(col) => {
                        if self.obj_has_columns(&obj, &[col.name.to_string()]).is_ok() {
                            return Err(AnalyzerError::AlreadyExists(AlreadyExists::Column(
                                obj.name(),
                                col.name.to_string(),
                            )));
                        }
                    }
                    AlterAction::AlterColumn(alter_col) => {
                        self.obj_has_columns(&obj, &[alter_col.name.to_string()])?;
                        let schema = obj.get_schema();

                        let (name, dtype_info, is_non_null) = schema
                            .get_dtype(&alter_col.name)
                            .ok_or_else(|| AnalyzerError::NotFound(alter_col.name.to_string()))?;

                        match &alter_col.action {
                            AlterColumnAction::SetDefault(value) => {
                                if matches!(value, Expr::Null) && is_non_null {
                                    return Err(AnalyzerError::ConstraintViolation(
                                        "Non null constraint".to_string(),
                                        name,
                                    ));
                                }
                            }
                            AlterColumnAction::SetDataType(value) => {
                                if matches!(value, DataTypeKind::Null) && is_non_null {
                                    return Err(AnalyzerError::ConstraintViolation(
                                        "Non null constraint".to_string(),
                                        name,
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
                            self.obj_has_columns(&obj, columns)?;
                            let other_obj = self.obj_exists(ref_table)?;
                            self.obj_has_columns(&other_obj, ref_columns)?;
                        }
                        TableConstraintExpr::PrimaryKey(cols) => {
                            let schema = obj.get_schema();
                            if schema.has_pk() {
                                return Err(AnalyzerError::MultiplePrimaryKeys);
                            };
                            self.obj_has_columns(&obj, cols)?;
                        }
                        TableConstraintExpr::Unique(cols) => {
                            self.obj_has_columns(&obj, cols)?;
                        }
                        _ => {}
                    },
                    AlterAction::DropColumn(col) => {
                        self.obj_has_columns(&obj, &[col.to_string()])?;
                    }
                    AlterAction::DropConstraint(name) => {
                        let schema = obj.get_schema();
                        if schema.has_constraint(name) {
                            return Err(AnalyzerError::AlreadyExists(AlreadyExists::Constraint(
                                name.to_string(),
                            )));
                        }
                    }
                }
            }
            Statement::Delete(stmt) => {
                let obj = self.obj_exists(&stmt.table)?;
                self.ctx
                    .table_aliases
                    .insert(stmt.table.to_string(), stmt.table.to_string());
                self.ctx.current_table = Some(stmt.table.to_string());
                if let Some(clause) = &stmt.where_clause {
                    self.analyze_expr(clause)?;
                }
            }
            Statement::Update(stmt) => {
                let obj = self.obj_exists(&stmt.table)?;
                self.ctx
                    .table_aliases
                    .insert(stmt.table.to_string(), stmt.table.to_string());
                self.ctx.current_table = Some(stmt.table.to_string());
                let schema = obj.get_schema();
                if let Some(clause) = &stmt.where_clause {
                    self.analyze_expr(clause)?;
                }

                for column in stmt.set_clauses.iter() {
                    let (name, dtype, is_non_null) = schema
                        .get_dtype(&column.column)
                        .ok_or_else(|| AnalyzerError::NotFound(column.column.to_string()))?;

                    if matches!(column.value, Expr::Null) && is_non_null {
                        return Err(AnalyzerError::ConstraintViolation(
                            "Non null constraint".to_string(),
                            name,
                        ));
                    }
                    self.analyze_value(dtype, &column.value)?;
                }
            }
            Statement::DropTable(stmt) => {
                if !stmt.if_exists {
                    let _ = self.obj_exists(&stmt.table)?;
                };
            }
            Statement::CreateIndex(stmt) => {
                if self.obj_exists(&stmt.name).is_ok() && !stmt.if_not_exists {
                    return Err(AnalyzerError::AlreadyExists(AlreadyExists::Index(
                        stmt.name.clone(),
                    )));
                }
                let obj = self.obj_exists(&stmt.table)?;
                let columns: Vec<String> =
                    stmt.columns.iter().map(|c| c.name.to_string()).collect();
                self.obj_has_columns(&obj, &columns)?;
            }
        }
        Ok(())
    }

    fn obj_exists(&self, obj_name: &str) -> Result<DBObject, AnalyzerError> {
        if let Some(subquery) = self.ctx.subqueries.get(obj_name) {
            return Ok(subquery.clone()); // TODO: avoid cloning here
        };

        let oid = self
            .db
            .search_obj(obj_name)
            .map_err(|_| AnalyzerError::NotFound(obj_name.to_string()))?;

        let obj = self
            .db
            .get_obj(oid)
            .map_err(|_| AnalyzerError::NotFound(obj_name.to_string()))?;

        Ok(obj)
    }

    fn obj_has_columns(&self, obj: &DBObject, cols: &[String]) -> Result<(), AnalyzerError> {
        if let Some(metadata) = obj.metadata() {
            if let Ok((schema, _)) = Schema::read_from(metadata) {
                for col in cols {
                    if !schema.has_column(col) {
                        return Err(AnalyzerError::NotFound(col.to_string()));
                    };
                }
            };
        };

        Ok(())
    }

    fn analyze_value(
        &self,
        dtype: DataTypeKind,
        value: &Expr,
    ) -> Result<DataTypeKind, AnalyzerError> {
        match value {
            Expr::Number(f) => {
                match dtype {
                    DataTypeKind::Boolean
                    | DataTypeKind::Byte
                    | DataTypeKind::Char
                    | DataTypeKind::SmallInt
                    | DataTypeKind::SmallUInt => {
                        if *f > u8::MAX as f64 {
                            return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                        };
                    }
                    DataTypeKind::HalfInt | DataTypeKind::HalfUInt => {
                        if *f > u16::MAX as f64 {
                            return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                        };
                    }
                    DataTypeKind::Int | DataTypeKind::UInt | DataTypeKind::Date => {
                        if *f > u32::MAX as f64 {
                            return Err(AnalyzerError::ArithmeticOverflow(*f, dtype));
                        };
                    }
                    _ => {}
                }

                Ok(DataTypeKind::BigInt)
            }
            Expr::String(item) => {
                let len = item.len();
                let numeric = item.parse::<f64>().is_ok();

                match dtype {
                    DataTypeKind::Byte | DataTypeKind::SmallInt | DataTypeKind::SmallUInt => {
                        if !numeric {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::SmallInt));
                        }

                        if len > crate::types::UInt8::SIZE {
                            return Err(AnalyzerError::ArithmeticOverflow(
                                item.parse::<f64>().unwrap(),
                                dtype,
                            ));
                        };
                    }
                    DataTypeKind::HalfInt | DataTypeKind::HalfUInt => {
                        if !numeric {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::HalfInt));
                        }

                        if len > crate::types::UInt16::SIZE {
                            return Err(AnalyzerError::ArithmeticOverflow(
                                item.parse::<f64>().unwrap(),
                                dtype,
                            ));
                        };
                    }
                    DataTypeKind::Int | DataTypeKind::UInt => {
                        if !numeric {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Int));
                        }

                        if len > crate::types::UInt32::SIZE {
                            return Err(AnalyzerError::ArithmeticOverflow(
                                item.parse::<f64>().unwrap(),
                                dtype,
                            ));
                        };
                    }
                    DataTypeKind::BigInt | DataTypeKind::BigUInt => {
                        if !numeric {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::BigInt));
                        }

                        if len > crate::types::UInt64::SIZE {
                            return Err(AnalyzerError::ArithmeticOverflow(
                                item.parse::<f64>().unwrap(),
                                dtype,
                            ));
                        };
                    }
                    DataTypeKind::Char => {
                        if len > crate::types::UInt8::SIZE {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Char));
                        };
                    }
                    DataTypeKind::Boolean => {
                        if item != "TRUE" && item != "FALSE" {
                            return Err(AnalyzerError::InvalidValue(DataTypeKind::Boolean));
                        };
                    }
                    DataTypeKind::Date => {
                        if !is_date_iso(item) {
                            return Err(AnalyzerError::InvalidFormat(
                                item.to_string(),
                                "YY-MM-DD".to_string(),
                            ));
                        }
                    }
                    DataTypeKind::DateTime => {
                        if !is_datetime_iso(item) {
                            return Err(AnalyzerError::InvalidFormat(
                                item.to_string(),
                                "YY-MM-DD:hh::mm::ss".to_string(),
                            ));
                        }
                    }
                    _ => {}
                }

                Ok(DataTypeKind::Blob)
            }
            Expr::Boolean(b) => {
                if !matches!(dtype, DataTypeKind::Boolean) {
                    return Err(AnalyzerError::InvalidValue(dtype));
                }

                Ok(DataTypeKind::Boolean)
            }
            _ => Ok(DataTypeKind::Null),
        }
    }
}

#[cfg(test)]
mod sql_analyzer_tests {
    use super::*;
    use super::*;
    use crate::database::schema::{Column, Database, Schema};
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::analyzer::{AlreadyExists, Analyzer, AnalyzerError};
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::types::DataTypeKind;
    use crate::{IncrementalVaccum, RQLiteConfig, ReadWriteVersion, TextEncoding};
    use serial_test::serial;
    use std::path::Path;
    use std::result;
    use tempfile::tempdir;

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
        dbg!(&result);
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
        dbg!(&result);
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
        dbg!(&result);
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
        dbg!(&result);
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
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
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
        assert!(matches!(result, Err(AnalyzerError::NotFound(_))));
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
        dbg!(&result);
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
        dbg!(&result);
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
        dbg!(&result);
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
