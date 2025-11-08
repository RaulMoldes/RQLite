use crate::database::schema::Schema;
use crate::database::schema::{AsBytes, DBObject, Database};
use crate::sql::ast::{
    AlterAction, AlterColumnAction, Expr, SelectItem, SelectStatement, Statement,
    TableConstraintExpr, TableReference, TransactionStatement, Values, WhenClause,
};
use crate::{storage::tuple, types::DataTypeKind};
use regex::Regex;
use std::{collections::HashMap, fmt::Display};

fn is_date_iso(s: &str) -> bool {
    // Format: YYYY-MM-DD
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    re.is_match(s)
}

fn is_datetime_iso(s: &str) -> bool {
    // Format: YYYY-MM-DDTHH:MM:SS
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap();
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
    InvalidExpression,
    AlreadyStarted,
    NotStarted,
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
            }
        }
    }
}

struct Analyzer<'a> {
    db: &'a Database,
    aliases: HashMap<String, DBObject>,
    is_on_transaction: bool,
}

impl<'a> Analyzer<'a> {
    fn analyze_ident(&mut self, ident: &str) -> Result<(), AnalyzerError> {
        Ok(()) // TODO.
    }

    fn analyze_func(&mut self, ident: &str) -> Result<(), AnalyzerError> {
        Ok(()) // TODO. SHOULD VALIDATE THAT THE FUNCTION EXISTS.
    }

    fn analyze_when(&mut self, clause: &WhenClause) -> Result<(), AnalyzerError> {
        self.analyze_expr(&clause.condition)?;
        self.analyze_expr(&clause.result)?;

        Ok(()) // TODO
    }

    fn analyze_expr(&mut self, expr: &Expr) -> Result<(), AnalyzerError> {
        match expr {
            Expr::QualifiedIdentifier { table, column } => {
                if let Some(obj) = self.aliases.get(table) {
                    if let Some(metadata) = obj.metadata() {
                        if let Ok((schema, _)) = Schema::read_from(metadata) {
                            if schema.has_column(column) {
                                return Ok(());
                            } else {
                                return Err(AnalyzerError::MissingColumns);
                            };
                        };
                    };
                };
                return Err(AnalyzerError::NotFound(table.clone()));
            }
            Expr::BinaryOp { left, op, right } => {
                self.analyze_expr(left)?;
                self.analyze_expr(right)?;
            }
            Expr::Identifier(str) => {
                self.analyze_ident(str)?;
            }
            Expr::Exists(stmt) => {
                self.analyze_select(stmt.as_ref())?;
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                self.analyze_func(name)?;
                for arg in args {
                    self.analyze_expr(arg)?;
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.analyze_expr(expr)?;
                self.analyze_expr(low)?;
                self.analyze_expr(high)?;
            }
            Expr::List(items) => {
                for item in items {
                    self.analyze_expr(item)?;
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                for item in when_clauses {
                    self.analyze_when(item)?;
                }
                if let Some(expr) = operand {
                    self.analyze_expr(expr)?;
                }
                if let Some(expr) = else_clause {
                    self.analyze_expr(expr)?;
                }
            }
            Expr::UnaryOp { op, expr } => {
                self.analyze_expr(expr)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn analyze_table_ref(&mut self, reference: &TableReference) -> Result<(), AnalyzerError> {
        match reference {
            TableReference::Table { name, alias } => {
                let oid = self
                    .db
                    .search_obj(name)
                    .map_err(|_| AnalyzerError::NotFound(name.clone()))?;
                let obj = self
                    .db
                    .get_obj(oid)
                    .map_err(|_| AnalyzerError::NotFound(name.clone()))?;

                if let Some(aliased) = alias {
                    self.aliases.insert(aliased.clone(), obj);
                } else {
                    self.aliases.insert(name.clone(), obj);
                };
            }
            TableReference::Join {
                left, right, on, ..
            } => {
                self.analyze_table_ref(left)?;
                self.analyze_table_ref(right)?;

                if let Some(Expr::BinaryOp { left, op, right }) = on {
                    self.analyze_expr(left)?;
                    self.analyze_expr(right)?;
                };
            }
            TableReference::Subquery { query, alias } => {
                self.analyze_select(query.as_ref())?;
            }
        };
        Ok(())
    }

    fn analyze_select(&mut self, stmt: &SelectStatement) -> Result<(), AnalyzerError> {
        if let Some(table_ref) = &stmt.from {
            self.analyze_table_ref(table_ref)?;
        }
        for item in stmt.columns.iter() {
            if let SelectItem::ExprWithAlias { expr, .. } = item {
                self.analyze_expr(expr)?;
            }
        }
        if let Some(clause) = &stmt.where_clause {
            self.analyze_expr(clause)?;
        }

        for order_by in stmt.order_by.iter() {
            self.analyze_expr(&order_by.expr)?;
        }

        for group_by in stmt.group_by.iter() {
            self.analyze_expr(group_by)?;
        }

        Ok(())
    }
    fn analyze(&mut self, stmt: &Statement) -> Result<(), AnalyzerError> {
        match stmt {
            Statement::Transaction(t) => match t {
                TransactionStatement::Begin => {
                    if self.is_on_transaction {
                        return Err(AnalyzerError::AlreadyStarted);
                    };

                    self.is_on_transaction = true;
                }
                TransactionStatement::Commit | TransactionStatement::Rollback => {
                    if !self.is_on_transaction {
                        return Err(AnalyzerError::NotStarted);
                    };

                    self.is_on_transaction = false;
                }
            },
            Statement::Select(s) => self.analyze_select(s)?,
            Statement::With(s) => {
                self.analyze_select(&s.body)?;
                for (alias, item) in s.ctes.iter() {
                    self.analyze_select(item)?;
                }
            }
            Statement::Insert(s) => {
                let obj = self.obj_exists(&s.table)?;
                let schema = self.get_schema(&obj);

                let column_dtypes: Vec<(String, DataTypeKind, bool)> =
                    if let Some(cols) = &s.columns {
                        self.obj_has_columns(&obj, cols)?;
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
                        self.analyze_select(expr)?;
                    }
                    Values::Values(expr_list) => {
                        for (i, row) in expr_list.iter().enumerate() {
                            if column_dtypes.len() != row.len() {
                                return Err(AnalyzerError::ColumnValueCountMismatch(i));
                            };

                            for (i, value) in row.iter().enumerate() {
                                let (name, dtype, is_non_null) = &column_dtypes[i];

                                if *is_non_null && matches!(value, Expr::Null) {
                                    return Err(AnalyzerError::ConstraintViolation(
                                        "Non null constraint".to_string(),
                                        name.to_owned(),
                                    ));
                                };

                                // TODO: Datatype validation is not yet implemented.
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
                        let schema = self.get_schema(&obj);

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
                                // TODO. I NEED TO REVIEW HOW TO PERFORM TYPE COERCION
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
                            let schema = self.get_schema(&obj);
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
                        let schema = self.get_schema(&obj);
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

                if let Some(clause) = &stmt.where_clause {
                    self.analyze_expr(clause)?;
                }
            }
            Statement::Update(stmt) => {
                let obj = self.obj_exists(&stmt.table)?;
                let schema = self.get_schema(&obj);
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

    fn get_schema(&self, obj: &DBObject) -> Schema {
        if let Some(metadata) = obj.metadata() {
            if let Ok((schema, _)) = Schema::read_from(metadata) {
                return schema;
            }
        };
        panic!("Object does not have schema");
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
}

fn validate_dataype(dtype: DataTypeKind, value: Expr) -> Result<(), AnalyzerError> {
    match value {
        Expr::Number(f) => match dtype {
            DataTypeKind::Boolean
            | DataTypeKind::Byte
            | DataTypeKind::Char
            | DataTypeKind::SmallInt
            | DataTypeKind::SmallUInt => {
                if f > u8::MAX as f64 {
                    return Err(AnalyzerError::ArithmeticOverflow(f, dtype));
                };
            }
            DataTypeKind::HalfInt | DataTypeKind::HalfUInt => {
                if f > u16::MAX as f64 {
                    return Err(AnalyzerError::ArithmeticOverflow(f, dtype));
                };
            }
            DataTypeKind::Int | DataTypeKind::UInt | DataTypeKind::Date => {
                if f > u32::MAX as f64 {
                    return Err(AnalyzerError::ArithmeticOverflow(f, dtype));
                };
            }
            _ => {}
        },
        Expr::String(item) => {
            let len = item.len();
            let numeric = item.parse::<f64>().is_ok();

            match dtype {
                DataTypeKind::Byte | DataTypeKind::SmallInt | DataTypeKind::SmallUInt => {
                    if !numeric {
                        return Err(AnalyzerError::InvalidExpression);
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
                        return Err(AnalyzerError::InvalidExpression);
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
                        return Err(AnalyzerError::InvalidExpression);
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
                        return Err(AnalyzerError::InvalidExpression);
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
                        return Err(AnalyzerError::InvalidExpression);
                    };
                }
                DataTypeKind::Boolean => {
                    if item != "TRUE" && item != "FALSE" {
                        return Err(AnalyzerError::InvalidExpression);
                    };
                }
                DataTypeKind::Date => {
                    if !(is_date_iso(&item) || numeric && len <= crate::types::Date::SIZE) {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }
                DataTypeKind::DateTime => {
                    if !(is_datetime_iso(&item) || numeric && len <= crate::types::DateTime::SIZE) {
                        return Err(AnalyzerError::InvalidExpression);
                    }
                }
                _ => {}
            }
        }
        Expr::Boolean(b) => {

            if !matches!(dtype, DataTypeKind::Boolean) {
                return Err(AnalyzerError::InvalidExpression);
            }
        },
        _ => {}
    }
    Ok(())
}
