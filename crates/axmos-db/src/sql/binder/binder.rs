// sql/binder/mod.rs

use crate::{
    io::pager::BtreeBuilder,
    multithreading::coordinator::Snapshot,
    schema::{
        base::{Column, Schema},
        catalog::CatalogTrait,
    },
    sql::parser::ast::{
        AlterAction, AlterColumnAction, AlterTableStatement, BinaryOperator, ColumnDefExpr,
        CreateIndexStatement, CreateTableStatement, DeleteStatement, DropTableStatement, Expr,
        InsertStatement, OrderByExpr, SelectItem, SelectStatement, Statement, TableConstraintExpr,
        TableReference, TransactionStatement, UnaryOperator, UpdateStatement, Values,
        WithStatement,
    },
    types::{DataType, DataTypeKind},
};

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

use super::bounds::*;
use super::{ScopeEntry, ScopeError, ScopeStack};

#[derive(Debug, Clone, PartialEq)]
pub enum BinderError {
    Scope(ScopeError),
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnCountMismatch { expected: usize, found: usize },
    InvalidExpression(String),
    Other(String),
}

impl Display for BinderError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Scope(e) => write!(f, "scope error: {}", e),
            Self::TableNotFound(name) => write!(f, "table '{}' not found", name),
            Self::ColumnNotFound(name) => write!(f, "column '{}' not found", name),
            Self::ColumnCountMismatch { expected, found } => {
                write!(
                    f,
                    "column count mismatch: expected {}, found {}",
                    expected, found
                )
            }
            Self::InvalidExpression(msg) => write!(f, "invalid expression: {}", msg),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for BinderError {}

impl From<ScopeError> for BinderError {
    fn from(e: ScopeError) -> Self {
        Self::Scope(e)
    }
}

pub type BinderResult<T> = Result<T, BinderError>;

pub struct Binder<C: CatalogTrait> {
    catalog: C,
    tree_builder: BtreeBuilder,
    snapshot: Snapshot,
    scopes: ScopeStack,
    ctes: HashMap<String, (usize, Schema)>,
    cte_queries: Vec<BoundSelect>,
}

impl<C: CatalogTrait> Binder<C> {
    pub fn new(catalog: C, tree_builder: BtreeBuilder, snapshot: Snapshot) -> Self {
        Self {
            catalog,
            tree_builder,
            snapshot,
            scopes: ScopeStack::new(),
            ctes: HashMap::new(),
            cte_queries: Vec::new(),
        }
    }

    /// Main entry point:
    ///
    /// The binder is in charge of consuming raw [Statement] data structures produced by the parser and generating [BoundStatement]
    pub fn bind(&mut self, stmt: &Statement) -> BinderResult<BoundStatement> {
        // Reset state
        self.ctes.clear();
        self.cte_queries.clear();

        match stmt {
            Statement::Select(s) => Ok(BoundStatement::Select(self.bind_select(s)?)),
            Statement::With(w) => Ok(BoundStatement::With(self.bind_with(w)?)),
            Statement::Insert(i) => Ok(BoundStatement::Insert(self.bind_insert(i)?)),
            Statement::Update(u) => Ok(BoundStatement::Update(self.bind_update(u)?)),
            Statement::Delete(d) => Ok(BoundStatement::Delete(self.bind_delete(d)?)),
            Statement::CreateTable(c) => {
                Ok(BoundStatement::CreateTable(self.bind_create_table(c)?))
            }
            Statement::CreateIndex(c) => {
                Ok(BoundStatement::CreateIndex(self.bind_create_index(c)?))
            }
            Statement::AlterTable(a) => Ok(BoundStatement::AlterTable(self.bind_alter_table(a)?)),
            Statement::DropTable(d) => Ok(BoundStatement::DropTable(self.bind_drop_table(d)?)),
            Statement::Transaction(t) => Ok(BoundStatement::Transaction(self.bind_transaction(t)?)),
        }
    }

    fn bind_select(&mut self, stmt: &SelectStatement) -> BinderResult<BoundSelect> {
        self.scopes.push();

        // Bind FROM clause first to establish scope.
        let from = stmt
            .from
            .as_ref()
            .map(|t| self.bind_table_ref(t))
            .transpose()?;

        // Bind WHERE (can reference FROM tables)
        let where_clause = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;

        // Bind GROUP BY
        let group_by: Vec<BoundExpression> = stmt
            .group_by
            .iter()
            .map(|e| self.bind_expr(e))
            .collect::<BinderResult<_>>()?;

        // Bind HAVING
        let having = stmt
            .having
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;

        // Bind SELECT columns and collect output info
        let (columns, output_names) = self.bind_select_items(&stmt.columns)?;

        // Register output aliases for ORDER BY resolution
        for (i, item) in columns.iter().enumerate() {
            if let Some(scope) = self.scopes.current_mut() {
                scope.add_output_alias(output_names[i].clone(), item.expr.data_type());
            }
        }

        // Bind ORDER BY (can reference output aliases)
        let order_by: Vec<BoundOrderBy> = stmt
            .order_by
            .iter()
            .map(|o| self.bind_order_by(o, &columns))
            .collect::<BinderResult<_>>()?;

        // Build output schema
        let schema = self.build_select_schema(&columns);

        self.scopes.pop();

        Ok(BoundSelect {
            distinct: stmt.distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit: stmt.limit,
            offset: stmt.offset,
            schema,
        })
    }

    fn calculate_column_offset(&self, target_scope_index: usize) -> usize {
        if let Some(scope) = self.scopes.current() {
            let mut offset = 0;
            for entry in scope.iter() {
                if entry.scope_index >= target_scope_index {
                    break;
                }
                offset += entry.schema.num_columns();
            }
            offset
        } else {
            0
        }
    }

    /// Binds a reference to a table. Adds to scope any new table entries.
    fn bind_table_ref(&mut self, table_ref: &TableReference) -> BinderResult<BoundTableRef> {
        match table_ref {
            TableReference::Table { name, alias } => {
                // Check CTEs first
                if let Some((cte_idx, schema)) = self.ctes.get(name) {
                    let cte_idx = *cte_idx;
                    let schema = schema.clone();
                    let ref_name = alias.clone().unwrap_or_else(|| name.clone());

                    self.add_to_scope(ScopeEntry {
                        ref_name,
                        scope_index: self.current_scope_size(),
                        schema: schema.clone(),
                        table_id: None,
                        cte_idx: Some(cte_idx),
                    })?;

                    return Ok(BoundTableRef::Cte { cte_idx, schema });
                }

                // Lookup in catalog
                let relation = self
                    .catalog
                    .get_relation_by_name(name, &self.tree_builder, &self.snapshot)
                    .map_err(|_| BinderError::TableNotFound(name.clone()))?;

                let table_id = relation.object_id();
                let schema = relation.schema().clone();
                let ref_name = alias.clone().unwrap_or_else(|| name.clone());

                self.add_to_scope(ScopeEntry {
                    ref_name,
                    scope_index: self.current_scope_size(),
                    schema: schema.clone(),
                    table_id: Some(table_id),
                    cte_idx: None,
                })?;

                Ok(BoundTableRef::BaseTable { table_id, schema })
            }

            TableReference::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left_bound = self.bind_table_ref(left)?;
                let right_bound = self.bind_table_ref(right)?;
                let condition = on.as_ref().map(|e| self.bind_expr(e)).transpose()?;
                let schema = self.merge_schemas(left_bound.schema(), right_bound.schema());

                Ok(BoundTableRef::Join {
                    left: Box::new(left_bound),
                    right: Box::new(right_bound),
                    join_type: *join_type,
                    condition,
                    schema,
                })
            }

            TableReference::Subquery { query, alias } => {
                // Subquery has its own scope
                let bound_query = self.bind_select(query)?;
                let schema = bound_query.schema.clone();

                self.add_to_scope(ScopeEntry {
                    ref_name: alias.clone(),
                    scope_index: self.current_scope_size(),
                    schema: schema.clone(),
                    table_id: None,
                    cte_idx: None,
                })?;

                Ok(BoundTableRef::Subquery {
                    query: Box::new(bound_query),
                    schema,
                })
            }
        }
    }

    /// Binds a slice of [SelectItem], producing an array of [BoundSelectItem]
    fn bind_select_items(
        &mut self,
        items: &[SelectItem],
    ) -> BinderResult<(Vec<BoundSelectItem>, Vec<String>)> {
        let mut result = Vec::new();
        let mut names = Vec::new();
        let mut output_idx = 0;

        for item in items {
            match item {
                SelectItem::Star => {
                    // Expand star to all columns from all tables in scope
                    if let Some(scope) = self.scopes.current() {
                        for entry in scope.iter() {
                            let offset = self.calculate_column_offset(entry.scope_index);

                            for (col_idx, col) in entry.schema.iter_columns().enumerate() {
                                let col_ref = BoundColumnRef {
                                    table_id: entry.table_id,
                                    scope_index: entry.scope_index,
                                    column_idx: offset + col_idx,
                                    data_type: col.datatype(),
                                };

                                result.push(BoundSelectItem {
                                    expr: BoundExpression::ColumnRef(col_ref),
                                    output_idx,
                                    output_name: col.name().to_string(),
                                });
                                names.push(col.name().to_string());
                                output_idx += 1;
                            }
                        }
                    }
                }

                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound_expr = self.bind_expr(expr)?;
                    let output_name = alias
                        .clone()
                        .unwrap_or_else(|| self.infer_output_name(expr));

                    result.push(BoundSelectItem {
                        expr: bound_expr,
                        output_idx,
                        output_name: output_name.clone(),
                    });
                    names.push(output_name);
                    output_idx += 1;
                }
            }
        }

        Ok((result, names))
    }

    /// Binds an [OrderByExpr] producing a [BoundOrderBy]
    fn bind_order_by(
        &mut self,
        order_by: &OrderByExpr,
        select_items: &[BoundSelectItem],
    ) -> BinderResult<BoundOrderBy> {
        // Check if it's an alias reference
        if let Expr::Identifier(name) = &order_by.expr {
            for item in select_items {
                if &item.output_name == name {
                    return Ok(BoundOrderBy {
                        expr: item.expr.clone(),
                        asc: order_by.asc,
                        nulls_first: false,
                    });
                }
            }
        }

        // Otherwise bind normally
        Ok(BoundOrderBy {
            expr: self.bind_expr(&order_by.expr)?,
            asc: order_by.asc,
            nulls_first: false,
        })
    }

    /// Entry point to bind [WithStatement]
    fn bind_with(&mut self, stmt: &WithStatement) -> BinderResult<BoundWith> {
        // Bind each CTE and register it
        for (name, cte_query) in &stmt.ctes {
            let bound_query = self.bind_select(cte_query)?;
            let schema = bound_query.schema.clone();
            let idx = self.cte_queries.len();
            self.cte_queries.push(bound_query);
            self.ctes.insert(name.clone(), (idx, schema));
        }

        // Bind main body
        let body = self.bind_select(&stmt.body)?;
        let ctes = std::mem::take(&mut self.cte_queries);

        Ok(BoundWith {
            recursive: stmt.recursive,
            ctes,
            body: Box::new(body),
        })
    }

    /// Entry point to bind [InsertStatement]. Produces [BoundInsert] statements.
    fn bind_insert(&mut self, stmt: &InsertStatement) -> BinderResult<BoundInsert> {
        let relation = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;

        let table_id = relation.object_id();
        let table_schema = relation.schema().clone();

        // Resolve column indices
        // If user specifies columns, use those indices
        // If not, use all columns EXCEPT the internal row_id (column 0)
        let column_indices: Vec<usize> = if let Some(cols) = &stmt.columns {
            cols.iter()
                .map(|name| {
                    table_schema
                        .column_index
                        .get(name)
                        .copied()
                        .ok_or_else(|| BinderError::ColumnNotFound(name.clone()))
                })
                .collect::<BinderResult<_>>()?
        } else {
            // Skip column 0 (internal row_id) when user doesn't specify columns
            (1..table_schema.num_columns()).collect()
        };

        // Bind values
        let source = match &stmt.values {
            Values::Values(rows) => {
                let mut bound_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() != column_indices.len() {
                        return Err(BinderError::ColumnCountMismatch {
                            expected: column_indices.len(),
                            found: row.len(),
                        });
                    }
                    let bound_row: Vec<BoundExpression> = row
                        .iter()
                        .map(|e| self.bind_literal_expr(e))
                        .collect::<BinderResult<_>>()?;
                    bound_rows.push(bound_row);
                }
                BoundInsertSource::Values(bound_rows)
            }
            Values::Query(select) => {
                let bound_query = self.bind_select(select)?;
                if bound_query.columns.len() != column_indices.len() {
                    return Err(BinderError::ColumnCountMismatch {
                        expected: column_indices.len(),
                        found: bound_query.columns.len(),
                    });
                }
                BoundInsertSource::Query(Box::new(bound_query))
            }
        };

        Ok(BoundInsert {
            table_id,
            columns: column_indices,
            source,
            table_schema,
        })
    }

    /// Binds an [UpdateStatement] producing a [BoundUpdate] statement.
    fn bind_update(&mut self, stmt: &UpdateStatement) -> BinderResult<BoundUpdate> {
        let relation = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;

        let table_id = relation.object_id();
        let table_schema = relation.schema().clone();

        // Add table to scope for WHERE and SET expressions
        self.scopes.push();
        self.add_to_scope(ScopeEntry {
            ref_name: stmt.table.clone(),
            scope_index: 0,
            schema: table_schema.clone(),
            table_id: Some(table_id),
            cte_idx: None,
        })?;

        // Bind SET clauses
        let assignments: Vec<BoundAssignment> = stmt
            .set_clauses
            .iter()
            .map(|sc| {
                let col_idx = table_schema
                    .column_index
                    .get(&sc.column)
                    .copied()
                    .ok_or_else(|| BinderError::ColumnNotFound(sc.column.clone()))?;
                Ok(BoundAssignment {
                    column_idx: col_idx,
                    value: self.bind_expr(&sc.value)?,
                })
            })
            .collect::<BinderResult<_>>()?;

        // Bind WHERE
        let filter = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;

        self.scopes.pop();

        Ok(BoundUpdate {
            table_id,
            assignments,
            filter,
            table_schema,
        })
    }

    /// Binds a [DeleteStatement] producing a [BoundDelete] statement
    fn bind_delete(&mut self, stmt: &DeleteStatement) -> BinderResult<BoundDelete> {
        let relation = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;

        let table_id = relation.object_id();
        let table_schema = relation.schema().clone();

        // Add table to scope for WHERE
        self.scopes.push();
        self.add_to_scope(ScopeEntry {
            ref_name: stmt.table.clone(),
            scope_index: 0,
            schema: table_schema.clone(),
            table_id: Some(table_id),
            cte_idx: None,
        })?;

        let filter = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;

        self.scopes.pop();

        Ok(BoundDelete {
            table_id,
            filter,
            table_schema,
        })
    }

    /// Binds a [CreateTableStatement], producing a [BoundCreateTable] statement
    fn bind_create_table(&mut self, stmt: &CreateTableStatement) -> BinderResult<BoundCreateTable> {
        // Build temporary schema for constraint resolution
        let temp_columns: Vec<Column> = stmt
            .columns
            .iter()
            .map(|c| Column::new_with_defaults(c.data_type, &c.name))
            .collect();
        let temp_schema = Schema::new_table(temp_columns);

        let columns: Vec<BoundColumnDef> = stmt
            .columns
            .iter()
            .map(|c| self.bind_column_def(c))
            .collect::<BinderResult<_>>()?;

        let constraints: Vec<BoundTableConstraint> = stmt
            .constraints
            .iter()
            .map(|c| self.bind_table_constraint(c, &temp_schema))
            .collect::<BinderResult<_>>()?;

        Ok(BoundCreateTable {
            table_name: stmt.table.clone(),
            columns,
            constraints,
            if_not_exists: stmt.if_not_exists,
        })
    }

    /// Binds a column definition. Creating a [BoundColumnDef] statement
    fn bind_column_def(&mut self, col: &ColumnDefExpr) -> BinderResult<BoundColumnDef> {
        let default = col
            .default
            .as_ref()
            .map(|e| self.bind_literal_expr(e))
            .transpose()?;

        Ok(BoundColumnDef {
            name: col.name.clone(),
            data_type: col.data_type,
            is_non_null: col.is_non_null,
            is_unique: col.is_unique,
            default,
        })
    }

    /// Binds [TableConstraintExpr], creating [BoundTableConstraint] Expr
    fn bind_table_constraint(
        &mut self,
        ct: &TableConstraintExpr,
        schema: &Schema,
    ) -> BinderResult<BoundTableConstraint> {
        match ct {
            TableConstraintExpr::PrimaryKey(cols) => {
                let indices = self.resolve_column_indices(cols, schema)?;
                Ok(BoundTableConstraint::PrimaryKey(indices))
            }
            TableConstraintExpr::Unique(cols) => {
                let indices = self.resolve_column_indices(cols, schema)?;
                Ok(BoundTableConstraint::Unique(indices))
            }
            TableConstraintExpr::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                let col_indices = self.resolve_column_indices(columns, schema)?;

                let ref_relation = self
                    .catalog
                    .get_relation_by_name(ref_table, &self.tree_builder, &self.snapshot)
                    .map_err(|_| BinderError::TableNotFound(ref_table.clone()))?;

                let ref_indices =
                    self.resolve_column_indices(ref_columns, ref_relation.schema())?;

                Ok(BoundTableConstraint::ForeignKey {
                    columns: col_indices,
                    ref_table_id: ref_relation.object_id(),
                    ref_columns: ref_indices,
                })
            }
        }
    }

    /// Entry point to bind [CreateIndexStatement], producing [BoundCreateIndex]
    fn bind_create_index(&mut self, stmt: &CreateIndexStatement) -> BinderResult<BoundCreateIndex> {
        let relation = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;

        let table_id = relation.object_id();
        let schema = relation.schema();

        let columns: Vec<BoundIndexColumn> = stmt
            .columns
            .iter()
            .map(|c| {
                let col_idx = schema
                    .column_index
                    .get(&c.name)
                    .copied()
                    .ok_or_else(|| BinderError::ColumnNotFound(c.name.clone()))?;
                Ok(BoundIndexColumn {
                    column_idx: col_idx,
                    ascending: c
                        .order
                        .as_ref()
                        .map(|o| matches!(o, crate::sql::parser::ast::OrderDirection::Asc))
                        .unwrap_or(true),
                })
            })
            .collect::<BinderResult<_>>()?;

        Ok(BoundCreateIndex {
            index_name: stmt.name.clone(),
            table_id,
            columns,
            unique: stmt.unique,
            if_not_exists: stmt.if_not_exists,
        })
    }

    /// Entry point to bind [AlterTableStatement], producing [BoundAlterTable]
    fn bind_alter_table(&mut self, stmt: &AlterTableStatement) -> BinderResult<BoundAlterTable> {
        let relation = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;

        let table_id = relation.object_id();
        let schema = relation.schema();

        let action = match &stmt.action {
            AlterAction::AddColumn(col) => BoundAlterAction::AddColumn(self.bind_column_def(col)?),
            AlterAction::DropColumn(name) => {
                let col_idx = schema
                    .column_index
                    .get(name)
                    .copied()
                    .ok_or_else(|| BinderError::ColumnNotFound(name.clone()))?;
                BoundAlterAction::DropColumn(col_idx)
            }
            AlterAction::AlterColumn(alter) => {
                let col_idx = schema
                    .column_index
                    .get(&alter.name)
                    .copied()
                    .ok_or_else(|| BinderError::ColumnNotFound(alter.name.clone()))?;

                let (new_type, set_default, drop_default, set_not_null, drop_not_null) =
                    match &alter.action {
                        AlterColumnAction::SetDataType(dt) => {
                            (Some(*dt), None, false, false, false)
                        }
                        AlterColumnAction::SetDefault(e) => {
                            (None, Some(self.bind_literal_expr(e)?), false, false, false)
                        }
                        AlterColumnAction::DropDefault => (None, None, true, false, false),
                        AlterColumnAction::SetNotNull => (None, None, false, true, false),
                        AlterColumnAction::DropNotNull => (None, None, false, false, true),
                    };

                BoundAlterAction::AlterColumn {
                    column_idx: col_idx,
                    new_type,
                    set_default,
                    drop_default,
                    set_not_null,
                    drop_not_null,
                }
            }
            AlterAction::AddConstraint(ct) => {
                BoundAlterAction::AddConstraint(self.bind_table_constraint(ct, schema)?)
            }
        };

        Ok(BoundAlterTable { table_id, action })
    }

    /// Entry point to bind [DropTableStatement], producing [BoundDropTable]
    fn bind_drop_table(&mut self, stmt: &DropTableStatement) -> BinderResult<BoundDropTable> {
        let table_id = self
            .catalog
            .get_relation_by_name(&stmt.table, &self.tree_builder, &self.snapshot)
            .ok()
            .map(|r| r.object_id());

        if table_id.is_none() && !stmt.if_exists {
            return Err(BinderError::TableNotFound(stmt.table.clone()));
        }

        Ok(BoundDropTable {
            table_id,
            table_name: stmt.table.clone(),
            if_exists: stmt.if_exists,
            cascade: stmt.cascade,
        })
    }

    fn bind_transaction(&self, stmt: &TransactionStatement) -> BinderResult<BoundTransaction> {
        Ok(match stmt {
            TransactionStatement::Begin => BoundTransaction::Begin,
            TransactionStatement::Commit => BoundTransaction::Commit,
            TransactionStatement::Rollback => BoundTransaction::Rollback,
        })
    }

    /// Binds an expression, producing [BoundExpression]
    fn bind_expr(&mut self, expr: &Expr) -> BinderResult<BoundExpression> {
        match expr {
            Expr::Number(n) => Ok(self.bind_number(*n)),
            Expr::String(s) => Ok(BoundExpression::Literal {
                value: DataType::Blob(s.as_str().into()),
            }),
            Expr::Boolean(b) => Ok(BoundExpression::Literal {
                value: DataType::Bool((*b).into()),
            }),
            Expr::Null => Ok(BoundExpression::Literal {
                value: DataType::Null,
            }),
            Expr::Star => Ok(BoundExpression::Star),

            Expr::Identifier(name) => {
                let resolved = self.scopes.resolve_column(name, None)?;
                Ok(BoundExpression::ColumnRef(BoundColumnRef {
                    table_id: resolved.table_id,
                    scope_index: resolved.scope_index,
                    column_idx: resolved.column_idx,
                    data_type: resolved.data_type,
                }))
            }

            Expr::QualifiedIdentifier { table, column } => {
                let resolved = self.scopes.resolve_column(column, Some(table))?;
                Ok(BoundExpression::ColumnRef(BoundColumnRef {
                    table_id: resolved.table_id,
                    scope_index: resolved.scope_index,
                    column_idx: resolved.column_idx,
                    data_type: resolved.data_type,
                }))
            }

            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, *op, right),

            Expr::UnaryOp { op, expr } => {
                let inner = self.bind_expr(expr)?;
                let result_type = self.infer_unary_type(*op, inner.data_type());
                Ok(BoundExpression::UnaryOp {
                    op: *op,
                    expr: Box::new(inner),
                    result_type,
                })
            }

            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => self.bind_function_call(name, args, *distinct),

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let bound_operand = operand
                    .as_ref()
                    .map(|o| self.bind_expr(o))
                    .transpose()?
                    .map(Box::new);

                let when_then: Vec<(BoundExpression, BoundExpression)> = when_clauses
                    .iter()
                    .map(|wc| Ok((self.bind_expr(&wc.condition)?, self.bind_expr(&wc.result)?)))
                    .collect::<BinderResult<_>>()?;

                let else_expr = else_clause
                    .as_ref()
                    .map(|e| self.bind_expr(e))
                    .transpose()?
                    .map(Box::new);

                let result_type = when_then
                    .first()
                    .map(|(_, t)| t.data_type())
                    .unwrap_or(DataTypeKind::Null);

                Ok(BoundExpression::Case {
                    operand: bound_operand,
                    when_then,
                    else_expr,
                    result_type,
                })
            }

            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(BoundExpression::Between {
                expr: Box::new(self.bind_expr(expr)?),
                low: Box::new(self.bind_expr(low)?),
                high: Box::new(self.bind_expr(high)?),
                negated: *negated,
            }),

            Expr::Subquery(select) => {
                let bound_select = self.bind_select(select)?;
                let result_type = bound_select
                    .schema
                    .iter_columns()
                    .next()
                    .map(|c| c.datatype())
                    .unwrap_or(DataTypeKind::Null);
                Ok(BoundExpression::Subquery {
                    query: Box::new(bound_select),
                    result_type,
                })
            }

            Expr::Exists(select) => {
                let bound_select = self.bind_select(select)?;
                Ok(BoundExpression::Exists {
                    query: Box::new(bound_select),
                    negated: false,
                })
            }

            Expr::List(_) => Err(BinderError::InvalidExpression(
                "standalone list".to_string(),
            )),
        }
    }

    fn bind_binary_op(
        &mut self,
        left: &Expr,
        op: BinaryOperator,
        right: &Expr,
    ) -> BinderResult<BoundExpression> {
        // Handle special operators
        match op {
            BinaryOperator::In | BinaryOperator::NotIn => {
                let left_expr = self.bind_expr(left)?;
                let negated = matches!(op, BinaryOperator::NotIn);

                if let Expr::List(items) = right {
                    let list = items
                        .iter()
                        .map(|e| self.bind_expr(e))
                        .collect::<BinderResult<_>>()?;
                    return Ok(BoundExpression::InList {
                        expr: Box::new(left_expr),
                        list,
                        negated,
                    });
                } else if let Expr::Subquery(subq) = right {
                    let bound_subq = self.bind_select(subq)?;
                    return Ok(BoundExpression::InSubquery {
                        expr: Box::new(left_expr),
                        query: Box::new(bound_subq),
                        negated,
                    });
                }
            }

            BinaryOperator::Is | BinaryOperator::IsNot => {
                let left_expr = self.bind_expr(left)?;
                if matches!(right, Expr::Null) {
                    return Ok(BoundExpression::IsNull {
                        expr: Box::new(left_expr),
                        negated: matches!(op, BinaryOperator::IsNot),
                    });
                }
            }

            _ => {}
        }

        // Normal binary operation
        let left_bound = self.bind_expr(left)?;
        let right_bound = self.bind_expr(right)?;
        let result_type =
            self.infer_binary_type(op, left_bound.data_type(), right_bound.data_type());

        Ok(BoundExpression::BinaryOp {
            left: Box::new(left_bound),
            op,
            right: Box::new(right_bound),
            result_type,
        })
    }

    /// Binds a function call
    fn bind_function_call(
        &mut self,
        name: &str,
        args: &[Expr],
        distinct: bool,
    ) -> BinderResult<BoundExpression> {
        let upper = name.to_uppercase();

        // Check if it's an aggregate
        if let Some(agg_func) = self.try_parse_aggregate(&upper) {
            let arg = args
                .first()
                .map(|a| self.bind_expr(a))
                .transpose()?
                .map(Box::new);

            let return_type = self.infer_aggregate_type(agg_func, arg.as_deref());

            return Ok(BoundExpression::Aggregate {
                func: agg_func,
                arg,
                distinct,
                return_type,
            });
        }

        // Scalar function
        let bound_args: Vec<BoundExpression> = args
            .iter()
            .map(|a| self.bind_expr(a))
            .collect::<BinderResult<_>>()?;

        let func = self.resolve_scalar_function(&upper);
        let return_type = self.infer_function_type(func.clone(), &bound_args);

        Ok(BoundExpression::Function {
            func,
            args: bound_args,
            distinct,
            return_type,
        })
    }

    /// Bind expressions that don't require table scope (literals, constants)
    fn bind_literal_expr(&mut self, expr: &Expr) -> BinderResult<BoundExpression> {
        match expr {
            Expr::Number(n) => Ok(self.bind_number(*n)),
            Expr::String(s) => Ok(BoundExpression::Literal {
                value: DataType::Blob(s.as_str().into()),
            }),
            Expr::Boolean(b) => Ok(BoundExpression::Literal {
                value: DataType::Bool((*b).into()),
            }),
            Expr::Null => Ok(BoundExpression::Literal {
                value: DataType::Null,
            }),
            Expr::UnaryOp { op, expr } => {
                let inner = self.bind_literal_expr(expr)?;
                let result_type = self.infer_unary_type(*op, inner.data_type());
                Ok(BoundExpression::UnaryOp {
                    op: *op,
                    expr: Box::new(inner),
                    result_type,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let left_bound = self.bind_literal_expr(left)?;
                let right_bound = self.bind_literal_expr(right)?;
                let result_type =
                    self.infer_binary_type(*op, left_bound.data_type(), right_bound.data_type());
                Ok(BoundExpression::BinaryOp {
                    left: Box::new(left_bound),
                    op: *op,
                    right: Box::new(right_bound),
                    result_type,
                })
            }
            _ => Err(BinderError::InvalidExpression(
                "column reference not allowed in this context".to_string(),
            )),
        }
    }

    fn bind_number(&self, n: f64) -> BoundExpression {
        let is_int = n.fract() == 0.0;

        let value = if is_int && n >= i32::MIN as f64 && n <= i32::MAX as f64 {
            DataType::Int((n as i32).into())
        } else if is_int && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            DataType::BigInt((n as i64).into())
        } else {
            DataType::Double(n.into())
        };

        BoundExpression::Literal { value }
    }

    // ========================================================================
    // Type inference helpers
    // ========================================================================

    fn try_parse_aggregate(&self, name: &str) -> Option<AggregateFunction> {
        match name {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            _ => None,
        }
    }

    fn resolve_scalar_function(&self, name: &str) -> ScalarFunction {
        match name {
            "LENGTH" | "CHAR_LENGTH" => ScalarFunction::Length,
            "UPPER" => ScalarFunction::Upper,
            "LOWER" => ScalarFunction::Lower,
            "LTRIM" => ScalarFunction::LTrim,
            "RTRIM" => ScalarFunction::RTrim,
            "CONCAT" => ScalarFunction::Concat,
            "ABS" => ScalarFunction::Abs,
            "ROUND" => ScalarFunction::Round,
            "CEIL" | "CEILING" => ScalarFunction::Ceil,
            "FLOOR" => ScalarFunction::Floor,
            "SQRT" => ScalarFunction::Sqrt,
            "COALESCE" => ScalarFunction::Coalesce,
            "NULLIF" => ScalarFunction::NullIf,
            _ => ScalarFunction::Unknown(name.to_string()),
        }
    }

    fn infer_binary_type(
        &self,
        op: BinaryOperator,
        left: DataTypeKind,
        right: DataTypeKind,
    ) -> DataTypeKind {
        match op {
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
            | BinaryOperator::IsNot => DataTypeKind::Bool,

            BinaryOperator::Concat => DataTypeKind::Blob,

            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => self.wider_numeric(left, right),
        }
    }

    fn infer_unary_type(&self, op: UnaryOperator, inner: DataTypeKind) -> DataTypeKind {
        match op {
            UnaryOperator::Not => DataTypeKind::Bool,
            UnaryOperator::Plus | UnaryOperator::Minus => inner,
        }
    }

    fn infer_aggregate_type(
        &self,
        func: AggregateFunction,
        arg: Option<&BoundExpression>,
    ) -> DataTypeKind {
        match func {
            AggregateFunction::Count => DataTypeKind::BigInt,
            AggregateFunction::Sum | AggregateFunction::Avg => DataTypeKind::Double,
            AggregateFunction::Min | AggregateFunction::Max => {
                arg.map(|a| a.data_type()).unwrap_or(DataTypeKind::Null)
            }
        }
    }

    fn infer_function_type(&self, func: ScalarFunction, args: &[BoundExpression]) -> DataTypeKind {
        match func {
            ScalarFunction::Length => DataTypeKind::Int,
            ScalarFunction::Upper
            | ScalarFunction::Lower
            | ScalarFunction::RTrim
            | ScalarFunction::LTrim
            | ScalarFunction::Concat => DataTypeKind::Blob,
            ScalarFunction::Abs
            | ScalarFunction::Round
            | ScalarFunction::Ceil
            | ScalarFunction::Floor
            | ScalarFunction::Sqrt => DataTypeKind::Double,
            ScalarFunction::Coalesce | ScalarFunction::NullIf | ScalarFunction::Cast => args
                .first()
                .map(|a| a.data_type())
                .unwrap_or(DataTypeKind::Null),
            ScalarFunction::Unknown(_) => DataTypeKind::Blob,
        }
    }

    fn wider_numeric(&self, a: DataTypeKind, b: DataTypeKind) -> DataTypeKind {
        match (a, b) {
            (DataTypeKind::Double, _) | (_, DataTypeKind::Double) => DataTypeKind::Double,
            (DataTypeKind::BigInt, _) | (_, DataTypeKind::BigInt) => DataTypeKind::BigInt,
            (DataTypeKind::Int, _) | (_, DataTypeKind::Int) => DataTypeKind::Int,
            _ => a,
        }
    }

    fn add_to_scope(&mut self, entry: ScopeEntry) -> BinderResult<()> {
        if let Some(scope) = self.scopes.current_mut() {
            scope.add_entry(entry)?;
        }
        Ok(())
    }

    fn current_scope_size(&self) -> usize {
        self.scopes.current().map_or(0, |s| s.iter().count())
    }

    /// Helper to merge the  schema of two tables.
    fn merge_schemas(&self, left: &Schema, right: &Schema) -> Schema {
        let mut cols: Vec<Column> = left.iter_columns().cloned().collect();
        cols.extend(right.iter_columns().cloned());
        Schema::new_table(cols)
    }

    /// Build the select schema of a select statement, given a set of [BoundSelectItem]
    fn build_select_schema(&self, columns: &[BoundSelectItem]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|item| Column::new_with_defaults(item.expr.data_type(), &item.output_name))
            .collect();
        Schema::new_table(cols)
    }

    /// Resolves column names into column indices.
    fn resolve_column_indices(
        &self,
        names: &[String],
        schema: &Schema,
    ) -> BinderResult<Vec<usize>> {
        names
            .iter()
            .map(|name| {
                schema
                    .column_index
                    .get(name)
                    .copied()
                    .ok_or_else(|| BinderError::ColumnNotFound(name.clone()))
            })
            .collect()
    }

    fn infer_output_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Identifier(name) => name.clone(),
            Expr::QualifiedIdentifier { column, .. } => column.clone(),
            Expr::FunctionCall { name, .. } => name.to_lowercase(),
            _ => "?column?".to_string(),
        }
    }
}
