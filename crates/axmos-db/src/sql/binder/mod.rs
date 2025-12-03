//! # Binder Module.
//! Performs mainly name and type resolution, transforming raw SQL statements into BoundStatements.
mod ast;
use crate::{
    OBJECT_ZERO,
    database::{
        SharedCatalog,
        errors::{BinderError, BinderResult},
        schema::{Column, Schema},
    },
    sql::{
        ast::{
            AlterAction as AstAlterAction, AlterColumnAction, BinaryOperator, ColumnConstraintExpr,
            ColumnDefExpr, Expr, OrderByExpr, SelectItem, SelectStatement, Statement,
            TableConstraintExpr, TableReference, TransactionStatement, UnaryOperator, Values,
            WithStatement,
        },
        binder::ast::*,
    },
    transactions::worker::Worker,
    types::{Blob, DataType, DataTypeKind, ObjectId},
};

use std::collections::HashMap;

#[derive(Debug, Clone)]
struct ScopeEntry {
    table_id: Option<ObjectId>,
    ref_name: String,
    table_idx: usize,
    schema: Schema,
    cte_idx: Option<usize>,
}

impl ScopeEntry {
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_column(&self, column_name: &str) -> Option<BoundColumnRef> {
        if let Some(idx) = self.get_schema().column_idx(column_name) {
            let data_type = self.get_schema().columns[*idx].dtype;
            return Some(BoundColumnRef {
                table_idx: self.table_idx,
                column_idx: *idx,
                data_type,
            });
        }
        None
    }
}

#[derive(Debug, Clone, Default)]
struct BinderScope {
    entries: HashMap<String, ScopeEntry>,
    entry_order: Vec<String>,
}

impl BinderScope {
    fn new() -> Self {
        Self::default()
    }

    /// Adds an entry to the scope of this binder.
    /// Entries are indexed by alias in the [entries] HashMap.
    fn add(&mut self, mut entry: ScopeEntry) -> BinderResult<()> {
        let ref_name = entry.ref_name.clone();
        if self.entries.contains_key(&ref_name) {
            return Err(BinderError::DuplicateAlias(ref_name));
        }
        entry.table_idx = self.entry_order.len();
        self.entry_order.push(ref_name.clone());
        self.entries.insert(ref_name, entry);
        Ok(())
    }

    /// Gets a reference to an entry in the scope given its name
    fn get(&self, name: &str) -> Option<&ScopeEntry> {
        self.entries.get(name)
    }

    /// Iterates over the entries in sorted order.
    fn iter(&self) -> impl Iterator<Item = &ScopeEntry> {
        self.entry_order
            .iter()
            .filter_map(|name| self.entries.get(name))
    }

    /// Finds a column in the scope.
    fn find_column(&self, column_name: &str) -> BinderResult<Option<BoundColumnRef>> {
        let mut found: Option<BoundColumnRef> = None;
        for entry in self.iter() {
            if let Some(found_col) = entry.get_column(column_name) {
                if found.is_some() {
                    return Err(BinderError::AmbiguousColumn(column_name.to_string()));
                }
                found = Some(found_col);
            }
        }

        Ok(found)
    }

    /// Faster access method to find a column given its table and the column name in the scope.
    fn find_qualified_column(
        &self,
        table: &str,
        column: &str,
    ) -> BinderResult<Option<BoundColumnRef>> {
        let entry = match self.get(table) {
            Some(e) => e,
            None => return Ok(None),
        };
        Ok(entry.get_column(column))
    }
}

/// Context for the main binder data structure.
/// The binder contains a stack of [Scope] object.
/// Every time you enter a scope, you push it to the stack. Then you keep adding objects to the scope until you get out of it.
/// As CTEs are more global like objects they go to a separate tracking data structure.
#[derive(Debug)]
struct BinderContext {
    scopes: Vec<BinderScope>,
    ctes: HashMap<String, (usize, Schema)>,
    cte_queries: Vec<BoundSelect>,
}

impl BinderContext {
    fn new() -> Self {
        Self {
            scopes: vec![BinderScope::new()],
            ctes: HashMap::new(),
            cte_queries: Vec::new(),
        }
    }

    fn push_scope(&mut self) {
        self.scopes.push(BinderScope::new());
    }
    fn pop_scope(&mut self) {
        self.scopes.pop();
    }
    fn current_scope(&self) -> &BinderScope {
        self.scopes.last().expect("No scope")
    }
    fn current_scope_mut(&mut self) -> &mut BinderScope {
        self.scopes.last_mut().expect("No scope")
    }
    fn add_to_scope(&mut self, entry: ScopeEntry) -> BinderResult<()> {
        self.current_scope_mut().add(entry)
    }

    fn add_cte(&mut self, name: String, query: BoundSelect) -> usize {
        let idx = self.cte_queries.len();
        let schema = query.schema.clone();
        self.cte_queries.push(query);
        self.ctes.insert(name, (idx, schema));
        idx
    }

    fn get_cte(&self, name: &str) -> Option<(usize, &Schema)> {
        self.ctes.get(name).map(|(idx, schema)| (*idx, schema))
    }

    fn take_cte_queries(&mut self) -> Vec<BoundSelect> {
        std::mem::take(&mut self.cte_queries)
    }

    fn find_column(&self, column: &str) -> BinderResult<BoundColumnRef> {
        for scope in self.scopes.iter().rev() {
            if let Some(col_ref) = scope.find_column(column)? {
                return Ok(col_ref);
            }
        }
        Err(BinderError::ColumnNotFound(column.to_string()))
    }

    fn find_qualified_column(&self, table: &str, column: &str) -> BinderResult<BoundColumnRef> {
        for scope in self.scopes.iter().rev() {
            if let Some(col_ref) = scope.find_qualified_column(table, column)? {
                return Ok(col_ref);
            }
        }
        Err(BinderError::ColumnNotFound(format!("{}.{}", table, column)))
    }
}

pub struct Binder {
    catalog: SharedCatalog,
    worker: Worker,
    ctx: BinderContext,
}

impl Binder {
    pub fn new(catalog: SharedCatalog, worker: Worker) -> Self {
        Self {
            catalog,
            worker,
            ctx: BinderContext::new(),
        }
    }

    pub fn bind(&mut self, stmt: &Statement) -> BinderResult<BoundStatement> {
        self.ctx = BinderContext::new();
        self.bind_statement(stmt)
    }

    fn bind_statement(&mut self, stmt: &Statement) -> BinderResult<BoundStatement> {
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
        let from = stmt
            .from
            .as_ref()
            .map(|t| self.bind_table_ref(t))
            .transpose()?;
        let where_clause = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;
        let group_by = stmt
            .group_by
            .iter()
            .map(|e| self.bind_expr(e))
            .collect::<BinderResult<_>>()?;
        let having = stmt
            .having
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;
        let columns = self.bind_select_items(&stmt.columns)?;
        let schema = self.build_select_schema(&columns);
        let order_by = stmt
            .order_by
            .iter()
            .map(|o| self.bind_order_by(o))
            .collect::<BinderResult<_>>()?;

        Ok(BoundSelect {
            distinct: stmt.distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit: stmt.limit,
            offset: None,
            schema,
        })
    }

    fn bind_table_ref(&mut self, table_ref: &TableReference) -> BinderResult<BoundTableRef> {
        match table_ref {
            TableReference::Table { name, alias } => {
                if let Some((cte_idx, schema)) = self.ctx.get_cte(name) {
                    let schema = schema.clone();
                    let ref_name = alias.as_deref().unwrap_or(name).to_string();
                    self.ctx.add_to_scope(ScopeEntry {
                        table_id: None,
                        ref_name,
                        table_idx: 0,
                        schema: schema.clone(),
                        cte_idx: Some(cte_idx),
                    })?;
                    return Ok(BoundTableRef::Cte { cte_idx, schema });
                }

                let relation = self
                    .catalog
                    .get_relation(name, self.worker.clone())
                    .map_err(|_| BinderError::TableNotFound(name.clone()))?;
                let table_id = relation.id();
                let schema = relation.schema().clone();
                let ref_name = alias.as_deref().unwrap_or(name).to_string();
                self.ctx.add_to_scope(ScopeEntry {
                    table_id: Some(table_id),
                    ref_name,
                    table_idx: 0,
                    schema: schema.clone(),
                    cte_idx: None,
                })?;
                Ok(BoundTableRef::BaseTable { table_id, schema })
            }
            TableReference::Join {
                left,
                join_type,
                right,
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
                self.ctx.push_scope();
                let bound_query = self.bind_select(query)?;
                let schema = bound_query.schema.clone();
                self.ctx.pop_scope();
                self.ctx.add_to_scope(ScopeEntry {
                    table_id: None,
                    ref_name: alias.clone(),
                    table_idx: 0,
                    schema: schema.clone(),
                    cte_idx: None,
                })?;
                Ok(BoundTableRef::Subquery {
                    query: Box::new(bound_query),
                    schema,
                })
            }
        }
    }

    fn bind_select_items(&mut self, items: &[SelectItem]) -> BinderResult<Vec<BoundSelectItem>> {
        let mut result = Vec::new();
        let mut output_idx = 0;
        for item in items {
            match item {
                SelectItem::Star => {
                    for entry in self.ctx.current_scope().iter() {
                        for (col_idx, col) in entry.schema.columns().iter().enumerate() {
                            result.push(BoundSelectItem {
                                expr: BoundExpression::ColumnRef(BoundColumnRef {
                                    table_idx: entry.table_idx,
                                    column_idx: col_idx,
                                    data_type: col.dtype,
                                }),
                                output_idx,
                            });
                            output_idx += 1;
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    result.push(BoundSelectItem {
                        expr: self.bind_expr(expr)?,
                        output_idx,
                    });
                    output_idx += 1;
                }
            }
        }
        Ok(result)
    }

    fn bind_order_by(&mut self, o: &OrderByExpr) -> BinderResult<BoundOrderBy> {
        Ok(BoundOrderBy {
            expr: self.bind_expr(&o.expr)?,
            asc: o.asc,
            nulls_first: false,
        })
    }

    fn build_select_schema(&self, columns: &[BoundSelectItem]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .enumerate()
            .map(|(i, item)| {
                Column::new_unindexed(item.expr.data_type(), &format!("col_{}", i), None)
            })
            .collect();
        Schema::from_columns(&cols, 0)
    }

    fn bind_with(&mut self, stmt: &WithStatement) -> BinderResult<BoundWith> {
        for (name, cte_query) in &stmt.ctes {
            self.ctx.push_scope();
            let bound_query = self.bind_select(cte_query)?;
            self.ctx.pop_scope();
            self.ctx.add_cte(name.clone(), bound_query);
        }
        let body = self.bind_select(&stmt.body)?;
        let ctes = self.ctx.take_cte_queries();
        Ok(BoundWith {
            recursive: stmt.recursive,
            ctes,
            body: Box::new(body),
        })
    }

    fn bind_insert(
        &mut self,
        stmt: &crate::sql::ast::InsertStatement,
    ) -> BinderResult<BoundInsert> {
        let relation = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;
        let table_id = relation.id();
        let table_schema = relation.schema().clone();

        let column_indices = if let Some(cols) = &stmt.columns {
            let mut indices: Vec<usize> = cols
                .iter()
                .map(|n| {
                    table_schema
                        .columns()
                        .iter()
                        .position(|c| c.name == *n)
                        .ok_or_else(|| BinderError::ColumnNotFound(n.clone()))
                })
                .collect::<BinderResult<_>>()?;
            indices.sort();
            indices
        } else {
            (0..table_schema.columns().len()).collect()
        };

        let source = match &stmt.values {
            Values::Values(rows) => {
                let original_to_physical: HashMap<usize, usize> = if let Some(cols) = &stmt.columns
                {
                    cols.iter()
                        .enumerate()
                        .filter_map(|(orig_idx, col_name)| {
                            let phys_idx = table_schema
                                .columns()
                                .iter()
                                .position(|c| c.name == *col_name)?;
                            let sorted_pos =
                                column_indices.iter().position(|&idx| idx == phys_idx)?;
                            Some((orig_idx, sorted_pos))
                        })
                        .collect()
                } else {
                    (0..table_schema.columns().len()).map(|i| (i, i)).collect()
                };

                let mut bound_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() != column_indices.len() {
                        return Err(BinderError::ColumnCountMismatch {
                            expected: column_indices.len(),
                            found: row.len(),
                        });
                    }
                    let mut bound_row: Vec<Option<BoundExpression>> =
                        vec![None; column_indices.len()];
                    for (orig_idx, expr) in row.iter().enumerate() {
                        if let Some(&sorted_pos) = original_to_physical.get(&orig_idx) {
                            bound_row[sorted_pos] = Some(self.bind_expr(expr)?);
                        }
                    }
                    bound_rows.push(
                        bound_row
                            .into_iter()
                            .enumerate()
                            .map(|(i, opt)| {
                                opt.ok_or_else(|| {
                                    BinderError::Internal(format!("Missing value at {}", i))
                                })
                            })
                            .collect::<BinderResult<_>>()?,
                    );
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

    fn bind_update(
        &mut self,
        stmt: &crate::sql::ast::UpdateStatement,
    ) -> BinderResult<BoundUpdate> {
        let relation = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;
        let table_id = relation.id();
        let table_schema = relation.schema().clone();

        self.ctx.add_to_scope(ScopeEntry {
            table_id: Some(table_id),
            ref_name: stmt.table.clone(),
            table_idx: 0,
            schema: table_schema.clone(),
            cte_idx: None,
        })?;

        let assignments = stmt
            .set_clauses
            .iter()
            .map(|sc| {
                let col_idx = table_schema
                    .columns()
                    .iter()
                    .position(|c| c.name == sc.column)
                    .ok_or_else(|| BinderError::ColumnNotFound(sc.column.clone()))?;
                Ok(BoundAssignment {
                    column_idx: col_idx,
                    value: self.bind_expr(&sc.value)?,
                })
            })
            .collect::<BinderResult<_>>()?;

        let filter = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;
        Ok(BoundUpdate {
            table_id,
            assignments,
            filter,
            table_schema,
        })
    }

    fn bind_delete(
        &mut self,
        stmt: &crate::sql::ast::DeleteStatement,
    ) -> BinderResult<BoundDelete> {
        let relation = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;
        let table_id = relation.id();
        let table_schema = relation.schema().clone();

        self.ctx.add_to_scope(ScopeEntry {
            table_id: Some(table_id),
            ref_name: stmt.table.clone(),
            table_idx: 0,
            schema: table_schema.clone(),
            cte_idx: None,
        })?;

        let filter = stmt
            .where_clause
            .as_ref()
            .map(|e| self.bind_expr(e))
            .transpose()?;
        Ok(BoundDelete {
            table_id,
            filter,
            table_schema,
        })
    }

    fn bind_create_table(
        &mut self,
        stmt: &crate::sql::ast::CreateTableStatement,
    ) -> BinderResult<BoundCreateTable> {
        let temp_cols: Vec<Column> = stmt
            .columns
            .iter()
            .map(|c| Column::new_unindexed(c.data_type, &c.name, None))
            .collect();
        let temp_schema = Schema::from_columns(&temp_cols, 0);

        let columns = stmt
            .columns
            .iter()
            .map(|col| self.bind_column_def(col))
            .collect::<BinderResult<_>>()?;
        let constraints = stmt
            .constraints
            .iter()
            .map(|ct| self.bind_table_constraint(ct, &temp_schema))
            .collect::<BinderResult<_>>()?;
        Ok(BoundCreateTable {
            table_name: stmt.table.clone(),
            columns,
            constraints,
            if_not_exists: false,
        })
    }

    fn bind_column_def(&mut self, col_def: &ColumnDefExpr) -> BinderResult<BoundColumnDef> {
        let constraints = col_def
            .constraints
            .iter()
            .map(|ct| self.bind_column_constraint(ct))
            .collect::<BinderResult<_>>()?;
        Ok(BoundColumnDef {
            name: col_def.name.clone(),
            data_type: col_def.data_type,
            constraints,
        })
    }

    fn bind_column_constraint(
        &mut self,
        ct: &ColumnConstraintExpr,
    ) -> BinderResult<BoundColumnConstraint> {
        match ct {
            ColumnConstraintExpr::PrimaryKey => Ok(BoundColumnConstraint::PrimaryKey),
            ColumnConstraintExpr::NotNull => Ok(BoundColumnConstraint::NotNull),
            ColumnConstraintExpr::Unique => Ok(BoundColumnConstraint::Unique),
            ColumnConstraintExpr::ForeignKey { table, column } => {
                let relation = self
                    .catalog
                    .get_relation(table, self.worker.clone())
                    .map_err(|_| BinderError::TableNotFound(table.clone()))?;
                let ref_col_idx = relation
                    .schema()
                    .columns()
                    .iter()
                    .position(|c| c.name == *column)
                    .ok_or_else(|| BinderError::ColumnNotFound(column.clone()))?;
                Ok(BoundColumnConstraint::ForeignKey {
                    ref_table_id: relation.id(),
                    ref_column_idx: ref_col_idx,
                })
            }
            ColumnConstraintExpr::Default(expr) => {
                Ok(BoundColumnConstraint::Default(self.bind_expr(expr)?))
            }
            ColumnConstraintExpr::Check(expr) => {
                Ok(BoundColumnConstraint::Check(self.bind_expr(expr)?))
            }
        }
    }

    fn bind_table_constraint(
        &mut self,
        ct: &TableConstraintExpr,
        schema: &Schema,
    ) -> BinderResult<BoundTableConstraint> {
        match ct {
            TableConstraintExpr::PrimaryKey(cols) => Ok(BoundTableConstraint::PrimaryKey(
                self.resolve_column_names(cols, schema)?,
            )),
            TableConstraintExpr::Unique(cols) => Ok(BoundTableConstraint::Unique(
                self.resolve_column_names(cols, schema)?,
            )),
            TableConstraintExpr::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                let col_indices = self.resolve_column_names(columns, schema)?;
                let relation = self
                    .catalog
                    .get_relation(ref_table, self.worker.clone())
                    .map_err(|_| BinderError::TableNotFound(ref_table.clone()))?;
                let ref_indices = self.resolve_column_names(ref_columns, relation.schema())?;
                Ok(BoundTableConstraint::ForeignKey {
                    columns: col_indices,
                    ref_table_id: relation.id(),
                    ref_columns: ref_indices,
                })
            }
            TableConstraintExpr::Check(expr) => {
                self.ctx.add_to_scope(ScopeEntry {
                    table_id: None,
                    ref_name: String::new(),
                    table_idx: 0,
                    schema: schema.clone(),
                    cte_idx: None,
                })?;
                Ok(BoundTableConstraint::Check(self.bind_expr(expr)?))
            }
        }
    }

    fn resolve_column_names(&self, names: &[String], schema: &Schema) -> BinderResult<Vec<usize>> {
        names
            .iter()
            .map(|name| {
                schema
                    .columns()
                    .iter()
                    .position(|c| c.name == *name)
                    .ok_or_else(|| BinderError::ColumnNotFound(name.clone()))
            })
            .collect()
    }

    fn bind_create_index(
        &mut self,
        stmt: &crate::sql::ast::CreateIndexStatement,
    ) -> BinderResult<BoundCreateIndex> {
        let relation = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;
        let table_id = relation.id();
        let table_schema = relation.schema();

        let columns = stmt
            .columns
            .iter()
            .map(|col| {
                let col_idx = table_schema
                    .columns()
                    .iter()
                    .position(|c| c.name == col.name)
                    .ok_or_else(|| BinderError::ColumnNotFound(col.name.clone()))?;
                Ok(BoundIndexColumn {
                    column_idx: col_idx,
                    ascending: col
                        .order
                        .as_ref()
                        .map(|o| matches!(o, crate::sql::ast::OrderDirection::Asc))
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

    fn bind_alter_table(
        &mut self,
        stmt: &crate::sql::ast::AlterTableStatement,
    ) -> BinderResult<BoundAlterTable> {
        let relation = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .map_err(|_| BinderError::TableNotFound(stmt.table.clone()))?;
        let table_id = relation.id();
        let table_schema = relation.schema();

        let action = match &stmt.action {
            AstAlterAction::AddColumn(col_def) => {
                BoundAlterAction::AddColumn(self.bind_column_def(col_def)?)
            }
            AstAlterAction::DropColumn(name) => {
                let col_idx = table_schema
                    .columns()
                    .iter()
                    .position(|c| c.name == *name)
                    .ok_or_else(|| BinderError::ColumnNotFound(name.clone()))?;
                BoundAlterAction::DropColumn(col_idx)
            }
            AstAlterAction::AlterColumn(alter_col) => {
                let col_idx = table_schema
                    .columns()
                    .iter()
                    .position(|c| c.name == alter_col.name)
                    .ok_or_else(|| BinderError::ColumnNotFound(alter_col.name.clone()))?;
                let (new_type, set_default, drop_default, set_not_null, drop_not_null) =
                    match &alter_col.action {
                        AlterColumnAction::SetDataType(dt) => {
                            (Some(*dt), None, false, false, false)
                        }
                        AlterColumnAction::SetDefault(expr) => {
                            (None, Some(self.bind_expr(expr)?), false, false, false)
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
            AstAlterAction::AddConstraint(ct) => {
                BoundAlterAction::AddConstraint(self.bind_table_constraint(ct, table_schema)?)
            }
            AstAlterAction::DropConstraint(name) => BoundAlterAction::DropConstraint(name.clone()),
        };
        Ok(BoundAlterTable { table_id, action })
    }

    fn bind_drop_table(
        &mut self,
        stmt: &crate::sql::ast::DropTableStatement,
    ) -> BinderResult<BoundDropTable> {
        let table_id = self
            .catalog
            .get_relation(&stmt.table, self.worker.clone())
            .ok()
            .map(|r| r.id());

        if table_id.is_none() && !stmt.if_exists {
            return Err(BinderError::TableNotFound(stmt.table.clone()));
        }
        Ok(BoundDropTable {
            table_id: table_id.unwrap_or(OBJECT_ZERO),
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

    fn bind_expr(&mut self, expr: &Expr) -> BinderResult<BoundExpression> {
        match expr {
            Expr::Number(n) => {
                let v = *n;
                let is_int = v.fract() == 0.0;
                let v_as_f32 = v as f32;

                if is_int && v >= u8::MIN as f64 && v <= u8::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::SmallUInt(crate::UInt8(v as u8)),
                    })
                } else if is_int && v >= u16::MIN as f64 && v <= u16::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::HalfUInt(crate::UInt16(v as u16)),
                    })
                } else if is_int && v >= u32::MIN as f64 && v <= u32::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::UInt(crate::UInt32(v as u32)),
                    })
                } else if is_int && v >= u64::MIN as f64 && v <= u64::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::BigUInt(crate::UInt64(v as u64)),
                    })
                } else if is_int && v >= i8::MIN as f64 && v <= i8::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::SmallInt(crate::Int8(v as i8)),
                    })
                } else if is_int && v >= i16::MIN as f64 && v <= i16::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::HalfInt(crate::Int16(v as i16)),
                    })
                } else if is_int && v >= i32::MIN as f64 && v <= i32::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::Int(crate::Int32(v as i32)),
                    })
                } else if is_int && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
                    Ok(BoundExpression::Literal {
                        value: DataType::BigInt(crate::Int64(v as i64)),
                    })
                } else if (v_as_f32 as f64) == v {
                    Ok(BoundExpression::Literal {
                        value: DataType::Float(crate::Float32(v as f32)),
                    })
                } else {
                    Ok(BoundExpression::Literal {
                        value: DataType::Double(crate::Float64(v as f64)),
                    })
                }
            }
            Expr::String(s) => Ok(BoundExpression::Literal {
                value: DataType::Text(Blob::from(s.as_str())),
            }),
            Expr::Boolean(b) => Ok(BoundExpression::Literal {
                value: DataType::Boolean(crate::UInt8::from(*b)),
            }),
            Expr::Null => Ok(BoundExpression::Literal {
                value: DataType::Null,
            }),
            Expr::Star => Ok(BoundExpression::Star),
            Expr::Identifier(name) => Ok(BoundExpression::ColumnRef(self.ctx.find_column(name)?)),
            Expr::QualifiedIdentifier { table, column } => Ok(BoundExpression::ColumnRef(
                self.ctx.find_qualified_column(table, column)?,
            )),

            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::In => {
                        let left_expr = self.bind_expr(left)?;
                        if let Expr::List(items) = right.as_ref() {
                            let list = items
                                .iter()
                                .map(|e| self.bind_expr(e))
                                .collect::<BinderResult<_>>()?;
                            return Ok(BoundExpression::InList {
                                expr: Box::new(left_expr),
                                list,
                                negated: false,
                            });
                        } else if let Expr::Subquery(subq) = right.as_ref() {
                            self.ctx.push_scope();
                            let bound_subq = self.bind_select(subq)?;
                            self.ctx.pop_scope();
                            return Ok(BoundExpression::InSubquery {
                                expr: Box::new(left_expr),
                                query: Box::new(bound_subq),
                                negated: false,
                            });
                        }
                    }
                    BinaryOperator::NotIn => {
                        let left_expr = self.bind_expr(left)?;
                        if let Expr::List(items) = right.as_ref() {
                            let list = items
                                .iter()
                                .map(|e| self.bind_expr(e))
                                .collect::<BinderResult<_>>()?;
                            return Ok(BoundExpression::InList {
                                expr: Box::new(left_expr),
                                list,
                                negated: true,
                            });
                        } else if let Expr::Subquery(subq) = right.as_ref() {
                            self.ctx.push_scope();
                            let bound_subq = self.bind_select(subq)?;
                            self.ctx.pop_scope();
                            return Ok(BoundExpression::InSubquery {
                                expr: Box::new(left_expr),
                                query: Box::new(bound_subq),
                                negated: true,
                            });
                        }
                    }
                    BinaryOperator::Is => {
                        let left_expr = self.bind_expr(left)?;
                        if matches!(right.as_ref(), Expr::Null) {
                            return Ok(BoundExpression::IsNull {
                                expr: Box::new(left_expr),
                                negated: false,
                            });
                        }
                    }
                    BinaryOperator::IsNot => {
                        let left_expr = self.bind_expr(left)?;
                        if matches!(right.as_ref(), Expr::Null) {
                            return Ok(BoundExpression::IsNull {
                                expr: Box::new(left_expr),
                                negated: true,
                            });
                        }
                    }
                    _ => {}
                }
                let left_bound = self.bind_expr(left)?;
                let right_bound = self.bind_expr(right)?;
                let result_type =
                    self.infer_binary_type(*op, left_bound.data_type(), right_bound.data_type());
                Ok(BoundExpression::BinaryOp {
                    left: Box::new(left_bound),
                    op: *op,
                    right: Box::new(right_bound),
                    result_type,
                })
            }

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
            } => {
                let upper = name.to_uppercase();
                if let Some(agg_kind) = self.try_parse_aggregate(&upper, args) {
                    // TODO: Review this.
                    let first_arg = if let Some(arg) = args.first() {
                        Some(Box::new(self.bind_expr(arg)?))
                    } else {
                        None
                    };

                    let return_type = self.infer_agg_type(agg_kind, first_arg.as_deref());
                    return Ok(BoundExpression::Aggregate {
                        func: agg_kind,
                        arg: first_arg,
                        distinct: *distinct,
                        return_type,
                    });
                };

                let bound_args: Vec<BoundExpression> =
                    args.iter()
                        .map(|a| self.bind_expr(a))
                        .collect::<BinderResult<Vec<BoundExpression>>>()?;

                let func = self.resolve_function(&upper);
                let return_type = self.infer_function_type(func, &bound_args);
                Ok(BoundExpression::Function {
                    func,
                    args: bound_args,
                    distinct: *distinct,
                    return_type,
                })
            }

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
                let bound_when_then: Vec<(BoundExpression, BoundExpression)> = when_clauses
                    .iter()
                    .map(|wc| Ok((self.bind_expr(&wc.condition)?, self.bind_expr(&wc.result)?)))
                    .collect::<BinderResult<_>>()?;
                let bound_else = else_clause
                    .as_ref()
                    .map(|e| self.bind_expr(e))
                    .transpose()?
                    .map(Box::new);
                let result_type = bound_when_then
                    .first()
                    .map(|(_, t)| t.data_type())
                    .unwrap_or(DataTypeKind::Null);
                Ok(BoundExpression::Case {
                    operand: bound_operand,
                    when_then: bound_when_then,
                    else_expr: bound_else,
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
                self.ctx.push_scope();
                let bound_select = self.bind_select(select)?;
                self.ctx.pop_scope();
                let result_type = bound_select
                    .schema
                    .columns()
                    .first()
                    .map(|c| c.dtype)
                    .unwrap_or(DataTypeKind::Null);
                Ok(BoundExpression::Subquery {
                    query: Box::new(bound_select),
                    result_type,
                })
            }

            Expr::Exists(select) => {
                self.ctx.push_scope();
                let bound_select = self.bind_select(select)?;
                self.ctx.pop_scope();
                Ok(BoundExpression::Exists {
                    query: Box::new(bound_select),
                    negated: false,
                })
            }

            Expr::List(_) => Err(BinderError::InvalidExpression(
                "Standalone list".to_string(),
            )),
        }
    }

    fn try_parse_aggregate(&self, name: &str, args: &[Expr]) -> Option<AggregateFunction> {
        match name {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            _ => None,
        }
    }

    fn resolve_function(&self, name: &str) -> Function {
        match name {
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => Function::Length,
            "UPPER" => Function::Upper,
            "LOWER" => Function::Lower,
            "TRIM" => Function::Trim,
            "LTRIM" => Function::LTrim,
            "RTRIM" => Function::RTrim,
            "SUBSTR" | "SUBSTRING" => Function::Substr,
            "CONCAT" => Function::Concat,
            "REPLACE" => Function::Replace,
            "ABS" => Function::Abs,
            "ROUND" => Function::Round,
            "CEIL" | "CEILING" => Function::Ceil,
            "FLOOR" => Function::Floor,
            "TRUNC" | "TRUNCATE" => Function::Trunc,
            "MOD" => Function::Mod,
            "POWER" | "POW" => Function::Power,
            "SQRT" => Function::Sqrt,
            "NOW" => Function::Now,
            "CURRENT_DATE" => Function::CurrentDate,
            "CURRENT_TIME" => Function::CurrentTime,
            "CURRENT_TIMESTAMP" => Function::CurrentTimestamp,
            "EXTRACT" => Function::Extract,
            "DATE_PART" => Function::DatePart,
            "COALESCE" => Function::Coalesce,
            "NULLIF" => Function::NullIf,
            "IFNULL" => Function::IfNull,
            "CAST" => Function::Cast,
            _ => Function::Unknown,
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
            | BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::In
            | BinaryOperator::NotIn
            | BinaryOperator::Is
            | BinaryOperator::IsNot
            | BinaryOperator::And
            | BinaryOperator::Or => DataTypeKind::Boolean,
            BinaryOperator::Concat => DataTypeKind::Text,
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => self.wider_numeric(left, right),
        }
    }

    fn infer_unary_type(&self, op: UnaryOperator, inner: DataTypeKind) -> DataTypeKind {
        match op {
            UnaryOperator::Not => DataTypeKind::Boolean,
            _ => inner,
        }
    }

    fn infer_agg_type(
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

    fn infer_function_type(&self, func: Function, args: &[BoundExpression]) -> DataTypeKind {
        match func {
            Function::Length => DataTypeKind::Int,
            Function::Upper
            | Function::Lower
            | Function::Trim
            | Function::LTrim
            | Function::RTrim
            | Function::Substr
            | Function::Concat
            | Function::Replace => DataTypeKind::Text,
            Function::Abs
            | Function::Round
            | Function::Ceil
            | Function::Floor
            | Function::Trunc => args
                .first()
                .map(|a| a.data_type())
                .unwrap_or(DataTypeKind::Double),
            Function::Mod | Function::Power | Function::Sqrt => DataTypeKind::Double,
            Function::Now | Function::CurrentTimestamp => DataTypeKind::DateTime,
            Function::CurrentDate => DataTypeKind::Date,
            Function::CurrentTime => DataTypeKind::DateTime,
            Function::Extract | Function::DatePart => DataTypeKind::Int,
            Function::Coalesce | Function::IfNull | Function::NullIf => args
                .first()
                .map(|a| a.data_type())
                .unwrap_or(DataTypeKind::Null),
            Function::Cast | Function::Unknown => DataTypeKind::Null,
        }
    }

    fn wider_numeric(&self, a: DataTypeKind, b: DataTypeKind) -> DataTypeKind {
        match (a, b) {
            (DataTypeKind::Double, _) | (_, DataTypeKind::Double) => DataTypeKind::Double,
            (DataTypeKind::Float, _) | (_, DataTypeKind::Float) => DataTypeKind::Float,
            (DataTypeKind::BigInt, _) | (_, DataTypeKind::BigInt) => DataTypeKind::BigInt,
            (DataTypeKind::Int, _) | (_, DataTypeKind::Int) => DataTypeKind::Int,
            _ => a,
        }
    }

    fn merge_schemas(&self, left: &Schema, right: &Schema) -> Schema {
        let mut cols = left.columns().clone();
        cols.extend(right.columns().clone());
        Schema::from_columns(&cols, 0)
    }
}

#[cfg(test)]
mod binder_tests {
    use super::*;
    use crate::database::Database;
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::{AxmosDBConfig, IncrementalVaccum, TextEncoding};
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
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, false);
        let worker = db.main_worker_cloned();
        db.catalog()
            .create_table("users", users_schema, worker.clone())?;

        let mut orders_schema = Schema::new();
        orders_schema.add_column("id", DataTypeKind::Int, true, true, false);
        orders_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        orders_schema.add_column("amount", DataTypeKind::Double, false, false, false);
        orders_schema.add_column("status", DataTypeKind::Text, false, false, false);
        db.catalog().create_table("orders", orders_schema, worker)?;

        Ok(db)
    }

    fn resolve_sql(sql: &str, db: &Database) -> BinderResult<BoundStatement> {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let stmt = parser
            .parse()
            .map_err(|e| BinderError::Internal(e.to_string()))?;
        let mut binder = Binder::new(db.catalog(), db.main_worker_cloned());
        binder.bind(&stmt)
    }

    #[test]
    #[serial]
    fn test_resolve_column_indices() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT name, age FROM users", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            // name is at physical index 1, age at index 3
            if let BoundExpression::ColumnRef(cr) = &select.columns[0].expr {
                assert_eq!(cr.column_idx, 1);
                assert_eq!(cr.data_type, DataTypeKind::Text);
            } else {
                panic!("Expected ColumnRef");
            }
            if let BoundExpression::ColumnRef(cr) = &select.columns[1].expr {
                assert_eq!(cr.column_idx, 3);
                assert_eq!(cr.data_type, DataTypeKind::Int);
            } else {
                panic!("Expected ColumnRef");
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_star_expansion() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT * FROM users", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            assert_eq!(select.columns.len(), 4);
            // Verify indices are sequential
            for (i, col) in select.columns.iter().enumerate() {
                assert_eq!(col.output_idx, i);
                if let BoundExpression::ColumnRef(cr) = &col.expr {
                    assert_eq!(cr.column_idx, i);
                }
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_qualified_column_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        // Both tables have 'id' - qualified names resolve correctly
        let result = resolve_sql("SELECT users.id, orders.id FROM users, orders", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            // First column from users (table_idx 0)
            if let BoundExpression::ColumnRef(cr) = &select.columns[0].expr {
                assert_eq!(cr.table_idx, 0);
                assert_eq!(cr.column_idx, 0);
            }
            // Second column from orders (table_idx 1)
            if let BoundExpression::ColumnRef(cr) = &select.columns[1].expr {
                assert_eq!(cr.table_idx, 1);
                assert_eq!(cr.column_idx, 0);
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_ambiguous_column_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT id FROM users, orders", &db);
        assert!(matches!(result, Err(BinderError::AmbiguousColumn(_))));
    }

    #[test]
    #[serial]
    fn test_column_not_found_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT nonexistent FROM users", &db);
        assert!(matches!(result, Err(BinderError::ColumnNotFound(_))));
    }

    #[test]
    #[serial]
    fn test_table_alias_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT u.name FROM users u", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            if let BoundExpression::ColumnRef(cr) = &select.columns[0].expr {
                assert_eq!(cr.table_idx, 0);
                assert_eq!(cr.column_idx, 1); // name
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_duplicate_alias_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT * FROM users t, orders t", &db);
        dbg!(&result);
        assert!(matches!(result, Err(BinderError::DuplicateAlias(_))));
    }

    #[test]
    #[serial]
    fn test_table_not_found_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT * FROM nonexistent", &db);
        assert!(matches!(result, Err(BinderError::TableNotFound(_))));
    }

    #[test]
    #[serial]
    fn test_insert_reorders_to_physical_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        // Columns specified in non-schema order: age, name, id, email
        let result = resolve_sql(
            "INSERT INTO users (age, name, id, email) VALUES (25, 'John', 1, 'j@t.com')",
            &db,
        )
        .unwrap();

        if let BoundStatement::Insert(insert) = result {
            // Should be reordered to physical order: id(0), name(1), email(2), age(3)
            assert_eq!(insert.columns, vec![0, 1, 2, 3]);

            // Values should also be reordered
            if let BoundInsertSource::Values(rows) = &insert.source {
                // First value should now be id=1 (was at position 2 in original)
                if let BoundExpression::Literal { value } = &rows[0][0] {
                    // Check it's the integer 1
                    assert!(matches!(value, DataType::SmallUInt(v) if v.0 == 1));
                }
                // Last value should be age=25 (was at position 0 in original)
                if let BoundExpression::Literal { value } = &rows[0][3] {
                    assert!(matches!(value, DataType::SmallUInt(v) if v.0 == 25));
                }
            }
        } else {
            panic!("Expected INSERT");
        }
    }

    #[test]
    #[serial]
    fn test_insert_column_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "INSERT INTO users (id, name) VALUES (1, 'John', 'extra')",
            &db,
        );
        assert!(matches!(
            result,
            Err(BinderError::ColumnCountMismatch { .. })
        ));
    }

    #[test]
    #[serial]
    fn test_join_condition_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        if let BoundStatement::Select(select) = result {
            if let Some(BoundTableRef::Join {
                condition: Some(cond),
                ..
            }) = &select.from
            {
                if let BoundExpression::BinaryOp { left, right, .. } = cond {
                    // u.id (table_idx 0, column_idx 0)
                    if let BoundExpression::ColumnRef(l) = left.as_ref() {
                        assert_eq!(l.table_idx, 0);
                        assert_eq!(l.column_idx, 0);
                    }
                    // o.user_id (table_idx 1, column_idx 1)
                    if let BoundExpression::ColumnRef(r) = right.as_ref() {
                        assert_eq!(r.table_idx, 1);
                        assert_eq!(r.column_idx, 1);
                    }
                }
            } else {
                panic!("Expected JOIN with condition");
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_join_merges_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        )
        .unwrap();

        if let BoundStatement::Select(select) = result {
            // users has 4 columns, orders has 4 columns = 8 total
            assert_eq!(select.columns.len(), 8);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_subquery_scope_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT * FROM (SELECT name FROM users) AS sub", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            // Subquery only selects 'name', so outer query sees 1 column
            assert_eq!(select.columns.len(), 1);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_correlated_subquery() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            &db,
        )
        .unwrap();

        if let BoundStatement::Select(select) = result {
            if let Some(BoundExpression::Exists { query, .. }) = &select.where_clause {
                // The subquery should have a WHERE clause referencing outer scope
                assert!(query.where_clause.is_some());
            } else {
                panic!("Expected EXISTS");
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_in_subquery() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            &db,
        )
        .unwrap();

        if let BoundStatement::Select(select) = result {
            if let Some(BoundExpression::InSubquery { negated, .. }) = &select.where_clause {
                assert!(!negated);
            } else {
                panic!("Expected InSubquery");
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_cte_reference() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "WITH adults AS (SELECT id, name FROM users WHERE age >= 18) SELECT * FROM adults",
            &db,
        )
        .unwrap();

        if let BoundStatement::With(with_stmt) = result {
            assert_eq!(with_stmt.ctes.len(), 1);

            // Main query should reference CTE
            if let Some(BoundTableRef::Cte { cte_idx, .. }) = &with_stmt.body.from {
                assert_eq!(*cte_idx, 0);
            } else {
                panic!("Expected CTE reference");
            }

            // CTE selects 2 columns (id, name)
            assert_eq!(with_stmt.body.columns.len(), 2);
        } else {
            panic!("Expected WITH");
        }
    }

    #[test]
    #[serial]
    fn test_expression_type_inference() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "SELECT age + 1, age > 18, COUNT(*), UPPER(name) FROM users",
            &db,
        )
        .unwrap();

        if let BoundStatement::Select(select) = result {
            // age + 1 -> numeric
            assert!(select.columns[0].expr.data_type().is_numeric());

            // age > 18 -> Boolean
            assert_eq!(select.columns[1].expr.data_type(), DataTypeKind::Boolean);

            // COUNT(*) -> BigInt
            assert_eq!(select.columns[2].expr.data_type(), DataTypeKind::BigInt);

            // UPPER(name) -> Text
            assert_eq!(select.columns[3].expr.data_type(), DataTypeKind::Text);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_aggregate_vs_scalar_function() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result =
            resolve_sql("SELECT COUNT(*), SUM(age), LENGTH(name) FROM users", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            // COUNT and SUM should be Aggregate
            assert!(matches!(
                &select.columns[0].expr,
                BoundExpression::Aggregate { .. }
            ));
            assert!(matches!(
                &select.columns[1].expr,
                BoundExpression::Aggregate { .. }
            ));
            // LENGTH should be Function
            assert!(matches!(
                &select.columns[2].expr,
                BoundExpression::Function { .. }
            ));
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    #[serial]
    fn test_update_assignment_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("UPDATE users SET age = age + 1 WHERE id = 1", &db).unwrap();

        if let BoundStatement::Update(update) = result {
            assert_eq!(update.assignments[0].column_idx, 3); // age is index 3
            // RHS should reference the same column
            if let BoundExpression::BinaryOp { left, .. } = &update.assignments[0].value {
                if let BoundExpression::ColumnRef(cr) = left.as_ref() {
                    assert_eq!(cr.column_idx, 3);
                }
            }
        } else {
            panic!("Expected UPDATE");
        }
    }

    #[test]
    #[serial]
    fn test_delete_filter_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("DELETE FROM users WHERE age < 18", &db).unwrap();

        if let BoundStatement::Delete(delete) = result {
            if let Some(BoundExpression::BinaryOp { left, .. }) = &delete.filter {
                if let BoundExpression::ColumnRef(cr) = left.as_ref() {
                    assert_eq!(cr.column_idx, 3); // age
                }
            }
        } else {
            panic!("Expected DELETE");
        }
    }

    #[test]
    #[serial]
    fn test_create_index_column_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("CREATE INDEX idx ON users(name, email)", &db).unwrap();

        if let BoundStatement::CreateIndex(idx) = result {
            assert_eq!(idx.columns.len(), 2);
            assert_eq!(idx.columns[0].column_idx, 1); // name
            assert_eq!(idx.columns[1].column_idx, 2); // email
        } else {
            panic!("Expected CREATE INDEX");
        }
    }

    #[test]
    #[serial]
    fn test_alter_table_drop_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("ALTER TABLE users DROP COLUMN age", &db).unwrap();

        if let BoundStatement::AlterTable(alter) = result {
            if let BoundAlterAction::DropColumn(col_idx) = &alter.action {
                assert_eq!(*col_idx, 3);
            } else {
                panic!("Expected DropColumn");
            }
        } else {
            panic!("Expected ALTER TABLE");
        }
    }

    #[test]
    #[serial]
    fn test_create_table_foreign_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql(
            "CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT REFERENCES orders(id))",
            &db,
        )
        .unwrap();

        if let BoundStatement::CreateTable(create) = result {
            // Find FK constraint on order_id column
            let order_id_constraints = &create.columns[1].constraints;
            let fk = order_id_constraints
                .iter()
                .find(|c| matches!(c, BoundColumnConstraint::ForeignKey { .. }));
            assert!(fk.is_some());
            if let Some(BoundColumnConstraint::ForeignKey { ref_column_idx, .. }) = fk {
                assert_eq!(*ref_column_idx, 0); // orders.id
            }
        } else {
            panic!("Expected CREATE TABLE");
        }
    }

    #[test]
    #[serial]
    fn test_select_generates_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path().join("test.db")).unwrap();

        let result = resolve_sql("SELECT name, age * 2 FROM users", &db).unwrap();

        if let BoundStatement::Select(select) = result {
            assert_eq!(select.schema.columns().len(), 2);
            assert_eq!(select.schema.columns()[0].dtype, DataTypeKind::Text);
            assert!(select.schema.columns()[1].dtype.is_numeric());
        } else {
            panic!("Expected SELECT");
        }
    }
}
