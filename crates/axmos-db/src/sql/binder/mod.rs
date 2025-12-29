use crate::{
    schema::base::{Column, Relation, Schema},
    types::{DataTypeKind, ObjectId},
};

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

pub mod analyzer;
pub mod binder;
pub mod bounds;

/// Object already exists errors
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseItem {
    Table(String),
    Index(String),
    Constraint(String),
    Column(String, String),
}

impl Display for DatabaseItem {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Index(index) => write!(f, "Index '{}'", index),
            Self::Table(table) => write!(f, "Table '{}' ", table),
            Self::Constraint(ct) => write!(f, "Constraint '{}'", ct),
            Self::Column(table, column) => {
                write!(f, "Column '{}' on table '{}'", column, table)
            }
        }
    }
}

/// Contains the minimum information needed for column resolution.
pub(crate) struct ScopeEntry {
    /// Reference name (alias or table name)
    pub ref_name: String,
    /// Position in the current scope (for output ordering)
    pub scope_index: usize,
    /// Schema of this table/subquery
    pub schema: Schema,
    /// Optional: table id (None for CTEs, subqueries)
    pub table_id: Option<ObjectId>,
    /// Optional: CTE index if this references a CTE
    pub cte_idx: Option<usize>,
}

/// Result of resolving a column
pub(crate) struct ResolvedColumn {
    pub scope_index: usize,
    pub column_idx: usize,
    pub data_type: DataTypeKind,
    pub table_id: Option<ObjectId>,
}

/// A single query scope containing table entries
pub(crate) struct QueryScope {
    entries: HashMap<String, ScopeEntry>,
    entry_order: Vec<String>,
    /// Output column aliases (for ORDER BY resolution)
    output_aliases: HashMap<String, DataTypeKind>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScopeError {
    DuplicateAlias(String),
    NotFound(DatabaseItem),
    AmbiguousColumn(String),
}

impl Display for ScopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::DuplicateAlias(item) => write!(f, "Duplicate alias {}", item),
            Self::NotFound(obj) => write!(f, "{} not found", obj),
            Self::AmbiguousColumn(st) => write!(f, "ambiguous column name {st}. Unable to resolve"),
        }
    }
}
impl Error for ScopeError {}
pub type ScopeResult<T> = Result<T, ScopeError>;

impl QueryScope {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            entry_order: Vec::new(),
            output_aliases: HashMap::new(),
        }
    }

    pub fn add_entry(&mut self, entry: ScopeEntry) -> ScopeResult<()> {
        let ref_name = entry.ref_name.clone();
        if self.entries.contains_key(&ref_name) {
            return Err(ScopeError::DuplicateAlias(ref_name.to_string()));
        }
        self.entry_order.push(ref_name.clone());
        self.entries.insert(ref_name, entry);
        Ok(())
    }

    pub fn add_output_alias(&mut self, name: String, dtype: DataTypeKind) {
        self.output_aliases.insert(name, dtype);
    }

    pub fn get_output_alias(&self, name: &str) -> Option<DataTypeKind> {
        self.output_aliases.get(name).copied()
    }

    pub fn get_entry(&self, name: &str) -> Option<&ScopeEntry> {
        self.entries.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ScopeEntry> {
        self.entry_order
            .iter()
            .filter_map(|name| self.entries.get(name))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Manages a stack of query scopes for nested queries.
/// Implements ColumnResolver to search from innermost to outermost scope.
pub(crate) struct ScopeStack {
    scopes: Vec<QueryScope>,
}

impl ScopeStack {
    pub fn new() -> Self {
        Self { scopes: Vec::new() }
    }

    pub fn push(&mut self) {
        self.scopes.push(QueryScope::new());
    }

    pub fn pop(&mut self) -> Option<QueryScope> {
        self.scopes.pop()
    }

    pub fn current(&self) -> Option<&QueryScope> {
        self.scopes.last()
    }

    pub fn current_mut(&mut self) -> Option<&mut QueryScope> {
        self.scopes.last_mut()
    }

    pub fn depth(&self) -> usize {
        self.scopes.len()
    }
}

impl ScopeStack {
    pub fn resolve_column(&self, column: &str, table: Option<&str>) -> ScopeResult<ResolvedColumn> {
        for scope in self.scopes.iter().rev() {
            match resolve_in_scope(scope, column, table) {
                Ok(resolved) => return Ok(resolved),
                Err(ScopeError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ScopeError::NotFound(DatabaseItem::Column(
            table.unwrap_or("unqualified").to_string(),
            column.to_string(),
        )))
    }
}

fn resolve_in_scope(
    scope: &QueryScope,
    column: &str,
    table: Option<&str>,
) -> ScopeResult<ResolvedColumn> {
    // For unqualified columns, check output aliases first
    if table.is_none() {
        if let Some(dtype) = scope.get_output_alias(column) {
            return Ok(ResolvedColumn {
                scope_index: 0,
                column_idx: 0, // Not meaningful for aliases
                data_type: dtype,
                table_id: None,
            });
        }
    }

    if let Some(table_name) = table {
        // Qualified column: table.column
        let entry = scope
            .get_entry(table_name)
            .ok_or_else(|| ScopeError::NotFound(DatabaseItem::Table(table_name.to_string())))?;

        let col_idx = entry.schema.column_index.get(column).ok_or_else(|| {
            ScopeError::NotFound(DatabaseItem::Column(
                table_name.to_string(),
                column.to_string(),
            ))
        })?;

        let col = entry.schema.column(*col_idx).ok_or_else(|| {
            ScopeError::NotFound(DatabaseItem::Column(
                table_name.to_string(),
                column.to_string(),
            ))
        })?;

        Ok(ResolvedColumn {
            scope_index: entry.scope_index,
            column_idx: *col_idx,
            data_type: col.datatype(),
            table_id: entry.table_id,
        })
    } else {
        // Unqualified column: search all entries, check for ambiguity
        let mut found: Option<ResolvedColumn> = None;
        let mut found_count = 0;

        for entry in scope.iter() {
            if let Some(col_idx) = entry.schema.column_index.get(column) {
                if let Some(col) = entry.schema.column(*col_idx) {
                    found = Some(ResolvedColumn {
                        scope_index: entry.scope_index,
                        column_idx: *col_idx,
                        data_type: col.datatype(),
                        table_id: entry.table_id,
                    });
                    found_count += 1;
                }
            }
        }

        match found_count {
            0 => Err(ScopeError::NotFound(DatabaseItem::Column(
                "unqualified".to_string(),
                column.to_string(),
            ))),
            1 => Ok(found.unwrap()),
            _ => Err(ScopeError::AmbiguousColumn(column.to_string())),
        }
    }
}
