use super::stats::Stats;
use crate::{
    storage::tuple::Row,
    types::{
        Blob, DataType, DataTypeKind, ObjectId, PageId, RowId, SerializationError,
        SerializationResult, UInt64,
    },
};
use rkyv::{
    Archive, Deserialize, Serialize, from_bytes, rancor::Error as RkyvError, to_bytes,
    util::AlignedVec,
};

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug)]
pub enum SchemaError {
    ConstraintViolation(String),
    ColumnNotFound(String),
    ColumnIndexOutOfBounds(usize),
    NotATable,
    InvalidDataType(DataTypeKind),
    DuplicatePrimaryKey,
    Other(String),
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ColumnNotFound(name) => {
                write!(f, "Column with name {name} was not found in schema.")
            }
            Self::ColumnIndexOutOfBounds(usize) => write!(
                f,
                "Column index out of bounds for the current schema. Max valid value is {usize}"
            ),
            Self::ConstraintViolation(st) => write!(f, "attempted to violate constraint {st}"),
            Self::InvalidDataType(dtype) => {
                write!(f, "datatype: {dtype} is invalid for this column")
            }
            Self::NotATable => {
                f.write_str("constraints or indexes cannot be added to index schemas.")
            }
            Self::DuplicatePrimaryKey => f.write_str(
                "attempted to add a duplicated primary key to an schema with a key already set.",
            ),
            Self::Other(string) => f.write_str(string),
        }
    }
}

impl Error for SchemaError {}
pub type SchemaResult<T> = Result<T, SchemaError>;

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Schema {
    pub(crate) columns: Vec<Column>,
    pub(crate) num_keys: usize,
    pub(crate) table_constraints: Option<Vec<TableConstraint>>,
    pub(crate) column_index: HashMap<String, usize>, // Quick index to bind columns
    pub(crate) table_indexes: Option<HashMap<ObjectId, Vec<usize>>>,
}

/// A handle representing an index.
///
/// Contains the index object id and the list of indexed columns.
pub struct IndexHandle {
    id: ObjectId,
    indexed_columns: Vec<usize>,
}

impl IndexHandle {
    pub(crate) fn new(id: ObjectId, columns: Vec<usize>) -> Self {
        Self {
            id,
            indexed_columns: columns,
        }
    }

    pub(crate) fn id(&self) -> ObjectId {
        self.id
    }

    pub(crate) fn indexed_column_ids(&self) -> &[usize] {
        &self.indexed_columns
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Schema {
    /// Collects an array of columns and builds a column index from it.
    fn build_column_index(columns: &[Column]) -> HashMap<String, usize> {
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name().to_string(), i))
            .collect()
    }

    /// Returns a reference to the columns vector
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub(crate) fn new_empty() -> Self {
        Self::new_table_with_num_keys(vec![], 0)
    }

    /// Collect all the index information from this schema.
    pub(crate) fn get_indexes(&self) -> Vec<IndexHandle> {
        if let Some(indexes) = &self.table_indexes {
            indexes
                .iter()
                .map(|(id, col_info)| IndexHandle::new(*id, col_info.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn has_primary_key(&self) -> bool {
        self.table_constraints.as_ref().is_some_and(|v| {
            v.iter()
                .any(|c| matches!(c, TableConstraint::PrimaryKey(_)))
        })
    }

    /// Create a new table schema.
    pub(crate) fn new_table(columns: Vec<Column>) -> Self {
        let column_index = Self::build_column_index(&columns);
        Self {
            columns,
            num_keys: 1, // Tables always have one key.
            table_constraints: Some(Vec::new()),
            table_indexes: Some(HashMap::new()),
            column_index,
        }
    }

    /// Create a new schema of any type
    pub(crate) fn new_table_with_num_keys(columns: Vec<Column>, num_keys: usize) -> Self {
        let column_index = Self::build_column_index(&columns);
        Self {
            columns,
            num_keys, // Tables always have one key.
            table_constraints: Some(Vec::new()),
            table_indexes: Some(HashMap::new()),
            column_index,
        }
    }

    /// Create a new index schema.
    pub(crate) fn new_index(columns: Vec<Column>, num_keys: usize) -> Self {
        let column_index = Self::build_column_index(&columns);
        Self {
            columns,
            num_keys,
            table_constraints: None,
            table_indexes: None,
            column_index,
        }
    }

    /// Moves out of itself and creates a new list of columns.
    pub(crate) fn into_column_list(self) -> Vec<Column> {
        self.columns
    }

    /// Index can be identified because they do not have table constraints.
    #[inline]
    pub(crate) fn is_table(&self) -> bool {
        self.table_constraints.is_some()
    }

    #[inline]
    pub(crate) fn is_index(&self) -> bool {
        self.table_constraints.is_none()
    }

    /// Accessors for a single column, key and value by index
    #[inline]
    pub(crate) fn value(&self, index: usize) -> Option<&Column> {
        self.columns.get(self.num_keys as usize + index)
    }

    /// Accessor for a column
    #[inline]
    pub(crate) fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    #[inline]
    pub(crate) fn num_keys(&self) -> usize {
        self.num_keys as usize
    }

    #[inline]
    pub(crate) fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub(crate) fn num_values(&self) -> usize {
        self.columns.len().saturating_sub(self.num_keys())
    }

    /// Accessor for a key (validates that the index is lower than [num_keys])
    #[inline]
    pub(crate) fn key(&self, index: usize) -> Option<&Column> {
        (index < self.num_keys)
            .then(|| index)
            .and_then(|i| self.columns.get(i))
    }

    #[inline]
    pub(crate) fn iter_keys(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().take(self.num_keys)
    }

    #[inline]
    pub(crate) fn iter_values(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().skip(self.num_keys)
    }

    #[inline]
    pub(crate) fn iter_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    /// Returns the binding (index) of a column by its name
    pub(crate) fn bind_column(&self, name: &str) -> SchemaResult<usize> {
        self.column_index
            .get(name)
            .ok_or(SchemaError::ColumnNotFound(name.to_string()))
            .copied()
    }

    /// Validate column indexes when adding constraints to the schema.
    ///
    /// Checks that all indexes in [list] are lower than [max].
    ///
    /// If not, returns and  IndexOutOfBounds error.
    fn validate_index_list(list: &[usize], max: usize) -> SchemaResult<()> {
        if let Some(&index) = list.iter().find(|&&i| i >= max) {
            return Err(SchemaError::ColumnIndexOutOfBounds(max));
        }

        Ok(())
    }

    /// Adds an index to a table schema.
    pub(crate) fn add_index(&mut self, index: IndexHandle) -> SchemaResult<()> {
        if let Some(indexes) = self.table_indexes.as_mut() {
            let last_index = self.columns.len() - 1;
            let columns = index.indexed_column_ids();
            Self::validate_index_list(&columns, last_index)?;
            indexes.insert(index.id, columns.to_vec());
            Ok(())
        } else {
            return Err(SchemaError::NotATable);
        }
    }

    /// Adds a constraint to the schema after validation has passed.
    pub(crate) fn add_constraint(&mut self, constraint: TableConstraint) -> SchemaResult<()> {
        let has_pk = self.has_primary_key();
        if let Some(constraints) = self.table_constraints.as_mut() {
            let last_index = self.columns.len() - 1;

            match &constraint {
                TableConstraint::Unique(columns) => {
                    Self::validate_index_list(&columns, last_index)?;
                }
                TableConstraint::ForeignKey { columns, info } => {
                    Self::validate_index_list(&columns, last_index)?;
                }
                TableConstraint::PrimaryKey(columns) => {
                    if has_pk {
                        return Err(SchemaError::DuplicatePrimaryKey);
                    }
                    Self::validate_index_list(&columns, last_index)?;
                }
            }

            constraints.push(constraint);
            Ok(())
        } else {
            return Err(SchemaError::NotATable);
        }
    }

    /// Serializes an schema to a blob object
    pub fn to_blob(&self) -> SerializationResult<Blob> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(Blob::from_unencoded_slice(&bytes))
    }

    /// Deserializes an schema from a blob object
    pub fn from_blob(blob: &Blob) -> SerializationResult<Self> {
        let data = blob
            .data()
            .map_err(|e| SerializationError::InvalidVarIntPrefix)?;

        //  It is impossible to avoid this copy here unless we fuck up the whole blob data structure since blob does not guarantee alignment.
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(data);
        let schema = from_bytes::<Schema, RkyvError>(&aligned)?;

        Ok(schema)
    }

    /// COllects all the objects that depend from this one.
    ///
    /// Useful for DELETE ... CASCADE operations.
    pub(crate) fn get_dependants(&self) -> Vec<ObjectId> {
        let mut deps = Vec::new();
        if self.is_index() {
            return deps;
        };

        for ct in self.table_constraints.as_ref().unwrap() {
            if let TableConstraint::ForeignKey { info, .. } = ct {
                deps.push(info.ref_table);
            };
        }

        deps.extend(self.table_indexes.as_ref().unwrap().keys());
        deps
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum TableConstraint {
    PrimaryKey(Vec<usize>), // Column ids that conform the primarykey
    ForeignKey {
        columns: Vec<usize>,
        info: ForeignKeyInfo,
    },
    Unique(Vec<usize>), // Unique set of column ids in the schema.
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKeyInfo {
    ref_table: ObjectId,
    ref_columns: Vec<usize>, // List of bound column indexes in the ref table
}

impl ForeignKeyInfo {
    pub(crate) fn for_table_and_columns(ref_table: ObjectId, ref_columns: Vec<usize>) -> Self {
        Self {
            ref_table,
            ref_columns,
        }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub(crate) dtype: u8,
    pub(crate) name: String,
    pub(crate) default: Option<Box<[u8]>>,
    pub(crate) is_unique: bool,
    pub(crate) is_non_null: bool,
}

impl Column {
    /// Create a new column with a name, a datatype and default constraints.
    pub(crate) fn new_with_defaults(dtype: DataTypeKind, name: &str) -> Self {
        Self {
            dtype: dtype as u8,
            name: name.to_string(),
            default: None,
            is_unique: false,
            is_non_null: false,
        }
    }

    /// Get a [str] reference to the column name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Add a default constraint to the column
    pub(crate) fn with_default(mut self, default: DataType) -> SchemaResult<Self> {
        if default.kind() != self.datatype() && !default.is_null() {
            return Err(SchemaError::InvalidDataType(default.kind()));
        };

        if default.is_null() && self.is_non_null {
            return Err(SchemaError::ConstraintViolation(
                "attempted to set a null default value on a non nullable column".to_string(),
            ));
        }

        // We cannot serialize safely nulls by now o we set the value to none.
        if default.is_null() {
            self.default = None;
        } else {
            let required_size = default.runtime_size();
            let mut writer = Vec::with_capacity(required_size);
            default.write_to(&mut writer, 0).map_err(|e| {
                SchemaError::Other(format!("failed to deserialize default dataype {e}"))
            })?;
            self.default = Some(writer.into_boxed_slice());
        };
        Ok(self)
    }

    /// Add a non null constraint to the column
    pub(crate) fn with_non_null_constraint(mut self) -> Self {
        self.is_non_null = true;
        self
    }

    /// Add a unique constraint to the column
    pub(crate) fn with_unique_constraint(mut self) -> Self {
        self.is_unique = true;
        self
    }

    pub(crate) fn datatype(&self) -> DataTypeKind {
        DataTypeKind::from_repr(self.dtype).unwrap_or(DataTypeKind::Null)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    object_id: ObjectId,
    root_page: PageId,
    name: String,
    schema: Schema,
    next_row_id: Option<RowId>, // Only for tables.
    stats: Option<Stats>,
}

impl Relation {
    /// Creates a table relation
    pub(crate) fn table(
        object_id: ObjectId,
        name: &str,
        root_page: PageId,
        columns: Vec<Column>,
    ) -> Self {
        let schema: Schema = Schema::new_table(columns);
        Self {
            object_id,
            name: name.to_string(),
            root_page,
            schema,
            next_row_id: Some(0),
            stats: None, // Initially set to none, will be set later when computed.
        }
    }

    /// Creates an index relation
    pub(crate) fn index(
        object_id: ObjectId,
        name: &str,
        root_page: PageId,
        columns: Vec<Column>,
        num_keys: usize,
    ) -> Self {
        let schema: Schema = Schema::new_index(columns, num_keys);
        Self {
            object_id,
            name: name.to_string(),
            root_page,
            schema,
            next_row_id: None,
            stats: None, // Initially set to none, will be set later when computed.
        }
    }

    pub(crate) fn with_stats(mut self, stats: Stats) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Returns the relation's object id
    pub(crate) fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub(crate) fn stats(&self) -> Option<&Stats> {
        self.stats.as_ref()
    }
    /// Returns the relation's name
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
    /// Returns the relation's root page id
    pub(crate) fn root(&self) -> PageId {
        self.root_page
    }
    /// Returns the relation's schema as a reference.
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Check whether this is a table relation or not.
    pub(crate) fn is_table(&self) -> bool {
        self.schema().is_table()
    }

    /// Check whether this is an index.
    pub(crate) fn is_index(&self) -> bool {
        self.schema().is_index()
    }

    /// Create a meta index formatted row from a given relation.
    pub(crate) fn to_meta_index_row(&self) -> Row {
        Row::new(
            vec![
                DataType::Blob(Blob::from(self.name())),
                DataType::BigUInt(UInt64::from(self.object_id)),
            ]
            .into_boxed_slice(),
        )
    }

    /// Convert this relation to a row in order to store it on the meta table
    pub(crate) fn to_meta_table_row(&self) -> Row {
        Row::new(
            vec![
                DataType::BigUInt(UInt64::from(self.object_id)),
                DataType::BigUInt(UInt64::from(self.root_page)),
                self.next_row_id
                    .map(|r| DataType::BigUInt(UInt64::from(r)))
                    .unwrap_or(DataType::Null),
                DataType::Blob(Blob::from(self.name())),
                DataType::Blob(self.schema.to_blob().expect("Failed to serialize schema")),
                self.stats
                    .as_ref()
                    .map(|s| DataType::Blob(s.to_blob().expect("Failed to serialize stats")))
                    .unwrap_or(DataType::Null),
            ]
            .into_boxed_slice(),
        )
    }

    /// BUilds a relation object from a row in the meta table.
    pub fn from_meta_table_row(row: Row) -> Self {
        let object_id = match &row[0] {
            DataType::BigUInt(v) => v.value() as ObjectId,
            _ => panic!("Invalid object_id type in meta table row"),
        };

        let root_page = match &row[1] {
            DataType::BigUInt(v) => v.value() as PageId,
            _ => panic!("Invalid root_page type in meta table row"),
        };

        let next_row_id = match &row[2] {
            DataType::BigUInt(v) => Some(v.value() as RowId),
            DataType::Null => None,
            _ => panic!("Invalid next_row_id type in meta table row"),
        };

        let name = match &row[3] {
            DataType::Blob(blob) => blob.as_str().expect("Invalid name encoding").to_string(),
            _ => panic!("Invalid name type in meta table row"),
        };

        let schema = match &row[4] {
            DataType::Blob(blob) => Schema::from_blob(blob).expect("Failed to deserialize schema"),
            _ => panic!("Invalid schema type in meta table row"),
        };

        let stats = match &row[5] {
            DataType::Blob(blob) => {
                Some(Stats::from_blob(blob).expect("Failed to deserialize stats"))
            }
            DataType::Null => None,
            _ => panic!("Invalid stats type in meta table row"),
        };

        Self {
            object_id,
            root_page,
            name,
            schema,
            next_row_id,
            stats,
        }
    }

    /// Gets the next row id for tables. Panics if called on an index.
    pub(crate) fn next_row_id(&self) -> UInt64 {
        UInt64::from(
            self.next_row_id
                .expect("next_row_id called on index relation"),
        )
    }

    /// Increments the row counter. Only valid for tables.
    pub(crate) fn increment_row_id(&mut self) {
        if let Some(ref mut row_id) = self.next_row_id {
            *row_id += 1;
        }
    }

    /// Get indexes from the schema (returns empty vec for index relations)
    pub(crate) fn get_indexes(&self) -> Vec<IndexHandle> {
        self.schema.get_indexes()
    }

    pub(crate) fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}
