use super::stats::Stats;
use crate::{
    tree::comparators::{
        CompositeComparator, DynComparator, FixedSizeBytesComparator, NumericComparator,
        SignedNumericComparator, VarlenComparator,
    },
    types::{
        Blob, DataType, DataTypeKind, ObjectId, PageId, RowId, SerializationError,
        SerializationResult,
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

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub(crate) struct Schema {
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

impl Schema {
    /// Collects an array of columns and builds a column index from it.
    fn build_column_index(columns: &[Column]) -> HashMap<String, usize> {
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name().to_string(), i))
            .collect()
    }

    /// Get a key comparator for this schema.
    pub(crate) fn comparator(&self) -> DynComparator {
        if self.num_keys > 1 {
            let comparators: Vec<DynComparator> = self
                .iter_keys()
                .map(|col| {
                    col.comparator()
                        .unwrap_or(DynComparator::Variable(VarlenComparator))
                })
                .collect();
            DynComparator::Composite(CompositeComparator::new(comparators))
        } else {
            self.columns
                .first()
                .map(|c| c.comparator())
                .flatten()
                .unwrap_or(DynComparator::Variable(VarlenComparator))
        }
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

    fn has_primary_key(&self) -> bool {
        self.table_constraints.as_ref().is_some_and(|v| {
            v.iter()
                .any(|c| matches!(c, TableConstraint::PrimaryKey(_)))
        })
    }

    /// Adds an index to a table schema.
    pub(crate) fn add_index(&mut self, index: IndexHandle) -> SchemaResult<()> {
        if let Some(indexes) = self.table_indexes.as_mut() {
            let last_index = self.columns.len() - 1;
            Self::validate_index_list(&index.indexed_columns, last_index)?;
            indexes.insert(index.id, index.indexed_columns);
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

    /// Build a comparator from this column.
    pub(crate) fn comparator(&self) -> Option<DynComparator> {
        match self.datatype() {
            DataTypeKind::Blob => Some(DynComparator::Variable(VarlenComparator)),
            DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Double
            | DataTypeKind::Float => Some(DynComparator::SignedNumeric(
                SignedNumericComparator::for_size(self.datatype().fixed_size().unwrap()),
            )),
            DataTypeKind::UInt | DataTypeKind::BigUInt => Some(DynComparator::StrictNumeric(
                NumericComparator::for_size(self.datatype().fixed_size().unwrap()),
            )),
            DataTypeKind::Bool => Some(DynComparator::FixedSizeBytes(
                FixedSizeBytesComparator::for_size(1),
            )),
            _ => None, // Null type does not have a comparator
        }
    }

    /// Add a default constraint to the column
    pub(crate) fn with_default(mut self, default: DataType) -> SchemaResult<Self> {
        if default.kind() != DataTypeKind::from_repr(self.dtype).unwrap_or(DataTypeKind::Null) {
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
        };

        let required_size = default.runtime_size();
        let mut writer = Vec::with_capacity(required_size);
        default.write_to(&mut writer, 0).map_err(|e| {
            SchemaError::Other(format!("failed to deserialize default dataype {e}"))
        })?;
        self.default = Some(writer.into_boxed_slice());
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

    /// Returns the relation's object id
    pub(crate) fn object_id(&self) -> ObjectId {
        self.object_id
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
}
