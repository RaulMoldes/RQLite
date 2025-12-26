use crate::{
    ObjectId, TransactionId, UInt64,
    io::pager::SharedPager,
    multithreading::coordinator::Snapshot,
    schema::Schema,
    storage::{
        page::BtreePage,
        tuple::{RefTupleAccessor, Row, Tuple, TupleBuilder, TupleError, TupleReader, TupleRef},
    },
    tree::{
        accessor::{BtreeReadAccessor, BtreeWriteAccessor, Position},
        bplustree::{Btree, BtreeError, BtreeResult, SearchResult},
        cell_ops::AsKeyBytes,
        comparators::DynComparator,
    },
    types::{Blob, DataType, DataTypeRef, PageId},
};

use super::{
    Stats,
    base::Relation,
    meta_index_schema, meta_table_schema,
    stats::{ColumnStats, StatsCollector},
};
use std::collections::HashSet;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};
pub(crate) struct BtreeBuilder {
    pager: SharedPager,
    min_keys: usize,
    num_siblings_per_side: usize,
}

#[derive(Debug)]
pub enum CatalogError {
    TableNotFound(ObjectId),
    InvalidObjectName(String),
    BtreeError(BtreeError),
    TupleError(TupleError),
    Other(String),
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::TableNotFound(id) => write!(f, "Table not found {id}"),
            Self::InvalidObjectName(name) => write!(f, "object with name {name}does not exist"),
            Self::BtreeError(err) => write!(f, "Btree error: {err}"),
            Self::TupleError(err) => write!(f, "Tuple error {err}"),
            Self::Other(string) => write!(f, "{string}"),
        }
    }
}

impl From<BtreeError> for CatalogError {
    fn from(value: BtreeError) -> Self {
        Self::BtreeError(value)
    }
}
impl From<TupleError> for CatalogError {
    fn from(value: TupleError) -> Self {
        Self::TupleError(value)
    }
}

impl std::error::Error for CatalogError {}
type CatalogResult<T> = Result<T, CatalogError>;

/// Gets current Unix timestamp.
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl BtreeBuilder {
    pub(crate) fn build_tree(
        &self,
        root: PageId,
        schema: &Schema,
    ) -> Btree<BtreePage, DynComparator, BtreeReadAccessor> {
        let comparator = schema.comparator();
        let pager = self.pager.clone();
        Btree::new(
            root,
            pager,
            self.min_keys,
            self.num_siblings_per_side,
            comparator,
        )
        .with_accessor(BtreeReadAccessor::new())
    }

    pub(crate) fn build_tree_mut(
        &self,
        root: PageId,
        schema: &Schema,
    ) -> Btree<BtreePage, DynComparator, BtreeWriteAccessor> {
        let comparator = schema.comparator();
        let pager = self.pager.clone();
        Btree::new(
            root,
            pager,
            self.min_keys,
            self.num_siblings_per_side,
            comparator,
        )
        .with_accessor(BtreeWriteAccessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct Catalog {
    meta_table: PageId,
    meta_index: PageId,

    last_stored_object: ObjectId,
}

impl Catalog {
    pub(crate) fn new(meta_table: PageId, meta_index: PageId) -> Self {
        Self {
            meta_table,
            meta_index,
            last_stored_object: 0,
        }
    }

    pub(crate) fn with_last_stored_object(mut self, last_stored: ObjectId) -> Self {
        self.last_stored_object = last_stored;
        self
    }

    /// Converts a relation into a meta table tuple for storage.
    pub(crate) fn relation_as_meta_table_tuple(
        relation: &Relation,
        transaction_id: TransactionId,
    ) -> CatalogResult<Tuple> {
        let schema = meta_table_schema();
        let builder = TupleBuilder::from_schema(&schema);
        let t = builder.build(relation.to_meta_table_row(), transaction_id)?;

        Ok(t)
    }

    /// Converts a relation into a meta table tuple for storage.
    pub(crate) fn relation_as_meta_index_tuple(
        relation: &Relation,
        transaction_id: TransactionId,
    ) -> CatalogResult<Tuple> {
        let schema = meta_table_schema();
        let builder = TupleBuilder::from_schema(&schema);
        let t = builder.build(relation.to_meta_index_row(), transaction_id)?;

        Ok(t)
    }

    /// Stores a new relation in the meta table.
    pub(crate) fn store_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        transaction_id: TransactionId,
    ) -> CatalogResult<()> {
        // Store the relation in the meta index.
        let schema = meta_index_schema();

        // Store the relation in the meta-table
        let mut tree = builder.build_tree_mut(self.meta_index, &schema);

        let tuple = Self::relation_as_meta_index_tuple(&relation, transaction_id)?;

        // Will replace existing relation if it exists.
        tree.upsert(self.meta_table, tuple.into_buildable_with_schema(&schema))?;

        let schema = meta_table_schema();
        let object_id = relation.object_id();

        // Store the relation in the meta-table
        let mut tree = builder.build_tree_mut(self.meta_table, &schema);

        let tuple = Self::relation_as_meta_table_tuple(&relation, transaction_id)?;

        // Will replace existing relation if it exists.
        tree.upsert(self.meta_table, tuple.into_buildable_with_schema(&schema))?;

        self.last_stored_object += 1;
        Ok(())
    }

    /// Gets a relation id from the meta index, looking up by name
    pub(crate) fn bind_relation(
        &self,
        name: &str,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<ObjectId> {
        let schema = meta_index_schema();
        let mut meta_index = builder.build_tree(self.meta_index, &schema);
        let name = Blob::from(name);
        let result = meta_index.search(name.key_bytes())?;

        match result {
            SearchResult::Found(pos) => {
                let row = meta_index
                    .get_row_at(pos, &schema, snapshot)?
                    .ok_or(CatalogError::InvalidObjectName(name.to_string()))?;

                let object_id = match &row[1] {
                    DataType::BigUInt(v) => v.value() as ObjectId,
                    _ => panic!("Invalid object_id type in meta index row"),
                };

                return Ok(object_id);
            }
            SearchResult::NotFound(_) => {
                return Err(CatalogError::InvalidObjectName(name.to_string()));
            }
        };
    }

    /// Gets a relation from the meta table
    pub(crate) fn get_relation(
        &self,
        id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree(self.meta_table, &schema);
        let row_id = UInt64(id);
        let result = meta_table.search(row_id.key_bytes())?;

        match result {
            SearchResult::Found(pos) => {
                let row = meta_table
                    .get_row_at(pos, &schema, snapshot)?
                    .ok_or(CatalogError::TableNotFound(id))?;
                return Ok(Relation::from_meta_table_row(row));
            }
            SearchResult::NotFound(_) => {
                return Err(CatalogError::TableNotFound(id));
            }
        };
    }

    /// Updates the metadata of a given relation using the provided RcPageAccessor.
    pub(crate) fn update_relation(
        &self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table, &schema);

        let tuple = Self::relation_as_meta_table_tuple(&relation, snapshot.xid())?;

        let result =
            meta_table.update(self.meta_table, tuple.into_buildable_with_schema(&schema))?;

        Ok(())
    }

    /// Removes a given relation using the provided RcPageAccessor.
    /// Cascades the removal if required.
    pub(crate) fn remove_relation(
        &self,
        rel: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        cascade: bool,
    ) -> CatalogResult<()> {
        if cascade {
            let schema = rel.schema();
            let dependants = schema.get_dependants();

            // Recursively remove object dependants
            for dep in dependants {
                let relation = self.get_relation(dep, builder, snapshot)?;
                self.remove_relation(relation, builder, snapshot, cascade)?;
            }
        };

        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table, &schema);

        let schema = meta_index_schema();
        let mut meta_index = builder.build_tree_mut(self.meta_index, &schema);

        let blob = Blob::from(rel.name());
        let row_id = UInt64::from(rel.object_id());
        meta_index.remove(self.meta_index, blob.key_bytes())?;
        meta_table.remove(self.meta_table, row_id.key_bytes())?;

        Ok(())
    }

    /// Recomputes the statistics for a single item in the catalog
    fn recompute_relation_stats(
        &self,
        id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        sample_rate: f64,
        max_sample_rows: usize,
    ) -> CatalogResult<()> {
        let relation = self.get_relation(id, builder, snapshot)?;
        let schema = relation.schema();
        let root = relation.root();
        let mut table = builder.build_tree(root, &schema);

        // Initialize collectors for each column
        let mut collectors: Vec<StatsCollector> = schema
            .iter_columns()
            .enumerate()
            .map(|(i, col)| StatsCollector::new(i, col))
            .collect();

        let mut row_count: u64 = 0;
        let mut total_row_size: u64 = 0;
        let mut page_set: HashSet<PageId> = HashSet::new();

        // Determine sample threshold
        let sample_threshold = if sample_rate < 1.0 {
            (1.0 / sample_rate) as u64
        } else {
            1
        };

        // Not sure if this approach will work as the iterator needs to acquire the RcPageAccessor mutably.
        for (i, position) in table.iter_forward()?.enumerate() {
            if let Ok(pos) = position {
                // Track pages
                page_set.insert(pos.entry());

                row_count += 1;

                // Apply sampling
                if sample_threshold > 1 && row_count % sample_threshold != 0 {
                    continue;
                }

                // Check max sample rows
                if max_sample_rows > 0 && row_count > max_sample_rows as u64 {
                    break;
                }

                // Process the tuple
                table.with_cell_at(pos, |bytes| {
                    total_row_size += bytes.len() as u64;

                    let reader = TupleReader::from_schema(&schema);
                    if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                        let tuple = TupleRef::new(bytes, layout);
                        let accessor = RefTupleAccessor::new(tuple, &schema);

                        // Collect key column stats
                        for i in 0..schema.num_keys() {
                            if let Ok(value) = accessor.key(i) {
                                collectors[i].observe(value);
                            }
                        }

                        // Collect value column stats
                        for i in 0..schema.num_values() {
                            let col_idx = schema.num_keys as usize + i;
                            if let Ok(value) = accessor.value(i) {
                                collectors[col_idx].observe(value);
                            }
                        }
                    }
                    Ok::<(), TupleError>(())
                })??;
            } else {
                return Err(CatalogError::Other(format!(
                    "Unable to compute stats for table {}. Failed at iteration {}",
                    id, i
                )));
            }
        }

        // Finalize statistics
        let sampled_rows = if sample_rate < 1.0 {
            (row_count as f64 * sample_rate) as u64
        } else {
            row_count
        };

        let avg_row_size = if sampled_rows > 0 {
            total_row_size as f64 / sampled_rows as f64
        } else {
            0.0
        };

        let mut stats = Stats::new()
            .with_row_count(row_count)
            .with_page_count(page_set.len())
            .with_avg_row_size(avg_row_size);

        stats.last_updated = current_timestamp();

        // Add column statistics
        for collector in collectors {
            let col_idx = collector.column_index();
            let col_stats = collector.finalize();
            stats.add_column_stats(col_idx, col_stats);
        }

        let updated_relation = relation.with_stats(stats);
        self.update_relation(updated_relation, &builder, &snapshot)?;

        Ok(())
    }

    /// Recomputes statistics for all the tables in the catalog
    pub(crate) fn analyze(
        &self,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        sample_rate: f64,
        max_sample_rows: usize,
    ) -> CatalogResult<()> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree(self.meta_table, &schema);

        let mut relations_to_analyze: Vec<ObjectId> = Vec::new();

        // Collect all relations from the meta table
        for iter_result in meta_table.iter_forward()? {
            if let Ok(pos) = iter_result {
                meta_table.with_cell_at(pos, |bytes| {
                    let reader = TupleReader::from_schema(&schema);
                    if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                        let tuple = TupleRef::new(bytes, layout);
                        let accessor = RefTupleAccessor::new(tuple, &schema);
                        if let Ok(DataTypeRef::BigUInt(value)) = accessor.key(0) {
                            relations_to_analyze.push(value.value())
                        }
                    }
                    Ok::<(), TupleError>(())
                })??;
            }
        }

        for id in relations_to_analyze {
            self.recompute_relation_stats(id, builder, snapshot, sample_rate, max_sample_rows)?;
        }

        Ok(())
    }
}
