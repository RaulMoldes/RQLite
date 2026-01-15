use rkyv::Serialize;

use crate::{
    ObjectId, SerializationError, TransactionId, UInt64,
    core::SerializableType,
    io::pager::{BtreeBuilder, SharedPager},
    make_shared_readonly,
    multithreading::coordinator::Snapshot,
    runtime::{RuntimeError, context::TransactionLogger},
    schema::{Schema, base::SchemaError},
    storage::tuple::{Tuple, TupleBuilder, TupleError, TupleReader, TupleRef},
    tree::{
        accessor::BtreePagePosition,
        bplustree::{BtreeError, SearchResult},
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

#[derive(Debug)]
pub enum CatalogError {
    TableNotFound(ObjectId),
    InvalidObjectName(String),
    BtreeError(BtreeError),
    TupleError(TupleError),
    Serialization(SerializationError),
    Schema(SchemaError),
    Other(String),
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::TableNotFound(id) => write!(f, "Table not found {id}"),
            Self::InvalidObjectName(name) => write!(f, "object with name {name}does not exist"),
            Self::BtreeError(err) => write!(f, "Btree error: {err}"),
            Self::TupleError(err) => write!(f, "Tuple error {err}"),
            Self::Serialization(err) => write!(f, "serialization error {err}"),
            Self::Schema(err) => write!(f, "schema error {err}"),
            Self::Other(string) => write!(f, "{string}"),
        }
    }
}

impl From<SchemaError> for CatalogError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
    }
}

impl From<BtreeError> for CatalogError {
    fn from(value: BtreeError) -> Self {
        Self::BtreeError(value)
    }
}
impl From<SerializationError> for CatalogError {
    fn from(value: SerializationError) -> Self {
        Self::Serialization(value)
    }
}
impl From<TupleError> for CatalogError {
    fn from(value: TupleError) -> Self {
        Self::TupleError(value)
    }
}

impl From<RuntimeError> for CatalogError {
    fn from(value: RuntimeError) -> Self {
        Self::Other(value.to_string())
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

/// Statistics from a vacuum operation.
#[derive(Debug, Default, Clone)]
pub struct VacuumStats {
    /// Number of user tables vacuumed
    pub tables_vacuumed: usize,
    /// Total bytes freed from user table tuples
    pub total_bytes_freed: usize,
    /// Bytes freed from meta table
    pub meta_table_bytes_freed: usize,
    /// Bytes freed from meta index
    pub meta_index_bytes_freed: usize,
    /// Number of transactions cleaned up from coordinator
    pub transactions_cleaned: usize,
}

impl VacuumStats {
    pub fn total_freed(&self) -> usize {
        self.total_bytes_freed + self.meta_table_bytes_freed + self.meta_index_bytes_freed
    }
}

make_shared_readonly!(SharedCatalog, Catalog);

pub struct Catalog {
    meta_table: PageId,
    meta_index: PageId,
    pager: SharedPager,
}

impl Catalog {
    pub(crate) fn new(pager: SharedPager, meta_table: PageId, meta_index: PageId) -> Self {
        Self {
            meta_table,
            meta_index,
            pager,
        }
    }

    /// Converts a relation into a meta table tuple for storage.
    pub(crate) fn relation_as_meta_table_tuple(
        relation: &Relation,
        transaction_id: TransactionId,
    ) -> CatalogResult<Tuple> {
        let schema = meta_table_schema();
        let builder = TupleBuilder::from_schema(&schema);
        let t = builder.build(&relation.to_meta_table_row(), transaction_id)?;

        Ok(t)
    }

    /// Converts a relation into a meta table tuple for storage.
    pub(crate) fn relation_as_meta_index_tuple(
        relation: &Relation,
        transaction_id: TransactionId,
    ) -> CatalogResult<Tuple> {
        let schema = meta_index_schema();
        let builder = TupleBuilder::from_schema(&schema);
        let row = relation.to_meta_index_row();
        let t = builder.build(&row, transaction_id)?;

        Ok(t)
    }

    /// Recomputes the statistics for a single item in the catalog
    fn recompute_relation_stats(
        &self,
        relation: &Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        sample_rate: f64,
        max_sample_rows: usize,
    ) -> CatalogResult<Option<Stats>> {
        let schema = relation.schema();
        let root = relation.root();

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

        let mut table = builder.build_tree(root);

        if table.is_empty()? {
            return Ok(None);
        };

        for (i, position) in table.iter_forward()?.enumerate() {
            let pos: BtreePagePosition = position?;
            // Track pages
            let entry: PageId = pos.entry();
            page_set.insert(entry);

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
            table.with_cell_at(pos, |bytes: &[u8]| {
                total_row_size += bytes.len() as u64;

                let reader = TupleReader::from_schema(&schema);
                if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                    let tuple = TupleRef::new(bytes, layout);

                    // Collect key column stats
                    for i in 0..schema.num_keys() {
                        if let Ok(value) = tuple.key_with(i, &schema) {
                            collectors[i].observe(value);
                        }
                    }

                    // Collect value column stats
                    for i in 0..schema.num_values() {
                        let col_idx = schema.num_keys as usize + i;
                        if let Ok(value) = tuple.value_with(i, &schema) {
                            collectors[col_idx].observe(value);
                        }
                    }
                }
                Ok::<(), TupleError>(())
            })??;
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

        Ok(Some(stats))
    }

    /// Recomputes statistics for all the tables in the catalog
    pub(crate) fn analyze(
        &self,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        logger: Option<&TransactionLogger>,
        sample_rate: f64,
        max_sample_rows: usize,
    ) -> CatalogResult<()> {
        let schema = meta_table_schema();

        let mut relations_to_analyze: Vec<ObjectId> = Vec::new();
        {
            let mut meta_table = builder.build_tree(self.meta_table);

            if meta_table.is_empty()? {
                return Ok(());
            }

            // Collect all relations from the meta table
            for iter_result in meta_table.iter_forward()? {
                if let Ok(pos) = iter_result {
                    meta_table.with_cell_at(pos, |bytes| {
                        let reader = TupleReader::from_schema(&schema);
                        if let Some(layout) = reader.parse_for_snapshot(bytes, &snapshot)? {
                            let tuple = TupleRef::new(bytes, layout);

                            if let Ok(DataTypeRef::BigUInt(value)) = tuple.key_with(0, &schema) {
                                relations_to_analyze.push(value.value())
                            }
                        }
                        Ok::<(), TupleError>(())
                    })??;
                }
            }
        }

        for id in relations_to_analyze {
            let relation = self.get_relation(id, builder, snapshot)?;
            let Some(stats) = self.recompute_relation_stats(
                &relation,
                builder,
                snapshot,
                sample_rate,
                max_sample_rows,
            )?
            else {
                continue;
            };

            self.update_relation(id, None, None, Some(stats), &builder, &snapshot)?;
        }

        Ok(())
    }

    /// Vacuums all tuples in all relations, removing old MVCC versions
    /// that are no longer needed by any transaction.
    ///
    /// Returns statistics about the vacuum operation.
    pub fn vacuum(
        &self,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        oldest_active_xid: TransactionId,
    ) -> CatalogResult<VacuumStats> {
        let mut stats = VacuumStats::default();

        // Collect all user relations
        let schema = meta_table_schema();
        let relations_to_vacuum: Vec<(ObjectId, PageId, Schema)> = {
            let mut meta_table = builder.build_tree(self.meta_table);

            if meta_table.is_empty()? {
                return Ok(stats);
            }

            let mut relations = Vec::new();

            for iter_result in meta_table.iter_forward()? {
                if let Ok(pos) = iter_result {
                    meta_table.with_cell_at(pos, |bytes| {
                        let reader = TupleReader::from_schema(&schema);
                        if let Some(layout) = reader.parse_for_snapshot(bytes, snapshot)? {
                            let tuple = TupleRef::new(bytes, layout);
                            let row = tuple.to_row_with(&schema)?;
                            let relation = Relation::from_meta_table_row(row);

                            relations.push((
                                relation.object_id(),
                                relation.root(),
                                relation.schema().clone(),
                            ));
                        }
                        Ok::<(), TupleError>(())
                    })??;
                }
            }

            relations
        };

        // Vacuum each user relation
        for (object_id, root, relation_schema) in relations_to_vacuum {
            let bytes_freed =
                self.vacuum_btree(root, &relation_schema, builder, snapshot, oldest_active_xid)?;

            stats.tables_vacuumed += 1;
            stats.total_bytes_freed += bytes_freed;
        }

        // vacuum the meta table itself
        stats.meta_table_bytes_freed += self.vacuum_btree(
            self.meta_table,
            &meta_table_schema(),
            builder,
            snapshot,
            oldest_active_xid,
        )?;

        // Vacuum the meta index
        stats.meta_index_bytes_freed += self.vacuum_btree(
            self.meta_index,
            &meta_index_schema(),
            builder,
            snapshot,
            oldest_active_xid,
        )?;

        Ok(stats)
    }

    /// Vacuums a single B-tree, iterating through all tuples and removing
    /// old versions.
    fn vacuum_btree(
        &self,
        root: PageId,
        schema: &Schema,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        oldest_active_xid: TransactionId,
    ) -> CatalogResult<usize> {
        let mut tuples_to_vaccum = Vec::new();
        let mut tuples_to_remove = Vec::new();
        let mut total_freed = 0;
        {
            let mut tree = builder.build_tree(root);

            if tree.is_empty()? {
                return Ok(0);
            }

            // Vacuum each tuple
            for position in tree.iter_forward()? {
                if let Ok(pos) = position {
                    tree.with_cell_at(pos, |bytes| {
                        let mut tuple = Tuple::from_slice_unchecked(bytes)?;
                        let xmin = tuple.xmin();

                        let freed = if snapshot.is_transaction_aborted(xmin) || tuple.is_deleted() {
                            let freed = tuple.full_data().len();
                            tuples_to_remove.push(tuple);
                            freed
                        } else {
                            let freed = tuple.vaccum_with(oldest_active_xid, schema)?;
                            if freed > 0 {
                                tuples_to_vaccum.push(tuple);
                            };
                            freed
                        };

                        total_freed += freed;
                        Ok::<(), TupleError>(())
                    })??;
                }
            }
        }

        let mut tree = builder.build_tree_mut(root);

        for tuple in tuples_to_vaccum {
            tree.update(tree.get_root(), tuple, &schema)?;
        }

        for tuple in tuples_to_remove {
            tree.remove_tuple(tree.get_root(), &tuple, &schema)?;
        }

        Ok(total_freed)
    }

    pub(crate) fn get_relation_by_name(
        &self,
        name: &str,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        let id = self.bind_relation(name, builder, snapshot)?;
        self.get_relation(id, builder, snapshot)
    }
    /// Updates the metadata of a given relation using the provided RcPageAccessor.
    pub(crate) fn update_relation(
        &self,
        relation_id: ObjectId,
        new_row_id: Option<u64>,
        new_schema: Option<Schema>,
        new_stats: Option<Stats>,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table);
        let relation_id_bytes = UInt64(relation_id).serialize()?;
        let mut changes = std::collections::HashMap::new();

        if let Some(id) = new_row_id {
            let col_id = schema.bind_value("next_row_id")?;
            changes.insert(col_id, DataType::BigUInt(UInt64::from(id)));
        }

        if let Some(table_schema) = new_schema {
            let col_id = schema.bind_value("schema")?;
            changes.insert(col_id, DataType::Blob(table_schema.to_blob()?));
        }

        if let Some(table_stats) = new_stats {
            let col_id = schema.bind_value("stats")?;
            changes.insert(col_id, DataType::Blob(table_stats.to_blob()?));
        }

        let position = match meta_table.search(&relation_id_bytes, &schema)? {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(()),
        };

        // Read the existing tuple
        let tuple_reader = TupleReader::from_schema(&schema);
        let existing_tuple = meta_table.with_cell_at(position, |bytes| {
            tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
            Tuple::from_slice_unchecked(bytes).ok()
        })?;

        let Some(mut tuple) = existing_tuple else {
            return Ok(());
        };

        tuple.add_version_with(&changes, snapshot.xid(), &schema)?;
        let result = meta_table.update(self.meta_table, tuple, &schema)?;

        Ok(())
    }

    /// Removes a given relation using the provided
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

        // First, deallocate the relation.
        // Deallocate the relation.
        {   println!("About to deallocate tree");
            let mut tree = builder.build_tree_mut(rel.root());
            tree.dealloc()?;
        }

        // Obtain the relation metadata
        let relation_id = rel.object_id();
        let relation_id_bytes = UInt64(relation_id).serialize()?;
        let relation_name_bytes = Blob::from(rel.name()).serialize()?;

        // First remove the relation from the meta table
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table);

        let position = match meta_table.search(&relation_id_bytes, &schema)? {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(()),
        };

        // Read the existing tuple
        let tuple_reader = TupleReader::from_schema(&schema);
        let existing_tuple = meta_table.with_cell_at(position, |bytes| {
            tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
            Tuple::from_slice_unchecked(bytes).ok()
        })?;

        let Some(mut tuple) = existing_tuple else {
            return Ok(());
        };

        // Get the old row for index maintenance
        let old_row = meta_table
            .get_row_at(position, &schema, &snapshot)?
            .expect("Tuple should exist");

        tuple.delete(snapshot.xid())?;
        meta_table.update(self.meta_table, tuple, &schema)?;

        // Now do the same with the meta index.
        let index_schema = meta_index_schema();
        let mut meta_index = builder.build_tree_mut(self.meta_index);

        let position = match meta_index.search(&relation_name_bytes, &index_schema)? {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return Ok(()),
        };

        // Read the existing tuple
        let tuple_reader = TupleReader::from_schema(&index_schema);
        let existing_tuple = meta_table.with_cell_at(position, |bytes| {
            tuple_reader.parse_for_snapshot(bytes, &snapshot).ok()??;
            Tuple::from_slice_unchecked(bytes).ok()
        })?;

        let Some(mut tuple) = existing_tuple else {
            return Ok(());
        };

        // Get the old row for index maintenance
        let old_row = meta_index
            .get_row_at(position, &index_schema, &snapshot)?
            .expect("Tuple should exist");

        // Apply the update
        tuple.delete(snapshot.xid())?;
        meta_index.update(self.meta_index, tuple, &index_schema)?;

        Ok(())
    }

    /// Removes a given relation using the provided id
    /// Cascades the removal if required.
    pub(crate) fn remove_relation_by_id(
        &self,
        relation_id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        cascade: bool,
        logger: Option<&TransactionLogger>,
    ) -> CatalogResult<()> {
        let rel = self.get_relation(relation_id, builder, snapshot)?;
        self.remove_relation(rel, builder, snapshot, cascade)
    }

    /// Stores a new relation in the meta table.
    pub(crate) fn store_relation(
        &self,
        relation: Relation,
        builder: &BtreeBuilder,
        transaction_id: TransactionId,
    ) -> CatalogResult<()> {
        // Store the relation in the meta index.
        let schema = meta_index_schema();

        // Store the relation in the meta-table
        let mut tree = builder.build_tree_mut(self.meta_index);

        let tuple = Self::relation_as_meta_index_tuple(&relation, transaction_id)?;

        // Will replace existing relation if it exists.
        tree.upsert(self.meta_index, tuple, &schema)?;

        let schema = meta_table_schema();
        let object_id = relation.object_id();

        // Store the relation in the meta-table
        let mut tree = builder.build_tree_mut(self.meta_table);
        let tuple = Self::relation_as_meta_table_tuple(&relation, transaction_id)?;

        // Will replace existing relation if it exists.
        tree.upsert(self.meta_table, tuple, &schema)?;

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
        let mut meta_index = builder.build_tree(self.meta_index);
        let name = Blob::from(name);
        let result = meta_index.search(name.as_ref(), &schema)?;

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

    pub(crate) fn get_next_object_id(&self) -> ObjectId {
        // Atomically get and increment the object ID in PageZero
        let object_id = {
            let mut pager = self.pager.write();
            let current = pager.get_last_stored_object();
            pager.set_last_stored_object(current + 1);
            current
        };
        object_id
    }

    /// Gets a relation from the meta table
    pub(crate) fn get_relation(
        &self,
        id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree(self.meta_table);

        let row_id = UInt64(id).serialize()?;

        let result = meta_table.search(row_id.as_ref(), &schema)?;

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
}

#[derive(Clone)]
pub struct StatisticsProvider<'a> {
    catalog: SharedCatalog,
    builder: BtreeBuilder,
    snapshot: &'a Snapshot,
}

impl<'a> StatisticsProvider<'a> {
    pub fn new(catalog: SharedCatalog, builder: BtreeBuilder, snapshot: &'a Snapshot) -> Self {
        Self {
            catalog,
            builder,
            snapshot,
        }
    }
}

impl<'a> StatsProvider for StatisticsProvider<'a> {
    fn get_stats(&self, object: ObjectId) -> Option<Stats> {
        let relation = self
            .catalog
            .get_relation(object, &self.builder, &self.snapshot)
            .ok()?;
        relation.stats().cloned()
    }

    fn get_column_stats(&self, object: ObjectId, column_index: usize) -> Option<ColumnStats> {
        let relation = self
            .catalog
            .get_relation(object, &self.builder, &self.snapshot)
            .ok()?;
        relation
            .stats()
            .map(|s| s.get_column_stats(column_index).cloned())
            .flatten()
    }
}

pub trait StatsProvider {
    fn get_stats(&self, object: ObjectId) -> Option<Stats>;
    fn get_column_stats(&self, object: ObjectId, column_index: usize) -> Option<ColumnStats>;
}
