use crate::{
    ObjectId, SerializationError, TransactionId, UInt64,
    core::SerializableType,
    io::pager::BtreeBuilder,
    make_shared,
    multithreading::coordinator::Snapshot,
    storage::tuple::{RefTupleAccessor, Tuple, TupleBuilder, TupleError, TupleReader, TupleRef},
    tree::{
        accessor::BtreePagePosition,
        bplustree::{BtreeError, SearchResult},
    },
    types::{Blob, DataType, DataTypeRef, PageId},
};

use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    Stats,
    base::Relation,
    meta_index_schema, meta_table_schema,
    stats::{ColumnStats, StatsCollector},
};
#[cfg(test)]
use std::collections::HashMap;
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
            Self::Other(string) => write!(f, "{string}"),
        }
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

impl std::error::Error for CatalogError {}
type CatalogResult<T> = Result<T, CatalogError>;

/// Gets current Unix timestamp.
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug)]
pub struct Catalog {
    meta_table: PageId,
    meta_index: PageId,
    last_stored_object: AtomicU64,
}

impl Catalog {
    pub(crate) fn new(meta_table: PageId, meta_index: PageId) -> Self {
        Self {
            meta_table,
            meta_index,
            last_stored_object: AtomicU64::new(0),
        }
    }

    pub(crate) fn with_last_stored_object(self, last_stored: ObjectId) -> Self {
        self.last_stored_object
            .store(last_stored, Ordering::Relaxed);
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
        let schema = meta_index_schema();
        let builder = TupleBuilder::from_schema(&schema);
        let t = builder.build(relation.to_meta_index_row(), transaction_id)?;

        Ok(t)
    }

    /// Recomputes the statistics for a single item in the catalog
    fn recompute_relation_stats(
        &mut self,
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
        &mut self,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
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
                            let accessor = RefTupleAccessor::new(tuple, &schema);
                            if let Ok(DataTypeRef::BigUInt(value)) = accessor.key(0) {
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

            let updated_relation = relation.with_stats(stats);
            self.update_relation(updated_relation, &builder, &snapshot)?;
        }

        Ok(())
    }
}

make_shared!(SharedCatalog, Catalog);

impl CatalogTrait for Catalog {
    /// Updates the metadata of a given relation using the provided RcPageAccessor.
    fn update_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()> {
        let schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table);

        let tuple = Self::relation_as_meta_table_tuple(&relation, snapshot.xid())?;

        let result = meta_table.update(self.meta_table, tuple, &schema)?;

        Ok(())
    }

    /// Removes a given relation using the provided RcPageAccessor.
    /// Cascades the removal if required.
    fn remove_relation(
        &mut self,
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

        let table_schema = meta_table_schema();
        let mut meta_table = builder.build_tree_mut(self.meta_table);

        let index_schema = meta_index_schema();
        let mut meta_index = builder.build_tree_mut(self.meta_index);

        let blob = Blob::from(rel.name());
        let row_id = UInt64::from(rel.object_id());
        meta_index.remove(self.meta_index, blob.as_ref(), &index_schema)?;
        meta_table.remove(self.meta_table, &row_id.serialize()?, &table_schema)?;

        Ok(())
    }

    /// Stores a new relation in the meta table.
    fn store_relation(
        &mut self,
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
    fn bind_relation(
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

    fn get_next_object_id(&self) -> ObjectId {
        self.last_stored_object.fetch_add(1, Ordering::Relaxed)
    }

    /// Gets a relation from the meta table
    fn get_relation(
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

impl CatalogTrait for SharedCatalog {
    fn store_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        transaction_id: TransactionId,
    ) -> CatalogResult<()> {
        self.write()
            .store_relation(relation, builder, transaction_id)
    }

    fn remove_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        cascade: bool,
    ) -> CatalogResult<()> {
        self.write()
            .remove_relation(relation, builder, snapshot, cascade)
    }

    fn update_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()> {
        self.write().update_relation(relation, builder, snapshot)
    }
    fn get_relation(
        &self,
        id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        self.read().get_relation(id, builder, snapshot)
    }

    fn bind_relation(
        &self,
        name: &str,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<ObjectId> {
        self.read().bind_relation(name, builder, snapshot)
    }

    fn get_next_object_id(&self) -> ObjectId {
        self.read().get_next_object_id()
    }
}

pub trait CatalogTrait {
    fn store_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        transaction_id: TransactionId,
    ) -> CatalogResult<()>;

    fn get_relation(
        &self,
        id: ObjectId,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation>;

    fn bind_relation(
        &self,
        name: &str,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<ObjectId>;

    fn update_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()>;

    fn remove_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        cascade: bool,
    ) -> CatalogResult<()>;

    fn get_relation_by_name(
        &self,
        name: &str,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        let id = self.bind_relation(name, builder, snapshot)?;
        self.get_relation(id, builder, snapshot)
    }

    fn get_next_object_id(&self) -> ObjectId;
}

#[cfg(test)]
pub type TestMetaTable = HashMap<ObjectId, Relation>;

#[cfg(test)]
pub type TestMetaIndex = HashMap<String, ObjectId>;

// Mock test catalog for tests
#[cfg(test)]
#[derive(Clone)]
pub struct MemCatalog {
    meta_table: TestMetaTable,
    meta_index: TestMetaIndex,
}

#[cfg(test)]
impl MemCatalog {
    pub fn new() -> Self {
        Self {
            meta_table: HashMap::new(),
            meta_index: HashMap::new(),
        }
    }
}

#[cfg(test)]
impl CatalogTrait for MemCatalog {
    fn bind_relation(
        &self,
        name: &str,
        _builder: &BtreeBuilder,
        _snapshot: &Snapshot,
    ) -> CatalogResult<ObjectId> {
        let object = self
            .meta_index
            .get(name)
            .ok_or(CatalogError::InvalidObjectName(name.to_string()))?;
        Ok(*object)
    }

    fn get_next_object_id(&self) -> ObjectId {
        self.meta_table.len() as u64
    }

    fn get_relation(
        &self,
        id: ObjectId,
        _builder: &BtreeBuilder,
        _snapshot: &Snapshot,
    ) -> CatalogResult<Relation> {
        let object = self
            .meta_table
            .get(&id)
            .ok_or(CatalogError::TableNotFound(id))?;
        Ok(object.clone())
    }

    fn store_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        transaction_id: TransactionId,
    ) -> CatalogResult<()> {
        self.meta_index
            .insert(relation.name().to_string(), relation.object_id());
        self.meta_table.insert(relation.object_id(), relation);

        Ok(())
    }

    fn update_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
    ) -> CatalogResult<()> {
        self.store_relation(relation, builder, snapshot.xid())
    }

    fn remove_relation(
        &mut self,
        relation: Relation,
        builder: &BtreeBuilder,
        snapshot: &Snapshot,
        cascade: bool,
    ) -> CatalogResult<()> {
        self.meta_table.remove(&relation.object_id());
        self.meta_index.remove(relation.name());
        Ok(())
    }
}

#[derive(Clone)]
pub struct StatisticsProvider<C: CatalogTrait + Clone> {
    catalog: C,
    builder: BtreeBuilder,
    snapshot: Snapshot,
}

impl<C> StatisticsProvider<C>
where
    C: CatalogTrait + Clone,
{
    pub fn new(catalog: C, builder: BtreeBuilder, snapshot: Snapshot) -> Self {
        Self {
            catalog,
            builder,
            snapshot,
        }
    }
}

impl<C: CatalogTrait + Clone> StatsProvider for StatisticsProvider<C> {
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

// Test para Catalog::analyze (añadir en catalog.rs dentro de #[cfg(test)])
#[cfg(test)]
mod analyze_tests {
    use super::*;
    use crate::{
        DBConfig,
        io::pager::{Pager, SharedPager},
        multithreading::coordinator::TransactionCoordinator,
        schema::base::Column,
        snapshot,
        storage::page::BtreePage,
        types::DataTypeKind,
    };
    use tempfile::TempDir;

    fn setup_test_db() -> (TempDir, SharedPager, BtreeBuilder, TransactionCoordinator) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let pager = Pager::from_config(DBConfig::default(), &path).unwrap();
        let pager = SharedPager::from(pager);

        let tree_builder = {
            let p = pager.read();
            BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                .with_pager(pager.clone())
        };

        let coordinator = TransactionCoordinator::new(pager.clone());

        (dir, pager, tree_builder, coordinator)
    }

    #[test]
    fn test_catalog_analyze_empty() {
        let (_dir, pager, tree_builder, coordinator) = setup_test_db();

        // Allocate meta pages
        let (meta_table, meta_index) = {
            let mut p = pager.write();
            let mt = p.allocate_page::<BtreePage>().unwrap();
            let mi = p.allocate_page::<BtreePage>().unwrap();
            (mt, mi)
        };

        let mut catalog = Catalog::new(meta_table, meta_index);
        let snapshot = snapshot!(xid: 1, xmin: 0);

        // Analyze on empty catalog should succeed
        let result = catalog.analyze(&tree_builder, &snapshot, 1.0, 10000);
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_catalog_analyze_with_table() {
        let (_dir, pager, tree_builder, coordinator) = setup_test_db();

        // Allocate meta pages
        let (meta_table, meta_index, table_root) = {
            let mut p = pager.write();
            let mt = p.allocate_page::<BtreePage>().unwrap();
            let mi = p.allocate_page::<BtreePage>().unwrap();
            let tr = p.allocate_page::<BtreePage>().unwrap();
            (mt, mi, tr)
        };

        let mut catalog = Catalog::new(meta_table, meta_index);
        let snapshot = snapshot!(xid: 1, xmin: 0);

        // Create a test table schema
        let relation = Relation::table(
            1,
            "test_table",
            table_root,
            vec![
                Column::new_with_defaults(DataTypeKind::BigInt, "id"),
                Column::new_with_defaults(DataTypeKind::Blob, "name"),
            ],
        );

        // Store the relation
        catalog
            .store_relation(relation, &tree_builder, snapshot.xid())
            .unwrap();

        // Now analyze
        let result = catalog.analyze(&tree_builder, &snapshot, 1.0, 10000);

        assert!(result.is_ok());
    }
}
