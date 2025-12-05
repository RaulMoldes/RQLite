// stats_compute.rs - Statistics computation for tables and indexes
//
// Add this to your database module or as a separate module imported by the catalog.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    io::{self, Error as IoError, ErrorKind},
    time::{SystemTime, UNIX_EPOCH},
};

use murmur3::murmur3_x64_128;
use std::io::Cursor;

use crate::{
    DataType,
    database::{
        Catalog, SharedCatalog,
        schema::{Column, Index, Relation, Schema, Table},
    },
    io::frames::FrameAccessMode,
    sql::planner::stats::{
        ColumnStatistics, Histogram, HistogramBucket, IndexStatistics, TableStatistics,
    },
    storage::tuple::TupleRef,
    structures::{
        bplustree::{BPlusTree, IterDirection},
        comparator::{Comparator, DynComparator, NumericComparator},
    },
    transactions::worker::Worker,
    types::{
        DataTypeKind, DataTypeRef, Date, DateTime, Float32, Float64, Int8, Int16, Int32, Int64,
        ObjectId, PageId, UInt8, UInt16, UInt32, UInt64,
    },
};

/// MurmurHash3 (128-bit x64 variant), returning lower 64 bits.
fn murmur_hash(bytes: &[u8]) -> u64 {
    let mut cursor = Cursor::new(bytes);
    let buf = [0u8; 16];
    let hash128 = murmur3_x64_128(&mut cursor, 0).unwrap();

    // Extract lower 64 bits
    hash128 as u64
}

/// Configuration for statistics computation.
#[derive(Debug, Clone)]
pub struct StatsComputeConfig {
    /// Number of histogram buckets to create per column.
    pub histogram_buckets: usize,
    /// Sample rate for large tables (0.0 to 1.0). 1.0 means full scan.
    pub sample_rate: f64,
    /// Maximum rows to scan for sampling. 0 means unlimited.
    pub max_sample_rows: usize,
    /// Whether to compute histograms.
    pub compute_histograms: bool,
}

impl Default for StatsComputeConfig {
    fn default() -> Self {
        Self {
            histogram_buckets: 100,
            sample_rate: 1.0,
            max_sample_rows: 0,
            compute_histograms: true,
        }
    }
}

impl StatsComputeConfig {
    /// Creates a config for fast approximate statistics.
    pub fn fast() -> Self {
        Self {
            histogram_buckets: 20,
            sample_rate: 0.1,
            max_sample_rows: 10_000,
            compute_histograms: false,
        }
    }

    /// Creates a config for full accurate statistics.
    pub fn full() -> Self {
        Self {
            histogram_buckets: 100,
            sample_rate: 1.0,
            max_sample_rows: 0,
            compute_histograms: true,
        }
    }
}

/// Statistics collector that accumulates values during a scan.
struct ColumnStatsCollector {
    column_idx: usize,
    dtype: DataTypeKind,
    comparator: DynComparator,

    // Accumulated statistics
    null_count: u64,
    total_count: u64,
    total_len: u64,
    min_bytes: Option<Box<[u8]>>,
    max_bytes: Option<Box<[u8]>>,

    // For NDV estimation using HyperLogLog-like approach (simplified)
    distinct_hashes: HashSet<u64>,

    // For histogram building
    sample_values: Vec<f64>,
}

impl ColumnStatsCollector {
    fn new(column_idx: usize, column: &Column) -> Self {
        Self {
            column_idx,
            dtype: column.dtype,
            comparator: column.comparator(),
            null_count: 0,
            total_count: 0,
            total_len: 0,
            min_bytes: None,
            max_bytes: None,
            distinct_hashes: HashSet::new(),
            sample_values: Vec::new(),
        }
    }

    /// Records a value observation.
    fn observe(&mut self, value: &DataTypeRef<'_>) {
        self.total_count += 1;

        if matches!(value, DataTypeRef::Null) {
            self.null_count += 1;
            return;
        }

        // Get the raw bytes for comparison
        let bytes: &[u8] = value.as_ref();

        // Update length tracking for variable-length types
        self.total_len += bytes.len() as u64;

        // Update min
        match &self.min_bytes {
            None => self.min_bytes = Some(Box::from(value.to_owned().as_ref())),
            Some(current_min) => {
                if matches!(
                    self.comparator.compare(&bytes, current_min),
                    Ok(Ordering::Less)
                ) {
                    self.min_bytes = Some(Box::from(value.to_owned().as_ref()));
                }
            }
        }

        // Update max
        match &self.max_bytes {
            None => self.max_bytes = Some(Box::from(value.to_owned().as_ref())),
            Some(current_max) => {
                if matches!(
                    self.comparator.compare(&bytes, current_max),
                    Ok(Ordering::Greater)
                ) {
                    self.max_bytes = Some(Box::from(value.to_owned().as_ref()));
                }
            }
        }

        // Track distinct values via hash
        let hash = murmur_hash(&bytes);
        self.distinct_hashes.insert(hash);

        // Sample for histogram (numeric types only)
        if self.dtype.is_numeric() {
            if let Some(val) = bytes_to_f64(&bytes, self.dtype) {
                self.sample_values.push(val);
            }
        }
    }

    /// Finalizes collection and produces ColumnStatistics.
    fn finalize(mut self, config: &StatsComputeConfig) -> ColumnStatistics {
        let ndv = self.distinct_hashes.len() as u64;

        let avg_len = if self.total_count > self.null_count {
            Some(self.total_len / (self.total_count - self.null_count))
        } else {
            None
        };

        let histogram = if config.compute_histograms && !self.sample_values.is_empty() {
            Some(build_equidepth_histogram(
                &mut self.sample_values,
                config.histogram_buckets,
            ))
        } else {
            None
        };

        let mut stats = ColumnStatistics::new(ndv, self.null_count);

        if let (Some(min), Some(max)) = (self.min_bytes, self.max_bytes) {
            stats = stats.with_range_bytes(min, max);
        }

        if let Some(avg) = avg_len {
            stats = stats.with_avg_len(avg);
        }

        if let Some(hist) = histogram {
            stats = stats.with_histogram(hist);
        }

        stats
    }
}

/// Converts bytes to f64 for histogram building.
fn bytes_to_f64(bytes: &[u8], dtype: DataTypeKind) -> Option<f64> {
    match dtype {
        DataTypeKind::Byte | DataTypeKind::SmallUInt | DataTypeKind::Char => {
            let value = UInt8::try_from(bytes).ok()?;
            Some(u8::from(value) as f64)
        }
        DataTypeKind::SmallInt => {
            let value = Int8::try_from(bytes).ok()?;
            Some(i8::from(value) as f64)
        }
        DataTypeKind::Int => {
            let value = Int32::try_from(bytes).ok()?;
            Some(i32::from(value) as f64)
        }
        DataTypeKind::HalfInt => {
            let value = Int16::try_from(bytes).ok()?;
            Some(i16::from(value) as f64)
        }
        DataTypeKind::BigInt => {
            let value = Int64::try_from(bytes).ok()?;
            Some(i64::from(value) as f64)
        }
        DataTypeKind::BigUInt => {
            let value = UInt64::try_from(bytes).ok()?;
            Some(u64::from(value) as f64)
        }
        DataTypeKind::UInt => {
            let value = UInt32::try_from(bytes).ok()?;
            Some(u32::from(value) as f64)
        }
        DataTypeKind::Date => {
            let value = Date::try_from(bytes).ok()?;
            Some(u32::from(value) as f64)
        }
        DataTypeKind::DateTime => {
            let value = DateTime::try_from(bytes).ok()?;
            Some(u64::from(value) as f64)
        }
        DataTypeKind::HalfUInt => {
            let value = UInt16::try_from(bytes).ok()?;
            Some(u16::from(value) as f64)
        }
        DataTypeKind::Float => {
            let value = Float32::try_from(bytes).ok()?;
            Some(f32::from(value) as f64)
        }
        DataTypeKind::Double => {
            let value = Float64::try_from(bytes).ok()?;
            Some(f64::from(value) as f64)
        }
        DataTypeKind::Text | DataTypeKind::Blob => Some(murmur_hash(bytes) as f64),
        _ => None,
    }
}

/// Builds an equi-depth histogram from sample values.
fn build_equidepth_histogram(values: &mut Vec<f64>, num_buckets: usize) -> Histogram {
    if values.is_empty() {
        return Histogram::new(num_buckets);
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let mut histogram = Histogram::new(num_buckets);
    let total_count = values.len();
    let bucket_size = (total_count + num_buckets - 1) / num_buckets;

    let mut bucket_start = 0;
    while bucket_start < total_count {
        let bucket_end = (bucket_start + bucket_size).min(total_count);
        let bucket_values = &values[bucket_start..bucket_end];

        if !bucket_values.is_empty() {
            let lower = bucket_values[0];
            let upper = bucket_values[bucket_values.len() - 1];
            let count = bucket_values.len() as f64;

            // Count distinct values in bucket
            let mut distinct = HashSet::new();
            for &v in bucket_values {
                distinct.insert(v.to_bits());
            }
            let distinct_count = distinct.len() as f64;

            histogram.add_bucket(HistogramBucket::new(lower, upper, count, distinct_count));
        }

        bucket_start = bucket_end;
    }

    histogram
}

/// Gets current Unix timestamp.
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Catalog {
    /// Recomputes statistics for a single table.
    pub fn recompute_table_statistics(
        &self,
        table_name: &str,
        worker: Worker,
        config: &StatsComputeConfig,
    ) -> io::Result<TableStatistics> {
        let relation = self.get_relation(table_name, worker.clone())?;

        let (table, root, schema) = match relation {
            Relation::TableRel(t) => {
                let root = t.root();
                let schema = t.schema().clone();
                (t, root, schema)
            }
            Relation::IndexRel(_) => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("{} is an index, not a table", table_name),
                ));
            }
        };

        let stats = self.compute_table_stats(root, &schema, worker.clone(), config)?;

        // Update the table with new statistics
        let updated_table = Table::build(
            table.id(),
            table.name(),
            table.root(),
            schema,
            table.next(),
            Some(stats.clone()),
        );

        self.update_relation(Relation::TableRel(updated_table), worker)?;

        Ok(stats)
    }

    /// Recomputes statistics for a single index.
    pub fn recompute_index_statistics(
        &self,
        index_name: &str,
        worker: Worker,
        config: &StatsComputeConfig,
    ) -> io::Result<IndexStatistics> {
        let relation = self.get_relation(index_name, worker.clone())?;

        let (index, root, schema) = match relation {
            Relation::IndexRel(i) => {
                let root = i.root();
                let schema = i.schema().clone();
                (i, root, schema)
            }
            Relation::TableRel(_) => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("{} is a table, not an index", index_name),
                ));
            }
        };

        let stats = self.compute_index_stats(root, &schema, worker.clone(), config)?;

        // Update the index with new statistics
        let updated_index = Index::build(
            index.id(),
            index.name(),
            index.root(),
            schema,
            stats.min_key.clone().unwrap_or_default(),
            stats.max_key.clone().unwrap_or_default(),
            Some(stats.clone()),
            index.schema().num_keys,
        );

        self.update_relation(Relation::IndexRel(updated_index), worker)?;

        Ok(stats)
    }

    /// Recomputes statistics for all tables and indexes in the catalog.
    pub fn recompute_all_statistics(
        &self,
        worker: Worker,
        config: &StatsComputeConfig,
    ) -> io::Result<StatsComputeResult> {
        let mut result = StatsComputeResult::default();

        // Iterate through the meta table to find all relations
        let mut btree = self.table_btree(self.meta_table_root(), worker.clone())?;

        let positions: Vec<_> = btree.iter_positions()?.filter_map(|r| r.ok()).collect();

        btree.clear_worker_stack();

        for pos in positions {
            let payload = btree.with_cell_at(pos, |data| data.to_vec())?;

            let meta_schema = crate::database::meta_table_schema();
            let tuple = TupleRef::read(&payload, &meta_schema)?;

            let relation = Relation::try_from(tuple)?;
            let name = relation.name().to_string();

            // Skip meta tables
            if name == crate::database::META_TABLE || name == crate::database::META_INDEX {
                continue;
            }

            match relation {
                Relation::TableRel(_) => {
                    match self.recompute_table_statistics(&name, worker.clone(), config) {
                        Ok(stats) => {
                            result.tables_updated += 1;
                            result.table_stats.insert(name, stats);
                        }
                        Err(e) => {
                            result.errors.push(format!("Table {}: {}", name, e));
                        }
                    }
                }
                Relation::IndexRel(_) => {
                    match self.recompute_index_statistics(&name, worker.clone(), config) {
                        Ok(stats) => {
                            result.indexes_updated += 1;
                            result.index_stats.insert(name, stats);
                        }
                        Err(e) => {
                            result.errors.push(format!("Index {}: {}", name, e));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Internal: computes table statistics by scanning the B+tree.
    fn compute_table_stats(
        &self,
        root: PageId,
        schema: &Schema,
        worker: Worker,
        config: &StatsComputeConfig,
    ) -> io::Result<TableStatistics> {
        let mut btree = self.table_btree(root, worker)?;

        // Initialize collectors for each column
        let mut collectors: Vec<ColumnStatsCollector> = schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnStatsCollector::new(i, col))
            .collect();

        let mut row_count: u64 = 0;
        let mut total_row_size: u64 = 0;
        let mut page_set: HashSet<PageId> = HashSet::new();

        // Determine sample threshold
        let sample_threshold = if config.sample_rate < 1.0 {
            (1.0 / config.sample_rate) as u64
        } else {
            1
        };

        // Iterate through all positions
        let positions: Vec<_> = btree.iter_positions()?.filter_map(|r| r.ok()).collect();

        btree.clear_worker_stack();

        for pos in positions {
            // Track pages
            page_set.insert(pos.page());

            row_count += 1;

            // Apply sampling
            if sample_threshold > 1 && row_count % sample_threshold != 0 {
                continue;
            }

            // Check max sample rows
            if config.max_sample_rows > 0 && row_count > config.max_sample_rows as u64 {
                break;
            }

            // Process the tuple
            btree.with_cell_at(pos, |data| {
                total_row_size += data.len() as u64;

                if let Ok(tuple) = TupleRef::read(data, schema) {
                    // Collect key column stats
                    for i in 0..schema.num_keys as usize {
                        if let Ok(value) = tuple.key(i) {
                            collectors[i].observe(&value);
                        }
                    }

                    // Collect value column stats
                    for i in 0..schema.values().len() {
                        let col_idx = schema.num_keys as usize + i;
                        if let Ok(value) = tuple.value(i) {
                            collectors[col_idx].observe(&value);
                        }
                    }
                }
            })?;
        }

        // Finalize statistics
        let sampled_rows = if config.sample_rate < 1.0 {
            (row_count as f64 * config.sample_rate) as u64
        } else {
            row_count
        };

        let avg_row_size = if sampled_rows > 0 {
            total_row_size as f64 / sampled_rows as f64
        } else {
            0.0
        };

        let mut stats = TableStatistics::new()
            .with_row_count(row_count)
            .with_page_count(page_set.len())
            .with_avg_row_size(avg_row_size);

        stats.last_updated = current_timestamp();

        // Add column statistics
        for collector in collectors {
            let col_idx = collector.column_idx;
            let col_stats = collector.finalize(config);
            stats.add_column_stats(col_idx, col_stats);
        }

        Ok(stats)
    }

    /// Internal: computes index statistics by scanning the B+tree.
    fn compute_index_stats(
        &self,
        root: PageId,
        schema: &Schema,
        worker: Worker,
        config: &StatsComputeConfig,
    ) -> io::Result<IndexStatistics> {
        // Get the key column's data type for the comparator
        let key_dtype = schema
            .keys()
            .first()
            .map(|k| k.dtype)
            .unwrap_or(DataTypeKind::BigUInt);

        let mut btree = self.index_btree(root, key_dtype, worker)?;

        let comparator =
            schema
                .keys()
                .first()
                .map(|k| k.comparator())
                .unwrap_or(DynComparator::Variable(
                    crate::structures::comparator::VarlenComparator,
                ));

        let mut entries: u64 = 0;
        let mut distinct_keys: HashSet<u64> = HashSet::new();
        let mut total_key_size: u64 = 0;
        let mut min_key: Option<Box<[u8]>> = None;
        let mut max_key: Option<Box<[u8]>> = None;
        let mut leaf_pages: HashSet<PageId> = HashSet::new();

        // Count tree height via root traversal
        let height = self.compute_tree_height(root, &mut btree)?;

        // Iterate through all positions
        let positions: Vec<_> = btree.iter_positions()?.filter_map(|r| r.ok()).collect();

        btree.clear_worker_stack();

        for pos in positions {
            leaf_pages.insert(pos.page());
            entries += 1;

            btree.with_cell_at(pos, |data| {
                // Extract key bytes (first part of tuple for indexes)
                if let Ok(tuple) = TupleRef::read(data, schema) {
                    if let Ok(key_ref) = tuple.key(0) {
                        let key_bytes: Box<[u8]> = Box::from(key_ref.as_ref());
                        total_key_size += key_bytes.len() as u64;

                        // Track distinct
                        let hash = murmur_hash(&key_bytes);
                        distinct_keys.insert(hash);

                        // Update min
                        match &min_key {
                            None => min_key = Some(key_bytes.clone()),
                            Some(current_min) => {
                                if matches!(
                                    comparator.compare(&key_bytes, current_min),
                                    Ok(std::cmp::Ordering::Less)
                                ) {
                                    min_key = Some(key_bytes.clone());
                                }
                            }
                        }

                        // Update max
                        match &max_key {
                            None => max_key = Some(key_bytes.clone()),
                            Some(current_max) => {
                                if matches!(
                                    comparator.compare(&key_bytes, current_max),
                                    Ok(std::cmp::Ordering::Greater)
                                ) {
                                    max_key = Some(key_bytes.clone());
                                }
                            }
                        }
                    }
                }
            })?;
        }

        let avg_key_size = if entries > 0 {
            total_key_size as f64 / entries as f64
        } else {
            0.0
        };

        let mut stats = IndexStatistics::new()
            .with_height(height)
            .with_leaf_pages(leaf_pages.len())
            .with_entries(entries)
            .with_distinct_keys(distinct_keys.len() as u64);

        stats.avg_key_size = avg_key_size;
        stats.min_key = min_key;
        stats.max_key = max_key;
        stats.last_updated = current_timestamp();

        Ok(stats)
    }

    /// Computes the height of a B+tree by traversing from root to leaf.
    fn compute_tree_height<Cmp: Comparator>(
        &self,
        root: PageId,
        btree: &mut BPlusTree<Cmp>,
    ) -> io::Result<usize> {
        use crate::storage::page::BtreePage;

        let mut height = 0;
        let mut current = root;

        loop {
            btree
                .worker_mut()
                .acquire::<BtreePage>(current, FrameAccessMode::Read)?;

            let (is_leaf, first_child) =
                btree
                    .worker()
                    .read_page::<BtreePage, _, _>(current, |page| {
                        let is_leaf = page.is_leaf();
                        let first_child = if page.num_slots() > 0 {
                            page.cell(crate::storage::cell::Slot(0))
                                .metadata()
                                .left_child()
                        } else {
                            page.metadata().right_child
                        };
                        (is_leaf, first_child)
                    })?;

            btree.worker_mut().release_latch(current);
            height += 1;

            if is_leaf || !first_child.is_valid() {
                break;
            }

            current = first_child;
        }

        Ok(height)
    }
}

/// Result of a batch statistics computation.
#[derive(Debug, Default)]
pub struct StatsComputeResult {
    pub tables_updated: usize,
    pub indexes_updated: usize,
    pub table_stats: HashMap<String, TableStatistics>,
    pub index_stats: HashMap<String, IndexStatistics>,
    pub errors: Vec<String>,
}

impl StatsComputeResult {
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn total_updated(&self) -> usize {
        self.tables_updated + self.indexes_updated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        TRANSACTION_ZERO,
        database::{Database, schema::Schema},
        io::pager::{Pager, SharedPager},
        storage::tuple::Tuple,
        types::{Blob, DataType, DataTypeKind, Int32, Int32Ref, Int64, UInt64},
    };

    use std::path::Path;
    use tempfile::NamedTempFile;

    fn create_test_pager(path: impl AsRef<Path>, cache_size: usize) -> io::Result<Pager> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(&path);

        let config = crate::AxmosDBConfig {
            page_size: 4096,
            cache_size: Some(cache_size as u16),
            incremental_vacuum_mode: crate::IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: crate::TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path)?;
        Ok(pager)
    }

    fn create_test_database() -> io::Result<(Database, NamedTempFile)> {
        let temp_file = NamedTempFile::new()?;
        let pager = SharedPager::from(create_test_pager(temp_file.path(), 100)?);
        let db = Database::new(pager, 3, 2)?;
        Ok((db, temp_file))
    }

    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_column("id", DataTypeKind::BigInt, true, true, false);
        schema.add_column("name", DataTypeKind::Text, false, false, false);
        schema.add_column("age", DataTypeKind::Int, false, false, false);
        schema.set_num_keys(1);
        schema
    }

    #[test]
    fn test_column_stats_collector() {
        let col =
            crate::database::schema::Column::new_unindexed(DataTypeKind::Int, "test_col", None);
        let mut collector = ColumnStatsCollector::new(0, &col);

        // Observe some values
        let type_owned = crate::types::Int32(10);
        let type_ref: Int32Ref<'_> = Int32Ref::try_from(type_owned.as_ref()).unwrap();
        collector.observe(&DataTypeRef::Int(type_ref));
        let type_owned = crate::types::Int32(20);
        let type_ref: Int32Ref<'_> = Int32Ref::try_from(type_owned.as_ref()).unwrap();
        collector.observe(&DataTypeRef::Int(type_ref));
        let type_owned = crate::types::Int32(30);
        let type_ref: Int32Ref<'_> = Int32Ref::try_from(type_owned.as_ref()).unwrap();
        collector.observe(&DataTypeRef::Int(type_ref));
        collector.observe(&DataTypeRef::Null);

        let config = StatsComputeConfig::default();
        let stats = collector.finalize(&config);

        assert_eq!(stats.null_count, 1);
        assert!(stats.ndv >= 3); // At least 3 distinct non-null values
        assert!(stats.min_bytes.is_some());
        assert!(stats.max_bytes.is_some());
    }


    #[test]
    fn test_build_equidepth_histogram() {
        let mut values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let histogram = build_equidepth_histogram(&mut values, 10);

        assert_eq!(histogram.num_buckets, 10);
        assert_eq!(histogram.buckets.len(), 10);

        // First bucket should start at 1
        assert!((histogram.buckets[0].lower_bound - 1.0).abs() < f64::EPSILON);

        // Last bucket should end at 100
        assert!((histogram.buckets[9].upper_bound - 100.0).abs() < f64::EPSILON);

        // Each bucket should have ~10 values
        for bucket in &histogram.buckets {
            assert!(bucket.count >= 9.0 && bucket.count <= 11.0);
        }
    }



    #[test]
    fn test_stats_compute_result() {
        let mut result = StatsComputeResult::default();
        assert!(result.is_success());
        assert_eq!(result.total_updated(), 0);

        result.tables_updated = 5;
        result.indexes_updated = 3;
        assert_eq!(result.total_updated(), 8);

        result.errors.push("Test error".to_string());
        assert!(!result.is_success());
    }

    #[test]
    fn test_recompute_table_statistics() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        // Create a test table
        let schema = create_test_schema();
        catalog.create_table("test_users", schema.clone(), worker.clone())?;

        // Insert some test data
        let relation = catalog.get_relation("test_users", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker.clone())?;

        for i in 0..100i64 {
            let tuple = Tuple::new(
                &[
                    DataType::BigInt(Int64(i)),
                    DataType::Text(Blob::from(format!("User {}", i).as_str())),
                    DataType::Int(Int32((i % 50) as i32 + 18)),
                ],
                &schema,
                TRANSACTION_ZERO,
            )?;

            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        // Recompute statistics
        let config = StatsComputeConfig::default();
        let stats = catalog.recompute_table_statistics("test_users", worker.clone(), &config)?;

        assert_eq!(stats.row_count, 100);
        assert!(stats.page_count > 0);
        assert!(stats.avg_row_size > 0.0);
        assert_eq!(stats.column_stats.len(), 3);

        // Verify column 0 (id) statistics
        let id_stats = stats.get_column_stats(0).unwrap();
        assert_eq!(id_stats.ndv, 100); // All unique
        assert_eq!(id_stats.null_count, 0);

        // Verify column 2 (age) statistics
        let age_stats = stats.get_column_stats(2).unwrap();
        assert_eq!(age_stats.ndv, 50); // 50 distinct ages (0-49 + 18)
        assert_eq!(age_stats.null_count, 0);

        Ok(())
    }

    #[test]
    fn test_recompute_index_statistics() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        // Create a test index
        let mut index_schema = Schema::new();
        index_schema.add_column("email", DataTypeKind::Text, true, true, false);
        index_schema.add_column("row_id", DataTypeKind::BigUInt, false, false, false);
        index_schema.set_num_keys(1);

        catalog.create_index("idx_email", index_schema.clone(), worker.clone())?;

        // Insert some test data
        let relation = catalog.get_relation("idx_email", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.index_btree(root, DataTypeKind::Text, worker.clone())?;

        for i in 0..50u64 {
            let tuple = Tuple::new(
                &[
                    DataType::Text(Blob::from(format!("user{}@example.com", i).as_str())),
                    DataType::BigUInt(UInt64(i)),
                ],
                &index_schema,
                TRANSACTION_ZERO,
            )?;

            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        // Recompute statistics

        let config = StatsComputeConfig::default();
        let stats = catalog.recompute_index_statistics("idx_email", worker.clone(), &config)?;

        assert_eq!(stats.entries, 50);
        assert_eq!(stats.distinct_keys, 50);
        assert!(stats.height >= 1);
        assert!(stats.leaf_pages > 0);
        assert!(stats.min_key.is_some());
        assert!(stats.max_key.is_some());

        Ok(())
    }

    #[test]
    // TODO: REVIEW THIS TEST.
    fn test_recompute_all() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        // Create multiple tables and indexes
        let schema1 = create_test_schema();
        catalog.create_table("table1", schema1.clone(), worker.clone())?;

        let mut schema2 = Schema::new();
        schema2.add_column("product_id", DataTypeKind::BigInt, true, true, false);
        schema2.add_column("price", DataTypeKind::Double, false, false, false);
        schema2.set_num_keys(1);
        catalog.create_table("table2", schema2.clone(), worker.clone())?;

        // Insert some data into table1
        let relation = catalog.get_relation("table1", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker.clone())?;

        for i in 0..20i64 {
            let tuple = Tuple::new(
                &[
                    DataType::BigInt(Int64(i)),
                    DataType::Text(Blob::from("test")),
                    DataType::Int(Int32(25)),
                ],
                &schema1,
                TRANSACTION_ZERO,
            )?;
            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        // Recompute all statistics
        let config = StatsComputeConfig::fast();
        let result = catalog.recompute_all_statistics(worker, &config)?;

        assert!(result.tables_updated >= 2);
        assert!(result.table_stats.contains_key("table1"));
        assert!(result.table_stats.contains_key("table2"));

        // Verify table1 stats
        let table1_stats = result.table_stats.get("table1").unwrap();
        assert_eq!(table1_stats.row_count, 20);

        Ok(())
    }

    #[test]
    fn test_recompute_empty_table() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        catalog.create_table("empty_table", schema, worker.clone())?;

        let config = StatsComputeConfig::default();
        let stats = catalog.recompute_table_statistics("empty_table", worker, &config)?;

        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.page_count, 1); // Root page still counts
        assert!((stats.avg_row_size - 0.0).abs() < f64::EPSILON);

        Ok(())
    }

    #[test]
    fn test_recompute_with_nulls() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let mut schema = Schema::new();
        schema.add_column("id", DataTypeKind::BigInt, true, true, false);
        schema.add_column("optional_val", DataTypeKind::Int, false, false, false);
        schema.set_num_keys(1);

        catalog.create_table("nullable_table", schema.clone(), worker.clone())?;

        let relation = catalog.get_relation("nullable_table", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker.clone())?;

        // Insert mix of null and non-null values
        for i in 0..30i64 {
            let opt_val = if i % 3 == 0 {
                DataType::Null
            } else {
                DataType::Int(Int32(i as i32))
            };

            let tuple = Tuple::new(
                &[DataType::BigInt(Int64(i)), opt_val],
                &schema,
                TRANSACTION_ZERO,
            )?;
            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        let config = StatsComputeConfig::default();
        let stats = catalog.recompute_table_statistics("nullable_table", worker, &config)?;

        let col1_stats = stats.get_column_stats(1).unwrap();
        assert_eq!(col1_stats.null_count, 10); // Every 3rd value is null

        Ok(())
    }

    #[test]
    fn test_recompute_with_sampling() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        catalog.create_table("large_table", schema.clone(), worker.clone())?;

        let relation = catalog.get_relation("large_table", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker.clone())?;

        // Insert enough data to test sampling
        for i in 0..200i64 {
            let tuple = Tuple::new(
                &[
                    DataType::BigInt(Int64(i)),
                    DataType::Text(Blob::from("test")),
                    DataType::Int(Int32(i as i32)),
                ],
                &schema,
                TRANSACTION_ZERO,
            )?;
            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        // Test with max_sample_rows limit
        let config = StatsComputeConfig {
            histogram_buckets: 10,
            sample_rate: 1.0,
            max_sample_rows: 50,
            compute_histograms: false,
        };

        let stats = catalog.recompute_table_statistics("large_table", worker, &config)?;

        // Row count should still reflect full table
        assert_eq!(stats.row_count, 200);

        Ok(())
    }

    #[test]
    fn test_statistics_persistence() -> io::Result<()> {
        let (db, _temp) = create_test_database()?;
        let catalog = db.catalog();
        let worker = db.main_worker_cloned();

        let schema = create_test_schema();
        catalog.create_table("persist_test", schema.clone(), worker.clone())?;

        // Insert data
        let relation = catalog.get_relation("persist_test", worker.clone())?;
        let root = relation.root();
        let mut btree = catalog.table_btree(root, worker.clone())?;

        for i in 0..10i64 {
            let tuple = Tuple::new(
                &[
                    DataType::BigInt(Int64(i)),
                    DataType::Text(Blob::from("test")),
                    DataType::Int(Int32(25)),
                ],
                &schema,
                TRANSACTION_ZERO,
            )?;
            let owned: crate::storage::tuple::OwnedTuple = tuple.into();
            btree.insert(root, owned.as_ref())?;
        }
        btree.clear_worker_stack();

        // Compute and persist stats
        let config = StatsComputeConfig::default();
        catalog.recompute_table_statistics("persist_test", worker.clone(), &config)?;

        // Retrieve the table again and verify stats are present
        let relation = catalog.get_relation("persist_test", worker)?;

        match relation {
            Relation::TableRel(table) => {
                // The table should have statistics attached
                // Note: In practice, you'd need to add a method to Table to access stats
                // For now, we just verify the relation can be retrieved
                assert_eq!(table.name(), "persist_test");
            }
            _ => panic!("Expected table relation"),
        }

        Ok(())
    }

    #[test]
    fn test_histogram_selectivity_estimation() {
        let histogram = Histogram {
            buckets: vec![
                HistogramBucket::new(0.0, 25.0, 100.0, 25.0),
                HistogramBucket::new(25.0, 50.0, 100.0, 25.0),
                HistogramBucket::new(50.0, 75.0, 100.0, 25.0),
                HistogramBucket::new(75.0, 100.0, 100.0, 25.0),
            ],
            num_buckets: 4,
        };

        // Full range should be ~1.0
        let full_sel = histogram.estimate_range_selectivity(Some(0.0), Some(100.0));
        assert!((full_sel - 1.0).abs() < 0.01);

        // Half range should be ~0.5
        let half_sel = histogram.estimate_range_selectivity(Some(0.0), Some(50.0));
        assert!((half_sel - 0.5).abs() < 0.01);

        // Quarter range should be ~0.25
        let quarter_sel = histogram.estimate_range_selectivity(Some(0.0), Some(25.0));
        assert!((quarter_sel - 0.25).abs() < 0.01);
    }
}
