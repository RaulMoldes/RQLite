use super::base::Column;
use crate::{
    DEFAULT_BTREE_MIN_KEYS, DEFAULT_PAGE_SIZE, DataType,
    storage::{BtreeOps, page::BtreePage},
    tree::{
        cell_ops::KeyBytes,
        comparators::{Comparator, DynComparator, Ranger, VarlenComparator},
    },
    types::{Blob, DataTypeKind, DataTypeRef, SerializationError, SerializationResult},
};
use rkyv::{
    Archive, Deserialize, Serialize, from_bytes, rancor::Error as RkyvError, to_bytes,
    util::AlignedVec,
};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};
/// Discrete selectivity values for easier estimation.
#[derive(Default, Copy, Clone, PartialEq)]
pub enum Selectivity {
    High = 4,
    Mid = 3,
    Low = 2,
    Empty = 1,
    #[default]
    Uncomputable = 0,
}

impl PartialOrd for Selectivity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (*self as u8).partial_cmp(&(*other as u8))
    }
}

impl From<f64> for Selectivity {
    fn from(value: f64) -> Self {
        if value < 0.05 {
            Selectivity::Empty
        } else if value < 0.33 {
            Selectivity::Low
        } else if value < 0.66 {
            Selectivity::Mid
        } else if value < 1.0 {
            Selectivity::High
        } else {
            Selectivity::Uncomputable
        }
    }
}

/// Statistics collector that accumulates values during a scan.
pub(crate) struct StatsCollector {
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
    distinct_hashes: HashSet<u128>,

    // For histogram building
    sample_values: Vec<f64>,
}

impl StatsCollector {
    pub fn new(column_idx: usize, column: &Column) -> Self {
        Self {
            column_idx,
            dtype: column.datatype(),
            comparator: column
                .comparator()
                .unwrap_or(DynComparator::Variable(VarlenComparator)),
            null_count: 0,
            total_count: 0,
            total_len: 0,
            min_bytes: None,
            max_bytes: None,
            distinct_hashes: HashSet::new(),
            sample_values: Vec::new(),
        }
    }

    pub fn column_index(&self) -> usize {
        self.column_idx
    }

    /// Records a value observation.
    pub fn observe(&mut self, value: DataTypeRef<'_>) {
        self.total_count += 1;

        if matches!(value, DataTypeRef::Null) {
            self.null_count += 1;
            return;
        }

        // Booleans are not tracked since it is not safe to use them in comparisons
        if matches!(self.dtype, DataTypeKind::Bool) {
            return;
        }

        // Get the raw bytes for comparison
        let bytes: &[u8] = value.as_bytes();

        // Update length tracking for variable-length types
        self.total_len += bytes.len() as u64;

        // Update min
        match &self.min_bytes {
            None => self.min_bytes = Some(Box::from(bytes)),
            Some(current_min) => {
                if matches!(
                    self.comparator
                        .compare(KeyBytes::from(bytes), KeyBytes::from(current_min.as_ref())),
                    Ok(Ordering::Less)
                ) {
                    self.min_bytes = Some(Box::from(bytes));
                }
            }
        }

        // Update max
        match &self.max_bytes {
            None => self.max_bytes = Some(Box::from(bytes)),
            Some(current_max) => {
                if matches!(
                    self.comparator
                        .compare(KeyBytes::from(bytes), KeyBytes::from(current_max.as_ref())),
                    Ok(Ordering::Greater)
                ) {
                    self.max_bytes = Some(Box::from(bytes));
                }
            }
        }

        // Track distinct values via hash
        let hash = value.hash128();
        self.distinct_hashes.insert(hash);
    }

    /// Finalizes collection and produces ColumnStatistics.
    pub fn finalize(self) -> ColumnStats {
        let ndv = self.distinct_hashes.len() as u64;

        let avg_len = if self.total_count > self.null_count {
            Some(self.total_len / (self.total_count - self.null_count))
        } else {
            None
        };

        let mut stats = ColumnStats::new(ndv, self.null_count);

        if let (Some(min), Some(max)) = (self.min_bytes, self.max_bytes) {
            stats = stats.with_range_bytes(min, max);
        }

        if let Some(avg) = avg_len {
            stats = stats.with_avg_len(avg);
        }

        stats
    }
}

/// Statistics for a single column.
///
/// Tracks value distribution information used for selectivity estimation.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// Number of distinct values (cardinality)
    pub ndv: u64,
    /// Average value length in the column.
    pub avg_len: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value in serialized byte form
    pub min_bytes: Option<Box<[u8]>>,
    /// Maximum value in serialized byte form
    pub max_bytes: Option<Box<[u8]>>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            ndv: 100,
            avg_len: BtreePage::ideal_max_payload_size(DEFAULT_PAGE_SIZE, DEFAULT_BTREE_MIN_KEYS)
                as u64,
            null_count: 0,
            min_bytes: None,
            max_bytes: None,
        }
    }
}

impl ColumnStats {
    /// Creates new column statistics with the given NDV and null count.
    pub fn new(ndv: u64, null_count: u64) -> Self {
        Self {
            ndv,
            null_count,
            ..Default::default()
        }
    }

    /// Sets the min/max byte ranges.
    pub fn with_range_bytes(mut self, min: Box<[u8]>, max: Box<[u8]>) -> Self {
        self.min_bytes = Some(min);
        self.max_bytes = Some(max);
        self
    }

    /// Sets the avg len of a column
    pub fn with_avg_len(mut self, avg_len: u64) -> Self {
        self.avg_len = avg_len;
        self
    }

    /// Estimates selectivity for equality predicate.
    ///
    /// Uses the number of distinct values to estimate the probability
    /// of matching a specific value.
    pub fn selectivity_equality(&self, row_count: f64) -> Selectivity {
        if self.ndv > 0 {
            Selectivity::from(1.0 / self.ndv as f64)
        } else if row_count > 0.0 {
            Selectivity::from(1.0 / row_count)
        } else {
            Selectivity::Uncomputable
        }
    }

    /// Estimates selectivity for a range predicate using byte-based comparison.
    ///
    /// This method uses the `Ranger` trait to compute byte differences between
    /// boundaries, allowing accurate range selectivity for any comparable type.
    ///
    /// # Arguments
    ///
    /// * `low` - Optional lower bound (inclusive)
    /// * `high` - Optional upper bound (inclusive)
    /// * `ranger` - A type implementing both `Comparator` and `Ranger`
    ///
    /// # Returns
    ///
    /// Estimated fraction of rows in the range (0.0 to 1.0)
    pub(crate) fn selectivity_range<R: Comparator + Ranger>(
        &self,
        low: KeyBytes<'_>,
        high: KeyBytes<'_>,
        ranger: &R,
    ) -> Selectivity {
        ranger
            .selectivity_range(low, high)
            .unwrap_or(Selectivity::Uncomputable)
    }

    /// Estimates null selectivity.
    pub fn selectivity_null(&self, row_count: f64) -> Selectivity {
        if row_count > 0.0 {
            Selectivity::from(self.null_count as f64 / row_count)
        } else {
            Selectivity::Empty
        }
    }

    /// Updates statistics with a new value.
    ///
    /// Used during statistics collection to incrementally track min/max/count.
    pub(crate) fn update_with_value<C: Comparator>(&mut self, value: KeyBytes<'_>, comparator: &C) {
        // Update min
        match &self.min_bytes {
            None => self.min_bytes = Some(value.to_vec().into_boxed_slice()),
            Some(current_min) => {
                if matches!(
                    comparator.compare(value, KeyBytes::from(current_min.as_ref())),
                    Ok(Ordering::Less)
                ) {
                    self.min_bytes = Some(value.to_vec().into_boxed_slice());
                }
            }
        }

        // Update max
        match &self.max_bytes {
            None => self.max_bytes = Some(value.to_vec().into_boxed_slice()),
            Some(current_max) => {
                if matches!(
                    comparator.compare(value, KeyBytes::from(current_max.as_ref())),
                    Ok(Ordering::Greater)
                ) {
                    self.max_bytes = Some(value.to_vec().into_boxed_slice());
                }
            }
        }
    }
}

/// Statistics for a table (stored in catalog metadata).
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Stats {
    /// Total row count
    pub row_count: u64,
    /// Number of pages used by the table
    pub page_count: usize,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Per-column statistics
    pub column_stats: HashMap<usize, ColumnStats>,
    /// Timestamp of last statistics update (Unix epoch)
    pub last_updated: u64,
}

impl Stats {
    /// Creates empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: sets row count.
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = count;
        self
    }

    /// Builder: sets page count.
    pub fn with_page_count(mut self, count: usize) -> Self {
        self.page_count = count;
        self
    }

    /// Builder: sets average row size.
    pub fn with_avg_row_size(mut self, size: f64) -> Self {
        self.avg_row_size = size;
        self
    }

    /// Adds statistics for a column.
    pub fn add_column_stats(&mut self, column_idx: usize, stats: ColumnStats) {
        self.column_stats.insert(column_idx, stats);
    }

    /// Gets statistics for a column.
    pub fn get_column_stats(&self, column_idx: usize) -> Option<&ColumnStats> {
        self.column_stats.get(&column_idx)
    }

    /// Merges another Stats into this one.
    ///
    /// Useful for combining statistics from multiple scans.
    pub fn merge(&mut self, other: &Stats) {
        self.row_count = self.row_count.max(other.row_count);
        self.page_count = self.page_count.max(other.page_count);

        if other.avg_row_size > 0.0 {
            self.avg_row_size = other.avg_row_size;
        }

        for (idx, stats) in &other.column_stats {
            self.column_stats.insert(*idx, stats.clone());
        }

        self.last_updated = self.last_updated.max(other.last_updated);
    }

    /// Serializes an stats object to a blob object
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
        let stats = from_bytes::<Stats, RkyvError>(data)?;
        Ok(stats)
    }
}
