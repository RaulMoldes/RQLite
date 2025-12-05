//! Statistics module for query cost estimation.
//!
//! This module provides statistics collection and estimation capabilities for
//! database tables and indexes. Statistics are used by the query optimizer to
//! estimate cardinalities and select optimal execution plans.
//!
//! # Key Components
//!
//! - `ColumnStatistics`: Per-column statistics including NDV, null counts, and value ranges
//! - `TableStatistics`: Table-level statistics aggregating column stats
//! - `IndexStatistics`: B-tree index statistics (height, leaf pages, etc.)
//! - `Histogram`: Equi-depth histogram for value distribution estimation
//!
//! # Range Calculations
//!
//! Statistics support byte-based range calculations using the `Ranger` trait from
//! the comparator module. This allows selectivity estimation for any comparable
//! data type without requiring conversion to floating point.

use crate::{
    io::AsBytes,
    sql::{
        ast::{BinaryOperator, UnaryOperator},
        binder::ast::BoundExpression,
    },
    structures::comparator::{Comparator, Ranger},
    types::{ObjectId, VarInt, varint::MAX_VARINT_LEN},
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{self, Read, Seek, Write},
};

/// Statistics about a query plan node.
///
/// Used during query optimization to estimate costs and cardinalities.
#[derive(Debug, Clone, Default)]
pub struct Statistics {
    /// Estimated row count
    pub row_count: f64,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Column statistics indexed by column position
    pub column_stats: HashMap<usize, ColumnStatistics>,
}

impl Statistics {
    /// Creates new statistics with the given row count and average row size.
    pub fn new(row_count: f64, avg_row_size: f64) -> Self {
        Self {
            row_count,
            avg_row_size,
            column_stats: HashMap::new(),
        }
    }

    /// Adds column statistics for a specific column index.
    pub fn with_column_stats(mut self, column_idx: usize, stats: ColumnStatistics) -> Self {
        self.column_stats.insert(column_idx, stats);
        self
    }

    /// Estimates selectivity for a predicate expression.
    ///
    /// Returns a value between 0.0 and 1.0 representing the fraction of rows
    /// expected to satisfy the predicate.
    pub fn estimate_selectivity(&self, predicate: &BoundExpression) -> f64 {
        match predicate {
            BoundExpression::BinaryOp {
                op, left, right, ..
            } => match op {
                BinaryOperator::Eq => 0.1,
                BinaryOperator::Neq => 0.9,
                BinaryOperator::Lt
                | BinaryOperator::Gt
                | BinaryOperator::Le
                | BinaryOperator::Ge => 0.33,
                BinaryOperator::And => {
                    self.estimate_selectivity(left) * self.estimate_selectivity(right)
                }
                BinaryOperator::Or => {
                    let l = self.estimate_selectivity(left);
                    let r = self.estimate_selectivity(right);
                    (l + r - l * r).min(1.0)
                }
                _ => 0.5,
            },
            BoundExpression::UnaryOp { op, expr, .. } => match op {
                UnaryOperator::Not => 1.0 - self.estimate_selectivity(expr),
                _ => 0.5,
            },
            BoundExpression::IsNull { negated, .. } => {
                if *negated {
                    0.95
                } else {
                    0.05
                }
            }
            BoundExpression::Between { negated, .. } => {
                let base = 0.25;
                if *negated { 1.0 - base } else { base }
            }
            BoundExpression::InList { list, negated, .. } => {
                let list_selectivity = (list.len() as f64) * 0.01;
                let sel = list_selectivity.min(0.5);
                if *negated { 1.0 - sel } else { sel }
            }
            _ => 0.5,
        }
    }
}

/// Statistics for a single column.
///
/// Tracks value distribution information used for selectivity estimation.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    /// Number of distinct values (cardinality)
    pub ndv: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value in serialized byte form
    pub min_bytes: Option<Box<[u8]>>,
    /// Maximum value in serialized byte form
    pub max_bytes: Option<Box<[u8]>>,
    /// Average length for variable-length types
    pub avg_len: Option<u64>,
    /// Optional histogram for detailed distribution
    pub histogram: Option<Histogram>,
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self {
            ndv: 100,
            null_count: 0,
            min_bytes: None,
            max_bytes: None,
            avg_len: None,
            histogram: None,
        }
    }
}

impl ColumnStatistics {
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

    /// Sets the average length for variable-length columns.
    pub fn with_avg_len(mut self, avg_len: u64) -> Self {
        self.avg_len = Some(avg_len);
        self
    }

    /// Attaches a histogram for detailed distribution.
    pub fn with_histogram(mut self, histogram: Histogram) -> Self {
        self.histogram = Some(histogram);
        self
    }

    /// Estimates selectivity for equality predicate.
    ///
    /// Uses the number of distinct values to estimate the probability
    /// of matching a specific value.
    pub fn selectivity_equality(&self, row_count: f64) -> f64 {
        if self.ndv > 0 {
            1.0 / self.ndv as f64
        } else if row_count > 0.0 {
            1.0 / row_count
        } else {
            0.1
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
    pub fn selectivity_range<R: Comparator + Ranger>(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        ranger: &R,
    ) -> f64 {
        let (stat_min, stat_max) = match (&self.min_bytes, &self.max_bytes) {
            (Some(min), Some(max)) => (min.as_ref(), max.as_ref()),
            _ => return 0.33, // Default when bounds unknown
        };

        // Verify max > min
        if !matches!(ranger.compare(stat_max, stat_min), Ok(Ordering::Greater)) {
            return 0.33;
        }

        // Compute total range in bytes
        let total_range = match ranger.range_bytes(stat_max, stat_min) {
            Ok(r) => r,
            Err(_) => return 0.33,
        };

        // Determine effective bounds (clamp query bounds to stat bounds)
        let effective_low = match low {
            Some(l) => {
                if matches!(ranger.compare(l, stat_min), Ok(Ordering::Greater)) {
                    l
                } else {
                    stat_min
                }
            }
            None => stat_min,
        };

        let effective_high = match high {
            Some(h) => {
                if matches!(ranger.compare(h, stat_max), Ok(Ordering::Less)) {
                    h
                } else {
                    stat_max
                }
            }
            None => stat_max,
        };

        // Check if range is valid (low <= high)
        if matches!(
            ranger.compare(effective_low, effective_high),
            Ok(Ordering::Greater)
        ) {
            return 0.0; // Empty range
        }

        // Compute overlap range
        let overlap_range = match ranger.range_bytes(effective_high, effective_low) {
            Ok(r) => r,
            Err(_) => return 0.33,
        };

        // Convert ranges to comparable values for ratio calculation
        let total_val = bytes_to_estimation_value(&total_range);
        let overlap_val = bytes_to_estimation_value(&overlap_range);

        if total_val > 0.0 {
            (overlap_val / total_val).clamp(0.0, 1.0)
        } else {
            0.33
        }
    }

    /// Estimates null selectivity.
    pub fn selectivity_null(&self, row_count: f64) -> f64 {
        if row_count > 0.0 {
            self.null_count as f64 / row_count
        } else {
            0.05
        }
    }

    /// Updates statistics with a new value.
    ///
    /// Used during statistics collection to incrementally track min/max/count.
    pub fn update_with_value<C: Comparator>(&mut self, value: &[u8], comparator: &C) {
        // Update min
        match &self.min_bytes {
            None => self.min_bytes = Some(value.to_vec().into_boxed_slice()),
            Some(current_min) => {
                if matches!(comparator.compare(value, current_min), Ok(Ordering::Less)) {
                    self.min_bytes = Some(value.to_vec().into_boxed_slice());
                }
            }
        }

        // Update max
        match &self.max_bytes {
            None => self.max_bytes = Some(value.to_vec().into_boxed_slice()),
            Some(current_max) => {
                if matches!(
                    comparator.compare(value, current_max),
                    Ok(Ordering::Greater)
                ) {
                    self.max_bytes = Some(value.to_vec().into_boxed_slice());
                }
            }
        }
    }
}

/// Converts byte range to a floating point value for ratio estimation.
///
/// This provides a best-effort conversion for selectivity calculations.
/// It handles variable-length encoded values (with VarInt prefix) and
/// fixed-size values.
fn bytes_to_estimation_value(bytes: &[u8]) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }

    // Try to decode as VarInt-prefixed (variable length)
    if let Ok((len, offset)) = VarInt::from_encoded_bytes(bytes) {
        let len_usize: usize = match len.try_into() {
            Ok(l) => l,
            Err(_) => return bytes_to_raw_value(bytes),
        };

        if offset + len_usize <= bytes.len() {
            return bytes_to_raw_value(&bytes[offset..offset + len_usize]);
        }
    }

    // Treat as raw bytes
    bytes_to_raw_value(bytes)
}

/// Converts raw bytes to a floating point estimation value.
///
/// Uses big-endian interpretation for lexicographic consistency.
fn bytes_to_raw_value(bytes: &[u8]) -> f64 {
    let mut value: f64 = 0.0;
    let mut multiplier: f64 = 1.0;

    // Use first 8 bytes max to avoid precision loss
    for (i, &byte) in bytes.iter().take(8).enumerate() {
        value += (byte as f64) * multiplier;
        multiplier *= 256.0;
    }

    // Scale by remaining length to account for longer values
    if bytes.len() > 8 {
        value *= (bytes.len() - 8) as f64;
    }

    value
}

/// Equi-depth histogram for value distribution.
///
/// Histograms provide more accurate selectivity estimates than simple
/// min/max bounds by capturing the actual distribution of values.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    /// Histogram buckets
    pub buckets: Vec<HistogramBucket>,
    /// Target number of buckets
    pub num_buckets: usize,
}

impl Histogram {
    /// Creates a new histogram with the specified bucket count.
    pub fn new(num_buckets: usize) -> Self {
        Self {
            buckets: Vec::with_capacity(num_buckets),
            num_buckets,
        }
    }

    /// Adds a bucket to the histogram.
    pub fn add_bucket(&mut self, bucket: HistogramBucket) {
        self.buckets.push(bucket);
    }

    /// Estimates selectivity for a range query.
    pub fn estimate_range_selectivity(&self, low: Option<f64>, high: Option<f64>) -> f64 {
        if self.buckets.is_empty() {
            return 0.33;
        }

        let total_count: f64 = self.buckets.iter().map(|b| b.count).sum();
        if total_count == 0.0 {
            return 0.33;
        }

        let mut selected_count = 0.0;

        for bucket in &self.buckets {
            let bucket_low = bucket.lower_bound;
            let bucket_high = bucket.upper_bound;
            let bucket_range = bucket_high - bucket_low;

            let overlap_low = low.map(|l| l.max(bucket_low)).unwrap_or(bucket_low);
            let overlap_high = high.map(|h| h.min(bucket_high)).unwrap_or(bucket_high);

            if overlap_low < overlap_high && bucket_range > 0.0 {
                let overlap_fraction = (overlap_high - overlap_low) / bucket_range;
                selected_count += bucket.count * overlap_fraction;
            }
        }

        (selected_count / total_count).min(1.0)
    }

    /// Estimates selectivity for an equality query.
    pub fn estimate_equality_selectivity(&self, value: f64) -> f64 {
        if self.buckets.is_empty() {
            return 0.1;
        }

        let total_count: f64 = self.buckets.iter().map(|b| b.count).sum();
        if total_count == 0.0 {
            return 0.1;
        }

        for bucket in &self.buckets {
            if value >= bucket.lower_bound && value <= bucket.upper_bound {
                if bucket.distinct_count > 0.0 {
                    return (bucket.count / bucket.distinct_count) / total_count;
                }
            }
        }

        0.0
    }
}

/// A single histogram bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    /// Lower bound of the bucket (inclusive)
    pub lower_bound: f64,
    /// Upper bound of the bucket (inclusive)
    pub upper_bound: f64,
    /// Number of rows in this bucket
    pub count: f64,
    /// Number of distinct values in this bucket
    pub distinct_count: f64,
}

impl HistogramBucket {
    /// Creates a new histogram bucket.
    pub fn new(lower_bound: f64, upper_bound: f64, count: f64, distinct_count: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            count,
            distinct_count,
        }
    }
}

impl AsBytes for ColumnStatistics {
    fn write_to<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        buffer.write_all(&self.ndv.to_le_bytes())?;
        buffer.write_all(&self.null_count.to_le_bytes())?;

        // Write min_bytes
        if let Some(min) = &self.min_bytes {
            buffer.write_all(&[1u8])?;
            let vbuf = VarInt::encode(min.len() as i64, &mut varint_buf);
            buffer.write_all(vbuf)?;
            buffer.write_all(min)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // Write max_bytes
        if let Some(max) = &self.max_bytes {
            buffer.write_all(&[1u8])?;
            let vbuf = VarInt::encode(max.len() as i64, &mut varint_buf);
            buffer.write_all(vbuf)?;
            buffer.write_all(max)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // Write avg_len
        if let Some(avg) = self.avg_len {
            buffer.write_all(&[1u8])?;
            buffer.write_all(&avg.to_le_bytes())?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // Write histogram
        if let Some(hist) = &self.histogram {
            buffer.write_all(&[1u8])?;
            hist.write_to(buffer)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> io::Result<Self> {
        let mut buf8 = [0u8; 8];

        bytes.read_exact(&mut buf8)?;
        let ndv = u64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let null_count = u64::from_le_bytes(buf8);

        let mut flag = [0u8; 1];

        bytes.read_exact(&mut flag)?;
        let min_bytes = if flag[0] == 1 {
            let varint = VarInt::read_buf(bytes)?;
            let (len, _) = VarInt::from_encoded_bytes(&varint)?;
            let len_usize: usize = len.try_into()?;
            let mut data = vec![0u8; len_usize];
            bytes.read_exact(&mut data)?;
            Some(data.into_boxed_slice())
        } else {
            None
        };

        bytes.read_exact(&mut flag)?;
        let max_bytes = if flag[0] == 1 {
            let varint = VarInt::read_buf(bytes)?;
            let (len, _) = VarInt::from_encoded_bytes(&varint)?;
            let len_usize: usize = len.try_into()?;
            let mut data = vec![0u8; len_usize];
            bytes.read_exact(&mut data)?;
            Some(data.into_boxed_slice())
        } else {
            None
        };

        bytes.read_exact(&mut flag)?;
        let avg_len = if flag[0] == 1 {
            bytes.read_exact(&mut buf8)?;
            Some(u64::from_le_bytes(buf8))
        } else {
            None
        };

        bytes.read_exact(&mut flag)?;
        let histogram = if flag[0] == 1 {
            Some(Histogram::read_from(bytes)?)
        } else {
            None
        };

        Ok(Self {
            ndv,
            null_count,
            min_bytes,
            max_bytes,
            avg_len,
            histogram,
        })
    }
}

impl AsBytes for Histogram {
    fn write_to<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        let vbuf = VarInt::encode(self.num_buckets as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        let vbuf = VarInt::encode(self.buckets.len() as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        for bucket in &self.buckets {
            bucket.write_to(buffer)?;
        }

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> io::Result<Self> {
        let varint = VarInt::read_buf(bytes)?;
        let (num_buckets, _) = VarInt::from_encoded_bytes(&varint)?;

        let varint = VarInt::read_buf(bytes)?;
        let (bucket_count, _) = VarInt::from_encoded_bytes(&varint)?;
        let bucket_count_usize: usize = bucket_count.try_into()?;

        let mut buckets = Vec::with_capacity(bucket_count_usize);
        for _ in 0..bucket_count_usize {
            buckets.push(HistogramBucket::read_from(bytes)?);
        }

        Ok(Self {
            buckets,
            num_buckets: num_buckets.try_into()?,
        })
    }
}

impl AsBytes for HistogramBucket {
    fn write_to<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        buffer.write_all(&self.lower_bound.to_le_bytes())?;
        buffer.write_all(&self.upper_bound.to_le_bytes())?;
        buffer.write_all(&self.count.to_le_bytes())?;
        buffer.write_all(&self.distinct_count.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> io::Result<Self> {
        let mut buf8 = [0u8; 8];

        bytes.read_exact(&mut buf8)?;
        let lower_bound = f64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let upper_bound = f64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let count = f64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let distinct_count = f64::from_le_bytes(buf8);

        Ok(Self {
            lower_bound,
            upper_bound,
            count,
            distinct_count,
        })
    }
}

/// Statistics for a table (stored in catalog metadata).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStatistics {
    /// Total row count
    pub row_count: u64,
    /// Number of pages used by the table
    pub page_count: usize,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Per-column statistics
    pub column_stats: HashMap<usize, ColumnStatistics>,
    /// Timestamp of last statistics update (Unix epoch)
    pub last_updated: u64,
}

impl TableStatistics {
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
    pub fn add_column_stats(&mut self, column_idx: usize, stats: ColumnStatistics) {
        self.column_stats.insert(column_idx, stats);
    }

    /// Gets statistics for a column.
    pub fn get_column_stats(&self, column_idx: usize) -> Option<&ColumnStatistics> {
        self.column_stats.get(&column_idx)
    }

    /// Merges another TableStatistics into this one.
    ///
    /// Useful for combining statistics from multiple scans.
    pub fn merge(&mut self, other: &TableStatistics) {
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
}

impl AsBytes for TableStatistics {
    fn write_to<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        buffer.write_all(&self.row_count.to_le_bytes())?;

        let vbuf = VarInt::encode(self.page_count as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        buffer.write_all(&self.avg_row_size.to_le_bytes())?;
        buffer.write_all(&self.last_updated.to_le_bytes())?;

        let vbuf = VarInt::encode(self.column_stats.len() as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        for (idx, stats) in &self.column_stats {
            let vbuf = VarInt::encode(*idx as i64, &mut varint_buf);
            buffer.write_all(vbuf)?;
            stats.write_to(buffer)?;
        }

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> io::Result<Self> {
        let mut buf8 = [0u8; 8];

        bytes.read_exact(&mut buf8)?;
        let row_count = u64::from_le_bytes(buf8);

        let varint = VarInt::read_buf(bytes)?;
        let (page_count, _) = VarInt::from_encoded_bytes(&varint)?;

        bytes.read_exact(&mut buf8)?;
        let avg_row_size = f64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let last_updated = u64::from_le_bytes(buf8);

        let varint = VarInt::read_buf(bytes)?;
        let (stats_count, _) = VarInt::from_encoded_bytes(&varint)?;
        let stats_usize: usize = stats_count.try_into()?;

        let mut column_stats = HashMap::new();
        for _ in 0..stats_usize {
            let varint = VarInt::read_buf(bytes)?;
            let (idx, _) = VarInt::from_encoded_bytes(&varint)?;
            let stats = ColumnStatistics::read_from(bytes)?;
            column_stats.insert(idx.try_into()?, stats);
        }

        Ok(Self {
            row_count,
            page_count: page_count.try_into()?,
            avg_row_size,
            column_stats,
            last_updated,
        })
    }
}

/// Statistics for an index.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexStatistics {
    /// B-tree height
    pub height: usize,
    /// Number of leaf pages
    pub leaf_pages: usize,
    /// Total number of entries
    pub entries: u64,
    /// Number of distinct keys
    pub distinct_keys: u64,
    /// Average key size in bytes
    pub avg_key_size: f64,
    /// Minimum key value
    pub min_key: Option<Box<[u8]>>,
    /// Maximum key value
    pub max_key: Option<Box<[u8]>>,
    /// Timestamp of last statistics update
    pub last_updated: u64,
}

impl IndexStatistics {
    /// Creates empty index statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: sets tree height.
    pub fn with_height(mut self, height: usize) -> Self {
        self.height = height;
        self
    }

    /// Builder: sets leaf page count.
    pub fn with_leaf_pages(mut self, pages: usize) -> Self {
        self.leaf_pages = pages;
        self
    }

    /// Builder: sets entry count.
    pub fn with_entries(mut self, entries: u64) -> Self {
        self.entries = entries;
        self
    }

    /// Builder: sets distinct key count.
    pub fn with_distinct_keys(mut self, keys: u64) -> Self {
        self.distinct_keys = keys;
        self
    }

    /// Builder: sets min/max keys.
    pub fn with_key_range(mut self, min: Box<[u8]>, max: Box<[u8]>) -> Self {
        self.min_key = Some(min);
        self.max_key = Some(max);
        self
    }

    /// Updates statistics with a new key value.
    pub fn update_with_key<C: Comparator>(&mut self, key: &[u8], comparator: &C) {
        // Update min
        match &self.min_key {
            None => self.min_key = Some(key.to_vec().into_boxed_slice()),
            Some(current_min) => {
                if matches!(comparator.compare(key, current_min), Ok(Ordering::Less)) {
                    self.min_key = Some(key.to_vec().into_boxed_slice());
                }
            }
        }

        // Update max
        match &self.max_key {
            None => self.max_key = Some(key.to_vec().into_boxed_slice()),
            Some(current_max) => {
                if matches!(comparator.compare(key, current_max), Ok(Ordering::Greater)) {
                    self.max_key = Some(key.to_vec().into_boxed_slice());
                }
            }
        }
    }
}

impl AsBytes for IndexStatistics {
    fn write_to<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        let mut varint_buf = [0u8; MAX_VARINT_LEN];

        let vbuf = VarInt::encode(self.height as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        let vbuf = VarInt::encode(self.leaf_pages as i64, &mut varint_buf);
        buffer.write_all(vbuf)?;

        buffer.write_all(&self.entries.to_le_bytes())?;
        buffer.write_all(&self.distinct_keys.to_le_bytes())?;
        buffer.write_all(&self.avg_key_size.to_le_bytes())?;

        // Write min_key
        if let Some(min) = &self.min_key {
            buffer.write_all(&[1u8])?;
            let vbuf = VarInt::encode(min.len() as i64, &mut varint_buf);
            buffer.write_all(vbuf)?;
            buffer.write_all(min)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // Write max_key
        if let Some(max) = &self.max_key {
            buffer.write_all(&[1u8])?;
            let vbuf = VarInt::encode(max.len() as i64, &mut varint_buf);
            buffer.write_all(vbuf)?;
            buffer.write_all(max)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        buffer.write_all(&self.last_updated.to_le_bytes())?;

        Ok(())
    }

    fn read_from<R: Read + Seek>(bytes: &mut R) -> io::Result<Self> {
        let mut buf8 = [0u8; 8];

        let varint = VarInt::read_buf(bytes)?;
        let (height, _) = VarInt::from_encoded_bytes(&varint)?;

        let varint = VarInt::read_buf(bytes)?;
        let (leaf_pages, _) = VarInt::from_encoded_bytes(&varint)?;

        bytes.read_exact(&mut buf8)?;
        let entries = u64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let distinct_keys = u64::from_le_bytes(buf8);

        bytes.read_exact(&mut buf8)?;
        let avg_key_size = f64::from_le_bytes(buf8);

        let mut flag = [0u8; 1];

        bytes.read_exact(&mut flag)?;
        let min_key = if flag[0] == 1 {
            let varint = VarInt::read_buf(bytes)?;
            let (len, _) = VarInt::from_encoded_bytes(&varint)?;
            let len_usize: usize = len.try_into()?;
            let mut data = vec![0u8; len_usize];
            bytes.read_exact(&mut data)?;
            Some(data.into_boxed_slice())
        } else {
            None
        };

        bytes.read_exact(&mut flag)?;
        let max_key = if flag[0] == 1 {
            let varint = VarInt::read_buf(bytes)?;
            let (len, _) = VarInt::from_encoded_bytes(&varint)?;
            let len_usize: usize = len.try_into()?;
            let mut data = vec![0u8; len_usize];
            bytes.read_exact(&mut data)?;
            Some(data.into_boxed_slice())
        } else {
            None
        };

        bytes.read_exact(&mut buf8)?;
        let last_updated = u64::from_le_bytes(buf8);

        Ok(Self {
            height: height.try_into()?,
            leaf_pages: leaf_pages.try_into()?,
            entries,
            distinct_keys,
            avg_key_size,
            min_key,
            max_key,
            last_updated,
        })
    }
}

/// Provider interface for statistics.
///
/// Implementations provide statistics access for the query optimizer.
pub trait StatisticsProvider: Send + Sync {
    /// Gets statistics for a table.
    fn get_table_stats(&self, table_id: ObjectId) -> Option<TableStatistics>;

    /// Gets statistics for a column.
    fn get_column_stats(&self, table_id: ObjectId, column_idx: usize) -> Option<ColumnStatistics>;

    /// Gets index statistics.
    fn get_index_stats(&self, index_name: &str) -> Option<IndexStatistics>;
}

/// Placeholder statistics provider for development/testing.
#[derive(Default)]
pub struct PlaceholderStatsProvider {
    default_row_count: u64,
    default_page_count: usize,
    table_cache: HashMap<ObjectId, TableStatistics>,
    index_cache: HashMap<String, IndexStatistics>,
}

impl PlaceholderStatsProvider {
    /// Creates a new placeholder provider with default values.
    pub fn new() -> Self {
        Self {
            default_row_count: 1000,
            default_page_count: 10,
            table_cache: HashMap::new(),
            index_cache: HashMap::new(),
        }
    }

    /// Sets the default row count for unknown tables.
    pub fn with_default_row_count(mut self, count: u64) -> Self {
        self.default_row_count = count;
        self
    }

    /// Pre-populates statistics for a table.
    pub fn add_table_stats(&mut self, table_id: ObjectId, stats: TableStatistics) {
        self.table_cache.insert(table_id, stats);
    }

    /// Pre-populates statistics for an index.
    pub fn add_index_stats(&mut self, index_name: String, stats: IndexStatistics) {
        self.index_cache.insert(index_name, stats);
    }
}

impl StatisticsProvider for PlaceholderStatsProvider {
    fn get_table_stats(&self, table_id: ObjectId) -> Option<TableStatistics> {
        if let Some(stats) = self.table_cache.get(&table_id) {
            return Some(stats.clone());
        }

        Some(TableStatistics {
            row_count: self.default_row_count,
            page_count: self.default_page_count,
            avg_row_size: 100.0,
            column_stats: HashMap::new(),
            last_updated: 0,
        })
    }

    fn get_column_stats(&self, table_id: ObjectId, column_idx: usize) -> Option<ColumnStatistics> {
        if let Some(table_stats) = self.table_cache.get(&table_id) {
            if let Some(col_stats) = table_stats.column_stats.get(&column_idx) {
                return Some(col_stats.clone());
            }
        }

        Some(ColumnStatistics::default())
    }

    fn get_index_stats(&self, index_name: &str) -> Option<IndexStatistics> {
        if let Some(stats) = self.index_cache.get(index_name) {
            return Some(stats.clone());
        }

        Some(IndexStatistics {
            height: 2,
            leaf_pages: 10,
            entries: 1000,
            distinct_keys: 1000,
            avg_key_size: 8.0,
            min_key: None,
            max_key: None,
            last_updated: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_column_statistics_serialization() {
        let original = ColumnStatistics {
            ndv: 500,
            null_count: 25,
            min_bytes: Some(vec![0, 0, 0, 0, 0, 0, 0, 1].into_boxed_slice()),
            max_bytes: Some(vec![0, 0, 0, 0, 0, 0, 3, 232].into_boxed_slice()),
            avg_len: Some(128),
            histogram: Some(Histogram {
                buckets: vec![
                    HistogramBucket::new(0.0, 100.0, 50.0, 45.0),
                    HistogramBucket::new(100.0, 200.0, 75.0, 60.0),
                ],
                num_buckets: 10,
            }),
        };

        let mut buffer = Cursor::new(Vec::new());
        original
            .write_to(&mut buffer)
            .expect("Serialization failed");

        buffer.set_position(0);
        let deserialized =
            ColumnStatistics::read_from(&mut buffer).expect("Deserialization failed");

        assert_eq!(original.ndv, deserialized.ndv);
        assert_eq!(original.null_count, deserialized.null_count);
        assert_eq!(original.min_bytes, deserialized.min_bytes);
        assert_eq!(original.max_bytes, deserialized.max_bytes);
        assert_eq!(original.avg_len, deserialized.avg_len);

        let orig_hist = original.histogram.as_ref().unwrap();
        let deser_hist = deserialized.histogram.as_ref().unwrap();
        assert_eq!(orig_hist.num_buckets, deser_hist.num_buckets);
        assert_eq!(orig_hist.buckets.len(), deser_hist.buckets.len());
    }

    #[test]
    fn test_table_statistics_serialization() {
        let mut original = TableStatistics::new()
            .with_row_count(10000)
            .with_page_count(50)
            .with_avg_row_size(256.5);

        original.last_updated = 1234567890;
        original.add_column_stats(0, ColumnStatistics::new(100, 5));
        original.add_column_stats(2, ColumnStatistics::new(50, 0).with_avg_len(64));

        let mut buffer = Cursor::new(Vec::new());
        original
            .write_to(&mut buffer)
            .expect("Serialization failed");

        buffer.set_position(0);
        let deserialized = TableStatistics::read_from(&mut buffer).expect("Deserialization failed");

        assert_eq!(original.row_count, deserialized.row_count);
        assert_eq!(original.page_count, deserialized.page_count);
        assert!((original.avg_row_size - deserialized.avg_row_size).abs() < f64::EPSILON);
        assert_eq!(original.last_updated, deserialized.last_updated);
        assert_eq!(original.column_stats.len(), deserialized.column_stats.len());
    }

    #[test]
    fn test_index_statistics_serialization() {
        let original = IndexStatistics::new()
            .with_height(3)
            .with_leaf_pages(100)
            .with_entries(50000)
            .with_distinct_keys(49500)
            .with_key_range(
                vec![0, 0, 0, 1].into_boxed_slice(),
                vec![0, 0, 255, 255].into_boxed_slice(),
            );

        let mut buffer = Cursor::new(Vec::new());
        original
            .write_to(&mut buffer)
            .expect("Serialization failed");

        buffer.set_position(0);
        let deserialized = IndexStatistics::read_from(&mut buffer).expect("Deserialization failed");

        assert_eq!(original.height, deserialized.height);
        assert_eq!(original.leaf_pages, deserialized.leaf_pages);
        assert_eq!(original.entries, deserialized.entries);
        assert_eq!(original.distinct_keys, deserialized.distinct_keys);
        assert_eq!(original.min_key, deserialized.min_key);
        assert_eq!(original.max_key, deserialized.max_key);
    }

    #[test]
    fn test_histogram_range_selectivity() {
        let hist = Histogram {
            buckets: vec![
                HistogramBucket::new(0.0, 100.0, 100.0, 100.0),
                HistogramBucket::new(100.0, 200.0, 100.0, 100.0),
                HistogramBucket::new(200.0, 300.0, 100.0, 100.0),
            ],
            num_buckets: 3,
        };

        // Full range
        let sel = hist.estimate_range_selectivity(Some(0.0), Some(300.0));
        assert!((sel - 1.0).abs() < 0.01);

        // Half range
        let sel = hist.estimate_range_selectivity(Some(0.0), Some(150.0));
        assert!((sel - 0.5).abs() < 0.01);

        // Single bucket
        let sel = hist.estimate_range_selectivity(Some(100.0), Some(200.0));
        assert!((sel - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_selectivity_equality() {
        let stats = ColumnStatistics::new(100, 0);
        let sel = stats.selectivity_equality(1000.0);
        assert!((sel - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn test_selectivity_null() {
        let stats = ColumnStatistics::new(100, 50);
        let sel = stats.selectivity_null(1000.0);
        assert!((sel - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bytes_to_estimation_value() {
        // Small values
        assert_eq!(bytes_to_raw_value(&[0]), 0.0);
        assert_eq!(bytes_to_raw_value(&[1]), 1.0);
        assert_eq!(bytes_to_raw_value(&[255]), 255.0);

        // Two bytes (little-endian style accumulation)
        let val = bytes_to_raw_value(&[0, 1]);
        assert_eq!(val, 256.0); // 0 + 1*256
    }

    #[test]
    fn test_column_stats_update_with_value() {
        use crate::structures::comparator::NumericComparator;

        let comparator = NumericComparator::for_size(4);
        let mut stats = ColumnStatistics::new(0, 0);

        // First value sets both min and max
        stats.update_with_value(&[50, 0, 0, 0], &comparator);
        assert!(stats.min_bytes.is_some());
        assert!(stats.max_bytes.is_some());

        // Lower value updates min
        stats.update_with_value(&[10, 0, 0, 0], &comparator);
        assert_eq!(stats.min_bytes.as_ref().unwrap()[0], 10);

        // Higher value updates max
        stats.update_with_value(&[100, 0, 0, 0], &comparator);
        assert_eq!(stats.max_bytes.as_ref().unwrap()[0], 100);
    }

    #[test]
    fn test_placeholder_provider() {
        let mut provider = PlaceholderStatsProvider::new().with_default_row_count(5000);
        let table_id = ObjectId::new();

        // Test default stats
        let stats = provider.get_table_stats(table_id).unwrap();
        assert_eq!(stats.row_count, 5000);

        // Add custom stats
        let custom_stats = TableStatistics::new().with_row_count(999);
        provider.add_table_stats(table_id, custom_stats);

        // Verify custom stats are returned
        let stats = provider.get_table_stats(table_id).unwrap();
        assert_eq!(stats.row_count, 999);
    }
}
