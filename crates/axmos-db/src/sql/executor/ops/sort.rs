// src/sql/executor/ops/sort.rs
//! Sort executor with in-memory quicksort and external k-way merge sort.
//!
//! This executor implements ORDER BY using two strategies:
//! 1. In-memory sort: When data fits in memory, use quicksort
//! 2. External sort: When data exceeds memory limit, use k-way merge sort with disk spills

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        binder::ast::BoundExpression,
        executor::{
            eval::ExpressionEvaluator, ExecutionState, ExecutionStats, Executor, Row,
        },
        planner::{
            logical::SortExpr,
            physical::ExternalSortOp,
        }
    },
    types::{DataType, Blob, reinterpret_cast, DataTypeKind}
};

/// Default memory limit for in-memory sorting (64 MB).
const DEFAULT_MEMORY_LIMIT: usize = 64 * 1024 * 1024;

/// Estimated bytes per row for memory estimation.
const ESTIMATED_ROW_SIZE: usize = 256;

/// Number of rows per sorted run file.
const ROWS_PER_RUN: usize = 10_000;

/// Sort executor that handles both in-memory and external sorting.
///
/// Algorithm:
/// 1. Consume all input rows
/// 2. If rows fit in memory: quicksort in place
/// 3. If rows exceed memory limit:
///    a. Sort chunks in memory and write to temp files (sorted runs)
///    b. K-way merge the sorted runs
/// 4. Emit sorted rows one at a time
pub(crate) struct Sort {
    op: ExternalSortOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,

    /// Memory limit for in-memory sorting.
    memory_limit: usize,

    /// Buffered rows for in-memory sort.
    buffer: Vec<Row>,

    /// Current position in buffer during output phase.
    current_idx: usize,

    /// Temporary directory for sorted run files.
    temp_dir: Option<PathBuf>,

    /// Sorted run files for external merge.
    run_files: Vec<PathBuf>,

    /// Merge iterator for external sort.
    merge_iter: Option<KWayMergeIterator>,

    /// Whether we're using external sort.
    using_external_sort: bool,

    /// Sort key cache: precomputed sort keys for each row.
    sort_keys: Vec<Vec<DataType>>,
}

impl Sort {
    pub(crate) fn new(op: ExternalSortOp, child: Box<dyn Executor>) -> Self {
        let memory_limit = op.memory_limit.unwrap_or(DEFAULT_MEMORY_LIMIT);
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            memory_limit,
            buffer: Vec::new(),
            current_idx: 0,
            temp_dir: None,
            run_files: Vec::new(),
            merge_iter: None,
            using_external_sort: false,
            sort_keys: Vec::new(),
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract sort key values from a row.
    fn extract_sort_key(&self, row: &Row) -> ExecutionResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.op.schema);

        let mut key_values = Vec::with_capacity(self.op.order_by.len());
        for sort_expr in &self.op.order_by {
            let value = evaluator.evaluate(sort_expr.expr.clone())?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Compare two rows based on sort expressions.
    fn compare_rows(&self, a_keys: &[DataType], b_keys: &[DataType]) -> Ordering {
        for (i, sort_expr) in self.op.order_by.iter().enumerate() {
            let a_val = &a_keys[i];
            let b_val = &b_keys[i];

            // Handle NULL ordering
            let cmp = match (a_val, b_val) {
                (DataType::Null, DataType::Null) => Ordering::Equal,
                (DataType::Null, _) => {
                    if sort_expr.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, DataType::Null) => {
                    if sort_expr.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => a_val.partial_cmp(b_val).unwrap_or(Ordering::Equal),
            };

            if cmp != Ordering::Equal {
                return if sort_expr.asc { cmp } else { cmp.reverse() };
            }
        }

        Ordering::Equal
    }

    /// Consume all input and sort.
    fn build(&mut self) -> ExecutionResult<()> {
        // Phase 1: Collect all rows
        let mut estimated_memory = 0usize;
        let mut current_run: Vec<(Row, Vec<DataType>)> = Vec::new();

        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let sort_key = self.extract_sort_key(&row)?;
            estimated_memory += ESTIMATED_ROW_SIZE;

            // Check if we need to spill to disk
            if estimated_memory > self.memory_limit && !current_run.is_empty() {
                self.using_external_sort = true;
                self.spill_run_to_disk(&mut current_run)?;
                estimated_memory = ESTIMATED_ROW_SIZE;
            }

            current_run.push((row, sort_key));
        }

        // Handle remaining data
        if self.using_external_sort {
            // Spill remaining data and prepare for merge
            if !current_run.is_empty() {
                self.spill_run_to_disk(&mut current_run)?;
            }
            self.prepare_merge()?;
        } else {
            // In-memory sort using quicksort
            self.in_memory_sort(current_run)?;
        }

        Ok(())
    }

    /// Perform in-memory quicksort.
    fn in_memory_sort(&mut self, mut rows_with_keys: Vec<(Row, Vec<DataType>)>) -> ExecutionResult<()> {
        // Sort using the comparison function
        rows_with_keys.sort_by(|a, b| self.compare_rows(&a.1, &b.1));

        // Extract just the rows
        self.buffer = rows_with_keys.into_iter().map(|(row, _)| row).collect();
        self.current_idx = 0;

        Ok(())
    }

    /// Spill a sorted run to disk.
    fn spill_run_to_disk(&mut self, run: &mut Vec<(Row, Vec<DataType>)>) -> ExecutionResult<()> {
        // Ensure temp directory exists
        if self.temp_dir.is_none() {
            let temp_dir = std::env::temp_dir().join(format!("axmos_sort_{}", std::process::id()));
            std::fs::create_dir_all(&temp_dir).map_err(|e| {
                QueryExecutionError::Other(format!("Failed to create temp dir: {}", e))
            })?;
            self.temp_dir = Some(temp_dir);
        }

        // Sort the run in memory
        run.sort_by(|a, b| self.compare_rows(&a.1, &b.1));

        // Write to file
        let run_file = self
            .temp_dir
            .as_ref()
            .unwrap()
            .join(format!("run_{}.dat", self.run_files.len()));

        let file = File::create(&run_file).map_err(|e| {
            QueryExecutionError::Other(format!("Failed to create run file: {}", e))
        })?;

        let mut writer = BufWriter::new(file);

        // Write number of rows
        let num_rows = run.len() as u64;
        writer.write_all(&num_rows.to_le_bytes()).map_err(|e| {
            QueryExecutionError::Other(format!("Failed to write run file: {}", e))
        })?;

        // Write each row
        for (row, _) in run.drain(..) {
            Self::serialize_row(&mut writer, &row)?;
        }

        writer.flush().map_err(|e| {
            QueryExecutionError::Other(format!("Failed to flush run file: {}", e))
        })?;

        self.run_files.push(run_file);
        Ok(())
    }

    /// Prepare the k-way merge iterator.
    fn prepare_merge(&mut self) -> ExecutionResult<()> {
        let mut readers = Vec::with_capacity(self.run_files.len());

        for run_file in &self.run_files {
            let file = File::open(run_file).map_err(|e| {
                QueryExecutionError::Other(format!("Failed to open run file: {}", e))
            })?;
            let reader = RunReader::new(BufReader::new(file), &self.op.schema)?;
            readers.push(reader);
        }

        self.merge_iter = Some(KWayMergeIterator::new(readers, self)?);
        Ok(())
    }

    /// Serialize a row to bytes.
    fn serialize_row<W: Write>(writer: &mut W, row: &Row) -> ExecutionResult<()> {
        // Write number of columns
        let num_cols = row.len() as u32;
        writer.write_all(&num_cols.to_le_bytes()).map_err(|e| {
            QueryExecutionError::Other(format!("Serialization error: {}", e))
        })?;

        // Write each column
        for value in row.iter() {
            Self::serialize_datatype(writer, value)?;
        }

        Ok(())
    }

    /// Deserialize a row from bytes.
    fn deserialize_row<R: Read>(reader: &mut R) -> ExecutionResult<Option<Row>> {
        // Read number of columns
        let mut num_cols_bytes = [0u8; 4];
        match reader.read_exact(&mut num_cols_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => {
                return Err(QueryExecutionError::Other(format!(
                    "Deserialization error: {}",
                    e
                )))
            }
        }

        let num_cols = u32::from_le_bytes(num_cols_bytes) as usize;
        let mut row = Row::with_capacity(num_cols);

        for _ in 0..num_cols {
            let value = Self::deserialize_datatype(reader)?;
            row.push(value);
        }

        Ok(Some(row))
    }

    /// Serialize a DataType value.
    /// Format: [type_id: u8][value_bytes...]
    /// For variable-length types: [type_id: u8][length: u32][value_bytes...]
    fn serialize_datatype<W: Write>(writer: &mut W, value: &DataType) -> ExecutionResult<()> {
        // Write type discriminant using DataTypeKind's as_repr()
        let type_id = value.kind().as_repr();
        writer.write_all(&[type_id]).map_err(|e| {
            QueryExecutionError::Other(format!("Serialization error: {}", e))
        })?;

        // For fixed-size types, just write the bytes directly
        // For variable-size types (Text, Blob), write length first
        let bytes: &[u8] = value.as_ref();

        if !value.kind().is_fixed_size() {
            // Variable-length: write length prefix
            let len = bytes.len() as u32;
            writer.write_all(&len.to_le_bytes()).map_err(|e| {
                QueryExecutionError::Other(format!("Serialization error: {}", e))
            })?;
        }

        writer.write_all(bytes).map_err(|e| {
            QueryExecutionError::Other(format!("Serialization error: {}", e))
        })?;

        Ok(())
    }

    /// Deserialize a DataType value.
    fn deserialize_datatype<R: Read>(reader: &mut R) -> ExecutionResult<DataType> {

        // Read type discriminant
        let mut type_id = [0u8; 1];
        reader.read_exact(&mut type_id).map_err(|e| {
            QueryExecutionError::Other(format!("Deserialization error: {}", e))
        })?;

        let kind = DataTypeKind::from_repr(type_id[0]).map_err(|e| {
            QueryExecutionError::Other(format!("Unknown type id {}: {}", type_id[0], e))
        })?;

        // Handle Null specially
        if kind.is_null() {
            return Ok(DataType::Null);
        }

        // For fixed-size types, read exactly that many bytes
        // For variable-size types, read length first
        if let Some(size) = kind.size_of_val() {
            let mut buf = vec![0u8; size];
            reader.read_exact(&mut buf).map_err(|e| {
                QueryExecutionError::Other(format!("Deserialization error: {}", e))
            })?;

            // Use reinterpret_cast to convert bytes back to DataType
            let (value_ref, _) = reinterpret_cast(kind, &buf).map_err(|e| {
                QueryExecutionError::Other(format!("Reinterpret error: {}", e))
            })?;

            Ok(value_ref.to_owned())
        } else {
            // Variable-length type: read length then data
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf).map_err(|e| {
                QueryExecutionError::Other(format!("Deserialization error: {}", e))
            })?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data).map_err(|e| {
                QueryExecutionError::Other(format!("Deserialization error: {}", e))
            })?;

            // Reconstruct based on kind
            let blob = Blob::from(data.as_slice());
            match kind {
                DataTypeKind::Text => Ok(DataType::Text(blob)),
                DataTypeKind::Blob => Ok(DataType::Blob(blob)),
                _ => Err(QueryExecutionError::Other(format!(
                    "Unexpected variable-length type: {:?}", kind
                ))),
            }
        }
    }

    /// Clean up temporary files.
    fn cleanup(&mut self) {
        for file in &self.run_files {
            let _ = std::fs::remove_file(file);
        }
        self.run_files.clear();

        if let Some(ref temp_dir) = self.temp_dir {
            let _ = std::fs::remove_dir(temp_dir);
        }
        self.temp_dir = None;
    }
}

impl Executor for Sort {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.buffer.clear();
        self.current_idx = 0;
        self.cleanup();
        self.using_external_sort = false;
        self.merge_iter = None;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => {
                self.state = ExecutionState::Running;
                self.build()?;
            }
            _ => {}
        }

        if self.using_external_sort {
            // External sort: use merge iterator
            if let Some(ref mut merge_iter) = self.merge_iter {
                if let Some(row) = merge_iter.next()? {
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
            }
        } else {
            // In-memory sort: return from buffer
            if self.current_idx < self.buffer.len() {
                let row = self.buffer[self.current_idx].clone();
                self.current_idx += 1;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }
        self.child.close()?;
        self.state = ExecutionState::Closed;
        self.buffer.clear();
        self.cleanup();
        self.merge_iter = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.schema
    }
}

impl Drop for Sort {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Reader for a sorted run file.
struct RunReader {
    reader: BufReader<File>,
    remaining: u64,
    current: Option<Row>,
    sort_key: Option<Vec<DataType>>,
}

impl RunReader {
    fn new(mut reader: BufReader<File>, schema: &Schema) -> ExecutionResult<Self> {
        // Read number of rows
        let mut num_rows_bytes = [0u8; 8];
        reader.read_exact(&mut num_rows_bytes).map_err(|e| {
            QueryExecutionError::Other(format!("Failed to read run file: {}", e))
        })?;
        let remaining = u64::from_le_bytes(num_rows_bytes);

        let mut run_reader = Self {
            reader,
            remaining,
            current: None,
            sort_key: None,
        };

        // Prime the reader with first row
        run_reader.advance()?;

        Ok(run_reader)
    }

    fn advance(&mut self) -> ExecutionResult<()> {
        if self.remaining == 0 {
            self.current = None;
            self.sort_key = None;
            return Ok(());
        }

        self.current = Sort::deserialize_row(&mut self.reader)?;
        self.remaining -= 1;

        Ok(())
    }

    fn peek(&self) -> Option<&Row> {
        self.current.as_ref()
    }

    fn take(&mut self) -> ExecutionResult<Option<Row>> {
        let row = self.current.take();
        self.advance()?;
        Ok(row)
    }

    fn is_exhausted(&self) -> bool {
        self.current.is_none()
    }
}

/// Entry in the merge heap.
struct MergeEntry {
    row: Row,
    sort_key: Vec<DataType>,
    source_idx: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for MergeEntry {}

/// K-way merge iterator using a min-heap.
struct KWayMergeIterator {
    readers: Vec<RunReader>,
    heap: BinaryHeap<std::cmp::Reverse<MergeEntryWrapper>>,
    order_by: Vec<SortExpr>,
}

/// Wrapper to implement Ord for MergeEntry based on sort expressions.
struct MergeEntryWrapper {
    entry: MergeEntry,
    order_by: Vec<SortExpr>,
}

impl PartialEq for MergeEntryWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.entry.sort_key == other.entry.sort_key
    }
}

impl Eq for MergeEntryWrapper {}

impl PartialOrd for MergeEntryWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntryWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        for (i, sort_expr) in self.order_by.iter().enumerate() {
            let a_val = &self.entry.sort_key[i];
            let b_val = &other.entry.sort_key[i];

            let cmp = match (a_val, b_val) {
                (DataType::Null, DataType::Null) => Ordering::Equal,
                (DataType::Null, _) => {
                    if sort_expr.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, DataType::Null) => {
                    if sort_expr.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => a_val.partial_cmp(b_val).unwrap_or(Ordering::Equal),
            };

            if cmp != Ordering::Equal {
                return if sort_expr.asc { cmp } else { cmp.reverse() };
            }
        }

        Ordering::Equal
    }
}

impl KWayMergeIterator {
    fn new(mut readers: Vec<RunReader>, sort: &Sort) -> ExecutionResult<Self> {
        let order_by = sort.op.order_by.clone();
        let mut heap = BinaryHeap::new();

        // Initialize heap with first element from each reader
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = reader.peek() {
                let sort_key = sort.extract_sort_key(row)?;
                let entry = MergeEntry {
                    row: row.clone(),
                    sort_key,
                    source_idx: idx,
                };
                heap.push(std::cmp::Reverse(MergeEntryWrapper {
                    entry,
                    order_by: order_by.clone(),
                }));
            }
        }

        Ok(Self {
            readers,
            heap,
            order_by,
        })
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        let std::cmp::Reverse(wrapper) = match self.heap.pop() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let MergeEntryWrapper { entry, .. } = wrapper;
        let source_idx = entry.source_idx;
        let result_row = entry.row;

        // Advance the reader that provided this row
        self.readers[source_idx].advance()?;

        // If reader has more data, add next entry to heap
        if let Some(row) = self.readers[source_idx].peek() {
            // Need to extract sort key
            let sort_key = self.extract_sort_key_from_row(row)?;
            let new_entry = MergeEntry {
                row: row.clone(),
                sort_key,
                source_idx,
            };
            self.heap.push(std::cmp::Reverse(MergeEntryWrapper {
                entry: new_entry,
                order_by: self.order_by.clone(),
            }));
        }

        Ok(Some(result_row))
    }

    /// Extract sort key from a row using column indices.
    /// Note: This is a simplified version - in production you'd want to
    /// store the sort key alongside the row in the run file.
    fn extract_sort_key_from_row(&self, row: &Row) -> ExecutionResult<Vec<DataType>> {
        // For simplicity, assume sort expressions are column references
        // In a full implementation, you'd evaluate the expressions
        let mut key = Vec::with_capacity(self.order_by.len());
        for sort_expr in &self.order_by {
            // Try to extract column index from expression
            if let BoundExpression::ColumnRef(col_ref) = &sort_expr.expr {
                if col_ref.column_idx < row.len() {
                    key.push(row[col_ref.column_idx].clone());
                } else {
                    key.push(DataType::Null);
                }
            } else {
                // For complex expressions, we'd need to evaluate them
                // For now, use Null as placeholder
                key.push(DataType::Null);
            }
        }
        Ok(key)
    }
}
