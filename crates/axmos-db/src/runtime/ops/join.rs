use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{
    runtime::{
        ExecutionStats, Executor, RuntimeResult,
        eval::ExpressionEvaluator,
    },
    schema::Schema,
    sql::{
        binder::bounds::BoundExpression,
        parser::ast::JoinType,
        planner::physical::{HashJoinOp, MergeJoinOp, NestedLoopJoinOp},
    },
    storage::tuple::Row,
    types::DataType,
};



/// A generic hash bucket that stores rows along with match tracking.
/// Used by HashJoin for tracking which rows have been matched in outer joins.
pub(crate) struct HashBucket<T> {
    pub rows: Vec<T>,
    pub matched: Vec<bool>,
}

impl<T> HashBucket<T> {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            matched: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            matched: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, row: T) {
        self.rows.push(row);
        self.matched.push(false);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn mark_matched(&mut self, idx: usize) {
        if idx < self.matched.len() {
            self.matched[idx] = true;
        }
    }

    pub fn is_matched(&self, idx: usize) -> bool {
        self.matched.get(idx).copied().unwrap_or(false)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.rows.iter()
    }

    pub fn iter_unmatched(&self) -> impl Iterator<Item = &T> {
        self.rows
            .iter()
            .zip(self.matched.iter())
            .filter_map(|(row, &matched)| if !matched { Some(row) } else { None })
    }
}

impl<T> Default for HashBucket<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracts key values from a row by evaluating key expressions.
/// This is used by both HashJoin and HashAggregate.
pub(crate) fn extract_key(
    row: &Row,
    key_exprs: &[BoundExpression],
    schema: &Schema,
) -> RuntimeResult<Vec<DataType>> {
    let evaluator = ExpressionEvaluator::new(row, schema);
    let mut key = Vec::with_capacity(key_exprs.len());
    for expr in key_exprs {
        key.push(evaluator.evaluate_as_single_value(expr)?);
    }
    Ok(key)
}

/// Checks if two key vectors match (used for equi-joins).
/// NULL values never match according to SQL semantics.
pub(crate) fn keys_match(left: &[DataType], right: &[DataType]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for (l, r) in left.iter().zip(right.iter()) {
        if matches!(l, DataType::Null) || matches!(r, DataType::Null) {
            return false;
        }
        if l != r {
            return false;
        }
    }
    true
}


fn combine_rows(left: &Row, right: &Row) -> Row {
    let mut values: Vec<DataType> = Vec::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        values.push(left[i].clone());
    }
    for i in 0..right.len() {
        values.push(right[i].clone());
    }
    Row::new(values.into_boxed_slice())
}

fn left_with_nulls(left: &Row, right_cols: usize) -> Row {
    let mut values: Vec<DataType> = Vec::with_capacity(left.len() + right_cols);
    for i in 0..left.len() {
        values.push(left[i].clone());
    }
    for _ in 0..right_cols {
        values.push(DataType::Null);
    }
    Row::new(values.into_boxed_slice())
}

fn nulls_with_right(right: &Row, left_cols: usize) -> Row {
    let mut values: Vec<DataType> = Vec::with_capacity(left_cols + right.len());
    for _ in 0..left_cols {
        values.push(DataType::Null);
    }
    for i in 0..right.len() {
        values.push(right[i].clone());
    }
    Row::new(values.into_boxed_slice())
}

fn evaluate_condition(
    combined: &Row,
    condition: &Option<BoundExpression>,
    schema: &Schema,
) -> RuntimeResult<bool> {
    match condition {
        None => Ok(true),
        Some(cond) => {
            let evaluator = ExpressionEvaluator::new(combined, schema);
            Ok(evaluator.evaluate_as_bool(cond)?)
        }
    }
}

// Nested Loop Join
pub(crate) struct NestedLoopJoin<Left: Executor, Right: Executor> {
    join_type: JoinType,
    condition: Option<BoundExpression>,
    output_schema: Schema,
    left_cols: usize,

    left: Left,
    right: Right,
    current_left_row: Option<Row>,
    right_buffer: Vec<Row>,
    right_idx: usize,
    right_buffered: bool,
    left_matched: bool,
    right_matched: Vec<bool>,
    emitting_unmatched_right: bool,
    unmatched_right_idx: usize,
    stats: ExecutionStats,
}

impl<Left: Executor, Right: Executor> NestedLoopJoin<Left, Right> {
    pub(crate) fn new(op: &NestedLoopJoinOp, left: Left, right: Right) -> Self {
        let total_cols = op.output_schema.num_columns();
        Self {
            join_type: op.join_type,
            condition: op.condition.clone(),
            left_cols: 0,
            output_schema: op.output_schema.clone(),
            left,
            right,
            current_left_row: None,
            stats: ExecutionStats::default(),
            right_buffer: Vec::new(),
            right_idx: 0,
            right_buffered: false,
            left_matched: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_idx: 0,
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn buffer_right(&mut self) -> RuntimeResult<()> {
        if self.right_buffered {
            return Ok(());
        }

        while let Some(row) = self.right.next()? {
            self.right_buffer.push(row);
        }
        self.right.close()?;

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.right_matched = vec![false; self.right_buffer.len()];
        }

        self.right_buffered = true;
        Ok(())
    }

    fn right_cols(&self) -> usize {
        self.output_schema.num_columns() - self.left_cols
    }
}

impl<Left: Executor, Right: Executor> Executor for NestedLoopJoin<Left, Right> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.left.open()?;
        self.right.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        self.buffer_right()?;

        if self.emitting_unmatched_right {
            while self.unmatched_right_idx < self.right_buffer.len() {
                let idx = self.unmatched_right_idx;
                self.unmatched_right_idx += 1;

                if !self.right_matched[idx] {
                    let row = nulls_with_right(&self.right_buffer[idx], self.left_cols);
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
            }
            return Ok(None);
        }

        loop {
            if self.current_left_row.is_none() {
                match self.left.next()? {
                    Some(row) => {
                        self.stats.rows_scanned += 1;
                        if self.left_cols == 0 {
                            self.left_cols = row.len();
                        }
                        self.current_left_row = Some(row);
                        self.right_idx = 0;
                        self.left_matched = false;
                    }
                    None => {
                        self.left.close()?;
                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            self.emitting_unmatched_right = true;
                            return self.next();
                        }
                        return Ok(None);
                    }
                }
            }

            let left_row = self.current_left_row.as_ref().unwrap();

            while self.right_idx < self.right_buffer.len() {
                let right_row = &self.right_buffer[self.right_idx];
                self.right_idx += 1;

                let combined = combine_rows(left_row, right_row);

                if evaluate_condition(&combined, &self.condition, &self.output_schema)? {
                    self.left_matched = true;

                    if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                        self.right_matched[self.right_idx - 1] = true;
                    }

                    self.stats.rows_produced += 1;
                    return Ok(Some(combined));
                }
            }

            if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                let row = left_with_nulls(left_row, self.right_cols());
                self.current_left_row = None;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }

            self.current_left_row = None;
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}

// Hash Join

pub(crate) struct HashJoin<Left: Executor, Right: Executor> {
    join_type: JoinType,
    left_keys: Vec<BoundExpression>,
    right_keys: Vec<BoundExpression>,
    condition: Option<BoundExpression>,
    output_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,

    left: Left,
    right: Right,

    hash_table: HashMap<Vec<DataType>, HashBucket<Row>>,
    build_done: bool,

    current_left_row: Option<Row>,
    current_left_key: Option<Vec<DataType>>,
    current_bucket_idx: usize,

    left_matched: bool,
    emitting_unmatched_right: bool,
    unmatched_buckets: Option<std::vec::IntoIter<HashBucket<Row>>>,
    current_unmatched_bucket: Option<HashBucket<Row>>,
    unmatched_row_idx: usize,

    stats: ExecutionStats,
}

impl<Left: Executor, Right: Executor> HashJoin<Left, Right> {
    pub(crate) fn new(
        op: &HashJoinOp,
        left: Left,
        right: Right,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            join_type: op.join_type,
            left_keys: op.left_keys.clone(),
            right_keys: op.right_keys.clone(),
            condition: op.condition.clone(),
            output_schema: op.output_schema.clone(),
            left_schema,
            right_schema,
            left,
            right,
            hash_table: HashMap::new(),
            build_done: false,
            current_left_row: None,
            current_left_key: None,
            current_bucket_idx: 0,
            left_matched: false,
            emitting_unmatched_right: false,
            unmatched_buckets: None,
            current_unmatched_bucket: None,
            unmatched_row_idx: 0,
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn build_hash_table(&mut self) -> RuntimeResult<()> {
        if self.build_done {
            return Ok(());
        }

        while let Some(row) = self.right.next()? {
            let key = extract_key(&row, &self.right_keys, &self.right_schema)?;
            self.hash_table
                .entry(key)
                .or_insert_with(HashBucket::new)
                .push(row);
        }
        self.right.close()?;
        self.build_done = true;
        Ok(())
    }

    fn left_cols(&self) -> usize {
        self.left_schema.num_columns()
    }

    fn right_cols(&self) -> usize {
        self.right_schema.num_columns()
    }
}

impl<Left: Executor, Right: Executor> Executor for HashJoin<Left, Right> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.left.open()?;
        self.right.open()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        self.build_hash_table()?;
        let num_left_columns =  self.left_cols();
        if self.emitting_unmatched_right {
            loop {

                if let Some(ref mut bucket) = self.current_unmatched_bucket {
                    while self.unmatched_row_idx < bucket.len() {
                        let idx = self.unmatched_row_idx;
                        self.unmatched_row_idx += 1;

                        if !bucket.is_matched(idx) {

                            let row = nulls_with_right(&bucket.rows[idx], num_left_columns);
                            self.stats.rows_produced += 1;
                            return Ok(Some(row));
                        }
                    }
                }

                if let Some(ref mut iter) = self.unmatched_buckets {
                    if let Some(bucket) = iter.next() {
                        self.current_unmatched_bucket = Some(bucket);
                        self.unmatched_row_idx = 0;
                        continue;
                    }
                }

                return Ok(None);
            }
        }

        loop {
            if self.current_left_row.is_none() {
                match self.left.next()? {
                    Some(row) => {
                        self.stats.rows_scanned += 1;
                        let key = extract_key(&row, &self.left_keys, &self.left_schema)?;
                        self.current_left_row = Some(row);
                        self.current_left_key = Some(key);
                        self.current_bucket_idx = 0;
                        self.left_matched = false;
                    }
                    None => {
                        self.left.close()?;

                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            self.emitting_unmatched_right = true;
                            let table = std::mem::take(&mut self.hash_table);
                            self.unmatched_buckets = Some(table.into_values().collect::<Vec<_>>().into_iter());
                            return self.next();
                        }
                        return Ok(None);
                    }
                }
            }

            let left_row = self.current_left_row.as_ref().unwrap();
            let left_key = self.current_left_key.as_ref().unwrap();

            if let Some(bucket) = self.hash_table.get_mut(left_key) {
                while self.current_bucket_idx < bucket.len() {
                    let right_row = &bucket.rows[self.current_bucket_idx];
                    self.current_bucket_idx += 1;

                    let combined = combine_rows(left_row, right_row);

                    if evaluate_condition(&combined, &self.condition, &self.output_schema)? {
                        self.left_matched = true;

                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            bucket.mark_matched(self.current_bucket_idx - 1);
                        }

                        self.stats.rows_produced += 1;
                        return Ok(Some(combined));
                    }
                }
            }

            if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                let row = left_with_nulls(left_row, self.right_cols());
                self.current_left_row = None;
                self.current_left_key = None;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }

            self.current_left_row = None;
            self.current_left_key = None;
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}

// Merge Join

pub(crate) struct MergeJoin<Left: Executor, Right: Executor> {
    join_type: JoinType,
    left_keys: Vec<BoundExpression>,
    right_keys: Vec<BoundExpression>,
    output_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,

    left: Left,
    right: Right,

    current_left: Option<Row>,
    current_right: Option<Row>,

    right_buffer: Vec<Row>,
    right_buffer_idx: usize,
    buffering_key: Option<Vec<DataType>>,

    left_exhausted: bool,
    right_exhausted: bool,
    left_matched: bool,
    right_matched: Vec<bool>,
    emitting_unmatched_right: bool,
    unmatched_right_idx: usize,

    stats: ExecutionStats,
}

impl<Left: Executor, Right: Executor> MergeJoin<Left, Right> {
    pub(crate) fn new(
        op: &MergeJoinOp,
        left: Left,
        right: Right,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            join_type: op.join_type,
            left_keys: op.left_keys.clone(),
            right_keys: op.right_keys.clone(),
            output_schema: op.output_schema.clone(),
            left_schema,
            right_schema,
            left,
            right,
            current_left: None,
            current_right: None,
            right_buffer: Vec::new(),
            right_buffer_idx: 0,
            buffering_key: None,
            left_exhausted: false,
            right_exhausted: false,
            left_matched: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_idx: 0,
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn compare_keys(&self, left_keys: &[DataType], right_keys: &[DataType]) -> Ordering {
        for (l, r) in left_keys.iter().zip(right_keys.iter()) {
            if matches!(l, DataType::Null) || matches!(r, DataType::Null) {
                return Ordering::Greater;
            }
            match l.partial_cmp(r) {
                Some(Ordering::Equal) => continue,
                Some(ord) => return ord,
                None => return Ordering::Greater,
            }
        }
        Ordering::Equal
    }

    fn advance_left(&mut self) -> RuntimeResult<()> {
        match self.left.next()? {
            Some(row) => {
                self.stats.rows_scanned += 1;
                self.current_left = Some(row);
            }
            None => {
                self.left_exhausted = true;
                self.current_left = None;
            }
        }
        Ok(())
    }

    fn advance_right(&mut self) -> RuntimeResult<()> {
        match self.right.next()? {
            Some(row) => {
                self.stats.rows_scanned += 1;
                self.current_right = Some(row);

                if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                    self.right_matched.push(false);
                }
            }
            None => {
                self.right_exhausted = true;
                self.current_right = None;
            }
        }
        Ok(())
    }

    fn left_cols(&self) -> usize {
        self.left_schema.num_columns()
    }

    fn right_cols(&self) -> usize {
        self.right_schema.num_columns()
    }

    fn buffer_matching_right_rows(&mut self, target_keys: &[DataType]) -> RuntimeResult<()> {
        self.right_buffer.clear();
        self.right_buffer_idx = 0;

        while let Some(ref right_row) = self.current_right {
            let right_keys = extract_key(right_row, &self.right_keys, &self.right_schema)?;

            if keys_match(target_keys, &right_keys) {
                self.right_buffer.push(right_row.clone());
                self.advance_right()?;
            } else {
                break;
            }
        }

        self.buffering_key = Some(target_keys.to_vec());
        Ok(())
    }
}

impl<Left: Executor, Right: Executor> Executor for MergeJoin<Left, Right> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.left.open()?;
        self.right.open()?;

        self.advance_left()?;
        self.advance_right()?;

        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.emitting_unmatched_right {
            while self.unmatched_right_idx < self.right_matched.len() {
                let idx = self.unmatched_right_idx;
                self.unmatched_right_idx += 1;

                if !self.right_matched[idx] && idx < self.right_buffer.len() {
                    let row = nulls_with_right(&self.right_buffer[idx], self.left_cols());
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
            }
            return Ok(None);
        }

        loop {
            if self.left_exhausted {
                if self.current_left.is_some() {
                    if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                        let left_row = self.current_left.take().unwrap();
                        let row = left_with_nulls(&left_row, self.right_cols());
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                    self.current_left = None;
                }

                if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                    self.emitting_unmatched_right = true;
                    return self.next();
                }
                return Ok(None);
            }

            if self.right_exhausted && self.right_buffer.is_empty() {
                if let Some(ref left_row) = self.current_left {
                    if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                        let row = left_with_nulls(left_row, self.right_cols());
                        self.current_left = None;
                        self.advance_left()?;
                        self.left_matched = false;
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                }

                self.advance_left()?;
                self.left_matched = false;
                continue;
            }

            if self.right_buffer_idx < self.right_buffer.len() {
                let left_row = self.current_left.as_ref().unwrap();
                let right_row = &self.right_buffer[self.right_buffer_idx];
                self.right_buffer_idx += 1;

                let combined = combine_rows(left_row, right_row);
                self.left_matched = true;
                self.stats.rows_produced += 1;
                return Ok(Some(combined));
            }

            if self.buffering_key.is_some() {
                if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let left_row = self.current_left.as_ref().unwrap();
                    let row = left_with_nulls(left_row, self.right_cols());
                    self.advance_left()?;
                    self.left_matched = false;
                    self.buffering_key = None;
                    self.right_buffer.clear();
                    self.right_buffer_idx = 0;
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }

                self.advance_left()?;
                self.left_matched = false;

                if self.left_exhausted {
                    continue;
                }

                let left_row = self.current_left.as_ref().unwrap();
                let left_keys = extract_key(left_row, &self.left_keys, &self.left_schema)?;

                if keys_match(&left_keys, self.buffering_key.as_ref().unwrap()) {
                    self.right_buffer_idx = 0;
                    continue;
                } else {
                    self.buffering_key = None;
                    self.right_buffer.clear();
                    self.right_buffer_idx = 0;
                }
            }

            let left_row = self.current_left.as_ref().unwrap();
            let left_keys = extract_key(left_row, &self.left_keys, &self.left_schema)?;

            if self.right_exhausted {
                if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let row = left_with_nulls(left_row, self.right_cols());
                    self.advance_left()?;
                    self.left_matched = false;
                    self.stats.rows_produced += 1;
                    return Ok(Some(row));
                }
                self.advance_left()?;
                self.left_matched = false;
                continue;
            }

            let right_row = self.current_right.as_ref().unwrap();
            let right_keys = extract_key(right_row, &self.right_keys, &self.right_schema)?;

            match self.compare_keys(&left_keys, &right_keys) {
                Ordering::Less => {
                    if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                        let row = left_with_nulls(left_row, self.right_cols());
                        self.advance_left()?;
                        self.left_matched = false;
                        self.stats.rows_produced += 1;
                        return Ok(Some(row));
                    }
                    self.advance_left()?;
                    self.left_matched = false;
                }
                Ordering::Greater => {
                    self.advance_right()?;
                }
                Ordering::Equal => {
                    self.buffer_matching_right_rows(&left_keys)?;
                }
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}
