//! Hash-based aggregation executor for GROUP BY queries.
use std::{collections::HashMap, vec::IntoIter};

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError},
        schema::Schema,
    },
    sql::{
        binder::ast::{AggregateFunction, BoundExpression},
        executor::{ExecutionState, ExecutionStats, Executor, Row, eval::ExpressionEvaluator},
        planner::{logical::AggregateExpr, physical::HashAggregateOp},
    },
    types::DataType,
};

/// Accumulator state for a single aggregate function.
/// Each accumulator tracks the running state needed to compute the final aggregate value.
#[derive(Clone, Debug)]
enum Accumulator {
    Count { count: u64 },
    Sum { sum: Option<DataType> },
    Avg { sum: Option<DataType>, count: u64 },
    Min { min: Option<DataType> },
    Max { max: Option<DataType> },
}

impl Accumulator {
    /// Create a new accumulator for the given aggregate function.
    fn new(func: &AggregateFunction) -> Self {
        match func {
            AggregateFunction::Count => Accumulator::Count { count: 0 },
            AggregateFunction::Sum => Accumulator::Sum { sum: None },
            AggregateFunction::Avg => Accumulator::Avg {
                sum: None,
                count: 0,
            },
            AggregateFunction::Min => Accumulator::Min { min: None },
            AggregateFunction::Max => Accumulator::Max { max: None },
        }
    }

    /// Accumulate a new value into the aggregate state.
    /// NULL values are skipped for most aggregates
    fn accumulate(&mut self, value: &DataType) -> ExecutionResult<()> {
        // Currently we are counting null values on all cases.
        // In the future, COUNT(column_name) should be considered an exception and do not count null values.
        if matches!(value, DataType::Null) && !matches!(self, Accumulator::Count { .. }) {
            return Ok(());
        }

        match self {
            Accumulator::Count { count } => {
                *count += 1;
            }
            Accumulator::Sum { sum } => {
                *sum = Some(match sum.take() {
                    None => value.clone(),
                    // Use DataType's built-in arithmetic_op for type-aware addition
                    Some(current) => current
                        .arithmetic_op(value, None, |a, b| a + b)
                        .map_err(|e| QueryExecutionError::Other(format!("Sum error: {}", e)))?,
                });
            }
            Accumulator::Avg { sum, count } => {
                *sum = Some(match sum.take() {
                    None => value.clone(),
                    Some(current) => current
                        .arithmetic_op(value, None, |a, b| a + b)
                        .map_err(|e| QueryExecutionError::Other(format!("Avg error: {}", e)))?,
                });
                *count += 1;
            }
            Accumulator::Min { min } => {
                *min = Some(match min.take() {
                    None => value.clone(),
                    Some(current) => {
                        // DataType implements PartialOrd with cross-type numeric comparison
                        if value < &current {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
            Accumulator::Max { max } => {
                *max = Some(match max.take() {
                    None => value.clone(),
                    Some(current) => {
                        if value > &current {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
        }
        Ok(())
    }

    /// Finalize the accumulator and return the aggregate result.
    fn finalize(self) -> ExecutionResult<DataType> {
        match self {
            Accumulator::Count { count } => Ok(DataType::BigInt((count as i64).into())),
            Accumulator::Sum { sum } => Ok(sum.unwrap_or(DataType::Null)),
            Accumulator::Avg { sum, count } => {
                if count == 0 {
                    return Ok(DataType::Null);
                }
                match sum {
                    Some(s) => {
                        // Use to_f64() which handles all numeric types via the macro-generated impl
                        let sum_f64 = s.to_f64();
                        if sum_f64.is_nan() {
                            return Err(QueryExecutionError::Other(
                                "Cannot compute average of non-numeric type".to_string(),
                            ));
                        }
                        Ok(DataType::Double((sum_f64 / count as f64).into()))
                    }
                    None => Ok(DataType::Null),
                }
            }
            Accumulator::Min { min } => Ok(min.unwrap_or(DataType::Null)),
            Accumulator::Max { max } => Ok(max.unwrap_or(DataType::Null)),
        }
    }
}

/// A group bucket containing accumulators for all aggregate functions.
struct GroupBucket {
    /// Key values for this group (for output).
    key: Vec<DataType>,
    /// One accumulator per aggregate expression.
    accumulators: Vec<Accumulator>,
}

impl GroupBucket {
    fn new(key: Vec<DataType>, aggregates: &[AggregateExpr]) -> Self {
        Self {
            key,
            accumulators: aggregates
                .iter()
                .map(|agg| Accumulator::new(&agg.func))
                .collect(),
        }
    }
}

/// Hash-based GROUP BY executor.
///
/// On first call to `next()`, consumes all input rows
/// On subsequent calls, for each row, evaluates group-by expressions to get the key. Hashes the key and look-up or create a bucket
///
/// Accumulates aggregate values into the bucket
/// Once all inputs are consumed, iterates through buckets emitting results
pub(crate) struct HashAggregate {
    op: HashAggregateOp,
    child: Box<dyn Executor>,
    state: ExecutionState,
    stats: ExecutionStats,

    /// Hash table mapping group keys to their buckets.
    /// We use Vec<DataType> as the key since DataType implements Hash + Eq.
    buckets: HashMap<Vec<DataType>, GroupBucket>,

    /// Iterator over completed buckets for output phase.
    output_iter: Option<IntoIter<GroupBucket>>,

    /// Whether the build phase is complete.
    build_complete: bool,
}

impl HashAggregate {
    pub(crate) fn new(op: HashAggregateOp, child: Box<dyn Executor>) -> Self {
        Self {
            op,
            child,
            state: ExecutionState::Closed,
            stats: ExecutionStats::default(),
            buckets: HashMap::new(),
            output_iter: None,
            build_complete: false,
        }
    }

    pub(crate) fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract group key values from a row by evaluating group-by expressions.
    fn extract_group_key(&self, row: &Row) -> ExecutionResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.op.input_schema);

        let mut key_values = Vec::with_capacity(self.op.group_by.len());
        for expr in &self.op.group_by {
            let value = evaluator.evaluate(expr.clone())?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Evaluate aggregate argument expressions and accumulate into a bucket.
    fn accumulate_row(
        row: &Row,
        bucket: &mut GroupBucket,
        schema: &Schema,
        aggregates: &[AggregateExpr],
    ) -> ExecutionResult<()> {
        let evaluator = ExpressionEvaluator::new(row, schema);
        let mut skip_nulls = true; // Skip nulls by default
        for (i, agg_expr) in aggregates.iter().enumerate() {
            let value = if agg_expr.arg.is_none() {
                DataType::Null // Value doesn't matter for COUNT(*)
            } else if let Some(ref arg) = agg_expr.arg {
                // Only evaluate non-Star arguments
                match arg {
                    BoundExpression::Star => DataType::Null,
                    other => evaluator.evaluate(other.clone())?,
                }
            } else {
                DataType::Null
            };

            bucket.accumulators[i].accumulate(&value)?;
        }

        Ok(())
    }

    /// Build phase: consume all input and build hash table.
    fn build(&mut self) -> ExecutionResult<()> {
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;

            let key = self.extract_group_key(&row)?;

            // Get or create bucket for this group
            let bucket = self
                .buckets
                .entry(key.clone())
                .or_insert_with(|| GroupBucket::new(key, &self.op.aggregates));

            Self::accumulate_row(&row, bucket, &self.op.input_schema, &self.op.aggregates)?;
        }

        self.build_complete = true;

        // Prepare output iterator
        let buckets: Vec<GroupBucket> = self.buckets.drain().map(|(_, v)| v).collect();
        self.output_iter = Some(buckets.into_iter());

        Ok(())
    }

    /// Produce a result row from a finalized bucket.
    fn bucket_to_row(&self, bucket: GroupBucket) -> ExecutionResult<Row> {
        let mut row = Row::with_capacity(bucket.key.len() + bucket.accumulators.len());

        // First, add group-by key values
        for value in bucket.key {
            row.push(value);
        }

        // Then, add finalized aggregate values
        for acc in bucket.accumulators {
            let value = acc.finalize()?;
            row.push(value);
        }

        Ok(row)
    }
}

impl Executor for HashAggregate {
    fn open(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Open | ExecutionState::Running) {
            return Err(QueryExecutionError::InvalidState(self.state));
        }

        self.child.open()?;
        self.state = ExecutionState::Open;
        self.stats = ExecutionStats::default();
        self.buckets.clear();
        self.output_iter = None;
        self.build_complete = false;

        Ok(())
    }

    fn next(&mut self) -> ExecutionResult<Option<Row>> {
        match self.state {
            ExecutionState::Closed => return Ok(None),
            ExecutionState::Open => {
                self.state = ExecutionState::Running;
                // Build phase on first call to next()
                self.build()?;
            }
            _ => {}
        }

        // Produce phase: emit rows from buckets
        if let Some(ref mut iter) = self.output_iter {
            if let Some(bucket) = iter.next() {
                let row = self.bucket_to_row(bucket)?;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }
        }

        // Handle empty GROUP BY (no rows or no group-by columns produces one row)
        if self.stats.rows_produced == 0 && self.op.group_by.is_empty() {
            // Emit a single row with default aggregate values
            let mut row = Row::with_capacity(self.op.aggregates.len());
            for agg_expr in &self.op.aggregates {
                let acc = Accumulator::new(&agg_expr.func);
                let value = acc.finalize()?;
                row.push(value);
            }
            self.stats.rows_produced += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn close(&mut self) -> ExecutionResult<()> {
        if matches!(self.state, ExecutionState::Closed) {
            return Ok(());
        }
        self.child.close()?;
        self.state = ExecutionState::Closed;
        self.buckets.clear();
        self.output_iter = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.op.output_schema
    }
}
