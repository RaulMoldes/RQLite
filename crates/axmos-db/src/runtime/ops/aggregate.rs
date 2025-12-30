// src/runtime/ops/aggregate.rs
use std::collections::HashMap;

use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeError, RuntimeResult,
        eval::ExpressionEvaluator,
    },
    sql::{
        binder::bounds::{AggregateFunction, BoundExpression},
        planner::{logical::AggregateExpr, physical::HashAggregateOp},
    },
    storage::tuple::Row,
    types::{DataType, TypeSystemError},
};

/// Accumulator state for a single aggregate function.
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
    fn accumulate(&mut self, value: &DataType) -> RuntimeResult<()> {
        // Skip NULL values for most aggregates except COUNT
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
                    Some(current) => current
                        .add(&value)
                        .map_err(|e| RuntimeError::Other(format!("Sum error: {}", e)))?,
                });
            }
            Accumulator::Avg { sum, count } => {
                *sum = Some(match sum.take() {
                    None => value.clone(),
                    Some(current) => current
                        .add(&value)
                        .map_err(|e| RuntimeError::Other(format!("Sum error: {}", e)))?,
                });
                *count += 1;
            }
            Accumulator::Min { min } => {
                *min = Some(match min.take() {
                    None => value.clone(),
                    Some(current) => {
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
    fn finalize(self) -> RuntimeResult<DataType> {
        match self {
            Accumulator::Count { count } => Ok(DataType::BigInt((count as i64).into())),
            Accumulator::Sum { sum } => Ok(sum.unwrap_or(DataType::Null)),
            Accumulator::Avg { sum, count } => {
                if count == 0 {
                    return Ok(DataType::Null);
                }
                match sum {
                    Some(s) => {
                        let sum_f64 = s.to_f64().ok_or(RuntimeError::TypeError(
                            TypeSystemError::UnexpectedDataType(s.kind()),
                        ))?;
                        if sum_f64.is_nan() {
                            return Err(RuntimeError::Other(
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
pub(crate) struct OpenHashAggregate<Child: RunningExecutor> {
    op: HashAggregateOp,
    child: Option<Child>,
    closed_child: Option<Child::Closed>,
    /// Hash table mapping group keys to their buckets.
    buckets: HashMap<Vec<DataType>, GroupBucket>,
    /// Iterator over completed buckets for output phase.
    output_iter: Option<std::vec::IntoIter<GroupBucket>>,
    /// Whether we've emitted the empty-group row
    emitted_empty_group: bool,
    /// Execution stats
    stats: ExecutionStats,
}

pub(crate) struct ClosedHashAggregate<Child: ClosedExecutor> {
    op: HashAggregateOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedHashAggregate<Child> {
    pub(crate) fn new(op: HashAggregateOp, child: Child, stats: Option<ExecutionStats>) -> Self {
        Self {
            op,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: ClosedExecutor> ClosedExecutor for ClosedHashAggregate<Child> {
    type Running = OpenHashAggregate<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenHashAggregate {
            op: self.op,
            child: Some(child),
            closed_child: None,
            buckets: HashMap::new(),
            output_iter: None,
            emitted_empty_group: false,
            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> OpenHashAggregate<Child> {
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Extract group key values from a row.
    fn extract_group_key(&self, row: &Row) -> RuntimeResult<Vec<DataType>> {
        let evaluator = ExpressionEvaluator::new(row, &self.op.input_schema);

        let mut key_values = Vec::with_capacity(self.op.group_by.len());
        for expr in &self.op.group_by {
            let value = evaluator.evaluate(expr)?;
            key_values.push(value);
        }

        Ok(key_values)
    }

    /// Build phase: consume all input and build hash table.
    fn build(&mut self) -> RuntimeResult<()> {
        if let Some(mut child) = self.child.take() {
            while let Some(row) = child.next()? {
                self.stats.rows_scanned += 1;

                let key = self.extract_group_key(&row)?;

                // Get or create bucket for this group
                let aggregates = &self.op.aggregates;
                let bucket = self
                    .buckets
                    .entry(key.clone())
                    .or_insert_with(|| GroupBucket::new(key, aggregates));

                // Accumulate the row
                let evaluator = ExpressionEvaluator::new(&row, &self.op.input_schema);
                for (i, agg_expr) in self.op.aggregates.iter().enumerate() {
                    let value = if agg_expr.arg.is_none() {
                        DataType::Null
                    } else if let Some(ref arg) = agg_expr.arg {
                        match arg {
                            BoundExpression::Star => DataType::Null,
                            other => evaluator.evaluate(other)?,
                        }
                    } else {
                        DataType::Null
                    };
                    bucket.accumulators[i].accumulate(&value)?;
                }
            }

            self.closed_child = Some(child.close()?);

            // Prepare output iterator
            let buckets: Vec<GroupBucket> = self.buckets.drain().map(|(_, v)| v).collect();
            self.output_iter = Some(buckets.into_iter());
        }

        Ok(())
    }

    /// Produce a result row from a finalized bucket.
    /// Produce a result row from a finalized bucket.
    fn bucket_to_row(&self, bucket: GroupBucket) -> RuntimeResult<Row> {
        let mut values: Vec<DataType> =
            Vec::with_capacity(bucket.key.len() + bucket.accumulators.len());

        let num_group_keys = bucket.key.len();

        // First, add group-by key values (cast to expected output type)
        for (i, value) in bucket.key.into_iter().enumerate() {
            let expected_type = self
                .op
                .output_schema
                .column(i)
                .ok_or(RuntimeError::ColumnNotFound(i))?
                .datatype();
            let casted = value
                .try_cast(expected_type)
                .map_err(|e| RuntimeError::TypeError(e))?;
            values.push(casted);
        }

        // Then, add finalized aggregate values (cast to expected output type)
        for (i, acc) in bucket.accumulators.into_iter().enumerate() {
            let value = acc.finalize()?;
            let col_idx = num_group_keys + i;
            let expected_type = self
                .op
                .output_schema
                .column(col_idx)
                .ok_or(RuntimeError::ColumnNotFound(i))?
                .datatype();
            let casted = value
                .try_cast(expected_type)
                .map_err(|e| RuntimeError::TypeError(e))?;
            values.push(casted);
        }

        Ok(Row::new(values.into_boxed_slice()))
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenHashAggregate<Child> {
    type Closed = ClosedHashAggregate<Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        // Build hash table on first call
        if self.child.is_some() {
            self.build()?;
        }

        // Produce phase: emit rows from buckets
        if let Some(ref mut iter) = self.output_iter {
            if let Some(bucket) = iter.next() {
                let row = self.bucket_to_row(bucket)?;
                self.stats.rows_produced += 1;
                return Ok(Some(row));
            }
        }

        // Handle empty GROUP BY (no group-by columns produces one row with default aggregates)
        if !self.emitted_empty_group && self.op.group_by.is_empty() && self.stats.rows_produced == 0
        {
            self.emitted_empty_group = true;
            let mut values: Vec<DataType> = Vec::with_capacity(self.op.aggregates.len());

            for (i, agg_expr) in self.op.aggregates.iter().enumerate() {
                let acc = Accumulator::new(&agg_expr.func);
                let value = acc.finalize()?;

                // Cast to expected output type
                let expected_type = self
                    .op
                    .output_schema
                    .column(i)
                    .ok_or(RuntimeError::ColumnNotFound(i))?
                    .datatype();
                let casted = value
                    .try_cast(expected_type)
                    .map_err(|e| RuntimeError::TypeError(e))?;
                values.push(casted);
            }

            self.stats.rows_produced += 1;
            return Ok(Some(Row::new(values.into_boxed_slice())));
        }

        Ok(None)
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedHashAggregate {
            op: self.op,
            child: self.closed_child.expect("Child should be closed"),
            stats: self.stats,
        })
    }
}
