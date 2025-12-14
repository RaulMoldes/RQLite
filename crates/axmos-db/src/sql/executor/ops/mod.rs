// src/sql/executor/ops/mod.rs

use crate::{
    database::{
        errors::{ExecutionResult, QueryExecutionError, TypeError},
        schema::Schema,
    },
    sql::executor::Row,
    storage::tuple::{Tuple, TupleRef},
    transactions::Snapshot,
    types::{DataType, DataTypeKind, TransactionId, UInt64},
};

pub(crate) mod agg;
pub(crate) mod delete;
pub(crate) mod filter;
pub(crate) mod insert;
pub(crate) mod limit;
pub(crate) mod materialize;
pub(crate) mod project;
pub(crate) mod seq_scan;
pub(crate) mod sort;
pub(crate) mod top_n;
pub(crate) mod update;
pub(crate) mod values;

pub(crate) fn get_row_id(row: &Row) -> ExecutionResult<UInt64> {
    let row_id = match &row[0] {
        DataType::BigUInt(id) => *id,
        _ => {
            return Err(QueryExecutionError::Type(TypeError::TypeMismatch {
                left: row[0].kind(),
                right: DataTypeKind::BigUInt,
            }));
        }
    };

    Ok(row_id)
}

/// Rebuilds an index entry from a list of values and column indexes, given the schema.
pub(crate) fn rebuild_index_entry<'schema>(
    index_values: &[DataType],
    columns: &[(usize, DataTypeKind)],
    schema: &'schema Schema,
    row_id: UInt64,
    tx_id: TransactionId,
) -> ExecutionResult<Tuple<'schema>> {
    let mut new_index_values: Vec<DataType> = Vec::with_capacity(columns.len() + 1);
    for (col_idx, _) in columns {
        new_index_values.push(index_values[*col_idx].clone());
    }
    new_index_values.push(DataType::BigUInt(row_id));
    Ok(Tuple::new(&new_index_values, schema, tx_id)?)
}

/// Loads the tuple data into memory in order to be able to fix up the indexes later.
pub(crate) fn capture_tuple_values_unchecked(
    old_bytes: Box<[u8]>,
    schema: &Schema,
    snapshot: &Snapshot,
) -> ExecutionResult<Vec<DataType>> {
    let old_tuple = TupleRef::read_for_snapshot(old_bytes.as_ref(), schema, snapshot)?.unwrap();

    // Collect values for index deletion
    let mut vals = Vec::with_capacity(schema.values().len());
    for i in 0..schema.values().len() {
        vals.push(old_tuple.value(i)?.to_owned());
    }
    Ok(vals)
}

pub(crate) fn cast_to_column_type(
    value: DataType,
    expected_kind: DataTypeKind,
) -> ExecutionResult<DataType> {
    if value.kind() == expected_kind {
        return Ok(value);
    }
    value.try_cast(expected_kind).map_err(|_| {
        QueryExecutionError::Type(TypeError::CastError {
            from: value.kind(),
            to: expected_kind,
        })
    })
}
