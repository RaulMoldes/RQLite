use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    collections::HashMap,
    io::{self, Error as IoError, ErrorKind},
};
use tempfile::{NamedTempFile, tempdir};

use crate::{
    AxmosDBConfig, DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE,
    database::{Database, errors::IntoBoxError, schema::Schema},
    io::{
        disk::FileOperations,
        pager::{Pager, SharedPager},
    },
    schema,
    storage::wal::{OwnedRecord, RecordRef, RecordType},
    structures::{bplustree::BPlusTree, comparator::Comparator},
    transactions::accessor::RcPageAccessor,
    types::{LogId, ObjectId, TransactionId},
};

mod btree;
mod macros;
mod sql;

pub(crate) use btree::*;
pub(crate) use sql::*;

use once_cell::sync::Lazy;

static PREDEFINED_SCHEMAS: Lazy<HashMap<&'static str, Schema>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert("users", users_schema());
    m.insert("orders", orders_schema());
    m.insert("products", products_schema());
    m.insert("idx_users_email", users_idx_schema());
    m.insert("order_items", order_items_schema());
    m
});

pub(crate) fn test_pager() -> io::Result<(Pager, NamedTempFile)> {
    let file = NamedTempFile::new()?;
    let dir = tempdir()?;
    let path = dir.path().join(file.path());
    let config = AxmosDBConfig::default();
    let mut pager = Pager::create(&path)?;
    pager.init(config)?;
    Ok((pager, file))
}

pub(crate) fn test_database() -> io::Result<(Database, NamedTempFile)> {
    let temp_file = NamedTempFile::new()?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(temp_file.path());
    let config = crate::AxmosDBConfig::default();
    let db = Database::new(config, &path)?;
    Ok((db, temp_file))
}

pub(crate) fn users_schema() -> Schema {
    schema!(keys: 1,
        id: Int { primary_key: "id_pk", not_null: "id_nn" },
        name: Text,
        email: Text { not_null: "email_nn" },
        age: Int,
        created_at: DateTime
    )
}

pub(crate) fn orders_schema() -> Schema {
    schema!(keys: 1,
        id: Int { primary_key: "id_pk", not_null: "id_nn" },
        user_id: Int,
        amount: Double
    )
}

pub(crate) fn products_schema() -> Schema {
    schema!(keys: 1,
        product_id: BigInt { primary_key: "product_id_pk", not_null: "product_id_nn" },
        name: Text,
        price: Double
    )
}

pub(crate) fn order_items_schema() -> Schema {
    schema!(keys: 1,
        id: Int { primary_key: "id_pk", not_null: "id_nn" },
        order_id: Int,
        product_id: Int,
        quantity: Int
    )
}

pub(crate) fn users_idx_schema() -> Schema {
    schema!(keys: 1,
        email: Text { primary_key: "email_pk", not_null: "email_nn" },
        user_id: BigUInt
    )
}

pub(crate) fn test_db_with_tables(
    table_names: Vec<String>,
) -> io::Result<(Database, NamedTempFile)> {
    let (db, tmp) = test_database()?;

    db.task_runner()
        .run(move |ctx| {
            for name in table_names {
                let schema = PREDEFINED_SCHEMAS
                    .get(name.as_str())
                    .ok_or_else(|| {
                        IoError::new(
                            ErrorKind::NotFound,
                            format!("Schema not found for table: {}", name),
                        )
                    })
                    .box_err()?;

                ctx.catalog()
                    .create_table(name.as_str(), schema.clone(), ctx.accessor())
                    .box_err()?;
            }
            Ok(())
        })
        .unwrap();

    Ok((db, tmp))
}

pub(crate) fn test_btree<Cmp: Comparator + Clone>(
    comparator: Cmp,
) -> io::Result<(BPlusTree<Cmp>, NamedTempFile)> {
    let (pager, f) = test_pager()?;
    let shared_pager = SharedPager::from(pager);
    let accessor = RcPageAccessor::new(shared_pager);
    let tree = BPlusTree::new(
        accessor,
        DEFAULT_BTREE_MIN_KEYS as usize,
        DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE as usize,
        comparator,
    )?;
    Ok((tree, f))
}

pub(crate) fn make_test_record(
    lsn_seed: u64,
    tid: u64,
    oid: u64,
    undo_len: usize,
    redo_len: usize,
) -> OwnedRecord {
    let mut rng = StdRng::seed_from_u64(lsn_seed);
    let undo: Vec<u8> = (0..undo_len).map(|_| rng.random()).collect();
    let redo: Vec<u8> = (0..redo_len).map(|_| rng.random()).collect();

    OwnedRecord::new(
        TransactionId::from(tid),
        if lsn_seed > 1 {
            Some(LogId::from(lsn_seed - 1))
        } else {
            None
        },
        ObjectId::from(oid),
        RecordType::Update,
        &undo,
        &redo,
    )
}

pub(crate) fn verify_record(
    record: &RecordRef<'_>,
    expected_seed: u64,
    tid: u64,
    undo_len: usize,
    redo_len: usize,
) {
    assert_eq!(record.tid(), TransactionId::from(tid));

    let mut rng = StdRng::seed_from_u64(expected_seed);

    let undo = record.undo_data();
    assert_eq!(undo.len(), undo_len);
    for &byte in undo {
        assert_eq!(byte, rng.random::<u8>());
    }

    let redo = record.redo_data();
    assert_eq!(redo.len(), redo_len);
    for &byte in redo {
        assert_eq!(byte, rng.random::<u8>());
    }
}
