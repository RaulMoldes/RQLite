use super::utils::{
    TestConfig, TestDb, assert_count, assert_key_exists, assert_key_missing, interleave,
    key_bytes_for_value, make_tuple, reverse, seq, shuffle, test_schema, zigzag,
};

use crate::{
    schema::{Column, Schema},
    storage::page::BtreePage,
    storage::tuple::{Row, TupleBuilder},
    tree::{accessor::BtreeWriteAccessor, bplustree::Btree},
    types::{Blob, DataType, DataTypeKind, UInt64},
};

macro_rules! test_btree {
    ($db:expr, $config:expr) => {{
        let root = $db
            .pager
            .write()
            .allocate_page::<BtreePage>()
            .expect("Failed to allocate root page");
        let accessor = BtreeWriteAccessor::new();
        Btree::new(root, $db.pager.clone(), $config.min_keys, $config.siblings)
            .with_accessor(accessor)
    }};
}

/// Test sequential insertion of tuples.
pub fn test_sequential_insert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("seq_insert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all keys
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);

        tree.insert(root, tuple, &schema)
            .expect(&format!("Insert failed for key {}", key));
    }

    // Verify all keys exist
    for key in &keys {
        assert_key_exists(&mut tree, *key, &schema)
            .expect(&format!("Key {} not found after insertion", key));
    }

    // Verify count
    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test random-order insertion.
pub fn test_random_insert(count: usize, seed: u64) {
    let config = TestConfig::default();
    let db = TestDb::new("random_insert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);
    let shuffled = shuffle(&keys, seed);

    // Insert in random order
    for (i, key) in shuffled.iter().enumerate() {
        let tuple = make_tuple(&schema, *key, 1);

        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Verify all keys exist (in original order)
    for (i, key) in keys.iter().enumerate() {
        assert_key_exists(&mut tree, *key, &schema).expect(&format!("Key {} not found", key));
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test reverse-order insertion (worst case for naive implementations).
pub fn test_reverse_insert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("reverse_insert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();

    let keys = seq(count);
    let reversed = reverse(&keys);

    for key in &reversed {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, *key, &schema).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test interleaved insertion.
pub fn test_interleaved_insert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("interleaved_insert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();

    let keys = seq(count);
    let interleaved = interleave(&keys);

    for key in &interleaved {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, *key, &schema).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test zigzag insertion.
pub fn test_zigzag_insert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("zigzag_insert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();

    let keys = seq(count);
    let zigzag_keys = zigzag(&keys);

    for key in &zigzag_keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, *key, &schema).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test deletion after insertion.
pub fn test_delete(count: usize, delete_count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("delete", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Delete first `delete_count` keys
    let to_delete: Vec<_> = keys.iter().take(delete_count).cloned().collect();

    for key in &to_delete {
        let key_bytes = key_bytes_for_value(*key);
        tree.remove(root, &key_bytes, &schema)
            .expect(&format!("Delete failed for key {}", key));
    }

    // Verify deleted keys are gone
    for key in &to_delete {
        assert_key_missing(&mut tree, *key, &schema)
            .expect(&format!("Deleted key {} still exists", key));
    }

    // Verify remaining keys exist
    for key in keys.iter().skip(delete_count) {
        assert_key_exists(&mut tree, *key, &schema)
            .expect(&format!("Remaining key {} not found", key));
    }

    assert_count(&mut tree, count - delete_count).expect("Count mismatch after delete");
}

/// Test random deletion order.
pub fn test_random_delete(count: usize, seed: u64) {
    let config = TestConfig::default();
    let db = TestDb::new("rand_delete", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Delete in random order
    let shuffled = shuffle(&keys, seed);
    for key in &shuffled {
        let key_bytes = key_bytes_for_value(*key);
        tree.remove(root, &key_bytes, &schema)
            .expect("Delete failed");
    }

    // Tree should be empty
    assert_count(&mut tree, 0).expect("Tree should be empty after deleting all keys");
}

/// Test upsert (insert or update).
pub fn test_upsert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("upsert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.upsert(root, tuple, &schema)
            .expect("Upsert (insert) failed");
    }

    assert_count(&mut tree, count).expect("Count mismatch after insert");

    // Upsert again (should update, not duplicate)
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 2);
        tree.upsert(root, tuple, &schema)
            .expect("Upsert (update) failed");
    }

    // Count should remain the same
    assert_count(&mut tree, count).expect("Count changed after upsert - duplicates created!");
}

/// Test insert + delete + reinsert cycle.
pub fn test_insert_delete_reinsert(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("reinsert", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Delete half
    let half = count / 2;
    for key in keys.iter().take(half) {
        let key_bytes = key_bytes_for_value(*key);
        tree.remove(root, &key_bytes, &schema)
            .expect("Delete failed");
    }

    assert_count(&mut tree, count - half).expect("Count mismatch after delete");

    // Reinsert deleted keys
    for key in keys.iter().take(half) {
        let tuple = make_tuple(&schema, *key, 2);
        tree.insert(root, tuple, &schema).expect("Reinsert failed");
    }

    assert_count(&mut tree, count).expect("Count mismatch after reinsert");

    // All keys should exist
    for key in &keys {
        assert_key_exists(&mut tree, *key, &schema).expect("Key not found after reinsert");
    }
}

/// Test tree deallocation.
pub fn test_dealloc(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("dealloc", &config).expect("Failed to create test db");
    let schema = test_schema();

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();
    let keys = seq(count);

    // Insert all keys to create a multi-level tree
    for key in &keys {
        let tuple = make_tuple(&schema, *key, 1);
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Verify data exists
    assert_count(&mut tree, count).expect("Count mismatch before dealloc");

    // Deallocate the tree
    tree.dealloc().expect("Dealloc failed");
}

/// Test deallocation of tree with overflow pages.
pub fn test_dealloc_with_overflow(count: usize) {
    let config = TestConfig::default().with_page_size(512); // Smaller pages to force overflow
    let db = TestDb::new("dealloc_overflow", &config).expect("Failed to create test db");

    // Schema with larger data to create overflow cells
    let schema = Schema::new_table(vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "data"),
    ]);

    let mut tree = test_btree!(&db, &config);
    let root = tree.get_root();

    // Insert with large values to create overflow pages
    for i in 0..count as u64 {
        let large_data = format!("{:0>500}", i); // 500-char string to force overflow
        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(i)),
            DataType::Blob(Blob::from(large_data)),
        ]));
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(&row, 1).expect("Failed to build tuple");
        tree.insert(root, tuple, &schema).expect("Insert failed");
    }

    // Deallocate the tree (should also deallocate overflow chains)
    tree.dealloc().expect("Dealloc with overflow failed");
}
