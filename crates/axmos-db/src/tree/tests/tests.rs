use super::utils::{
    TestConfig, TestCopyKeyTrait, TestDb, TestKeyI32, TestKeyI64, assert_count, assert_key_exists,
    assert_key_missing, interleave, reverse, seq, shuffle, zigzag,
};

use crate::{
    storage::page::BtreePage,
    tree::{accessor::BtreeWriteAccessor, bplustree::Btree},
};

macro_rules! test_btree {
    ($db:expr, $config:expr, $key_ty:ty) => {{
        let root = $db
            .pager
            .write()
            .allocate_page::<BtreePage>()
            .expect("Failed to allocate root page");
        let accessor = BtreeWriteAccessor::new();
        let comparator = <$key_ty as TestCopyKeyTrait>::get_comparator();
        Btree::new(
            root,
            $db.pager.clone(),
            $config.min_keys,
            $config.siblings,
            comparator,
        )
        .with_accessor(accessor)
    }};
}

/// Test sequential insertion of fixed-size keys.
pub fn test_sequential_insert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("seq_insert", &config).expect("Failed to create test db");

    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);

    // Insert all keys
    for key in &keys {
        tree.insert(root, *key)
            .expect(&format!("Insert failed for key {:?}", key));
    }

    // Verify all keys exist
    for key in &keys {
        assert_key_exists(&mut tree, key)
            .expect(&format!("Key {:?} not found after insertion", key));
    }

    // Verify count
    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test random-order insertion.
fn test_random_insert<T: TestCopyKeyTrait>(count: usize, seed: u64) {
    let config = TestConfig::default();
    let db = TestDb::new("random_insert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);
    let shuffled = shuffle(&keys, seed);

    // Insert in random order
    for (i, key) in shuffled.iter().enumerate() {
        tree.insert(root, *key).expect("Insert failed");
    }

    // Verify all keys exist (in original order)
    for key in &keys {
        assert_key_exists(&mut tree, key).expect(&format!("Key not found {:?}", key));
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test reverse-order insertion (worst case for naive implementations).
fn test_reverse_insert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("reverse_insert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();

    let keys = seq::<T>(count);
    let reversed = reverse(&keys);

    for key in &reversed {
        tree.insert(root, *key).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, key).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test interleaved insertion
fn test_interleaved_insert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("interleaved_insert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();

    let keys = seq::<T>(count);
    let interleaved = interleave(&keys);

    for key in &interleaved {
        tree.insert(root, *key).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, key).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test zigzag insertion
fn test_zigzag_insert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("zigzag_insert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();

    let keys = seq::<T>(count);
    let zigzag = zigzag(&keys);

    for key in &zigzag {
        tree.insert(root, *key).expect("Insert failed");
    }

    for key in &keys {
        assert_key_exists(&mut tree, key).expect("Key not found");
    }

    assert_count(&mut tree, count).expect("Count mismatch");
}

/// Test deletion after insertion.
fn test_delete<T: TestCopyKeyTrait>(count: usize, delete_count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("delete", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);

    // Insert all
    for key in &keys {
        tree.insert(root, *key).expect("Insert failed");
    }

    // Delete first `delete_count` keys
    let to_delete: Vec<_> = keys.iter().take(delete_count).cloned().collect();

    for key in &to_delete {
        tree.remove(root, key.key_bytes())
            .expect(&format!("Delete failed for key {:?}", key));
    }

    // Verify deleted keys are gone
    for key in &to_delete {
        assert_key_missing(&mut tree, key).expect(&format!("Deleted key {:?} still exists", key));
    }

    // Verify remaining keys exist
    for key in keys.iter().skip(delete_count) {
        assert_key_exists(&mut tree, key).expect(&format!("Remaining key {:?} not found", key));
    }

    assert_count(&mut tree, count - delete_count).expect("Count mismatch after delete");
}

/// Test random deletion order.
fn test_random_delete<T: TestCopyKeyTrait>(count: usize, seed: u64) {
    let config = TestConfig::default();
    let db = TestDb::new("rand_delete", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);

    // Insert all
    for key in &keys {
        tree.insert(root, *key).expect("Insert failed");
    }

    // Delete in random order
    let shuffled = shuffle(&keys, seed);
    for key in &shuffled {
        tree.remove(root, key.key_bytes()).expect("Delete failed");
    }

    // Tree should be empty
    assert_count(&mut tree, 0).expect("Tree should be empty after deleting all keys");
}

/// Test upsert (insert or update).
fn test_upsert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("upsert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);

    // Insert all
    for key in &keys {
        tree.upsert(root, *key).expect("Upsert (insert) failed");
    }

    assert_count(&mut tree, count).expect("Count mismatch after insert");

    // Upsert again (should update, not duplicate)
    for key in &keys {
        tree.upsert(root, *key).expect("Upsert (update) failed");
    }

    // Count should remain the same
    assert_count(&mut tree, count).expect("Count changed after upsert - duplicates created!");
}

/// Test insert + delete + reinsert cycle.
pub fn test_insert_delete_reinsert<T: TestCopyKeyTrait>(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("reinsert", &config).expect("Failed to create test db");
    let mut tree = test_btree!(&db, &config, T);
    let root = tree.get_root();
    let keys = seq::<T>(count);

    // Insert all
    for key in &keys {
        tree.insert(root, *key).expect("Insert failed");
    }

    // Delete half
    let half = count / 2;
    for key in keys.iter().take(half) {
        tree.remove(root, key.key_bytes()).expect("Delete failed");
    }

    // tree.export_to("after_first_delete.json").unwrap();

    assert_count(&mut tree, count - half).expect("Count mismatch after delete");

    // Reinsert deleted keys
    for key in keys.iter().take(half) {
        tree.insert(root, *key).expect("Reinsert failed");
    }

    assert_count(&mut tree, count).expect("Count mismatch after reinsert");

    // All keys should exist
    for key in &keys {
        assert_key_exists(&mut tree, key).expect("Key not found after reinsert");
    }
}

// Macro to autogenerate test functors.
macro_rules! fn_test {
    (
        $(#[$attr:meta])*
        $vis:vis test = $test_fn:ident ( $( $arg:ident : $arg_ty:ty ),* ),
        keys = [ $( $fn_name:ident : $key_ty:ty ),+ $(,)? ]
    ) => {
        fn_test!(@expand $(#[$attr])* $vis, $test_fn, [ $( $arg : $arg_ty ),* ], [ $( $fn_name : $key_ty ),+ ]);
    };

    (@expand $(#[$attr:meta])* $vis:vis, $test_fn:ident, [ $( $arg:ident : $arg_ty:ty ),* ], [ $fn_name:ident : $key_ty:ty ]) => {
        $(#[$attr])*
        $vis fn $fn_name( $( $arg : $arg_ty ),* ) {
            $test_fn::<$key_ty>( $( $arg ),* );
        }
    };

    (@expand $(#[$attr:meta])* $vis:vis, $test_fn:ident, [ $( $arg:ident : $arg_ty:ty ),* ], [ $fn_name:ident : $key_ty:ty, $( $rest:tt )+ ]) => {
        $(#[$attr])*
        $vis fn $fn_name( $( $arg : $arg_ty ),* ) {
            $test_fn::<$key_ty>( $( $arg ),* );
        }
        fn_test!(@expand $(#[$attr])* $vis, $test_fn, [ $( $arg : $arg_ty ),* ], [ $( $rest )+ ]);
    };
}

fn_test!(
    pub test = test_sequential_insert(count: usize),
    keys = [
        test_seq_i64: TestKeyI64,
        test_seq_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_random_insert(count: usize, seed: u64),
    keys = [
        test_rand_i64: TestKeyI64,
        test_rand_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_interleaved_insert(count: usize),
    keys = [
        test_interleaved_i64: TestKeyI64,
        test_interleaved_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_upsert(count: usize),
    keys = [
        test_upsert_i64: TestKeyI64,
        test_upsert_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_insert_delete_reinsert(count: usize),
    keys = [
        test_insert_delete_reinsert_i64: TestKeyI64,
        test_insert_delete_reinsert_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_random_delete(count: usize, seed: u64),
    keys = [
        test_rand_del_i64: TestKeyI64,
        test_rand_del_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_delete(count: usize, delete_count: usize),
    keys = [
        test_del_i64: TestKeyI64,
        test_del_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_reverse_insert(count: usize),
    keys = [
        test_reverse_i64: TestKeyI64,
        test_reverse_i32: TestKeyI32,
    ]
);

fn_test!(
    pub test = test_zigzag_insert(count: usize),
    keys = [
        test_zigzag_i64: TestKeyI64,
        test_zigzag_i32: TestKeyI32,
    ]
);
