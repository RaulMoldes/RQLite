use crate::{
    DBConfig,
    io::pager::{Pager, SharedPager},
    multithreading::coordinator::Snapshot,
    schema::{Column, Schema},
    storage::tuple::{Row, Tuple, TupleBuilder},
    tree::{
        accessor::TreeReader,
        bplustree::{Btree, BtreeError, BtreeResult, SearchResult},
    },
    types::{Blob, DataType, DataTypeKind, UInt64},
};

use rand::{SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha20Rng;

use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Test configuration builder.
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub page_size: usize,
    pub min_keys: usize,
    pub siblings: usize,
    pub cache_size: usize,
}

impl TestConfig {
    pub fn with_page_size(mut self, size: usize) -> Self {
        self.page_size = size;
        self
    }

    pub fn with_min_keys(mut self, keys: usize) -> Self {
        self.min_keys = keys;
        self
    }

    pub fn with_siblings(mut self, siblings: usize) -> Self {
        self.siblings = siblings;
        self
    }

    pub fn to_db_config(&self) -> DBConfig {
        DBConfig {
            page_size: self.page_size,
            min_keys_per_page: self.min_keys,
            num_siblings_per_side: self.siblings,
            cache_size: self.cache_size,
            pool_size: 2,
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            page_size: 4096,
            min_keys: 3,
            siblings: 1,
            cache_size: 200,
        }
    }
}

/// Generate unique temporary database path.
pub fn temp_db_path(prefix: &str) -> PathBuf {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "axmos_{}_{}_{}_{}.db",
        prefix,
        process::id(),
        counter,
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

/// Clean up test database files.
pub fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    if let Some(dir) = path.parent() {
        let _ = std::fs::remove_file(dir.join("axmos.log"));
    }
}

/// Test database wrapper that handles setup and cleanup.
pub struct TestDb {
    pub path: PathBuf,
    pub pager: SharedPager,
}

impl TestDb {
    pub fn new(prefix: &str, config: &TestConfig) -> io::Result<Self> {
        let path = temp_db_path(prefix);
        let pager = Pager::from_config(config.to_db_config(), &path)?;
        let shared_pager = SharedPager::from(pager);
        Ok(Self {
            path,
            pager: shared_pager,
        })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = self.pager.write().flush();
        cleanup(&self.path);
    }
}

/// Schema factory for test tuples.
/// Creates a simple schema with a BigUInt key and a Blob value.
pub fn test_schema() -> Schema {
    Schema::new_table(vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "data"),
    ])
}

/// Create a test tuple with the given key value.
pub fn make_tuple(schema: &Schema, key: u64, xmin: u64) -> Tuple {
    let row = Row::new(Box::new([
        DataType::BigUInt(UInt64(key)),
        DataType::Blob(Blob::from(format!("value_{}", key))),
    ]));
    let builder = TupleBuilder::from_schema(schema);
    builder.build(row, xmin).expect("Failed to build tuple")
}

/// Extract the key bytes from a tuple for searching.
pub fn tuple_key_bytes(tuple: &Tuple, schema: &Schema) -> Box<[u8]> {
    let offset = Tuple::keys_offset(schema.num_values());
    Box::from(&tuple.effective_data()[offset..])
}

/// Extract key bytes for a given key value (without creating a full tuple).
pub fn key_bytes_for_value(key: u64) -> Box<[u8]> {
    let key_data = UInt64(key);
    Box::from(key_data.as_ref())
}

/// Type alias for testing btrees.
pub(crate) type TestBtree<Acc> = Btree<Acc>;

/// Create a default snapshot for testing.
pub fn test_snapshot() -> Snapshot {
    Snapshot::default()
}

/// Sequential keys: [0, 1, 2, ..., count-1]
pub fn seq(count: usize) -> Vec<u64> {
    (0..count as u64).collect()
}

/// Shuffle keys with given seed.
pub fn shuffle(keys: &[u64], seed: u64) -> Vec<u64> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut result = keys.to_vec();
    result.shuffle(&mut rng);
    result
}

/// Reverse key order.
pub fn reverse(keys: &[u64]) -> Vec<u64> {
    keys.iter().rev().cloned().collect()
}

/// Interleave: odd indices first, then even.
pub fn interleave(keys: &[u64]) -> Vec<u64> {
    let mut result = Vec::with_capacity(keys.len());
    for i in (1..keys.len()).step_by(2) {
        result.push(keys[i]);
    }
    for i in (0..keys.len()).step_by(2) {
        result.push(keys[i]);
    }
    result
}

/// Generate keys in "zigzag" pattern: 0, n-1, 1, n-2, 2, n-3, ...
pub fn zigzag(keys: &[u64]) -> Vec<u64> {
    let n = keys.len();
    let mut result = Vec::with_capacity(n);
    let mut lo = 0;
    let mut hi = n.saturating_sub(1);
    let mut take_low = true;

    while lo <= hi && result.len() < n {
        if take_low {
            result.push(keys[lo]);
            lo += 1;
        } else {
            result.push(keys[hi]);
            if hi == 0 {
                break;
            }
            hi -= 1;
        }
        take_low = !take_low;
    }
    result
}

/// Verify a key exists in the tree.
pub fn assert_key_exists<Acc: TreeReader>(
    tree: &mut TestBtree<Acc>,
    key: u64,
    schema: &Schema,
) -> BtreeResult<()> {
    let key_bytes = key_bytes_for_value(key);
    match tree.search(&key_bytes, schema)? {
        SearchResult::Found(_) => {
            tree.accessor_mut()?.clear();
            Ok(())
        }
        SearchResult::NotFound(_) => {
            tree.accessor_mut()?.clear();
            Err(BtreeError::Other(format!("Key {} not found", key)))
        }
    }
}

/// Verify a key does not exist in the tree.
pub fn assert_key_missing<Acc: TreeReader>(
    tree: &mut TestBtree<Acc>,
    key: u64,
    schema: &Schema,
) -> BtreeResult<()> {
    let key_bytes = key_bytes_for_value(key);
    match tree.search(&key_bytes, &schema)? {
        SearchResult::Found(_) => {
            tree.accessor_mut()?.clear();
            Err(BtreeError::Other(format!("Key {} should not exist", key)))
        }
        SearchResult::NotFound(_) => {
            tree.accessor_mut()?.clear();
            Ok(())
        }
    }
}

/// Count all keys in the tree via forward iteration.
pub fn count_keys<Acc: TreeReader + Default>(tree: &mut TestBtree<Acc>) -> BtreeResult<usize> {
    let mut count = 0;
    let mut iter = tree.iter_forward()?;
    while let Some(result) = iter.next() {
        let _ = result?;
        count += 1;
    }
    Ok(count)
}

/// Verify key count matches expected.
pub fn assert_count<Acc: TreeReader + Default>(
    tree: &mut TestBtree<Acc>,
    expected: usize,
) -> BtreeResult<()> {
    if expected == 0 {
        assert!(tree.is_empty()?);
        return Ok(());
    }
    let actual = count_keys(tree)?;
    if actual != expected {
        Err(BtreeError::Other(format!(
            "Count mismatch: expected {}, got {}",
            expected, actual
        )))
    } else {
        Ok(())
    }
}
