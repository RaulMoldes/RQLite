use crate::{
    DBConfig,
    io::pager::{Pager, SharedPager},
    storage::{cell::OwnedCell, page::BtreePage},
    tree::{
        accessor::{Accessor, BtreeWriteAccessor},
        bplustree::{Btree, BtreeError, BtreeResult, SearchResult},
        cell_ops::{AsKeyBytes, KeyBytes, Serializable},
        comparators::{
            Comparator, DynComparator,
            numeric::{NumericComparator, SignedNumericComparator},
        },
    },
    varint::MAX_VARINT_LEN,
};

use rand::{
    Rng, SeedableRng,
    distr::{Distribution, StandardUniform, uniform::SampleUniform},
    seq::SliceRandom,
};
use rand_chacha::ChaCha20Rng;

use std::{
    collections::BTreeSet,
    fmt::Debug as FmtDebug,
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub trait TestCopyKeyTrait:
    From<usize> + Serializable + FmtDebug + Copy + for<'a> AsKeyBytes<'a>
{
    fn get_comparator() -> DynComparator;
}

// Macro to automate the generation of btrees for different key types.
macro_rules! impl_test_key_comparator {
    ($key_ty:ty, signed) => {
        impl TestCopyKeyTrait for $key_ty {
            fn get_comparator() -> DynComparator {
                DynComparator::SignedNumeric(SignedNumericComparator::with_type::<Self>())
            }
        }
    };
    ($key_ty:ty, unsigned) => {
        impl TestCopyKeyTrait for $key_ty {
            fn get_comparator() -> DynComparator {
                DynComparator::StrictNumeric(NumericComparator::with_type::<Self>())
            }
        }
    };
}

// Macro to automate the generation of fixed size keys for the bplustree.
macro_rules! fixed_size_test_key {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub $inner);

        impl $name {
            pub fn new(value: $inner) -> Self {
                Self(value)
            }

            pub fn value(&self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self::new(value)
            }
        }

        impl From<usize> for $name {
            fn from(value: usize) -> Self {
                Self::new(value as $inner)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        &self.0 as *const $inner as *const u8,
                        std::mem::size_of::<$inner>(),
                    )
                }
            }
        }

        impl From<$name> for Box<[u8]> {
            fn from(value: $name) -> Self {
                Box::from(value.as_ref())
            }
        }

        impl From<$name> for $crate::storage::cell::OwnedCell {
            fn from(key: $name) -> $crate::storage::cell::OwnedCell {
                $crate::storage::cell::OwnedCell::new(key.as_ref())
            }
        }

        impl $crate::tree::cell_ops::Serializable for $name {
            fn serialized_size(&self) -> usize {
                std::mem::size_of::<$inner>()
            }
        }

        impl<'a> $crate::tree::cell_ops::AsKeyBytes<'a> for $name {
            fn as_key_bytes(&'a self) -> $crate::tree::cell_ops::KeyBytes<'a> {
                $crate::tree::cell_ops::KeyBytes::from(self.as_ref())
            }
        }
    };
}

fixed_size_test_key!(TestKeyI8, i8);
fixed_size_test_key!(TestKeyU8, u8);
fixed_size_test_key!(TestKeyI16, i16);
fixed_size_test_key!(TestKeyU16, u16);
fixed_size_test_key!(TestKeyI32, i32);
fixed_size_test_key!(TestKeyU32, u32);
fixed_size_test_key!(TestKeyI64, i64);
fixed_size_test_key!(TestKeyU64, u64);
fixed_size_test_key!(TestKeyI128, i128);
fixed_size_test_key!(TestKeyU128, u128);

impl_test_key_comparator!(TestKeyI8, signed);
impl_test_key_comparator!(TestKeyI16, signed);
impl_test_key_comparator!(TestKeyI32, signed);
impl_test_key_comparator!(TestKeyI64, signed);
impl_test_key_comparator!(TestKeyI128, signed);
impl_test_key_comparator!(TestKeyU8, unsigned);
impl_test_key_comparator!(TestKeyU16, unsigned);
impl_test_key_comparator!(TestKeyU32, unsigned);
impl_test_key_comparator!(TestKeyU64, unsigned);
impl_test_key_comparator!(TestKeyU128, unsigned);

/// Variable-length string key with VarInt prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TestVarKey {
    encoded: Vec<u8>,
}

impl TestVarKey {
    /// Create a key from a string (encodes with VarInt length prefix).
    pub fn from_str(s: &str) -> Self {
        use crate::types::VarInt;
        let raw = s.as_bytes();
        let mut buffer = [0u8; MAX_VARINT_LEN];
        let len_bytes = VarInt::encode(raw.len() as i64, &mut buffer);
        let mut encoded = len_bytes.to_vec();
        encoded.extend_from_slice(raw);
        Self { encoded }
    }

    /// Create a key with specific total size (for overflow testing).
    pub fn with_size(prefix: &str, total_size: usize) -> Self {
        use crate::types::VarInt;
        let prefix_bytes = prefix.as_bytes();
        let padding = total_size.saturating_sub(prefix_bytes.len());
        let mut raw = Vec::with_capacity(total_size);
        raw.extend_from_slice(prefix_bytes);
        raw.extend(std::iter::repeat(b'X').take(padding));

        let mut buffer = [0u8; MAX_VARINT_LEN];
        let len_bytes = VarInt::encode(raw.len() as i64, &mut buffer);
        let mut encoded = len_bytes.to_vec();
        encoded.extend_from_slice(&raw);
        Self { encoded }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

impl AsRef<[u8]> for TestVarKey {
    fn as_ref(&self) -> &[u8] {
        &self.encoded
    }
}

impl From<TestVarKey> for Box<[u8]> {
    fn from(value: TestVarKey) -> Self {
        value.encoded.into_boxed_slice()
    }
}

impl From<TestVarKey> for OwnedCell {
    fn from(key: TestVarKey) -> OwnedCell {
        OwnedCell::new(&key.encoded)
    }
}

impl Serializable for TestVarKey {
    fn serialized_size(&self) -> usize {
        self.encoded.len()
    }
}

impl<'a> AsKeyBytes<'a> for TestVarKey {
    fn as_key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes::from(self.encoded.as_slice())
    }
}

/// Key generators for tests.

/// Sequential i32 keys: [0, 1, 2, ..., count-1]
pub fn seq<T: From<usize>>(count: usize) -> Vec<T> {
    (0..count).map(|v| T::from(v)).collect()
}

/// Random unique keys
pub fn rand_unique<K, I>(count: usize, seed: u64) -> Vec<K>
where
    K: From<I> + Ord,
    I: Ord + Copy + SampleUniform,
    StandardUniform: Distribution<I>,
{
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut set = BTreeSet::new();

    while set.len() < count {
        set.insert(K::from(rng.random::<I>()));
    }

    set.into_iter().collect()
}

/// Random (possibly duplicate) keys
pub fn rand_keys<K, I>(count: usize, seed: u64) -> Vec<K>
where
    K: From<I>,
    I: Copy + SampleUniform,
    StandardUniform: Distribution<I>,
{
    let mut rng = ChaCha20Rng::seed_from_u64(seed);

    (0..count).map(|_| K::from(rng.random::<I>())).collect()
}

/// Sequential variable-length string keys.
pub fn seq_var(count: usize, prefix: &str) -> Vec<TestVarKey> {
    (0..count)
        .map(|i| TestVarKey::from_str(&format!("{}{:08}", prefix, i)))
        .collect()
}

/// Variable-length keys with random sizes.
pub fn rand_size_var(count: usize, seed: u64, min: usize, max: usize) -> Vec<TestVarKey> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            let size = rng.random_range(min..=max);
            TestVarKey::with_size(&format!("{:08}", i), size)
        })
        .collect()
}

/// Shuffle keys with given seed.
pub fn shuffle<T: Clone>(keys: &[T], seed: u64) -> Vec<T> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut result = keys.to_vec();
    result.shuffle(&mut rng);
    result
}

/// Reverse key order.
pub fn reverse<T: Clone>(keys: &[T]) -> Vec<T> {
    keys.iter().rev().cloned().collect()
}

/// Interleave: odd indices first, then even.
pub fn interleave<T: Clone>(keys: &[T]) -> Vec<T> {
    let mut result = Vec::with_capacity(keys.len());
    for i in (1..keys.len()).step_by(2) {
        result.push(keys[i].clone());
    }
    for i in (0..keys.len()).step_by(2) {
        result.push(keys[i].clone());
    }
    result
}

/// Generate keys in "zigzag" pattern: 0, n-1, 1, n-2, 2, n-3, ...
pub fn zigzag<T: Clone>(keys: &[T]) -> Vec<T> {
    let n = keys.len();
    let mut result = Vec::with_capacity(n);
    let mut lo = 0;
    let mut hi = n.saturating_sub(1);
    let mut take_low = true;

    while lo <= hi && result.len() < n {
        if take_low {
            result.push(keys[lo].clone());
            lo += 1;
        } else {
            result.push(keys[hi].clone());
            if hi == 0 {
                break;
            }
            hi -= 1;
        }
        take_low = !take_low;
    }
    result
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
            cache_size: 64,
        }
    }
}

// Test database data structure
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
        // Flush and cleanup
        let _ = self.pager.write().flush();
        cleanup(&self.path);
    }
}

// Type alias for testing btrees.
pub(crate) type TestBtree<Cmp> = Btree<BtreePage, Cmp, BtreeWriteAccessor>;

/// Assertion utilities
/// Verify a key exists in the tree.
pub fn assert_key_exists<K, Cmp: Comparator + Clone>(
    tree: &mut TestBtree<Cmp>,
    key: &K,
) -> BtreeResult<()>
where
    K: for<'a> AsKeyBytes<'a>,
{
    let key_bytes = key.as_key_bytes();
    match tree.search(key_bytes)? {
        SearchResult::Found(_) => {
            tree.accessor_mut()?.clear();
            Ok(())
        }
        SearchResult::NotFound(_) => {
            tree.accessor_mut()?.clear();
            Err(BtreeError::Other("Key not found".into()))
        }
    }
}

/// Verify a key does not exist in the tree.
pub fn assert_key_missing<K, Cmp: Comparator + Clone>(
    tree: &mut TestBtree<Cmp>,
    key: &K,
) -> BtreeResult<()>
where
    K: for<'a> AsKeyBytes<'a>,
{
    let key_bytes = key.as_key_bytes();
    match tree.search(key_bytes)? {
        SearchResult::Found(_) => {
            tree.accessor_mut()?.clear();
            Err(BtreeError::Other("Key should not exist".into()))
        }
        SearchResult::NotFound(_) => {
            tree.accessor_mut()?.clear();
            Ok(())
        }
    }
}

/// Count all keys in the tree via forward iteration.
pub fn count_keys<Cmp: Comparator + Clone>(tree: &mut TestBtree<Cmp>) -> BtreeResult<usize> {
    let mut count = 0;
    let mut iter = tree.iter_forward()?;
    while let Some(result) = iter.next() {
        let _ = result?;
        count += 1;
    }
    Ok(count)
}

/// Verify key count matches expected.
pub fn assert_count<Cmp: Comparator + Clone>(
    tree: &mut TestBtree<Cmp>,
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
