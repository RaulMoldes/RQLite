use crate::{
    AxmosDBConfig, DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, DEFAULT_CACHE_SIZE,
    DEFAULT_PAGE_SIZE, default_num_workers,
    io::pager::{Pager, SharedPager},
    matrix_tests, param_tests, param3_tests,
    storage::{cell::OwnedCell, core::traits::BtreeBuffer, page::BtreePage},
    tree::{
        accessor::{AccessMode, Position},
        bplustree::{Btree, IterDirection, SearchResult},
        cell_ops::{AsKeyBytes, CellBuilder, Deserializable, KeyBytes, Reassembler, Serializable},
        comparators::{
            Comparator, VarlenComparator, fixed::FixedSizeBytesComparator,
            numeric::NumericComparator,
        },
    },
    types::varint::VarInt,
};

use rand::{RngCore, SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha20Rng;
use serial_test::serial;
use tempfile::tempdir;

#[derive(Debug, Clone)]
pub struct KeyValuePair {
    data: Box<[u8]>,
    key_size: usize,
}

impl KeyValuePair {
    pub fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: &K, value: &V) -> Self {
        let mut data = Vec::with_capacity(key.as_ref().len() + value.as_ref().len());
        data.extend_from_slice(key.as_ref());
        data.extend_from_slice(value.as_ref());
        Self {
            data: data.into_boxed_slice(),
            key_size: key.as_ref().len(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.data[..self.key_size]
    }
}

impl<'a> AsKeyBytes<'a> for KeyValuePair {
    fn as_key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes::from(self.key())
    }
}

impl AsRef<[u8]> for KeyValuePair {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<KeyValuePair> for Box<[u8]> {
    fn from(value: KeyValuePair) -> Self {
        Box::from(value.as_ref())
    }
}

impl From<KeyValuePair> for OwnedCell {
    fn from(value: KeyValuePair) -> Self {
        Self::new(value.as_ref())
    }
}

impl Serializable for KeyValuePair {
    fn serialized_size(&self) -> usize {
        self.data.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct TestKey(i32);

impl TestKey {
    pub fn new(value: i32) -> Self {
        Self(value)
    }
}

impl<'a> AsKeyBytes<'a> for TestKey {
    fn as_key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes::from(self.as_ref())
    }
}

impl AsRef<[u8]> for TestKey {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                &self.0 as *const i32 as *const u8,
                std::mem::size_of::<i32>(),
            )
        }
    }
}

impl From<TestKey> for Box<[u8]> {
    fn from(value: TestKey) -> Self {
        Box::from(value.as_ref())
    }
}

impl From<TestKey> for OwnedCell {
    fn from(value: TestKey) -> Self {
        Self::new(value.as_ref())
    }
}

impl Serializable for TestKey {
    fn serialized_size(&self) -> usize {
        std::mem::size_of::<i32>()
    }
}

impl Deserializable for TestKey {
    fn from_borrowed(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), std::mem::size_of::<i32>());
        let mut value = 0i32;
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut value as *mut i32 as *mut u8,
                std::mem::size_of::<i32>(),
            );
        }
        TestKey(value)
    }

    fn from_owned(bytes: Box<[u8]>) -> Self {
        Self::from_borrowed(&bytes)
    }
}

#[derive(Debug, Clone)]
pub struct TestVarLengthKey {
    encoded: Vec<u8>,
}

impl TestVarLengthKey {
    pub fn from_string(s: &str) -> Self {
        let raw = s.as_bytes();
        let mut buffer = [0u8; crate::varint::MAX_VARINT_LEN];
        let mut encoded = VarInt::encode(raw.len() as i64, &mut buffer).to_vec();
        encoded.extend_from_slice(raw);
        Self { encoded }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

impl AsRef<[u8]> for TestVarLengthKey {
    fn as_ref(&self) -> &[u8] {
        &self.encoded
    }
}

impl From<TestVarLengthKey> for Box<[u8]> {
    fn from(value: TestVarLengthKey) -> Self {
        Box::from(value.as_ref())
    }
}

impl From<TestVarLengthKey> for OwnedCell {
    fn from(value: TestVarLengthKey) -> Self {
        Self::new(value.as_ref())
    }
}

impl<'a> AsKeyBytes<'a> for TestVarLengthKey {
    fn as_key_bytes(&'a self) -> KeyBytes<'a> {
        KeyBytes::from(self.as_ref())
    }
}

impl Serializable for TestVarLengthKey {
    fn serialized_size(&self) -> usize {
        self.encoded.len()
    }
}

impl Deserializable for TestVarLengthKey {
    fn from_borrowed(bytes: &[u8]) -> Self {
        Self {
            encoded: bytes.to_vec(),
        }
    }

    fn from_owned(bytes: Box<[u8]>) -> Self {
        Self {
            encoded: bytes.into_vec(),
        }
    }
}

pub fn gen_repeating_pattern(pattern: &[u8], size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    while v.len() < size {
        let take = std::cmp::min(pattern.len(), size - v.len());
        v.extend_from_slice(&pattern[..take]);
    }
    v
}

pub fn gen_ovf_blob(page_size: usize, pages: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(page_size * pages);
    let header = format!(r#"{{"type":"overflow_test","pages":{pages},"page_size":{page_size}}}\n"#);
    out.extend_from_slice(header.as_bytes());
    let pattern = b"In the quiet field of night,\nDreams walk slow beneath the light.\n";
    let chunk_repeat = gen_repeating_pattern(pattern, page_size / 2);
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    for i in 0..pages {
        out.extend_from_slice(&chunk_repeat);
        let mut rnd = vec![0u8; page_size / 2];
        rng.fill_bytes(&mut rnd);
        out.extend_from_slice(&rnd);
        let meta = format!("\n--SEG {i}--\n");
        out.extend_from_slice(meta.as_bytes());
    }
    out
}

fn create_test_config(page_size: u32, cache_size: u16) -> AxmosDBConfig {
    AxmosDBConfig::new(
        page_size,
        cache_size,
        default_num_workers() as u8,
        DEFAULT_BTREE_MIN_KEYS,
        DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE,
    )
}

fn create_test_pager(page_size: u32, cache_size: u16) -> (SharedPager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let config = create_test_config(page_size, cache_size);
    let pager = Pager::from_config(config, &path).unwrap();
    (SharedPager::from(pager), dir)
}

fn create_default_pager() -> (SharedPager, tempfile::TempDir) {
    create_test_pager(DEFAULT_PAGE_SIZE, DEFAULT_CACHE_SIZE as u16)
}

fn create_test_tree<Cmp: Comparator + Clone>(pager: SharedPager, comparator: Cmp) -> Btree<Cmp> {
    let root = pager.write().allocate_page::<BtreePage>().unwrap();
    Btree::new(
        root,
        pager,
        DEFAULT_BTREE_MIN_KEYS as usize,
        DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE as usize,
        comparator,
    )
}

#[test]
fn test_position_new() {
    let pos = Position::new(42, 5);
    assert_eq!(pos.page(), 42);
    assert_eq!(pos.slot(), 5);
}

#[test]
fn test_position_start_pos() {
    let pos = Position::start_pos(100);
    assert_eq!(pos.page(), 100);
    assert_eq!(pos.slot(), 0);
}

#[test]
fn test_position_setters() {
    let mut pos = Position::new(1, 2);
    pos.set_page(10);
    pos.set_slot(20);
    assert_eq!(pos.page(), 10);
    assert_eq!(pos.slot(), 20);
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_search_empty_tree() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);

    let key = TestKey::new(42);
    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    assert!(matches!(result, SearchResult::NotFound(_)));
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_insert_remove_single_key() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(42);

    tree.insert(root, key.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    assert!(matches!(result, SearchResult::Found(_)));
    tree.accessor_mut().unwrap().clear();

    tree.remove(root, key.as_key_bytes()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    assert!(matches!(result, SearchResult::NotFound(_)));
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_insert_duplicates() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(42);
    tree.insert(root, key.clone()).unwrap();

    let result = tree.insert(root, key);
    assert!(result.is_err());
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_update_single_key() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(1);
    let kv1 = KeyValuePair::new(&key, b"Original");
    let kv2 = KeyValuePair::new(&key, b"Updated");

    tree.insert(root, kv1.clone()).unwrap();
    tree.update(root, kv2.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    if let SearchResult::Found(pos) = result {
        let mut reader = tree.into_reader();
        let data = reader.with_cell_at(pos, |d| d.to_vec()).unwrap();
        assert_eq!(data, kv2.as_ref());
    } else {
        panic!("Key should be found");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_upsert_single_key() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key1 = TestKey::new(1);
    let key2 = TestKey::new(2);

    let kv1 = KeyValuePair::new(&key1, b"Original");
    let kv2 = KeyValuePair::new(&key1, b"Updated");
    let kv3 = KeyValuePair::new(&key2, b"Created");

    tree.upsert(root, kv1.clone()).unwrap();
    tree.upsert(root, kv2.clone()).unwrap();
    tree.upsert(root, kv3.clone()).unwrap();

    let result1 = tree.search(key1.as_key_bytes(), AccessMode::Read).unwrap();
    assert!(matches!(result1, SearchResult::Found(_)));
    tree.accessor_mut().unwrap().clear();

    let result2 = tree.search(key2.as_key_bytes(), AccessMode::Read).unwrap();
    assert!(matches!(result2, SearchResult::Found(_)));
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_remove_nonexistent_fails() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(42);
    let result = tree.remove(root, key.as_key_bytes());
    assert!(result.is_err());
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_variable_length_keys() {
    let (pager, _dir) = create_default_pager();
    let comparator = VarlenComparator;
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..50 {
        let key = TestVarLengthKey::from_string(&format!("Hello_{i}"));
        tree.insert(root, key).unwrap();
    }

    for i in 0..50 {
        let key = TestVarLengthKey::from_string(&format!("Hello_{i}"));
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key Hello_{} not found",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_overflow_chain() {
    let (pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, 128);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(1);
    let data = gen_ovf_blob(4096, 20, 42);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    if let SearchResult::Found(pos) = result {
        let mut reader = tree.into_reader();
        let cell_data = reader.with_cell_at(pos, |d| d.to_vec()).unwrap();
        assert_eq!(cell_data.len(), kv.as_ref().len());
        assert_eq!(cell_data, kv.as_ref());
    } else {
        panic!("Key should be found");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_multiple_overflow_chain() {
    let (pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, 128);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..40 {
        let key = TestKey::new(i);
        tree.insert(root, key).unwrap();
    }

    let key = TestKey::new(40);
    let data = gen_ovf_blob(4096, 20, 42);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
    if let SearchResult::Found(pos) = result {
        let mut reader = tree.into_reader();
        let cell_data = reader.with_cell_at(pos, |d| d.to_vec()).unwrap();
        assert_eq!(cell_data.len(), kv.as_ref().len());
    } else {
        panic!("Key should be found");
    }
}

// REVIEW
#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_overflow_keys() {
    let (pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, 128);
    let comparator = VarlenComparator;
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let large_key_size = 5000;

    for i in 0..10 {
        let key_content = format!("KEY_{i:04}_") + &"X".repeat(large_key_size - 10);
        let key = TestVarLengthKey::from_string(&key_content);
        tree.insert(root, key).unwrap();
    }

    for i in 0..10 {
        let key_content = format!("KEY_{i:04}_") + &"X".repeat(large_key_size - 10);
        let key = TestVarLengthKey::from_string(&key_content);

        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        if let SearchResult::Found(pos) = result {
            let mut reader = tree.clone().into_reader();
            let data = reader.with_cell_at(pos, |d| d.to_vec()).unwrap();
            assert_eq!(data, key.as_bytes());
        } else {
            panic!("Key {} not found", i);
        }
        tree.accessor_mut().unwrap().clear();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_split_root() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..50 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key).unwrap();
    }

    for i in 0..50 {
        let key = TestKey::new(i * 10);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} not found",
            i * 10
        );
        tree.accessor_mut().unwrap().clear();
    }
}

fn test_insert_sequential(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} not found",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }
}

fn test_insert_reverse(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in (0..num_keys).rev() {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} not found",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }
}

fn test_insert_random(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let mut keys: Vec<i32> = (0..num_keys as i32).collect();
    let mut rng = ChaCha20Rng::seed_from_u64(42);
    keys.shuffle(&mut rng);

    for &k in &keys {
        let key = TestKey::new(k);
        tree.insert(root, key).unwrap();
    }

    for &k in &keys {
        let key = TestKey::new(k);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} not found",
            k
        );
        tree.accessor_mut().unwrap().clear();
    }
}

param_tests!(test_insert_sequential, n => [10, 50, 100, 500, 1000]);
param_tests!(test_insert_reverse, n => [10, 50, 100, 500, 1000]);
param_tests!(test_insert_random, n => [10, 50, 100, 500, 1000, 5000]);

fn test_delete_range(num_keys: usize, delete_start: usize, delete_end: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    let end = delete_end.min(num_keys);
    for i in delete_start..end {
        let key = TestKey::new(i as i32);
        tree.remove(root, key.as_key_bytes()).unwrap();
    }

    for i in delete_start..end {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::NotFound(_)),
            "Key {} should be deleted",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }

    for i in 0..delete_start {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} should exist",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }

    for i in end..num_keys {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(
            matches!(result, SearchResult::Found(_)),
            "Key {} should exist",
            i
        );
        tree.accessor_mut().unwrap().clear();
    }
}

param3_tests!(test_delete_range, n, start, end => [
    (100, 0, 10),
    (100, 45, 55),
    (100, 90, 100),
    (200, 0, 50),
    (200, 50, 150),
    (200, 150, 200),
    (500, 100, 400)
]);

// =============================================================================
// Deallocation Tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_dealloc_bfs_with_overflow_chains() {
    let (pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, 128);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..20 {
        let key = TestKey::new(i);
        tree.insert(root, key).unwrap();
    }

    for i in 20..30 {
        let key = TestKey::new(i);
        let data = gen_ovf_blob(4096, 5, i as u64);
        let kv = KeyValuePair::new(&key, &data);
        tree.insert(root, kv).unwrap();
    }

    tree.dealloc().unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_dealloc_bfs_large_tree() {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..10000 {
        let key = TestKey::new(i);
        tree.insert(root, key).unwrap();
    }

    tree.dealloc().unwrap();
}

// =============================================================================
// Iterator Tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_iter_positions_empty_tree() {
    let (pager, _dir) = create_default_pager();
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let tree = create_test_tree(pager, comparator);

    let positions: Vec<Position> = tree
        .into_iter_forward()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(positions.len(), 0);
}

fn test_iter_positions_forward(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    let positions: Vec<Position> = tree
        .into_iter_forward()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(
        positions.len(),
        num_keys,
        "Should iterate over all {} keys",
        num_keys
    );
}

fn test_iter_positions_backward(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    let positions: Vec<Position> = tree
        .into_iter_backward()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(
        positions.len(),
        num_keys,
        "Should iterate backwards over all {} keys",
        num_keys
    );
}

param_tests!(test_iter_positions_forward, n => [10, 50, 100, 500, 1000]);
param_tests!(test_iter_positions_backward, n => [10, 50, 100, 500, 1000]);

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_iter_positions_from_middle() {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key).unwrap();
    }

    let start_key = TestKey::new(500);
    let result = tree
        .search(start_key.as_key_bytes(), AccessMode::Read)
        .unwrap();

    if let SearchResult::Found(pos) = result {
        let positions: Vec<Position> = tree
            .try_into_positional_iterator(pos, IterDirection::Forward)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            positions.len(),
            50,
            "Should have 50 positions from 500 onwards"
        );
    } else {
        panic!("Key 500 should be found");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_iter_positions_from_backward() {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key).unwrap();
    }

    let start_key = TestKey::new(500);
    let result = tree
        .search(start_key.as_key_bytes(), AccessMode::Read)
        .unwrap();

    if let SearchResult::Found(pos) = result {
        let positions: Vec<Position> = tree
            .try_into_positional_iterator(pos, IterDirection::Backward)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            positions.len(),
            51,
            "Should have 51 positions from 500 backwards"
        );
    } else {
        panic!("Key 500 should be found");
    }
}

// =============================================================================
// BtreeReader with_cell_at Tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_with_cell_at_single() {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(42);
    tree.insert(root, key.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();

    if let SearchResult::Found(pos) = result {
        let mut reader = tree.into_reader();
        let value = reader
            .with_cell_at(pos, |data| {
                i32::from_ne_bytes(data[..4].try_into().unwrap())
            })
            .unwrap();

        assert_eq!(value, 42);
    } else {
        panic!("Key should be found");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_with_cell_at_overflow() {
    let (pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, 128);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    let key = TestKey::new(42);
    let data = gen_ovf_blob(4096, 10, 123);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.clone()).unwrap();

    let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();

    if let SearchResult::Found(pos) = result {
        let mut reader = tree.into_reader();
        let cell_len = reader.with_cell_at(pos, |d| d.len()).unwrap();
        assert_eq!(
            cell_len,
            kv.as_ref().len(),
            "Overflow cell should be fully reassembled"
        );
    } else {
        panic!("Key should be found");
    }
}

fn test_with_cells_at_batch(num_keys: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    let positions: Vec<Position> = tree
        .clone()
        .into_iter_forward()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    let mut reader = tree.into_reader();
    let values: Vec<i32> = reader
        .with_cells_at(&positions, |_pos, data| {
            i32::from_ne_bytes(data[..4].try_into().unwrap())
        })
        .unwrap();

    assert_eq!(values.len(), num_keys);

    for (i, &v) in values.iter().enumerate() {
        assert_eq!(v, i as i32, "Value at position {} should be {}", i, i);
    }
}

param_tests!(test_with_cells_at_batch, n => [10, 50, 100]);

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_with_cells_at_partial() {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i);
        tree.insert(root, key).unwrap();
    }

    let all_positions: Vec<Position> = tree
        .clone()
        .into_iter_forward()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    let selected: Vec<Position> = all_positions
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 10 == 0)
        .map(|(_, p)| *p)
        .collect();

    let mut reader = tree.into_reader();
    let keys: Vec<i32> = reader
        .with_cells_at(&selected, |_pos, data| {
            i32::from_ne_bytes(data[..4].try_into().unwrap())
        })
        .unwrap();

    assert_eq!(keys.len(), 10);
    assert_eq!(keys, vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
}

// =============================================================================
// CellBuilder Tests
// =============================================================================

fn test_cell_builder_small(page_size: usize) {
    let (pager, _dir) = create_test_pager(page_size as u32, 32);

    let page_id = pager.write().allocate_page::<BtreePage>().unwrap();
    let max_size = pager
        .write()
        .with_page::<BtreePage, _, _>(page_id, |page| page.free_space() as usize)
        .unwrap();

    let builder = CellBuilder::new(max_size, DEFAULT_BTREE_MIN_KEYS as usize, pager);

    let key = TestKey::new(42);
    let cell = builder.build_cell(key).unwrap();

    assert!(!cell.metadata().is_overflow());
}

fn test_cell_builder_overflow(page_size: usize) {
    let (pager, _dir) = create_test_pager(page_size as u32, 64);

    let page_id = pager.write().allocate_page::<BtreePage>().unwrap();
    let max_size = pager
        .write()
        .with_page::<BtreePage, _, _>(page_id, |page| page.free_space() as usize)
        .unwrap();

    let builder = CellBuilder::new(max_size, DEFAULT_BTREE_MIN_KEYS as usize, pager.clone());

    let large_data = gen_ovf_blob(page_size, 3, 42);
    let key = TestKey::new(1);
    let kv = KeyValuePair::new(&key, &large_data);
    let cell = builder.build_cell(kv).unwrap();

    assert!(cell.metadata().is_overflow());

    let mut reassembler = Reassembler::new(pager);
    reassembler.reassemble(cell.as_cell_ref()).unwrap();
    let reassembled = reassembler.into_boxed_slice();

    let expected_len = std::mem::size_of::<i32>() + large_data.len();
    assert_eq!(reassembled.len(), expected_len);
}

param_tests!(test_cell_builder_small, size => [4096, 8192, 16384]);
param_tests!(test_cell_builder_overflow, size => [4096, 8192]);

#[test]
#[cfg_attr(miri, ignore)]
#[serial]
fn test_reassembler_non_overflow() {
    let (pager, _dir) = create_default_pager();

    let page_id = pager.write().allocate_page::<BtreePage>().unwrap();
    let max_size = pager
        .write()
        .with_page::<BtreePage, _, _>(page_id, |page| page.free_space() as usize)
        .unwrap();

    let builder = CellBuilder::new(max_size, DEFAULT_BTREE_MIN_KEYS as usize, pager.clone());

    let key = TestKey::new(123);
    let cell = builder.build_cell(key.clone()).unwrap();

    let mut reassembler = Reassembler::new(pager);
    reassembler.reassemble(cell.as_cell_ref()).unwrap();
    let data = reassembler.into_boxed_slice();

    assert_eq!(data.as_ref(), key.as_ref());
}

fn test_insert_delete_stress(num_inserts: usize, num_deletes: usize) {
    let (pager, _dir) = create_default_pager();
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_inserts {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    let to_delete = num_deletes.min(num_inserts);
    for i in 0..to_delete {
        let key = TestKey::new(i as i32);
        tree.remove(root, key.as_key_bytes()).unwrap();
    }

    for i in 0..num_inserts {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();

        if i < to_delete {
            assert!(
                matches!(result, SearchResult::NotFound(_)),
                "Key {} should be deleted",
                i
            );
        } else {
            assert!(
                matches!(result, SearchResult::Found(_)),
                "Key {} should exist",
                i
            );
        }
        tree.accessor_mut().unwrap().clear();
    }
}

matrix_tests!(
    test_insert_delete_stress,
    inserts => [50, 100, 200, 500],
    deletes => [10, 25, 50, 100]
);

// =============================================================================
// Page Size Matrix Tests
// =============================================================================

fn test_tree_page_size_inserts(page_size: usize, num_keys: usize) {
    let (pager, _dir) = create_test_pager(page_size as u32, 64);
    let comparator = NumericComparator::with_type::<i32>();
    let mut tree = create_test_tree(pager, comparator);
    let root = tree.get_root();

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        tree.insert(root, key).unwrap();
    }

    for i in 0..num_keys {
        let key = TestKey::new(i as i32);
        let result = tree.search(key.as_key_bytes(), AccessMode::Read).unwrap();
        assert!(matches!(result, SearchResult::Found(_)));
        tree.accessor_mut().unwrap().clear();
    }
}

matrix_tests!(
    test_tree_page_size_inserts,
    page_size => [4096, 8192, 16384],
    num_keys => [50, 100, 500]
);
