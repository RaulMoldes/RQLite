mod macros;

use crate::{
    AxmosDBConfig, IncrementalVaccum, TextEncoding, assert_cmp, delete_test, insert_tests,
    io::{
        frames::{FrameAccessMode, Position},
        pager::{Pager, SharedPager},
    },
    structures::{
        bplustree::{
            BPlusTree, Comparator, FixedSizeBytesComparator, NumericComparator, SearchResult,
            VarlenComparator,
        },
        kvp::KeyValuePair,
    },
    transactions::{TransactionController, worker::Worker},
    types::VarInt,
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serial_test::serial;
use std::{cmp::Ordering, io};

use tempfile::tempdir;

// Test key type
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
struct TestKey(i32);

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

struct TestVarLengthKey(Vec<u8>);

impl TestVarLengthKey {
    pub fn from_string(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }

    fn as_bytes(&self) -> Vec<u8> {
        let mut buffer = [0u8; crate::types::varint::MAX_VARINT_LEN];
        let mut bytes_data = VarInt::encode(self.0.len() as i64, &mut buffer).to_vec();
        bytes_data.extend_from_slice(self.0.as_ref());
        bytes_data
    }
}

fn create_test_btree<Cmp: Comparator>(
    page_size: u32,
    capacity: usize,
    min_keys: usize,
    comparator: Cmp,
) -> io::Result<BPlusTree<Cmp>> {
    let dir = tempdir()?;
    let path = dir.path().join("test_btree.db");

    let config = AxmosDBConfig {
        page_size,
        cache_size: Some(capacity as u16),
        incremental_vacuum_mode: IncrementalVaccum::Disabled,
        min_keys: min_keys as u8,
        text_encoding: TextEncoding::Utf8,
    };

    let shared_pager = SharedPager::from(Pager::from_config(config, &path)?);
    let ctl = TransactionController::new();
    let worker = Worker::new(shared_pager);
    BPlusTree::new(worker, min_keys, 2, comparator)
}

pub fn gen_random_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; size];
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    rng.fill_bytes(&mut v);
    v
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

#[test]
fn test_comparators() -> std::io::Result<()> {
    let fixed_comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let varlen_comparator = VarlenComparator;
    let numeric_comparator = NumericComparator::with_type::<TestKey>();

    assert_cmp!(fixed_comparator, TestKey(1), TestKey(1), Ordering::Equal);
    assert_cmp!(fixed_comparator, TestKey(511), TestKey(767), Ordering::Less);
    assert_cmp!(fixed_comparator, TestKey(3), TestKey(2), Ordering::Greater);
    // Fixed size comparators compare byte by byte, therefore the ordering might not always match strict numerical order. That is why we have [NumericComparator].
    assert_cmp!(
        fixed_comparator,
        TestKey(511),
        TestKey(762),
        Ordering::Greater
    );
    assert_cmp!(fixed_comparator, TestKey(251), TestKey(763), Ordering::Less);

    assert_cmp!(numeric_comparator, TestKey(1), TestKey(1), Ordering::Equal);
    assert_cmp!(
        numeric_comparator,
        TestKey(511),
        TestKey(767),
        Ordering::Less
    );
    assert_cmp!(
        numeric_comparator,
        TestKey(3),
        TestKey(2),
        Ordering::Greater
    );
    // Numeric comparator always follows strict numerical order.
    assert_cmp!(
        numeric_comparator,
        TestKey(762),
        TestKey(511),
        Ordering::Greater
    );
    assert_cmp!(
        numeric_comparator,
        TestKey(251),
        TestKey(763),
        Ordering::Less
    );

    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string("Hello how are you"),
        TestVarLengthKey::from_string("Hello how are you"),
        Ordering::Equal,
        varlen
    );
    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string("Hello how are you 2"),
        TestVarLengthKey::from_string("Hello how are you 1"),
        Ordering::Greater,
        varlen
    );

    Ok(())
}

/// Searches on an empty bplustree.
/// The search should not fail but also should not return any results.
#[test]
#[serial]
fn test_search_empty_tree() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let tree = create_test_btree(4096, 1, 4, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);
    let key = TestKey(42);

    let result = tree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;
    assert!(matches!(result, SearchResult::NotFound(_)));

    Ok(())
}

/// Inserts a key on the tree and searches for it.
/// Validates that the retrieved key matches the inserted data.
#[test]
#[serial]
fn test_insert_remove_single_key() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut btree = create_test_btree(4096, 1, 4, comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert a key-value pair
    let key = TestKey(42);
    let bytes = key.as_ref();
    btree.insert(root, bytes)?;

    // Retrieve it back
    let retrieved = btree.search(&start_pos, bytes, FrameAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), bytes);
    btree.clear_worker_stack();
    btree.remove(root, bytes)?;

    let retrieved = btree.search(&start_pos, bytes, FrameAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::NotFound(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_none());

    Ok(())
}

#[test]
#[serial]
fn test_insert_duplicates() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut btree = create_test_btree(4096, 1, 4, comparator)?;
    let root = btree.get_root();
    // Insert a key-value pair
    let key = TestKey(42);
    let bytes = key.as_ref();
    btree.insert(root, bytes)?;
    let result = btree.insert(root, bytes);
    assert!(result.is_err()); // Should fail
    Ok(())
}

#[test]
#[serial]
fn test_update_single_key() -> io::Result<()> {
    let key = TestKey(1);
    let data = b"Original";
    let kv = KeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = KeyValuePair::new(&key, data2);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut btree = create_test_btree(4096, 1, 4, comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);
    btree.insert(root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_worker_stack();

    btree.update(root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv2.as_ref());
    btree.clear_worker_stack();

    Ok(())
}

#[test]
#[serial]
fn test_upsert_single_key() -> io::Result<()> {
    let key = TestKey(1);
    let key2 = TestKey(2);

    let data = b"Original";
    let kv = KeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = KeyValuePair::new(&key, data2);

    let data3 = b"Created";
    let kv3 = KeyValuePair::new(&key2, data3);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut btree = create_test_btree(4096, 1, 4, comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);
    btree.insert(root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_worker_stack();

    btree.upsert(root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv2.as_ref());
    btree.clear_worker_stack();

    btree.upsert(root, kv3.as_ref())?;

    let retrieved = btree.search(&start_pos, key2.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv3.as_ref());
    btree.clear_worker_stack();

    Ok(())
}

#[test]
#[serial] // TO REVIEW.
fn test_variable_length_keys() -> io::Result<()> {
    let comparator = VarlenComparator;
    let mut tree = create_test_btree(4096, 3, 3, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert enough keys to force root to split
    for i in 0..50 {
        let key = TestVarLengthKey::from_string(&format!("Hello_{i}"));
        tree.insert(root, &key.as_bytes())?;
    }
    println!("{}", tree.json()?);

    for i in 0..50 {
        let key = TestVarLengthKey::from_string(&format!("Hello_{i}"));

        let retrieved = tree.search(&start_pos, &key.as_bytes(), FrameAccessMode::Read)?;
        // Search does not release the latch on the node so we have to do it ourselves.
        let cell = tree.get_payload(retrieved)?;
        tree.clear_worker_stack();
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().as_ref(), key.as_bytes());
    }

    println!("{}", tree.json()?);
    Ok(())
}

#[test]
#[serial]
fn test_overflow_chain() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();

    // Overflow chains are very memory wasteful. Therefore we prefer to allocate the tree wih a larger cache size.
    let mut tree = create_test_btree(4096, 100, 3, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);
    let key = TestKey(1);
    let data = gen_ovf_blob(4096, 20, 42);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.as_ref())?;
    let retrieved = tree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = tree.get_payload(retrieved)?;
    assert!(cell.is_some());
    let cell = cell.unwrap();
    assert_eq!(cell.as_ref().len(), kv.as_ref().len());
    assert_eq!(cell.as_ref(), kv.as_ref());
    tree.clear_worker_stack();
    Ok(())
}

#[test]
#[serial]
fn test_multiple_overflow_chain() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    // Overflow chains are very memory wasteful. Therefore we prefer to allocate the tree wih a larger cache size.
    let mut tree = create_test_btree(4096, 100, 3, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert some data first:
    for i in 0..40 {
        let small_data = TestKey(i);
        tree.insert(root, small_data.as_ref())?;
    }

    let key = TestKey(40);
    let data = gen_ovf_blob(4096, 20, 42);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.as_ref())?;
    let retrieved = tree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = tree.get_payload(retrieved)?;
    assert!(cell.is_some());
    let cell = cell.unwrap();
    assert_eq!(cell.as_ref().len(), kv.as_ref().len());
    assert_eq!(cell.as_ref(), kv.as_ref());
    tree.clear_worker_stack();
    Ok(())
}

#[test]
#[serial]
fn test_split_root() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 3, 2, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert enough keys to force root to split
    for i in 0..50 {
        let key = TestKey(i * 10);
        tree.insert(root, key.as_ref())?;
    }
    println!("{}", tree.json()?);

    for i in 0..50 {
        let key = TestKey(i * 10);

        let retrieved = tree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;
        // Search does not release the latch on the node so we have to do it ourselves.
        let cell = tree.get_payload(retrieved)?;
        tree.clear_worker_stack();
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().as_ref(), key.as_ref());
    }

    println!("{}", tree.json()?);
    Ok(())
}

delete_test!(test_delete_1, 4096, 100, 5, 100, [10..20, 30..40, 60..70]);

delete_test!(
    test_delete_2,
    8192,
    200,
    10,
    200,
    [5..10, 50..60, 100..110, 150..160, 190..200]
);

insert_tests! {
    test_insert_sequential => {
        page_size: 4096,
        capacity: 10,
        num_inserts: 100,
        random: false
    },
    test_insert_random_small => {
        page_size: 4096,
        capacity: 10,
        num_inserts: 100,
        random: true
    },
    test_insert_random_medium => {
        page_size: 4096,
        capacity: 100,
        num_inserts: 20000,
        random: true
    },
    test_insert_random_large => {
        page_size: 8192,
        capacity: 100,
        num_inserts: 50000,
        random: true
    },
}

// REVISAR!
#[test]
#[serial]
fn test_overflow_keys() -> io::Result<()> {
    let comparator = VarlenComparator;
    let mut tree = create_test_btree(4096, 30, 4, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    let large_key_size = 5000;

    for i in 0..10 {
        let key_content = format!("KEY_{i:04}_") + &"X".repeat(large_key_size - 10);
        let key = TestVarLengthKey::from_string(&key_content);

        tree.insert(root, &key.as_bytes())?;
    }

    for i in 0..10 {
        let key_content = format!("KEY_{i:04}_") + &"X".repeat(large_key_size - 10);
        let key = TestVarLengthKey::from_string(&key_content);

        let retrieved = tree.search(&start_pos, &key.as_bytes(), FrameAccessMode::Read)?;
        let cell = tree.get_payload(retrieved)?;
        tree.clear_worker_stack();

        assert!(cell.is_some());
        assert_eq!(cell.unwrap().as_ref(), key.as_bytes());
    }

    let huge_key = TestVarLengthKey::from_string(&("HUGE_KEY_".to_string() + &"Y".repeat(8000)));
    let huge_value = gen_ovf_blob(4096, 5, 99);

    let mut combined_data = huge_key.as_bytes();
    combined_data.extend_from_slice(&huge_value);

    tree.insert(root, &combined_data)?;

    let retrieved = tree.search(&start_pos, &huge_key.as_bytes(), FrameAccessMode::Read)?;
    let cell = tree.get_payload(retrieved)?;
    tree.clear_worker_stack();

    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), &combined_data);

    Ok(())
}

#[test]
#[serial]
fn test_btree_iterator_iter_adv() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 10, 4, comparator)?;
    let root = tree.get_root();

    // Insert test data
    let test_data: Vec<(TestKey, Vec<u8>)> = (0..1000)
        .map(|i| {
            let key = TestKey(i);
            let value = format!("value_{i}").into_bytes();
            (key, value)
        })
        .collect();

    // Insert key value pairs
    for (key, value) in &test_data {
        let kv = KeyValuePair::new(key, value);
        tree.insert(root, kv.as_ref())?;
    }

    let mut collected = Vec::new();

    for item in tree.iter()? {
        match item {
            Ok(payload) => {
                collected.push(payload);
            }
            Err(e) => return Err(e),
        }
    }

    assert_eq!(
        collected.len(),
        1000,
        "Should iterate over all 100 elements"
    );

    // Verify keys are sorted
    for pair in collected.windows(2) {
        let key_i = i32::from_ne_bytes(pair[0].as_ref()[..4].try_into().unwrap());
        let key_next = i32::from_ne_bytes(pair[1].as_ref()[..4].try_into().unwrap());

        assert!(
            key_i < key_next,
            "Keys should be in ascending order: {:?} !< {:?}",
            key_i,
            key_next
        );
    }

    Ok(())
}

#[test]
#[serial]
fn test_btree_iterator_iter_rev() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();
    let mut collected = Vec::new();

    let test_data: Vec<(TestKey, Vec<u8>)> = (0..10000)
        .map(|i| {
            let key = TestKey(i);
            let value = format!("value_{i}").into_bytes();
            (key, value)
        })
        .collect();

    for (key, value) in &test_data {
        let kv = KeyValuePair::new(key, value);

        tree.insert(root, kv.as_ref())?;
    }

    for item in tree.iter_rev()? {
        match item {
            Ok(payload) => {
                collected.push(payload);
            }
            Err(e) => return Err(e),
        }
    }

    assert_eq!(collected.len(), 10000);

    // Verify keys are sorted
    for pair in collected.windows(2) {
        let key_i = i32::from_ne_bytes(pair[0].as_ref()[..4].try_into().unwrap());
        let key_next = i32::from_ne_bytes(pair[1].as_ref()[..4].try_into().unwrap());

        assert!(
            key_i > key_next,
            "Keys should be in descending order: {:?} !< {:?}",
            key_i,
            key_next
        );
    }

    Ok(())
}
