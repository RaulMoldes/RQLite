mod macros;

use crate::{
    AxmosDBConfig, IncrementalVaccum, TextEncoding, assert_cmp, delete_test, insert_tests,
    io::{
        frames::{FrameAccessMode, Position},
        pager::{Pager, SharedPager},
    },
    storage::{
        cell::Slot,
        page::{BtreePage, OverflowPage},
    },
    structures::{
        bplustree::{
            BPlusTree, Comparator, FixedSizeBytesComparator, IterDirection, NumericComparator,
            SearchResult, VarlenComparator,
        },
        kvp::KeyValuePair,
    },
    transactions::worker::Worker,
    types::{PAGE_ZERO, PageId, VarInt},
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serial_test::serial;
use std::collections::{HashSet, VecDeque};
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
    let mut tree = create_test_btree(4096, 3, 3, comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert enough keys to force root to split
    for i in 0..50 {
        let key = TestKey(i * 10);
        tree.insert(root, key.as_ref())?;
    }
   // println!("{}", tree.json()?);

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
#[serial] // TEST USED TO GENERATE AN INFINITE LOOP. MUST REVIEW TREE INSERTIONS WITH OVERFLOW PAGES
fn test_dealloc_bfs_with_overflow_chains() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();

    // Insert some regular keys
    for i in 0..20 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    // Insert keys with overflow data
    for i in 20..30 {
        let key = TestKey(i);
        let data = gen_ovf_blob(4096, 5, i as u64);
        let kv = KeyValuePair::new(&key, &data);
        tree.insert(root, kv.as_ref())?;
    }

    tree.dealloc()?;

    Ok(())
}

#[test]
#[serial]
fn test_dealloc_bfs_large_tree() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();

    // Insert enough to create a deep tree
    for i in 0..10000 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    tree.dealloc()?;

    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_multiple_pages() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..1000 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    let positions: Vec<Position> = tree.iter_positions()?.filter_map(|r| r.ok()).collect();

    assert_eq!(positions.len(), 1000, "Should have 1000 positions");

    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_rev_empty_tree() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 10, 4, comparator)?;

    let positions: Vec<Position> = tree.iter_positions_rev()?.filter_map(|r| r.ok()).collect();

    assert_eq!(positions.len(), 0, "Empty tree should yield no positions");

    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_rev_multiple_pages() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..1000 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    let positions: Vec<Position> = tree.iter_positions_rev()?.filter_map(|r| r.ok()).collect();

    assert_eq!(positions.len(), 1000, "Should have 1000 positions");
    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_from_middle() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey(i * 10); // Keys: 0, 10, 20, ..., 990
        tree.insert(root, key.as_ref())?;
    }

    // Start from key 500
    let start_key = TestKey(500);
    let positions: Vec<Position> = tree
        .iter_positions_from(start_key.as_ref(), IterDirection::Forward)?
        .filter_map(|r| r.ok())
        .collect();

    // Should get keys from 500 onwards (500, 510, 520, ..., 990) = 50 keys
    assert_eq!(
        positions.len(),
        50,
        "Should have 50 positions from 500 onwards"
    );

    Ok(())
}
#[test]
#[serial]
fn test_overflow_chain_integrity() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();

    // Insert a single overflow cell
    let key = TestKey(1);
    let data = gen_ovf_blob(4096, 5, 42);
    let kv = KeyValuePair::new(&key, &data);

    println!("Inserting overflow cell, total size: {}", kv.as_ref().len());

    tree.insert(root, kv.as_ref())?;

    // Now manually inspect the overflow chain
    tree.worker_mut()
        .acquire::<BtreePage>(root, FrameAccessMode::Read)?;

    let (is_overflow, overflow_start) =
        tree.worker().read_page::<BtreePage, _, _>(root, |btree| {
            let cell = btree.cell(Slot(0));
            println!("Cell metadata: {:?}", cell.metadata());
            println!("Cell len: {}", cell.len());
            println!("Cell is_overflow: {}", cell.metadata().is_overflow());

            if cell.metadata().is_overflow() {
                let ovf_page = cell.overflow_page();
                println!("Overflow page from cell: {}", ovf_page);
                (true, ovf_page)
            } else {
                (false, PAGE_ZERO)
            }
        })?;

    tree.worker_mut().release_latch(root);

    assert!(is_overflow, "Cell should be overflow");
    assert!(overflow_start.is_valid(), "Overflow page should be valid");

    // Now traverse the overflow chain and verify it terminates
    let mut current = overflow_start;
    let mut chain_length = 0;
    let mut visited: HashSet<PageId> = HashSet::new();

    println!("\nTraversing overflow chain:");
    while current.is_valid() {
        if visited.contains(&current) {
            panic!("Cycle detected in overflow chain at page {}", current);
        }
        visited.insert(current);
        chain_length += 1;

        println!("  Overflow page {}: {}", chain_length, current);

        tree.worker_mut()
            .acquire::<OverflowPage>(current, FrameAccessMode::Read)?;

        let (next, num_bytes) = tree
            .worker()
            .read_page::<OverflowPage, _, _>(current, |ovf| {
                println!("    num_bytes: {}", ovf.metadata().num_bytes);
                println!("    next: {}", ovf.metadata().next);
                (ovf.metadata().next, ovf.metadata().num_bytes)
            })?;

        tree.worker_mut().release_latch(current);

        // Sanity check: num_bytes should be reasonable
        assert!(
            num_bytes > 0 && num_bytes <= 4096,
            "Suspicious num_bytes: {} at page {}",
            num_bytes,
            current
        );

        current = next;

        // Safety limit
        if chain_length > 100 {
            panic!("Overflow chain too long, likely infinite loop");
        }
    }

    println!("\nOverflow chain length: {}", chain_length);
    println!("Visited pages: {:?}", visited);

    Ok(())
}
#[test]
#[serial]
fn test_iter_positions_from_backward() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey(i * 10);
        tree.insert(root, key.as_ref())?;
    }

    // Start from key 500 and go backward
    let start_key = TestKey(500);
    let positions: Vec<Position> = tree
        .iter_positions_from(start_key.as_ref(), IterDirection::Backward)?
        .filter_map(|r| r.ok())
        .collect();

    // Should get keys from 500 backwards (500, 490, 480, ..., 0) = 51 keys
    assert_eq!(
        positions.len(),
        51,
        "Should have 51 positions from 500 backwards"
    );

    Ok(())
}

#[test]
#[serial]
fn test_with_cell_at() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 10, 4, comparator)?;
    let root = tree.get_root();

    let test_data: Vec<(TestKey, Vec<u8>)> = (0..50)
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

    // Collect positions
    let positions: Vec<Position> = tree.iter_positions()?.filter_map(|r| r.ok()).collect();

    // Access each cell via with_cell_at
    for (i, pos) in positions.iter().enumerate() {
        let key_value = tree.with_cell_at(*pos, |data| {
            let key = i32::from_ne_bytes(data[..4].try_into().unwrap());
            key
        })?;

        assert_eq!(key_value, i as i32, "Key should match index");
    }

    Ok(())
}

#[test]
#[serial] // REVIEW THIS TEST. FAILS WITH RefCell already borrowed.
fn test_with_cell_at_overflow() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();

    let key = TestKey(42);
    let data = gen_ovf_blob(4096, 10, 123);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.as_ref())?;

    let positions: Vec<Position> = tree.iter_positions()?.filter_map(|r| r.ok()).collect();

    assert_eq!(positions.len(), 1);

    let cell_len = tree.with_cell_at(positions[0], |data| data.len())?;

    assert_eq!(
        cell_len,
        kv.as_ref().len(),
        "Overflow cell should be fully reassembled"
    );

    Ok(())
}

#[test]
#[serial]
fn test_with_cells_at_batch() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    let positions: Vec<Position> = tree.iter_positions()?.filter_map(|r| r.ok()).collect();

    // Batch access all cells
    let keys: Vec<i32> = tree.with_cells_at(&positions, |_pos, data| {
        i32::from_ne_bytes(data[..4].try_into().unwrap())
    })?;

    assert_eq!(keys.len(), 100);

    // Verify order is preserved
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(*key, i as i32, "Keys should be in original order");
    }

    Ok(())
}

#[test]
#[serial]
fn test_with_cells_at_partial() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 50, 4, comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    let all_positions: Vec<Position> = tree.iter_positions()?.filter_map(|r| r.ok()).collect();

    // Only access every 10th position
    let selected: Vec<Position> = all_positions
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 10 == 0)
        .map(|(_, p)| *p)
        .collect();

    let keys: Vec<i32> = tree.with_cells_at(&selected, |_pos, data| {
        i32::from_ne_bytes(data[..4].try_into().unwrap())
    })?;

    assert_eq!(keys.len(), 10);
    assert_eq!(keys, vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);

    Ok(())
}

#[test]
#[serial]
fn test_dealloc_with_overflow() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let mut tree = create_test_btree(4096, 100, 4, comparator)?;
    let root = tree.get_root();

    // Insert regular keys first (this will allocate btree pages)
    println!("Inserting regular keys...");
    for i in 0..20 {
        let key = TestKey(i);
        tree.insert(root, key.as_ref())?;
    }

    // Collect all btree pages
    let mut btree_pages: HashSet<PageId> = HashSet::new();
    let mut queue: VecDeque<PageId> = VecDeque::new();
    queue.push_back(root);

    while let Some(page_id) = queue.pop_front() {
        if !page_id.is_valid() || btree_pages.contains(&page_id) {
            continue;
        }
        btree_pages.insert(page_id);

        tree.worker_mut()
            .acquire::<BtreePage>(page_id, FrameAccessMode::Read)?;

        let children: Vec<PageId> = tree
            .worker()
            .read_page::<BtreePage, _, _>(page_id, |btree| btree.iter_children().collect())?;

        tree.worker_mut().release_latch(page_id);

        for child in children {
            if child.is_valid() {
                queue.push_back(child);
            }
        }
    }

    println!("BTree pages after regular inserts: {:?}", btree_pages);

    // Now insert overflow keys
    println!("\nInserting overflow keys...");
    for i in 20..30 {
        let key = TestKey(i);
        let data = gen_ovf_blob(4096, 5, i as u64);
        let kv = KeyValuePair::new(&key, &data);
        tree.insert(root, kv.as_ref())?;
    }

    // Now scan ALL pages and check what we find
    println!("\nScanning all btree pages for overflow cells...");

    let mut all_btree_pages: HashSet<PageId> = HashSet::new();
    let mut overflow_starts: Vec<(PageId, Slot, PageId)> = Vec::new(); // (btree_page, slot, overflow_start)

    queue.clear();
    queue.push_back(root);

    while let Some(page_id) = queue.pop_front() {
        if !page_id.is_valid() || all_btree_pages.contains(&page_id) {
            continue;
        }
        all_btree_pages.insert(page_id);

        tree.worker_mut()
            .acquire::<BtreePage>(page_id, FrameAccessMode::Read)?;

        let (children, overflows) =
            tree.worker()
                .read_page::<BtreePage, _, _>(page_id, |btree| {
                    let children: Vec<PageId> = btree.iter_children().collect();

                    let overflows: Vec<(Slot, PageId)> = (0..btree.num_slots())
                        .filter_map(|i| {
                            let cell = btree.cell(Slot(i));
                            if cell.metadata().is_overflow() {
                                Some((Slot(i), cell.overflow_page()))
                            } else {
                                None
                            }
                        })
                        .collect();

                    (children, overflows)
                })?;

        tree.worker_mut().release_latch(page_id);

        for (slot, ovf) in overflows {
            println!(
                "  Page {} slot {} -> overflow start {}",
                page_id, slot.0, ovf
            );
            overflow_starts.push((page_id, slot, ovf));
        }

        for child in children {
            if child.is_valid() {
                queue.push_back(child);
            }
        }
    }

    println!("\nAll BTree pages: {:?}", all_btree_pages);
    println!("Overflow starts found: {}", overflow_starts.len());

    // Now verify each overflow chain
    for (btree_page, slot, start) in &overflow_starts {
        println!(
            "\nVerifying overflow chain from btree page {} slot {}, start {}:",
            btree_page, slot.0, start
        );

        // Check if overflow start is actually a btree page (BUG!)
        if all_btree_pages.contains(start) {
            println!("  ERROR: Overflow start {} is a BTree page!", start);

            // Let's see what the cell actually contains
            tree.worker_mut()
                .acquire::<BtreePage>(*btree_page, FrameAccessMode::Read)?;

            tree.worker()
                .read_page::<BtreePage, _, _>(*btree_page, |btree| {
                    let cell = btree.cell(*slot);
                    println!("  Cell metadata: {:?}", cell.metadata());
                    println!(
                        "  Cell used bytes: {:?}",
                        &cell.used()[..std::cmp::min(32, cell.used().len())]
                    );
                    println!("  Cell len: {}", cell.len());
                })?;

            tree.worker_mut().release_latch(*btree_page);

            panic!("Overflow page {} is actually a BTree page!", start);
        }

        let mut current = *start;
        let mut visited: HashSet<PageId> = HashSet::new();
        let mut chain_len = 0;

        while current.is_valid() {
            if visited.contains(&current) {
                panic!("Cycle in overflow chain at {}", current);
            }
            if all_btree_pages.contains(&current) {
                panic!(
                    "Overflow chain entered btree page {} after {} hops",
                    current, chain_len
                );
            }
            visited.insert(current);
            chain_len += 1;

            tree.worker_mut()
                .acquire::<OverflowPage>(current, FrameAccessMode::Read)?;

            let next = tree
                .worker()
                .read_page::<OverflowPage, _, _>(current, |ovf| ovf.metadata().next)?;

            tree.worker_mut().release_latch(current);
            current = next;

            if chain_len > 50 {
                panic!("Chain too long");
            }
        }

        println!("  Chain OK, length: {}", chain_len);
    }

    println!("\nAll overflow chains verified OK");

    // Now test dealloc
    println!("\nCalling dealloc...");
    tree.dealloc()?;

    Ok(())
}
