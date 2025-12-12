use crate::{
    assert_cmp, delete_test, insert_tests,
    io::frames::{FrameAccessMode, Position},
    storage::{
        cell::Slot,
        page::{BtreePage, OverflowPage},
    },
    structures::{
        bplustree::{IterDirection, SearchResult},
        comparator::{Comparator, FixedSizeBytesComparator, NumericComparator, VarlenComparator},
    },
    test_utils::{KeyValuePair, TestKey, TestVarLengthKey, gen_ovf_blob, test_btree},
    types::{PAGE_ZERO, PageId},
};

use rand::SeedableRng;

use serial_test::serial;
use std::{cmp::Ordering, collections::HashSet, io};

#[test]
fn test_comparators() -> std::io::Result<()> {
    let fixed_comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let varlen_comparator = VarlenComparator;
    let numeric_comparator = NumericComparator::with_type::<TestKey>();

    assert_cmp!(
        fixed_comparator,
        TestKey::new(1),
        TestKey::new(1),
        Ordering::Equal
    );
    assert_cmp!(
        fixed_comparator,
        TestKey::new(511),
        TestKey::new(767),
        Ordering::Less
    );
    assert_cmp!(
        fixed_comparator,
        TestKey::new(3),
        TestKey::new(2),
        Ordering::Greater
    );
    // Fixed size comparators compare byte by byte, therefore the ordering might not always match strict numerical order. That is why we have [NumericComparator].
    assert_cmp!(
        fixed_comparator,
        TestKey::new(511),
        TestKey::new(762),
        Ordering::Greater
    );
    assert_cmp!(
        fixed_comparator,
        TestKey::new(251),
        TestKey::new(763),
        Ordering::Less
    );

    assert_cmp!(
        numeric_comparator,
        TestKey::new(1),
        TestKey::new(1),
        Ordering::Equal
    );
    assert_cmp!(
        numeric_comparator,
        TestKey::new(511),
        TestKey::new(767),
        Ordering::Less
    );
    assert_cmp!(
        numeric_comparator,
        TestKey::new(3),
        TestKey::new(2),
        Ordering::Greater
    );
    // Numeric comparator always follows strict numerical order.
    assert_cmp!(
        numeric_comparator,
        TestKey::new(762),
        TestKey::new(511),
        Ordering::Greater
    );
    assert_cmp!(
        numeric_comparator,
        TestKey::new(251),
        TestKey::new(763),
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
    let (tree, f) = test_btree(comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);
    let key = TestKey::new(42);

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
    let (mut btree, f) = test_btree(comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert a key-value pair
    let key = TestKey::new(42);
    let bytes = key.as_ref();
    btree.insert(root, bytes)?;

    // Retrieve it back
    let retrieved = btree.search(&start_pos, bytes, FrameAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), bytes);
    btree.clear_accessor_stack();
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
    let (mut btree, f) = test_btree(comparator)?;
    let root = btree.get_root();
    // Insert a key-value pair
    let key = TestKey::new(42);
    let bytes = key.as_ref();
    btree.insert(root, bytes)?;
    let result = btree.insert(root, bytes);
    assert!(result.is_err()); // Should fail
    Ok(())
}

#[test]
#[serial]
fn test_update_single_key() -> io::Result<()> {
    let key = TestKey::new(1);
    let data = b"Original";
    let kv = KeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = KeyValuePair::new(&key, data2);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let (mut btree, f) = test_btree(comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);
    btree.insert(root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_accessor_stack();

    btree.update(root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv2.as_ref());
    btree.clear_accessor_stack();

    Ok(())
}

#[test]
#[serial]
fn test_upsert_single_key() -> io::Result<()> {
    let key = TestKey::new(1);
    let key2 = TestKey::new(2);

    let data = b"Original";
    let kv = KeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = KeyValuePair::new(&key, data2);

    let data3 = b"Created";
    let kv3 = KeyValuePair::new(&key2, data3);
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let (mut btree, f) = test_btree(comparator)?;

    let root = btree.get_root();
    let start_pos = Position::start_pos(root);
    btree.insert(root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_accessor_stack();

    btree.upsert(root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv2.as_ref());
    btree.clear_accessor_stack();

    btree.upsert(root, kv3.as_ref())?;

    let retrieved = btree.search(&start_pos, key2.as_ref(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv3.as_ref());
    btree.clear_accessor_stack();

    Ok(())
}

#[test]
#[serial] // TO REVIEW.
fn test_variable_length_keys() -> io::Result<()> {
    let comparator = VarlenComparator;
    let (mut tree, f) = test_btree(comparator)?;
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
        tree.clear_accessor_stack();
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);
    let key = TestKey::new(1);
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
    tree.clear_accessor_stack();
    Ok(())
}

#[test]
#[serial]
fn test_multiple_overflow_chain() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    // Overflow chains are very memory wasteful. Therefore we prefer to allocate the tree wih a larger cache size.
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert some data first:
    for i in 0..40 {
        let small_data = TestKey::new(i);
        tree.insert(root, small_data.as_ref())?;
    }

    let key = TestKey::new(40);
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
    tree.clear_accessor_stack();
    Ok(())
}

#[test]
#[serial]
fn test_split_root() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();
    let start_pos = Position::start_pos(root);

    // Insert enough keys to force root to split
    for i in 0..50 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key.as_ref())?;
    }
    // println!("{}", tree.json()?);

    for i in 0..50 {
        let key = TestKey::new(i * 10);

        let retrieved = tree.search(&start_pos, key.as_ref(), FrameAccessMode::Read)?;
        // Search does not release the latch on the node so we have to do it ourselves.
        let cell = tree.get_payload(retrieved)?;
        tree.clear_accessor_stack();
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().as_ref(), key.as_ref());
    }

    println!("{}", tree.json()?);
    Ok(())
}

delete_test!(
    test_delete_2 => {
        num_keys: 200,
        sequences: [5..10, 50..60, 100..110, 150..160, 190..200]
    },

    test_delete_1 => {
        num_keys: 100,
        sequences: [10..20, 30..40, 60..70]
    },
);

insert_tests! {
    test_insert_sequential => {

        num_inserts: 100,
        random: false
    },
    test_insert_random_small => {

        num_inserts: 100,
        random: true
    },
    test_insert_random_medium => {

        num_inserts: 20000,
        random: true
    },
    test_insert_random_large => {

        num_inserts: 50000,
        random: true
    },
}

#[test]
#[serial]
fn test_overflow_keys() -> io::Result<()> {
    let comparator = VarlenComparator;
    let (mut tree, f) = test_btree(comparator)?;
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
        tree.clear_accessor_stack();

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
    tree.clear_accessor_stack();

    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), &combined_data);

    Ok(())
}

#[test]
#[serial] // TEST USED TO GENERATE AN INFINITE LOOP. MUST REVIEW TREE INSERTIONS WITH OVERFLOW PAGES
fn test_dealloc_bfs_with_overflow_chains() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    // Insert some regular keys
    for i in 0..20 {
        let key = TestKey::new(i);
        tree.insert(root, key.as_ref())?;
    }

    // Insert keys with overflow data
    for i in 20..30 {
        let key = TestKey::new(i);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    // Insert enough to create a deep tree
    for i in 0..10000 {
        let key = TestKey::new(i);
        tree.insert(root, key.as_ref())?;
    }

    tree.dealloc()?;

    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_multiple_pages() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..1000 {
        let key = TestKey::new(i);
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
    let (mut tree, f) = test_btree(comparator)?;

    let positions: Vec<Position> = tree.iter_positions_rev()?.filter_map(|r| r.ok()).collect();

    assert_eq!(positions.len(), 0, "Empty tree should yield no positions");

    Ok(())
}

#[test]
#[serial]
fn test_iter_positions_rev_multiple_pages() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..1000 {
        let key = TestKey::new(i);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i * 10); // Keys: 0, 10, 20, ..., 990
        tree.insert(root, key.as_ref())?;
    }

    // Start from key 500
    let start_key = TestKey::new(500);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    // Insert a single overflow cell
    let key = TestKey::new(1);
    let data = gen_ovf_blob(4096, 5, 42);
    let kv = KeyValuePair::new(&key, &data);

    println!("Inserting overflow cell, total size: {}", kv.as_ref().len());

    tree.insert(root, kv.as_ref())?;

    // Now manually inspect the overflow chain
    tree.accessor_mut()
        .acquire::<BtreePage>(root, FrameAccessMode::Read)?;

    let (is_overflow, overflow_start) =
        tree.accessor()
            .read_page::<BtreePage, _, _>(root, |btree| {
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

    tree.accessor_mut().release_latch(root);

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

        tree.accessor_mut()
            .acquire::<OverflowPage>(current, FrameAccessMode::Read)?;

        let (next, num_bytes) =
            tree.accessor()
                .read_page::<OverflowPage, _, _>(current, |ovf| {
                    println!("    num_bytes: {}", ovf.metadata().num_bytes);
                    println!("    next: {}", ovf.metadata().next);
                    (ovf.metadata().next, ovf.metadata().num_bytes)
                })?;

        tree.accessor_mut().release_latch(current);

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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key.as_ref())?;
    }

    // Start from key 500 and go backward
    let start_key = TestKey::new(500);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    let test_data: Vec<(TestKey, Vec<u8>)> = (0..50)
        .map(|i| {
            let key = TestKey::new(i);
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
#[serial]
fn test_with_cell_at_overflow() -> io::Result<()> {
    let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    let key = TestKey::new(42);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i);
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
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i);
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
