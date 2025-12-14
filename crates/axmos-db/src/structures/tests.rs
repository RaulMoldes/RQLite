use crate::{
    assert_cmp, assert_range, delete_test, insert_tests,
    io::frames::{FrameAccessMode, Position},
    structures::{
        bplustree::{IterDirection, SearchResult},
        builder::IntoCell,
        comparator::{
            Comparator, CompositeComparator, DynComparator, FixedSizeBytesComparator,
            NumericComparator, Ranger, SignedNumericComparator, VarlenComparator, subtract_bytes,
        },
    },
    test_utils::{KeyValuePair, TestKey, TestVarLengthKey, gen_ovf_blob, test_btree},
    types::Blob,
    types::VarInt,
};

use rand::SeedableRng;

use serial_test::serial;
use std::{cmp::Ordering, io};

#[test]
fn test_comparators() -> io::Result<()> {
    let fixed_comparator = FixedSizeBytesComparator::with_type::<TestKey>();

    // Basic equality
    assert_cmp!(
        fixed_comparator,
        TestKey::new(1),
        TestKey::new(1),
        Ordering::Equal
    );
    assert_cmp!(
        fixed_comparator,
        TestKey::new(0),
        TestKey::new(0),
        Ordering::Equal
    );
    assert_cmp!(
        fixed_comparator,
        TestKey::new(-1),
        TestKey::new(-1),
        Ordering::Equal
    );

    // Lexicographic ordering (byte-by-byte, may differ from numeric)
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

    // Raw byte comparisons
    let fixed_4 = FixedSizeBytesComparator::for_size(4);
    assert_cmp!(
        fixed_4,
        &[0u8, 1, 0, 0],
        &[1u8, 0, 0, 0],
        Ordering::Less,
        raw
    );
    assert_cmp!(
        fixed_4,
        &[1u8, 0, 0, 0],
        &[0u8, 1, 0, 0],
        Ordering::Greater,
        raw
    );
    assert_cmp!(
        fixed_4,
        &[1u8, 2, 3, 4],
        &[1u8, 2, 3, 4],
        Ordering::Equal,
        raw
    );

    // Range bytes for fixed size
    let fixed_2 = FixedSizeBytesComparator::for_size(2);
    assert_range!(
        fixed_2,
        &[0x01u8, 0x00],
        &[0x00u8, 0xFF],
        &[0x00, 0x01],
        raw
    );

    let numeric_comparator = NumericComparator::with_type::<TestKey>();

    // Basic equality
    assert_cmp!(
        numeric_comparator,
        TestKey::new(1),
        TestKey::new(1),
        Ordering::Equal
    );
    assert_cmp!(
        numeric_comparator,
        TestKey::new(0),
        TestKey::new(0),
        Ordering::Equal
    );

    // Strict numerical ordering
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

    // u64 numeric comparisons
    let numeric_u64 = NumericComparator::with_type::<u64>();
    let a_bytes = 100u64.to_ne_bytes();
    let b_bytes = 50u64.to_ne_bytes();
    assert_cmp!(numeric_u64, &a_bytes, &b_bytes, Ordering::Greater, raw);
    assert_cmp!(numeric_u64, &b_bytes, &a_bytes, Ordering::Less, raw);
    assert_cmp!(numeric_u64, &a_bytes, &a_bytes, Ordering::Equal, raw);

    // Range bytes for numeric
    let diff = numeric_u64.range_bytes(&a_bytes, &b_bytes)?;
    let diff_value = numeric_u64.read_native_u64(&diff);
    assert_eq!(diff_value, 50, "100 - 50 should be 50");

    let signed_comparator = SignedNumericComparator::with_type::<i64>();

    let neg10 = (-10i64).to_ne_bytes();
    let pos10 = 10i64.to_ne_bytes();
    let neg20 = (-20i64).to_ne_bytes();
    let zero = 0i64.to_ne_bytes();

    assert_cmp!(signed_comparator, &neg10, &pos10, Ordering::Less, raw);
    assert_cmp!(signed_comparator, &neg10, &neg20, Ordering::Greater, raw);
    assert_cmp!(signed_comparator, &neg20, &neg10, Ordering::Less, raw);
    assert_cmp!(signed_comparator, &zero, &pos10, Ordering::Less, raw);
    assert_cmp!(signed_comparator, &zero, &neg10, Ordering::Greater, raw);
    assert_cmp!(signed_comparator, &pos10, &pos10, Ordering::Equal, raw);

    // i32 signed comparisons
    let signed_i32 = SignedNumericComparator::with_type::<i32>();
    let i32_neg = (-100i32).to_ne_bytes();
    let i32_pos = 100i32.to_ne_bytes();
    assert_cmp!(signed_i32, &i32_neg, &i32_pos, Ordering::Less, raw);
    assert_cmp!(signed_i32, &i32_pos, &i32_neg, Ordering::Greater, raw);

    let varlen_comparator = VarlenComparator;

    // String comparisons via TestVarLengthKey
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
    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string("apple"),
        TestVarLengthKey::from_string("banana"),
        Ordering::Less,
        varlen
    );
    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string("zebra"),
        TestVarLengthKey::from_string("apple"),
        Ordering::Greater,
        varlen
    );
    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string(""),
        TestVarLengthKey::from_string(""),
        Ordering::Equal,
        varlen
    );
    assert_cmp!(
        varlen_comparator,
        TestVarLengthKey::from_string("a"),
        TestVarLengthKey::from_string("aa"),
        Ordering::Less,
        varlen
    );

    // Blob comparisons
    let blob_apple = Blob::from("apple");
    let blob_apple_bytes: &[u8] = blob_apple.as_ref();
    let blob_banana = Blob::from("banana");
    let blob_banana_bytes: &[u8] = blob_banana.as_ref();
    let blob_hello = Blob::from("hello");
    let blob_hello_bytes: &[u8] = blob_hello.as_ref();

    assert_cmp!(
        varlen_comparator,
        blob_apple_bytes,
        blob_banana_bytes,
        Ordering::Less,
        raw
    );
    assert_cmp!(
        varlen_comparator,
        blob_hello_bytes,
        blob_hello_bytes,
        Ordering::Equal,
        raw
    );

    // Range bytes for varlen (equal keys = empty range)
    let range = varlen_comparator.range_bytes(blob_hello.as_ref(), blob_hello.as_ref())?;
    assert!(
        range.is_empty(),
        "Equal varlen keys should have empty range"
    );

    let dyn_numeric = DynComparator::StrictNumeric(NumericComparator::for_size(8));
    let dyn_lexical = DynComparator::FixedSizeBytes(FixedSizeBytesComparator::for_size(8));
    let dyn_signed = DynComparator::SignedNumeric(SignedNumericComparator::for_size(8));
    let dyn_varlen = DynComparator::Variable(VarlenComparator);

    let val_256 = 256u64.to_ne_bytes();
    let val_255 = 255u64.to_ne_bytes();

    // Numeric: 256 > 255
    assert_cmp!(dyn_numeric, &val_256, &val_255, Ordering::Greater, raw);

    // Signed via DynComparator
    let signed_neg = (-50i64).to_ne_bytes();
    let signed_pos = 50i64.to_ne_bytes();
    assert_cmp!(dyn_signed, &signed_neg, &signed_pos, Ordering::Less, raw);

    // Variable via DynComparator
    assert_cmp!(
        dyn_varlen,
        blob_apple_bytes,
        blob_banana_bytes,
        Ordering::Less,
        raw
    );

    // Two integers (i32, i32)
    let composite_2i32 = CompositeComparator::new(vec![
        DynComparator::SignedNumeric(SignedNumericComparator::for_size(4)),
        DynComparator::SignedNumeric(SignedNumericComparator::for_size(4)),
    ]);

    let mut key_1_2 = Vec::new();
    key_1_2.extend_from_slice(&1i32.to_ne_bytes());
    key_1_2.extend_from_slice(&2i32.to_ne_bytes());

    let mut key_1_3 = Vec::new();
    key_1_3.extend_from_slice(&1i32.to_ne_bytes());
    key_1_3.extend_from_slice(&3i32.to_ne_bytes());

    let mut key_2_1 = Vec::new();
    key_2_1.extend_from_slice(&2i32.to_ne_bytes());
    key_2_1.extend_from_slice(&1i32.to_ne_bytes());

    let mut key_1_100 = Vec::new();
    key_1_100.extend_from_slice(&1i32.to_ne_bytes());
    key_1_100.extend_from_slice(&100i32.to_ne_bytes());

    // (1, 2) < (1, 3) - first equal, second differs
    assert_cmp!(composite_2i32, &key_1_2, &key_1_3, Ordering::Less, raw);
    // (2, 1) > (1, 100) - first column dominates
    assert_cmp!(composite_2i32, &key_2_1, &key_1_100, Ordering::Greater, raw);
    // Equal keys
    assert_cmp!(composite_2i32, &key_1_2, &key_1_2, Ordering::Equal, raw);

    // Integer + Text (i64, Text)
    let composite_i64_text = CompositeComparator::new(vec![
        DynComparator::SignedNumeric(SignedNumericComparator::for_size(8)),
        DynComparator::Variable(VarlenComparator),
    ]);

    let mut key_1_apple = Vec::new();
    key_1_apple.extend_from_slice(&1i64.to_ne_bytes());
    key_1_apple.extend_from_slice(blob_apple.as_ref());

    let mut key_1_banana = Vec::new();
    key_1_banana.extend_from_slice(&1i64.to_ne_bytes());
    key_1_banana.extend_from_slice(blob_banana.as_ref());

    let blob_zebra = Blob::from("zebra");
    let mut key_2_apple = Vec::new();
    key_2_apple.extend_from_slice(&2i64.to_ne_bytes());
    key_2_apple.extend_from_slice(blob_apple.as_ref());

    let mut key_1_zebra = Vec::new();
    key_1_zebra.extend_from_slice(&1i64.to_ne_bytes());
    key_1_zebra.extend_from_slice(blob_zebra.as_ref());

    // (1, "apple") < (1, "banana")
    assert_cmp!(
        composite_i64_text,
        &key_1_apple,
        &key_1_banana,
        Ordering::Less,
        raw
    );
    // (2, "apple") > (1, "zebra") - first column dominates
    assert_cmp!(
        composite_i64_text,
        &key_2_apple,
        &key_1_zebra,
        Ordering::Greater,
        raw
    );

    // Key size calculation
    let composite_i32_text = CompositeComparator::new(vec![
        DynComparator::SignedNumeric(SignedNumericComparator::for_size(4)),
        DynComparator::Variable(VarlenComparator),
    ]);

    let blob_hello_for_size = Blob::from("hello");
    let mut key_for_size = Vec::new();
    key_for_size.extend_from_slice(&42i32.to_ne_bytes());
    key_for_size.extend_from_slice(blob_hello_for_size.as_ref());

    let size = composite_i32_text.key_size(&key_for_size)?;
    // 4 bytes for i32 + varint(5) + 5 bytes for "hello" = 4 + 1 + 5 = 10
    assert_eq!(size, 10, "Composite key size should be 10");

    let sub_result = subtract_bytes(&[5], &[3])?;
    let (len, offset) = VarInt::from_encoded_bytes(&sub_result)?;
    let len_usize: usize = len.try_into()?;
    assert_eq!(len_usize, 1);
    assert_eq!(sub_result[offset], 2);

    // With borrow: 0x0100 - 0x00FF = 0x0001
    let sub_borrow = subtract_bytes(&[0x01, 0x00], &[0x00, 0xFF])?;
    let (_, borrow_offset) = VarInt::from_encoded_bytes(&sub_borrow)?;
    assert_eq!(&sub_borrow[borrow_offset..], &[0x01]);

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

    let result = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
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

    btree.insert(root, key.clone())?;

    // Retrieve it back
    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), key.as_ref());
    btree.clear_accessor_stack();
    btree.remove(root, key.key_bytes())?;

    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
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
    btree.insert(root, key.clone())?;
    let result = btree.insert(root, key);
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
    btree.insert(root, kv.clone())?;

    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_accessor_stack();

    btree.update(root, kv2.clone())?;

    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

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
    btree.insert(root, kv.clone())?;

    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv.as_ref());
    btree.clear_accessor_stack();

    btree.upsert(root, kv2.clone())?;

    let retrieved = btree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found(_)));
    let cell = btree.get_payload(retrieved)?;
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), kv2.as_ref());
    btree.clear_accessor_stack();

    btree.upsert(root, kv3.clone())?;

    let retrieved = btree.search(&start_pos, key2.key_bytes(), FrameAccessMode::Read)?;

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
        tree.insert(root, key)?;
    }
    println!("{}", tree.json()?);

    for i in 0..50 {
        let key = TestVarLengthKey::from_string(&format!("Hello_{i}"));

        let retrieved = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
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

    tree.insert(root, kv.clone())?;
    let retrieved = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

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
        tree.insert(root, small_data)?;
    }

    let key = TestKey::new(40);
    let data = gen_ovf_blob(4096, 20, 42);
    let kv = KeyValuePair::new(&key, &data);

    tree.insert(root, kv.clone())?;
    let retrieved = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;

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
        tree.insert(root, key)?;
    }

    for i in 0..50 {
        let key = TestKey::new(i * 10);

        let retrieved = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
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

        tree.insert(root, key.clone())?;
    }

    for i in 0..10 {
        let key_content = format!("KEY_{i:04}_") + &"X".repeat(large_key_size - 10);
        let key = TestVarLengthKey::from_string(&key_content);

        let retrieved = tree.search(&start_pos, key.key_bytes(), FrameAccessMode::Read)?;
        let cell = tree.get_payload(retrieved)?;
        tree.clear_accessor_stack();

        assert!(cell.is_some());
        assert_eq!(cell.unwrap().as_ref(), key.as_bytes());
    }

    let huge_key = TestVarLengthKey::from_string(&("HUGE_KEY_".to_string() + &"Y".repeat(8000)));
    let huge_value = gen_ovf_blob(4096, 5, 99);

    let mut combined_data = huge_key.as_bytes().to_vec();
    combined_data.extend_from_slice(&huge_value);

    let varlen = TestVarLengthKey::from_raw_bytes(combined_data);
    tree.insert(root, varlen.clone())?;

    let retrieved = tree.search(&start_pos, huge_key.key_bytes(), FrameAccessMode::Read)?;
    let cell = tree.get_payload(retrieved)?;
    tree.clear_accessor_stack();

    assert!(cell.is_some());
    assert_eq!(cell.unwrap().as_ref(), varlen.as_ref());

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
        tree.insert(root, key)?;
    }

    // Insert keys with overflow data
    for i in 20..30 {
        let key = TestKey::new(i);
        let data = gen_ovf_blob(4096, 5, i as u64);
        let kv = KeyValuePair::new(&key, &data);
        tree.insert(root, kv)?;
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
        tree.insert(root, key)?;
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
        tree.insert(root, key)?;
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
        tree.insert(root, key)?;
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
        tree.insert(root, key)?;
    }

    // Start from key 500
    let start_key = TestKey::new(500);
    let positions: Vec<Position> = tree
        .iter_positions_from(start_key.key_bytes(), IterDirection::Forward)?
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
fn test_iter_positions_from_backward() -> io::Result<()> {
    let comparator = NumericComparator::with_type::<TestKey>();
    let (mut tree, f) = test_btree(comparator)?;
    let root = tree.get_root();

    for i in 0..100 {
        let key = TestKey::new(i * 10);
        tree.insert(root, key)?;
    }

    // Start from key 500 and go backward
    let start_key = TestKey::new(500);
    let positions: Vec<Position> = tree
        .iter_positions_from(start_key.key_bytes(), IterDirection::Backward)?
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
        tree.insert(root, kv)?;
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

    tree.insert(root, kv.clone())?;

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
        tree.insert(root, key)?;
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
        tree.insert(root, key)?;
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
