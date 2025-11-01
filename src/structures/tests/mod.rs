mod macros;

use crate::io::pager::{Pager, SharedPager};
use crate::storage::cell::Slot;
use crate::structures::bplustree::{BPlusTree, NodeAccessMode, SearchResult};
use crate::{
    delete_test, insert_tests, IncrementalVaccum, RQLiteConfig, ReadWriteVersion, TextEncoding,
};
use serial_test::serial;
use std::io;
use tempfile::tempdir;

// Test key type
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
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

impl std::fmt::Display for TestKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestKey: {}", self.0)
    }
}

impl AsMut<[u8]> for TestKey {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                &mut self.0 as *mut i32 as *mut u8,
                std::mem::size_of::<i32>(),
            )
        }
    }
}

impl TryFrom<&[u8]> for TestKey {
    type Error = std::io::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid length for TestKey: expected 4 bytes",
            ));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        Ok(TestKey(i32::from_ne_bytes(arr)))
    }
}

#[derive(Debug, Clone)]
struct TestKeyValuePair {
    data: Vec<u8>,
}

impl TestKeyValuePair {
    fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: &K, value: &V) -> Self {
        let mut data = Vec::with_capacity(key.as_ref().len() + value.as_ref().len());
        data.extend_from_slice(key.as_ref());
        data.extend_from_slice(value.as_ref());
        Self { data }
    }

    fn as_ref(&self) -> &[u8] {
        &self.data
    }

    fn key(&self) -> &[u8] {
        &self.data[..4]
    }

    fn value(&self) -> &[u8] {
        &self.data[4..]
    }
}

impl AsRef<[u8]> for TestKeyValuePair {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl TryFrom<&[u8]> for TestKeyValuePair {
    type Error = std::io::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid data length: expected at least 4 bytes, got {}",
                    bytes.len()
                ),
            ));
        }

        Ok(Self {
            data: bytes.to_vec(),
        })
    }
}

fn create_test_btree(
    page_size: u32,
    capacity: usize,
    min_keys: usize,
) -> io::Result<BPlusTree<TestKey>> {
    let dir = tempdir()?;
    let path = dir.path().join("test_btree.db");

    let config = RQLiteConfig {
        page_size,
        cache_size: Some(capacity as u16),
        incremental_vacuum_mode: IncrementalVaccum::Disabled,
        read_write_version: ReadWriteVersion::Legacy,
        text_encoding: TextEncoding::Utf8,
    };

    let pager = Pager::from_config(config, &path)?;
    Ok(BPlusTree::new(SharedPager::from(pager), min_keys, 2))
}

#[test]
fn validate_test_key() {
    let key = TestKey(9);
    let bytes = key.as_ref();
    let key_result = TestKey::try_from(bytes);
    assert!(key_result.is_ok());
    assert_eq!(
        key_result.unwrap(),
        key,
        "Deserialization failed without padding"
    );

    let mut padded = Vec::from(bytes);
    padded.extend_from_slice(&[0x00, 0xFF, 0xAA]); // Extra bytes

    // Validate that with padding we can compare the results effectibvely too.
    let padded_result = TestKey::try_from(padded.as_ref());
    assert!(
        padded_result.is_ok(),
        "Deserialization failed when adding padding"
    );
    assert_eq!(padded_result.unwrap(), key);
}

/// Searches on an empty bplustree.
/// The search should not fail but also should not return any results.
#[test]
#[serial]
fn test_search_empty_tree() -> io::Result<()> {
    let mut tree = create_test_btree(4096, 1, 2)?;
    let root = tree.get_root();
    let start_pos = (root, Slot(0));
    let key = TestKey(42);

    let result = tree.search(&start_pos, &key, NodeAccessMode::Read)?;
    assert!(matches!(result, SearchResult::NotFound(_)));

    Ok(())
}

/// Inserts a key on the tree and searches for it.
/// Validates that the retrieved key matches the inserted data.
#[test]
#[serial]
fn test_insert_remove_single_key() -> io::Result<()> {
    let mut btree = create_test_btree(4096, 1, 2)?;

    let root = btree.get_root();
    let start_pos = (root, Slot(0));
    // Insert a key-value pair
    let key = TestKey(42);

    btree.insert(&root, key.as_ref())?;

    // Retrieve it back
    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), key.as_ref());
    btree.clear_stack();
    btree.remove(&root, &key)?;

    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;
    assert!(matches!(retrieved, SearchResult::NotFound(_)));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_none());

    Ok(())
}

#[test]
#[serial]
fn test_insert_duplicates() -> io::Result<()> {
    let mut btree = create_test_btree(4096, 1, 2)?;
    let root = btree.get_root();
    // Insert a key-value pair
    let key = TestKey(42);
    btree.insert(&root, key.as_ref())?;
    let result = btree.insert(&root, key.as_ref());
    assert!(result.is_err()); // Should fail
    Ok(())
}

#[test]
#[serial]
fn test_update_single_key() -> io::Result<()> {
    let key = TestKey(1);
    let data = b"Original";
    let kv = TestKeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = TestKeyValuePair::new(&key, data2);

    let mut btree = create_test_btree(4096, 1, 2)?;

    let root = btree.get_root();
    let start_pos = (root, Slot(0));
    btree.insert(&root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), kv.as_ref());
    btree.clear_stack();

    btree.update(&root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), kv2.as_ref());
    btree.clear_stack();

    Ok(())
}

#[test]
#[serial]
fn test_upsert_single_key() -> io::Result<()> {
    let key = TestKey(1);
    let key2 = TestKey(2);

    let data = b"Original";
    let kv = TestKeyValuePair::new(&key, data);

    let data2 = b"Updated";
    let kv2 = TestKeyValuePair::new(&key, data2);

    let data3 = b"Created";
    let kv3 = TestKeyValuePair::new(&key2, data3);

    let mut btree = create_test_btree(4096, 1, 2)?;

    let root = btree.get_root();
    let start_pos = (root, Slot(0));
    btree.insert(&root, kv.as_ref())?;

    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), kv.as_ref());
    btree.clear_stack();

    btree.upsert(&root, kv2.as_ref())?;

    let retrieved = btree.search(&start_pos, &key, NodeAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found((root, Slot(0)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), kv2.as_ref());
    btree.clear_stack();

    btree.upsert(&root, kv3.as_ref())?;

    let retrieved = btree.search(&start_pos, &key2, NodeAccessMode::Read)?;

    assert!(matches!(retrieved, SearchResult::Found((root, Slot(1)))));
    let cell = btree.get_cell_from_result(retrieved);
    assert!(cell.is_some());
    assert_eq!(cell.unwrap().used(), kv3.as_ref());
    btree.clear_stack();

    Ok(())
}

#[test]
#[serial]
fn test_split_root() -> io::Result<()> {
    let mut tree = create_test_btree(4096, 3, 2)?;
    let root = tree.get_root();
    let start_pos = (root, Slot(0));

    // Insert enough keys to force root to split
    for i in 0..50 {
        let key = TestKey(i * 10);
        tree.insert(&root, key.as_ref())?;
    }
    println!("{}", tree.json()?);

    for i in 0..50 {
        let key = TestKey(i * 10);

        let retrieved = tree.search(&start_pos, &key, NodeAccessMode::Read)?;
        // Search does not release the latch on the node so we have to do it ourselves.
        let cell = tree.get_cell_from_result(retrieved);
        tree.clear_stack();
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().used(), key.as_ref());
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
        num_inserts: 240,
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
fn test_test() -> std::io::Result<()> {
    let mut tree = create_test_btree(4096, 100, 3)?;
    let root = tree.get_root();
    let start_pos = (root, Slot(0));
    let N = 1608;
    // Generate random insertion order
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    let mut keys: Vec<i32> = (0..N).collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    keys.shuffle(&mut rng);

    let last = keys.pop().unwrap();

    // Insert keys in specified order
    for (i, key_val) in keys.iter().enumerate() {
        let key = TestKey(*key_val);
     
        tree.insert(&root, key.as_ref())?;
        std::fs::write(format!("tree_{i}.json"), tree.json()?)?;
    }
    std::fs::write("tree_ok.json", tree.json()?)?;
    let key = TestKey(last);
    tree.insert(&root, key.as_ref())?;
    std::fs::write("tree_bad.json", tree.json()?)?;

    // Verify all keys exist (in sequential order)
    for i in 0..(N - 1) {
        let key = TestKey(i);
        dbg!(i);
        let retrieved = tree.search(&start_pos, &key, NodeAccessMode::Read)?;
        assert!(matches!(retrieved, SearchResult::Found(_)));
        let cell = tree.get_cell_from_result(retrieved);
        tree.clear_stack();
        assert!(cell.is_some());
        assert_eq!(cell.unwrap().used(), key.as_ref());
    }

    Ok(())
}
