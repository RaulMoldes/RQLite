use crate::{
    AxmosDBConfig, IncrementalVaccum, TextEncoding,
    io::pager::{Pager, SharedPager},
    structures::{bplustree::BPlusTree, comparator::Comparator},
    transactions::worker::Worker,
    types::VarInt,
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::io;
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

    pub fn as_ref(&self) -> &[u8] {
        &self.data
    }

    pub fn key(&self) -> &[u8] {
        &self.data[..self.key_size]
    }

    pub fn value(&self) -> &[u8] {
        &self.data[self.key_size..]
    }
}

impl AsRef<[u8]> for KeyValuePair {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

// Test key type
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct TestKey(i32);

impl TestKey {
    pub fn new(value: i32) -> Self {
        Self(value)
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

pub struct TestVarLengthKey(Vec<u8>);

impl TestVarLengthKey {
    pub fn from_string(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buffer = [0u8; crate::types::varint::MAX_VARINT_LEN];
        let mut bytes_data = VarInt::encode(self.0.len() as i64, &mut buffer).to_vec();
        bytes_data.extend_from_slice(self.0.as_ref());
        bytes_data
    }
}

pub fn create_test_btree<Cmp: Comparator + Clone>(
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
