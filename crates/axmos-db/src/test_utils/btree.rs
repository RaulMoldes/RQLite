use crate::{
    storage::cell::OwnedCell,
    structures::builder::{IntoCell, KeyBytes},
    types::VarInt,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

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

    pub fn value(&self) -> &[u8] {
        &self.data[self.key_size..]
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
    fn from(kv: KeyValuePair) -> OwnedCell {
        OwnedCell::new_with_writer(kv.data.len(), |buf| {
            buf.copy_from_slice(&kv.data);
            kv.data.len()
        })
    }
}

impl IntoCell for KeyValuePair {
    fn serialized_size(&self) -> usize {
        self.data.len()
    }

    fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(self.key())
    }
}

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

impl From<TestKey> for Box<[u8]> {
    fn from(value: TestKey) -> Self {
        Box::from(value.as_ref())
    }
}

impl From<TestKey> for OwnedCell {
    fn from(key: TestKey) -> OwnedCell {
        let bytes = key.as_ref();
        OwnedCell::new_with_writer(bytes.len(), |buf| {
            buf.copy_from_slice(bytes);
            bytes.len()
        })
    }
}

impl IntoCell for TestKey {
    fn serialized_size(&self) -> usize {
        std::mem::size_of::<i32>()
    }

    fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(self.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct TestVarLengthKey {
    encoded: Vec<u8>,
}

impl TestVarLengthKey {
    pub fn from_raw_bytes(bytes: Vec<u8>) -> Self {
        Self { encoded: bytes }
    }
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
    fn from(key: TestVarLengthKey) -> OwnedCell {
        OwnedCell::new_with_writer(key.encoded.len(), |buf| {
            buf.copy_from_slice(&key.encoded);
            key.encoded.len()
        })
    }
}

impl IntoCell for TestVarLengthKey {
    fn serialized_size(&self) -> usize {
        self.encoded.len()
    }

    fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(self.encoded.as_slice())
    }
}

/// Wrapper for raw byte slices that implements IntoCell
pub struct RawBytes {
    data: Box<[u8]>,
    key_len: usize,
}

impl RawBytes {
    pub fn new(data: &[u8], key_len: usize) -> Self {
        Self {
            data: data.to_vec().into_boxed_slice(),
            key_len,
        }
    }

    pub fn key_only(data: &[u8]) -> Self {
        Self {
            data: data.to_vec().into_boxed_slice(),
            key_len: data.len(),
        }
    }
}

impl From<RawBytes> for OwnedCell {
    fn from(raw: RawBytes) -> OwnedCell {
        OwnedCell::new_with_writer(raw.data.len(), |buf| {
            buf.copy_from_slice(&raw.data);
            raw.data.len()
        })
    }
}

impl From<RawBytes> for Box<[u8]> {
    fn from(value: RawBytes) -> Self {
        Box::from(value.as_ref())
    }
}

impl AsRef<[u8]> for RawBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl IntoCell for RawBytes {
    fn serialized_size(&self) -> usize {
        self.data.len()
    }

    fn key_bytes(&self) -> KeyBytes<'_> {
        KeyBytes::from(&self.data[..self.key_len])
    }
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
