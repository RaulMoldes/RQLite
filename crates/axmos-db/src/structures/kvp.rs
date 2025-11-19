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
