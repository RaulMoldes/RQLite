use std::hash::Hash;
use std::ops::Index;
use crate::make_shared;
use std::collections::HashMap;
use super::Transaction;
use crate::types::TxId;

// HashMap wrapper.
// We need it to be able to create the proper shared type on top of it.
#[derive(Debug)]
pub(crate) struct MemTable<K,V>
where K: Hash + Eq
{
    map: HashMap<K,V>
}

impl<K, V> Default for MemTable<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}


impl<K, V> From<HashMap<K, V>> for MemTable<K, V>
where
    K: Eq + Hash,
{
    fn from(map: HashMap<K, V>) -> Self {
        Self { map }
    }
}

impl<K, V> IntoIterator for MemTable<K, V>
where
    K: Eq + Hash,
{
    type Item = (K, V);
    type IntoIter = std::collections::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}


impl<'a, K, V> IntoIterator for &'a MemTable<K, V>
where
    K: Eq + Hash,
{
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}


impl<'a, K, V> IntoIterator for &'a mut MemTable<K, V>
where
    K: Eq + Hash,
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = std::collections::hash_map::IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter_mut()
    }
}

impl<K, V> MemTable<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.map.insert(key, value)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn clear(&mut self) {
        self.map.clear()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.map.iter_mut()
    }
}

impl<K, V> Index<&K> for MemTable<K, V>
where
    K: Eq + Hash,
{
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        self.map.get(key).expect("Key not found in MemTable")
    }
}

make_shared!(TransactionTable, MemTable<TxId, Transaction>);
