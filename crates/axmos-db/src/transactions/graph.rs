use crate::types::TransactionId;
use std::{
    collections::{HashMap, HashSet},
    fmt,
};

#[repr(transparent)]
pub struct WFGCycle(Vec<TransactionId>);

impl From<&[TransactionId]> for WFGCycle {
    fn from(value: &[TransactionId]) -> Self {
        Self(value.to_vec())
    }
}

impl WFGCycle {
    pub fn iter(&self) -> std::slice::Iter<'_, TransactionId> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, TransactionId> {
        self.0.iter_mut()
    }

    pub fn get(&self, index: usize) -> Option<&TransactionId> {
        self.0.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut TransactionId> {
        self.0.get_mut(index)
    }

    pub fn push(&mut self, item: TransactionId) {
        self.0.push(item)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[repr(transparent)]
pub struct WaitForGraph(HashMap<TransactionId, HashSet<TransactionId>>);

impl WaitForGraph {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn entry(
        &mut self,
        key: TransactionId,
    ) -> std::collections::hash_map::Entry<'_, TransactionId, HashSet<TransactionId>> {
        self.0.entry(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TransactionId, &HashSet<TransactionId>)> {
        self.0.iter()
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&TransactionId, &mut HashSet<TransactionId>)> {
        self.0.iter_mut()
    }

    pub fn get(&self, tx: &TransactionId) -> Option<&HashSet<TransactionId>> {
        self.0.get(tx)
    }

    pub fn get_mut(&mut self, tx: &TransactionId) -> Option<&mut HashSet<TransactionId>> {
        self.0.get_mut(tx)
    }

    pub fn insert(&mut self, tx: TransactionId, deps: HashSet<TransactionId>) {
        self.0.insert(tx, deps);
    }

    pub fn remove(&mut self, tx: &TransactionId) {
        self.0.remove(tx);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &TransactionId> {
        self.0.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &HashSet<TransactionId>> {
        self.0.values()
    }

    /// Add an edge to the graph
    pub fn add_edge(&mut self, tx: TransactionId, depends_on: TransactionId) {
        self.0.entry(tx).or_default().insert(depends_on);
    }

    /// Remove edge
    pub fn remove_edge(&mut self, tx: TransactionId, depends_on: &TransactionId) {
        if let Some(deps) = self.0.get_mut(&tx) {
            deps.remove(depends_on);
            if deps.is_empty() {
                // clean up empty entry
                self.0.remove(&tx);
            }
        }
    }

    /// Return all transactions that `tx` waits for
    pub fn dependents_of(&self, tx: TransactionId) -> Option<&HashSet<TransactionId>> {
        self.0.get(&tx)
    }

    /// Return all transactions that wait for `target`
    pub fn waiters_of(&self, target: TransactionId) -> Vec<TransactionId> {
        self.0
            .iter()
            .filter_map(|(from, deps)| {
                if deps.contains(&target) {
                    Some(*from)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl fmt::Display for WFGCycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self
            .0
            .iter()
            .map(|tx| tx.to_string())
            .collect::<Vec<_>>()
            .join(" -> ");

        write!(f, "{}", s)
    }
}

impl fmt::Display for WaitForGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (from, to_set) in self.0.iter() {
            let deps = to_set
                .iter()
                .map(|tx| tx.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            writeln!(f, "{} -> [{}]", from, deps)?;
        }
        Ok(())
    }
}
