//! An insertion-ordered map. Redis keeps small hashes (and sets) in
//! insertion order, and clients see that order in HGETALL and friends.

use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct OrderedMap {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: HashMap<Vec<u8>, usize>,
}

impl OrderedMap {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, k: &[u8]) -> Option<&Vec<u8>> {
        self.index.get(k).map(|&i| &self.entries[i].1)
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        self.index.contains_key(k)
    }

    /// Inserts or overwrites in place. Returns true if the key was new.
    pub fn insert(&mut self, k: Vec<u8>, v: Vec<u8>) -> bool {
        if let Some(&i) = self.index.get(&k) {
            self.entries[i].1 = v;
            return false;
        }
        self.index.insert(k.clone(), self.entries.len());
        self.entries.push((k, v));
        true
    }

    /// Removes a key, keeping the order of the rest.
    pub fn remove(&mut self, k: &[u8]) -> bool {
        let Some(i) = self.index.remove(k) else { return false };
        self.entries.remove(i);
        for (key, _) in &self.entries[i..] {
            *self.index.get_mut(key).unwrap() -= 1;
        }
        true
    }

    pub fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>)> {
        self.entries.iter()
    }

    pub fn entry_at(&self, i: usize) -> &(Vec<u8>, Vec<u8>) {
        &self.entries[i]
    }
}

/// An insertion-ordered set of byte strings with O(1) lookups.
#[derive(Clone, Debug, Default)]
pub struct OrderedSet {
    items: Vec<Vec<u8>>,
    index: HashMap<Vec<u8>, usize>,
}

impl OrderedSet {
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn contains(&self, v: &[u8]) -> bool {
        self.index.contains_key(v)
    }

    /// Appends `v` unless present. Returns true if it was new.
    pub fn insert(&mut self, v: &[u8]) -> bool {
        if self.index.contains_key(v) {
            return false;
        }
        self.index.insert(v.to_vec(), self.items.len());
        self.items.push(v.to_vec());
        true
    }

    /// Removes `v`, keeping the order of the rest (O(n)).
    pub fn remove(&mut self, v: &[u8]) -> bool {
        let Some(i) = self.index.remove(v) else { return false };
        self.items.remove(i);
        for item in &self.items[i..] {
            *self.index.get_mut(item).unwrap() -= 1;
        }
        true
    }

    /// Removes `v` in O(1), moving the last element into its place.
    pub fn swap_remove(&mut self, v: &[u8]) -> bool {
        let Some(i) = self.index.remove(v) else { return false };
        self.items.swap_remove(i);
        if let Some(moved) = self.items.get(i) {
            *self.index.get_mut(moved).unwrap() = i;
        }
        true
    }

    pub fn get(&self, i: usize) -> &[u8] {
        &self.items[i]
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderedMap, OrderedSet};

    #[test]
    fn ordered_set_removals() {
        let mut s = OrderedSet::default();
        for v in ["a", "b", "c", "d"] {
            assert!(s.insert(v.as_bytes()));
        }
        assert!(!s.insert(b"b"));
        assert!(s.remove(b"b"));
        assert!(s.swap_remove(b"a"));
        let items: Vec<_> = s.iter().cloned().collect();
        assert_eq!(items, [b"d".to_vec(), b"c".to_vec()]);
        assert!(s.contains(b"c") && !s.contains(b"a"));
        assert!(s.swap_remove(b"c") && s.get(0) == b"d");
    }

    #[test]
    fn keeps_order_through_overwrites_and_removals() {
        let mut m = OrderedMap::default();
        assert!(m.insert(b"z".to_vec(), b"1".to_vec()));
        assert!(m.insert(b"a".to_vec(), b"2".to_vec()));
        assert!(m.insert(b"m".to_vec(), b"3".to_vec()));
        assert!(!m.insert(b"a".to_vec(), b"20".to_vec()));
        assert!(m.remove(b"z"));
        assert!(!m.remove(b"z"));
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, [b"a".to_vec(), b"m".to_vec()]);
        assert_eq!(m.get(b"a"), Some(&b"20".to_vec()));
        assert_eq!(m.get(b"m"), Some(&b"3".to_vec()));
        assert_eq!(m.len(), 2);
    }
}
