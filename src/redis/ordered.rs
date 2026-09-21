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

#[cfg(test)]
mod tests {
    use super::OrderedMap;

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
