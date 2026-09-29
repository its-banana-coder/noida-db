// The engine's mutation methods use `Result<_, ()>` as a simple presence/
// absence signal; the server layer maps it directly to protocol replies
// (STORED/NOT_STORED/NOT_FOUND) and never needs a richer error type.
#![allow(clippy::result_unit_err)]

use std::collections::HashMap;
use std::sync::Arc;

/// Milliseconds since the Unix epoch. Injected so tests control time.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Item {
    pub data: Vec<u8>,
    pub flags: u32,
    pub cas: u64,
    /// Absolute unix time in seconds, or 0 for never expire.
    pub exptime: u64,
}

impl Item {
    pub fn is_expired(&self, now_sec: u64) -> bool {
        self.exptime != 0 && self.exptime <= now_sec
    }
}

pub struct Engine {
    pub items: HashMap<Vec<u8>, Item>,
    pub next_cas: u64,
    pub clock: Clock,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        }))
    }
}

impl Engine {
    pub fn new(clock: Clock) -> Self {
        Self { items: HashMap::new(), next_cas: 1, clock }
    }

    pub fn now_sec(&self) -> u64 {
        (self.clock)() / 1000
    }

    pub fn purge_expired(&mut self) {
        let now_sec = self.now_sec();
        self.items.retain(|_, v| !v.is_expired(now_sec));
    }

    /// Helper to calculate absolute expiry time based on memcached rules.
    /// A negative `exptime` means "expire immediately" (memcached treats it
    /// as already expired, e.g. clients that pass a negative TTL to delete
    /// on next access).
    pub fn compute_exptime(&self, exptime: i64) -> u64 {
        if exptime == 0 {
            0
        } else if exptime < 0 {
            self.now_sec().max(1)
        } else if exptime <= 60 * 60 * 24 * 30 {
            // Relative time if <= 30 days
            self.now_sec() + exptime as u64
        } else {
            // Absolute unix time
            exptime as u64
        }
    }

    pub fn set(&mut self, key: Vec<u8>, flags: u32, exptime: i64, data: Vec<u8>) -> Result<(), ()> {
        let abs_exptime = self.compute_exptime(exptime);
        let cas = self.next_cas;
        self.next_cas += 1;
        self.items.insert(key, Item { data, flags, cas, exptime: abs_exptime });
        Ok(())
    }

    pub fn add(&mut self, key: Vec<u8>, flags: u32, exptime: i64, data: Vec<u8>) -> Result<(), ()> {
        let now = self.now_sec();
        if let Some(item) = self.items.get(&key)
            && !item.is_expired(now)
        {
            return Err(());
        }
        self.set(key, flags, exptime, data)
    }

    pub fn replace(
        &mut self,
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
    ) -> Result<(), ()> {
        let now = self.now_sec();
        let exists_and_valid = self.items.get(&key).is_some_and(|item| !item.is_expired(now));
        if !exists_and_valid {
            return Err(());
        }
        self.set(key, flags, exptime, data)
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Item> {
        let now = self.now_sec();
        if let Some(item) = self.items.get(key) {
            if item.is_expired(now) {
                self.items.remove(key);
                None
            } else {
                self.items.get(key)
            }
        } else {
            None
        }
    }

    pub fn append(&mut self, key: &[u8], data: &[u8]) -> Result<(), ()> {
        let now = self.now_sec();
        if let Some(item) = self.items.get_mut(key) {
            if item.is_expired(now) {
                return Err(());
            }
            item.data.extend_from_slice(data);
            item.cas = self.next_cas;
            self.next_cas += 1;
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn prepend(&mut self, key: &[u8], data: &[u8]) -> Result<(), ()> {
        let now = self.now_sec();
        if let Some(item) = self.items.get_mut(key) {
            if item.is_expired(now) {
                return Err(());
            }
            let mut new_data = data.to_vec();
            new_data.extend_from_slice(&item.data);
            item.data = new_data;
            item.cas = self.next_cas;
            self.next_cas += 1;
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn cas(
        &mut self,
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        cas_unique: u64,
    ) -> Result<(), bool> {
        let now = self.now_sec();
        if let Some(item) = self.items.get(&key) {
            if item.is_expired(now) {
                return Err(false); // NOT_FOUND
            }
            if item.cas != cas_unique {
                return Err(true); // EXISTS
            }
        } else {
            return Err(false); // NOT_FOUND
        }
        let _ = self.set(key, flags, exptime, data);
        Ok(()) // STORED
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        let now = self.now_sec();
        if let Some(item) = self.items.get(key) {
            if item.is_expired(now) {
                self.items.remove(key);
                return Err(());
            }
            self.items.remove(key);
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn incr_decr(&mut self, key: &[u8], delta: u64, incr: bool) -> Result<u64, Result<(), ()>> {
        let now = self.now_sec();
        if let Some(item) = self.items.get_mut(key) {
            if item.is_expired(now) {
                return Err(Err(())); // NOT_FOUND
            }
            let s = match std::str::from_utf8(&item.data) {
                Ok(s) => s.trim(),
                Err(_) => return Err(Ok(())), // Non-numeric
            };
            let mut val = match s.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return Err(Ok(())), // Non-numeric
            };
            if incr {
                val = val.wrapping_add(delta);
            } else {
                val = val.saturating_sub(delta);
            }
            item.data = val.to_string().into_bytes();
            item.cas = self.next_cas;
            self.next_cas += 1;
            Ok(val)
        } else {
            Err(Err(())) // NOT_FOUND
        }
    }

    pub fn touch(&mut self, key: &[u8], exptime: i64) -> Result<(), ()> {
        let now = self.now_sec();
        let new_exptime = self.compute_exptime(exptime);
        if let Some(item) = self.items.get_mut(key) {
            if item.is_expired(now) {
                return Err(());
            }
            item.exptime = new_exptime;
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn flush_all(&mut self, delay: i64) {
        if delay <= 0 {
            self.items.clear();
        } else {
            let expiration_time = self.now_sec() + delay as u64;
            for item in self.items.values_mut() {
                if item.exptime == 0 || item.exptime > expiration_time {
                    item.exptime = expiration_time;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn make_test_engine() -> (Engine, Arc<AtomicU64>) {
        let time = Arc::new(AtomicU64::new(1_000_000_000_000)); // Start at a nice round number in ms
        let t2 = Arc::clone(&time);
        let clock: Clock = Arc::new(move || t2.load(Ordering::SeqCst));
        (Engine::new(clock), time)
    }

    #[test]
    fn test_set_get() {
        let (mut engine, _) = make_test_engine();
        assert!(engine.set(b"key".to_vec(), 0, 0, b"val".to_vec()).is_ok());

        let item = engine.get(b"key").unwrap();
        assert_eq!(item.data, b"val");
    }

    #[test]
    fn test_expiry_relative() {
        let (mut engine, time) = make_test_engine();
        // Set with 10s relative expiry
        engine.set(b"key".to_vec(), 0, 10, b"val".to_vec()).unwrap();

        assert!(engine.get(b"key").is_some());

        // Advance 11s
        time.fetch_add(11_000, Ordering::SeqCst);

        assert!(engine.get(b"key").is_none());
    }

    #[test]
    fn test_expiry_absolute() {
        let (mut engine, time) = make_test_engine();
        let current_sec = engine.now_sec();
        // Set with absolute expiry 5s in the future
        let abs_time = current_sec + 5;
        // Ensure it's treated as absolute (memcached treats > 30 days as absolute)
        // 30 days is 2592000. So we need abs_time > 2592000.
        // current_sec is 1_000_000_000, so it works.
        engine.set(b"key".to_vec(), 0, abs_time as i64, b"val".to_vec()).unwrap();

        assert!(engine.get(b"key").is_some());

        // Advance 6s
        time.fetch_add(6_000, Ordering::SeqCst);

        assert!(engine.get(b"key").is_none());
    }

    #[test]
    fn test_expiry_negative_is_immediate() {
        let (mut engine, _) = make_test_engine();
        // A negative exptime means "expire immediately" per the memcached
        // protocol, not a parse error.
        engine.set(b"key".to_vec(), 0, -1, b"val".to_vec()).unwrap();
        assert!(engine.get(b"key").is_none());
    }

    #[test]
    fn test_cas_behavior() {
        let (mut engine, _) = make_test_engine();
        engine.set(b"key".to_vec(), 0, 0, b"val1".to_vec()).unwrap();

        let cas_val = engine.get(b"key").unwrap().cas;

        // Valid CAS update
        assert_eq!(engine.cas(b"key".to_vec(), 0, 0, b"val2".to_vec(), cas_val), Ok(()));

        // Invalid CAS update
        assert_eq!(engine.cas(b"key".to_vec(), 0, 0, b"val3".to_vec(), cas_val), Err(true)); // EXISTS

        // Missing key CAS update
        assert_eq!(engine.cas(b"missing".to_vec(), 0, 0, b"val4".to_vec(), 123), Err(false)); // NOT_FOUND
    }
}
