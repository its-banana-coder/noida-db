//! In-process engine tests for Kafka.
//! Follows the Redis module pattern: one file per area, real error codes, clock injection, no networking.

mod admin;
mod coordinator;
mod idempotence;
mod offsets;
mod produce_fetch;
mod topics;
mod transactions;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::engine::Engine;

pub const START_MS: u64 = 1_700_000_000_000;

/// An engine with a controllable clock.
pub struct T {
    pub engine: Engine,
    now: Arc<AtomicU64>,
}

impl T {
    pub fn new() -> Self {
        let now = Arc::new(AtomicU64::new(START_MS));
        let clock = now.clone();
        let engine = Engine::with_clock(
            "127.0.0.1".to_string(),
            9092,
            Arc::new(move || clock.load(Ordering::SeqCst)),
        );
        Self { engine, now }
    }

    pub fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}
