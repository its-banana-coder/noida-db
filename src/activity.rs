//! The activity feed: a ring buffer of the last requests every service
//! handled, one line each, for `noida-db activity` and the control API.
//!
//! Each service calls `record` where it dispatches a request. It costs one
//! uncontended lock and a short string copy, and nothing at all until the
//! control API turns the feed on (`enable`), so tests and embedders that
//! never read it don't pay for it.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// How many events the feed keeps.
pub const CAPACITY: usize = 1000;
/// How long one event's text may be, in characters.
pub const MAX_TEXT: usize = 200;

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub ts_ms: u64,
    pub service: &'static str,
    pub text: String,
}

/// The ring buffer itself (one per process, behind `FEED`).
pub struct Feed {
    next_seq: u64,
    events: VecDeque<Event>,
}

impl Default for Feed {
    fn default() -> Self {
        Feed::new()
    }
}

impl Feed {
    pub const fn new() -> Self {
        Feed { next_seq: 1, events: VecDeque::new() }
    }

    pub fn push(&mut self, ts_ms: u64, service: &'static str, text: &str) {
        if self.events.len() == CAPACITY {
            self.events.pop_front();
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.push_back(Event { seq, ts_ms, service, text: one_line(text) });
    }

    /// Up to `limit` events from `since` on (oldest first), or the latest
    /// `limit` when `since` is 0, and the `since` to pass next time. A
    /// `since` from a previous run of the server (past the newest event)
    /// starts over from now.
    pub fn read(&self, since: u64, limit: usize) -> (u64, Vec<Event>) {
        let limit = limit.clamp(1, CAPACITY);
        let events: Vec<Event> = if since == 0 {
            let skip = self.events.len().saturating_sub(limit);
            self.events.iter().skip(skip).cloned().collect()
        } else {
            self.events.iter().filter(|e| e.seq >= since).take(limit).cloned().collect()
        };
        let next = match events.last() {
            Some(e) => e.seq + 1,
            None if since == 0 || since > self.next_seq => self.next_seq,
            None => since,
        };
        (next, events)
    }
}

static FEED: Mutex<Feed> = Mutex::new(Feed::new());
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Turns the feed on (the control API does, when it starts).
pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Whether anything records; a caller that has to build its text checks
/// this first.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Adds one event for `service`. `text` is made one line and truncated.
pub fn record(service: &'static str, text: &str) {
    if !enabled() {
        return;
    }
    let ts_ms = now_ms();
    FEED.lock().unwrap_or_else(|e| e.into_inner()).push(ts_ms, service, text);
}

/// See `Feed::read`.
pub fn read(since: u64, limit: usize) -> (u64, Vec<Event>) {
    FEED.lock().unwrap_or_else(|e| e.into_inner()).read(since, limit)
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `text` on one line: whitespace runs (newlines included) become one
/// space, other control characters go, and anything past `MAX_TEXT`
/// characters becomes "…".
pub fn one_line(text: &str) -> String {
    let mut out = String::with_capacity(text.len().min(MAX_TEXT + 3));
    let mut chars = 0;
    let mut space = false;
    for c in text.trim().chars() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if c.is_control() {
            continue;
        }
        if space {
            out.push(' ');
            chars += 1;
            space = false;
        }
        if chars == MAX_TEXT {
            out.push('…');
            break;
        }
        out.push(c);
        chars += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(n: usize) -> Feed {
        let mut f = Feed::new();
        for i in 0..n {
            f.push(i as u64, "redis", &format!("SET k{i}"));
        }
        f
    }

    #[test]
    fn keeps_the_last_capacity_events() {
        let f = feed(CAPACITY + 5);
        let (next, events) = f.read(0, CAPACITY);
        assert_eq!(events.len(), CAPACITY);
        assert_eq!(events[0].seq, 6);
        assert_eq!(events.last().unwrap().text, format!("SET k{}", CAPACITY + 4));
        assert_eq!(next, CAPACITY as u64 + 6);
    }

    #[test]
    fn reads_page_forward_with_next() {
        let mut f = feed(5);
        let (next, events) = f.read(0, 2);
        assert_eq!(events.iter().map(|e| e.seq).collect::<Vec<_>>(), [4, 5]);
        assert_eq!(next, 6);
        assert!(f.read(next, 10).1.is_empty());
        assert_eq!(f.read(next, 10).0, 6);
        f.push(9, "postgres", "SELECT 1");
        let (next, events) = f.read(next, 10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].service, "postgres");
        assert_eq!(next, 7);
        // From the start, a page at a time.
        let (next, events) = f.read(1, 4);
        assert_eq!(events.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2, 3, 4]);
        assert_eq!(f.read(next, 4).1.len(), 2);
    }

    #[test]
    fn since_from_an_earlier_run_starts_over() {
        let f = feed(3);
        assert_eq!(f.read(500, 10), (4, vec![]));
        // Older than the buffer: from the oldest kept.
        let f = feed(CAPACITY + 10);
        assert_eq!(f.read(2, 1).1[0].seq, 11);
    }

    #[test]
    fn text_is_one_short_line() {
        assert_eq!(
            one_line("  SELECT *\n  FROM t\r\n\tWHERE x = 1;\n"),
            "SELECT * FROM t WHERE x = 1;"
        );
        assert_eq!(one_line("a\u{7}b"), "ab");
        let long = "x".repeat(MAX_TEXT + 50);
        let short = one_line(&long);
        assert_eq!(short.chars().count(), MAX_TEXT + 1);
        assert!(short.ends_with('…'));
        assert_eq!(one_line(&"é".repeat(MAX_TEXT)).chars().count(), MAX_TEXT);
    }
}
