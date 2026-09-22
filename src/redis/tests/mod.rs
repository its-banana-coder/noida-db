//! Command-level tests against the in-process engine. Expected replies and
//! error strings are Redis 7.2's, byte for byte.

mod connection;
mod hashes;
mod introspection;
mod lists;
mod sets;
mod strings_keys;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::resp::{Value, split_inline};
use super::{ClientConn, Engine, Session};

pub const START_MS: u64 = 1_700_000_000_000;

/// An engine with a controllable clock and one connected client.
pub struct T {
    pub engine: Engine,
    pub session: Session,
    now: Arc<AtomicU64>,
}

impl T {
    pub fn new() -> T {
        let now = Arc::new(AtomicU64::new(START_MS));
        let clock = now.clone();
        let mut engine = Engine::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
        let session = engine.connect(test_conn(1));
        T { engine, session, now }
    }

    /// Connects another client to the same engine.
    pub fn connect(&mut self) -> Session {
        let n = self.engine.client_count() as u16 + 1;
        self.engine.connect(test_conn(n))
    }

    pub fn run(&mut self, line: &str) -> Value {
        let args = split_inline(line.as_bytes()).expect("bad test command");
        self.engine.execute(&mut self.session, &args)
    }

    pub fn run_as(&mut self, session: &mut Session, line: &str) -> Value {
        let args = split_inline(line.as_bytes()).expect("bad test command");
        self.engine.execute(session, &args)
    }

    pub fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}

fn test_conn(n: u16) -> ClientConn {
    ClientConn {
        addr: format!("127.0.0.1:{}", 50000 + n),
        laddr: "127.0.0.1:6379".into(),
        fd: 7 + n as i64,
        kill: None,
    }
}

pub fn ok() -> Value {
    Value::ok()
}
pub fn nil() -> Value {
    Value::Null
}
pub fn int(n: i64) -> Value {
    Value::Integer(n)
}
pub fn bulk(s: &str) -> Value {
    Value::bulk(s)
}
pub fn simple(s: &str) -> Value {
    Value::Simple(s.into())
}
pub fn err(s: &str) -> Value {
    Value::err(s)
}
pub fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}
pub fn bulks(items: &[&str]) -> Value {
    arr(items.iter().map(|s| bulk(s)).collect())
}
pub fn simples(items: &[&str]) -> Value {
    Value::Set(items.iter().map(|s| simple(s)).collect())
}
pub fn map(pairs: Vec<(&str, Value)>) -> Value {
    Value::Map(pairs.into_iter().map(|(k, v)| (bulk(k), v)).collect())
}
pub fn sorted(v: Value) -> Value {
    match v {
        Value::Array(mut items) => {
            items.sort_by_key(|i| format!("{i:?}"));
            Value::Array(items)
        }
        other => other,
    }
}
/// The text of a bulk or verbatim reply.
pub fn text(v: &Value) -> String {
    match v {
        Value::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
        Value::Verbatim(_, b) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("expected text, got {other:?}"),
    }
}

pub const NOT_INT: &str = "ERR value is not an integer or out of range";
pub const SYNTAX: &str = "ERR syntax error";
