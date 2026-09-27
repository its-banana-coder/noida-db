//! Helpers shared by integration tests.
#![allow(dead_code)]

use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpStream};

use noida::redis::resp::{self, Value};

/// Starts a fresh noida-db Redis server on a free port.
pub fn start_noida_redis() -> SocketAddr {
    noida::redis::server::spawn("127.0.0.1:0").expect("start noida-db redis")
}

/// A minimal raw RESP client, so tests see exact replies.
pub struct RawClient {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RawClient {
    pub fn connect(addr: SocketAddr) -> RawClient {
        let stream = TcpStream::connect(addr).expect("connect");
        RawClient { reader: BufReader::new(stream.try_clone().unwrap()), writer: stream }
    }

    pub fn send(&mut self, args: &[&[u8]]) {
        let v = Value::Array(args.iter().map(Value::bulk).collect());
        let mut out = Vec::new();
        resp::encode(&v, 2, &mut out);
        self.writer.write_all(&out).unwrap();
    }

    pub fn send_raw(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).unwrap();
    }

    /// Makes reads give up after `ms` instead of blocking forever.
    pub fn set_timeout(&self, ms: u64) {
        self.writer.set_read_timeout(Some(std::time::Duration::from_millis(ms))).unwrap();
    }

    /// Like `read`, but `None` on timeout, error or a closed connection.
    pub fn try_read(&mut self) -> Option<Value> {
        resp::read_value(&mut self.reader).ok().flatten()
    }

    /// The local port of this connection (how MONITOR names a client).
    pub fn local_port(&self) -> u16 {
        self.writer.local_addr().unwrap().port()
    }

    pub fn read(&mut self) -> Option<Value> {
        resp::read_value(&mut self.reader).expect("read reply")
    }

    pub fn cmd(&mut self, args: &[&[u8]]) -> Value {
        self.send(args);
        self.read().expect("connection closed")
    }

    /// Runs an inline-style command line, e.g. `"SET k v"`.
    pub fn run(&mut self, line: &str) -> Value {
        let args: Vec<&[u8]> = line.split_whitespace().map(str::as_bytes).collect();
        self.cmd(&args)
    }
}
