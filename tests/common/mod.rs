//! Helpers shared by integration tests.
#![allow(dead_code)]

use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpStream};

use noida::redis::resp::{self, Value};

/// Starts a fresh noida Redis server on a free port.
pub fn start_noida_redis() -> SocketAddr {
    noida::redis::server::spawn("127.0.0.1:0").expect("start noida redis")
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
