//! ClickHouse compatibility: the HTTP interface first (native TCP protocol
//! is milestone 4). See docs/specs/clickhouse.md.

pub mod engine;
pub mod error;
pub mod format;
pub mod http;
pub mod server;
pub mod sql;

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves ClickHouse's HTTP interface on background
/// threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    server::spawn(addr)
}
