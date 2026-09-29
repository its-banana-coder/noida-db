//! Memcached compatibility. Not implemented yet; see docs/SERVICE_GUIDE.md.

pub mod engine;
pub mod server;

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves Memcached on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    server::spawn(addr)
}
