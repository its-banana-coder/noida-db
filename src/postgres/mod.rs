//! Postgres compatibility. Not implemented yet; see docs/SERVICE_GUIDE.md.

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves Postgres on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let _ = addr;
    Err(io::Error::new(io::ErrorKind::Unsupported, "postgres is not implemented yet"))
}
