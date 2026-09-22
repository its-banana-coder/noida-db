//! Postgres compatibility. Work in progress; see docs/SERVICE_GUIDE.md.

pub mod datetime;
pub mod json;
pub mod numeric;
pub mod tz;

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves Postgres on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let _ = addr;
    Err(io::Error::new(io::ErrorKind::Unsupported, "postgres is not implemented yet"))
}
