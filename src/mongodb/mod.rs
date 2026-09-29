//! MongoDB compatibility.

pub mod engine;
pub mod server;
pub mod wire;

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves MongoDB on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    server::spawn(addr)
}
