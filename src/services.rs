//! The registry of services compiled into this binary.

use std::io;
use std::net::SocketAddr;

/// Starts `name` on `addr`. `None` if the service isn't built into this
/// binary (its Cargo feature is off, or it isn't implemented yet).
pub fn start(name: &str, addr: &str) -> Option<io::Result<SocketAddr>> {
    match name {
        #[cfg(feature = "redis")]
        "redis" => Some(crate::redis::server::spawn(addr)),
        #[cfg(feature = "postgres")]
        "postgres" => Some(crate::postgres::spawn(addr)),
        #[cfg(feature = "kafka")]
        "kafka" => Some(crate::kafka::spawn(addr)),
        #[cfg(feature = "memcached")]
        "memcached" => Some(crate::memcached::spawn(addr)),
        _ => {
            let _ = addr;
            None
        }
    }
}
