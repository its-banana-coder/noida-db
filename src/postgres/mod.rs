//! Postgres compatibility. Work in progress; see docs/SERVICE_GUIDE.md.

// PgError carries Postgres's full error fields. It travels by value on the
// rare error path, where its size costs nothing worth the indirection.
#![allow(clippy::result_large_err)]

pub mod auth;
pub mod casts;
pub mod catalog;
pub mod datetime;
pub mod error;
pub mod funcs;
pub mod json;
pub mod keywords;
pub mod numeric;
pub mod plan;
pub mod session;
pub mod sigs;
pub mod types;
pub mod tz;

use std::io;
use std::net::SocketAddr;

/// Binds `addr` and serves Postgres on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let _ = addr;
    Err(io::Error::new(io::ErrorKind::Unsupported, "postgres is not implemented yet"))
}
