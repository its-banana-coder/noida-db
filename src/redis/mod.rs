//! Redis 7.2 compatibility: RESP over TCP on port 6379.

mod admin;
mod command_meta;
mod connection;
mod engine;
mod glob;
mod hashes;
mod keys;
pub mod longdouble;
mod meta;
mod num;
mod ordered;
pub mod resp;
pub mod server;
mod strings;
#[cfg(test)]
mod tests;

pub use engine::{ClientConn, Engine, Session, command_names, is_implemented};

/// The Redis version noida reports (HELLO, INFO).
pub const REDIS_VERSION: &str = "7.2.5";
