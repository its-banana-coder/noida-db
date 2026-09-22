//! Redis 7.2 compatibility: RESP over TCP on port 6379.

mod admin;
mod blocking;
mod command_meta;
mod connection;
mod engine;
mod glob;
mod hashes;
mod keys;
mod lists;
pub mod longdouble;
mod meta;
mod num;
mod ordered;
pub mod resp;
pub mod server;
mod sets;
mod strings;
#[cfg(test)]
mod tests;

pub use engine::{ClientConn, Engine, Session, command_names, is_implemented};

/// The Redis version noida reports (HELLO, INFO).
pub const REDIS_VERSION: &str = "7.2.5";
