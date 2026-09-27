//! Redis 7.2 compatibility: RESP over TCP on port 6379.

mod admin;
mod bitops;
mod blocking;
mod cjson;
mod command_meta;
mod config;
mod config_table;
mod connection;
mod devtools;
mod double;
mod engine;
mod geo;
mod glob;
mod hashes;
mod keys;
mod lists;
pub mod longdouble;
mod meta;
mod multi;
mod num;
mod ordered;
mod pubsub;
pub mod resp;
pub(crate) mod scripting;
pub mod server;
mod sets;
mod sha1;
mod streams;
mod strings;
#[cfg(test)]
mod tests;
mod zsets;

pub use engine::{ClientConn, Engine, Session, command_names, is_implemented};

/// The Redis version noida reports (HELLO, INFO).
pub const REDIS_VERSION: &str = "7.2.5";
