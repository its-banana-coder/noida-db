//! Redis 7.2 compatibility: RESP over TCP on port 6379.

mod admin;
mod bitops;
mod blocking;
mod cjson;
mod cmsgpack;
mod command_meta;
mod config;
mod config_table;
mod connection;
mod debug;
mod devtools;
mod double;
mod engine;
mod functions;
mod geo;
mod glob;
mod hashes;
mod hll;
mod keys;
mod lists;
pub mod longdouble;
mod luabit;
mod meta;
mod monitor;
mod multi;
mod num;
mod ordered;
mod pubsub;
mod rdb;
pub mod resp;
pub(crate) mod scripting;
pub mod server;
mod sets;
mod sha1;
mod sort;
mod streams;
mod strings;
#[cfg(test)]
mod tests;
mod zsets;

pub use engine::{ClientConn, Engine, Session, command_names, is_implemented};

/// The Redis version noida-db reports (HELLO, INFO).
pub const REDIS_VERSION: &str = "7.2.5";
