//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod analysis;
mod dates;
mod engine;
mod query_string;
mod scoring;
mod search;
mod server;
mod sorting;

#[cfg(test)]
mod tests;

pub use server::spawn;
pub use server::spawn_persistent;
pub use server::spawn_persistent_for_test;
