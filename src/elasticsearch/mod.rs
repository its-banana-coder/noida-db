//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod analysis;
mod cat;
mod dates;
mod dsl;
mod engine;
mod explain;
mod field_caps;
mod fields;
mod fuzzy;
mod highlight;
mod intervals;
mod mlt;
mod painless;
mod queries;
mod query_string;
mod rescore;
mod scoring;
mod search;
mod server;
mod sorting;
mod suggest;
mod templates;
mod vectors;

#[cfg(test)]
mod tests;

pub use server::spawn;
pub use server::spawn_persistent;
pub use server::spawn_persistent_for_test;
