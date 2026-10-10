//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod analysis;
mod cat;
mod dates;
mod docparse;
mod engine;
mod features;
mod field_caps;
mod fields;
mod highlight;
mod index_sort;
mod jsonpos;
mod limits;
mod lookup;
mod names;
mod painless;
mod profile;
mod quantize;
mod queries;
mod query_string;
mod ranges;
mod rescore;
mod retrievers;
mod scoring;
mod script_fields;
mod search;
mod server;
mod sorting;
mod suggest;
mod synthetic;
mod templates;
mod termvectors;
mod tsdb;
mod typed_keys;
mod vectors;

#[cfg(test)]
mod tests;

pub use server::spawn;
pub use server::spawn_persistent;
pub use server::spawn_persistent_for_test;
