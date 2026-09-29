//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod analysis;
mod engine;
mod scoring;
mod search;
mod server;

#[cfg(test)]
mod tests;

pub use server::spawn;
