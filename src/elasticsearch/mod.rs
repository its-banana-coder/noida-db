//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod engine;
mod server;

#[cfg(test)]
mod tests;

pub use server::spawn;
