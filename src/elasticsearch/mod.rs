//! Minimal Elasticsearch 8.15 compatible HTTP service for local development.

mod engine;
mod server;

pub use server::spawn;
