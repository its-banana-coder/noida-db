//! noida: one tiny local binary standing in for Postgres, MySQL, Redis,
//! Kafka, Elasticsearch, ClickHouse, Memcached, MongoDB and RabbitMQ during
//! development.
//!
//! Each service is a module behind its own Cargo feature.

pub mod config;
pub mod services;

#[cfg(feature = "redis")]
pub mod redis;
