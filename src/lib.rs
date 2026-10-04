//! noida-db: one tiny local binary standing in for Postgres, MySQL, Redis,
//! Kafka, Elasticsearch, ClickHouse and MongoDB during
//! development.
//!
//! Each service is a module behind its own Cargo feature.

pub mod config;
pub mod persistence;
pub mod services;

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
