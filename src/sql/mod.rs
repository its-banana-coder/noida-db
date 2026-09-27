//! The dialect-neutral pieces of noida's SQL engine, shared by the Postgres
//! and MySQL services: exact numerics, calendar arithmetic, time zones and
//! JSON. Nothing here knows about a wire protocol, a SQLSTATE or a catalog.
//!
//! Dialect rules (parsing, error codes, type names, output formats) stay in
//! `src/postgres/` and `src/mysql/`.

pub mod datetime;
pub mod json;
pub mod numeric;
pub mod tz;
