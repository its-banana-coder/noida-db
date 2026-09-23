//! Per-connection settings (GUCs): SET, SHOW, RESET, current_setting().

use std::collections::BTreeMap;

use super::error::{PgError, PgResult, code};
use super::types::FmtCtx;
use super::tz::{self, Zone};

/// The Postgres version noida reports.
pub const SERVER_VERSION: &str = "16.4";
pub const SERVER_VERSION_NUM: &str = "160004";

/// (name, default, reported to the client in ParameterStatus)
const DEFAULTS: &[(&str, &str, bool)] = &[
    ("application_name", "", true),
    ("client_encoding", "UTF8", true),
    ("DateStyle", "ISO, MDY", true),
    ("default_transaction_read_only", "off", true),
    ("in_hot_standby", "off", true),
    ("integer_datetimes", "on", true),
    ("IntervalStyle", "postgres", true),
    ("is_superuser", "on", true),
    ("server_encoding", "UTF8", true),
    ("server_version", SERVER_VERSION, true),
    ("session_authorization", "postgres", true),
    ("standard_conforming_strings", "on", true),
    ("TimeZone", "UTC", true),
    ("server_version_num", SERVER_VERSION_NUM, false),
    ("search_path", "\"$user\", public", false),
    ("extra_float_digits", "1", false),
    ("bytea_output", "hex", false),
    ("statement_timeout", "0", false),
    ("lock_timeout", "0", false),
    ("idle_in_transaction_session_timeout", "0", false),
    ("client_min_messages", "notice", false),
    ("default_transaction_isolation", "read committed", false),
    ("transaction_isolation", "read committed", false),
    ("transaction_read_only", "off", false),
    ("default_transaction_deferrable", "off", false),
    ("transaction_deferrable", "off", false),
    ("max_connections", "100", false),
    ("max_identifier_length", "63", false),
    ("block_size", "8192", false),
    ("lc_collate", "C", false),
    ("lc_ctype", "C", false),
    ("lc_messages", "C", false),
    ("lc_monetary", "C", false),
    ("lc_numeric", "C", false),
    ("lc_time", "C", false),
    ("work_mem", "4MB", false),
    ("maintenance_work_mem", "64MB", false),
    ("shared_buffers", "128MB", false),
    ("port", "5432", false),
    ("listen_addresses", "localhost", false),
    ("ssl", "off", false),
    ("password_encryption", "scram-sha-256", false),
    ("synchronous_commit", "on", false),
    ("row_security", "on", false),
    ("check_function_bodies", "on", false),
    ("xmloption", "content", false),
    ("jit", "off", false),
    ("wal_level", "replica", false),
    ("max_wal_senders", "10", false),
    ("default_tablespace", "", false),
    ("temp_tablespaces", "", false),
    ("default_text_search_config", "pg_catalog.english", false),
    ("timezone_abbreviations", "Default", false),
    ("array_nulls", "on", false),
    ("backslash_quote", "safe_encoding", false),
    ("escape_string_warning", "on", false),
    ("quote_all_identifiers", "off", false),
    ("enable_seqscan", "on", false),
    ("enable_indexscan", "on", false),
    ("log_statement", "none", false),
    ("log_min_duration_statement", "-1", false),
    ("tcp_keepalives_idle", "7200", false),
    ("track_activities", "on", false),
    ("data_directory", "/var/lib/postgresql/data", false),
    ("config_file", "/var/lib/postgresql/data/postgresql.conf", false),
    ("hba_file", "/var/lib/postgresql/data/pg_hba.conf", false),
    ("max_prepared_transactions", "0", false),
    ("max_locks_per_transaction", "64", false),
    ("wal_segment_size", "16MB", false),
    ("segment_size", "1GB", false),
    ("data_checksums", "off", false),
    ("server_version_full", "", false),
    ("cluster_name", "", false),
    ("session_replication_role", "origin", false),
    ("vacuum_cost_delay", "0", false),
    ("effective_cache_size", "4GB", false),
    ("random_page_cost", "4", false),
    ("default_statistics_target", "100", false),
    ("constraint_exclusion", "partition", false),
    ("cursor_tuple_fraction", "0.1", false),
    ("from_collapse_limit", "8", false),
    ("join_collapse_limit", "8", false),
    ("geqo", "on", false),
    ("plan_cache_mode", "auto", false),
    ("huge_pages", "try", false),
    ("max_parallel_workers_per_gather", "2", false),
    ("max_parallel_workers", "8", false),
    ("max_worker_processes", "8", false),
    ("autovacuum", "on", false),
    ("krb_caseins_users", "off", false),
    ("krb_server_keyfile", "", false),
    ("unix_socket_directories", "/var/run/postgresql", false),
    ("dynamic_shared_memory_type", "posix", false),
    ("event_triggers", "on", false),
    ("allow_system_table_mods", "off", false),
    ("gin_fuzzy_search_limit", "0", false),
    ("ignore_system_indexes", "off", false),
    ("trace_notify", "off", false),
    ("transform_null_equals", "off", false),
    ("lo_compat_privileges", "off", false),
    ("operator_precedence_warning", "off", false),
    ("enable_partition_pruning", "on", false),
];

#[derive(Clone)]
pub struct Settings {
    /// Current values by canonical name.
    values: BTreeMap<String, String>,
    /// Values at session start (RESET goes back here).
    session_defaults: BTreeMap<String, String>,
    pub zone: Zone,
}

fn canonical(name: &str) -> Option<&'static str> {
    let l = name.to_ascii_lowercase();
    DEFAULTS.iter().find(|(n, _, _)| n.to_ascii_lowercase() == l).map(|(n, _, _)| *n)
}

impl Default for Settings {
    fn default() -> Self {
        let values: BTreeMap<String, String> =
            DEFAULTS.iter().map(|(n, v, _)| (n.to_string(), v.to_string())).collect();
        let mut s = Settings { session_defaults: values.clone(), values, zone: Zone::utc() };
        s.values
            .insert("server_version_full".into(), format!("PostgreSQL {SERVER_VERSION} (noida)"));
        s.session_defaults = s.values.clone();
        s
    }
}

pub fn unrecognized(name: &str) -> PgError {
    PgError::new(code::UNDEFINED_OBJECT, format!("unrecognized configuration parameter \"{name}\""))
}

impl Settings {
    /// Names reported to the client at startup (and when changed).
    pub fn reported() -> impl Iterator<Item = &'static str> {
        DEFAULTS.iter().filter(|(_, _, r)| *r).map(|(n, _, _)| *n)
    }

    pub fn is_reported(name: &str) -> Option<&'static str> {
        let c = canonical(name)?;
        DEFAULTS.iter().any(|(n, _, r)| *n == c && *r).then_some(c)
    }

    pub fn get(&self, name: &str) -> PgResult<String> {
        let l = name.to_ascii_lowercase();
        if let Some(c) = canonical(&l) {
            return Ok(self.values.get(c).cloned().unwrap_or_default());
        }
        // Custom (dotted) settings.
        if l.contains('.') {
            return self.values.get(&l).cloned().ok_or_else(|| unrecognized(name));
        }
        if l == "all" {
            return Err(unrecognized(name));
        }
        Err(unrecognized(name))
    }

    /// Like `current_setting(name, true)`: NULL when unknown.
    pub fn get_opt(&self, name: &str) -> Option<String> {
        self.get(name).ok()
    }

    pub fn all(&self) -> Vec<(String, String)> {
        self.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Applies SET. Returns the canonical name if it must be reported.
    pub fn set(&mut self, name: &str, value: &str) -> PgResult<Option<&'static str>> {
        let l = name.to_ascii_lowercase();
        let Some(c) = canonical(&l) else {
            if l.contains('.') {
                self.values.insert(l, value.to_string());
                return Ok(None);
            }
            return Err(unrecognized(name));
        };
        let value = self.validate(c, value)?;
        self.values.insert(c.to_string(), value);
        Ok(DEFAULTS.iter().find(|(n, _, r)| *n == c && *r).map(|_| c))
    }

    /// Captures current values as the RESET target (after startup params).
    pub fn mark_session_defaults(&mut self) {
        self.session_defaults = self.values.clone();
    }

    pub fn reset(&mut self, name: &str) -> PgResult<Option<&'static str>> {
        let l = name.to_ascii_lowercase();
        if l == "all" {
            self.values = self.session_defaults.clone();
            self.zone = tz::lookup(&self.values["TimeZone"]).unwrap_or(Zone::utc());
            return Ok(None);
        }
        let c = canonical(&l).ok_or_else(|| unrecognized(name))?;
        let v = self.session_defaults.get(c).cloned().unwrap_or_default();
        self.set(c, &v)
    }

    fn validate(&mut self, name: &'static str, value: &str) -> PgResult<String> {
        let bad = |v: &str| {
            PgError::new(
                code::INVALID_PARAMETER_VALUE,
                format!("invalid value for parameter \"{name}\": \"{v}\""),
            )
        };
        let read_only = [
            "server_version",
            "server_version_num",
            "server_encoding",
            "integer_datetimes",
            "lc_collate",
            "lc_ctype",
            "max_identifier_length",
            "block_size",
            "is_superuser",
            "in_hot_standby",
            "max_connections",
            "port",
            "shared_buffers",
            "data_directory",
            "wal_level",
            "segment_size",
            "wal_segment_size",
            "data_checksums",
            "server_version_full",
        ];
        if read_only.contains(&name) {
            return Err(PgError::new(
                code::CANT_CHANGE_RUNTIME_PARAM,
                format!("parameter \"{name}\" cannot be changed"),
            ));
        }
        Ok(match name {
            "TimeZone" => {
                let z = tz::lookup(value).ok_or_else(|| bad(value))?;
                self.zone = z;
                let l = value.to_ascii_lowercase();
                if l == "utc" || l == "z" {
                    "UTC".into()
                } else if value.contains('/') {
                    tz::canonical_name(value)
                } else {
                    value.to_string()
                }
            }
            "client_encoding" => {
                let u = value.to_ascii_uppercase().replace(['-', '_'], "");
                match u.as_str() {
                    "UTF8" | "UNICODE" => "UTF8".into(),
                    "SQLASCII" => "SQL_ASCII".into(),
                    "LATIN1" => "LATIN1".into(),
                    _ => {
                        return Err(PgError::new(
                            code::FEATURE_NOT_SUPPORTED,
                            format!("conversion between {value} and UTF8 is not supported"),
                        ));
                    }
                }
            }
            "DateStyle" => {
                let v = value.to_ascii_lowercase();
                let parts: Vec<&str> = v.split(',').map(str::trim).collect();
                for p in &parts {
                    if !matches!(
                        *p,
                        "iso"
                            | "mdy"
                            | "dmy"
                            | "ymd"
                            | "us"
                            | "euro"
                            | "european"
                            | "noneuropean"
                            | "sql"
                            | "postgres"
                            | "german"
                    ) {
                        return Err(bad(value));
                    }
                }
                if parts.iter().any(|p| matches!(*p, "sql" | "postgres" | "german")) {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        "noida only supports DateStyle ISO output",
                    ));
                }
                let order = if parts.iter().any(|p| matches!(*p, "dmy" | "euro" | "european")) {
                    "DMY"
                } else if parts.contains(&"ymd") {
                    "YMD"
                } else {
                    "MDY"
                };
                format!("ISO, {order}")
            }
            "IntervalStyle" => match value.to_ascii_lowercase().as_str() {
                "postgres" => "postgres".into(),
                "iso_8601" => "iso_8601".into(),
                "sql_standard" | "postgres_verbose" => {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        format!("IntervalStyle {value} is not supported"),
                    ));
                }
                _ => return Err(bad(value)),
            },
            "extra_float_digits" => {
                let n: i32 = value.trim().parse().map_err(|_| {
                    PgError::new(
                        code::INVALID_PARAMETER_VALUE,
                        format!("invalid value for parameter \"{name}\": \"{value}\""),
                    )
                })?;
                if !(-15..=3).contains(&n) {
                    return Err(PgError::new(
                        code::INVALID_PARAMETER_VALUE,
                        format!(
                            "{n} is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)"
                        ),
                    ));
                }
                n.to_string()
            }
            "bytea_output" => match value.to_ascii_lowercase().as_str() {
                v @ ("hex" | "escape") => v.to_string(),
                _ => return Err(bad(value)),
            },
            "standard_conforming_strings" => match super::types::parse_bool(value) {
                Some(true) => "on".into(),
                Some(false) => {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        "standard_conforming_strings = off is not supported",
                    ));
                }
                None => return Err(bad(value)),
            },
            "transaction_isolation" | "default_transaction_isolation" => {
                match value.to_ascii_lowercase().as_str() {
                    v @ ("read committed" | "repeatable read" | "serializable"
                    | "read uncommitted") => v.to_string(),
                    _ => return Err(bad(value)),
                }
            }
            n if bool_setting(n) => match super::types::parse_bool(value) {
                Some(b) => if b { "on" } else { "off" }.into(),
                None => {
                    return Err(PgError::new(
                        code::INVALID_PARAMETER_VALUE,
                        format!("parameter \"{name}\" requires a Boolean value"),
                    ));
                }
            },
            _ => value.to_string(),
        })
    }

    pub fn fmt(&self) -> FmtCtx {
        FmtCtx {
            zone: self.zone.clone(),
            interval_iso: self.values.get("IntervalStyle").is_some_and(|v| v == "iso_8601"),
            bytea_escape: self.values.get("bytea_output").is_some_and(|v| v == "escape"),
            extra_float_digits: self
                .values
                .get("extra_float_digits")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
        }
    }

    /// Schemas named by search_path (with `$user` expanded, missing ones kept).
    pub fn search_path(&self, user: &str) -> Vec<String> {
        let sp = self.values.get("search_path").cloned().unwrap_or_default();
        split_path(&sp)
            .into_iter()
            .map(|s| if s == "$user" { user.to_string() } else { s })
            .collect()
    }
}

fn bool_setting(n: &str) -> bool {
    matches!(
        n,
        "default_transaction_read_only"
            | "transaction_read_only"
            | "default_transaction_deferrable"
            | "transaction_deferrable"
            | "row_security"
            | "check_function_bodies"
            | "jit"
            | "synchronous_commit"
            | "array_nulls"
            | "escape_string_warning"
            | "quote_all_identifiers"
            | "enable_seqscan"
            | "enable_indexscan"
            | "track_activities"
            | "autovacuum"
            | "transform_null_equals"
    )
}

/// Splits a search_path value, honoring double quotes.
pub fn split_path(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            ',' if !quoted => {
                let t = cur.trim();
                if !t.is_empty() || was_quoted {
                    out.push(if was_quoted {
                        cur.trim().to_string()
                    } else {
                        t.to_ascii_lowercase()
                    });
                }
                cur.clear();
                was_quoted = false;
            }
            c => cur.push(c),
        }
    }
    let t = cur.trim();
    if !t.is_empty() {
        out.push(if was_quoted { t.to_string() } else { t.to_ascii_lowercase() });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_show_reset() {
        let mut s = Settings::default();
        assert_eq!(s.get("timezone").unwrap(), "UTC");
        assert_eq!(s.set("TimeZone", "utc").unwrap(), Some("TimeZone"));
        assert_eq!(s.set("extra_float_digits", "3").unwrap(), None);
        assert_eq!(s.get("EXTRA_FLOAT_DIGITS").unwrap(), "3");
        assert_eq!(s.set("nope", "1").unwrap_err().code, code::UNDEFINED_OBJECT);
        assert_eq!(s.set("server_version", "1").unwrap_err().code, code::CANT_CHANGE_RUNTIME_PARAM);
        s.set("myapp.user_id", "42").unwrap();
        assert_eq!(s.get("myapp.user_id").unwrap(), "42");
        s.reset("extra_float_digits").unwrap();
        assert_eq!(s.get("extra_float_digits").unwrap(), "1");
        assert_eq!(s.set("DateStyle", "iso, dmy").unwrap(), Some("DateStyle"));
        assert_eq!(s.get("datestyle").unwrap(), "ISO, DMY");
    }

    #[test]
    fn search_path_split() {
        assert_eq!(split_path("\"$user\", public"), vec!["$user", "public"]);
        assert_eq!(split_path("Foo,\"Bar\""), vec!["foo", "Bar"]);
    }
}
