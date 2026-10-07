//! Per-command and per-error counters for INFO commandstats / errorstats
//! and the Stats section's `total_commands_processed` /
//! `total_error_replies` (Redis 7.2's shapes; CONFIG RESETSTAT clears them).

use std::collections::BTreeMap;

#[derive(Default)]
pub struct CmdStat {
    pub calls: u64,
    pub usec: u64,
    /// Refused before running (arity, auth, OOM, ...).
    pub rejected: u64,
    /// Ran and replied with an error.
    pub failed: u64,
}

#[derive(Default)]
pub struct Stats {
    /// By full name ("get", "client|list").
    pub commands: BTreeMap<String, CmdStat>,
    /// By error code (the error's first word).
    pub errors: BTreeMap<String, u64>,
    pub commands_processed: u64,
    pub error_replies: u64,
}

impl Stats {
    pub fn call(&mut self, name: &str, usec: u64, failed: bool) {
        let s = self.commands.entry(name.to_string()).or_default();
        s.calls += 1;
        s.usec += usec;
        if failed {
            s.failed += 1;
        }
        self.commands_processed += 1;
    }

    pub fn reject(&mut self, name: &str) {
        self.commands.entry(name.to_string()).or_default().rejected += 1;
        self.commands_processed += 1;
    }

    /// An error reply sent to a client.
    pub fn error(&mut self, msg: &str) {
        let code = msg.split(' ').next().unwrap_or("ERR");
        *self.errors.entry(code.to_string()).or_default() += 1;
        self.error_replies += 1;
    }

    /// INFO commandstats lines (name, value).
    pub fn command_lines(&self) -> Vec<(String, String)> {
        self.commands
            .iter()
            .filter(|(_, s)| s.calls > 0 || s.rejected > 0 || s.failed > 0)
            .map(|(name, s)| {
                let per_call = if s.calls == 0 { 0.0 } else { s.usec as f64 / s.calls as f64 };
                (
                    format!("cmdstat_{name}"),
                    format!(
                        "calls={},usec={},usec_per_call={per_call:.2},rejected_calls={},failed_calls={}",
                        s.calls, s.usec, s.rejected, s.failed
                    ),
                )
            })
            .collect()
    }

    /// INFO errorstats lines.
    pub fn error_lines(&self) -> Vec<(String, String)> {
        self.errors
            .iter()
            .map(|(code, n)| (format!("errorstat_{code}"), format!("count={n}")))
            .collect()
    }
}
