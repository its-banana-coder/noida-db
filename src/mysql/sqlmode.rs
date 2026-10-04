//! A session's `sql_mode`: how strictly writes are checked. MySQL 8's
//! default is strict; apps such as WordPress turn strict mode off for their
//! sessions and rely on MySQL's lenient conversions instead (clamping
//! out-of-range numbers, truncating long strings, zero dates). Both have to
//! behave like MySQL, or an integration test can pass for the wrong reason.

/// MySQL 8.0's default `sql_mode`.
pub const DEFAULT: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

#[derive(Clone, Debug, PartialEq)]
pub struct SqlMode {
    modes: Vec<String>,
}

impl Default for SqlMode {
    fn default() -> Self {
        Self::parse(DEFAULT)
    }
}

impl SqlMode {
    /// Parses a `SET sql_mode = '...'` value, expanding the combination
    /// modes the way MySQL does (`@@sql_mode` shows the expanded list).
    pub fn parse(s: &str) -> Self {
        let mut modes: Vec<String> = Vec::new();
        let mut add = |m: &str| {
            if !m.is_empty() && !modes.iter().any(|x| x == m) {
                modes.push(m.to_string());
            }
        };
        for m in s.split(',').map(|m| m.trim().to_ascii_uppercase()) {
            match m.as_str() {
                "TRADITIONAL" => {
                    for x in [
                        "STRICT_TRANS_TABLES",
                        "STRICT_ALL_TABLES",
                        "NO_ZERO_IN_DATE",
                        "NO_ZERO_DATE",
                        "ERROR_FOR_DIVISION_BY_ZERO",
                        "NO_ENGINE_SUBSTITUTION",
                    ] {
                        add(x);
                    }
                }
                "ANSI" => {
                    for x in [
                        "REAL_AS_FLOAT",
                        "PIPES_AS_CONCAT",
                        "ANSI_QUOTES",
                        "IGNORE_SPACE",
                        "ONLY_FULL_GROUP_BY",
                    ] {
                        add(x);
                    }
                }
                other => add(other),
            }
        }
        Self { modes }
    }

    fn has(&self, m: &str) -> bool {
        self.modes.iter().any(|x| x == m)
    }

    /// Strict mode: invalid or out-of-range values are errors, not
    /// adjusted with a warning.
    pub fn strict(&self) -> bool {
        self.has("STRICT_TRANS_TABLES") || self.has("STRICT_ALL_TABLES")
    }

    /// `'0000-00-00'` is rejected (strict) rather than stored.
    pub fn no_zero_date(&self) -> bool {
        self.has("NO_ZERO_DATE")
    }

    /// Division by zero in an INSERT/UPDATE is an error (strict) rather
    /// than NULL.
    pub fn error_for_division_by_zero(&self) -> bool {
        self.has("ERROR_FOR_DIVISION_BY_ZERO")
    }

    /// Non-aggregated columns must be functionally dependent on GROUP BY.
    pub fn only_full_group_by(&self) -> bool {
        self.has("ONLY_FULL_GROUP_BY")
    }

    pub fn as_str(&self) -> String {
        self.modes.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_strict_and_wordpress_style_modes_are_not() {
        let d = SqlMode::default();
        assert!(d.strict() && d.no_zero_date() && d.error_for_division_by_zero());
        assert_eq!(d.as_str(), DEFAULT);
        // WordPress removes the modes it can't work with.
        let wp = SqlMode::parse("NO_ENGINE_SUBSTITUTION");
        assert!(!wp.strict() && !wp.no_zero_date());
        let t = SqlMode::parse("traditional");
        assert!(t.strict() && t.as_str().contains("STRICT_ALL_TABLES"));
        assert_eq!(SqlMode::parse("").as_str(), "");
    }
}
