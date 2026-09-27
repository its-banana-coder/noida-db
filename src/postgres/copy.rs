//! `COPY ... FROM/TO STDIN/STDOUT`: the text and CSV row formats.
//!
//! `COPY` is implemented by turning the whole thing into ordinary SQL that
//! already goes through the engine's normal path — a `COPY t FROM STDIN` is
//! rewritten into batches of `INSERT INTO t VALUES (...)` (so every
//! default, constraint, generated column and sequence behaves exactly as it
//! does for a real `INSERT`), and `COPY t TO STDOUT` runs `SELECT * FROM t`
//! and formats the rows it gets back. This module only has the row codec;
//! `server.rs` drives the interactive CopyData exchange.
//!
//! `FORMAT BINARY` is not implemented (a clear error, not a silent one): see
//! docs/LIMITATIONS.md.

use sqlparser::ast as a;

use super::error::{PgError, PgResult, code};
use super::types::{FmtCtx, Type, Value, to_text};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Format {
    Text,
    Csv,
}

/// The `WITH (...)` / legacy options that shape a `COPY`'s row format.
#[derive(Clone, Debug)]
pub struct CopySpec {
    pub format: Format,
    pub delimiter: char,
    /// The exact text that denotes SQL NULL (`\N` for text, `""` for csv).
    pub null: String,
    pub quote: char,
    pub escape: char,
    pub header: bool,
}

impl Default for CopySpec {
    fn default() -> Self {
        CopySpec {
            format: Format::Text,
            delimiter: '\t',
            null: "\\N".into(),
            quote: '"',
            escape: '"',
            header: false,
        }
    }
}

/// Reads a `COPY`'s options into a [`CopySpec`], erroring on anything this
/// doesn't implement (`FORMAT BINARY`) rather than guessing.
pub fn spec_from_options(
    options: &[a::CopyOption],
    legacy: &[a::CopyLegacyOption],
) -> PgResult<CopySpec> {
    let mut s = CopySpec::default();
    let (mut delimiter, mut null) = (None, None);
    for o in legacy {
        if matches!(o, a::CopyLegacyOption::Binary) {
            return Err(unsupported_binary());
        }
    }
    for o in options {
        match o {
            a::CopyOption::Format(name) => {
                s.format = match name.value.to_ascii_lowercase().as_str() {
                    "text" => Format::Text,
                    "csv" => Format::Csv,
                    "binary" => return Err(unsupported_binary()),
                    other => {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            format!("COPY format \"{other}\" not recognized"),
                        ));
                    }
                };
            }
            a::CopyOption::Delimiter(c) => delimiter = Some(*c),
            a::CopyOption::Null(n) => null = Some(n.clone()),
            a::CopyOption::Header(h) => s.header = *h,
            a::CopyOption::Quote(c) => s.quote = *c,
            a::CopyOption::Escape(c) => s.escape = *c,
            _ => {}
        }
    }
    // CSV's own defaults (comma, empty-string null) differ from text's, but
    // only when the option wasn't given explicitly.
    if s.format == Format::Csv {
        s.delimiter = delimiter.unwrap_or(',');
        s.null = null.unwrap_or_default();
    } else {
        s.delimiter = delimiter.unwrap_or('\t');
        s.null = null.unwrap_or_else(|| "\\N".into());
    }
    Ok(s)
}

fn unsupported_binary() -> PgError {
    PgError::new(code::FEATURE_NOT_SUPPORTED, "COPY ... (FORMAT BINARY) is not supported")
}

/// Splits one line of COPY input (no trailing newline) into fields, each
/// `None` for SQL NULL.
pub fn decode_line(line: &str, spec: &CopySpec) -> PgResult<Vec<Option<String>>> {
    match spec.format {
        Format::Text => {
            Ok(line.split(spec.delimiter).map(|f| decode_text_field(f, spec)).collect())
        }
        Format::Csv => decode_csv_line(line, spec),
    }
}

fn decode_text_field(raw: &str, spec: &CopySpec) -> Option<String> {
    if raw == spec.null {
        return None;
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('v') => out.push('\u{b}'),
            Some(d @ '0'..='7') => {
                let mut n = d.to_digit(8).unwrap();
                for _ in 0..2 {
                    let Some(&next) = chars.peek() else { break };
                    let Some(v) = next.to_digit(8) else { break };
                    n = n * 8 + v;
                    chars.next();
                }
                out.push(n as u8 as char);
            }
            Some(other) => out.push(other),
            None => {}
        }
    }
    Some(out)
}

fn decode_csv_line(line: &str, spec: &CopySpec) -> PgResult<Vec<Option<String>>> {
    let mut fields = vec![];
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == spec.escape && spec.escape != spec.quote && chars.peek() == Some(&spec.quote) {
                cur.push(chars.next().unwrap());
            } else if c == spec.quote {
                if chars.peek() == Some(&spec.quote) {
                    cur.push(chars.next().unwrap());
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == spec.quote && cur.is_empty() {
            in_quotes = true;
            quoted = true;
        } else if c == spec.delimiter {
            fields.push(if !quoted && cur == spec.null {
                None
            } else {
                Some(std::mem::take(&mut cur))
            });
            quoted = false;
        } else {
            cur.push(c);
        }
    }
    if in_quotes {
        return Err(PgError::new(code::BAD_COPY_FILE_FORMAT, "unterminated CSV quoted field"));
    }
    fields.push(if !quoted && cur == spec.null { None } else { Some(cur) });
    Ok(fields)
}

/// One row as a line of COPY output, with its trailing newline.
pub fn encode_row(vals: &[Value], tys: &[Type], spec: &CopySpec, fmt: &FmtCtx) -> String {
    let mut line = String::new();
    for (i, (v, ty)) in vals.iter().zip(tys).enumerate() {
        if i > 0 {
            line.push(spec.delimiter);
        }
        if v.is_null() {
            line.push_str(&spec.null);
            continue;
        }
        let text = to_text(v, *ty, fmt);
        match spec.format {
            Format::Text => encode_text_field(&text, spec, &mut line),
            Format::Csv => encode_csv_field(&text, spec, &mut line),
        }
    }
    line.push('\n');
    line
}

fn encode_text_field(text: &str, spec: &CopySpec, out: &mut String) {
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == spec.delimiter => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
}

fn encode_csv_field(text: &str, spec: &CopySpec, out: &mut String) {
    let needs_quote = text.is_empty() && spec.null.is_empty()
        || text.contains(spec.delimiter)
        || text.contains(spec.quote)
        || text.contains('\n')
        || text.contains('\r');
    if !needs_quote {
        out.push_str(text);
        return;
    }
    out.push(spec.quote);
    for c in text.chars() {
        if c == spec.quote {
            out.push(spec.escape);
        }
        out.push(c);
    }
    out.push(spec.quote);
}

/// A csv header line naming `cols`.
pub fn header_row(cols: &[String], spec: &CopySpec) -> String {
    let mut line = String::new();
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            line.push(spec.delimiter);
        }
        encode_csv_field(c, spec, &mut line);
    }
    line.push('\n');
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> CopySpec {
        CopySpec::default()
    }

    #[test]
    fn text_roundtrip() {
        let s = spec();
        // A backslash before a character with no special meaning, per
        // Postgres's text-format docs, represents that character alone —
        // confirmed against a real server: `\N` outside a whole field is
        // not the NULL marker, and unescapes to a bare "N".
        let fields = decode_line("a\\tb\tNULL is \\N here\t\\N", &s).unwrap();
        assert_eq!(fields, vec![Some("a\tb".into()), Some("NULL is N here".into()), None]);
    }

    #[test]
    fn text_escapes() {
        let s = spec();
        assert_eq!(decode_text_field("a\\nb\\\\c\\x", &s), Some("a\nb\\cx".into()));
        assert_eq!(decode_text_field("\\101", &s), Some("A".into()));
    }

    #[test]
    fn csv_quoting_and_null() {
        let mut s = spec();
        s.format = Format::Csv;
        s.delimiter = ',';
        s.null = String::new();
        let fields = decode_line("a,\"b,c\",\"say \"\"hi\"\"\",,\"\"", &s).unwrap();
        assert_eq!(
            fields,
            vec![
                Some("a".into()),
                Some("b,c".into()),
                Some("say \"hi\"".into()),
                None,
                Some("".into()),
            ]
        );
    }

    #[test]
    fn csv_format_defaults_to_comma_and_empty_null() {
        let opts = [a::CopyOption::Format(a::Ident::new("csv"))];
        let s = spec_from_options(&opts, &[]).unwrap();
        assert_eq!((s.delimiter, s.null.as_str()), (',', ""));

        // An explicit option still wins over the format's own default.
        let opts = [a::CopyOption::Format(a::Ident::new("csv")), a::CopyOption::Delimiter(';')];
        let s = spec_from_options(&opts, &[]).unwrap();
        assert_eq!(s.delimiter, ';');
    }

    #[test]
    fn csv_unterminated_quote_errors() {
        let mut s = spec();
        s.format = Format::Csv;
        s.delimiter = ',';
        assert!(decode_line("\"unterminated", &s).is_err());
    }

    #[test]
    fn encode_text_escapes_specials() {
        let s = spec();
        let fmt = FmtCtx::default();
        let line = encode_row(
            &[Value::text("a\tb\nc\\d"), Value::Null],
            &[Type::TEXT, Type::TEXT],
            &s,
            &fmt,
        );
        assert_eq!(line, "a\\tb\\nc\\\\d\t\\N\n");
    }

    #[test]
    fn encode_csv_quotes_when_needed() {
        let mut s = spec();
        s.format = Format::Csv;
        s.delimiter = ',';
        s.null = String::new();
        let fmt = FmtCtx::default();
        let line =
            encode_row(&[Value::text("a,b"), Value::Null], &[Type::TEXT, Type::TEXT], &s, &fmt);
        assert_eq!(line, "\"a,b\",\n");
    }
}
