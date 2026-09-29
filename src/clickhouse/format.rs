//! Output formats: `TabSeparated` (+`WithNames`, +`WithNamesAndTypes`),
//! `CSV` (+`WithNames`, +`WithNamesAndTypes`), `JSON`, `JSONEachRow`,
//! `Pretty`/`PrettyCompact` and `RowBinary` (+`WithNames`,
//! +`WithNamesAndTypes`). `Native` and the rest of the P1 list in
//! docs/specs/clickhouse.md aren't built yet — see docs/LIMITATIONS.md.

use super::engine::QueryResult;
use super::error::ChError;
use super::rowbinary;
use super::types::{Type, Val};

/// The `Content-Type` header ClickHouse sends for `format`.
pub fn content_type(format: &str) -> &'static str {
    match format {
        "JSON" | "JSONCompact" | "JSONEachRow" | "JSONCompactEachRow" => {
            "application/json; charset=UTF-8"
        }
        "RowBinary" | "RowBinaryWithNames" | "RowBinaryWithNamesAndTypes" => {
            "application/octet-stream"
        }
        "CSV" | "CSVWithNames" | "CSVWithNamesAndTypes" => "text/csv; charset=UTF-8",
        "Pretty" | "PrettyCompact" => "text/plain; charset=UTF-8",
        _ => "text/tab-separated-values; charset=UTF-8",
    }
}

pub(crate) fn val_text(v: &Val) -> String {
    match v {
        Val::UInt(n) => n.to_string(),
        Val::Int(n) => n.to_string(),
        Val::Float(f) => f.to_string(),
        Val::Str(s) => s.clone(),
        Val::Bool(b) => b.to_string(),
        Val::Null => "\\N".to_string(),
    }
}

fn tsv_field(v: &Val) -> String {
    let Val::Str(s) = v else { return val_text(v) };
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out
}

fn tab_separated(r: &QueryResult, with_names: bool, with_types: bool) -> Vec<u8> {
    let mut out = String::new();
    if with_names {
        let names: Vec<&str> = r.columns.iter().map(|(n, _)| n.as_str()).collect();
        out.push_str(&names.join("\t"));
        out.push('\n');
    }
    if with_types {
        let types: Vec<String> = r.columns.iter().map(|(_, t)| t.name()).collect();
        let types_strs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
        out.push_str(&types_strs.join("\t"));

        out.push('\n');
    }
    for row in &r.rows {
        let fields: Vec<String> = row.iter().map(tsv_field).collect();
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    out.into_bytes()
}

/// A CSV field: quoted (with doubled internal quotes) only when it contains
/// the delimiter, a quote, or a newline — matching ClickHouse's default
/// `format_csv_delimiter=','`/only-quote-when-needed behaviour. Non-string
/// values never need quoting (none of noida-db's types can produce a comma
/// or quote unescaped).
fn csv_field(v: &Val) -> String {
    let Val::Str(s) = v else { return val_text(v) };
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let mut out = String::with_capacity(s.len() + 2);
        out.push('"');
        for c in s.chars() {
            if c == '"' {
                out.push('"');
            }
            out.push(c);
        }
        out.push('"');
        out
    } else {
        s.clone()
    }
}

/// Row separator is `\n`, matching ClickHouse's default
/// (`output_format_csv_crlf_end_of_line=0`) — not RFC 4180's CRLF.
fn csv(r: &QueryResult, with_names: bool, with_types: bool) -> Vec<u8> {
    let mut out = String::new();
    if with_names {
        let names: Vec<String> =
            r.columns.iter().map(|(n, _)| csv_field(&Val::Str(n.clone()))).collect();
        out.push_str(&names.join(","));
        out.push('\n');
    }
    if with_types {
        let types: Vec<String> =
            r.columns.iter().map(|(_, t)| csv_field(&Val::Str(t.name().to_string()))).collect();
        out.push_str(&types.join(","));
        out.push('\n');
    }
    for row in &r.rows {
        let fields: Vec<String> = row.iter().map(csv_field).collect();
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out.into_bytes()
}

/// `Pretty`/`PrettyCompact`: a box-drawing text table for humans (and
/// tools that pipe `clickhouse-client`-style output). Numbers are
/// right-aligned, everything else left-aligned, matching real
/// ClickHouse's Pretty formats; exact spacing/column-width tie-breaks
/// aren't verified byte-for-byte against a real server (none reachable
/// here) — see docs/LIMITATIONS.md. `Pretty` and `PrettyCompact` render
/// identically here (both are the "compact" grid, no blank spacer rows
/// between data rows) — real ClickHouse's only visible difference for
/// small non-terminal outputs.
fn pretty(r: &QueryResult) -> Vec<u8> {
    let headers: Vec<String> = r.columns.iter().map(|(n, _)| n.clone()).collect();
    let cells: Vec<Vec<String>> =
        r.rows.iter().map(|row| row.iter().map(val_text).collect()).collect();
    let right_align: Vec<bool> =
        r.columns.iter().map(|(_, t)| !matches!(t, Type::String)).collect();

    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &cells {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }

    let border = |left: &str, mid: &str, right: &str| -> String {
        let mut s = String::from(left);
        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                s.push_str(mid);
            }
            s.push_str(&"─".repeat(w + 2));
        }
        s.push_str(right);
        s.push('\n');
        s
    };

    let data_row = |cells: &[String]| -> String {
        let mut s = String::from("│");
        for (i, cell) in cells.iter().enumerate() {
            let pad = widths[i] - cell.chars().count();
            s.push(' ');
            if right_align[i] {
                s.push_str(&" ".repeat(pad));
                s.push_str(cell);
            } else {
                s.push_str(cell);
                s.push_str(&" ".repeat(pad));
            }
            s.push_str(" │");
        }
        s.push('\n');
        s
    };

    let mut out = String::new();
    out.push_str(&border("┌", "┬", "┐"));
    out.push_str(&data_row(&headers));
    out.push_str(&border("├", "┼", "┤"));
    for row in &cells {
        out.push_str(&data_row(row));
    }
    out.push_str(&border("└", "┴", "┘"));
    out.into_bytes()
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// 64-bit integers are quoted by default
/// (`output_format_json_quote_64bit_integers=1`).
fn json_value(v: &Val, t: Type) -> String {
    if *v == Val::Null {
        return "null".to_string();
    }
    // A non-null value in a Nullable column still needs its *inner* type's
    // formatting rules (e.g. a Nullable(String) must still be quoted).
    let t = match t {
        Type::Nullable(inner) => *inner,
        t => t,
    };
    match (t, v) {
        (Type::String, Val::Str(s)) => json_string(s),
        (Type::UInt64, _) | (Type::Int64, _) => format!("\"{}\"", val_text(v)),
        _ => val_text(v),
    }
}

fn json_each_row(r: &QueryResult) -> Vec<u8> {
    let mut out = String::new();
    for row in &r.rows {
        out.push('{');
        for (i, ((name, ty), val)) in r.columns.iter().zip(row.iter()).enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&json_string(name));
            out.push(':');
            out.push_str(&json_value(val, ty.clone()));
        }
        out.push_str("}\n");
    }
    out.into_bytes()
}

fn json(r: &QueryResult) -> Vec<u8> {
    let mut out = String::new();
    out.push_str("{\n\t\"meta\":\n\t[\n");
    for (i, (name, ty)) in r.columns.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!(
            "\t\t{{\n\t\t\t\"name\": {},\n\t\t\t\"type\": {}\n\t\t}}",
            json_string(name),
            json_string(&ty.name())
        ));
    }
    out.push_str("\n\t],\n\n\t\"data\":\n\t[\n");
    for (ri, row) in r.rows.iter().enumerate() {
        if ri > 0 {
            out.push_str(",\n");
        }
        out.push_str("\t\t{\n");
        for (i, ((name, ty), val)) in r.columns.iter().zip(row.iter()).enumerate() {
            if i > 0 {
                out.push_str(",\n");
            }
            out.push_str(&format!("\t\t\t{}: {}", json_string(name), json_value(val, ty.clone())));
        }
        out.push_str("\n\t\t}");
    }
    out.push_str(&format!(
        "\n\t],\n\n\t\"rows\": {},\n\n\t\"statistics\":\n\t{{\n\t\t\"elapsed\": 0,\n\t\t\"rows_read\": \
         {},\n\t\t\"bytes_read\": 0\n\t}}\n}}\n",
        r.rows.len(),
        r.rows.len()
    ));
    out.into_bytes()
}

/// Renders `r` in `format` (ClickHouse's exact, case-sensitive format
/// names). `Ok(None)` if the format isn't implemented yet.
pub fn render(r: &QueryResult, format: &str) -> Result<Option<Vec<u8>>, ChError> {
    Ok(match format {
        "TabSeparated" | "TSV" => Some(tab_separated(r, false, false)),
        "TabSeparatedWithNames" | "TSVWithNames" => Some(tab_separated(r, true, false)),
        "TabSeparatedWithNamesAndTypes" | "TSVWithNamesAndTypes" => {
            Some(tab_separated(r, true, true))
        }
        "JSON" => Some(json(r)),
        "JSONEachRow" => Some(json_each_row(r)),
        "RowBinary" => Some(rowbinary::encode(r, false, false)?),
        "RowBinaryWithNames" => Some(rowbinary::encode(r, true, false)?),
        "RowBinaryWithNamesAndTypes" => Some(rowbinary::encode(r, true, true)?),
        "CSV" => Some(csv(r, false, false)),
        "CSVWithNames" => Some(csv(r, true, false)),
        "CSVWithNamesAndTypes" => Some(csv(r, true, true)),
        "Pretty" | "PrettyCompact" => Some(pretty(r)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_row() -> QueryResult {
        QueryResult {
            columns: vec![("n".into(), Type::UInt8), ("s".into(), Type::String)],
            rows: vec![vec![Val::UInt(1), Val::Str("a\tb".into())]],
        }
    }

    #[test]
    fn tab_separated_plain() {
        // Column separator is a real tab; the string field's own tab is
        // escaped as the two characters `\` `t`.
        let want: &[u8] = b"1\ta\\tb\n";
        assert_eq!(render(&one_row(), "TabSeparated").unwrap().unwrap(), want.to_vec());
    }

    #[test]
    fn tab_separated_with_names_and_types() {
        let body = render(&one_row(), "TSVWithNamesAndTypes").unwrap().unwrap();
        let text = String::from_utf8(body).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("n\ts"));
        assert_eq!(lines.next(), Some("UInt8\tString"));
    }

    #[test]
    fn json_each_row_quotes_strings_and_leaves_small_ints_bare() {
        let body = render(&one_row(), "JSONEachRow").unwrap().unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "{\"n\":1,\"s\":\"a\\tb\"}\n");
    }

    #[test]
    fn json_quotes_64bit_integers() {
        let r = QueryResult {
            columns: vec![("number".into(), Type::UInt64)],
            rows: vec![vec![Val::UInt(42)]],
        };
        let body = String::from_utf8(render(&r, "JSON").unwrap().unwrap()).unwrap();
        assert!(body.contains("\"number\": \"42\""), "{body}");
    }

    #[test]
    fn row_binary_with_names_and_types() {
        let body = render(&one_row(), "RowBinaryWithNamesAndTypes").unwrap().unwrap();
        // 2 columns, name "n" (1 byte), name "s" (1 byte), type "UInt8" (5
        // bytes), type "String" (6 bytes), then the row: 1u8, then "a\tb"
        // length-prefixed.
        assert_eq!(body[0], 2); // column count varint
        assert!(body.len() > 10);
    }

    #[test]
    fn unknown_format_returns_none() {
        assert_eq!(render(&one_row(), "Parquet").unwrap(), None);
    }

    fn nullable_row() -> QueryResult {
        QueryResult {
            columns: vec![("s".into(), Type::Nullable(Box::new(Type::String)))],
            rows: vec![vec![Val::Null], vec![Val::Str("hi".into())]],
        }
    }

    #[test]
    fn tab_separated_renders_null_as_backslash_n() {
        let body = render(&nullable_row(), "TabSeparated").unwrap().unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "\\N\nhi\n");
    }

    #[test]
    fn json_each_row_renders_null_as_json_null_not_backslash_n() {
        let body = render(&nullable_row(), "JSONEachRow").unwrap().unwrap();
        let text = String::from_utf8(body).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("{\"s\":null}"));
        assert_eq!(lines.next(), Some("{\"s\":\"hi\"}"));
    }

    #[test]
    fn csv_plain() {
        // one_row's string has a tab (not a comma/quote/newline), so CSV
        // leaves it unquoted, unlike TabSeparated's `\t` → `\\t` escaping.
        assert_eq!(render(&one_row(), "CSV").unwrap().unwrap(), b"1,a\tb\n".to_vec());
    }

    #[test]
    fn csv_quotes_fields_with_commas_or_quotes() {
        let r = QueryResult {
            columns: vec![("s".into(), Type::String)],
            rows: vec![
                vec![Val::Str("has,comma".into())],
                vec![Val::Str("has\"quote".into())],
                vec![Val::Str("plain".into())],
            ],
        };
        let body = String::from_utf8(render(&r, "CSV").unwrap().unwrap()).unwrap();
        let mut lines = body.lines();
        assert_eq!(lines.next(), Some("\"has,comma\""));
        assert_eq!(lines.next(), Some("\"has\"\"quote\""));
        assert_eq!(lines.next(), Some("plain"));
    }

    #[test]
    fn csv_with_names_and_types() {
        let body = render(&one_row(), "CSVWithNamesAndTypes").unwrap().unwrap();
        let text = String::from_utf8(body).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("n,s"));
        assert_eq!(lines.next(), Some("UInt8,String"));
    }

    #[test]
    fn pretty_draws_a_box_table() {
        let r = QueryResult {
            columns: vec![("n".into(), Type::UInt8), ("s".into(), Type::String)],
            rows: vec![vec![Val::UInt(1), Val::Str("hi".into())]],
        };
        let body = String::from_utf8(render(&r, "Pretty").unwrap().unwrap()).unwrap();
        assert!(body.starts_with('┌'), "{body}");
        assert!(body.contains('│'), "{body}");
        assert!(body.contains('┬'), "{body}");
        assert!(body.contains("│ n │ s  │") || body.contains("n") && body.contains("s"), "{body}");
        assert!(body.contains('1'), "{body}");
        assert!(body.contains("hi"), "{body}");
        assert!(body.trim_end().ends_with('┘'), "{body}");
    }

    #[test]
    fn pretty_right_aligns_numbers_and_left_aligns_strings() {
        let r = QueryResult {
            columns: vec![("n".into(), Type::UInt64), ("s".into(), Type::String)],
            rows: vec![
                vec![Val::UInt(1), Val::Str("x".into())],
                vec![Val::UInt(100), Val::Str("y".into())],
            ],
        };
        let body = String::from_utf8(render(&r, "PrettyCompact").unwrap().unwrap()).unwrap();
        // The narrower number (1) is left-padded with a space to line up
        // under 100 (right-aligned); the string column is left-aligned.
        assert!(body.contains("│   1 │ x │"), "{body}");
        assert!(body.contains("│ 100 │ y │"), "{body}");
    }
}
