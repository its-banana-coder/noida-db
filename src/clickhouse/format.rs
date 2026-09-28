//! Output formats: `TabSeparated` (+`WithNames`, +`WithNamesAndTypes`),
//! `JSON` and `JSONEachRow`. The rest of the P0 list in
//! docs/specs/clickhouse.md (`CSV`, `RowBinary`, `Native`, ...) isn't built
//! yet — see docs/LIMITATIONS.md.

use super::engine::QueryResult;
use super::types::{Type, Val};

/// The `Content-Type` header ClickHouse sends for `format`.
pub fn content_type(format: &str) -> &'static str {
    match format {
        "JSON" | "JSONCompact" | "JSONEachRow" | "JSONCompactEachRow" => {
            "application/json; charset=UTF-8"
        }
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
        let types: Vec<&str> = r.columns.iter().map(|(_, t)| t.name()).collect();
        out.push_str(&types.join("\t"));
        out.push('\n');
    }
    for row in &r.rows {
        let fields: Vec<String> = row.iter().map(tsv_field).collect();
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
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
            out.push_str(&json_value(val, *ty));
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
            json_string(ty.name())
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
            out.push_str(&format!("\t\t\t{}: {}", json_string(name), json_value(val, *ty)));
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
/// names). `None` if the format isn't implemented yet.
pub fn render(r: &QueryResult, format: &str) -> Option<Vec<u8>> {
    match format {
        "TabSeparated" | "TSV" => Some(tab_separated(r, false, false)),
        "TabSeparatedWithNames" | "TSVWithNames" => Some(tab_separated(r, true, false)),
        "TabSeparatedWithNamesAndTypes" | "TSVWithNamesAndTypes" => {
            Some(tab_separated(r, true, true))
        }
        "JSON" => Some(json(r)),
        "JSONEachRow" => Some(json_each_row(r)),
        _ => None,
    }
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
        assert_eq!(render(&one_row(), "TabSeparated").unwrap(), want.to_vec());
    }

    #[test]
    fn tab_separated_with_names_and_types() {
        let body = render(&one_row(), "TSVWithNamesAndTypes").unwrap();
        let text = String::from_utf8(body).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("n\ts"));
        assert_eq!(lines.next(), Some("UInt8\tString"));
    }

    #[test]
    fn json_each_row_quotes_strings_and_leaves_small_ints_bare() {
        let body = render(&one_row(), "JSONEachRow").unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "{\"n\":1,\"s\":\"a\\tb\"}\n");
    }

    #[test]
    fn json_quotes_64bit_integers() {
        let r = QueryResult {
            columns: vec![("number".into(), Type::UInt64)],
            rows: vec![vec![Val::UInt(42)]],
        };
        let body = String::from_utf8(render(&r, "JSON").unwrap()).unwrap();
        assert!(body.contains("\"number\": \"42\""), "{body}");
    }

    #[test]
    fn unknown_format_returns_none() {
        assert_eq!(render(&one_row(), "Parquet"), None);
    }
}
