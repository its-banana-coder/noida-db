//! Collations: the built-in ones (with Postgres's oids), plus
//! `CREATE COLLATION`. Text compares bytewise (as the C collation)
//! except under a nondeterministic case-insensitive ICU collation
//! (`locale = 'und-u-ks-level2'`), where equality ignores case.

use serde::{Deserialize, Serialize};

use super::catalog::DbState;
use super::error::{PgError, PgResult, code};

/// A `CREATE COLLATION`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Collation {
    pub oid: u32,
    pub schema: u32,
    /// 'c' (libc), 'i' (icu), 'd' (default).
    pub provider: char,
    pub deterministic: bool,
    pub locale: String,
}

/// What a name resolves to.
#[derive(Clone, Debug, PartialEq)]
pub struct Info {
    pub oid: u32,
    pub name: String,
    pub provider: char,
    pub deterministic: bool,
    pub locale: String,
}

/// Built-in collations: (oid, name, provider, locale), as Postgres 16
/// numbers them.
pub const BUILTIN: &[(u32, &str, char, &str)] = &[
    (100, "default", 'd', ""),
    (950, "C", 'c', "C"),
    (951, "POSIX", 'c', "POSIX"),
    (962, "ucs_basic", 'c', "C"),
    (12344, "C.utf8", 'c', "C.utf8"),
    (12345, "und-x-icu", 'i', "und"),
    (12355, "ar-x-icu", 'i', "ar"),
    (12441, "cs-x-icu", 'i', "cs"),
    (12445, "da-x-icu", 'i', "da"),
    (12450, "de-x-icu", 'i', "de"),
    (12475, "el-x-icu", 'i', "el"),
    (12478, "en-x-icu", 'i', "en"),
    (12510, "en-GB-x-icu", 'i', "en-GB"),
    (12575, "en-US-x-icu", 'i', "en-US"),
    (12587, "es-x-icu", 'i', "es"),
    (12652, "fi-x-icu", 'i', "fi"),
    (12659, "fr-x-icu", 'i', "fr"),
    (12733, "he-x-icu", 'i', "he"),
    (12735, "hi-x-icu", 'i', "hi"),
    (12756, "it-x-icu", 'i', "it"),
    (12761, "ja-x-icu", 'i', "ja"),
    (12795, "ko-x-icu", 'i', "ko"),
    (12886, "nb-x-icu", 'i', "nb"),
    (12894, "nl-x-icu", 'i', "nl"),
    (12928, "pl-x-icu", 'i', "pl"),
    (12933, "pt-x-icu", 'i', "pt"),
    (12935, "pt-BR-x-icu", 'i', "pt-BR"),
    (12959, "ru-x-icu", 'i', "ru"),
    (13036, "sv-x-icu", 'i', "sv"),
    (13066, "tr-x-icu", 'i', "tr"),
    (13077, "uk-x-icu", 'i', "uk"),
    (13120, "zh-x-icu", 'i', "zh"),
];

pub fn lookup(db: &DbState, name: &str) -> Option<Info> {
    if let Some(c) = db.collations.get(name) {
        return Some(Info {
            oid: c.oid,
            name: name.to_string(),
            provider: c.provider,
            deterministic: c.deterministic,
            locale: c.locale.clone(),
        });
    }
    BUILTIN.iter().find(|(_, n, ..)| *n == name).map(|(oid, n, p, l)| Info {
        oid: *oid,
        name: n.to_string(),
        provider: *p,
        deterministic: true,
        locale: l.to_string(),
    })
}

/// `collation "x" for encoding "UTF8" does not exist`.
pub fn not_found(name: &str) -> PgError {
    PgError::new(
        code::UNDEFINED_OBJECT,
        format!("collation \"{name}\" for encoding \"UTF8\" does not exist"),
    )
}

pub fn resolve(db: &DbState, name: &str) -> PgResult<Info> {
    lookup(db, name).ok_or_else(|| not_found(name))
}

/// Equality under this collation ignores case: a nondeterministic ICU
/// collation at strength 1 or 2 (`-ks-level1` / `-ks-level2`).
pub fn case_insensitive(info: &Info) -> bool {
    !info.deterministic && (info.locale.contains("ks-level1") || info.locale.contains("ks-level2"))
}

/// Types that take a collation: strings and arrays of them.
pub fn collatable(ty: super::types::Type) -> bool {
    ty.is_string() || (ty.array && ty.elem().is_string())
}

/// A column's collation, by name (`None` is the default).
pub fn column_info(db: &DbState, name: Option<&str>) -> Option<Info> {
    name.and_then(|n| lookup(db, n))
}

/// The CALL that `CREATE COLLATION` / `DROP COLLATION` become (sqlparser
/// parses neither).
pub const CALL_NAME: &str = "noida_collation_ddl";

/// `CREATE COLLATION ...` / `DROP COLLATION ...` as `CALL noida_collation_ddl('<text>')`.
pub fn rewrite(sql: &str) -> Option<String> {
    let t = sql.trim_start();
    let words: Vec<String> = t.split_whitespace().take(2).map(|w| w.to_ascii_lowercase()).collect();
    if words.len() < 2 || words[1] != "collation" || !(words[0] == "create" || words[0] == "drop") {
        return None;
    }
    let body = t.trim_end().trim_end_matches(';');
    Some(format!("CALL {CALL_NAME}('{}')", body.replace('\'', "''")))
}

/// Runs a rewritten CREATE/DROP COLLATION against `db`; returns the
/// command tag and any notice.
pub fn ddl(db: &mut DbState, text: &str) -> PgResult<(&'static str, Option<String>)> {
    let toks = tokens(text);
    let mut i = 0;
    let word = |i: usize| toks.get(i).map(|t| t.to_ascii_lowercase());
    let create = word(0).as_deref() == Some("create");
    i += 2;
    let mut if_flag = false;
    if word(i).as_deref() == Some("if") {
        if_flag = true;
        i += if create { 3 } else { 2 };
    }
    let name = toks.get(i).map(|t| unquote(t)).ok_or_else(|| syntax(""))?;
    // A schema-qualified name keeps only its last part.
    let name = name.rsplit('.').next().unwrap_or(&name).to_string();
    i += 1;
    if !create {
        let exists = db.collations.remove(&name).is_some();
        if !exists {
            if if_flag {
                return Ok((
                    "DROP COLLATION",
                    Some(format!("collation \"{name}\" does not exist, skipping")),
                ));
            }
            return Err(not_found(&name));
        }
        return Ok(("DROP COLLATION", None));
    }
    if lookup(db, &name).is_some() {
        if if_flag {
            return Ok((
                "CREATE COLLATION",
                Some(format!("collation \"{name}\" already exists, skipping")),
            ));
        }
        return Err(PgError::new(
            code::DUPLICATE_OBJECT,
            format!("collation \"{name}\" already exists"),
        ));
    }
    let mut provider = 'c';
    let mut deterministic = true;
    let mut locale = String::new();
    if word(i).as_deref() == Some("from") {
        let src = toks.get(i + 1).map(|t| unquote(t)).unwrap_or_default();
        let info = resolve(db, &src)?;
        provider = info.provider;
        deterministic = info.deterministic;
        locale = info.locale;
    } else {
        // ( key = value [, ...] )
        let mut j = i;
        while j < toks.len() {
            let k = toks[j].to_ascii_lowercase();
            if toks.get(j + 1).map(String::as_str) == Some("=") {
                let v = toks.get(j + 2).map(|t| unquote(t)).unwrap_or_default();
                match k.as_str() {
                    "provider" => provider = if v.eq_ignore_ascii_case("icu") { 'i' } else { 'c' },
                    "deterministic" => {
                        deterministic =
                            !matches!(v.to_ascii_lowercase().as_str(), "false" | "off" | "no" | "0")
                    }
                    "locale" | "lc_collate" => locale = v,
                    _ => {}
                }
                j += 3;
            } else {
                j += 1;
            }
        }
        if !deterministic && provider != 'i' {
            return Err(PgError::new(
                code::FEATURE_NOT_SUPPORTED,
                "nondeterministic collations not supported with this provider",
            ));
        }
    }
    let oid = db.alloc_oid();
    db.collations.insert(
        name,
        Collation { oid, schema: super::catalog::PUBLIC_NS, provider, deterministic, locale },
    );
    Ok(("CREATE COLLATION", None))
}

fn syntax(near: &str) -> PgError {
    PgError::new(code::SYNTAX_ERROR, format!("syntax error at or near \"{near}\""))
}

fn unquote(t: &str) -> String {
    if t.len() >= 2
        && ((t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')))
    {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_lowercase()
    }
}

/// Words, quoted strings/identifiers and the punctuation `( ) , =`.
fn tokens(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '\'' || c == '"' {
            let q = c;
            let mut w = String::from(q);
            chars.next();
            while let Some(d) = chars.next() {
                w.push(d);
                if d == q {
                    if chars.peek() == Some(&q) {
                        chars.next();
                        continue;
                    }
                    break;
                }
            }
            out.push(w);
        } else if "(),=".contains(c) {
            out.push(c.to_string());
            chars.next();
        } else {
            let mut w = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_whitespace() || "(),=".contains(d) {
                    break;
                }
                w.push(d);
                chars.next();
            }
            out.push(w);
        }
    }
    out
}
