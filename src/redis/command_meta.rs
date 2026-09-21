//! Command metadata (generated from Redis's commands.def into `meta.rs`) and
//! the COMMAND INFO / DOCS / GETKEYS replies built from it, ported from
//! Redis's server.c so the output matches exactly.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::glob;
use super::meta::COMMANDS;
use super::resp::Value;

pub struct CommandMeta {
    /// Full name: "get", or "client|setname" for a subcommand.
    pub name: &'static str,
    pub summary: Option<&'static str>,
    pub complexity: Option<&'static str>,
    pub since: Option<&'static str>,
    pub doc_flags: &'static [&'static str],
    pub replaced_by: Option<&'static str>,
    pub deprecated_since: Option<&'static str>,
    pub group: &'static str,
    pub history: Option<&'static [(&'static str, &'static str)]>,
    pub tips: &'static [&'static str],
    pub arity: i64,
    pub flags: &'static [&'static str],
    pub acl: &'static [&'static str],
    pub key_specs: &'static [KeySpec],
    /// Name of Redis's command-specific key-extraction function, if any.
    pub getkeys: Option<&'static str>,
    pub args: Option<&'static [Arg]>,
    pub subcommands: &'static [CommandMeta],
}

pub struct KeySpec {
    pub notes: Option<&'static str>,
    pub flags: &'static [&'static str],
    pub bs: Bs,
    pub fk: Fk,
}

pub enum Bs {
    Unknown,
    Index(i64),
    Keyword(&'static str, i64),
}

pub enum Fk {
    Unknown,
    Range { lastkey: i64, keystep: i64, limit: i64 },
    Keynum { keynumidx: i64, firstkey: i64, keystep: i64 },
}

pub struct Arg {
    pub name: &'static str,
    pub typ: &'static str,
    pub display_text: Option<&'static str>,
    pub key_spec_index: i64,
    pub token: Option<&'static str>,
    pub summary: Option<&'static str>,
    pub since: Option<&'static str>,
    pub flags: &'static [&'static str],
    pub deprecated_since: Option<&'static str>,
    pub subargs: &'static [Arg],
}

/// Looks up a command by full name ("get", "client|setname").
pub fn lookup(fullname: &str) -> Option<&'static CommandMeta> {
    static INDEX: OnceLock<HashMap<&'static str, &'static CommandMeta>> = OnceLock::new();
    INDEX
        .get_or_init(|| {
            let mut m = HashMap::new();
            for c in COMMANDS {
                m.insert(c.name, c);
                for s in c.subcommands {
                    m.insert(s.name, s);
                }
            }
            m
        })
        .get(fullname)
        .copied()
}

/// Command flags in the order COMMAND reports them (hidden ones omitted).
const FLAG_ORDER: &[&str] = &[
    "write",
    "readonly",
    "denyoom",
    "module",
    "admin",
    "pubsub",
    "noscript",
    "blocking",
    "loading",
    "stale",
    "skip_monitor",
    "skip_slowlog",
    "asking",
    "fast",
    "no_auth",
    "no_mandatory_keys",
    "no_async_loading",
    "no_multi",
    "movablekeys",
    "allow_busy",
];

/// ACL categories in Redis's table order.
const ACL_ORDER: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// Key-spec flags: (generated name, reported name), in report order.
const KEY_FLAG_ORDER: &[(&str, &str)] = &[
    ("ro", "RO"),
    ("rw", "RW"),
    ("ow", "OW"),
    ("rm", "RM"),
    ("access", "access"),
    ("update", "update"),
    ("insert", "insert"),
    ("delete", "delete"),
    ("not_key", "not_key"),
    ("incomplete", "incomplete"),
    ("variable_flags", "variable_flags"),
];

fn ordered_set(order: &[&str], present: impl Fn(&str) -> bool) -> Value {
    Value::Set(order.iter().filter(|f| present(f)).map(|f| Value::Simple((*f).into())).collect())
}

pub fn key_flags(flags: &[&str]) -> Value {
    Value::Set(
        KEY_FLAG_ORDER
            .iter()
            .filter(|(gen_name, _)| flags.contains(gen_name))
            .map(|(_, shown)| Value::Simple((*shown).into()))
            .collect(),
    )
}

fn b(s: &str) -> Value {
    Value::bulk(s)
}

fn map(pairs: Vec<(&str, Value)>) -> Value {
    Value::Map(pairs.into_iter().map(|(k, v)| (b(k), v)).collect())
}

impl CommandMeta {
    pub fn has_flag(&self, f: &str) -> bool {
        self.flags.contains(&f)
    }

    /// Explicit categories plus the implicit ones Redis derives from flags
    /// (`setImplicitACLCategories`).
    pub fn acl_categories(&self) -> Vec<&'static str> {
        let explicit = |c: &str| self.acl.contains(&c);
        let flag = |f: &str| self.has_flag(f);
        let fast = explicit("fast") || flag("fast");
        ACL_ORDER
            .iter()
            .copied()
            .filter(|c| match *c {
                "write" => explicit(c) || flag("write"),
                "read" => explicit(c) || (flag("readonly") && !explicit("scripting")),
                "admin" | "dangerous" => explicit(c) || flag("admin"),
                "pubsub" => explicit(c) || flag("pubsub"),
                "fast" => fast,
                "blocking" => explicit(c) || flag("blocking"),
                "slow" => explicit(c) || !fast,
                _ => explicit(c),
            })
            .collect()
    }

    /// (movablekeys, firstkey, lastkey, keystep), a port of
    /// `populateCommandLegacyRangeSpec`.
    fn legacy_range(&self) -> (bool, i64, i64, i64) {
        let specs = self.key_specs;
        if specs.is_empty() {
            return (false, 0, 0, 0);
        }
        if let [KeySpec { bs: Bs::Index(pos), fk: Fk::Range { lastkey, keystep, .. }, flags, .. }] =
            specs
        {
            let last = if *lastkey >= 0 { lastkey + pos } else { *lastkey };
            return (flags.contains(&"incomplete"), *pos, last, *keystep);
        }
        let mut movable = false;
        let (mut first, mut last, mut prev_last) = (i64::MAX, 0i64, 0i64);
        for spec in specs {
            let (Bs::Index(pos), Fk::Range { lastkey, keystep, .. }) = (&spec.bs, &spec.fk) else {
                movable = true;
                continue;
            };
            if *keystep != 1 || (prev_last != 0 && prev_last != pos - 1) {
                movable = true;
                continue;
            }
            if spec.flags.contains(&"incomplete") {
                movable = true;
            }
            first = first.min(*pos);
            let abs = if *lastkey >= 0 { lastkey + pos } else { *lastkey };
            // Redis compares as unsigned, so a negative lastkey wins.
            last = if (abs as u64) > (last as u64) { abs } else { last };
            prev_last = last;
        }
        if first == i64::MAX {
            return (true, 0, 0, 0);
        }
        (movable, first, last, 1)
    }

    /// One entry of COMMAND / COMMAND INFO.
    pub fn info(&self, implemented: &dyn Fn(&str) -> bool) -> Value {
        let (movable, first, last, step) = self.legacy_range();
        let flags =
            ordered_set(
                FLAG_ORDER,
                |f| {
                    if f == "movablekeys" { movable } else { self.has_flag(f) }
                },
            );
        let cats = Value::Set(
            self.acl_categories().into_iter().map(|c| Value::Simple(format!("@{c}"))).collect(),
        );
        let tips = Value::Set(self.tips.iter().map(|t| b(t)).collect());
        let specs = Value::Set(self.key_specs.iter().map(key_spec).collect());
        let subs = self.subcommand_values(implemented, |s| s.info(implemented));
        Value::Array(vec![
            b(self.name),
            Value::Integer(self.arity),
            flags,
            Value::Integer(first),
            Value::Integer(last),
            Value::Integer(step),
            cats,
            tips,
            specs,
            subs,
        ])
    }

    fn subcommand_values(
        &self,
        implemented: &dyn Fn(&str) -> bool,
        f: impl Fn(&CommandMeta) -> Value,
    ) -> Value {
        if self.subcommands.is_empty() {
            return Value::Set(vec![]);
        }
        Value::Array(self.subcommands.iter().filter(|s| implemented(s.name)).map(f).collect())
    }

    /// One entry of COMMAND DOCS (the value; the caller adds the name).
    pub fn docs(&self, implemented: &dyn Fn(&str) -> bool) -> Value {
        let mut m: Vec<(&str, Value)> = Vec::new();
        if let Some(s) = self.summary {
            m.push(("summary", b(s)));
        }
        if let Some(s) = self.since {
            m.push(("since", b(s)));
        }
        m.push(("group", b(self.group)));
        if let Some(s) = self.complexity {
            m.push(("complexity", b(s)));
        }
        if !self.doc_flags.is_empty() {
            m.push((
                "doc_flags",
                ordered_set(&["deprecated", "syscmd"], |f| self.doc_flags.contains(&f)),
            ));
        }
        if let Some(s) = self.deprecated_since {
            m.push(("deprecated_since", b(s)));
        }
        if let Some(s) = self.replaced_by {
            m.push(("replaced_by", b(s)));
        }
        if let Some(h) = self.history {
            let entries = h.iter().map(|(since, what)| Value::Array(vec![b(since), b(what)]));
            m.push(("history", Value::Set(entries.collect())));
        }
        if let Some(args) = self.args {
            m.push(("arguments", arg_list(args)));
        }
        if !self.subcommands.is_empty() {
            let subs = self
                .subcommands
                .iter()
                .filter(|s| implemented(s.name))
                .map(|s| (b(s.name), s.docs(implemented)))
                .collect();
            m.push(("subcommands", Value::Map(subs)));
        }
        map(m)
    }

    fn has_keyspec(&self) -> bool {
        self.key_specs.iter().any(|s| !s.flags.contains(&"not_key"))
    }

    /// `doesCommandHaveKeys`.
    pub fn has_keys(&self) -> bool {
        self.getkeys.is_some() || self.has_keyspec()
    }

    /// The keys in `argv` (argv[0] is the command name) as (position,
    /// flags). A port of `getKeysFromCommandWithSpecs`: key specs first,
    /// then the command's own key function.
    pub fn keys(&self, argv: &[Vec<u8>]) -> Vec<(usize, &'static [&'static str])> {
        let varflags = self.key_specs.iter().any(|s| s.flags.contains(&"variable_flags"));
        if self.has_keyspec()
            && !varflags
            && let Some(keys) = self.keys_from_specs(argv)
        {
            return keys;
        }
        match self.getkeys {
            Some(f) => getkeys_proc(f, argv),
            None => vec![],
        }
    }

    /// A port of `getKeysUsingKeySpecs`. `None` means an invalid or
    /// incomplete spec, and the caller falls back to the key function.
    fn keys_from_specs(&self, argv: &[Vec<u8>]) -> Option<Vec<(usize, &'static [&'static str])>> {
        let argc = argv.len() as i64;
        let mut keys = Vec::new();
        for spec in self.key_specs {
            if spec.flags.contains(&"not_key") {
                continue;
            }
            let first = match &spec.bs {
                Bs::Index(pos) => *pos,
                Bs::Keyword(kw, startfrom) => {
                    let start = if *startfrom > 0 { *startfrom } else { argc + startfrom };
                    let end = if *startfrom > 0 { argc - 1 } else { 1 };
                    let mut i = start;
                    let mut found = 0;
                    while i != end {
                        if i >= argc || i < 1 {
                            break;
                        }
                        if argv[i as usize].eq_ignore_ascii_case(kw.as_bytes()) {
                            found = i + 1;
                            break;
                        }
                        i += if start <= end { 1 } else { -1 };
                    }
                    if found == 0 {
                        continue;
                    }
                    found
                }
                Bs::Unknown => return None,
            };
            let (first, last, step) = match &spec.fk {
                Fk::Range { lastkey, keystep, limit } => {
                    let last = if *lastkey >= 0 {
                        first + lastkey
                    } else if *limit == 0 {
                        argc + lastkey
                    } else {
                        first + ((argc - first) / limit + lastkey)
                    };
                    (first, last, *keystep)
                }
                Fk::Keynum { keynumidx, firstkey, keystep } => {
                    let idx = first + keynumidx;
                    if idx >= argc || idx < 0 {
                        return None;
                    }
                    let n = super::num::parse_int(&argv[idx as usize]).filter(|n| *n >= 0)?;
                    let first = first + firstkey;
                    (first, first + n - 1, *keystep)
                }
                Fk::Unknown => return None,
            };
            if last >= argc || last < first || first >= argc {
                return None;
            }
            let mut i = first;
            while i <= last {
                // Variable-arity commands may declare more keys than given.
                if i < argc {
                    keys.push((i as usize, spec.flags));
                }
                i += step.max(1);
            }
            if spec.flags.contains(&"incomplete") {
                return None;
            }
        }
        Some(keys)
    }
}

/// Redis's command-specific key functions (`*GetKeys` in db.c), for
/// commands whose key specs can't express their keys exactly.
fn getkeys_proc(name: &str, argv: &[Vec<u8>]) -> Vec<(usize, &'static [&'static str])> {
    match name {
        "setGetKeys" => {
            let has_get = argv.iter().skip(3).any(|a| a.eq_ignore_ascii_case(b"get"));
            let flags: &'static [&'static str] =
                if has_get { &["rw", "access", "update"] } else { &["ow", "update"] };
            vec![(1, flags)]
        }
        _ => vec![],
    }
}

fn key_spec(s: &KeySpec) -> Value {
    let mut m = Vec::new();
    if let Some(n) = s.notes {
        m.push(("notes", b(n)));
    }
    m.push(("flags", key_flags(s.flags)));
    let bs = match &s.bs {
        Bs::Unknown => map(vec![("type", b("unknown")), ("spec", map(vec![]))]),
        Bs::Index(i) => {
            map(vec![("type", b("index")), ("spec", map(vec![("index", Value::Integer(*i))]))])
        }
        Bs::Keyword(kw, from) => map(vec![
            ("type", b("keyword")),
            ("spec", map(vec![("keyword", b(kw)), ("startfrom", Value::Integer(*from))])),
        ]),
    };
    m.push(("begin_search", bs));
    let fk = match &s.fk {
        Fk::Unknown => map(vec![("type", b("unknown")), ("spec", map(vec![]))]),
        Fk::Range { lastkey, keystep, limit } => map(vec![
            ("type", b("range")),
            (
                "spec",
                map(vec![
                    ("lastkey", Value::Integer(*lastkey)),
                    ("keystep", Value::Integer(*keystep)),
                    ("limit", Value::Integer(*limit)),
                ]),
            ),
        ]),
        Fk::Keynum { keynumidx, firstkey, keystep } => map(vec![
            ("type", b("keynum")),
            (
                "spec",
                map(vec![
                    ("keynumidx", Value::Integer(*keynumidx)),
                    ("firstkey", Value::Integer(*firstkey)),
                    ("keystep", Value::Integer(*keystep)),
                ]),
            ),
        ]),
    };
    m.push(("find_keys", fk));
    map(m)
}

fn arg_list(args: &[Arg]) -> Value {
    Value::Array(
        args.iter()
            .map(|a| {
                let block = a.typ == "oneof" || a.typ == "block";
                let mut m = vec![("name", b(a.name)), ("type", b(a.typ))];
                if !block {
                    m.push(("display_text", b(a.display_text.unwrap_or(a.name))));
                }
                if a.key_spec_index != -1 {
                    m.push(("key_spec_index", Value::Integer(a.key_spec_index)));
                }
                if let Some(t) = a.token {
                    m.push(("token", b(t)));
                }
                if let Some(s) = a.summary {
                    m.push(("summary", b(s)));
                }
                if let Some(s) = a.since {
                    m.push(("since", b(s)));
                }
                if let Some(s) = a.deprecated_since {
                    m.push(("deprecated_since", b(s)));
                }
                if !a.flags.is_empty() {
                    m.push((
                        "flags",
                        ordered_set(&["optional", "multiple", "multiple_token"], |f| {
                            a.flags.contains(&f)
                        }),
                    ));
                }
                if block {
                    m.push(("arguments", arg_list(a.subargs)));
                }
                map(m)
            })
            .collect(),
    )
}

/// Top-level commands, for COMMAND / COMMAND LIST.
pub fn all() -> &'static [CommandMeta] {
    COMMANDS
}

/// COMMAND LIST FILTERBY: whether `cmd` passes the filter.
pub fn list_filter(cmd: &CommandMeta, kind: &str, arg: &[u8]) -> bool {
    match kind {
        "aclcat" => {
            let cat = String::from_utf8_lossy(arg).to_ascii_lowercase();
            ACL_ORDER.contains(&cat.as_str()) && cmd.acl_categories().contains(&cat.as_str())
        }
        "pattern" => glob::matches(arg, cmd.name.as_bytes(), true),
        // noida loads no modules.
        _ => false,
    }
}
