#!/usr/bin/env python3
"""Generates src/redis/meta.rs from Redis's own command table.

The source is src/commands.def from the Redis repository (tag 7.2). It holds
every command's arity, flags, ACL categories, key specs, tips, history and
argument docs, which is exactly what COMMAND INFO / DOCS / GETKEYS report.

usage: scripts/gen-redis-commands.py [path/to/commands.def]
Without a path, the file is fetched with `gh api`.
"""

import re
import subprocess
import sys
from pathlib import Path

REF = "7.2"
OUT = Path(__file__).resolve().parent.parent / "src/redis/meta.rs"

GROUPS = {
    "COMMAND_GROUP_GENERIC": "generic", "COMMAND_GROUP_STRING": "string",
    "COMMAND_GROUP_LIST": "list", "COMMAND_GROUP_SET": "set",
    "COMMAND_GROUP_SORTED_SET": "sorted-set", "COMMAND_GROUP_HASH": "hash",
    "COMMAND_GROUP_PUBSUB": "pubsub", "COMMAND_GROUP_TRANSACTIONS": "transactions",
    "COMMAND_GROUP_CONNECTION": "connection", "COMMAND_GROUP_SERVER": "server",
    "COMMAND_GROUP_SCRIPTING": "scripting", "COMMAND_GROUP_HYPERLOGLOG": "hyperloglog",
    "COMMAND_GROUP_CLUSTER": "cluster", "COMMAND_GROUP_SENTINEL": "sentinel",
    "COMMAND_GROUP_GEO": "geo", "COMMAND_GROUP_STREAM": "stream",
    "COMMAND_GROUP_BITMAP": "bitmap", "COMMAND_GROUP_MODULE": "module",
}

ARG_TYPES = {
    "ARG_TYPE_STRING": "string", "ARG_TYPE_INTEGER": "integer", "ARG_TYPE_DOUBLE": "double",
    "ARG_TYPE_KEY": "key", "ARG_TYPE_PATTERN": "pattern", "ARG_TYPE_UNIX_TIME": "unix-time",
    "ARG_TYPE_PURE_TOKEN": "pure-token", "ARG_TYPE_ONEOF": "oneof", "ARG_TYPE_BLOCK": "block",
}


# ---- a tiny parser for C initializer lists ----

TOKEN = re.compile(
    r'\s*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<num>-?\d+)|(?P<id>[A-Za-z_][A-Za-z_0-9]*)'
    r'|(?P<punct>[{}(),=|.]))'
)


def tokenize(src):
    pos, out = 0, []
    while pos < len(src):
        m = TOKEN.match(src, pos)
        if not m:
            if src[pos:].strip() == "":
                break
            raise SyntaxError(f"cannot tokenize at: {src[pos:pos + 60]!r}")
        pos = m.end()
        kind = m.lastgroup
        out.append((kind, m.group(kind)))
    return out


def c_string(lit):
    body = lit[1:-1]
    return re.sub(r'\\(.)', lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), body)


class Parser:
    def __init__(self, tokens):
        self.t, self.i = tokens, 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def take(self, val=None):
        tok = self.t[self.i]
        if val is not None and tok[1] != val:
            raise SyntaxError(f"expected {val}, got {tok}")
        self.i += 1
        return tok

    def value(self):
        kind, val = self.peek()
        if val == "{":
            self.take("{")
            items = self.items("}")
            self.take("}")
            return ("list", items)
        if val == ".":
            path = []
            while self.peek()[1] == ".":
                self.take(".")
                path.append(self.take()[1])
            self.take("=")
            return ("desig", ".".join(path), self.value())
        if kind == "str":
            s = ""
            while self.peek()[0] == "str":
                s += c_string(self.take()[1])
            return ("str", s)
        if kind == "num":
            return ("num", int(self.take()[1]))
        if kind == "id":
            name = self.take()[1]
            if self.peek()[1] == "(":
                self.take("(")
                args = self.items(")")
                self.take(")")
                return ("call", name, args)
            names = [name]
            while self.peek()[1] == "|":
                self.take("|")
                names.append(self.take()[1])
            return ("id", names)
        raise SyntaxError(f"unexpected {self.peek()}")

    def items(self, close):
        out = []
        while self.peek()[1] != close:
            out.append(self.value())
            if self.peek()[1] == ",":
                self.take(",")
        return out


def parse(src):
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return Parser(tokenize(src)).items(None)


# ---- read the named tables out of commands.def ----

def load_tables(text):
    tables = {}
    for m in re.finditer(r"#define (\w+) NULL", text):
        tables[m.group(1)] = None
    decl = re.compile(
        r"^(?:commandHistory|const char \*|keySpec|struct COMMAND_ARG|struct COMMAND_STRUCT)\s*"
        r"(\w+)\[\d*\] = \{\n(.*?)^\};",
        re.M | re.S,
    )
    for m in decl.finditer(text):
        tables[m.group(1)] = parse(m.group(2))
    return tables


def opt_str(v):
    return v[1] if v[0] == "str" else None


def ident_list(v):
    if v[0] == "num":
        return []
    return [n for n in v[1] if not n.endswith("_NONE")]


def rust_str(s):
    if s is None:
        return "None"
    return "Some(" + rust_lit(s) + ")"


def rust_lit(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def rust_strs(items):
    return "&[" + ", ".join(rust_lit(i) for i in items) + "]"


def flag_names(idents, prefix):
    return [i[len(prefix):].lower() for i in idents]


def key_spec(v, tables):
    # {notes, flags, BS_TYPE, .bs.x={..}, FK_TYPE, .fk.x={..}}
    items = v[1]
    notes = opt_str(items[0])
    flags = flag_names(ident_list(items[1]), "CMD_KEY_")
    rest = items[2:]
    bs_type = rest[0][1][0]
    bs_val = rest[1]
    fk_type = rest[2][1][0]
    fk_val = rest[3]
    if bs_type == "KSPEC_BS_INDEX":
        bs = f"Bs::Index({bs_val[2][1][0][1]})"
    elif bs_type == "KSPEC_BS_KEYWORD":
        kw, start = bs_val[2][1]
        bs = f"Bs::Keyword({rust_lit(kw[1])}, {start[1]})"
    else:
        bs = "Bs::Unknown"
    if fk_type == "KSPEC_FK_RANGE":
        a, b, c = (x[1] for x in fk_val[2][1])
        fk = f"Fk::Range {{ lastkey: {a}, keystep: {b}, limit: {c} }}"
    elif fk_type == "KSPEC_FK_KEYNUM":
        a, b, c = (x[1] for x in fk_val[2][1])
        fk = f"Fk::Keynum {{ keynumidx: {a}, firstkey: {b}, keystep: {c} }}"
    else:
        fk = "Fk::Unknown"
    return (f"KeySpec {{ notes: {rust_str(notes)}, flags: {rust_strs(flags)}, "
            f"bs: {bs}, fk: {fk} }}")


def args_table(name, tables):
    rows = tables.get(name)
    if not rows:
        return "&[]"
    out = []
    for row in rows:
        call = row[1][0]
        extra = {d[1]: d[2] for d in row[1][1:] if d[0] == "desig"}
        (aname, atype, ksi, token, summary, since, flags, _nsub, deprecated) = call[2]
        display = opt_str(extra["display_text"]) if "display_text" in extra else None
        subargs = args_table(extra["subargs"][1][0], tables) if "subargs" in extra else "&[]"
        out.append(
            f"Arg {{ name: {rust_lit(aname[1])}, typ: {rust_lit(ARG_TYPES[atype[1][0]])}, "
            f"display_text: {rust_str(display)}, key_spec_index: {ksi[1]}, "
            f"token: {rust_str(opt_str(token))}, summary: {rust_str(opt_str(summary))}, "
            f"since: {rust_str(opt_str(since))}, "
            f"flags: {rust_strs(flag_names(ident_list(flags), 'CMD_ARG_'))}, "
            f"deprecated_since: {rust_str(opt_str(deprecated))}, subargs: {subargs} }}"
        )
    return "&[" + ", ".join(out) + "]"


def command(row, tables, container):
    call = row[1][0]
    extra = {d[1]: d[2] for d in row[1][1:] if d[0] == "desig"}
    (name, summary, complexity, since, doc_flags, replaced, deprecated, _group, group_enum,
     history, _nh, tips, _nt, _proc, arity, flags, acl, key_specs, _nks, getkeys, _na) = call[2]
    fullname = f"{container}|{name[1]}" if container else name[1]

    hist_rows = tables.get(history[1][0])
    hist = ("None" if hist_rows is None else
            "Some(&[" + ", ".join(f"({rust_lit(h[1][0][1])}, {rust_lit(h[1][1][1])})"
                                  for h in hist_rows) + "])")
    tip_rows = tables.get(tips[1][0]) or []
    specs = tables.get(key_specs[1][0]) or []
    subs = ""
    if "subcommands" in extra:
        rows = [r for r in tables[extra["subcommands"][1][0]] if r[0] == "list" and r[1] and r[1][0][0] == "call"]
        subs = ", ".join(command(r, tables, name[1]) for r in rows)
    args = args_table(extra["args"][1][0], tables) if "args" in extra else None

    return (
        f"CommandMeta {{ name: {rust_lit(fullname)}, summary: {rust_str(opt_str(summary))}, "
        f"complexity: {rust_str(opt_str(complexity))}, since: {rust_str(opt_str(since))}, "
        f"doc_flags: {rust_strs(flag_names(ident_list(doc_flags), 'CMD_DOC_'))}, "
        f"replaced_by: {rust_str(opt_str(replaced))}, deprecated_since: {rust_str(opt_str(deprecated))}, "
        f"group: {rust_lit(GROUPS[group_enum[1][0]])}, history: {hist}, "
        f"tips: {rust_strs(t[1] for t in tip_rows)}, arity: {arity[1]}, "
        f"flags: {rust_strs(flag_names(ident_list(flags), 'CMD_'))}, "
        f"acl: {rust_strs(flag_names(ident_list(acl), 'ACL_CATEGORY_'))}, "
        f"key_specs: &[{', '.join(key_spec(s, tables) for s in specs)}], "
        f"getkeys: {rust_str(None if getkeys[0] != 'id' or getkeys[1][0] == 'NULL' else getkeys[1][0])}, "
        f"args: {'None' if args is None else 'Some(' + args + ')'}, subcommands: &[{subs}] }}"
    )


def main():
    if len(sys.argv) > 1:
        text = Path(sys.argv[1]).read_text()
    else:
        text = subprocess.run(
            ["gh", "api", f"repos/redis/redis/contents/src/commands.def?ref={REF}",
             "-H", "Accept: application/vnd.github.raw"],
            check=True, capture_output=True, text=True,
        ).stdout
    tables = load_tables(text)
    main_start = text.index("/* Main command table */")
    main_rows = parse(text[text.index("{", text.index("=", main_start)) + 1:text.index("\n};", main_start)])
    rows = [r for r in main_rows if r[0] == "list" and r[1] and r[1][0][0] == "call"]
    body = ",\n    ".join(command(r, tables, None) for r in rows)
    OUT.write_text(
        "// @generated by scripts/gen-redis-commands.py from Redis "
        f"{REF}'s src/commands.def. Do not edit.\n"
        "#![cfg_attr(rustfmt, rustfmt_skip)]\n\n"
        "use super::command_meta::{Arg, Bs, CommandMeta, Fk, KeySpec};\n\n"
        f"pub static COMMANDS: &[CommandMeta] = &[\n    {body},\n];\n"
    )
    print(f"wrote {OUT} ({len(rows)} commands)")


if __name__ == "__main__":
    main()
