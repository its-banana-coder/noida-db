#!/usr/bin/env python3
"""Generates src/redis/config_table.rs: every CONFIG parameter Redis 7.2 has.

Types, ranges, enum values and flags come from Redis's src/config.c; the
defaults come from a real redis-server started without a configuration
file, the only faithful source for defaults built out of C constants.

usage: scripts/gen-redis-config.py <path/to/redis/src> [redis-server]
"""

import re
import subprocess
import sys
import time
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "src/redis/config_table.rs"
PORT = "6399"

INT_KINDS = {
    "createIntConfig", "createUIntConfig", "createLongLongConfig",
    "createULongConfig", "createULongLongConfig", "createSizeTConfig",
    "createSSizeTConfig", "createTimeTConfig", "createOffTConfig",
}

# C constants used in the ranges, as Rust expressions.
CONSTS = {
    "LONG_MAX": "i64::MAX", "LONG_MIN": "i64::MIN", "LLONG_MAX": "i64::MAX",
    "LLONG_MIN": "i64::MIN", "INT_MAX": "2147483647", "INT_MIN": "-2147483648",
    "UINT_MAX": "4294967295", "ULONG_MAX": "i64::MAX", "SIZE_MAX": "i64::MAX",
    "SSIZE_MAX": "i64::MAX", "CONFIG_MIN_HZ": "1", "CONFIG_MAX_HZ": "500",
    "OBJ_SHARED_INTEGERS": "10000", "CONFIG_BINDADDR_MAX": "16",
    "NET_MAX_WRITES_PER_EVENT": "65536",
    "PROTO_MAX_QUERYBUF_LEN": "1073741824",
    "LOG_MAX_LEN": "1024", "CONFIG_DEFAULT_MAX_CLIENTS": "10000",
}


def split_args(text):
    """Splits a C argument list on its top-level commas."""
    out, depth, cur, in_str = [], 0, "", False
    for ch in text:
        if in_str:
            cur += ch
            if ch == '"' and not cur.endswith('\\"'):
                in_str = False
            continue
        if ch == '"':
            in_str = True
            cur += ch
        elif ch in "([{":
            depth += 1
            cur += ch
        elif ch in ")]}":
            depth -= 1
            cur += ch
        elif ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    out.append(cur.strip())
    return out


def enum_tables(src):
    """table name -> the value names it accepts."""
    tables = {}
    for m in re.finditer(r"configEnum (\w+)\[\] = \{(.*?)\n\};", src, re.S):
        tables[m.group(1)] = re.findall(r'\{\s*"([^"]+)"', m.group(2))
    return tables


def lit(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def number(expr):
    expr = expr.strip()
    if re.fullmatch(r"0[0-7]+", expr):
        return str(int(expr, 8))  # C octal literal
    if re.fullmatch(r"-?\d+", expr):
        return expr
    if expr in CONSTS:
        return CONSTS[expr]
    m = re.fullmatch(r"(\w+)\s*\*\s*(\d+)", expr)
    if m and m.group(1) in CONSTS:
        return f"{CONSTS[m.group(1)]} * {m.group(2)}"
    m = re.fullmatch(r"(\d+)(?:\s*\*\s*(\d+))?(?:\s*\*\s*(\d+))?", expr)
    if m:
        v = 1
        for g in m.groups():
            if g:
                v *= int(g)
        return str(v)
    return "i64::MAX" if "MAX" in expr else "0"


def parse_configs(src):
    """Every parameter's name, alias, kind and constraints."""
    start = src.index("standardConfig static_configs[] = {")
    body = src[start:src.index("\n};", start)]
    out = {}
    for m in re.finditer(r"create(\w+)Config\(", body):
        kind = "create" + m.group(1) + "Config"
        depth, i = 1, m.end()
        while depth:
            depth += {"(": 1, ")": -1}.get(body[i], 0)
            i += 1
        args = split_args(body[m.end():i - 1])
        name = args[0].strip('"')
        entry = {
            "alias": None if args[1] == "NULL" else args[1].strip('"'),
            "immutable": "IMMUTABLE_CONFIG" in args[2],
            "multi_arg": "MULTI_ARG_CONFIG" in args[2],
        }
        if kind == "createBoolConfig":
            entry["kind"] = "Bool"
        elif kind == "createEnumConfig":
            entry["kind"] = "Enum"
            entry["enum"] = args[3]
        elif kind in ("createStringConfig", "createSDSConfig"):
            entry["kind"] = "String"
        elif kind == "createSpecialConfig":
            entry["kind"] = "Special"
        elif kind in INT_KINDS:
            entry["kind"] = "Numeric"
            entry["lower"], entry["upper"], entry["num_flags"] = args[3], args[4], args[7]
        else:
            continue
        out[name] = entry
    return out


def live_defaults(server, names):
    """Each parameter's default, asked for one at a time so that values
    holding spaces or quotes can't shift the pairing."""
    proc = subprocess.Popen(
        [server, "--port", PORT, "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(50):
            time.sleep(0.1)
            r = subprocess.run(["redis-cli", "-p", PORT, "ping"], capture_output=True, text=True)
            if r.stdout.strip() == "PONG":
                break
        out = {}
        for name in names:
            got = subprocess.run(
                ["redis-cli", "-p", PORT, "config", "get", name],
                capture_output=True, text=True, check=True,
            ).stdout.split("\n")
            if len(got) >= 2 and got[0] == name:
                out[name] = got[1]
    finally:
        proc.terminate()
        proc.wait()
    return out


def main():
    src_dir = Path(sys.argv[1])
    server = sys.argv[2] if len(sys.argv) > 2 else str(src_dir / "redis-server")
    src = (src_dir / "config.c").read_text()
    enums = enum_tables(src)
    configs = parse_configs(src)
    names = sorted({n for name, c in configs.items() for n in (name, c["alias"]) if n})
    defaults = live_defaults(server, names)
    # Defaults that depend on how the generator ran, not on Redis.
    defaults["port"] = "6379"
    defaults["dir"] = ""
    defaults["save"] = "3600 1 300 100 60 10000"
    defaults["appendonly"] = "no"

    rows = []
    for name in sorted(defaults):
        info, alias_of = configs.get(name), None
        if info is None:
            for canonical, c in configs.items():
                if c["alias"] == name:
                    info, alias_of = c, canonical
                    break
        if info is None:
            continue
        kind = info["kind"]
        detail = "Kind::Special"
        if kind == "Bool":
            detail = "Kind::Bool"
        elif kind == "String":
            detail = "Kind::Text"
        elif kind == "Enum":
            detail = "Kind::Enum(&[" + ", ".join(lit(v) for v in enums[info["enum"]]) + "])"
        elif kind == "Numeric":
            nf = info["num_flags"]
            detail = (
                f"Kind::Number {{ lower: {number(info['lower'])}, "
                f"upper: {number(info['upper'])}, "
                f"memory: {str('MEMORY_CONFIG' in nf).lower()}, "
                f"percent: {str('PERCENT_CONFIG' in nf).lower()}, "
                f"octal: {str('OCTAL_CONFIG' in nf).lower()} }}"
            )
        other = info["alias"] if alias_of is None else alias_of
        rows.append(
            f"    Param {{ name: {lit(name)}, "
            f"alias: {'None' if other is None else 'Some(' + lit(other) + ')'}, "
            f"kind: {detail}, default: {lit(defaults[name])}, "
            f"immutable: {str(info['immutable']).lower()}, "
            f"multi_arg: {str(info['multi_arg']).lower()} }}"
        )
    OUT.write_text(
        "// @generated by scripts/gen-redis-config.py from Redis 7.2's src/config.c\n"
        "// and a default redis-server's CONFIG GET *. Do not edit.\n"
        "#![cfg_attr(rustfmt, rustfmt_skip)]\n\n"
        "use super::config::{Kind, Param};\n\n"
        "pub static PARAMS: &[Param] = &[\n" + ",\n".join(rows) + ",\n];\n"
    )
    print(f"wrote {OUT} ({len(rows)} parameters)")


if __name__ == "__main__":
    main()
