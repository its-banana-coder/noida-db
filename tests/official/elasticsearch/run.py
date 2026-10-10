"""Runs Elasticsearch's own REST API YAML test suite (the one every official
client runs) against a server, and reports how much of it passes.

    tests/official/elasticsearch/fetch.sh            # once: the 8.15.3 suite
    python3 tests/official/elasticsearch/run.py http://127.0.0.1:9200 [dir-or-file ...]

Implements the runner contract in the suite's README: `do` (by API name,
resolved through the JSON API specs), `catch`, `headers`, `warnings`,
`match` (regexes included), `length`, `is_true`/`is_false`, `gt`/`gte`/
`lt`/`lte`, `contains`, `close_to`, `set` and stash substitution,
`setup`/`teardown`, and `requires`/`skip`. A test needing something a
runner may decline (the capabilities API, a runner feature not listed in
FEATURES, `awaits_fix`, another distribution) is skipped, never counted
as a failure; the same skips apply to any server, so a run against real
Elasticsearch calibrates the runner.

Prints a per-directory table and writes a JSON report (`--json PATH`).
"""

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

import yaml


class Loader(yaml.SafeLoader):
    """SafeLoader without the timestamp resolver: dates stay strings, as
    the suite's other runners read them."""


Loader.yaml_implicit_resolvers = {
    k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
    for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
# YAML 1.2 floats (as the Java runner reads them): `2.012916202E9`, an
# exponent without a sign, is a number, not a string.
Loader.add_implicit_resolver(
    "tag:yaml.org,2002:float",
    re.compile(r"^[-+]?[0-9][0-9_]*\.[0-9_]*[eE][-+]?[0-9]+$"),
    list("-+0123456789"),
)

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.environ.get(
    "ES_SUITE", os.path.join(HERE, "../../../target/official/elasticsearch/rest-api-spec")
)
API_DIR = os.path.join(SUITE, "src/main/resources/rest-api-spec/api")
TEST_DIR = os.path.join(SUITE, "src/yamlRestTest/resources/rest-api-spec/test")
VERSION = (8, 15, 3)
FEATURES = {
    "headers", "stash_in_path", "stash_in_key", "embedded_stash_key", "warnings",
    "warnings_regex", "allowed_warnings", "allowed_warnings_regex", "contains",
    "close_to", "arbitrary_key", "default_shards", "xpack",
}


class Skip(Exception):
    pass


class Fail(Exception):
    pass


def load_apis():
    apis = {}
    for f in os.listdir(API_DIR):
        if f.endswith(".json") and not f.startswith("_"):
            with open(os.path.join(API_DIR, f)) as fh:
                apis.update(json.load(fh))
    return apis


APIS = None


def parse_version(v):
    v = v.strip()
    if not v:
        return None
    parts = re.findall(r"\d+", v)
    return tuple(int(p) for p in (parts + ["0", "0", "0"])[:3])


def in_range(spec):
    """`version` ranges in legacy skip sections: 'all', ' - 7.99.99',
    '8.0.0 - 8.14.99', several separated by commas."""
    for r in str(spec).split(","):
        r = r.strip()
        if r == "all":
            return True
        lo, _, hi = r.partition("-")
        lo, hi = parse_version(lo) or (0, 0, 0), parse_version(hi) or (99, 99, 99)
        if lo <= VERSION <= hi:
            return True
    return False


def check_prereqs(section):
    for step in section:
        if not isinstance(step, dict):
            continue
        if "requires" in step:
            req = step["requires"]
            feats = req.get("test_runner_features") or req.get("features") or []
            feats = [feats] if isinstance(feats, str) else feats
            for f in feats:
                if f not in FEATURES:
                    raise Skip(f"runner feature {f}")
            if req.get("capabilities"):
                raise Skip("capabilities API")
            cf = req.get("cluster_features") or []
            cf = [cf] if isinstance(cf, str) else cf
            for f in cf:
                m = re.match(r"gte_v(.*)", f)
                if m and parse_version(m.group(1)) > VERSION:
                    raise Skip(f"needs {f}")
        if "skip" in step:
            sk = step["skip"]
            feats = sk.get("features") or []
            feats = [feats] if isinstance(feats, str) else feats
            for f in feats:
                if f not in FEATURES:
                    raise Skip(f"runner feature {f}")
            if sk.get("awaits_fix"):
                raise Skip("awaits_fix")
            if sk.get("capabilities"):
                raise Skip("capabilities API")
            if "version" in sk and in_range(sk["version"]):
                raise Skip(f"version {sk['version']}")
            cf = sk.get("cluster_features") or []
            cf = [cf] if isinstance(cf, str) else cf
            for f in cf:
                m = re.match(r"gte_v(.*)", f)
                if m is None or parse_version(m.group(1)) <= VERSION:
                    raise Skip(f"cluster feature {f}")
            if sk.get("os"):
                pass


class Runner:
    def __init__(self, base):
        self.base = base.rstrip("/")
        self.stash = {}
        self.response = None
        self.headers = {}

    # --- requests ---------------------------------------------------------

    def http(self, method, path, query, body, headers, ndjson):
        url = self.base + path + ("?" + urllib.parse.urlencode(query) if query else "")
        hdrs = {}
        data = None
        if body is not None:
            if ndjson:
                items = body if isinstance(body, list) else [body]
                lines = [b if isinstance(b, str) else json.dumps(b) for b in items]
                data = ("\n".join(lines) + "\n").encode()
                hdrs["Content-Type"] = "application/x-ndjson"
            elif isinstance(body, str):
                data = body.encode()
                hdrs["Content-Type"] = "application/json"
            else:
                data = json.dumps(body).encode()
                hdrs["Content-Type"] = "application/json"
        hdrs.update(headers)
        req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, r.read(), dict(r.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read(), dict(e.headers)

    def call(self, api, params, headers):
        spec = APIS.get(api)
        if spec is None:
            raise Skip(f"unknown api {api}")
        params = dict(params or {})
        body = params.pop("body", None)
        ignore = params.pop("ignore", None)
        parts_given = {k for k in params}
        best = None
        for p in spec["url"]["paths"]:
            parts = set((p.get("parts") or {}).keys())
            if parts <= parts_given and (best is None or len(parts) > len(best[1])):
                best = (p, parts)
        if best is None:
            return ("param", None)
        p, parts = best
        path = p["path"]
        for part in parts:
            v = params.pop(part)
            if isinstance(v, list):
                v = ",".join(str(x) for x in v)
            path = path.replace("{" + part + "}", urllib.parse.quote(str(v), safe=",*"))
        methods = p["methods"]
        method = methods[0]
        if body is not None and method in ("GET", "HEAD") and "POST" in methods:
            method = "POST"
        query = {}
        for k, v in params.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            elif isinstance(v, list):
                v = ",".join(str(x) for x in v)
            query[k] = v
        ndjson = "x-ndjson" in str(spec.get("headers", {}).get("content_type", ""))
        # JSON unless the API's own default is text (`_cat`).
        if not api.startswith("cat.") and not any(k.lower() == "accept" for k in headers):
            headers = dict(headers, Accept="application/json")
        status, raw, rh = self.http(method, path, query, body, headers, ndjson)
        text = raw.decode("utf-8", "replace")
        if method == "HEAD":
            parsed = status == 200
        else:
            try:
                parsed = json.loads(text) if text.strip() else ""
            except ValueError:
                parsed = text
        # `ignore: 404`: that status is fine for this call.
        if ignore is not None:
            ignored = ignore if isinstance(ignore, list) else [ignore]
            if status in ignored:
                return (200, parsed, rh)
        return (status, parsed, rh)

    # --- stash / paths ----------------------------------------------------

    def subst(self, v):
        if isinstance(v, str):
            m = re.fullmatch(r"\$\{?(\w+)\}?", v)
            if m and m.group(1) in self.stash:
                return self.stash[m.group(1)]
            if v == "$body":
                return self.response

            def rep(m):
                k = m.group(1)
                return str(self.stash.get(k, m.group(0)))

            return re.sub(r"\$\{(\w+)\}", rep, v)
        if isinstance(v, dict):
            return {self.subst(k): self.subst(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self.subst(x) for x in v]
        return v

    def lookup(self, path):
        if path in ("$body", ""):
            return self.response
        cur = self.response
        # As the Java runner's ObjectPath: a backslash escapes the next
        # dot and is itself dropped (`a\.b` and `a\\.b` both name key "a.b").
        segs, cur_seg, escape = [], [], False
        for ch in path:
            if ch == "\\":
                escape = True
                continue
            if ch == "." and not escape:
                segs.append("".join(cur_seg))
                cur_seg = []
                continue
            escape = False
            cur_seg.append(ch)
        segs.append("".join(cur_seg))
        # Empty segments (`key.`) are dropped, as ObjectPath drops them.
        segs = [s for s in segs if s]
        for seg in segs:
            if seg == "_arbitrary_key_" and isinstance(cur, dict) and cur:
                return next(iter(cur))
            if seg.startswith("$") and seg[1:] in self.stash:
                seg = str(self.stash[seg[1:]])
            elif "${" in seg:
                seg = self.subst(seg)
            if isinstance(cur, dict):
                if seg not in cur:
                    return None
                cur = cur[seg]
            elif isinstance(cur, list):
                try:
                    cur = cur[int(seg)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return cur

    # --- assertions -------------------------------------------------------

    @staticmethod
    def equal(actual, expected):
        if isinstance(expected, str) and len(expected) > 1 and expected.strip().startswith("/") and expected.strip().endswith("/"):
            pat = expected.strip()[1:-1]
            return isinstance(actual, (str, int, float)) and re.search(pat, str(actual), re.X) is not None
        if isinstance(expected, bool) or isinstance(actual, bool):
            return actual == expected and type(actual) is type(expected)
        if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            return float(actual) == float(expected)
        if isinstance(expected, dict) and isinstance(actual, dict):
            return set(expected) == set(actual) and all(Runner.equal(actual[k], expected[k]) for k in expected)
        if isinstance(expected, list) and isinstance(actual, list):
            return len(expected) == len(actual) and all(Runner.equal(a, e) for a, e in zip(actual, expected))
        if isinstance(expected, (int, float)) and isinstance(actual, str):
            try:
                return float(actual) == float(expected)
            except ValueError:
                return False
        return actual == expected

    def step(self, step):
        (kind, arg), = step.items()
        if kind in ("requires", "skip"):
            return
        if kind == "do":
            arg = dict(arg)
            catch = arg.pop("catch", None)
            headers = self.subst(arg.pop("headers", {}) or {})
            warnings = arg.pop("warnings", None)
            arg.pop("allowed_warnings", None)
            warnings_regex = arg.pop("warnings_regex", None)
            arg.pop("allowed_warnings_regex", None)
            arg.pop("node_selector", None)
            (api, params), = arg.items()
            params = self.subst(params or {})
            res = self.call(api, params, headers)
            if res[0] == "param":
                if catch == "param":
                    return
                raise Fail(f"{api}: missing required path parts")
            status, body, rh = res
            self.response = body
            if catch:
                expected = {
                    "missing": [404], "conflict": [409], "unauthorized": [401],
                    "forbidden": [403], "request_timeout": [408], "bad_request": [400],
                    "unavailable": [503],
                }.get(catch)
                if catch == "param":
                    raise Fail(f"{api}: expected a client-side param error, got {status}")
                if expected is not None:
                    if status not in expected:
                        raise Fail(f"{api}: expected {catch}, got {status} {str(body)[:200]}")
                elif catch == "request":
                    if status < 400 or status in (404, 409, 401, 403, 408):
                        raise Fail(f"{api}: expected a request error, got {status}")
                elif catch.startswith("/"):
                    if status < 400 or not re.search(catch.strip("/"), json.dumps(body) if not isinstance(body, str) else body):
                        raise Fail(f"{api}: expected error matching {catch}, got {status} {str(body)[:200]}")
                return
            if status >= 400 and not (status == 404 and body is False):
                raise Fail(f"{api}: HTTP {status} {str(body)[:300]}")
            if warnings or warnings_regex:
                got = rh.get("Warning", "") or rh.get("warning", "")
                for w in warnings or []:
                    if w not in got:
                        raise Fail(f"{api}: missing warning {w!r}")
                for w in warnings_regex or []:
                    if not re.search(w, got):
                        raise Fail(f"{api}: missing warning matching {w!r}")
            return
        if kind == "set":
            for path, var in arg.items():
                self.stash[var] = self.lookup(self.subst(path))
            return
        if kind == "match":
            for path, expected in arg.items():
                actual = self.lookup(self.subst(path)) if path != "$body" else self.response
                expected = self.subst(expected)
                if not self.equal(actual, expected):
                    raise Fail(f"match {path}: expected {json.dumps(expected)[:200]}, got {json.dumps(actual)[:200]}")
            return
        if kind == "length":
            for path, n in arg.items():
                actual = self.lookup(self.subst(path)) if path != "$body" else self.response
                if actual is None or not hasattr(actual, "__len__") or len(actual) != self.subst(n):
                    raise Fail(f"length {path}: expected {n}, got {json.dumps(actual)[:200]}")
            return
        if kind == "exists":
            if self.lookup(self.subst(arg)) is None:
                raise Fail(f"exists {arg}: missing")
            return
        if kind in ("is_true", "is_false"):
            v = self.lookup(self.subst(arg))
            # As the Java runner: only null, false, "", "false" and 0 are false.
            truthy = not (v is None or v is False or v == "" or v == "false" or (type(v) in (int, float) and v == 0))
            if (kind == "is_true") != truthy:
                raise Fail(f"{kind} {arg}: got {json.dumps(v)[:200]}")
            return
        if kind in ("gt", "gte", "lt", "lte"):
            for path, n in arg.items():
                v = self.lookup(self.subst(path))
                n = self.subst(n)
                try:
                    ok = {"gt": v > n, "gte": v >= n, "lt": v < n, "lte": v <= n}[kind]
                except TypeError:
                    ok = False
                if not ok:
                    raise Fail(f"{kind} {path}: {v!r} vs {n!r}")
            return
        if kind == "contains":
            for path, expected in arg.items():
                v = self.lookup(self.subst(path))
                expected = self.subst(expected)
                if isinstance(v, str):
                    ok = isinstance(expected, str) and expected in v
                elif isinstance(v, list):
                    if isinstance(expected, dict):
                        ok = any(isinstance(x, dict) and all(self.equal(x.get(k), e) for k, e in expected.items()) for x in v)
                    else:
                        ok = any(self.equal(x, expected) for x in v)
                else:
                    ok = False
                if not ok:
                    raise Fail(f"contains {path}: {json.dumps(expected)[:120]} not in {json.dumps(v)[:200]}")
            return
        if kind == "close_to":
            for path, spec in arg.items():
                v = self.lookup(self.subst(path))
                if not isinstance(v, (int, float)) or abs(v - spec["value"]) > spec["error"]:
                    raise Fail(f"close_to {path}: {v!r} vs {spec}")
            return
        raise Skip(f"step {kind}")

    def cleanup(self):
        """Back to an empty cluster: scrolls, data streams, every
        non-system index (by name: Elasticsearch refuses wildcard deletes
        by default) and all templates."""
        calls = [("DELETE", "/_search/scroll/_all", {})]
        try:
            status, raw, _ = self.http("GET", "/_cat/indices", {"format": "json", "h": "index", "expand_wildcards": "all"}, None, {}, False)
            names = [r["index"] for r in json.loads(raw)] if status == 200 else []
        except Exception:  # noqa: BLE001
            names = []
        try:
            status, raw, _ = self.http("GET", "/_data_stream", {}, None, {}, False)
            streams = [d["name"] for d in json.loads(raw).get("data_streams", [])] if status == 200 else []
        except Exception:  # noqa: BLE001
            streams = []
        for ds in streams:
            calls.append(("DELETE", f"/_data_stream/{urllib.parse.quote(ds)}", {}))
        # Snapshots and repositories go too (the Java runner's
        # wipeSnapshots): a snapshot left behind would clash by name.
        try:
            status, raw, _ = self.http("GET", "/_snapshot/_all", {}, None, {}, False)
            for repo, spec in (json.loads(raw) if status == 200 else {}).items():
                if spec.get("type") == "fs":
                    calls.append(("DELETE", f"/_snapshot/{urllib.parse.quote(repo)}/*", {}))
                calls.append(("DELETE", f"/_snapshot/{urllib.parse.quote(repo)}", {}))
        except Exception:  # noqa: BLE001
            pass
        # Dot-prefixed indices a test created go too (as the Java runner's
        # wipe does); a real system index just refuses the delete.
        for n in names:
            if not n.startswith(".ds-"):
                calls.append(("DELETE", "/" + urllib.parse.quote(n), {"expand_wildcards": "all"}))
        # Templates by name (wildcard deletes aren't accepted everywhere).
        for listing, key, path in [
            ("/_index_template", "index_templates", "/_index_template/"),
            ("/_component_template", "component_templates", "/_component_template/"),
        ]:
            try:
                status, raw, _ = self.http("GET", listing, {}, None, {}, False)
                for t in json.loads(raw).get(key, []) if status == 200 else []:
                    calls.append(("DELETE", path + urllib.parse.quote(t["name"]), {}))
            except Exception:  # noqa: BLE001
                pass
        try:
            status, raw, _ = self.http("GET", "/_template", {}, None, {}, False)
            for name in json.loads(raw) if status == 200 else {}:
                if not name.startswith("."):
                    calls.append(("DELETE", "/_template/" + urllib.parse.quote(name), {}))
        except Exception:  # noqa: BLE001
            pass
        # Cluster settings a test changed go back to their defaults (the
        # Java runner does the same): a leftover
        # `cluster.routing.allocation.enable: none` breaks every later test.
        try:
            status, raw, _ = self.http("GET", "/_cluster/settings", {"flat_settings": "true"}, None, {}, False)
            cur = json.loads(raw) if status == 200 else {}
            keep = {"action.destructive_requires_name"}
            reset = {k: {n: None for n in (cur.get(k) or {}) if n not in keep} for k in ("persistent", "transient")}
            if any(reset.values()):
                self.http("PUT", "/_cluster/settings", {}, reset, {}, False)
        except Exception:  # noqa: BLE001
            pass
        for method, path, q in calls:
            try:
                status, _, _ = self.http(method, path, q, None, {}, False)
                # An index a failed test left read-only (or metadata-
                # blocked) refuses the delete: lift its blocks and retry.
                if status == 403 and method == "DELETE" and not path.startswith("/_"):
                    unblock = {"index.blocks.read_only": None, "index.blocks.metadata": None}
                    self.http("PUT", path + "/_settings", {}, unblock, {}, False)
                    self.http(method, path, q, None, {}, False)
            except Exception:  # noqa: BLE001
                pass

    def run_file(self, path):
        with open(path) as fh:
            docs = [d for d in yaml.load_all(fh, Loader=Loader) if d]
        setup, teardown, tests = [], [], []
        for d in docs:
            for name, steps in d.items():
                if name == "setup":
                    setup = steps or []
                elif name == "teardown":
                    teardown = steps or []
                else:
                    tests.append((name, steps or []))
        results = []
        try:
            check_prereqs(setup)
            check_prereqs(teardown)
        except Skip as s:
            return [(n, "skip", str(s)) for n, _ in tests]
        for name, steps in tests:
            self.stash = {}
            self.response = None
            try:
                check_prereqs(steps)
                self.cleanup()
                for st in setup:
                    self.step(st)
                for st in steps:
                    self.step(st)
                results.append((name, "pass", ""))
            except Skip as s:
                results.append((name, "skip", str(s)))
            except Fail as f:
                results.append((name, "fail", str(f)))
            except Exception as e:  # noqa: BLE001
                results.append((name, "fail", f"{type(e).__name__}: {e}"))
            finally:
                try:
                    for st in teardown:
                        self.step(st)
                except Exception:
                    pass
                self.cleanup()
        return results


def main():
    global APIS
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    json_out = None
    if "--json" in sys.argv:
        json_out = sys.argv[sys.argv.index("--json") + 1]
        args = [a for a in args if a != json_out]
    base, targets = args[0], args[1:]
    APIS = load_apis()
    files = []
    for t in targets or sorted(os.listdir(TEST_DIR)):
        p = t if os.path.isabs(t) else os.path.join(TEST_DIR, t)
        if os.path.isdir(p):
            files += sorted(os.path.join(p, f) for f in os.listdir(p) if f.endswith(".yml"))
        elif p.endswith(".yml"):
            files.append(p)
    r = Runner(base)
    report = {}
    for f in files:
        d = os.path.basename(os.path.dirname(f))
        # Elasticsearch's own test clusters allow wildcard deletes through
        # node config, which cluster settings APIs don't show; here it is a
        # persistent setting, so it's left out where those APIs are tested.
        allow = None if d.startswith(("cluster.put_settings", "cluster.get_settings")) else False
        r.http("PUT", "/_cluster/settings", {}, {"persistent": {"action.destructive_requires_name": allow}}, {}, False)
        for name, status, why in r.run_file(f):
            report.setdefault(d, []).append({"file": os.path.basename(f), "test": name, "status": status, "why": why})
    tot = {"pass": 0, "fail": 0, "skip": 0}
    for d, rows in sorted(report.items()):
        c = {k: sum(1 for x in rows if x["status"] == k) for k in tot}
        for k in tot:
            tot[k] += c[k]
        print(f"{d:40} pass {c['pass']:4}  fail {c['fail']:4}  skip {c['skip']:4}")
    run = tot["pass"] + tot["fail"]
    print(f"\nTOTAL pass {tot['pass']} / {run} run ({100.0 * tot['pass'] / max(run, 1):.1f}%), skipped {tot['skip']}")
    if json_out:
        with open(json_out, "w") as fh:
            json.dump(report, fh, indent=1, ensure_ascii=False)


if __name__ == "__main__":
    main()
