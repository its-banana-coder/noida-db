"""Elasticsearch edge-case differential: the same requests against a real
Elasticsearch node and noida-db, compared response by response.

    python3 es_edges.py http://127.0.0.1:19200 http://127.0.0.1:9200 [scenario ...]

Standard library only. Responses are normalized before comparing: `took`
is dropped, floats are rounded, and an error compares by HTTP status and
`error.type` (reason texts are printed but not compared). Exit status is
the number of differing steps.
"""

import gzip
import json
import os
import sys
import urllib.error
import urllib.request

REAL, OURS = sys.argv[1], sys.argv[2]
ONLY = set(sys.argv[3:])
VOLATILE = {"took", "_shards", "pit_id", "_scroll_id", "uuid", "index_uuid", "creation_date", "id"}


def call(base, method, path, body=None, headers=None, raw=False):
    data = None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        if isinstance(body, (bytes, bytearray)):
            data = bytes(body)
        elif isinstance(body, list):  # ndjson
            data = ("\n".join(json.dumps(l) for l in body) + "\n").encode()
            hdrs["Content-Type"] = "application/x-ndjson"
        else:
            data = json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, payload, rh = r.status, r.read(), r.headers
    except urllib.error.HTTPError as e:
        status, payload, rh = e.code, e.read(), e.headers
    if rh.get("Content-Encoding") == "gzip":
        payload = gzip.decompress(payload)
    if raw:
        return status, payload.decode(errors="replace"), rh
    try:
        return status, json.loads(payload) if payload else None, rh
    except ValueError:
        return status, payload.decode(errors="replace"), rh


def norm(v):
    if isinstance(v, float):
        return float(f"{v:.5g}")
    if isinstance(v, dict):
        return {k: norm(x) for k, x in v.items() if k not in VOLATILE}
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    return v


def summarize(status, body):
    if isinstance(body, dict) and "error" in body and status >= 400:
        err = body["error"]
        kind = err.get("type") if isinstance(err, dict) else err
        return {"status": status, "error": kind}
    return {"status": status, "body": norm(body)}


class Ctx:
    """Per-server state a later step can refer to (scroll ids, PIT ids)."""

    def __init__(self, base):
        self.base = base
        self.vars = {}

    def subst(self, v):
        if isinstance(v, str):
            for k, x in self.vars.items():
                v = v.replace("{" + k + "}", x)
            return v
        if isinstance(v, dict):
            return {self.subst(k): self.subst(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self.subst(x) for x in v]
        return v


def run_step(ctx, step):
    if step[0] == "sleep":
        import time
        time.sleep(step[1])
        return {"slept": step[1]}
    method, path, body = step[0], step[1], step[2] if len(step) > 2 else None
    opts = step[3] if len(step) > 3 else {}
    status, resp, _ = call(
        ctx.base, method, ctx.subst(path), ctx.subst(body), headers=opts.get("headers")
    )
    for var, key in opts.get("save", {}).items():
        if isinstance(resp, dict) and key in resp:
            ctx.vars[var] = resp[key]
    if "pick" in opts and status < 400:
        resp = opts["pick"](resp)
    return summarize(status, resp)


DOCS = [
    {"title": "The quick brown fox", "body": "jumps over the lazy dog", "price": 10,
     "tags": ["animal", "fast"], "date": "2024-01-15T10:00:00Z", "status": "active"},
    {"title": "Quick brown dogs", "body": "are not as quick as foxes", "price": 25,
     "tags": ["animal"], "date": "2024-01-20T12:30:00Z", "status": "inactive"},
    {"title": "Lazy cats sleep", "body": "all day long in the sun", "price": 5,
     "tags": ["pet", "lazy"], "date": "2024-02-03T08:15:00Z", "status": "active"},
    {"title": "Brown bears", "body": "the brown bear eats honey and fish", "price": 40,
     "tags": ["wild"], "date": "2024-03-10T23:59:59Z", "status": "active"},
    {"title": "Foxes and hounds", "body": "a quick hunt through the forest", "price": 15,
     "tags": ["animal", "hunt"], "date": "2024-03-11T00:00:01Z", "status": "pending"},
]

# Periodic refresh off: Elasticsearch's 1s refresh (and its search-idle
# refresh) would make near-real-time steps depend on timing.
STATIC = {"index": {"refresh_interval": "-1"}}

MAPPING = {
    "settings": STATIC,
    "mappings": {
        "properties": {
            "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "body": {"type": "text"},
            "price": {"type": "integer"},
            "tags": {"type": "keyword"},
            "date": {"type": "date"},
            "status": {"type": "keyword"},
        }
    }
}


def setup(index="edge", mapping=MAPPING, docs=DOCS):
    steps = [("DELETE", f"/{index}?ignore_unavailable=true"), ("PUT", f"/{index}", mapping)]
    bulk = []
    for i, d in enumerate(docs, 1):
        bulk.append({"index": {"_index": index, "_id": str(i)}})
        bulk.append(d)
    steps.append(("POST", "/_bulk?refresh=true", bulk, {"pick": lambda r: r["errors"]}))
    return steps


def hits(r):
    return [(h["_id"], h.get("_score"), h.get("sort")) for h in r["hits"]["hits"]]


def ids(r):
    return [h["_id"] for h in r["hits"]["hits"]]


S = "/edge/_search"
SCENARIOS = {}


def scenario(name, steps):
    SCENARIOS[name] = steps


scenario("sort_values", setup() + [
    ("POST", S, {"sort": [{"price": "desc"}], "size": 3}),
    ("POST", S, {"sort": ["price"], "query": {"match": {"title": "quick"}}}),
    ("POST", S, {"sort": [{"price": "asc"}], "track_scores": True, "query": {"match": {"title": "quick"}}}),
    ("POST", S, {"sort": [{"date": "asc"}], "size": 2}),
    ("POST", S, {"sort": [{"title.raw": "asc"}], "size": 2}),
    ("POST", S, {"sort": [{"status": "asc"}, {"price": "desc"}]}),
    ("POST", S, {"sort": [{"nope": {"order": "asc", "unmapped_type": "long"}}], "size": 2}),
    ("POST", S, {"sort": [{"nope": "asc"}]}),
    ("POST", S, {"sort": [{"_score": "desc"}, {"price": "asc"}], "query": {"match": {"body": "quick"}}}),
])

scenario("search_after", setup() + [
    ("POST", S, {"sort": [{"price": "asc"}], "size": 2}, {"pick": hits}),
    ("POST", S, {"sort": [{"price": "asc"}], "size": 2, "search_after": [10]}, {"pick": hits}),
    ("POST", S, {"sort": [{"price": "asc"}], "size": 2, "search_after": [25]}, {"pick": hits}),
    ("POST", S, {"sort": [{"price": "asc"}], "size": 2, "search_after": [40]}, {"pick": hits}),
    ("POST", S, {"sort": [{"status": "asc"}, {"price": "asc"}], "size": 2, "search_after": ["active", 10]}, {"pick": hits}),
    ("POST", S, {"size": 2, "search_after": [10]}),
    ("POST", S, {"sort": [{"price": "asc"}], "from": 1, "search_after": [10]}),
])

scenario("scroll", setup() + [
    ("POST", "/edge/_search?scroll=1m", {"size": 2, "sort": ["price"]},
     {"save": {"sid": "_scroll_id"}, "pick": hits}),
    ("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "{sid}"}, {"pick": hits}),
    ("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "{sid}"}, {"pick": hits}),
    ("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "{sid}"}, {"pick": hits}),
    ("DELETE", "/_search/scroll", {"scroll_id": "{sid}"}),
    ("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "{sid}"}),
    # How many contexts `_all` frees depends on what else is open.
    ("DELETE", "/_search/scroll/_all", None, {"pick": lambda r: r["succeeded"]}),
])

scenario("pit", setup() + [
    ("POST", "/edge/_pit?keep_alive=1m", None, {"save": {"pit": "id"}, "pick": lambda r: sorted(r)}),
    ("POST", "/edge/_doc/9?refresh=true", {"title": "new doc after pit", "price": 1}),
    ("POST", "/_search", {"pit": {"id": "{pit}", "keep_alive": "1m"}, "sort": [{"price": "asc"}], "size": 10},
     {"pick": hits}),
    ("POST", "/_search", {"pit": {"id": "{pit}"}, "sort": [{"price": "asc"}], "size": 2, "search_after": [10]},
     {"pick": hits}),
    # search_after carrying the implicit _shard_doc tiebreaker's value.
    ("POST", "/_search", {"pit": {"id": "{pit}"}, "sort": [{"price": "asc"}], "size": 2, "search_after": [10, 0]},
     {"pick": hits}),
    ("POST", "/_search", {"pit": {"id": "{pit}"}, "sort": [{"price": "asc"}], "size": 2, "search_after": [10, 0, 1]}),
    ("POST", "/edge/_search", {"pit": {"id": "{pit}"}}),
    ("DELETE", "/_pit", {"id": "{pit}"}, {"pick": lambda r: r}),
    ("POST", "/_search", {"pit": {"id": "{pit}"}}),
])

scenario("query_string", setup() + [
    ("POST", S, {"query": {"query_string": {"query": "quick"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "title:quick AND body:lazy"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "title:(quick OR lazy)"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "brown -bears", "default_field": "title"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "\"quick brown\"", "default_field": "title"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "price:[10 TO 20]"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "price:>20"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "fox*", "default_field": "title"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "status:active AND NOT tags:wild"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "_exists_:tags"}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "quick brown", "default_operator": "AND", "fields": ["title", "body"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"query_string": {"query": "title:(quick"}}}),
    ("POST", S, {"query": {"query_string": {"query": "quick", "fields": ["title^3", "body"]}}}, {"pick": hits}),
])

scenario("simple_query_string", setup() + [
    ("POST", S, {"query": {"simple_query_string": {"query": "quick fox"}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "+quick -lazy", "fields": ["title", "body"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "\"brown fox\"", "fields": ["title"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "fox*", "fields": ["title"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "cats | bears", "fields": ["title"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "title:(quick", "fields": ["title"]}}}, {"pick": ids}),
    ("POST", S, {"query": {"simple_query_string": {"query": "quick brown", "default_operator": "and", "fields": ["title"]}}}, {"pick": ids}),
])

scenario("highlight", setup() + [
    ("POST", S, {"query": {"match": {"title": "quick"}}, "highlight": {"fields": {"title": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"match": {"body": "quick"}},
                 "highlight": {"pre_tags": ["<b>"], "post_tags": ["</b>"], "fields": {"body": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"multi_match": {"query": "brown", "fields": ["title", "body"]}},
                 "highlight": {"fields": {"title": {}, "body": {}}}},
     {"pick": lambda r: sorted((h["_id"], json.dumps(h.get("highlight"), sort_keys=True)) for h in r["hits"]["hits"])}),
    ("POST", S, {"query": {"match": {"title": "quick"}}, "highlight": {"fields": {"body": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"match": {"title": "quick"}},
                 "highlight": {"require_field_match": False, "fields": {"body": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"match_phrase": {"body": "brown bear"}}, "highlight": {"fields": {"body": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"term": {"status": "active"}}, "highlight": {"fields": {"status": {}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
    ("POST", S, {"query": {"match": {"title": "fox"}}, "highlight": {"fields": {"title": {"number_of_fragments": 0}}}},
     {"pick": lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]}),
])

LONG_DOCS = [
    {"text": "Elasticsearch is a distributed search engine. It stores documents as JSON. "
             "Search is fast because of the inverted index! Do you like search? "
             "The quick brown fox jumps over the lazy dog while the search engine indexes everything "
             "in near real time, which makes search results visible about one second after indexing.",
     "tags": ["search", "engine", "json"], "title": "Search engines"},
    {"text": "Nothing to see here", "tags": ["misc"], "title": "Other"},
]
LONG_MAPPING = {"settings": STATIC, "mappings": {"properties": {"text": {"type": "text"}, "tags": {"type": "keyword"},
                                            "title": {"type": "text"}}}}
HL = lambda r: [(h["_id"], h.get("highlight")) for h in r["hits"]["hits"]]
L = "/long/_search"
scenario("highlight_long", setup("long", LONG_MAPPING, LONG_DOCS) + [
    ("POST", L, {"query": {"match": {"text": "search"}}, "highlight": {"fields": {"text": {}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "search"}}, "highlight": {"fields": {"text": {"number_of_fragments": 2}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "search"}}, "highlight": {"fields": {"text": {"fragment_size": 30}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "index"}}, "highlight": {"fields": {"text": {"fragment_size": 20, "number_of_fragments": 3}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "index"}}, "highlight": {"fields": {"title": {"no_match_size": 10}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "index"}},
                 "highlight": {"require_field_match": False, "fields": {"title": {"no_match_size": 10}}}}, {"pick": HL}),
    ("POST", L, {"query": {"terms": {"tags": ["search", "json"]}}, "highlight": {"fields": {"tags": {}}}}, {"pick": HL}),
    ("POST", L, {"query": {"query_string": {"query": "text:(fox OR dog) AND title:search*"}},
                 "highlight": {"fields": {"*": {}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match_phrase": {"text": "search engine"}}, "highlight": {"fields": {"text": {"number_of_fragments": 0}}}}, {"pick": HL}),
    ("POST", L, {"query": {"prefix": {"text": "ind"}}, "highlight": {"fields": {"text": {"pre_tags": ["["], "post_tags": ["]"]}}}}, {"pick": HL}),
    ("POST", L, {"query": {"fuzzy": {"text": "serch"}}, "highlight": {"fields": {"text": {"number_of_fragments": 1}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "search"}}, "highlight": {"order": "score", "fields": {"text": {"number_of_fragments": 2}}}}, {"pick": HL}),
    ("POST", L, {"query": {"match": {"text": "nothing"}}, "highlight": {"fields": {"text": {}}}, "_source": False}, {"pick": HL}),
])


# Highlighting beyond the basics: highlighter types, tag schemas,
# encoders, per-field queries, multi-fields, boundary scanners, nested
# inner hits and the errors apps hit.
HLM_DOCS = [
    {"title": "The quick brown fox", "tags": ["fox", "animal"], "num": 42, "k": "abc", "kk": "abcdef",
     "html": "<b>Fish & chips</b> are quick to cook",
     "body": "The quick brown fox jumps over the lazy dog. Foxes are quick and clever animals! "
             "Did you know a fox can run fast? The brown fox lives in the forest near the river, "
             "where quick streams flow and the lazy dog never goes because it is far too lazy.",
     "multi": ["first quick value", "second slow value", "third quick one here"],
     "comments": [{"author": "ann", "text": "quick reply here"}, {"author": "bob", "text": "a slow answer"},
                  {"author": "cy", "text": "another quick note"}]},
    {"title": "Lazy dogs and quick cats", "tags": ["dog"], "num": 7, "k": "xyz", "kk": "x",
     "html": "quick <i>tags</i>", "body": "Nothing about foxes here, only quick cats and lazy dogs.",
     "multi": ["slow"], "comments": [{"author": "dan", "text": "nothing"}]},
]
HLM_MAPPING = {"settings": STATIC,
               "mappings": {"properties": {
    "title": {"type": "text", "fields": {"raw": {"type": "keyword"}, "std": {"type": "text"}}},
    "body": {"type": "text"}, "tags": {"type": "keyword"}, "num": {"type": "integer"},
    "k": {"type": "keyword"}, "kk": {"type": "keyword", "ignore_above": 3}, "html": {"type": "text"},
    "multi": {"type": "text"}, "tv": {"type": "text", "term_vector": "with_positions_offsets"},
    "comments": {"type": "nested", "properties": {"author": {"type": "keyword"}, "text": {"type": "text"}}},
}}}
for d in HLM_DOCS:
    d["tv"] = d["title"]
HM = "/hl-m/_search"
def hl(q, h, **kw):
    b = {"query": q, "highlight": h}
    b.update(kw)
    return ("POST", HM, b, {"pick": HL})
scenario("highlight_more", setup("hl-m", HLM_MAPPING, HLM_DOCS) + [
    hl({"multi_match": {"query": "quick fox", "fields": ["t*"]}}, {"fields": {"*": {}}}),
    hl({"multi_match": {"query": "quick fox", "fields": ["title*"]}}, {"fields": {"title*": {}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"*": {}}}),
    hl({"match_phrase": {"body": "quick brown fox"}}, {"fields": {"body": {}}}),
    hl({"match_phrase": {"body": "quick brown fox"}}, {"fields": {"body": {"type": "plain"}}}),
    hl({"match_phrase": {"body": "lazy dog"}}, {"fields": {"body": {"number_of_fragments": 0}}}),
    hl({"match": {"body": "quick fox"}}, {"tags_schema": "styled", "fields": {"body": {"number_of_fragments": 0}}}),
    hl({"match": {"body": "quick fox"}}, {"pre_tags": ["<a>", "<b>"], "post_tags": ["</a>", "</b>"], "fields": {"body": {"number_of_fragments": 0}}}),
    hl({"match": {"html": "quick fish"}}, {"encoder": "html", "fields": {"html": {}}}),
    hl({"match": {"html": "quick fish"}}, {"fields": {"html": {}}}),
    hl({"match": {"multi": "quick"}}, {"fields": {"multi": {}}}),
    hl({"match": {"multi": "quick"}}, {"fields": {"multi": {"number_of_fragments": 0}}}),
    hl({"match": {"multi": "quick"}}, {"fields": {"multi": {"number_of_fragments": 1}}}),
    hl({"match": {"multi": "quick"}}, {"fields": {"multi": {"type": "plain"}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"body": {"no_match_size": 20}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"body": {"no_match_size": 20, "number_of_fragments": 0}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"title": {"highlight_query": {"match": {"title": "fox"}}}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"title": {"matched_fields": ["title", "title.std"]}}}),
    hl({"match": {"title.std": "quick"}}, {"fields": {"title": {"matched_fields": ["title", "title.std"]}}}),
    hl({"match": {"body": "fox"}}, {"fields": {"body": {"fragment_size": 30, "boundary_scanner": "word"}}}),
    hl({"match": {"body": "fox"}}, {"fields": {"body": {"fragment_size": 30, "boundary_scanner": "sentence"}}}),
    hl({"match": {"body": "fox"}}, {"fields": {"body": {"type": "plain", "fragment_size": 30}}}),
    hl({"match": {"body": "fox"}}, {"fields": {"body": {"type": "plain", "fragment_size": 30, "number_of_fragments": 2}}}),
    hl({"match": {"body": "lazy"}}, {"fields": {"body": {"type": "plain", "number_of_fragments": 0}}}),
    hl({"match": {"body": "river"}}, {"fields": {"body": {}}}),
    hl({"match": {"body": "river"}}, {"max_analyzed_offset": 20, "fields": {"body": {}}}),
    hl({"match": {"body": "river"}}, {"max_analyzed_offset": -1, "fields": {"body": {}}}),
    hl({"match": {"tv": "quick"}}, {"fields": {"tv": {"type": "fvh"}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"title": {"type": "fvh"}}}),
    hl({"term": {"k": "abc"}}, {"fields": {"k": {}}}),
    hl({"term": {"num": 42}}, {"fields": {"num": {}}}),
    hl({"prefix": {"kk": "ab"}}, {"fields": {"kk": {}}}),
    hl({"match": {"title": "quick"}}, {"pre_tags": [], "post_tags": [], "fields": {"title": {}}}),
    hl({"match": {"title": "quick"}}, {"fields": [{"title": {}}, {"body": {}}]}),
    hl({"fuzzy": {"title": {"value": "quikc"}}}, {"fields": {"title": {}}}),
    hl({"match": {"title": {"query": "quick lazy", "operator": "and"}}}, {"fields": {"title": {}}}),
    hl({"bool": {"must": {"match": {"title": "quick"}}, "must_not": {"match": {"title": "cats"}}}}, {"fields": {"title": {}}}),
    hl({"query_string": {"query": "qui*"}}, {"fields": {"title": {}, "body": {"number_of_fragments": 1}}}),
    hl({"match": {"body": "quick"}}, {"order": "score", "fields": {"body": {"fragment_size": 40, "number_of_fragments": 2}}}),
    hl({"nested": {"path": "comments", "query": {"match": {"comments.text": "quick"}},
                   "inner_hits": {"highlight": {"fields": {"comments.text": {}}}}}}, {"fields": {"title": {}}}),
    ("POST", HM, {"query": {"nested": {"path": "comments", "query": {"match": {"comments.text": "quick"}},
                                       "inner_hits": {"_source": False, "highlight": {"fields": {"comments.text": {}}}}}}},
     {"pick": lambda r: [(h["_id"], h.get("inner_hits")) for h in r["hits"]["hits"]]}),
    hl({"match": {"title": "quick"}}, {"type": "nope", "fields": {"title": {}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"title": {"fragmenter": "simple", "type": "plain", "fragment_size": 10}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"title": {"fragmenter": "nope", "type": "plain"}}}),
    hl({"match": {"html": "quick fish"}}, {"encoder": "html", "fields": {"html": {"type": "plain"}}}),
    hl({"match": {"title": "quick"}}, {"fields": {"body": {"type": "plain", "no_match_size": 20}}}),
    hl({"match": {"multi": "quick"}}, {"fields": {"multi": {"type": "plain", "number_of_fragments": 0}}}),
    hl({"match": {"multi": "quick value"}}, {"order": "score", "fields": {"multi": {"type": "plain"}}}),
    hl({"terms": {"tags": ["fox", "dog"]}}, {"fields": {"tags": {"type": "plain"}}}),
    hl({"match": {"body": "quick lazy"}}, {"fields": {"body": {"type": "plain", "fragment_size": 50, "number_of_fragments": 3, "order": "score"}}}),
    hl({"match_phrase": {"body": "lazy dog"}}, {"fields": {"body": {"type": "plain", "fragment_size": 20}}}),
    hl({"match_phrase": {"body": "lazy dog"}}, {"fields": {"body": {"type": "plain", "fragment_size": 20, "number_of_fragments": 2}}}),
    hl({"match": {"multi": "quick value"}}, {"fields": {"multi": {"type": "plain", "number_of_fragments": 1}}}),
    hl({"nested": {"path": "comments", "query": {"match": {"comments.text": "quick"}}}}, {"fields": {"*": {}}}),
    hl({"bool": {"should": [{"match_phrase": {"body": "lazy dog"}},
                            {"nested": {"path": "comments", "query": {"match": {"comments.text": "quick"}}}}]}},
       {"fields": {"body": {"number_of_fragments": 0}}}),
    hl({"match_phrase": {"title": "quick brown"}}, {"fields": {"title": {}, "title.std": {}, "tv": {"type": "fvh"}}}),
    hl({"match": {"body": "fox"}}, {"fields": {"body": {"boundary_scanner": "chars"}}}),
    hl({"match": {"body": "fox"}}, {"tags_schema": "nope", "fields": {"body": {}}}),
    hl({"match": {"body": "fox"}}, {"pre_tags": ["<x>"], "fields": {"body": {}}}),
    hl({"match": {"title": "quick"}}, {"require_field_match": False, "fields": {"title": {"matched_fields": ["title.std"]}}}),
    ("DELETE", "/hl-m"),
] + setup("hl-o", {"settings": {"index": {"refresh_interval": "-1", "highlight.max_analyzed_offset": 30}},
                   "mappings": {"properties": {"f": {"type": "text"}, "g": {"type": "text", "index_options": "offsets"}}}},
          [{"f": "The quick brown fox went to the forest and saw another fox.",
            "g": "The quick brown fox went to the forest and saw another fox."}]) + [
    ("POST", "/hl-o/_search", {"query": {"match": {"f": "fox"}}, "highlight": {"fields": {"f": {}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"f": "fox"}}, "highlight": {"fields": {"f": {"type": "plain"}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"g": "fox"}}, "highlight": {"fields": {"g": {}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"f": "fox"}}, "highlight": {"max_analyzed_offset": 20, "fields": {"f": {}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"f": "fox"}}, "highlight": {"max_analyzed_offset": 18, "fields": {"f": {}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"g": "fox"}}, "highlight": {"max_analyzed_offset": 20, "fields": {"g": {"type": "plain"}}}}, {"pick": HL}),
    ("POST", "/hl-o/_search", {"query": {"match": {"f": "fox"}}, "highlight": {"max_analyzed_offset": 50, "fields": {"f": {}}}}, {"pick": HL}),
    ("DELETE", "/hl-o"),
])

# Suggesters: term ("did you mean"), phrase, and completion (autocomplete)
# with weights, fuzzy, regex and category/geo contexts.
SUG_DOCS = [
    {"title": "Amsterdam meetup", "body": "The quick brown fox jumps", "genre": "music",
     "loc": {"lat": 52.37, "lon": 4.89},
     "sug": {"input": ["Nevermind", "Nirvana"], "weight": 34},
     "csug": {"input": "Star Wars", "contexts": {"kind": ["movie", "space"]}}},
    {"title": "Amsterdam city guide", "body": "quick brown foxes jumped over the lazy dog", "genre": "travel",
     "loc": {"lat": 52.36, "lon": 4.90},
     "sug": ["Nirvana band", "Nine Inch-Nails", "nirvana"],
     "csug": {"input": "Star Trek", "weight": 5, "contexts": {"kind": "space"}}},
    {"title": "Berlin meetup", "body": "lazy dogs sleep all day", "genre": "music",
     "loc": {"lat": 52.52, "lon": 13.40},
     "sug": {"input": "Nirvaan", "weight": 3},
     "csug": {"input": "Stargate", "contexts": {"kind": "movie"}}},
    {"title": "Amsterdam museums and meetups", "body": "the brown bear eats honey", "genre": "travel",
     "loc": {"lat": 48.85, "lon": 2.35},
     "sug": {"input": ["Nirvana", "Neon Indian"], "weight": 2},
     "csug": {"input": "Star Wars", "contexts": {"kind": "movie"}}},
    {"title": "Berlinn guide", "body": "brown brwn browns", "genre": "misc",
     "loc": {"lat": 52.53, "lon": 13.41},
     "sug": "Nirvana", "csug": {"input": "Starship", "contexts": {"kind": "space"}}},
]
SUG_MAPPING = {"settings": STATIC, "mappings": {"properties": {
    "title": {"type": "text"}, "body": {"type": "text"}, "genre": {"type": "keyword"},
    "loc": {"type": "geo_point"},
    "sug": {"type": "completion"},
    "csug": {"type": "completion", "contexts": [{"name": "kind", "type": "category"}]},
    "gsug": {"type": "completion", "contexts": [{"name": "genre", "type": "category", "path": "genre"},
                                                {"name": "where", "type": "geo", "precision": "10km", "path": "loc"}]},
}}}
for d in SUG_DOCS:
    d["gsug"] = d["title"]
SG = "/sug-t/_search"
SUG = lambda r: r.get("suggest")
scenario("suggest", setup("sug-t", SUG_MAPPING, SUG_DOCS) + [
    ("GET", "/sug-t/_mapping"),
    # term
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "term": {"field": "title"}}}}),
    ("POST", SG, {"suggest": {"text": "the amsterdma meetpu", "s": {"term": {"field": "title"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwn brown", "term": {"field": "body", "suggest_mode": "always"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwn brown", "term": {"field": "body", "suggest_mode": "popular"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwn", "term": {"field": "body", "sort": "frequency"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwnn", "term": {"field": "body", "max_edits": 1}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "rown", "term": {"field": "body", "prefix_length": 0}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "laz dgo", "term": {"field": "body", "min_word_length": 2}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwn", "term": {"field": "body", "size": 1}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "bronw berlni", "term": {"field": "body", "string_distance": "levenshtein"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "bronw berlni", "term": {"field": "title", "string_distance": "damerau_levenshtein"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "bronw berlni", "term": {"field": "title", "string_distance": "jaro_winkler"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "bronw berlni", "term": {"field": "body", "string_distance": "ngram"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "musik", "term": {"field": "genre"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "x", "term": {"field": "nope"}}}}),
    ("POST", SG, {"suggest": {"s": {"text": "x", "term": {}}}}),
    ("POST", SG, {"suggest": {"s": {"term": {"field": "title"}}}}),
    ("POST", SG, {"suggest": {"s": {"text": "x", "term": {"field": "title", "suggest_mode": "nope"}}}}),
    ("POST", SG, {"query": {"match": {"title": "meetup"}}, "suggest": {"s": {"text": "meetpu", "term": {"field": "title"}}}}, {"pick": lambda r: (ids(r), r["hits"]["total"], r["suggest"])}),
    ("POST", SG, {"size": 0, "suggest": {"s": {"text": "meetpu", "term": {"field": "title"}}}}),
    ("POST", SG + "?typed_keys=true", {"suggest": {"t": {"text": "meetpu", "term": {"field": "title"}},
                                                   "c": {"prefix": "nir", "completion": {"field": "sug"}},
                                                   "p": {"text": "meetpu", "phrase": {"field": "title"}}}}, {"pick": lambda r: sorted(r["suggest"])}),
    # phrase
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "phrase": {"field": "title"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "phrase": {"field": "title", "max_errors": 2}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "phrase": {"field": "title", "size": 1, "highlight": {"pre_tag": "<em>", "post_tag": "</em>"}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "berlni gide", "phrase": {"field": "title", "confidence": 0, "max_errors": 0.5}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "lazy dgo", "phrase": {"field": "body", "smoothing": {"laplace": {"alpha": 0.7}}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "brwn foxs", "phrase": {"field": "body", "gram_size": 2, "max_errors": 2,
                                                                   "direct_generator": [{"field": "body", "suggest_mode": "always", "min_word_length": 3}]}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "phrase": {"field": "title", "max_errors": 2,
                                                                           "collate": {"query": {"source": {"match_phrase": {"title": "{{suggestion}}"}}}, "prune": True}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "amsterdma meetpu", "phrase": {"field": "title", "max_errors": 2,
                                                                           "collate": {"query": {"source": {"match_phrase": {"title": "{{suggestion}}"}}}}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"text": "x", "phrase": {"field": "nope"}}}}),
    # completion
    ("POST", SG, {"suggest": {"s": {"prefix": "nir", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nir", "completion": {"field": "sug", "skip_duplicates": True}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nir", "completion": {"field": "sug", "size": 2}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "NINE inch-n", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nrv", "completion": {"field": "sug", "fuzzy": {"fuzziness": 1}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nirv", "completion": {"field": "sug", "fuzzy": True}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nivrana", "completion": {"field": "sug", "fuzzy": {}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"regex": "n[aeiou]r", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"regex": "ne.*", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"text": "nev", "s": {"completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug", "contexts": {"kind": "movie"}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug", "contexts": {"kind": [{"context": "movie", "boost": 3}, "space"]}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug", "skip_duplicates": True, "contexts": {"kind": ["movie", "space"]}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug", "contexts": {"kind": [{"context": "mo", "prefix": True}]}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug"}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "csug", "contexts": {"zz": "movie"}}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "a", "completion": {"field": "gsug", "contexts": {"genre": "music"}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "b", "completion": {"field": "gsug", "contexts": {"where": {"lat": 52.52, "lon": 13.40}}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "", "completion": {"field": "gsug", "contexts": {"where": [{"context": {"lat": 52.37, "lon": 4.89}, "boost": 2}, {"lat": 52.52, "lon": 13.40}]}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "a", "completion": {"field": "gsug", "contexts": {"where": {"context": {"lat": 52.0, "lon": 5.0}, "precision": 2}}}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "title"}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {"field": "nope"}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "completion": {}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "st", "nope": {"field": "sug"}}}}),
    ("POST", SG, {"suggest": {"s": {"completion": {"field": "sug"}}}}),
    ("POST", SG, {"suggest": {"s": {"prefix": "123", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("POST", SG, {"suggest": {"s": {"prefix": "nir", "completion": {"field": "sug"}}}, "aggs": {"g": {"terms": {"field": "genre"}}}},
     {"pick": lambda r: (r["hits"]["total"], r["aggregations"], len(r["suggest"]["s"][0]["options"]))}),
    ("PUT", "/sug-t/_doc/9", {"csug": {"input": "Solo"}}),
    ("PUT", "/sug-t/_doc/9", {"sug": {"input": "x", "weight": "abc"}}),
    ("PUT", "/sug-t/_doc/9", {"sug": {"input": "x", "weight": -1}}),
    ("PUT", "/sug-t/_doc/9", {"sug": {"inputs": "x"}}),
    ("PUT", "/sug-t/_doc/9", {"sug": 5}),
    ("PUT", "/sug-t/_doc/9", {"sug": {"input": "Zebra", "weight": "7"}, "csug": {"input": "Solo", "contexts": {"kind": "movie"}}}),
    ("POST", "/sug-t/_refresh"),
    ("POST", SG, {"suggest": {"s": {"prefix": "z", "completion": {"field": "sug"}}}}, {"pick": SUG}),
    ("DELETE", "/sug-t"),
])

scenario("date_math_range", setup() + [
    ("POST", S, {"query": {"range": {"date": {"gte": "2024-01-15", "lt": "2024-02-01"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "2024-01-15||+1M/M"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gt": "2024-03-10||/d"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "2024-03-10||/d", "lte": "2024-03-10||/d"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"lt": "2024-03-10||/d"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "15/01/2024", "format": "dd/MM/yyyy"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": 1705312800000}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "now-100y", "lte": "now"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "2024-01-15T10:00:00+05:00"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "2024-03-11", "time_zone": "+01:00"}}}}, {"pick": ids}),
    ("POST", S, {"query": {"range": {"date": {"gte": "not a date"}}}}),
])

scenario("date_histogram", setup() + [
    ("POST", S, {"size": 0, "aggs": {"m": {"date_histogram": {"field": "date", "calendar_interval": "month"}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"w": {"date_histogram": {"field": "date", "calendar_interval": "1w"}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"d": {"date_histogram": {"field": "date", "fixed_interval": "10d", "min_doc_count": 1}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"m": {"date_histogram": {"field": "date", "calendar_interval": "month", "format": "yyyy-MM"}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"m": {"date_histogram": {"field": "date", "calendar_interval": "month",
                 "extended_bounds": {"min": "2023-12-01", "max": "2024-05-01"}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"m": {"date_histogram": {"field": "date", "calendar_interval": "month"},
                 "aggs": {"p": {"sum": {"field": "price"}}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"d": {"date_histogram": {"field": "date", "calendar_interval": "day", "time_zone": "+05:00", "min_doc_count": 1}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"q": {"date_histogram": {"field": "date", "calendar_interval": "quarter"}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"h": {"date_histogram": {"field": "date", "fixed_interval": "12h", "min_doc_count": 1, "order": {"_count": "desc"}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", S, {"size": 0, "aggs": {"bad": {"date_histogram": {"field": "date", "calendar_interval": "2M"}}}}),
    ("POST", S, {"size": 0, "aggs": {"r": {"date_range": {"field": "date", "ranges": [{"to": "2024-02-01"}, {"from": "2024-02-01"}]}}}},
     {"pick": lambda r: r["aggregations"]}),
])

NESTED_MAPPING = {"settings": STATIC, "mappings": {"properties": {
    "name": {"type": "keyword"},
    "comments": {"type": "nested", "properties": {
        "author": {"type": "keyword"}, "stars": {"type": "integer"}, "text": {"type": "text"}}},
}}}
NESTED_DOCS = [
    {"name": "a", "comments": [{"author": "kim", "stars": 5, "text": "great product"},
                               {"author": "lee", "stars": 1, "text": "awful"}]},
    {"name": "b", "comments": [{"author": "kim", "stars": 1, "text": "bad"},
                               {"author": "lee", "stars": 5, "text": "great"}]},
    {"name": "c", "comments": []},
    {"name": "d"},
]
N = "/nest/_search"
scenario("nested", setup("nest", NESTED_MAPPING, NESTED_DOCS) + [
    ("POST", N, {"query": {"nested": {"path": "comments", "query": {"bool": {"must": [
        {"term": {"comments.author": "kim"}}, {"term": {"comments.stars": 5}}]}}}}}, {"pick": ids}),
    ("POST", N, {"query": {"bool": {"must": [
        {"term": {"comments.author": "kim"}}, {"term": {"comments.stars": 5}}]}}}, {"pick": ids}),
    ("POST", N, {"query": {"nested": {"path": "comments", "query": {"match": {"comments.text": "great"}},
                 "inner_hits": {}}}},
     {"pick": lambda r: [(h["_id"], [(ih["_nested"], ih["_source"]) for ih in h["inner_hits"]["comments"]["hits"]["hits"]]) for h in r["hits"]["hits"]]}),
    ("POST", N, {"query": {"nested": {"path": "comments", "query": {"range": {"comments.stars": {"gte": 2}}}, "score_mode": "max"}}}, {"pick": ids}),
    ("POST", N, {"size": 0, "aggs": {"c": {"nested": {"path": "comments"}, "aggs": {"by": {"terms": {"field": "comments.author"}}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", N, {"query": {"nested": {"path": "nope", "query": {"match_all": {}}}}}),
    ("POST", N, {"query": {"nested": {"path": "nope", "query": {"match_all": {}}, "ignore_unmapped": True}}}, {"pick": ids}),
    ("POST", N, {"query": {"exists": {"field": "comments"}}}, {"pick": ids}),
])

scenario("painless_update", setup() + [
    ("POST", "/edge/_update/1", {"script": {"source": "ctx._source.price += params.n", "params": {"n": 5}}}),
    ("GET", "/edge/_doc/1", None, {"pick": lambda r: r["_source"]["price"]}),
    ("POST", "/edge/_update/1", {"script": {"source": "ctx._source.tags.add(params.t)", "params": {"t": "new"}}}),
    ("GET", "/edge/_doc/1", None, {"pick": lambda r: r["_source"]["tags"]}),
    ("POST", "/edge/_update/1", {"script": "ctx._source.status = 'archived'"}),
    ("GET", "/edge/_doc/1", None, {"pick": lambda r: r["_source"]["status"]}),
    ("POST", "/edge/_update/1", {"script": {"source": "if (ctx._source.price > 100) { ctx._source.big = true } else { ctx.op = 'noop' }"}}),
    ("POST", "/edge/_update/2", {"script": {"source": "ctx._source.remove('body')"}}),
    ("GET", "/edge/_doc/2", None, {"pick": lambda r: sorted(r["_source"])}),
    ("POST", "/edge/_update/2", {"script": {"source": "ctx.op = 'delete'"}}),
    ("GET", "/edge/_doc/2"),
    ("POST", "/edge/_update/77", {"script": {"source": "ctx._source.n += 1"}, "upsert": {"n": 0}}),
    ("GET", "/edge/_doc/77", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/edge/_update/77", {"script": {"source": "ctx._source.n += 1"}, "upsert": {"n": 0}}),
    ("GET", "/edge/_doc/77", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/edge/_update/3", {"script": {"source": "ctx._source.title = ctx._source.title.toUpperCase() + '!'"}}),
    ("GET", "/edge/_doc/3", None, {"pick": lambda r: r["_source"]["title"]}),
    ("POST", "/edge/_update/3", {"script": {"source": "ctx._source.missing.foo = 1"}}),
    ("POST", "/edge/_update/3", {"script": {"source": "this is not painless ((("}}),
    ("POST", "/edge/_update_by_query?refresh=true", {"query": {"term": {"status": "active"}},
                                                     "script": {"source": "ctx._source.price *= 2"}},
     {"pick": lambda r: (r["updated"], r["total"])}),
    ("POST", S, {"query": {"term": {"status": "active"}}, "sort": ["price"]}, {"pick": lambda r: [h["_source"]["price"] for h in r["hits"]["hits"]]}),
])

SRC = lambda r: r.get("_source")
scenario("painless_more", setup() + [
    ("POST", "/edge/_update/1", {"doc": {"price": 10}}),
    ("POST", "/edge/_update/1", {"doc": {"price": 10}, "detect_noop": False}),
    ("POST", "/edge/_update/1", {"doc": {"extra": {"a": 1}}}),
    ("POST", "/edge/_update/1", {"doc": {"extra": {"b": 2}}}),
    ("GET", "/edge/_doc/1", None, {"pick": SRC}),
    ("POST", "/edge/_update/1", {"script": {"source": """
        int total = 0;
        for (def t : ctx._source.tags) { total += t.length(); }
        ctx._source.tag_len = total;
        ctx._source.counts = new HashMap();
        for (int i = 0; i < 3; i++) { ctx._source.counts.put('k' + i, i * i); }
        ctx._source.ratio = ctx._source.price / 4;
        ctx._source.fratio = ctx._source.price / 4.0;
        ctx._source.tags.removeIf(x -> x == 'fast');
        ctx._source.flag = ctx._source.containsKey('missing') ? 'yes' : 'no';
    """}}),
    ("GET", "/edge/_doc/1", None, {"pick": SRC}),
    ("POST", "/edge/_update/88", {"scripted_upsert": True, "script": {"source": "ctx._source.hits = params.n * 2", "params": {"n": 21}}, "upsert": {}}),
    ("GET", "/edge/_doc/88", None, {"pick": SRC}),
    ("POST", "/edge/_update/89", {"doc": {"a": 1}, "doc_as_upsert": True}),
    ("GET", "/edge/_doc/89", None, {"pick": SRC}),
    ("POST", "/edge/_update/90", {"doc": {"a": 1}}),
    ("POST", "/edge/_update/1", {}),
    ("POST", "/edge/_update/1", {"script": {"source": "ctx._source.x = params.missing.y"}}),
    ("POST", "/edge/_update/1", {"script": {"source": "ctx._source.x = 1 / 0"}}),
    ("POST", "/edge/_update/1", {"script": {"source": "ctx.op = 'banana'"}}),
    ("POST", "/edge/_update/1?if_seq_no=0&if_primary_term=1", {"doc": {"z": 1}}),
    ("POST", "/edge/_update/2?refresh=true", {"doc": {"status": "active"}}),
    ("POST", "/edge/_search", {"query": {"term": {"status": "active"}}}, {"pick": ids}),
    ("POST", "/_bulk?refresh=true", [{"update": {"_index": "edge", "_id": "3"}},
                                      {"script": {"source": "ctx._source.price += 1000"}}],
     {"pick": lambda r: [(list(i)[0], i[list(i)[0]]["result"]) for i in r["items"]]}),
    ("GET", "/edge/_doc/3", None, {"pick": lambda r: r["_source"]["price"]}),
    ("POST", "/edge/_refresh"),
    ("POST", "/edge/_update/4", {"doc": {"price": 41}}),
    ("POST", "/edge/_delete_by_query", {"query": {"range": {"price": {"gte": 30}}}}),
    ("POST", "/edge/_delete_by_query?conflicts=proceed&refresh=true", {"query": {"range": {"price": {"gte": 30}}}}),
    ("POST", "/edge/_update_by_query?refresh=true", {"query": {"match_all": {}},
        "script": {"source": "if (ctx._source.price != null && ctx._source.price > 20) { ctx.op = 'noop' } else { ctx._source.cheap = true }"}}),
    ("POST", "/edge/_search", {"query": {"exists": {"field": "cheap"}}}, {"pick": ids}),
])

scenario("auto_refresh", [
    ("DELETE", "/auto?ignore_unavailable=true"),
    ("PUT", "/auto", {"settings": {"index": {"refresh_interval": "1s"}}}),
    ("POST", "/auto/_search", {}, {"pick": ids}),
    ("POST", "/auto/_doc/1", {"a": 1}),
    ("sleep", 2),
    ("POST", "/auto/_search", {}, {"pick": ids}),
    ("PUT", "/auto/_settings", {"index": {"refresh_interval": "-1"}}),
    ("POST", "/auto/_doc/2", {"a": 2}),
    ("sleep", 2),
    ("POST", "/auto/_search", {}, {"pick": ids}),
    ("POST", "/auto/_refresh"),
    ("POST", "/auto/_search", {}, {"pick": ids}),
])

scenario("cat_cluster", setup() + [
    ("GET", "/_cluster/health", None, {"pick": lambda r: (r["status"], r["number_of_nodes"], r["timed_out"])}),
    ("GET", "/_cluster/health/edge", None, {"pick": lambda r: (r["status"], r["active_primary_shards"])}),
    ("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s", None, {"pick": lambda r: r["timed_out"]}),
    ("GET", "/_cat/indices/edge?format=json&h=index,health,status,docs.count", None),
    ("GET", "/_cat/count/edge?format=json&h=count", None),
    ("GET", "/_cat/health?format=json&h=status,node.total", None),
    ("GET", "/_cat/aliases?format=json", None),
    ("GET", "/_cluster/settings", None),
    ("GET", "/_nodes", None, {"pick": lambda r: r["_nodes"]}),
    ("GET", "/edge/_stats", None, {"pick": lambda r: {k: v for k, v in r["indices"]["edge"]["primaries"]["docs"].items() if k != "total_size_in_bytes"}}),
])

scenario("uri_search", setup() + [
    ("GET", "/edge/_search?q=quick", None, {"pick": ids}),
    ("GET", "/edge/_search?q=title:quick+AND+status:active", None, {"pick": ids}),
    ("GET", "/edge/_search?q=brown&df=title&sort=price:desc&size=2", None, {"pick": hits}),
    ("GET", "/edge/_search?q=%22quick%20brown%22&df=title", None, {"pick": ids}),
    ("GET", "/edge/_search?q=price:%5B10+TO+20%5D", None, {"pick": ids}),
    ("GET", "/edge/_count?q=status:active", None),
    ("GET", "/edge/_search?q=quick&_source_includes=title,price&size=1", None, {"pick": lambda r: [h["_source"] for h in r["hits"]["hits"]]}),
    ("GET", "/edge/_search?q=title:(quick", None),
])

scenario("gzip", setup() + [
    ("POST", S, gzip.compress(json.dumps({"query": {"term": {"status": "active"}}}).encode()),
     {"headers": {"Content-Encoding": "gzip"}, "pick": ids}),
    ("POST", S, {"query": {"term": {"status": "active"}}},
     {"headers": {"Accept-Encoding": "gzip"}, "pick": ids}),
])

scenario("errors", setup() + [
    ("POST", "/missing-index/_search", {}),
    ("POST", "/missing-index/_search?ignore_unavailable=true", {}),
    ("POST", S, {"query": {"nope": {}}}),
    ("POST", S, {"query": {"match": {}}}),
    ("POST", S, {"size": -1}),
    ("POST", S, {"from": -1}),
    ("POST", S, {"from": 9999, "size": 10}),
    ("POST", S, {"aggs": {"x": {"nope": {}}}}),
    ("POST", S, b"{not json"),
    ("GET", "/edge/_doc/nope"),
    ("PUT", "/edge", {}),
    ("PUT", "/Bad-Index", {}),
    ("POST", "/edge/_doc/1/_update", {}),
    ("DELETE", "/missing-index"),
])

def fields_of(r):
    return [(h["_id"], h.get("fields"), "_source" in h, h.get("_version"), h.get("_seq_no"))
            for h in r["hits"]["hits"]]


# Found by the official YAML REST suite: `fields`, `docvalue_fields`,
# `stored_fields`, `version`/`seq_no_primary_term`, `rest_total_hits_as_int`
# and term/exists queries on `_id`/`_index` were ignored.
scenario("fetch_fields", setup() + [
    ("POST", S, {"query": {"ids": {"values": ["1"]}}, "fields": ["title*", "price", "date", "tags"]},
     {"pick": fields_of}),
    ("POST", S, {"query": {"ids": {"values": ["1"]}}, "fields": [{"field": "date", "format": "yyyy/MM/dd"}],
                 "_source": False}, {"pick": fields_of}),
    ("POST", S, {"query": {"ids": {"values": ["1"]}}, "fields": [{"field": "status", "format": "yyyy"}]}),
    ("POST", S, {"query": {"ids": {"values": ["1"]}}, "docvalue_fields": ["price", "tags"]}, {"pick": fields_of}),
    ("POST", S, {"query": {"ids": {"values": ["1"]}}, "docvalue_fields": ["body"]}),
    ("POST", S, {"query": {"ids": {"values": ["2"]}}, "stored_fields": ["_none_"]},
     {"pick": lambda r: r["hits"]["hits"]}),
    ("POST", S, {"query": {"ids": {"values": ["2"]}}, "stored_fields": []}, {"pick": fields_of}),
    ("POST", S, {"query": {"ids": {"values": ["2"]}}, "version": True, "seq_no_primary_term": True},
     {"pick": fields_of}),
    ("POST", S + "?rest_total_hits_as_int=true", {"query": {"term": {"_id": "3"}}},
     {"pick": lambda r: (r["hits"]["total"], ids(r))}),
    ("POST", S + "?rest_total_hits_as_int=true&track_total_hits=false", {},
     {"pick": lambda r: r["hits"]["total"]}),
    ("POST", S + "?rest_total_hits_as_int=true", {"query": {"exists": {"field": "_index"}}},
     {"pick": lambda r: r["hits"]["total"]}),
    ("POST", S, {"query": {"terms": {"_index": ["edge"]}}, "size": 0},
     {"pick": lambda r: r["hits"]["total"]}),
])

# Found by the official YAML REST suite: component templates were stored
# as index templates, legacy `_template` didn't exist, every matching
# template was merged (instead of the highest priority one winning), and
# GET /_index_template (no name) was a 404.
scenario("templates", [
    ("DELETE", "/tpl-logs-1"), ("DELETE", "/tpl-other"),
    ("DELETE", "/_index_template/tpl-*"), ("DELETE", "/_component_template/tpl-*"),
    ("DELETE", "/_template/tpl-*"),
    ("PUT", "/_component_template/tpl-ct", {"template": {
        "settings": {"number_of_replicas": 0},
        "mappings": {"properties": {"obj.a": {"type": "keyword"}}}}}),
    ("PUT", "/_index_template/tpl-low", {"index_patterns": "tpl-logs-*", "priority": 1,
                                          "template": {"settings": {"number_of_shards": 3}}}),
    ("PUT", "/_index_template/tpl-high", {"index_patterns": ["tpl-logs-*"], "priority": 5,
                                           "composed_of": ["tpl-ct"],
                                           "template": {"aliases": {"tpl-alias": {"routing": "b"}},
                                                        "mappings": {"properties": {"obj.b": {"type": "long"}}}}}),
    ("PUT", "/_index_template/tpl-clash", {"index_patterns": ["tpl-logs-a*"], "priority": 5}),
    ("PUT", "/_index_template/tpl-bad", {"index_patterns": ["tpl-x*"], "composed_of": ["tpl-nope"]}),
    ("PUT", "/_index_template/tpl-nopat", {"template": {}}),
    ("PUT", "/_index_template/tpl-low?create=true", {"index_patterns": ["tpl-zz*"]}),
    ("GET", "/_index_template/tpl-high"),
    ("GET", "/_index_template/tpl-*", None, {"pick": lambda r: sorted(t["name"] for t in r["index_templates"])}),
    ("GET", "/_index_template/tpl-none*"),
    ("GET", "/_index_template/tpl-nope"),
    ("GET", "/_index_template/tpl-a,tpl-b"),
    ("DELETE", "/_index_template/tpl-nope"),
    ("DELETE", "/_component_template/tpl-nope"),
    ("GET", "/_component_template/tpl-ct"),
    ("DELETE", "/_component_template/tpl-ct"),
    # (Elasticsearch adds its default `_tier_preference` setting; compared without it.)
    ("POST", "/_index_template/_simulate_index/tpl-logs-9", None,
     {"pick": lambda r: (r["template"]["settings"]["index"]["number_of_replicas"],
                         r["template"]["mappings"], r["template"]["aliases"], r["overlapping"])}),
    ("POST", "/_index_template/_simulate_index/nothing-matches"),
    ("PUT", "/tpl-logs-1", {"mappings": {"properties": {"obj.c": {"type": "text"}}}}),
    ("GET", "/tpl-logs-1", None, {"pick": lambda r: {k: v for k, v in r["tpl-logs-1"].items() if k != "settings"}}),
    ("GET", "/tpl-logs-1/_settings", None,
     {"pick": lambda r: {k: r["tpl-logs-1"]["settings"]["index"][k] for k in ("number_of_shards", "number_of_replicas")}}),
    ("PUT", "/_template/tpl-legacy", {"index_patterns": ["tpl-o*"], "order": 2, "version": 3,
                                      "settings": {"number_of_shards": 2}}),
    ("PUT", "/_template/tpl-legacy0", {"index_patterns": ["tpl-*"], "order": 0,
                                       "settings": {"number_of_shards": 4, "number_of_replicas": 0}}),
    ("PUT", "/_template/tpl-legacy?create=true", {"index_patterns": ["tpl-o*"]}),
    ("PUT", "/_template/tpl-nopat", {"order": 1}),
    ("GET", "/_template/tpl-legacy"),
    ("GET", "/_template/tpl-legacy?flat_settings=true"),
    ("GET", "/_template/tpl-missing"),
    ("HEAD", "/_template/tpl-legacy"),
    ("PUT", "/tpl-other", {}),
    ("GET", "/tpl-other/_settings", None,
     {"pick": lambda r: {k: r["tpl-other"]["settings"]["index"][k] for k in ("number_of_shards", "number_of_replicas")}}),
    ("DELETE", "/_template/tpl-missing"),
    ("DELETE", "/tpl-logs-1"), ("DELETE", "/tpl-other"),
    ("DELETE", "/_index_template/tpl-*"), ("DELETE", "/_component_template/tpl-*"),
    ("DELETE", "/_template/tpl-*"),
])

# Found by the official YAML REST suite: global /_mget, mget validation and
# per-doc errors, _source path filtering, external versions, op_type,
# delete of a missing doc, routing (on a multi-shard index a GET without
# the routing value misses the document), require_alias, update `_source`
# and unknown fields, count min_score / body validation, realtime=false.
scenario("document_apis", [
    ("DELETE", "/docs-a"), ("DELETE", "/docs-r"),
    ("PUT", "/docs-a", {"settings": {"number_of_replicas": 0, "refresh_interval": "-1"}}),
    ("PUT", "/docs-a/_doc/1", {"include": {"field1": "v1", "field2": "v2"}, "count": 1}),
    ("GET", "/docs-a/_doc/1?realtime=false"),
    ("GET", "/docs-a/_doc/1?realtime=false&refresh=true"),
    ("GET", "/docs-a/_doc/1?_source=include.field1"),
    ("GET", "/docs-a/_doc/1?_source_includes=include&_source_excludes=*.field2"),
    ("GET", "/docs-a/_source/1?_source_includes=include.field2"),
    ("GET", "/docs-a/_doc/1?version=7"),
    ("POST", "/_mget", {"docs": [{"_index": "docs-a", "_id": "1"}, {"_index": "docs-a", "_id": "9"},
                                 {"_index": "docs-missing", "_id": "1"},
                                 {"_index": "docs-a", "_id": "1", "_source": ["count"]}]}),
    ("POST", "/docs-a/_mget", {"ids": [1, 2]}),
    ("POST", "/_mget", {"docs": [{"_index": "docs-a"}]}),
    ("POST", "/_mget", {"docs": [{"_id": "1"}]}),
    ("POST", "/_mget", {}),
    ("PUT", "/docs-a/_doc/2?version=5&version_type=external", {"a": 1}),
    ("PUT", "/docs-a/_doc/2?version=5&version_type=external", {"a": 2}),
    ("PUT", "/docs-a/_doc/2?version=5&version_type=external_gte", {"a": 3}),
    ("PUT", "/docs-a/_doc/2?version=6", {"a": 4}),
    ("PUT", "/docs-a/_doc/2?op_type=create", {"a": 5}),
    ("PUT", "/docs-a/_create/3?version=1&version_type=external", {"a": 6}),
    ("DELETE", "/docs-a/_doc/2?version=4&version_type=external"),
    ("DELETE", "/docs-a/_doc/2?if_seq_no=99&if_primary_term=1"),
    ("DELETE", "/docs-a/_doc/2?version=9&version_type=external"),
    ("DELETE", "/docs-a/_doc/nope"),
    ("PUT", "/docs-a/_doc/" + "x" * 513, {"a": 1}),
    ("POST", "/docs-a/_update/1?_source=count", {"doc": {"count": 2}}),
    ("POST", "/docs-a/_update/1", {"dac": {"count": 3}}),
    ("PUT", "/docs-a/_doc/5?require_alias=true", {"a": 1}),
    ("POST", "/docs-a/_refresh"),
    ("POST", "/docs-a/_count?min_score=1", {"query": {"match_all": {}}}),
    ("POST", "/docs-a/_count", {"match": {"a": 1}}),
    ("PUT", "/docs-r", {"settings": {"number_of_shards": 5, "number_of_routing_shards": 5, "number_of_replicas": 0},
                        "mappings": {"_routing": {"required": False}}}),
    ("PUT", "/docs-r/_doc/1?routing=5", {"foo": "bar"}),
    ("GET", "/docs-r/_doc/1?routing=5"),
    ("GET", "/docs-r/_doc/1"),
    ("POST", "/docs-r/_mget", {"docs": [{"_id": "1"}, {"_id": "1", "routing": "4"}, {"_id": "1", "routing": "5"}]}),
    ("POST", "/docs-r/_update/1", {"doc": {"foo": "baz"}}),
    ("DELETE", "/docs-r/_doc/1"),
    ("DELETE", "/docs-r/_doc/1?routing=5"),
    ("DELETE", "/docs-a"), ("DELETE", "/docs-r"),
])

# Found by the official YAML REST suite: alias APIs only took one
# concrete index, ignored filters/routing/is_write_index, alias name
# expressions (globs, exclusions, lists) and missing-alias 404s; a search
# through a filtered alias returned unfiltered documents.
scenario("aliases", [
    ("DELETE", "/al-1"), ("DELETE", "/al-2"), ("DELETE", "/al-3"),
    ("PUT", "/al-1", {"settings": {"number_of_replicas": 0}, "aliases": {"al_a": {}, "al_b": {}}}),
    ("PUT", "/al-2", {"settings": {"number_of_replicas": 0}, "aliases": {"al_a": {}}}),
    ("PUT", "/al-3", {"settings": {"number_of_replicas": 0}}),
    ("PUT", "/al-1/_alias/al_f", {"filter": {"term": {"kind": "x"}}, "routing": 5, "is_write_index": True}),
    ("PUT", "/al-1/_alias/al_*", {}),
    ("PUT", "/al-1/_alias/al-2", {}),
    ("PUT", "/al-*/_alias/al_all", {}),
    ("GET", "/_alias/al_a"),
    ("GET", "/_alias/al_*,-al_a"),
    ("GET", "/_alias/al_a,nope"),
    ("GET", "/_alias/al_a,nope,nope2"),
    ("GET", "/al-1/_alias"),
    ("GET", "/al-3/_alias"),
    ("GET", "/al-1/_alias/al_f"),
    ("HEAD", "/_alias/al_f"),
    ("HEAD", "/_alias/nope"),
    ("GET", "/nope-index/_alias/al_a"),
    ("POST", "/al-1/_doc/1?refresh=true", {"kind": "x"}),
    ("POST", "/al-1/_doc/2?refresh=true", {"kind": "y"}),
    ("POST", "/al_f/_count", {}),
    ("POST", "/al-1/_count", {}),
    ("POST", "/al_f,al-1/_count", {}),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "al-3", "alias": "al_c"}},
                                       {"remove": {"index": "al-3", "alias": "nope"}}]}),
    ("POST", "/_aliases", {"actions": [{"remove": {"index": "al-3", "alias": "nope", "must_exist": True}},
                                       {"add": {"index": "al-3", "alias": "al_d"}}]}),
    ("HEAD", "/_alias/al_d"),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "al-3", "alias": "al_e", "must_exist": True}}]}),
    ("POST", "/_aliases", {"actions": [{"remove_index": {"index": "al-3"}},
                                       {"add": {"index": "al-2", "alias": "al-3"}}]}),
    ("GET", "/al-2/_alias"),
    ("DELETE", "/al-*/_alias/al_a"),
    ("DELETE", "/al-*/_alias/al_a"),
    ("GET", "/_cat/aliases?h=alias,index,filter,routing.index,routing.search,is_write_index&s=index,alias", None,
     {"pick": lambda r: r}),
    ("DELETE", "/al-1"), ("DELETE", "/al-2"), ("DELETE", "/al-3"),
])

# Found by the official YAML REST suite: _field_caps and _msearch didn't
# exist ("no handler").
scenario("field_caps_msearch", [
    ("DELETE", "/fc-1"), ("DELETE", "/fc-2"),
    ("PUT", "/fc-1", {"settings": {"number_of_replicas": 0}, "mappings": {"properties": {
        "t": {"type": "text", "fields": {"k": {"type": "keyword"}}}, "n": {"type": "double"},
        "o": {"properties": {"a": {"type": "long", "index": False, "meta": {"unit": "ms"}},
                             "b": {"type": "keyword", "doc_values": False}}},
        "nest": {"type": "nested", "properties": {"x": {"type": "keyword"}}}}}}),
    ("PUT", "/fc-2", {"settings": {"number_of_replicas": 0}, "mappings": {"properties": {
        "t": {"type": "text"}, "n": {"type": "long"}, "d": {"type": "date"},
        "o": {"properties": {"a": {"type": "long", "meta": {"unit": "s"}}, "b": {"type": "keyword"}}}}}}),
    ("GET", "/fc-1,fc-2/_field_caps?fields=t,t.k,n,d,o.a,o.b"),
    ("GET", "/fc-*/_field_caps?fields=*&filters=-metadata"),
    ("GET", "/fc-1/_field_caps?fields=*&filters=-metadata,-multifield,-parent,-nested"),
    ("GET", "/fc-1/_field_caps?fields=*&types=keyword,long"),
    ("GET", "/fc-1,fc-2/_field_caps?fields=d&include_unmapped=true"),
    ("GET", "/fc-1/_field_caps"),
    ("GET", "/fc-1,nope/_field_caps?fields=t"),
    ("GET", "/fc-1,nope/_field_caps?fields=t&ignore_unavailable=true"),
    ("POST", "/fc-1/_doc/1?refresh=true", {"t": "hello", "n": 1}),
    ("POST", "/fc-2/_doc/1?refresh=true", {"t": "bye", "n": 5, "d": "2020-01-01"}),
    ("POST", "/fc-*/_field_caps?fields=n", {"index_filter": {"range": {"n": {"gte": 3}}}}),
    ("POST", "/fc-*/_field_caps?fields=n", {"index_filter": {"term": {"d": "2020-01-01"}}}),
    ("POST", "/fc-*/_field_caps?fields=n", {"index_filter": {"range": {"d": {"gte": "2030"}}}}),
    ("GET", "/fc-1/_field_caps?fields=*&filters=-metadata,-bogus"),
    ("GET", "/fc-1/_field_caps?fields=*&filters=-metadata&include_empty_fields=false"),
    ("POST", "/_msearch", [{"index": "fc-1"}, {"query": {"match_all": {}}},
                           {"index": "nope"}, {"query": {"match_all": {}}},
                           {"index": ["fc-1", "fc-2"]}, {"size": 0, "aggs": {"m": {"max": {"field": "n"}}}}]),
    ("POST", "/fc-2/_msearch?rest_total_hits_as_int=true", [{}, {"query": {"match_all": {}}}]),
    ("POST", "/fc-2/_msearch?rest_total_hits_as_int=true", [{}, {"track_total_hits": 10}]),
    ("POST", "/_msearch", b""),
    ("DELETE", "/fc-1"), ("DELETE", "/fc-2"),
])


# Query DSL beyond the basics: what relevance tuning, autocomplete and
# location features send.
GEO_MAPPING = {
    "settings": STATIC,
    "mappings": {"properties": {
        "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
        "body": {"type": "text"},
        "price": {"type": "integer"},
        "tags": {"type": "keyword"},
        "date": {"type": "date"},
        "status": {"type": "keyword"},
        "loc": {"type": "geo_point"},
    }},
}
GEO_DOCS = [dict(d, loc=l) for d, l in zip(DOCS, [
    {"lat": 52.52, "lon": 13.405}, "48.8566,2.3522", [-0.1276, 51.5072],
    {"lat": 40.4168, "lon": -3.7038}, "41.9028, 12.4964"])]
G = "/geo/_search"


def ids_scores(r):
    return [(h["_id"], round(h["_score"], 3) if h.get("_score") is not None else None)
            for h in r["hits"]["hits"]]


scenario("query_dsl_more", setup("geo", GEO_MAPPING, GEO_DOCS) + [
    ("POST", G, {"query": {"match_phrase_prefix": {"title": "quick br"}}}, {"pick": ids}),
    ("POST", G, {"query": {"match_phrase_prefix": {"title": {"query": "brown b", "max_expansions": 10}}}}, {"pick": ids}),
    ("POST", G, {"query": {"match_phrase_prefix": {"body": "the"}}}, {"pick": ids}),
    ("POST", G, {"query": {"match_bool_prefix": {"title": "quick br"}}}, {"pick": ids}),
    ("POST", G, {"query": {"match_bool_prefix": {"title": {"query": "fox quick", "operator": "and"}}}}, {"pick": ids}),
    ("POST", G, {"query": {"boosting": {"positive": {"match": {"title": "brown"}},
                                        "negative": {"term": {"tags": "wild"}},
                                        "negative_boost": 0.5}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"query": {"match": {"title": "brown"}},
                                              "field_value_factor": {"field": "price", "factor": 1.2, "modifier": "sqrt", "missing": 1}}}},
     {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"query": {"match_all": {}},
                                              "functions": [{"filter": {"term": {"status": "active"}}, "weight": 3},
                                                            {"filter": {"term": {"tags": "animal"}}, "weight": 2}],
                                              "score_mode": "sum", "boost_mode": "replace"}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"query": {"match": {"body": "quick"}},
                                              "functions": [{"field_value_factor": {"field": "price"}}],
                                              "boost_mode": "sum", "max_boost": 20}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"functions": [{"gauss": {"price": {"origin": 20, "scale": 10, "decay": 0.5}}}]}}},
     {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"functions": [{"exp": {"date": {"origin": "2024-03-01", "scale": "30d"}}}],
                                              "boost_mode": "replace"}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"function_score": {"functions": [{"linear": {"price": {"origin": 0, "scale": 20, "offset": 5}}}],
                                              "score_mode": "max", "boost_mode": "replace", "min_score": 0.5}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"script_score": {"query": {"match": {"title": "brown"}},
                                            "script": {"source": "_score * doc['price'].value"}}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"script_score": {"query": {"match_all": {}},
                                            "script": {"source": "doc['price'].value / params.d", "params": {"d": 5}},
                                            "min_score": 3}}}, {"pick": ids_scores}),
    ("POST", G, {"query": {"script_score": {"query": {"match_all": {}}, "script": "-1"}}}),
    ("POST", G, {"query": {"combined_fields": {"query": "quick fox", "fields": ["title", "body"]}}}, {"pick": ids}),
    ("POST", G, {"query": {"combined_fields": {"query": "quick fox", "fields": ["title", "body"], "operator": "and"}}}, {"pick": ids}),
    ("POST", G, {"query": {"geo_distance": {"distance": "1200km", "loc": {"lat": 50.0, "lon": 8.0}}}}, {"pick": ids}),
    ("POST", G, {"query": {"geo_distance": {"distance": "900km", "loc": "48.0,3.0"}}}, {"pick": ids}),
    ("POST", G, {"query": {"geo_bounding_box": {"loc": {"top_left": {"lat": 53, "lon": -1}, "bottom_right": {"lat": 45, "lon": 14}}}}}, {"pick": ids}),
    ("POST", G, {"sort": [{"_geo_distance": {"loc": {"lat": 48.8, "lon": 2.3}, "order": "asc", "unit": "km"}}]},
     {"pick": lambda r: [(h["_id"], [round(x, 1) for x in h["sort"]]) for h in r["hits"]["hits"]]}),
    ("POST", G, {"query": {"bool": {"should": [{"match": {"title": {"query": "quick", "_name": "q_title"}}},
                                               {"term": {"tags": {"value": "pet", "_name": "q_pet"}}},
                                               {"range": {"price": {"gte": 30, "_name": "pricey"}}}]}}},
     {"pick": lambda r: sorted((h["_id"], sorted(h.get("matched_queries", []))) for h in r["hits"]["hits"])}),
    ("POST", G, {"query": {"bool": {"filter": [{"term": {"status": "active"}}], "_name": "outer"}}},
     {"pick": lambda r: sorted((h["_id"], h.get("matched_queries")) for h in r["hits"]["hits"])}),
    ("POST", G, {"collapse": {"field": "status"}, "sort": [{"price": "asc"}]},
     {"pick": lambda r: [(h["_id"], h.get("fields")) for h in r["hits"]["hits"]]}),
    ("POST", G, {"collapse": {"field": "status", "inner_hits": {"name": "same", "size": 2, "sort": [{"price": "desc"}]}},
                 "sort": [{"price": "desc"}]},
     {"pick": lambda r: [(h["_id"], [x["_id"] for x in h.get("inner_hits", {}).get("same", {}).get("hits", {}).get("hits", [])]) for h in r["hits"]["hits"]]}),
    ("POST", G, {"collapse": {"field": "body"}}),
] + setup() + [
    ("POST", "/geo,edge/_search", {"indices_boost": [{"geo": 2.0}], "query": {"match": {"title": "brown"}}},
     {"pick": lambda r: [(h["_index"], h["_id"]) for h in r["hits"]["hits"]]}),
    ("POST", G, {"from": -1}),
    ("POST", G, {"terminate_after": -1}),
    ("POST", G, {"track_total_hits": -2}),
    ("DELETE", "/geo"),
    ("DELETE", "/edge"),
])


# Index management over index expressions (lists, wildcards, _all) and
# the mapping/settings APIs apps call at startup and in migrations.
def mk(name, props=None, settings=None):
    body = {"settings": dict(STATIC, **(settings or {}))}
    if props is not None:
        body["mappings"] = {"properties": props}
    return [("DELETE", f"/{name}"), ("PUT", f"/{name}", body)]


scenario("index_management", mk("im-1", {"t": {"type": "text"}, "k": {"type": "keyword"}, "o": {"properties": {"n": {"type": "long"}}}})
         + mk("im-2", {"t": {"type": "keyword"}}) + mk("im-x") + [
    ("GET", "/im-1/_mapping"),
    ("GET", "/im-x/_mapping"),
    ("GET", "/im-1,im-2/_mapping"),
    ("GET", "/im-*/_mapping"),
    ("GET", "/im-nope/_mapping"),
    ("GET", "/im-nope/_mapping?ignore_unavailable=true"),
    ("GET", "/im-nope*/_mapping"),
    ("GET", "/im-nope*/_mapping?allow_no_indices=false"),
    ("GET", "/_mapping", None, {"pick": lambda r: sorted(k for k in r if k.startswith("im-"))}),
    ("GET", "/_all/_mapping", None, {"pick": lambda r: sorted(k for k in r if k.startswith("im-"))}),
    ("GET", "/im-1/_mapping/field/t"),
    ("GET", "/im-1/_mapping/field/o.n,k"),
    ("GET", "/im-1/_mapping/field/nope"),
    ("GET", "/im-*/_mapping/field/t"),
    ("GET", "/_mapping/field/k", None, {"pick": lambda r: {k: v for k, v in r.items() if k.startswith("im-")} if isinstance(r, dict) else r}),
    ("GET", "/im-1/_mapping/field/*", None, {"pick": lambda r: sorted(r.get("im-1", {}).get("mappings", {}))}),
    ("GET", "/im-1/_mapping/field/o.*"),
    ("PUT", "/im-*/_mapping", {"properties": {"added": {"type": "integer"}}}),
    ("GET", "/im-2,im-x/_mapping"),
    ("PUT", "/im-1/_mapping", {"_doc": {"properties": {"z": {"type": "keyword"}}}}),
    ("PUT", "/im-1/_mapping", {"properties": {"t": {"type": "keyword"}}}),
    ("PUT", "/im-1/_mapping", {"properties": {"bad": {"type": "no_such_type"}}}),
    ("PUT", "/im-q", {"mappings": {"properties": {"bad": {"type": "no_such_type"}}}}),
    ("GET", "/im-1/_settings/index.number_of_shards"),
    ("GET", "/im-1/_settings/index.number_of_*"),
    ("GET", "/im-1,im-2/_settings/index.refresh_interval"),
    ("GET", "/_settings/index.number_of_shards", None, {"pick": lambda r: {k: v for k, v in r.items() if k.startswith("im-")}}),
    ("GET", "/im-1/_settings?include_defaults=true", None,
     {"pick": lambda r: (r["im-1"]["settings"]["index"].get("refresh_interval"), r["im-1"].get("defaults", {}).get("index", {}).get("max_result_window"))}),
    ("PUT", "/im-*/_settings", {"index": {"number_of_replicas": 0}}),
    ("GET", "/im-2/_settings/index.number_of_replicas"),
    ("PUT", "/im-1/_settings", {"index": {"number_of_shards": 3}}),
    ("PUT", "/im-1/_settings", {"index": {"no_such_setting": 1}}),
    ("PUT", "/im-1/_settings?preserve_existing=true", {"index": {"number_of_replicas": 2, "max_result_window": 500}}),
    ("GET", "/im-1/_settings/index.number_of_replicas,index.max_result_window"),
    ("PUT", "/im-1/_settings", {"index": {"max_result_window": None}}),
    ("GET", "/im-1/_settings/index.max_result_window"),
    ("PUT", "/_settings", {"index": {"number_of_replicas": 0}}, {"pick": lambda r: r}),
    ("GET", "/im-1,im-2", None, {"pick": lambda r: sorted(r)}),
    ("GET", "/im-*", None, {"pick": lambda r: sorted(r)}),
    ("GET", "/im-nope"),
    ("GET", "/im-nope?ignore_unavailable=true"),
    ("GET", "/im-nope*"),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "im-2", "alias": "im-alias"}}]}),
    ("DELETE", "/im-alias"),
    ("DELETE", "/im-nope"),
    ("DELETE", "/im-nope?ignore_unavailable=true"),
    ("DELETE", "/im-1,im-2"),
    ("GET", "/im-*", None, {"pick": lambda r: sorted(r)}),
    ("DELETE", "/im-*"), ("DELETE", "/im-x"),
    ("GET", "/im-*"),
])

scenario("filter_path_closed", setup("fpc") + [
    ("GET", "/fpc/_search?filter_path=hits.total"),
    ("GET", "/fpc/_search?filter_path=hits.hits._id,hits.hits._source.title&sort=price"),
    ("GET", "/fpc/_search?filter_path=-hits.hits._source,-took,-_shards&size=2&sort=price"),
    ("GET", "/fpc/_search?filter_path=**._id&size=2&sort=price"),
    ("GET", "/fpc/_search?filter_path=hits.*.total"),
    ("GET", "/fpc/_search?filter_path=nope"),
    ("GET", "/fpc/_doc/1?filter_path=_source.*"),
    ("GET", "/fpc/_mapping?filter_path=*.mappings.properties.title"),
    ("GET", "/fpc/_search?filter_path=hits.hits&size=0"),
    ("GET", "/nope-idx/_search?filter_path=error.type"),
    ("GET", "/_cluster/health?filter_path=status,number_of_nodes"),
    ("GET", "/fpc?features=aliases,settings", None, {"pick": lambda r: r["fpc"]["mappings"]}),
    ("GET", "/fpc?human=true", None, {"pick": lambda r: sorted(r["fpc"]["settings"]["index"])}),
    ("GET", "/fpc/_settings", None, {"pick": lambda r: sorted(r["fpc"]["settings"]["index"])}),
    ("GET", "/_fpc"),
    ("PUT", "/fpc-closed", {"settings": {"number_of_replicas": 0}}),
    ("POST", "/fpc-closed/_close"),
    ("GET", "/fpc-closed/_search"),
    ("GET", "/fpc-closed/_count"),
    ("PUT", "/fpc-closed/_doc/1", {"a": 1}),
    ("GET", "/fpc-closed/_doc/1"),
    ("GET", "/fpc*/_search?filter_path=hits.total"),
    ("GET", "/fpc*/_search?expand_wildcards=all&filter_path=hits.total"),
    ("GET", "/fpc*?expand_wildcards=closed", None, {"pick": lambda r: sorted(r)}),
    ("GET", "/fpc*", None, {"pick": lambda r: sorted(r)}),
    ("GET", "/fpc*/_settings/index.number_of_replicas"),
    ("GET", "/fpc*/_mapping"),
    ("POST", "/fpc*/_open"),
    ("GET", "/fpc-closed/_count"),
    ("DELETE", "/fpc-closed"),
    ("PUT", "/fpc-a", {"aliases": {"fpc-al": {}}}),
    ("DELETE", "/fpc-al?ignore_unavailable=true"),
    ("DELETE", "/fpc-al,fpc-nope?ignore_unavailable=true"),
    ("GET", "/fpc-a", None, {"pick": lambda r: list(r)}),
    ("DELETE", "/fpc-a"),
])

def _bulk_docs(index, n):
    out = []
    for i in range(n):
        out.append({"index": {"_index": index, "_id": str(i)}})
        out.append({"n": i, "k": f"k{i % 7}"})
    return out


scenario("scroll_slice_shards", [
    ("DELETE", "/ss-1?ignore_unavailable=true"),
    ("PUT", "/ss-1", {"settings": {"number_of_shards": 3, "number_of_replicas": 0}}),
    ("POST", "/_bulk?refresh=true", _bulk_docs("ss-1", 40), {"pick": lambda r: r["errors"]}),
    ("GET", "/ss-1/_search?size=0", None, {"pick": lambda r: (r["_shards"], r["hits"]["total"])}),
    ("GET", "/ss-1/_count"),
    ("GET", "/_cluster/health/ss-1", None, {"pick": lambda r: (r["active_primary_shards"], r["active_shards"], r["unassigned_shards"], r["status"])}),
    ("GET", "/ss-1/_search?scroll=1m&size=0"),
    ("GET", "/ss-1/_search?scroll=1m&request_cache=true"),
    ("GET", "/ss-1/_search?scroll=1000h"),
    ("POST", "/ss-1/_search", {"slice": {"id": 0, "max": 2}}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 2, "max": 2}}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 0, "max": 1}}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": -1, "max": 2}}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 0, "max": 1025}}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 0, "max": 4}, "size": 100}, {"pick": lambda r: r["_shards"]["total"]}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 1, "max": 4}, "size": 100}, {"pick": lambda r: len(r["hits"]["hits"]) == r["hits"]["total"]["value"]}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 2, "max": 4}, "size": 100}, {"pick": lambda r: r["timed_out"]}),
    ("POST", "/ss-1/_search?scroll=1m", {"slice": {"id": 3, "max": 4, "field": "n"}, "size": 100}, {"pick": lambda r: r["hits"]["total"]["relation"]}),
    ("DELETE", "/ss-1"),
])

def _items(r):
    return [{k: {f: v.get(f) for f in ("_index", "_id", "status", "result")} | ({"error": v["error"]["type"]} if "error" in v else {})
             for k, v in it.items()} for it in r["items"]]


scenario("bulk_alias_targets", [
    ("DELETE", "/bk-1?ignore_unavailable=true"), ("DELETE", "/bk-2?ignore_unavailable=true"),
    ("DELETE", "/bk-3?ignore_unavailable=true"),
    ("POST", "/_bulk", [{"index": {"_index": "bk-1", "_id": "1"}}, {"a": 1}, {}, {"a": 2}]),
    ("POST", "/_bulk", [{"foo": {"_index": "bk-1"}}, {"a": 1}]),
    ("POST", "/_bulk?refresh=true", [
        {"index": {"_index": "bk-1", "_id": ""}}, {"f": 1},
        {"index": {"_index": "bk-1", "_id": "id"}}, {"f": 2},
        {"create": {"_index": "bk-1", "_id": "c"}}, {"f": 4},
        {"index": {"_index": "bk-1", "_id": "oc", "op_type": "create"}}, {"f": 5},
        {"index": {"_index": "bk-1", "_id": "oc", "op_type": "create"}}, {"f": 6},
        {"update": {"_index": "bk-1", "_id": "id", "_source": True}}, {"doc": {"g": 1}},
        {"update": {"_index": "bk-1", "_id": "missing"}}, {"doc": {"g": 1}},
        {"delete": {"_index": "bk-1", "_id": "nope"}},
        {"delete": {"_index": "bk-1", "_id": "c"}},
    ], {"pick": _items}),
    ("GET", "/bk-1/_count"),
    ("PUT", "/bk-2", {"aliases": {"bk-al": {}}}),
    ("PUT", "/bk-3", {"aliases": {"bk-al": {}}}),
    ("GET", "/bk-al/_doc/1"),
    ("PUT", "/bk-al/_doc/1", {"a": 1}),
    ("POST", "/bk-al/_doc", {"a": 1}),
    ("POST", "/bk-al/_update/1", {"doc": {"a": 2}}),
    ("DELETE", "/bk-al/_doc/1"),
    ("POST", "/_bulk", [{"index": {"_index": "bk-al", "_id": "1"}}, {"a": 1}], {"pick": _items}),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "bk-3", "alias": "bk-al", "is_write_index": True}}]}),
    ("PUT", "/bk-al/_doc/1?refresh=true", {"a": 1}, {"pick": lambda r: (r["_index"], r["result"])}),
    ("GET", "/bk-al/_doc/1"),
    ("GET", "/bk-3/_doc/1", None, {"pick": lambda r: r["found"]}),
    ("POST", "/_bulk", [{"index": {"_index": "bk-al", "_id": "2"}}, {"a": 1}], {"pick": _items}),
    ("PUT", "/bk-2/_alias/bk-al2", {"is_write_index": False}),
    ("PUT", "/bk-al2/_doc/1", {"a": 1}),
    ("GET", "/bk-al2/_doc/1", None, {"pick": lambda r: r["found"]}),
    ("DELETE", "/bk-1"), ("DELETE", "/bk-2"), ("DELETE", "/bk-3"),
])

# dense_vector fields and kNN search: mapping defaults and validation,
# document checks, the top-level `knn` option, the `knn` query and vector
# functions in scoring scripts. Scores are compared only for HNSW/flat
# fields: the default int8_hnsw field's scores are quantized
# approximations in real Elasticsearch (noida's are exact), so for it only
# the order is.
KNN_PROPS = {
    "name": {"type": "keyword"},
    "l2": {"type": "dense_vector", "dims": 3, "similarity": "l2_norm", "index_options": {"type": "hnsw"}},
    "cos": {"type": "dense_vector", "dims": 3, "similarity": "cosine", "index_options": {"type": "hnsw", "m": 32}},
    "dot": {"type": "dense_vector", "dims": 2, "similarity": "dot_product", "index_options": {"type": "flat"}},
    "mip": {"type": "dense_vector", "dims": 3, "similarity": "max_inner_product", "index_options": {"type": "hnsw"}},
    "byte": {"type": "dense_vector", "dims": 3, "element_type": "byte", "similarity": "dot_product"},
    "bytel2": {"type": "dense_vector", "dims": 3, "element_type": "byte", "similarity": "l2_norm"},
    "bits": {"type": "dense_vector", "dims": 16, "element_type": "bit"},
    "q8": {"type": "dense_vector", "dims": 3},
    "raw": {"type": "dense_vector", "dims": 3, "index": False},
    "paras": {"type": "nested", "properties": {
        "pid": {"type": "keyword"},
        "vec": {"type": "dense_vector", "dims": 3, "similarity": "l2_norm", "index_options": {"type": "hnsw"}},
    }},
}
KNN_DOCS = [
    {"name": "a", "l2": [1, 2, 3], "cos": [1, 2, 3], "dot": [0.6, 0.8], "mip": [1, 2, 3], "byte": [1, 2, 3],
     "bytel2": [1, 2, 3], "bits": [1, 3], "q8": [1, 2, 3], "raw": [1, 1, 1],
     "paras": [{"pid": "a0", "vec": [1, 0, 0]}, {"pid": "a1", "vec": [5, 5, 5]}]},
    {"name": "b", "l2": [-1, 0.5, 2], "cos": [-1, 0.5, 2], "dot": [0.8, 0.6], "mip": [-1, 0.5, 2], "byte": [-100, 50, 3],
     "bytel2": [-100, 50, 3], "bits": "ff00", "q8": [-1, 0.5, 2],
     "paras": [{"pid": "b0", "vec": [0, 1, 0]}]},
    {"name": "c", "l2": [3, -2, 0.1], "cos": [3, -2, 0.1], "dot": [0, 1], "mip": [3, -2, 0.1], "byte": "05f909",
     "bytel2": [5, -7, 9], "bits": [0, 0], "q8": [3, -2, 0.1],
     "paras": [{"pid": "c0", "vec": [9, 9, 9]}, {"pid": "c1", "vec": [0.5, 0.5, 0]}, {"pid": "c2", "vec": [2, 2, 2]}]},
    {"name": "a", "l2": [0, 0, 1], "cos": [0, 0, 1], "dot": [1, 0], "mip": [0, 0, 1], "byte": [0, 0, 1],
     "bytel2": [0, 0, 1], "bits": [255, 255], "q8": [0, 0, 1]},
    {"name": "d"},
]
K = "/knn-docs/_search"
ACK = {"pick": lambda r: r.get("acknowledged")}


def knn_hits(r):
    return [(h["_id"], h.get("_score"), h.get("matched_queries")) for h in r["hits"]["hits"]] + [r["hits"].get("total")]


def knn_ids(r):
    return [h["_id"] for h in r["hits"]["hits"]] + [r["hits"].get("total")]


def knn_inner(r):
    return [(h["_id"], h["_score"], [(x["_nested"]["offset"], x["_score"], x.get("fields"))
                                     for x in h["inner_hits"]["paras"]["hits"]["hits"]],
             h["inner_hits"]["paras"]["hits"]["total"]) for h in r["hits"]["hits"]]


def knn_bad_mapping(name, prop):
    return [("DELETE", f"/{name}?ignore_unavailable=true"),
            ("PUT", f"/{name}", {"mappings": {"properties": {"v": prop}}})]


def kq(field, vector, **kw):
    return dict({"field": field, "query_vector": vector}, **kw)


scenario("knn", [("DELETE", "/knn-docs?ignore_unavailable=true"),
                 ("PUT", "/knn-docs", {"settings": STATIC, "mappings": {"properties": KNN_PROPS}}, ACK),
                 ("GET", "/knn-docs/_mapping")]
         + [("PUT", f"/knn-docs/_doc/{i}", d, {"pick": lambda r: r.get("result")}) for i, d in enumerate(KNN_DOCS, 1)]
         + [("POST", "/knn-docs/_refresh", None, {"pick": lambda r: True})] + [
    # Each similarity's scores.
    ("POST", K, {"knn": kq("l2", [0.5, 1, -1], k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("cos", [0.5, 1, -1], k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("dot", [0.6, 0.8], k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("mip", [0.5, 1, -1], k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("byte", [5, -7, 9], k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("byte", "05f909", k=5, num_candidates=10), "_source": False}),
    ("POST", K, {"knn": kq("bytel2", [5, -7, 9], k=2), "_source": False}),
    ("POST", K, {"knn": kq("bits", [1, 2], k=5), "_source": False}),
    ("POST", K, {"knn": kq("bits", "0102", k=5, similarity=2), "_source": False}),
    ("POST", K, {"knn": kq("q8", [0.5, 1, -1], k=3)}, {"pick": knn_ids}),
    # k, num_candidates, size and from.
    ("POST", K, {"knn": kq("l2", [0, 0, 0]), "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0]), "size": 2, "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0]), "size": 2, "from": 1, "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=3), "from": 1, "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], num_candidates=3), "size": 2}, {"pick": knn_ids}),
    # filter, similarity, boost, _name, combined with a query.
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=2, filter={"term": {"name": "a"}}), "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=2, filter=[{"term": {"name": "a"}}, {"ids": {"values": ["4"]}}]), "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=4, similarity=3), "_source": False}),
    ("POST", K, {"knn": kq("cos", [1, 2, 3], k=4, similarity=0.5), "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=2, boost=2, _name="near"),
                 "query": {"constant_score": {"filter": {"term": {"name": "c"}}, "_name": "is_c"}}, "_source": False}),
    ("POST", K, {"knn": [kq("l2", [0, 0, 0], k=2), kq("cos", [3, -2, 0], k=2, boost=0.5)], "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=3), "aggs": {"names": {"terms": {"field": "name"}}}, "size": 1},
     {"pick": lambda r: (knn_ids(r), r["aggregations"])}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=3), "sort": [{"name": "desc"}], "_source": False}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=3), "fields": ["name", "l2"], "_source": False}),
    ("POST", "/knn-docs/_search?search_type=query_then_fetch", {"knn": kq("l2", [0, 0, 0])}),
    # Nested vectors: the nearest parents, with their nearest objects.
    ("POST", K, {"knn": kq("paras.vec", [1, 1, 0], k=2, inner_hits={"size": 2, "_source": False, "fields": ["paras.pid"]}),
                 "_source": False}, {"pick": knn_inner}),
    ("POST", K, {"knn": kq("paras.vec", [1, 1, 0], k=3, similarity=1.5, inner_hits={"_source": False}),
                 "_source": False}, {"pick": knn_inner}),
    ("POST", K, {"knn": kq("paras.vec", [1, 1, 0], k=3, filter={"term": {"name": "c"}}), "_source": False}),
    ("POST", K, {"query": {"nested": {"path": "paras", "query": {"knn": kq("paras.vec", [1, 1, 0], k=2)},
                                      "inner_hits": {"_source": False}}}, "_source": False}, {"pick": knn_inner}),
    # The knn query.
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], k=2)}, "_source": False}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0])}, "size": 1, "_source": False}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], num_candidates=2)}, "_source": False}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], k=2, boost=3, _name="kq", filter={"term": {"name": "a"}})}, "_source": False}),
    ("POST", K, {"query": {"bool": {"must": [{"knn": kq("l2", [0, 0, 0], k=2)}], "filter": [{"term": {"name": "a"}}]}}, "_source": False}),
    ("POST", K, {"query": {"bool": {"should": [{"knn": kq("l2", [0, 0, 0], k=1)}, {"constant_score": {"filter": {"term": {"name": "c"}}}}]}}, "_source": False}),
    ("POST", K, {"query": {"dis_max": {"queries": [{"knn": kq("l2", [0, 0, 0], k=2)}, {"constant_score": {"filter": {"term": {"name": "b"}}}}], "tie_breaker": 0.5}}, "_source": False}),
    ("POST", K, {"query": {"function_score": {"query": {"knn": kq("l2", [0, 0, 0], k=3)},
                                              "functions": [{"filter": {"term": {"name": "a"}}, "weight": 10}]}}, "_source": False}),
    ("POST", K, {"query": {"constant_score": {"filter": {"knn": kq("l2", [0, 0, 0], k=2)}}}, "_source": False}),
    ("POST", "/knn-docs/_count", {"query": {"knn": kq("l2", [0, 0, 0], k=2)}}),
    # Vector functions in scoring scripts.
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "l2"}},
                                            "script": {"source": "cosineSimilarity(params.qv, 'l2') + 1.0", "params": {"qv": [0.5, 1, -1]}}}},
                 "_source": False}),
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "l2"}},
                                            "script": {"source": "dotProduct(params.qv, 'l2') + 100", "params": {"qv": [0.5, 1, -1]}}}},
                 "_source": False}),
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "l2"}},
                                            "script": {"source": "1 / (1 + l1norm(params.qv, 'l2'))", "params": {"qv": [0.5, 1, -1]}}}},
                 "_source": False}),
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "l2"}},
                                            "script": {"source": "1 / (1 + l2norm(params.qv, doc['l2']))", "params": {"qv": [0.5, 1, -1]}}}},
                 "_source": False}),
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "byte"}},
                                            "script": {"source": "1 / (1 + hamming(params.qv, 'byte'))", "params": {"qv": [5, -7, 9]}}}},
                 "_source": False}),
    ("POST", K, {"query": {"script_score": {"query": {"exists": {"field": "raw"}},
                                            "script": {"source": "doc['raw'].vectorValue[0] + doc['raw'].magnitude"}}},
                 "_source": False}),
    # Search errors.
    ("POST", K, {"knn": kq("nope", [0, 0, 0])}),
    ("POST", K, {"knn": kq("name", [0, 0, 0])}),
    ("POST", K, {"knn": kq("raw", [0, 0, 0])}),
    ("POST", K, {"knn": kq("l2", [0, 0])}),
    ("POST", K, {"knn": kq("cos", [0, 0, 0])}),
    ("POST", K, {"knn": kq("dot", [1, 1])}),
    ("POST", K, {"knn": kq("byte", [1.5, 2, 3])}),
    ("POST", K, {"knn": kq("byte", [500, 2, 3])}),
    ("POST", K, {"knn": kq("byte", "abc")}),
    ("POST", K, {"knn": kq("l2", [1, "x", 3])}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=0)}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=5, num_candidates=2)}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], num_candidates=10001)}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], k=10001)}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0]), "size": 0}),
    ("POST", K, {"knn": {"field": "l2"}}),
    ("POST", K, {"knn": {"query_vector": [0, 0, 0]}}),
    ("POST", K, {"knn": kq("l2", [0, 0, 0], bogus=1)}),
    ("POST", K, {"knn": 5}),
    ("POST", K, {"knn": []}, {"pick": knn_ids}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], k=5, num_candidates=3)}}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], k=0)}}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0], inner_hits={})}}),
    ("POST", K, {"query": {"knn": {"field": "l2"}}}),
    ("POST", K, {"query": {"knn": kq("nope", [0, 0, 0])}}),
    ("POST", K, {"query": {"knn": kq("l2", [0, 0, 0])}, "size": 0}),
    # Only knn and exists queries work on a vector field.
    ("POST", K, {"query": {"exists": {"field": "l2"}}}, {"pick": knn_ids}),
    ("POST", K, {"query": {"term": {"l2": 1}}}),
    ("POST", K, {"query": {"terms": {"l2": [1]}}}),
    ("POST", K, {"query": {"match": {"l2": "x"}}}),
    ("POST", K, {"query": {"range": {"l2": {"gte": 1}}}}),
    ("POST", K, {"query": {"prefix": {"l2": "x"}}}),
    ("POST", K, {"sort": ["l2"]}),
    ("POST", K, {"aggs": {"x": {"terms": {"field": "l2"}}}}),
    ("POST", K, {"aggs": {"x": {"avg": {"field": "l2"}}}}),
    ("POST", K, {"docvalue_fields": ["l2"]}),
    # Document checks.
    ("PUT", "/knn-docs/_doc/x", {"l2": [1, 2]}),
    ("PUT", "/knn-docs/_doc/x", {"l2": [1, 2, 3, 4]}),
    ("PUT", "/knn-docs/_doc/x", {"l2": [1, "a", 3]}),
    ("PUT", "/knn-docs/_doc/x", {"l2": "abc"}),
    ("PUT", "/knn-docs/_doc/x", {"l2": 5}),
    ("PUT", "/knn-docs/_doc/x", {"l2": {"a": 1}}),
    ("PUT", "/knn-docs/_doc/x", {"l2": [1e40, 0, 0]}),
    ("PUT", "/knn-docs/_doc/x", {"cos": [0, 0, 0]}),
    ("PUT", "/knn-docs/_doc/x", {"dot": [3, 4]}),
    ("PUT", "/knn-docs/_doc/x", {"byte": [1, 2, 300]}),
    ("PUT", "/knn-docs/_doc/x", {"byte": [1.5, 2, 3]}),
    ("PUT", "/knn-docs/_doc/x", {"bits": [1, 2, 3]}),
    ("PUT", "/knn-docs/_doc/x", {"paras": [{"vec": [1, 2]}]}),
    ("POST", "/_bulk", [{"index": {"_index": "knn-docs", "_id": "y"}}, {"l2": [1, 2]},
                        {"index": {"_index": "knn-docs", "_id": "z"}}, {"l2": [1, 2, 3]}],
     {"pick": lambda r: [(list(i.values())[0]["status"], list(i.values())[0].get("error", {}).get("type")) for i in r["items"]]}),
    ("PUT", "/knn-docs/_doc/x", {"l2": None, "name": "x"}, {"pick": lambda r: r.get("result")}),
    ("DELETE", "/knn-docs/_doc/x", None, {"pick": lambda r: r.get("result")}),
    ("DELETE", "/knn-docs/_doc/z", None, {"pick": lambda r: r.get("result")}),
    # Mapping updates.
    ("PUT", "/knn-docs/_mapping", {"properties": {"l2": {"type": "dense_vector", "dims": 3, "similarity": "l2_norm", "index_options": {"type": "int8_hnsw", "m": 20}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"l2": {"type": "dense_vector", "dims": 3, "similarity": "l2_norm", "index_options": {"type": "flat"}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"l2": {"type": "dense_vector", "dims": 3, "similarity": "l2_norm", "index_options": {"type": "int8_hnsw", "m": 16}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"l2": {"type": "dense_vector", "dims": 4, "similarity": "l2_norm", "index_options": {"type": "int8_hnsw", "m": 20}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"l2": {"type": "dense_vector", "dims": 3, "similarity": "cosine", "index_options": {"type": "int8_hnsw", "m": 20}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"dot": {"type": "dense_vector", "dims": 2, "similarity": "dot_product", "index_options": {"type": "int8_flat"}}}}),
    ("PUT", "/knn-docs/_mapping", {"properties": {"cos": {"type": "dense_vector", "dims": 3, "element_type": "byte", "index_options": {"type": "hnsw", "m": 32}}}}),
    ("GET", "/knn-docs/_mapping/field/l2,dot"),
    # Dynamic mapping: 128 to 4096 floats are a vector.
    ("DELETE", "/knn-dyn?ignore_unavailable=true"),
    ("PUT", "/knn-dyn/_doc/1", {"v": [i + 0.5 for i in range(128)], "short": [0.5] * 127, "ints": list(range(200)),
                                "long": [0.5] * 4097, "o": {"inner": 1}}, {"pick": lambda r: r.get("result")}),
    ("GET", "/knn-dyn/_mapping"),
    ("PUT", "/knn-dyn/_doc/2", {"v": [0.5, 1.5]}),
    ("PUT", "/knn-dyn/_doc/3", {"v": [0.0] * 128}),
    # Mapped without dims: the first document sets them.
    ("DELETE", "/knn-nodims?ignore_unavailable=true"),
    ("PUT", "/knn-nodims", {"mappings": {"properties": {"v": {"type": "dense_vector"}, "b": {"type": "dense_vector", "element_type": "byte"}}}}, ACK),
    ("POST", "/knn-nodims/_search", {"knn": kq("v", [1, 2, 3])}, {"pick": knn_ids}),
    ("GET", "/knn-nodims/_mapping"),
    ("PUT", "/knn-nodims/_doc/1?refresh=true", {"v": [1, 2, 3], "b": "807f0a"}, {"pick": lambda r: r.get("result")}),
    ("GET", "/knn-nodims/_mapping"),
    ("PUT", "/knn-nodims/_doc/2", {"v": [1, 2]}),
] + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 5000})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 0})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": "x"})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "similarity": "foo"})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index": False, "similarity": "cosine"})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index": False, "index_options": {"type": "hnsw"}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "element_type": "foo"})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index_options": {"type": "foo"}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index_options": {}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index_options": {"type": "hnsw", "bar": 1}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "foo": 1})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 8, "element_type": "bit", "similarity": "cosine"})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "element_type": "byte", "index_options": {"type": "int8_hnsw"}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index_options": {"type": "int4_hnsw"}})
  + knn_bad_mapping("knn-bad", {"type": "dense_vector", "dims": 3, "index": "x"})
  + knn_bad_mapping("knn-ok", {"type": "dense_vector", "dims": 4, "index_options": {"type": "int4_flat"}})
  + [("GET", "/knn-ok/_mapping")]
  + knn_bad_mapping("knn-ok", {"type": "dense_vector", "dims": 4, "index_options": {"type": "int8_hnsw", "confidence_interval": 0.5, "m": 8}})
  + [("GET", "/knn-ok/_mapping")]
  + knn_bad_mapping("knn-ok", {"type": "dense_vector", "dims": 4, "element_type": "byte", "index_options": {"type": "flat"}})
  + [("GET", "/knn-ok/_mapping")] + [
    ("DELETE", "/knn-docs"), ("DELETE", "/knn-dyn"), ("DELETE", "/knn-nodims"),
    ("DELETE", "/knn-ok"), ("DELETE", "/knn-bad?ignore_unavailable=true"),
])


# --- Index lifecycle and admin (rollover, resize, blocks, resolve,
# snapshots, ingest, stored scripts, reindex). Names start with `lc-`.

def lc_strip(v):
    """Drops the volatile parts of lifecycle responses (timestamps,
    durations, uuids, node ids)."""
    drop = {"timestamp", "start_time", "start_time_in_millis", "end_time", "end_time_in_millis",
            "duration_in_millis", "duration", "time_in_millis", "took", "uuid", "creation_date",
            "creation_date_string", "size_in_bytes", "file_count", "size", "nodes", "ingest_took"}
    if isinstance(v, dict):
        return {k: lc_strip(x) for k, x in v.items() if k not in drop}
    if isinstance(v, list):
        return [lc_strip(x) for x in v]
    return v


LC_STRIP = {"pick": lc_strip}


def lc_clean(names):
    return [("DELETE", f"/{names}?ignore_unavailable=true")]


LC_UNBLOCK = {f"index.blocks.{b}": None for b in ("read_only", "metadata", "write", "read", "read_only_allow_delete")}


scenario("lc_rollover", lc_clean("lc-roll-1,lc-roll-000002,lc-roll-explicit,lc-rw-000001,lc-rw-000002,lc-rplain,lc-rtwo-1,lc-rtwo-2") + [
    ("PUT", "/lc-roll-1", {"aliases": {"lc-roll": {}, "lc-roll-search": {}}}, ACK),
    ("POST", "/lc-roll/_rollover", {"conditions": {"max_docs": 1}}),
    ("PUT", "/lc-roll-1/_doc/1?refresh=true", {"foo": "hello world"}, {"pick": lambda r: r["result"]}),
    ("POST", "/lc-roll/_rollover?dry_run=true", {"conditions": {"max_docs": 1}}),
    ("POST", "/lc-roll/_rollover", {"conditions": {"max_docs": 1, "min_age": "7d"}}),
    ("POST", "/lc-roll/_rollover", {"conditions": {"max_docs": 1, "min_docs": 1, "max_size": "1b",
                                                   "max_primary_shard_docs": 5, "min_size": "0b"}}),
    ("GET", "/_alias/lc-roll"),
    ("GET", "/_alias/lc-roll-search"),
    ("POST", "/lc-roll/_rollover", {"conditions": {"min_docs": 1}}),
    ("POST", "/lc-roll/_rollover", {"conditions": {"bogus": 1}}),
    ("POST", "/lc-roll/_rollover", {"conditions": {"max_docs": "x"}}),
    ("POST", "/lc-roll/_rollover", {"bogus": {}}),
    ("POST", "/lc-roll/_rollover?lazy=true"),
    ("POST", "/lc-roll-1/_rollover"),
    ("POST", "/lc-roll-nosuch/_rollover"),
    ("POST", "/lc-roll-search/_rollover"),
    ("POST", "/lc-roll/_rollover/lc-roll-000002"),
    ("POST", "/lc-roll/_rollover/Bad-Name"),
    ("POST", "/lc-roll/_rollover/lc-roll-explicit", {"conditions": {"max_age": "0s"},
                                                    "settings": {"index.number_of_shards": 2},
                                                    "mappings": {"properties": {"k": {"type": "keyword"}}},
                                                    "aliases": {"lc-roll-extra": {}}}),
    ("GET", "/_alias/lc-roll,lc-roll-extra"),
    ("GET", "/lc-roll-explicit/_mapping"),
    ("GET", "/lc-roll-explicit/_settings", None, {"pick": lambda r: r["lc-roll-explicit"]["settings"]["index"]["number_of_shards"]}),
    # An explicit write index stays in the alias, no longer the write index.
    ("PUT", "/lc-rw-000001", {"aliases": {"lc-rw": {"is_write_index": True}, "lc-rw-other": {}}}, ACK),
    ("POST", "/lc-rw/_rollover"),
    ("GET", "/_alias/lc-rw,lc-rw-other"),
    ("POST", "/lc-rw-other/_rollover"),
    ("PUT", "/lc-rw/_doc/1?refresh=true", {"a": 1}, {"pick": lambda r: r["_index"]}),
    # No write index among several; a name without a number.
    ("PUT", "/lc-rtwo-1", {"aliases": {"lc-rtwo": {}}}, ACK),
    ("PUT", "/lc-rtwo-2", {"aliases": {"lc-rtwo": {}}}, ACK),
    ("POST", "/lc-rtwo/_rollover"),
    ("PUT", "/lc-rplain", {"aliases": {"lc-rplain-a": {}}}, ACK),
    ("POST", "/lc-rplain-a/_rollover"),
] + lc_clean("lc-roll-1,lc-roll-000002,lc-roll-explicit,lc-rw-000001,lc-rw-000002,lc-rplain,lc-rtwo-1,lc-rtwo-2"))

scenario("lc_resize", lc_clean("lc-src,lc-tgt,lc-tgt-shrunk,lc-tgt-clone,lc-tgt-x") + [
    ("PUT", "/lc-src", {"settings": {"number_of_shards": 2, "number_of_replicas": 0},
                        "mappings": {"properties": {"foo": {"type": "text"}, "n": {"type": "nested"}}},
                        "aliases": {"lc-src-alias": {}}}, ACK),
    ("PUT", "/lc-src/_doc/1", {"foo": "hello world", "n": [{"a": 1}]}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-src/_doc/2?routing=r1", {"foo": "hello world 2"}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-src/_doc/3?refresh=true", {"foo": "hello world 3"}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 4}}),
    ("PUT", "/lc-src/_clone/lc-tgt", {"settings": {"index.number_of_shards": 3}}),
    ("PUT", "/lc-src/_shrink/lc-tgt", {"settings": {"index.number_of_shards": 3}}),
    ("PUT", "/lc-src/_settings", {"index.blocks.write": True}, ACK),
    ("PUT", "/lc-src/_split/lc-tgt", {}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 1}}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 3}}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 4, "index.number_of_routing_shards": 8}}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 4}, "mappings": {}}),
    ("PUT", "/lc-src/_split/Lc-Tgt", {"settings": {"index.number_of_shards": 4}}),
    ("PUT", "/lc-nosuch/_split/lc-tgt", {"settings": {"index.number_of_shards": 4}}),
    ("PUT", "/lc-src-alias/_clone/lc-tgt"),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 4, "index.number_of_replicas": 0},
                                     "aliases": {"lc-tgt-alias": {}}}),
    ("PUT", "/lc-src/_split/lc-tgt", {"settings": {"index.number_of_shards": 4}}),
    ("GET", "/lc-tgt", None, LC_STRIP),
    ("GET", "/lc-tgt/_doc/1"),
    ("GET", "/lc-tgt/_doc/2?routing=r1"),
    ("POST", "/lc-tgt/_search", {"query": {"match": {"foo": "hello"}}}, {"pick": lambda r: sorted(h["_id"] for h in r["hits"]["hits"])}),
    ("POST", "/lc-tgt/_search", {"query": {"nested": {"path": "n", "query": {"term": {"n.a": 1}}}}}, {"pick": lambda r: [h["_id"] for h in r["hits"]["hits"]]}),
    ("PUT", "/lc-src/_shrink/lc-tgt-shrunk", {"settings": {"index.number_of_replicas": 0}}),
    ("GET", "/lc-tgt-shrunk/_settings", None, {"pick": lambda r: {k: v for k, v in r["lc-tgt-shrunk"]["settings"]["index"].items() if k in ("number_of_shards", "number_of_replicas", "blocks", "routing_partition_size")}}),
    ("GET", "/lc-tgt-shrunk/_doc/3", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/lc-src/_clone/lc-tgt-clone"),
    ("GET", "/lc-tgt-clone/_settings", None, {"pick": lambda r: {k: v for k, v in r["lc-tgt-clone"]["settings"]["index"].items() if k in ("number_of_shards", "number_of_replicas", "blocks")}}),
    ("GET", "/lc-tgt-clone/_count", None, {"pick": lambda r: r["count"]}),
    ("PUT", "/lc-tgt-clone/_doc/9", {"a": 1}),
    ("PUT", "/lc-tgt-clone/_settings", {"index.blocks.write": False}, ACK),
    ("PUT", "/lc-tgt-clone/_doc/9", {"a": 1}, {"pick": lambda r: (r["result"], r["_version"])}),
    ("PUT", "/lc-src/_shrink/lc-tgt-x", {"settings": {"index.number_of_shards": 1}, "max_primary_shard_size": "1gb"}),
    ("PUT", "/lc-src/_shrink/lc-tgt-x", {"max_primary_shard_size": "1b"}),
    ("GET", "/lc-tgt-x/_settings", None, {"pick": lambda r: r["lc-tgt-x"]["settings"]["index"]["number_of_shards"]}),
] + lc_clean("lc-src,lc-tgt,lc-tgt-shrunk,lc-tgt-clone,lc-tgt-x"))

scenario("lc_blocks", [("PUT", "/lc-blk/_settings", LC_UNBLOCK), ("PUT", "/lc-blk2/_settings", LC_UNBLOCK)]
         + lc_clean("lc-blk,lc-blk2") + [
    ("PUT", "/lc-blk", {"settings": {"number_of_shards": 1}}, ACK),
    ("PUT", "/lc-blk/_doc/1?refresh=true", {"a": 1}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-blk/_block/write"),
    ("PUT", "/lc-blk/_doc/2", {"a": 1}),
    ("POST", "/lc-blk/_doc", {"a": 1}),
    ("DELETE", "/lc-blk/_doc/1"),
    ("POST", "/lc-blk/_update/1", {"doc": {"a": 2}}),
    ("POST", "/_bulk", [{"index": {"_index": "lc-blk", "_id": "3"}}, {"a": 1}, {"delete": {"_index": "lc-blk", "_id": "1"}}],
     {"pick": lambda r: [(list(i.values())[0]["status"], list(i.values())[0].get("error", {}).get("type")) for i in r["items"]]}),
    ("GET", "/lc-blk/_doc/1"),
    ("POST", "/lc-blk/_delete_by_query", {"query": {"match_all": {}}}, LC_STRIP),
    ("POST", "/lc-blk/_update_by_query", {"query": {"match_all": {}}}, LC_STRIP),
    ("POST", "/lc-blk/_search", {"size": 0}, {"pick": lambda r: r["hits"]["total"]}),
    ("PUT", "/lc-blk/_mapping", {"properties": {"b": {"type": "keyword"}}}),
    ("PUT", "/lc-blk/_settings", {"index.blocks.write": False}, ACK),
    ("PUT", "/lc-blk/_block/read_only"),
    ("PUT", "/lc-blk/_doc/2", {"a": 1}),
    ("PUT", "/lc-blk/_mapping", {"properties": {"c": {"type": "keyword"}}}),
    ("PUT", "/lc-blk/_settings", {"index.refresh_interval": "2s"}),
    ("PUT", "/lc-blk/_alias/lc-blk-a"),
    ("POST", "/lc-blk/_close"),
    ("DELETE", "/lc-blk"),
    ("GET", "/lc-blk/_mapping"),
    ("PUT", "/lc-blk/_settings", {"index.blocks.read_only": False}, ACK),
    ("PUT", "/lc-blk/_block/read"),
    ("GET", "/lc-blk/_doc/1"),
    ("POST", "/lc-blk/_search"),
    ("POST", "/lc-blk/_count"),
    ("PUT", "/lc-blk/_doc/5?refresh=true", {"a": 5}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-blk/_settings", {"index.blocks.read": False}, ACK),
    ("PUT", "/lc-blk/_block/metadata"),
    ("GET", "/lc-blk/_mapping"),
    ("GET", "/lc-blk/_settings"),
    ("GET", "/lc-blk"),
    ("PUT", "/lc-blk/_doc/6?refresh=true", {"a": 6}, {"pick": lambda r: r["result"]}),
    ("POST", "/lc-blk/_search", {"size": 0}, {"pick": lambda r: r["hits"]["total"]}),
    ("DELETE", "/lc-blk"),
    ("PUT", "/lc-blk/_settings", {"index.blocks.metadata": False}, ACK),
    ("PUT", "/lc-blk/_settings", {"index.blocks.read_only_allow_delete": True}, ACK),
    ("PUT", "/lc-blk/_doc/7", {"a": 7}),
    ("PUT", "/lc-blk/_mapping", {"properties": {"d": {"type": "keyword"}}}, ACK),
    ("PUT", "/lc-blk/_block/bogus"),
    ("PUT", "/lc-blk-nosuch/_block/write"),
    ("PUT", "/lc-blk/_settings", {"index.blocks.read_only_allow_delete": None}, ACK),
    ("PUT", "/lc-blk2", {"settings": {"index.blocks.write": True, "index.blocks.read_only": True}}, ACK),
    ("PUT", "/lc-blk2/_doc/1", {"a": 1}),
    ("PUT", "/lc-blk2/_settings", {"index.blocks.write": None, "index.blocks.read_only": None}, ACK),
] + lc_clean("lc-blk,lc-blk2"))

scenario("lc_resolve", lc_clean("lc-res1,lc-res2,lc-res3") + [
    ("PUT", "/lc-res1", {"aliases": {"lc-res-a": {}, "lc-res-b": {"is_write_index": True}}}, ACK),
    ("PUT", "/lc-res2", {"aliases": {"lc-res-a": {}}, "settings": {"index.hidden": True}}, ACK),
    ("PUT", "/lc-res3", {"aliases": {"lc-res-c": {}}}, ACK),
    ("POST", "/lc-res3/_close", None, ACK),
    ("GET", "/_resolve/index/lc-res*"),
    ("GET", "/_resolve/index/lc-res*?expand_wildcards=all"),
    ("GET", "/_resolve/index/lc-res*?expand_wildcards=closed"),
    ("GET", "/_resolve/index/lc-res*,-lc-res1"),
    ("GET", "/_resolve/index/lc-res-a"),
    ("GET", "/_resolve/index/lc-res2"),
    ("GET", "/_resolve/index/lc-res-nosuch"),
    ("GET", "/_resolve/index/lc-res-nosuch?ignore_unavailable=true"),
    ("GET", "/_resolve/index/lc-res-nosuch*"),
    ("GET", "/_resolve/cluster/lc-res*"),
    ("GET", "/_resolve/cluster/lc-res3"),
    ("GET", "/_resolve/cluster/lc-res3*?expand_wildcards=closed"),
    ("GET", "/_resolve/cluster/lc-res-a"),
    ("GET", "/_resolve/cluster/lc-res-a,lc-res-nosuch"),
    ("GET", "/_resolve/cluster/lc-res-a,lc-res-nosuch*"),
] + lc_clean("lc-res1,lc-res2,lc-res3"))


def lc_snap_sorted(s):
    s = lc_strip(s)
    if isinstance(s.get("indices"), list):
        s["indices"] = sorted(s["indices"])
    return s


def lc_snap_info(r):
    return [lc_snap_sorted(s) for s in r["snapshots"]]


scenario("lc_snapshots", lc_clean("lc-sn1,lc-sn2,lc-snr1,lc-snq2") + [
    ("DELETE", "/_snapshot/lc-repo-a/*"), ("DELETE", "/_snapshot/lc-repo-a"),
    ("DELETE", "/_snapshot/lc-repo-b"), ("DELETE", "/_snapshot/lc-repo-ro"),
    ("PUT", "/_snapshot/lc-repo-a", {"type": "fs", "settings": {"location": "lc-repo-a-loc", "compress": True}}),
    ("GET", "/_snapshot/lc-repo-a"),
    ("GET", "/_snapshot/lc-repo-nosuch"),
    ("PUT", "/_snapshot/lc-repo-b", {"type": "fs", "settings": {}}),
    ("PUT", "/_snapshot/lc-repo-b", {"type": "nope", "settings": {}}),
    ("PUT", "/_snapshot/lc-repo-b", {"settings": {"location": "x"}}),
    ("POST", "/_snapshot/lc-repo-a/_verify", None, {"pick": lambda r: sorted(r)}),
    ("POST", "/_snapshot/lc-repo-a/_cleanup"),
    ("POST", "/_snapshot/lc-repo-nosuch/_verify"),
    ("PUT", "/lc-sn1", {"settings": {"number_of_shards": 1, "number_of_replicas": 0}, "aliases": {"lc-sn-alias": {}}}, ACK),
    ("PUT", "/lc-sn2", {"settings": {"number_of_shards": 2, "number_of_replicas": 0}}, ACK),
    ("PUT", "/lc-sn1/_doc/1?refresh=true", {"a": 1}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-sn2/_doc/1?refresh=true", {"b": 2}, {"pick": lambda r: r["result"]}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s1?wait_for_completion=true",
     {"indices": "lc-sn1,lc-sn2", "include_global_state": False, "metadata": {"by": "me"}},
     {"pick": lambda r: lc_snap_sorted(r["snapshot"])}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s1", {"indices": "lc-sn1"}),
    ("PUT", "/_snapshot/lc-repo-a/Lc-Bad", {"indices": "lc-sn1"}),
    ("PUT", "/_snapshot/lc-repo-a/_bad", {"indices": "lc-sn1"}),
    ("PUT", "/_snapshot/lc-repo-nosuch/lc-s1", {"indices": "lc-sn1"}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s2?wait_for_completion=true", {"indices": "lc-sn-missing"}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s2?wait_for_completion=true",
     {"indices": "lc-sn-missing", "ignore_unavailable": True, "include_global_state": False}, LC_STRIP),
    ("PUT", "/_snapshot/lc-repo-a/lc-s3?wait_for_completion=true", {"indices": ["lc-sn2"], "feature_states": ["none"]}, LC_STRIP),
    ("PUT", "/_snapshot/lc-repo-a/lc-s4", {"indices": "lc-sn1", "include_global_state": False}),
    ("sleep", 2),
    ("GET", "/_snapshot/lc-repo-a/lc-s1", None, {"pick": lc_snap_info}),
    ("GET", "/_snapshot/lc-repo-a/lc-s1?verbose=false", None, {"pick": lc_snap_info}),
    ("GET", "/_snapshot/lc-repo-a/lc-s1?index_details=true&human=true&include_repository=false&index_names=false", None, {"pick": lc_snap_info}),
    ("GET", "/_snapshot/lc-repo-a/lc-nosuch"),
    ("GET", "/_snapshot/lc-repo-a/lc-nosuch?ignore_unavailable=true"),
    ("GET", "/_snapshot/lc-repo-a/lc-s1,lc-nosuch"),
    ("GET", "/_snapshot/lc-repo-a/_all?sort=name", None, {"pick": lambda r: ([s["snapshot"] for s in r["snapshots"]], r["total"], r["remaining"])}),
    ("GET", "/_snapshot/lc-repo-a/*?sort=name&order=desc&size=2", None, {"pick": lambda r: ([s["snapshot"] for s in r["snapshots"]], r["total"], r["remaining"], r.get("next"))}),
    ("GET", "/_snapshot/lc-repo-a/*?sort=name&size=1&offset=1", None, {"pick": lambda r: ([s["snapshot"] for s in r["snapshots"]], r["total"], r["remaining"])}),
    ("GET", "/_snapshot/lc-repo-a/*?sort=index_count", None, {"pick": lambda r: [s["snapshot"] for s in r["snapshots"]]}),
    ("GET", "/_snapshot/lc-repo-a/*?sort=bogus"),
    ("GET", "/_snapshot/lc-repo-a/_current"),
    ("GET", "/_snapshot/lc-repo-a/lc-s1/_status", None, LC_STRIP),
    ("GET", "/_snapshot/lc-repo-a/lc-nosuch/_status"),
    ("GET", "/_snapshot/lc-repo-a/lc-nosuch/_status?ignore_unavailable=true"),
    ("GET", "/_snapshot/lc-repo-a/_status"),
    ("GET", "/_snapshot/lc-repo-a", None, {"pick": lambda r: sorted(r["lc-repo-a"])}),
    # Clone.
    ("PUT", "/_snapshot/lc-repo-a/lc-s1/_clone/lc-s5", {"indices": "lc-sn2"}),
    ("GET", "/_snapshot/lc-repo-a/lc-s5", None, {"pick": lc_snap_info}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s1/_clone/lc-s5", {"indices": "lc-sn2"}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s1/_clone/lc-s6", {"indices": "nomatch"}),
    ("PUT", "/_snapshot/lc-repo-a/lc-s1/_clone/lc-s6", {}),
    ("PUT", "/_snapshot/lc-repo-a/lc-nosuch/_clone/lc-s6", {"indices": "lc-sn1"}),
    # Restore.
    ("POST", "/_snapshot/lc-repo-a/lc-s1/_restore", {"indices": "lc-sn1"}),
    ("POST", "/_snapshot/lc-repo-a/lc-s1/_restore?wait_for_completion=true",
     {"indices": "lc-sn1", "rename_pattern": "lc-sn(.+)", "rename_replacement": "lc-snr$1"}),
    ("GET", "/lc-snr1/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("GET", "/_alias/lc-sn-alias"),
    ("POST", "/_snapshot/lc-repo-a/lc-s1/_restore",
     {"indices": "lc-sn2", "rename_pattern": "lc-sn(.+)", "rename_replacement": "lc-snq$1", "include_aliases": False,
      "index_settings": {"index.number_of_replicas": 0}}),
    ("GET", "/lc-snq2/_settings", None, {"pick": lambda r: r["lc-snq2"]["settings"]["index"]["number_of_replicas"]}),
    ("DELETE", "/lc-sn1", None, ACK),
    ("POST", "/_snapshot/lc-repo-a/lc-s1/_restore?wait_for_completion=true", {"indices": "lc-sn1"}),
    ("GET", "/lc-sn1/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("GET", "/_alias/lc-sn-alias", None, {"pick": lambda r: sorted(r)}),
    ("POST", "/lc-sn2/_close", None, ACK),
    ("POST", "/_snapshot/lc-repo-a/lc-s3/_restore?wait_for_completion=true"),
    ("GET", "/lc-sn2/_count", None, {"pick": lambda r: r["count"]}),
    ("POST", "/_snapshot/lc-repo-a/lc-nosuch/_restore"),
    # A read-only repository over the same location sees the snapshots.
    ("PUT", "/_snapshot/lc-repo-ro", {"type": "fs", "settings": {"location": "lc-repo-a-loc", "readonly": True}}),
    ("GET", "/_snapshot/lc-repo-ro/*?sort=name", None, {"pick": lambda r: [s["snapshot"] for s in r["snapshots"]]}),
    ("PUT", "/_snapshot/lc-repo-ro/lc-s9?wait_for_completion=true", {"indices": "lc-sn1"}),
    ("DELETE", "/_snapshot/lc-repo-ro/lc-s1"),
    ("GET", "/_snapshot/lc-repo-ro", None, {"pick": lambda r: lc_strip(r)}),
    ("GET", "/_snapshot/lc-repo-a,lc-repo-ro/lc-s1", None, {"pick": lambda r: [(s["snapshot"], s["repository"]) for s in r["snapshots"]]}),
    # Delete.
    ("DELETE", "/_snapshot/lc-repo-a/lc-nosuch"),
    ("DELETE", "/_snapshot/lc-repo-a/lc-s1,lc-s2"),
    ("DELETE", "/_snapshot/lc-repo-a/lc-s*"),
    ("GET", "/_snapshot/lc-repo-a/_all"),
    ("DELETE", "/_snapshot/lc-repo-nosuch/lc-s1"),
    ("DELETE", "/_snapshot/lc-repo-ro"),
    ("DELETE", "/_snapshot/lc-repo-a"),
    ("DELETE", "/_snapshot/lc-repo-a"),
] + lc_clean("lc-sn1,lc-sn2,lc-snr1,lc-snq2"))

LC_PIPE = "/_ingest/pipeline/_simulate"


def lc_sim(processors, *docs):
    return ("POST", LC_PIPE, {"pipeline": {"processors": processors}, "docs": [{"_source": d} for d in docs]}, LC_STRIP)


scenario("lc_ingest_crud", [
    ("DELETE", "/_ingest/pipeline/lc-p*"),
    ("PUT", "/_ingest/pipeline/lc-p1", {"description": "d", "version": 3, "_meta": {"k": 1},
                                        "processors": [{"set": {"field": "x", "value": "{{a}}-y", "tag": "t"}}]}),
    ("GET", "/_ingest/pipeline/lc-p1"),
    ("GET", "/_ingest/pipeline/lc-p1?summary=true"),
    ("GET", "/_ingest/pipeline/lc-p*"),
    ("GET", "/_ingest/pipeline/lc-nosuch"),
    ("DELETE", "/_ingest/pipeline/lc-nosuch"),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"nope": {}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"set": {"value": 1}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"set": {"field": "a"}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": {"set": {"field": "a", "value": 1}}}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"set": {"field": "a", "value": 1, "bogus": 2}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"description": "x", "processors": [], "invalid_field": {}}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"description": "x"}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"convert": {"field": "a", "type": "bogus"}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"rename": {"field": "a"}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"set": {"field": "a", "value": 1}}], "on_failure": []}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"foreach": {"field": "a", "processor": {"nope": {}}}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [{"grok": {"field": "a", "patterns": ["%{NOPE:x}"]}}]}),
    ("PUT", "/_ingest/pipeline/lc-p2", {"processors": [], "version": "x"}),
    ("DELETE", "/_ingest/pipeline/lc-p1"),
    ("GET", "/_ingest/pipeline/lc-p1"),
])

scenario("lc_ingest_processors", [
    lc_sim([{"set": {"field": "x", "value": "{{a}}-{{{b.c}}}"}}, {"set": {"field": "o.p", "value": {"k": "{{a}}"}}},
            {"set": {"field": "a", "value": "new", "override": False}}, {"set": {"field": "e", "value": "", "ignore_empty_value": True}},
            {"set": {"field": "copy", "copy_from": "b"}}],
           {"a": "A", "b": {"c": 1}}),
    lc_sim([{"lowercase": {"field": "s"}}, {"uppercase": {"field": "l"}}, {"trim": {"field": "t", "target_field": "t2"}}],
           {"s": "AbC", "l": ["x", "y"], "t": "  pad  "}),
    lc_sim([{"lowercase": {"field": "missing"}}], {"a": 1}),
    lc_sim([{"lowercase": {"field": "missing", "ignore_missing": True}}], {"a": 1}),
    lc_sim([{"lowercase": {"field": "n"}}], {"n": 5}),
    lc_sim([{"lowercase": {"field": "n"}}], {"n": None}),
    lc_sim([{"remove": {"field": ["a", "b.c"]}}, {"remove": {"field": "zz", "ignore_missing": True}}], {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}),
    lc_sim([{"remove": {"field": "zz"}}], {"a": 1}),
    lc_sim([{"remove": {"keep": ["a", "b.c"]}}], {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}),
    lc_sim([{"rename": {"field": "a", "target_field": "b.x"}}], {"a": 1, "b": {}}),
    lc_sim([{"rename": {"field": "a", "target_field": "c"}}], {"a": 1, "c": 2}),
    lc_sim([{"rename": {"field": "a", "target_field": "c", "override": True}}], {"a": 1, "c": 2}),
    lc_sim([{"rename": {"field": "zz", "target_field": "c"}}], {"a": 1}),
    lc_sim([{"append": {"field": "t", "value": ["b", "c"]}}, {"append": {"field": "new", "value": "x"}},
            {"append": {"field": "s", "value": "z"}}, {"append": {"field": "d", "value": ["a"], "allow_duplicates": False}}],
           {"t": ["a"], "s": "y", "d": ["a"]}),
    lc_sim([{"convert": {"field": "i", "type": "integer"}}, {"convert": {"field": "f", "type": "float"}},
            {"convert": {"field": "b", "type": "boolean"}}, {"convert": {"field": "s", "type": "string"}},
            {"convert": {"field": "l", "type": "long"}}, {"convert": {"field": "au", "type": "auto"}},
            {"convert": {"field": "lst", "type": "integer"}}, {"convert": {"field": "d", "type": "double", "target_field": "d2"}}],
           {"i": "42", "f": "1.5", "b": "TRUE", "s": 7, "l": "123456789012", "au": "3.25", "lst": ["1", "2"], "d": "2"}),
    lc_sim([{"convert": {"field": "i", "type": "integer"}}], {"i": "x1"}),
    lc_sim([{"convert": {"field": "i", "type": "boolean"}}], {"i": "yes"}),
    lc_sim([{"date": {"field": "d", "formats": ["ISO8601"]}}, {"date": {"field": "u", "formats": ["UNIX"], "target_field": "u2"}},
            {"date": {"field": "m", "formats": ["UNIX_MS"], "target_field": "m2"}},
            {"date": {"field": "p", "formats": ["dd/MM/yyyy"], "target_field": "p2", "output_format": "yyyy-MM-dd"}}],
           {"d": "2024-01-15T10:00:00Z", "u": "1700000000", "m": "1700000000123", "p": "15/01/2024"}),
    lc_sim([{"date": {"field": "d", "formats": ["ISO8601"]}}], {"d": "not a date"}),
    lc_sim([{"split": {"field": "s", "separator": ","}}, {"split": {"field": "w", "separator": "\\s+"}},
            {"split": {"field": "p", "separator": ",", "preserve_trailing": True}}],
           {"s": "a,b,,", "w": "one  two three", "p": "a,b,,"}),
    lc_sim([{"join": {"field": "l", "separator": "-"}}], {"l": ["a", 1, True]}),
    lc_sim([{"join": {"field": "l", "separator": "-"}}], {"l": "abc"}),
    lc_sim([{"gsub": {"field": "g", "pattern": "(\\d+)", "replacement": "<$1>"}}, {"gsub": {"field": "h", "pattern": "\\.", "replacement": "-"}}],
           {"g": "a12b3", "h": "1.2.3"}),
    lc_sim([{"grok": {"field": "m", "patterns": ["%{IP:client.ip} %{WORD:verb} %{URIPATHPARAM:req} %{NUMBER:bytes:int} %{NUMBER:dur:float}"]}}],
           {"m": "55.3.244.1 GET /index.html?a=1 15824 0.043"}),
    lc_sim([{"grok": {"field": "m", "patterns": ["%{FOO:x}-%{WORD:y}", "%{TIMESTAMP_ISO8601:ts} %{LOGLEVEL:level} %{GREEDYDATA:msg}"],
                      "pattern_definitions": {"FOO": "[a-z]+"}, "trace_match": True}}],
           {"m": "2024-01-15T10:00:00Z ERROR something broke"}),
    lc_sim([{"grok": {"field": "m", "patterns": ["%{INT:n}"]}}], {"m": "abc"}),
    lc_sim([{"dissect": {"field": "m", "pattern": "%{a} %{b} [%{c}] %{+a}"}}], {"m": "x y [z] w"}),
    lc_sim([{"dissect": {"field": "m", "pattern": "%{a}|%{b}"}}], {"m": "nope"}),
    lc_sim([{"json": {"field": "j"}}, {"json": {"field": "k", "target_field": "kk"}}, {"json": {"field": "r", "add_to_root": True}}],
           {"j": "{\"a\": [1, 2]}", "k": "3", "r": "{\"top\": true}"}),
    lc_sim([{"kv": {"field": "q", "field_split": "&", "value_split": "="}},
            {"kv": {"field": "q", "field_split": "&", "value_split": "=", "target_field": "p", "prefix": "x_", "exclude_keys": ["b"]}}],
           {"q": "a=1&b=2&a=3"}),
    lc_sim([{"dot_expander": {"field": "a.b"}}, {"dot_expander": {"field": "*", "path": "o"}}],
           {"a.b": 1, "o": {"x.y": 2, "z": 3}}),
    lc_sim([{"sort": {"field": "n"}}, {"sort": {"field": "s", "order": "desc", "target_field": "s2"}}], {"n": [3, 1, 2], "s": ["b", "c", "a"]}),
    lc_sim([{"urldecode": {"field": "u"}}, {"html_strip": {"field": "h"}}, {"bytes": {"field": "b"}}],
           {"u": "a%20b%2Bc+d", "h": "<p>Hi <b>there</b></p>", "b": "1kb"}),
    lc_sim([{"csv": {"field": "c", "target_fields": ["x", "y", "z"]}}], {"c": "1,\"a,b\",3"}),
    lc_sim([{"foreach": {"field": "l", "processor": {"uppercase": {"field": "_ingest._value"}}}}], {"l": ["a", "b"]}),
    lc_sim([{"foreach": {"field": "l", "processor": {"uppercase": {"field": "_ingest._value"}}}}], {"l": "x"}),
    lc_sim([{"script": {"source": "ctx.b = ctx.a * params.f; ctx.remove('a')", "params": {"f": 3}}}], {"a": 2}),
    lc_sim([{"set": {"field": "y", "value": 1, "if": "ctx.a == 2"}}, {"set": {"field": "z", "value": 1, "if": "ctx.a == 1"}}], {"a": 1}),
    lc_sim([{"fail": {"message": "bad {{a}}"}}], {"a": "Q"}),
    lc_sim([{"drop": {}}], {"a": 1}),
    lc_sim([{"rename": {"field": "nope", "target_field": "y", "tag": "rt",
                        "on_failure": [{"set": {"field": "err", "value": "{{_ingest.on_failure_message}}|{{_ingest.on_failure_processor_type}}|{{_ingest.on_failure_processor_tag}}"}}]}}],
           {"a": 1}),
    lc_sim([{"rename": {"field": "nope", "target_field": "y", "ignore_failure": True}}, {"set": {"field": "ok", "value": True}}], {"a": 1}),
    lc_sim([{"set": {"field": "_index", "value": "other"}}, {"set": {"field": "_id", "value": "new-id"}}], {"a": 1}),
    ("POST", LC_PIPE + "?verbose=true", {"pipeline": {"processors": [{"set": {"field": "a", "value": 1}}, {"lowercase": {"field": "zz", "tag": "t1"}}]},
                                         "docs": [{"_source": {"b": 1}}]}, LC_STRIP),
    ("POST", LC_PIPE, {"docs": [{"_source": {"a": 1}}]}),
    ("POST", "/_ingest/pipeline/lc-nosuch/_simulate", {"docs": [{"_source": {"a": 1}}]}),
    ("POST", LC_PIPE, {"pipeline": {"processors": [{"set": {"field": "x", "value": "{{_index}}/{{_id}}"}}]},
                       "docs": [{"_index": "i", "_id": "7", "_routing": "r", "_source": {"a": 1}}]}, LC_STRIP),
])

scenario("lc_ingest_writes", lc_clean("lc-ing,lc-ing-plain,lc-ing-routed,lc-ing-b") + [
    ("DELETE", "/_ingest/pipeline/lc-*"),
    ("PUT", "/_ingest/pipeline/lc-up", {"processors": [{"uppercase": {"field": "a"}}]}),
    ("PUT", "/_ingest/pipeline/lc-final", {"processors": [{"set": {"field": "final", "value": True}}]}),
    ("PUT", "/_ingest/pipeline/lc-drop", {"processors": [{"drop": {"if": "ctx.d == true"}}]}),
    ("PUT", "/_ingest/pipeline/lc-fail", {"processors": [{"fail": {"message": "bad {{a}}"}}]}),
    ("PUT", "/_ingest/pipeline/lc-route", {"processors": [{"set": {"field": "_index", "value": "lc-ing-routed"}}]}),
    ("PUT", "/_ingest/pipeline/lc-nested", {"processors": [{"pipeline": {"name": "lc-up"}}, {"set": {"field": "n", "value": 1}}]}),
    ("PUT", "/_ingest/pipeline/lc-onf", {"processors": [{"rename": {"field": "nope", "target_field": "y"}}],
                                         "on_failure": [{"set": {"field": "err", "value": "{{_ingest.on_failure_message}}|{{_ingest.on_failure_processor_type}}|{{_ingest.on_failure_pipeline}}"}}]}),
    ("PUT", "/lc-ing", {"settings": {"default_pipeline": "lc-up", "final_pipeline": "lc-final"}}, ACK),
    ("PUT", "/lc-ing/_doc/1?refresh=true", {"a": "x"}, {"pick": lambda r: (r["result"], r["_index"])}),
    ("GET", "/lc-ing/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("PUT", "/lc-ing/_doc/2?pipeline=_none", {"a": "x"}, {"pick": lambda r: r["result"]}),
    ("GET", "/lc-ing/_doc/2", None, {"pick": lambda r: r["_source"]}),
    ("PUT", "/lc-ing/_doc/3?pipeline=lc-drop", {"d": True}),
    ("GET", "/lc-ing/_doc/3", None, {"pick": lambda r: r["found"]}),
    ("PUT", "/lc-ing/_doc/4?pipeline=lc-fail", {"a": "Q"}),
    ("PUT", "/lc-ing/_doc/5?pipeline=lc-nosuch", {"a": "Q"}),
    ("PUT", "/lc-ing/_doc/6?pipeline=lc-onf", {"a": "Q"}, {"pick": lambda r: r["result"]}),
    ("GET", "/lc-ing/_doc/6", None, {"pick": lambda r: r["_source"]}),
    ("PUT", "/lc-ing/_doc/7?pipeline=lc-nested", {"a": "q"}, {"pick": lambda r: r["result"]}),
    ("GET", "/lc-ing/_doc/7", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/lc-ing/_doc?pipeline=lc-up", {"a": "auto"}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-ing-plain/_doc/1?pipeline=lc-route", {"a": "r"}, {"pick": lambda r: (r["result"], r["_index"])}),
    ("GET", "/lc-ing-routed/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/_bulk?pipeline=lc-drop", [{"index": {"_index": "lc-ing-b", "_id": "1"}}, {"d": True},
                                          {"index": {"_index": "lc-ing-b", "_id": "2", "pipeline": "lc-fail"}}, {"a": 1},
                                          {"index": {"_index": "lc-ing-b", "_id": "3", "pipeline": "lc-nosuch"}}, {"a": 1},
                                          {"create": {"_index": "lc-ing-b", "_id": "4", "pipeline": "lc-up"}}, {"a": "c"}],
     {"pick": lambda r: (r["errors"], "ingest_took" in r, [lc_strip(list(i.values())[0]) for i in r["items"]])}),
    ("POST", "/_bulk", [{"index": {"_index": "lc-ing-b", "_id": "9"}}, {"a": 1}], {"pick": lambda r: (r["errors"], "ingest_took" in r)}),
    ("GET", "/lc-ing-b/_doc/4", None, {"pick": lambda r: r["_source"]}),
    ("DELETE", "/_ingest/pipeline/lc-up"),
    ("POST", "/_ingest/_simulate", {"docs": [{"_index": "lc-ing", "_id": "1", "_source": {"a": "y"}},
                                             {"_index": "lc-ing-other", "_id": "2", "_source": {"a": "y"}}]}),
    ("PUT", "/_ingest/pipeline/lc-up", {"processors": [{"uppercase": {"field": "a"}}]}),
    ("POST", "/_ingest/_simulate", {"docs": [{"_index": "lc-ing", "_id": "1", "_source": {"a": "y"}},
                                             {"_index": "lc-ing-other", "_id": "2", "_source": {"a": "y"}}]}),
    ("POST", "/_ingest/_simulate?pipeline=lc-onf", {"docs": [{"_index": "lc-ing-other", "_id": "1", "_source": {"a": "x"}}]}),
    ("POST", "/_ingest/_simulate?pipeline=lc-fail", {"docs": [{"_index": "lc-ing-other", "_id": "1", "_source": {"a": "x"}}]}),
    ("POST", "/_ingest/_simulate?pipeline=lc-drop", {"docs": [{"_index": "lc-ing-other", "_id": "1", "_source": {"d": True}}]}),
    ("POST", "/_ingest/_simulate?pipeline=lc-nosuch", {"docs": [{"_index": "lc-ing-other", "_id": "1", "_source": {"d": True}}]}),
    ("POST", "/_ingest/lc-ing/_simulate", {"docs": [{"_id": "1", "_source": {"a": "x"}}],
                                           "pipeline_substitutions": {"lc-up": {"processors": [{"lowercase": {"field": "a"}}]}}}),
    ("POST", "/_ingest/lc-ing/_simulate", {"docs": [{"_id": "1", "_source": {"a": "x"}}],
                                           "pipeline_substitutions": {"lc-up": {"processors": [{"nope": {}}]}}}),
    ("POST", "/_ingest/lc-ing/_simulate", {"docs": [{"_id": "1", "_source": {"a": "x"}}],
                                           "pipeline_substitutions": {"lc-unused": {"processors": [{"nope": {}}]}}}),
] + lc_clean("lc-ing,lc-ing-plain,lc-ing-routed,lc-ing-b") + [("DELETE", "/_ingest/pipeline/lc-*")])

scenario("lc_scripts", lc_clean("lc-sc") + [
    ("DELETE", "/_scripts/lc-s1"), ("DELETE", "/_scripts/lc-s2"),
    ("PUT", "/_scripts/lc-s1", {"script": {"lang": "painless", "source": "ctx._source.n += params.k"}}),
    ("PUT", "/_scripts/lc-s2", {"script": {"lang": "painless", "source": "doc['n'].value * params.f"}}),
    ("GET", "/_scripts/lc-s1"),
    ("GET", "/_scripts/lc-nosuch"),
    ("DELETE", "/_scripts/lc-nosuch"),
    ("PUT", "/_scripts/lc-s3", {"script": {"lang": "painless"}}),
    ("PUT", "/_scripts/lc-s3", {"script": {"source": "1"}}),
    ("PUT", "/_scripts/lc-s3", {"bogus": {"lang": "painless"}}),
    ("PUT", "/_scripts/lc-s3", {"script": {"lang": "python", "source": "1"}}),
    ("PUT", "/_scripts/lc-s3", {"script": {"lang": "mustache", "source": {"query": {"match": {"a": "{{q}}"}}}}}),
    ("GET", "/_scripts/lc-s3"),
    ("DELETE", "/_scripts/lc-s3"),
    ("PUT", "/lc-sc/_doc/1?refresh=true", {"n": 1}, {"pick": lambda r: r["result"]}),
    ("POST", "/lc-sc/_update/1", {"script": {"id": "lc-s1", "params": {"k": 5}}}, {"pick": lambda r: r["result"]}),
    ("GET", "/lc-sc/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("POST", "/lc-sc/_update/1", {"script": {"id": "lc-nosuch"}}),
    ("POST", "/lc-sc/_refresh", None, {"pick": lambda r: 1}),
    ("POST", "/lc-sc/_search", {"query": {"script_score": {"query": {"match_all": {}}, "script": {"id": "lc-s2", "params": {"f": 3}}}}},
     {"pick": lambda r: [h["_score"] for h in r["hits"]["hits"]]}),
    ("POST", "/lc-sc/_search", {"query": {"script_score": {"query": {"match_all": {}}, "script": {"id": "lc-nosuch"}}}}),
    ("POST", "/lc-sc/_update_by_query?refresh=true", {"script": {"id": "lc-s1", "params": {"k": 1}}}, {"pick": lambda r: r["updated"]}),
    ("GET", "/lc-sc/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("GET", "/_script_language"),
    ("GET", "/_script_context", None, {"pick": lambda r: [c["name"] for c in r["contexts"]][:5]}),
    ("DELETE", "/_scripts/lc-s1"), ("DELETE", "/_scripts/lc-s2"),
] + lc_clean("lc-sc"))

scenario("lc_reindex", lc_clean("lc-ri-src,lc-ri-dst,lc-ri-q,lc-ri-s,lc-ri-b,lc-ri-p,lc-ri-w,lc-ri-x") + [
    ("PUT", "/lc-ri-src/_doc/1", {"a": 1, "o": {"x": 1, "y": 2}}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-ri-src/_doc/2?routing=r", {"a": 2}, {"pick": lambda r: r["result"]}),
    ("PUT", "/lc-ri-src/_doc/3?refresh=true", {"a": 3}, {"pick": lambda r: r["result"]}),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-dst"}}, LC_STRIP),
    ("GET", "/lc-ri-dst/_doc/2?routing=r", None, {"pick": lambda r: (r["_source"], r.get("_routing"))}),
    ("POST", "/_reindex", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-dst", "op_type": "create"}},
     {"pick": lambda r: lc_strip({k: v for k, v in r.items() if k != "failures"})}),
    ("POST", "/_reindex", {"conflicts": "proceed", "source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-dst", "op_type": "create"}}, LC_STRIP),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-dst"}}, LC_STRIP),
    ("GET", "/lc-ri-dst/_doc/1", None, {"pick": lambda r: r["_version"]}),
    ("POST", "/_reindex", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-src"}}),
    ("POST", "/_reindex", {"source": {"index": "lc-ri-nosuch"}, "dest": {"index": "lc-ri-x"}}),
    ("POST", "/_reindex", {"source": {"index": "lc-ri-src"}}),
    ("POST", "/_reindex", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-x"}, "bogus": 1}),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src", "query": {"range": {"a": {"gte": 2}}}, "_source": ["a"]},
                                        "dest": {"index": "lc-ri-q"}, "max_docs": 1}, LC_STRIP),
    ("POST", "/lc-ri-q/_search", {"sort": ["a"]}, {"pick": lambda r: [(h["_id"], h["_source"]) for h in r["hits"]["hits"]]}),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-s"},
                                        "script": {"source": "ctx._source.b = ctx._source.a + 10; if (ctx._id == '2') { ctx.op = 'noop' }"}}, LC_STRIP),
    ("POST", "/lc-ri-s/_search", {"sort": ["a"]}, {"pick": lambda r: [(h["_id"], h["_source"]) for h in r["hits"]["hits"]]}),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src", "size": 1}, "dest": {"index": "lc-ri-b"}}, LC_STRIP),
    ("PUT", "/_ingest/pipeline/lc-ri-p", {"processors": [{"set": {"field": "piped", "value": True}}]}),
    ("POST", "/_reindex?refresh=true", {"source": {"index": "lc-ri-src", "query": {"term": {"a": 1}}}, "dest": {"index": "lc-ri-p", "pipeline": "lc-ri-p"}}, LC_STRIP),
    ("GET", "/lc-ri-p/_doc/1", None, {"pick": lambda r: r["_source"]}),
    ("DELETE", "/_ingest/pipeline/lc-ri-p"),
    ("POST", "/_reindex?wait_for_completion=false", {"source": {"index": "lc-ri-src"}, "dest": {"index": "lc-ri-w"}}, {"pick": lambda r: sorted(r)}),
    ("sleep", 2),
] + lc_clean("lc-ri-src,lc-ri-dst,lc-ri-q,lc-ri-s,lc-ri-b,lc-ri-p,lc-ri-w,lc-ri-x"))

scenario("lc_misc", lc_clean("lc-misc") + [
    ("PUT", "/lc-misc", None, ACK),
    ("POST", "/lc-misc/_forcemerge?max_num_segments=10&only_expunge_deletes=true"),
    ("POST", "/lc-misc/_forcemerge?max_num_segments=1"),
    ("POST", "/lc-misc/_forcemerge?wait_for_completion=false", None, {"pick": lambda r: sorted(r)}),
    ("POST", "/lc-misc/_flush?force=true&wait_if_ongoing=false"),
    ("POST", "/lc-misc/_flush?force=true"),
    ("GET", "/_migration/system_features", None, {"pick": lambda r: (r["migration_status"], [f["feature_name"] for f in r["features"]])}),
    ("POST", "/_migration/system_features"),
] + lc_clean("lc-misc"))
# --- search response features and document parsing (resp_*) ----------

RESULT = {"pick": lambda r: r.get("result")}


def resp_hits(r):
    return [(h["_id"], h.get("_score"), h.get("sort"), h.get("fields"), h.get("_ignored"),
             h.get("ignored_field_values")) for h in r["hits"]["hits"]] + [r["hits"].get("total")]


def resp_inner(r):
    return [(h["_id"], h.get("fields"), {n: ([x["_id"] for x in ih["hits"]["hits"]], ih["hits"]["total"])
                                         for n, ih in h.get("inner_hits", {}).items()})
            for h in r["hits"]["hits"]]


scenario("resp_docparse", [
    ("DELETE", "/resp-dp?ignore_unavailable=true"),
    ("PUT", "/resp-dp", {"settings": STATIC, "mappings": {"dynamic": "strict", "properties": {
        "ip": {"type": "ip", "ignore_malformed": True}, "n": {"type": "integer"},
        "d": {"type": "date", "ignore_malformed": True}, "k": {"type": "keyword", "ignore_above": 3},
        "b": {"type": "boolean"}, "o": {"type": "object", "dynamic": True},
        "f": {"type": "object", "dynamic": False}}}}, ACK),
    ("PUT", "/resp-dp/_doc/1", {"ip": "10.0.0.1", "n": 5, "k": "ab"}, RESULT),
    ("PUT", "/resp-dp/_doc/2", {"ip": "garbage", "d": "nope", "k": "abcdef"}, RESULT),
    ("PUT", "/resp-dp/_doc/3", {"ip": ["1.1.1.1", "bad", 7], "n": "12", "o": {"new": "x"}}, RESULT),
    ("PUT", "/resp-dp/_doc/4", {"f": {"anything": [1, "a"]}, "b": "false"}, RESULT),
    ("PUT", "/resp-dp/_doc/5", {"zz": 1}),
    ("PUT", "/resp-dp/_doc/5", {"n": "abc"}),
    ("PUT", "/resp-dp/_doc/5", {"n": 3000000000}),
    ("PUT", "/resp-dp/_doc/5", {"n": True}),
    ("PUT", "/resp-dp/_doc/5", {"b": "yes"}),
    ("PUT", "/resp-dp/_doc/5", {"ip": {"object": "wow"}}),
    ("PUT", "/resp-dp/_doc/5", {"k": {"a": 1}}),
    ("PUT", "/resp-dp/_doc/5", {"o": "concrete"}),
    ("PUT", "/resp-dp/_doc/5", {"o": {"p": {"q": 1}}, "n": "1e3"}, RESULT),
    ("POST", "/resp-dp/_refresh"),
    ("GET", "/resp-dp/_mapping"),
    ("GET", "/resp-dp/_doc/2?stored_fields=_ignored"),
    ("GET", "/resp-dp/_doc/1?stored_fields=_ignored"),
    ("POST", "/resp-dp/_search", {"query": {"exists": {"field": "_ignored"}}, "sort": ["_doc"]}, {"pick": resp_hits}),
    ("POST", "/resp-dp/_search", {"query": {"term": {"_ignored": "k"}}}, {"pick": ids}),
    ("POST", "/resp-dp/_search", {"query": {"ids": {"values": ["2", "3"]}}, "_source": False, "fields": ["ip", "k", "d"],
                                  "sort": ["_doc"]}, {"pick": resp_hits}),
    ("DELETE", "/resp-dt?ignore_unavailable=true"),
    ("PUT", "/resp-dt", {"settings": STATIC, "mappings": {"dynamic_templates": [
        {"strs": {"match_mapping_type": "string", "match": "k_*", "mapping": {"type": "keyword"}}},
        {"longs": {"match_mapping_type": "long", "mapping": {"type": "integer"}}},
        {"named": {"mapping": {"type": "keyword"}}},
        {"rt": {"match": "r_*", "runtime": {}}}]}}, ACK),
    ("PUT", "/resp-dt/_doc/1", {"k_a": "x", "n": 5, "f": 1.5, "s": "text", "d": "2020-01-01", "d2": "2015/09/02",
                                "dotted.name": "y", "r_x": "v", "e": [], "nul": None}, RESULT),
    ("GET", "/resp-dt/_mapping"),
    ("POST", "/_bulk", [{"index": {"_index": "resp-dt", "_id": "2", "dynamic_templates": {"t": "named"}}},
                        {"t": "abc"},
                        {"index": {"_index": "resp-dt", "_id": "3", "dynamic_templates": {"u": "missing"}}},
                        {"u": "abc"},
                        {"index": {"_index": "resp-dt", "_id": "4", "bogus": 1}}, {"x": 1}],
     ),
    ("POST", "/_bulk", [{"index": {"_index": "resp-dt", "_id": "2", "dynamic_templates": {"t": "named"}}},
                        {"t": "abc"},
                        {"index": {"_index": "resp-dt", "_id": "3", "dynamic_templates": {"u": "missing"}}},
                        {"u": "abc"}],
     {"pick": lambda r: [(list(i)[0], i[list(i)[0]]["status"], i[list(i)[0]].get("error", {}).get("type"))
                         for i in r["items"]]}),
    ("GET", "/resp-dt/_mapping/field/t,u"),
    ("GET", "/resp-dt/_mapping/field/k_a?include_defaults=true"),
    ("DELETE", "/resp-dp"), ("DELETE", "/resp-dt"),
])

scenario("resp_termvectors", [
    ("DELETE", "/resp-tvx?ignore_unavailable=true"),
    ("PUT", "/resp-tvx", {"settings": STATIC, "mappings": {"properties": {
        "text": {"type": "text", "term_vector": "with_positions_offsets"}, "kw": {"type": "keyword"},
        "plain": {"type": "text"}}}}, ACK),
    ("PUT", "/resp-tvx/_doc/1?refresh=true", {"text": "The quick brown fox is brown.", "kw": "Hello World",
                                              "plain": "some text here"}, RESULT),
    ("PUT", "/resp-tvx/_doc/2?refresh=true", {"text": "brown cows", "plain": "more text"}, RESULT),
    ("GET", "/resp-tvx/_termvectors/1?term_statistics=true"),
    ("GET", "/resp-tvx/_termvectors/1?fields=kw,plain&positions=false"),
    ("GET", "/resp-tvx/_termvectors/1?fields=plain&offsets=false&field_statistics=false"),
    ("GET", "/resp-tvx/_termvectors/9"),
    ("GET", "/resp-nope/_termvectors/1"),
    ("POST", "/resp-tvx/_termvectors", {"doc": {"text": "brown cow", "x": "y"}, "term_statistics": True}),
    ("POST", "/resp-tvx/_termvectors/1?bogus=1"),
    ("POST", "/resp-tvx/_termvectors/1", {"versionType": "x"}),
    ("POST", "/_mtermvectors", {"docs": [{"_index": "resp-tvx", "_id": "1", "fields": ["plain"]},
                                         {"_index": "resp-tvx", "_id": "9"},
                                         {"_index": "resp-nope", "_id": "1"}]}),
    ("POST", "/resp-tvx/_mtermvectors?fields=plain", {"ids": ["1", "2"]}),
    ("POST", "/resp-tvx/_mtermvectors", {"docs": [{"_id": "1", "_routing": "x"}]}),
    ("POST", "/resp-tvx/_mtermvectors", {}),
    ("DELETE", "/resp-tvx"),
])

COLLAPSE_DOCS = [
    {"g": 1, "tag": "A", "sort": 10, "t": "red fox"}, {"g": 1, "tag": "B", "sort": 6, "t": "red red fox"},
    {"g": 1, "tag": "A", "sort": 24, "t": "blue fox"}, {"g": 25, "tag": "B", "sort": 10, "t": "red dog"},
    {"g": 25, "tag": "A", "sort": 5, "t": "red"}, {"g": 3, "tag": "B", "sort": 36, "t": "fox"},
]

scenario("resp_collapse", setup("resp-col", {"settings": STATIC, "mappings": {"properties": {
    "g": {"type": "integer"}, "tag": {"type": "keyword"}, "sort": {"type": "long"}, "t": {"type": "text"}}}},
    COLLAPSE_DOCS) + [
    ("POST", "/resp-col/_search", {"collapse": {"field": "g", "inner_hits": {"name": "s", "size": 2, "sort": [{"sort": "asc"}]}},
                                   "sort": [{"sort": "desc"}]}, {"pick": resp_inner}),
    ("POST", "/resp-col/_search", {"query": {"match": {"t": "red"}},
                                   "collapse": {"field": "g", "inner_hits": [{"name": "a", "size": 1},
                                                                             {"name": "b", "collapse": {"field": "tag"}}]}},
     {"pick": resp_inner}),
    ("POST", "/resp-col/_search?rest_total_hits_as_int=true",
     {"collapse": {"field": "g", "inner_hits": {"name": "s", "version": True, "_source": False, "fields": ["tag"]}},
      "sort": [{"sort": "desc"}]}, {"pick": resp_inner}),
    ("POST", "/resp-col/_search", {"collapse": {"field": "g", "inner_hits": {"size": 1}}}),
    ("POST", "/resp-col/_search", {"collapse": {"field": "g", "inner_hits": {"name": "x", "collapse": {"field": "tag", "inner_hits": {}}}}}),
    ("POST", "/resp-col/_search?scroll=1m", {"collapse": {"field": "g"}}),
    ("POST", "/resp-col/_search", {"collapse": {"field": "t"}}),
    ("POST", "/resp-col/_search", {"collapse": {"field": "g"}, "search_after": [6], "sort": [{"sort": "desc"}]}),
    ("POST", "/resp-col/_search", {"query": {"match": {"t": "red"}}, "collapse": {"field": "g"},
                                   "rescore": {"window_size": 2, "query": {"rescore_query": {"match": {"t": "fox"}},
                                                                           "query_weight": 0.5, "rescore_query_weight": 2}}},
     {"pick": resp_hits}),
    ("POST", "/resp-col/_search", {"rescore": {"query": {"rescore_query": {"match_all": {}}}}, "sort": ["sort"]}),
    ("DELETE", "/resp-col"),
])

scenario("resp_limits", setup("resp-lim", {"settings": {"index": {"refresh_interval": "-1", "max_docvalue_fields_search": 2,
                                                                     "max_script_fields": 1, "max_result_window": 5,
                                                                     "max_terms_count": 2, "number_of_shards": 3}},
                                           "mappings": {"properties": {"k": {"type": "keyword"}, "n": {"type": "long"}}}},
                                [{"k": "a", "n": 1}, {"k": "b", "n": 2}, {"k": "c", "n": 3}]) + [
    ("POST", "/resp-lim/_search", {"match": {"k": "a"}}),
    ("POST", "/resp-lim/_search", {"query": {"match_all": {}}, "bogus": [1]}),
    ("POST", "/resp-lim/_search", {"query": {"term": {"k": "a"}, "preference": "_local"}}),
    ("POST", "/resp-lim/_search", {"from": -1}),
    ("POST", "/resp-lim/_search?from=-1"),
    ("POST", "/resp-lim/_search", {"size": 6}),
    ("POST", "/resp-lim/_search?scroll=1m", {"size": 6}),
    ("POST", "/resp-lim/_search", {"docvalue_fields": ["k", "n", "x"]}),
    ("POST", "/resp-lim/_search", {"script_fields": {"a": {"script": "1"}, "b": {"script": "2"}}}),
    ("POST", "/resp-lim/_search", {"query": {"terms": {"k": ["a", "b", "c"]}}}),
    ("POST", "/resp-lim/_search", {"query": {"regexp": {"k": "a" * 1001}}}),
    ("POST", "/resp-lim/_search", {"query": {"prefix": {"k": "a" * 1001}}}),
    ("POST", "/resp-lim/_search", {"rescore": [{"window_size": 10001, "query": {"rescore_query": {"match_all": {}}}}]}),
    ("POST", "/resp-lim/_search?batched_reduce_size=1"),
    ("POST", "/resp-lim/_search?pre_filter_shard_size=0"),
    ("POST", "/resp-lim/_search?batched_reduce_size=2", {"size": 0, "aggs": {"k": {"terms": {"field": "k"}}}},
     {"pick": lambda r: (r.get("num_reduce_phases"), r["hits"]["total"])}),
    ("POST", "/resp-lim/_search", {"track_total_hits": -2}),
    ("POST", "/resp-lim/_search?terminate_after=-1"),
    ("POST", "/resp-lim/_count?terminate_after=-1"),
    ("POST", "/resp-lim/_search", {"indices_boost": {"resp-lim": 2}}),
    ("POST", "/resp-lim/_search", {"indices_boost": [{"resp-nope": 2}]}),
    ("POST", "/resp-nomatch*/_search"),
    ("POST", "/resp-nomatch*/_count"),
    ("POST", "/resp-nomatch*/_search?allow_no_indices=false"),
    ("POST", "/resp-lim,resp-nope/_search"),
    ("POST", "/<resp-nope-{now/d}>/_search"),
    ("GET", "/resp-lim/_search_shards?routing=x", None, {"pick": lambda r: (r["indices"], [[(c["index"], c["shard"], c["primary"]) for c in g] for g in r["shards"]])}),
    ("GET", "/resp-lim/_search_shards", None, {"pick": lambda r: (r["indices"], [[(c["index"], c["shard"]) for c in g] for g in r["shards"]])}),
    ("DELETE", "/resp-lim"),
])

scenario("resp_fields", [
    ("DELETE", "/resp-fl?ignore_unavailable=true"),
    ("PUT", "/resp-fl", {"settings": STATIC, "mappings": {"properties": {
        "id": {"type": "keyword"}, "idAlias": {"type": "alias", "path": "_id"},
        "num": {"type": "integer"}, "numAlias": {"type": "alias", "path": "num"},
        "user": {"type": "nested", "properties": {"first": {"type": "keyword"},
                                                  "address": {"type": "nested"}}},
        "owner": {"type": "text", "fields": {"length": {"type": "token_count", "analyzer": "standard"}}},
        "ts": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
        "flat": {"type": "flattened"}, "geo": {"type": "geo_point"},
        "off": {"type": "object", "enabled": False}}}}, ACK),
    ("PUT", "/resp-fl/_doc/1?refresh=true", {"id": "x", "num": 7, "owner": "Anna Ott", "ts": "2021-02-11 08:30:04.828",
                                             "user": [{"first": "John", "address": {"city": "Berlin"}, "acct": {"size": 1}},
                                                      {"first": "Alice", "address": [{"city": "Paris"}, {"zip": "1"}]},
                                                      {"last": "Snow"}],
                                             "flat": {"a": "b", "c": ["d", "e"]}, "geo": {"lat": 41.12, "lon": -71.34},
                                             "off": {"z": "q"}}, RESULT),
    ("POST", "/resp-fl/_search", {"_source": False, "fields": ["*"]}, {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"_source": False, "fields": ["user.address*"]}, {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"_source": False, "fields": [{"field": "*", "include_unmapped": True}]}, {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"_source": False, "fields": ["_id", "_index", "_version", "_*", "flat.c", "flat.*"]},
     {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"_source": False, "fields": [{"field": "geo", "format": "wkt"}, {"field": "ts", "format": "yyyy"}]},
     {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"fields": ["_seq_no"]}),
    ("POST", "/resp-fl/_search", {"fields": [{"field": "id", "format": "yyyy"}]}),
    ("POST", "/resp-fl/_search", {"fields": [{"field": "i*", "format": "yyyy"}]}),
    ("POST", "/resp-fl/_search", {"script_fields": {"a": {"script": "doc['num'].value * 2"},
                                                    "b": {"script": {"source": "params._source.id + params.x", "params": {"x": "!"}}},
                                                    "c": {"script": "return null"}}},
     {"pick": lambda r: [(h.get("_source"), h.get("fields")) for h in r["hits"]["hits"]]}),
    ("POST", "/resp-fl/_search", {"sort": [{"ts": {"order": "asc", "format": "strict_date_optional_time_nanos"}}]},
     {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"sort": [{"ts": {"order": "asc"}}], "search_after": ["2021-02-11 08:30:04.827"]},
     {"pick": resp_hits}),
    ("POST", "/resp-fl/_search", {"sort": [{"ts": {"order": "asc", "format": "epoch_millis"}}], "search_after": ["2021-02-11T08:30:04.828Z"]}),
    ("POST", "/resp-fl/_search", {"sort": ["_shard_doc"]}),
    ("DELETE", "/resp-fl"),
])


def resp_profile(r):
    out = []
    for sh in r["profile"]["shards"]:
        f = sh.get("fetch")
        out.append((sh["shard_id"], sh["index"], sh.get("cluster"), sorted(sh), len(sh["node_id"]),
                    f and (f["type"], f["debug"], [c["type"] for c in f.get("children", [])]),
                    sh.get("dfs") and sorted(sh["dfs"]),
                    [(s["query"][0]["type"], [c["name"] for c in s["collector"]]) for s in sh.get("searches", [])]))
    return out


scenario("resp_profile", setup("resp-prof", {"settings": {"index": {"refresh_interval": "-1", "number_of_shards": 2}},
                                             "mappings": {"properties": {"k": {"type": "keyword"}, "t": {"type": "text"}}}},
                               [{"k": "a", "t": "quick fox"}, {"k": "b", "t": "lazy dog"}]) + [
    ("POST", "/resp-prof/_search", {"profile": True, "query": {"term": {"k": "a"}}}, {"pick": resp_profile}),
    ("POST", "/resp-prof/_search", {"profile": True, "_source": False, "fields": ["k"], "query": {"term": {"k": "a"}}},
     {"pick": resp_profile}),
    ("POST", "/resp-prof/_search", {"profile": True, "stored_fields": "_none_", "query": {"term": {"k": "a"}}},
     {"pick": resp_profile}),
    ("POST", "/resp-prof/_search?search_type=dfs_query_then_fetch", {"profile": True, "query": {"term": {"k": "zz"}}},
     {"pick": resp_profile}),
    ("POST", "/resp-prof/_search", {"profile": True, "query": {"bool": {"must": [{"match": {"t": "quick fox"}}],
                                                                        "filter": [{"term": {"k": "a"}}]}}},
     {"pick": lambda r: [s["searches"][0]["query"][0]["description"] for s in r["profile"]["shards"]]}),
    ("DELETE", "/resp-prof"),
])

scenario("resp_misc", setup("resp-misc", {"settings": STATIC, "mappings": {"properties": {
    "k": {"type": "keyword"}, "n": {"type": "long"}, "f": {"type": "double"}, "d": {"type": "date"}}}},
    [{"k": "a", "n": 1, "f": 1.5, "d": "2020-01-01"}, {"k": "b", "n": 2, "f": 2.5, "d": "2021-01-01"}]) + [
    ("POST", "/resp-misc/_search?typed_keys=true", {"size": 0, "aggs": {
        "t": {"terms": {"field": "k"}, "aggs": {"m": {"max": {"field": "n"}}}}, "l": {"terms": {"field": "n"}},
        "dt": {"terms": {"field": "f"}}, "a": {"avg": {"field": "n"}}, "c": {"cardinality": {"field": "k"}},
        "r": {"range": {"field": "d", "ranges": [{"to": "2020-06-01"}]}}, "f": {"filter": {"term": {"k": "a"}}},
        "h": {"histogram": {"field": "n", "interval": 1}}, "m": {"missing": {"field": "k"}}}},
     {"pick": lambda r: sorted(r["aggregations"])}),
    ("POST", "/_msearch?typed_keys=true", [{"index": "resp-misc"}, {"size": 0, "aggs": {"f": {"filter": {"term": {"k": "a"}}}}}],
     {"pick": lambda r: [sorted(x.get("aggregations", {})) for x in r["responses"]]}),
    ("POST", "/resp-misc/_pit?keep_alive=1m", None, {"save": {"pit": "id"}, "pick": lambda r: "id" in r}),
    ("POST", "/_msearch", [{"index": "resp-misc"}, {"pit": {"id": "{pit}"}}]),
    ("POST", "/_msearch", [{}, {"pit": {"id": "{pit}"}, "query": {"match": {"_index": "resp-misc"}}}],
     {"pick": lambda r: [x["hits"]["total"] for x in r["responses"]]}),
    ("DELETE", "/_pit", {"id": "{pit}"}, {"pick": lambda r: r}),
    ("POST", "/resp-misc/_search?include_named_queries_score=true",
     {"query": {"bool": {"should": [{"term": {"k": {"value": "a", "_name": "ka"}}}, {"match_all": {"_name": "all"}}]}}},
     {"pick": lambda r: [(h["_id"], sorted(h.get("matched_queries"))) for h in r["hits"]["hits"]]}),
    ("PUT", "/%3Cresp-dm-%7B2022-12-31%7C%7C%2Fd%7Byyyy-MM-dd%7D%7D%3E", None, ACK),
    ("GET", "/resp-dm-2022-12-31", None, {"pick": lambda r: sorted(r)}),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "<resp-dm-{2022-12-31||/d{yyyy-MM-dd}}>",
                                                "alias": "<resp-dma-{2022-12-31||/d{yyyy-MM-dd}}>"}}]}, ACK),
    ("GET", "/_alias/resp-dma-2022-12-31"),
    ("GET", "/resp-misc/_mget", {"docs": [{"_id": "1", "_routing": "x"}]}),
    ("POST", "/_bulk", [{"index": {"_index": "resp-misc", "_id": "9", "_type": "x"}}, {"k": "z"}]),
    ("POST", "/resp-misc/_search", {"runtime_mappings": {"loc": {"type": "lookup", "target_index": "resp-misc",
                                                                 "input_field": "k", "target_field": "k",
                                                                 "fetch_fields": ["n"]}},
                                    "fields": ["loc"], "_source": False, "sort": ["n"]}, {"pick": resp_hits}),
    ("POST", "/resp-misc/_search", {"runtime_mappings": {"loc": {"type": "lookup", "target_index": "resp-misc",
                                                                 "input_field": "k", "target_field": "k",
                                                                 "fetch_fields": ["n"]}},
                                    "query": {"match": {"loc": "x"}}}),
    ("DELETE", "/resp-misc"), ("DELETE", "/resp-dm-2022-12-31"),
])
# --- time-series indices (index.mode: time_series) and range fields -------

TSDB_TS = {"start_time": "2021-04-28T00:00:00Z", "end_time": "2021-04-29T00:00:00Z"}


def tsdb_index(name, mappings, routing_path=("metricset", "k8s.pod.uid"), **settings):
    index = {"mode": "time_series", "routing_path": list(routing_path),
             "time_series": dict(TSDB_TS), "refresh_interval": "-1"}
    index.update(settings)
    return [("DELETE", f"/{name}?ignore_unavailable=true"),
            ("PUT", f"/{name}", {"settings": {"index": index}, "mappings": mappings}, ACK)]


TSDB_MAPPING = {"properties": {
    "@timestamp": {"type": "date"},
    "metricset": {"type": "keyword", "time_series_dimension": True},
    "k8s": {"properties": {"pod": {"properties": {
        "uid": {"type": "keyword", "time_series_dimension": True},
        "name": {"type": "keyword"},
        "ip": {"type": "ip", "time_series_dimension": True},
        "network": {"properties": {"tx": {"type": "long", "time_series_metric": "counter"},
                                   "rx": {"type": "long", "time_series_metric": "gauge"}}}}}}}}}

TSDB_DOCS = []
for _ts, _name, _uid, _ip, _tx in [
        ("2021-04-28T18:50:04.467Z", "cat", "947e4ced-1786-4e53-9e0c-5c447e959507", "10.10.55.1", 2001818691),
        ("2021-04-28T18:50:24.467Z", "cat", "947e4ced-1786-4e53-9e0c-5c447e959507", "10.10.55.1", 2005177954),
        ("2021-04-28T18:51:04.467Z", "cat", "947e4ced-1786-4e53-9e0c-5c447e959507", "10.10.55.2", 2012916202),
        ("2021-04-28T18:50:03.142Z", "dog", "df3145b3-0563-4d3b-a0f7-897eb2876ea9", "10.10.55.3", 1434521831),
        ("2021-04-28T18:50:53.142Z", "dog", "df3145b3-0563-4d3b-a0f7-897eb2876ea9", "::ffff:10.10.55.3", 1434587694)]:
    TSDB_DOCS += [{"index": {}}, {"@timestamp": _ts, "metricset": "pod", "k8s": {"pod": {
        "name": _name, "uid": _uid, "ip": _ip, "network": {"tx": _tx, "rx": 802133794}}}}]


def items(r):
    return [(k, v["status"], v.get("_id"), v.get("result"), v.get("error", {}).get("type"))
            for i in r["items"] for k, v in i.items()]


def hit_meta(r):
    return [(h["_id"], h.get("sort"), h.get("fields")) for h in r["hits"]["hits"]]


scenario("tsdb_ids", tsdb_index("tsdb-ids", TSDB_MAPPING) + [
    ("POST", "/tsdb-ids/_bulk?refresh=true", TSDB_DOCS, {"pick": items}),
    # The same series and timestamp again: the same _id, overwritten.
    ("POST", "/tsdb-ids/_bulk?refresh=true", TSDB_DOCS[:2], {"pick": items}),
    ("POST", "/tsdb-ids/_search", {"sort": ["_tsid", "@timestamp"], "fields": ["_tsid", "_ts_routing_hash"],
                                   "_source": False}, {"pick": hit_meta}),
    ("POST", "/tsdb-ids/_search", {"size": 0, "aggs": {"t": {"terms": {"field": "_tsid", "order": {"_key": "desc"}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", "/tsdb-ids/_search", {"size": 0, "aggs": {"t": {"terms": {"field": "k8s.pod.ip", "order": {"_key": "asc"}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("POST", "/tsdb-ids/_search", {"query": {"ids": {"values": ["cZZNs7B9sSWsyrL5AAABeRnRGTM"]}}, "_source": False},
     {"pick": hit_meta}),
    ("GET", "/tsdb-ids/_doc/cZZNs7B9sSWsyrL5AAABeRnRGTM", None, {"pick": lambda r: (r["_id"], r["found"])}),
    ("GET", "/tsdb-ids/_doc/cZZNs7B9sSWsyrL5AAABeRnRGTM?routing=x"),
    ("GET", "/tsdb-ids/_doc/nope"),
    ("DELETE", "/tsdb-ids/_doc/nope"),
    ("POST", "/tsdb-ids/_mget", {"ids": ["cZZNs7B9sSWsyrL5AAABeRnRGTM", "nope"]},
     {"pick": lambda r: [(d["_id"], d.get("found"), d.get("error", {}).get("type")) for d in r["docs"]]}),
    # The index API without an id creates: the same document again conflicts.
    ("POST", "/tsdb-ids/_doc", TSDB_DOCS[1]),
    ("POST", "/tsdb-ids/_doc?op_type=index", TSDB_DOCS[1], {"pick": lambda r: (r["_id"], r["result"], r["_version"])}),
    ("PUT", "/tsdb-ids/_doc/abc", TSDB_DOCS[1]),
    ("PUT", "/tsdb-ids/_doc/cZZNs7B9sSWsyrL5AAABeRnRGTM", TSDB_DOCS[1], {"pick": lambda r: (r["_id"], r["result"])}),
    ("POST", "/tsdb-ids/_bulk", [{"create": {}}, TSDB_DOCS[1], {"delete": {"_id": "cZZNs7B9sSWsyrL5AAABeRnRGTM"}},
                                 {"delete": {"_id": "bad id"}}], {"pick": items}),
    ("DELETE", "/tsdb-ids"),
])

scenario("tsdb_errors", tsdb_index("tsdb-err", TSDB_MAPPING) + [
    ("POST", "/tsdb-err/_doc", {"metricset": "pod", "k8s": {"pod": {"uid": "u"}}}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-28T01:00:00Z"}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-27T23:59:59.999Z", "metricset": "pod"}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-29T00:00:00Z", "metricset": "pod"}),
    ("POST", "/tsdb-err/_doc?routing=r", {"@timestamp": "2021-04-28T01:00:00Z", "metricset": "pod"}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-28T01:00:00Z", "metricset": ["a", "b"]}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-28T01:00:00Z", "metricset": True}),
    ("POST", "/tsdb-err/_bulk", [{"index": {}}, {"@timestamp": "2021-04-28T01:00:00Z", "metricset": "a", "k8s": {"pod": {"ip": ["1.1.1.1", "2.2.2.2"]}}},
                                 {"index": {"routing": "r"}}, {"@timestamp": "2021-04-28T01:00:00Z", "metricset": "a"},
                                 {"update": {"_id": "x"}}, {"doc": {}},
                                 {"index": {}}, {"@timestamp": "2021-04-28T01:00:00Z", "metricset": "a", "unmapped": "x"}],
     {"pick": items}),
    ("POST", "/tsdb-err/_update/x", {"doc": {}}),
    ("POST", "/tsdb-err/_search?routing=x"),
    ("POST", "/tsdb-err/_search", {"sort": ["_id"]}),
    ("POST", "/tsdb-err/_search", {"aggs": {"i": {"terms": {"field": "_id"}}}}),
    ("POST", "/tsdb-err/_search", {"query": {"term": {"_tsid": "x"}}}),
    ("POST", "/tsdb-err/_search", {"size": 0, "aggs": {"f": {"filter": {"term": {"_tsid": "x"}}}}}),
    ("POST", "/tsdb-err/_search", {"runtime_mappings": {"metricset": {"type": "keyword"}}}),
    ("POST", "/tsdb-err/_alias/tsdb-err-a", {"routing": "x"}),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "tsdb-err", "alias": "tsdb-err-b", "search_routing": "x"}}]}),
    ("PUT", "/tsdb-err/_mapping", {"properties": {"k8s": {"properties": {"pod": {"properties": {"uid": {"type": "keyword"}}}}}}}),
    ("PUT", "/tsdb-err/_mapping", {"properties": {"metricset2": {"type": "keyword"}}}, ACK),
    ("PUT", "/tsdb-err/_mapping", {"properties": {"n": {"type": "nested"}}}),
    ("PUT", "/tsdb-err/_settings", {"index": {"time_series": {"end_time": "2021-04-28T12:00:00Z"}}}),
    ("PUT", "/tsdb-err/_settings", {"index": {"time_series": {"end_time": "2021-04-30T00:00:00Z"}}}, ACK),
    ("PUT", "/tsdb-err/_settings", {"index": {"time_series": {"start_time": "2021-04-27T00:00:00Z"}}}),
    ("PUT", "/tsdb-err/_settings", {"index": {"routing_path": ["x"]}}),
    ("POST", "/tsdb-err/_doc", {"@timestamp": "2021-04-29T12:00:00Z", "metricset": "late"},
     {"pick": lambda r: (r["_id"], r["result"])}),
    ("DELETE", "/tsdb-err"),
])

TS_BAD = {"mode": "time_series", "routing_path": ["dim"], "time_series": dict(TSDB_TS)}
DIM = {"dim": {"type": "keyword", "time_series_dimension": True}}


def tsdb_bad_create(settings=None, mappings=None):
    body = {"settings": {"index": settings if settings is not None else TS_BAD}}
    if mappings is not None:
        body["mappings"] = mappings
    return [("PUT", "/tsdb-bad", body), ("DELETE", "/tsdb-bad?ignore_unavailable=true")]


scenario("tsdb_settings", [("DELETE", "/tsdb-bad?ignore_unavailable=true")]
         + tsdb_bad_create({**TS_BAD, "sort.field": ["a"]})
         + tsdb_bad_create({**TS_BAD, "sort.order": ["desc"]})
         + tsdb_bad_create({**TS_BAD, "routing_partition_size": 2, "number_of_shards": 4})
         + tsdb_bad_create({"mode": "time_series", "time_series": dict(TSDB_TS)})
         + tsdb_bad_create({"mode": "time_series", "routing_path": [], "time_series": dict(TSDB_TS)})
         + tsdb_bad_create({"mode": "time_series", "routing_path": [], "time_series": {"start_time": "", "end_time": ""}})
         + tsdb_bad_create({"routing_path": ["dim"]})
         + tsdb_bad_create({"time_series": {"start_time": "2021-04-28T00:00:00Z"}})
         + tsdb_bad_create({"mode": "bogus"})
         + tsdb_bad_create(TS_BAD, {"_routing": {"required": True}, "properties": DIM})
         + tsdb_bad_create(TS_BAD, {"properties": {"dim": {"type": "keyword"}}})
         + tsdb_bad_create(TS_BAD, {"properties": {"dim": {"properties": {"a": {"type": "keyword", "time_series_dimension": True}}}}})
         + tsdb_bad_create(TS_BAD, {"properties": {**DIM, "@timestamp": {"type": "long"}}})
         + tsdb_bad_create(TS_BAD, {"properties": {**DIM, "n": {"type": "nested"}}})
         + tsdb_bad_create(TS_BAD, {"_source": {"mode": "stored"}, "properties": DIM})
         + tsdb_bad_create(TS_BAD, {"_source": {"includes": ["a"]}, "properties": DIM})
         + tsdb_bad_create(TS_BAD, {"_data_stream_timestamp": "x", "properties": DIM})
         + tsdb_bad_create(TS_BAD, {"runtime": {"c": {"type": "long", "time_series_metric": "counter"}}, "properties": DIM})
         + tsdb_bad_create(TS_BAD, {"runtime": {"@timestamp": {"type": "date"}}, "properties": DIM})
         + tsdb_bad_create({}, {"properties": {"n": {"type": "nested", "properties": DIM}}})
         + tsdb_bad_create({}, {"properties": DIM, "runtime": {"dim": {"type": "keyword"}}})
         + [("PUT", "/tsdb-bad", {"settings": {"index": TS_BAD}, "mappings": {"properties": DIM}}, ACK),
            ("GET", "/tsdb-bad/_mapping"),
            ("GET", "/tsdb-bad/_settings", None, {"pick": lambda r: {k: v for k, v in r["tsdb-bad"]["settings"]["index"].items()
                                                                       if k in ("mode", "routing_path", "time_series")}}),
            ("GET", "/tsdb-bad/_field_caps?fields=dim,_tsid,_ts_routing_hash,@timestamp"),
            ("DELETE", "/tsdb-bad")])

scenario("tsdb_dims", tsdb_index("tsdb-dims", {"dynamic_templates": [
    {"kw": {"match_mapping_type": "string", "mapping": {"type": "keyword", "time_series_dimension": True}}}],
    "properties": {"@timestamp": {"type": "date"}, "n": {"type": "long", "time_series_dimension": True},
                   "u": {"type": "unsigned_long", "time_series_dimension": True},
                   "ip": {"type": "ip", "time_series_dimension": True},
                   "flat": {"type": "flattened", "time_series_dimensions": ["a.b", "c"]},
                   "v": {"type": "double", "time_series_metric": "gauge"}}}, routing_path=["k"]) + [
    ("POST", "/tsdb-dims/_bulk?refresh=true", [
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:00Z", "k": "a", "n": 1, "v": 1.5},
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:01Z", "k": "a", "n": "1", "v": 2.5},
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:02Z", "k": "a", "n": 1.9, "u": 18446744073709551615},
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:03Z", "k": "b", "ip": "2001:0db8:0:0:0:0:0:1", "other": "x"},
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:04Z", "k": "b", "flat": {"a": {"b": "x", "z": "y"}, "c": 7}},
        {"index": {}}, {"@timestamp": "2021-04-28T01:00:05Z", "k": "c", "n": "abc"},
    ], {"pick": items}),
    ("POST", "/tsdb-dims/_search", {"size": 0, "aggs": {"t": {"terms": {"field": "_tsid", "order": {"_key": "asc"}},
                                                             "aggs": {"v": {"sum": {"field": "v"}}}}}},
     {"pick": lambda r: r["aggregations"]}),
    ("GET", "/tsdb-dims/_mapping"),
    ("DELETE", "/tsdb-dims"),
])

scenario("tsdb_nanos", tsdb_index("tsdb-nanos", {"properties": {"@timestamp": {"type": "date_nanos"},
                                                               "k": {"type": "keyword", "time_series_dimension": True}}},
                                  routing_path=["k"], time_series={"start_time": "2021-09-26T03:09:42Z",
                                                                   "end_time": "2021-09-26T03:09:52Z"}) + [
    ("POST", "/tsdb-nanos/_doc?refresh=true", {"@timestamp": "2021-09-26T03:09:42.123456789Z", "k": "a"},
     {"pick": lambda r: (r["_id"], r["result"])}),
    ("POST", "/tsdb-nanos/_doc", {"@timestamp": "2021-09-26T03:09:41.123456789Z", "k": "a"}),
    ("POST", "/tsdb-nanos/_doc", {"@timestamp": "2021-09-26T03:09:52.000000001Z", "k": "a"}),
    ("POST", "/tsdb-nanos/_search", {"docvalue_fields": ["@timestamp"], "sort": ["@timestamp"], "_source": False},
     {"pick": hit_meta}),
    ("DELETE", "/tsdb-nanos"),
    ("DELETE", "/rng-nanos?ignore_unavailable=true"),
    ("PUT", "/rng-nanos", {"mappings": {"properties": {"d": {"type": "date_nanos"}}}}, ACK),
    ("PUT", "/rng-nanos/_doc/1", {"d": "1969-12-31T23:59:59Z"}),
    ("PUT", "/rng-nanos/_doc/2", {"d": "2263-01-01T00:00:00Z"}),
    ("PUT", "/rng-nanos/_doc/3?refresh=true", {"d": "2018-10-29T12:12:12.987654321Z"}, {"pick": lambda r: r["result"]}),
    ("PUT", "/rng-nanos/_doc/4?refresh=true", {"d": "2018-10-29T12:12:12.123456789Z"}, {"pick": lambda r: r["result"]}),
    ("POST", "/rng-nanos/_search", {"sort": [{"d": "desc"}], "_source": False}, {"pick": hit_meta}),
    ("DELETE", "/rng-nanos"),
])

RNG_MAPPING = {"settings": STATIC, "mappings": {"properties": {
    t: {"type": t} for t in ["integer_range", "long_range", "float_range", "double_range", "date_range", "ip_range"]}}}

scenario("range_fields", setup("rng-fields", RNG_MAPPING, [
    {"integer_range": {"gte": 1, "lte": 5}, "long_range": {"gt": None, "lte": 5}, "float_range": {"gte": 1.5, "lt": 3},
     "double_range": {"gt": 1, "lte": 5}, "date_range": {"gte": "2017-09-01", "lte": "2017-09-05"},
     "ip_range": "192.168.0.0/24"},
    {"integer_range": {"gte": 1, "lte": 3}, "long_range": {"gte": 10}, "float_range": {"gte": 3, "lte": 4},
     "double_range": {"gte": 4, "lte": 5}, "date_range": {"gte": "2017-09-01", "lte": "2017-09-03"},
     "ip_range": {"gte": "192.168.0.1", "lte": "192.168.0.3"}},
    {"integer_range": {"gte": 4, "lte": 5}, "long_range": {"gte": None, "lt": None},
     "date_range": {"gte": "2019-12-15T12:00:00.000Z", "lte": "2019-12-15T13:00:00.000Z"},
     "ip_range": {"gte": "10.0.0.1", "lte": "10.0.0.5"}},
    {"integer_range": None},
]) + [
    ("POST", "/rng-fields/_search", {"query": {"range": {f: dict(q, relation=rel)}}, "_source": False}, {"pick": ids})
    for f, q in [("integer_range", {"gte": 3, "lte": 4}), ("long_range", {"gte": 5, "lte": 10}),
                 ("float_range", {"gt": 3, "lt": 3.5}), ("double_range", {"gte": 1, "lt": 4}),
                 ("date_range", {"gte": "2017-09-03", "lte": "2017-09-04"}),
                 ("date_range", {"gt": "2019-12-15||/d"}), ("date_range", {"lte": "2019-12-15||/d"}),
                 ("ip_range", {"gte": "192.168.0.2", "lte": "192.168.0.3"})]
    for rel in ["intersects", "contains", "within"]
] + [
    ("POST", "/rng-fields/_search", {"query": {"term": {"long_range": -9223372036854775808}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"term": {"long_range": 9223372036854775806}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"term": {"ip_range": "192.168.0.200"}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"terms": {"integer_range": [0, 5]}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"query_string": {"query": "integer_range:[2 TO 3]"}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"range": {"integer_range": {"gte": 3, "relation": "disjoint"}}}}),
    ("POST", "/rng-fields/_search", {"query": {"range": {"integer_range": {"gte": 3, "relation": "foo"}}}}),
    ("PUT", "/rng-fields/_doc/7?refresh=true", {"ip_range": {"gte": None, "lte": "10.10.10.10"}}, {"pick": lambda r: r["result"]}),
    ("PUT", "/rng-fields/_doc/8?refresh=true", {"ip_range": {"gt": "2001:db8::", "lt": "200a:100::"}}, {"pick": lambda r: r["result"]}),
    ("POST", "/rng-fields/_search", {"query": {"term": {"ip_range": "10.0.0.0"}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-fields/_search", {"query": {"term": {"ip_range": "2001:db9::1"}}, "_source": False}, {"pick": ids}),
    ("PUT", "/rng-fields/_doc/9", {"integer_range": {"gte": 5, "lte": 1}}),
    ("PUT", "/rng-fields/_doc/9", {"integer_range": {"gte": "a"}}),
    ("PUT", "/rng-fields/_doc/9", {"integer_range": 5}),
    ("DELETE", "/rng-fields"),
])

scenario("date_rounding", setup("rng-dates", {"settings": STATIC, "mappings": {"properties": {"d": {"type": "date"},
                                                                                         "y": {"type": "date", "format": "uuuu"}}}}, [
    {"d": "1970-01-01T00:00:01Z"}, {"d": "1500-01-01T12:00:00Z"}, {"d": "2017-09-04T10:00:00Z"},
    {"d": "2017-09-04T23:59:59.999Z", "y": "2017"}]) + [
    ("POST", "/rng-dates/_search", {"query": {"range": {"d": q}}, "_source": False, "sort": ["_doc"]}, {"pick": ids})
    for q in [{"gte": 1000, "lte": 2023}, {"gte": "0", "lte": "1000"}, {"lte": "2017-09-04"}, {"lt": "2017-09-04"},
              {"gt": "2017-09-04T09"}, {"lte": "2017-09"}, {"gt": "2017-09-04T23:59:59"},
              {"gte": 1500, "lte": 1500, "format": "uuuu"}, {"gte": 1000, "lte": 2017, "format": "uuuu"}]
] + [
    ("POST", "/rng-dates/_search", {"query": {"term": {"d": "2017-09-04T10:00:00Z"}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-dates/_search", {"query": {"term": {"d": "2017-09-04"}}, "_source": False, "sort": ["_doc"]}, {"pick": ids}),
    ("POST", "/rng-dates/_search", {"query": {"match": {"d": "1500-01-01T12:00:00.000Z"}}, "_source": False}, {"pick": ids}),
    ("POST", "/rng-dates/_search", {"query": {"query_string": {"query": "d:\"2017-09-04T10:00:00Z\""}}, "_source": False}, {"pick": ids}),
    ("DELETE", "/rng-dates"),
])

scenario("dynamic_settings", [
    ("DELETE", "/rng-dyn?ignore_unavailable=true"),
    ("PUT", "/rng-dyn", {"settings": STATIC, "mappings": {
        "dynamic_templates": [{"kw": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}},
                              {"ints": {"match": "i_*", "mapping": {"type": "integer"}}},
                              {"x": {"match": "x_*", "match_mapping_type": "*",
                                     "mapping": {"type": "{dynamic_type}", "meta": {"n": "{name}"}}}}],
        "properties": {"off": {"type": "object", "dynamic": "false"}, "strict": {"type": "object", "dynamic": "strict"},
                       "rt": {"type": "object", "dynamic": "runtime"}}}}, ACK),
    ("PUT", "/rng-dyn/_doc/1", {"s": "x", "i_a": 5, "x_b": "2021-01-01", "x_c": 1.5, "x_d": "str", "dd": "2021-01-01",
                                "off": {"q": 1}, "rt": {"k": "v", "n": 1}, "o": {"p": {"q": True}}},
     {"pick": lambda r: r["result"]}),
    ("GET", "/rng-dyn/_mapping"),
    ("PUT", "/rng-dyn/_doc/2", {"strict": {"new": 1}}),
    ("DELETE", "/rng-dyn"),
])
# --- stats / cluster / node / cat APIs (indices prefixed `st-`) -----------

ST_SETTINGS = {"settings": {"index": {"number_of_shards": 2, "number_of_replicas": 0, "refresh_interval": "-1"}},
               "mappings": {"properties": {"name": {"type": "keyword"}, "body": {"type": "text", "fielddata": True},
                                           "sug": {"type": "completion"}, "n": {"type": "long"}}}}
ST_DOCS = [{"index": {"_index": "st-a", "_id": str(i)}} if j == 0 else
           {"name": f"n{i}", "body": f"word{i} common", "sug": f"sug{i}", "n": i}
           for i in range(1, 6) for j in range(2)]


def section_keys(r):
    return {k: sorted(v) if isinstance(v, dict) else v for k, v in r["_all"]["total"].items()}


def st_counts(r):
    t = r["indices"]["st-a"]["total"]
    return {"docs": t["docs"]["count"], "index_total": t["indexing"]["index_total"],
            "delete_total": t["indexing"]["delete_total"], "noop": t["indexing"]["noop_update_total"],
            "get": [t["get"]["total"], t["get"]["exists_total"], t["get"]["missing_total"]],
            "query_total": t["search"]["query_total"], "suggest_total": t["search"]["suggest_total"],
            "segments": t["segments"]["count"], "translog_ops": t["translog"]["operations"],
            "shard_stats": t["shard_stats"], "health": r["indices"]["st-a"]["health"],
            "status": r["indices"]["st-a"]["status"]}


def keys_of(path):
    def pick(r):
        for k in path:
            r = r[k]
        return sorted(r) if isinstance(r, dict) else r
    return pick


def node_entry(r):
    return next(iter(r["nodes"].values()))


def shape(v):
    """A response's structure: keys kept, values reduced to their type."""
    if isinstance(v, dict):
        return {k: shape(x) for k, x in v.items()}
    if isinstance(v, list):
        return [shape(v[0])] if v else []
    return type(v).__name__


scenario("stats_indices", [
    ("DELETE", "/st-a?ignore_unavailable=true"),
    ("PUT", "/st-a", ST_SETTINGS, {"pick": lambda r: r["acknowledged"]}),
    ("GET", "/st-a/_stats", None, {"pick": st_counts}),
    ("POST", "/_bulk?refresh=true", ST_DOCS, {"pick": lambda r: r["errors"]}),
    ("GET", "/st-a/_doc/1", None, {"pick": lambda r: r["found"]}),
    ("GET", "/st-a/_doc/nope"),
    ("POST", "/st-a/_update/2", {"doc": {"n": 2}}, {"pick": lambda r: r["result"]}),
    ("POST", "/st-a/_update/2", {"doc": {"n": 3}}, {"pick": lambda r: r["result"]}),
    ("DELETE", "/st-a/_doc/5", None, {"pick": lambda r: r["result"]}),
    ("POST", "/st-a/_search", {"query": {"match_all": {}}}, {"pick": lambda r: r["hits"]["total"]}),
    ("POST", "/st-a/_search", {"suggest": {"s": {"prefix": "sug", "completion": {"field": "sug"}}}},
     {"pick": lambda r: len(r["suggest"]["s"][0]["options"])}),
    ("GET", "/st-a/_stats", None, {"pick": st_counts}),
    ("GET", "/st-a/_stats", None, {"pick": section_keys}),
    ("GET", "/st-a/_stats?level=shards", None, {"pick": lambda r: shape(r["indices"]["st-a"]["shards"]["0"][0])}),
    ("GET", "/st-a/_stats?human", None, {"pick": lambda r: shape(r["_all"])}),
    ("GET", "/st-a/_stats/docs,store,merge", None, {"pick": section_keys}),
    ("GET", "/st-a/_stats?level=shards", None, {"pick": lambda r: sorted(r["indices"]["st-a"]["shards"]["0"][0])}),
    ("GET", "/st-a/_stats?level=cluster", None, {"pick": lambda r: sorted(r)}),
    ("GET", "/st-a/_stats/fieldata"),
    ("GET", "/st-a/_stats?level=bad"),
    ("GET", "/st-zz/_stats"),
    ("GET", "/st-zz*/_stats", None, {"pick": lambda r: r["_all"]}),
    ("POST", "/st-a/_search", {"sort": ["body"], "size": 1}, {"pick": lambda r: r["hits"]["hits"][0]["sort"]}),
    ("GET", "/st-a/_stats/fielddata?fielddata_fields=body", None,
     {"pick": lambda r: sorted(r["_all"]["total"]["fielddata"]["fields"])}),
    ("GET", "/st-a/_stats/completion?completion_fields=*", None,
     {"pick": lambda r: sorted(r["_all"]["total"]["completion"]["fields"])}),
    ("POST", "/st-a/_search", {"stats": ["g1"], "size": 0}, {"pick": lambda r: r["hits"]["total"]}),
    ("GET", "/st-a/_stats/search?groups=g*", None,
     {"pick": lambda r: r["_all"]["total"]["search"]["groups"]["g1"]["query_total"]}),
    ("POST", "/st-a/_flush", None, {"pick": lambda r: r["_shards"]["failed"]}),
    ("GET", "/st-a/_stats/translog", None, {"pick": lambda r: r["_all"]["total"]["translog"]["operations"]}),
    ("PUT", "/st-b", {"settings": {"index.translog.retention.size": "1mb"}}),
    ("DELETE", "/st-a"),
])

scenario("stats_cat", [
    ("DELETE", "/st-a?ignore_unavailable=true"),
    ("PUT", "/st-a", ST_SETTINGS, {"pick": lambda r: r["acknowledged"]}),
    ("POST", "/_bulk?refresh=true", ST_DOCS, {"pick": lambda r: r["errors"]}),
    ("GET", "/_cat/indices/st-a?h=health,status,index,pri,rep,docs.count,docs.deleted&v"),
    ("GET", "/_cat/indices/st-a?h=i,dc,p&format=json"),
    ("GET", "/_cat/indices/st-a?health=bogus"),
    ("GET", "/_cat/indices/st-zz"),
    ("GET", "/_cat/count/st-a?h=count"),
    ("GET", "/_cat/shards/st-a?h=index,shard,prirep,state,docs&s=shard&v"),
    ("GET", "/_cat/segments/st-a?h=index,shard,prirep,segment,generation,docs.count,docs.deleted&s=shard&format=json"),
    ("GET", "/_cat/recovery/st-a?h=index,shard,type,stage,files,translog_ops_percent&v"),
    ("GET", "/_cat/thread_pool/write,generic?h=name,type,queue_size,core,max,keep_alive&v"),
    ("GET", "/_cat/nodes?h=node.role,master&v"),
    ("GET", "/_cat/indices/st-a?help"),
    ("GET", "/_cat/shards?help"),
    ("GET", "/_cat/nodes?help"),
    ("GET", "/_cat/segments?help"),
    ("GET", "/_cat/recovery?help"),
    ("GET", "/_cat/allocation?help"),
    ("GET", "/_cat/health?help"),
    ("GET", "/_cat/thread_pool?help"),
    ("GET", "/_cat/tasks?help"),
    ("GET", "/_cat/fielddata?help"),
    ("GET", "/_cat/nodeattrs?help"),
    ("GET", "/_cat/plugins?v"),
    ("POST", "/st-a/_close", None, {"pick": lambda r: r["acknowledged"]}),
    ("GET", "/_cat/indices/st-a?h=health,status,index,pri,rep,docs.count&format=json"),
    ("GET", "/_cat/segments/st-a"),
    ("GET", "/_cat/recovery/st-a?h=index,shard,type,stage&format=json"),
    ("DELETE", "/st-a"),
])

scenario("stats_cluster", [
    ("DELETE", "/st-a?ignore_unavailable=true"),
    ("PUT", "/st-a", ST_SETTINGS, {"pick": lambda r: r["acknowledged"]}),
    ("GET", "/_cluster/health/st-a?level=shards", None,
     {"pick": lambda r: {k: v for k, v in r.items()
                         if k not in ("cluster_name", "number_of_pending_tasks", "task_max_waiting_in_queue_millis")}}),
    ("GET", "/_cluster/health/st-a?wait_for_status=green&timeout=1s", None, {"pick": lambda r: r["status"]}),
    ("GET", "/_cluster/health/st-a?level=bogus"),
    ("GET", "/_cluster/state/metadata,routing_table/st-a", None,
     {"pick": lambda r: [sorted(r), sorted(r["metadata"]["indices"]["st-a"]),
                         r["metadata"]["indices"]["st-a"]["state"],
                         sorted(r["routing_table"]["indices"]["st-a"]["shards"])]}),
    ("GET", "/_cluster/state/master_node,version", None, {"pick": sorted}),
    ("GET", "/_cluster/state/bogus", None, {"pick": sorted}),
    ("GET", "/_cluster/state/metadata/st-nothere*?allow_no_indices=false"),
    ("GET", "/_cluster/state/metadata/st-foobla?ignore_unavailable=false"),
    ("GET", "/_cluster/allocation/explain", {"index": "st-a", "shard": 0, "primary": True},
     {"pick": lambda r: [sorted(r), r["current_state"], sorted(r["current_node"])]}),
    ("GET", "/_cluster/stats", None, {"pick": lambda r: [sorted(r), sorted(r["indices"]), sorted(r["nodes"]),
                                                         sorted(r["nodes"]["count"])]}),
    ("GET", "/_nodes/_all/_none", None, {"pick": lambda r: sorted(node_entry(r))}),
    ("GET", "/_nodes/stats/indices/docs", None, {"pick": lambda r: sorted(node_entry(r)["indices"])}),
    ("GET", "/_nodes/stats/os,jvm", None, {"pick": lambda r: sorted(node_entry(r))}),
    ("GET", "/_nodes/stats/indices", None, {"pick": lambda r: shape(node_entry(r)["indices"])}),
    ("GET", "/_nodes/stats/discovery,indexing_pressure,transport,thread_pool,script,script_cache,breaker", None,
     {"pick": lambda r: {k: sorted(v) if isinstance(v, dict) else v for k, v in node_entry(r).items()
                         if k in ("discovery", "indexing_pressure", "transport", "script", "script_cache")}}),
    ("GET", "/_nodes/stats/indices/mappings?level=indices", None,
     {"pick": lambda r: node_entry(r)["indices"]["indices"]["st-a"]}),
    ("GET", "/_nodes/stats/transprot"),
    ("GET", "/_nodes/stats/os/docs"),
    ("GET", "/_nodes/hot_threads?type=bogus"),
    ("GET", "/_nodes/settings", None, {"pick": lambda r: sorted(node_entry(r))}),
    ("GET", "/_info/_all,ingest"),
    ("GET", "/_info/ingest,bogus"),
    ("GET", "/_info/script", None, {"pick": lambda r: sorted(r["script"])}),
    ("GET", "/_health_report/master_is_stable?verbose=false", None,
     {"pick": lambda r: [sorted(r), r["indicators"]["master_is_stable"]["status"]]}),
    ("GET", "/_health_report/bogus"),
    ("GET", "/_tasks/foo:1"),
    ("GET", "/_tasks/bogus"),
    ("POST", "/_tasks/_cancel?actions=unknown_action"),
    ("GET", "/_tasks?actions=cluster:monitor/tasks/lists&group_by=none", None,
     {"pick": lambda r: [t["action"] for t in r["tasks"]]}),
    ("GET", "/_capabilities?method=GET&path=/_capabilities&parameters=method,path", None, {"pick": lambda r: r["supported"]}),
    ("GET", "/_capabilities?method=GET&path=/_capabilities&parameters=bogus", None, {"pick": lambda r: r["supported"]}),
    ("GET", "/_capabilities?method=PUT&path=/%7Bindex%7D&capabilities=logsdb_index_mode", None, {"pick": lambda r: r["supported"]}),
    ("GET", "/_features"),
    ("GET", "/_remote/info"),
    ("POST", "/_cluster/voting_config_exclusions"),
    ("POST", "/_internal/prevalidate_node_removal"),
    ("POST", "/_internal/prevalidate_node_removal?names=st-nonode"),
    ("PUT", "/_internal/desired_nodes/st-h/1?dry_run=true",
     {"nodes": [{"settings": {"node.name": "x"}, "processors": 8, "memory": "1gb", "storage": "1gb"}]}),
    ("PUT", "/_internal/desired_nodes/st-h/1?dry_run=true",
     {"nodes": [{"settings": {}, "processors": 8, "memory": "1gb", "storage": "1gb"}]}),
    ("PUT", "/_internal/desired_nodes/st-h/1?dry_run=true", {"nodes": []}),
    ("PUT", "/_internal/desired_nodes/st-h/asa?dry_run=true",
     {"nodes": [{"settings": {"node.name": "x"}, "processors": 8, "memory": "1gb", "storage": "1gb"}]}),
    ("DELETE", "/st-a"),
])

scenario("stats_shards", [
    ("DELETE", "/st-a?ignore_unavailable=true"),
    ("PUT", "/st-a", ST_SETTINGS, {"pick": lambda r: r["acknowledged"]}),
    ("POST", "/_bulk?refresh=true", ST_DOCS, {"pick": lambda r: r["errors"]}),
    ("GET", "/st-a/_segments", None,
     {"pick": lambda r: {s: [c["segments"]["_0"]["num_docs"] for c in copies]
                         for s, copies in r["indices"]["st-a"]["shards"].items()}}),
    ("GET", "/st-a/_recovery", None,
     {"pick": lambda r: [(s["id"], s["type"], s["stage"], s["primary"]) for s in r["st-a"]["shards"]]}),
    ("GET", "/st-a/_recovery?human&detailed=true", None,
     {"pick": lambda r: sorted(r["st-a"]["shards"][0]["index"]["files"])}),
    ("GET", "/st-zz*/_recovery"),
    ("GET", "/st-zz/_recovery"),
    ("GET", "/st-a/_shard_stores?status=green", None,
     {"pick": lambda r: {s: [x["allocation"] for x in v["stores"]]
                         for s, v in r["indices"]["st-a"]["shards"].items()}}),
    ("GET", "/st-a/_shard_stores"),
    ("POST", "/st-a/_disk_usage"),
    ("POST", "/st-a/_disk_usage?run_expensive_tasks=true", None,
     {"pick": lambda r: [sorted(r["st-a"]), sorted(r["st-a"]["fields"])]}),
    ("POST", "/st-a/_close", None, {"pick": lambda r: r["acknowledged"]}),
    ("GET", "/st-a/_segments"),
    ("GET", "/st-a/_recovery", None,
     {"pick": lambda r: [(s["id"], s["type"], s["stage"]) for s in r["st-a"]["shards"]]}),
    ("GET", "/st-a/_stats"),
    ("DELETE", "/st-a"),
])
# --- Retrievers, quantized kNN scores, feature fields (vec-*) ---------

VEC_RET = "/vec-ret/_search"


def vec_hits(r):
    out = [(h["_id"], h.get("_score"), h.get("fields"), h.get("sort")) for h in r["hits"]["hits"]]
    return out + [r["hits"].get("total"), r["hits"].get("max_score"), r.get("terminated_early"),
                  r.get("aggregations")]


VEC_RET_DOCS = [
    {"t": "a b", "k": "x", "n": 1, "v": [1, 1]},
    {"t": "a", "k": "y", "n": 2, "v": [2, 2]},
    {"t": "b", "k": "x", "n": 3, "v": [3, 3]},
    {"t": "a c", "k": "x", "n": 4, "v": [4, 4]},
    {"t": "c", "k": "y", "n": 5, "v": [5, 5]},
]

VEC_KNN = {"field": "v", "query_vector": [2, 2], "k": 3, "num_candidates": 5}

scenario("vec_retrievers", setup("vec-ret", {"settings": {"number_of_shards": 1}, "mappings": {"properties": {
    "t": {"type": "text"}, "k": {"type": "keyword"}, "n": {"type": "integer"},
    "v": {"type": "dense_vector", "dims": 2, "similarity": "l2_norm"}}}}, VEC_RET_DOCS) + [
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match": {"t": "a"}}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match": {"t": "a"}}, "filter": {"term": {"k": "x"}}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"filter": [{"term": {"k": "x"}}, {"range": {"n": {"gt": 1}}}]}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match_all": {}}, "sort": [{"n": "desc"}], "search_after": [4]}}, "size": 2}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match_all": {}}, "sort": [{"n": "asc"}], "collapse": {"field": "k"}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match": {"t": "a"}}, "min_score": 0.5}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match": {"t": "a c"}}, "_name": "q"}}, "size": 1, "from": 1, "fields": ["k"]}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"match_all": {}}}}, "size": 0, "aggs": {"a": {"terms": {"field": "k"}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"standard": {"filter": {"bool": {"must_not": {"term": {"k": "y"}}}}, "sort": ["n"], "terminate_after": 2}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"knn": VEC_KNN}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, filter={"term": {"k": "x"}})}, "fields": ["n"]}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, similarity=1.5, _name="kn")}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, filter=[{"term": {"k": "x"}}])}, "size": 1, "from": 1}, {"pick": vec_hits}),
    # Errors.
    ("POST", VEC_RET, {"retriever": {"knn": {"field": "v", "query_vector": [1, 1]}}}),
    ("POST", VEC_RET, {"retriever": {"knn": {"field": "v", "query_vector": [1, 1], "k": 2}}}),
    ("POST", VEC_RET, {"retriever": {"knn": {"query_vector": [1, 1], "k": 2, "num_candidates": 2}}}),
    ("POST", VEC_RET, {"retriever": {"knn": {"field": "v", "k": 2, "num_candidates": 2}}}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, k=0)}}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, num_candidates=1)}}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, num_candidates=20000)}}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, boost=2)}}),
    ("POST", VEC_RET, {"retriever": {"knn": dict(VEC_KNN, query_vector=[1, 2, 3])}}),
    ("POST", VEC_RET, {"retriever": {"knn": {"field": "v", "query_vector_builder": {"text_embedding": {"model_id": "m", "model_text": "x"}}, "k": 1, "num_candidates": 2}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {}}, "query": {"match_all": {}}, "knn": VEC_KNN, "sort": ["n"], "min_score": 1, "search_after": [1], "terminate_after": 2}),
    ("POST", VEC_RET, {"retriever": {"standard": {}}, "sub_searches": [{"query": {"match_all": {}}}]}),
    ("POST", VEC_RET, {"retriever": {}}),
    ("POST", VEC_RET, {"retriever": []}),
    ("POST", VEC_RET, {"retriever": "x"}),
    ("POST", VEC_RET, {"retriever": {"standard": 1}}),
    ("POST", VEC_RET, {"retriever": {"foo": {}}}),
    ("POST", VEC_RET, {"retriever": {"standar": {}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {}, "knn": VEC_KNN}}),
    ("POST", VEC_RET, {"retriever": {"standard": {"bad": 1}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {"query": {"bogus": {}}}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {"filter": [{"bogus": {}}]}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {"sort": "n"}}}),
    ("POST", VEC_RET, {"retriever": {"standard": {"terminate_after": -1}}}),
    ("POST", VEC_RET, {"retriever": {"rrf": 1}}),
    # sub_searches and rank without a license.
    ("POST", VEC_RET, {"sub_searches": [{"query": {"match": {"t": "a"}}}, {"query": {"match": {"t": "b"}}}]}),
    ("POST", VEC_RET, {"sub_searches": [{"query": {"match": {"t": "a"}}}]}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"sub_searches": [{"query": {"match": {"t": "a"}}}], "knn": VEC_KNN}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"sub_searches": [{"query": {"match": {"t": "a"}}}], "query": {"match_all": {}}}),
    ("POST", VEC_RET, {"sub_searches": [{"query": {"match": {"t": "a"}}, "x": 1}]}),
    ("POST", VEC_RET, {"sub_searches": {}}),
    ("POST", VEC_RET, {"rank": {"foo": {}}}),
    ("POST", VEC_RET, {"rank": {}}),
    ("POST", VEC_RET, {"rank": 1}),
    # terminate_after on a plain search.
    ("POST", VEC_RET, {"terminate_after": 2, "query": {"match": {"t": "a"}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"terminate_after": 1, "query": {"constant_score": {"filter": {"term": {"k": "x"}}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"terminate_after": 10, "query": {"constant_score": {"filter": {"term": {"k": "x"}}}}}, {"pick": vec_hits}),
    ("POST", VEC_RET, {"terminate_after": 2, "query": {"bool": {"must_not": {"term": {"k": "y"}}}}, "sort": [{"n": "desc"}]}, {"pick": vec_hits}),
    ("DELETE", "/vec-ret"),
])


def vec_vectors(seed, n, dims, unit=False):
    import random
    rnd = random.Random(seed)
    out = []
    for _ in range(n):
        v = [round(rnd.uniform(-50, 50), 3) for _ in range(dims)]
        if unit:
            m = sum(x * x for x in v) ** 0.5
            v = [x / m for x in v]
        out.append(v)
    return out


def vec_scores(r):
    return [(h["_id"], h["_score"]) for h in r["hits"]["hits"]]


def vec_quantized(name, opts, sim, vectors, queries):
    props = {"v": {"type": "dense_vector", "dims": len(vectors[0]), "similarity": sim, "index_options": opts}}
    steps = [("DELETE", f"/{name}?ignore_unavailable=true"),
             ("PUT", f"/{name}", {"settings": {"number_of_shards": 1}, "mappings": {"properties": props}}, ACK)]
    bulk = []
    for i, v in enumerate(vectors, 1):
        bulk += [{"index": {"_index": name, "_id": str(i)}}, {"v": v}]
    steps.append(("POST", "/_bulk?refresh=true", bulk, {"pick": lambda r: r["errors"]}))
    for q in queries:
        steps.append(("POST", f"/{name}/_search", {"knn": {"field": "v", "query_vector": q, "k": 8, "num_candidates": 100}, "size": 8, "_source": False}, {"pick": vec_scores}))
    steps.append(("DELETE", f"/{name}"))
    return steps


VEC_Q = vec_vectors(1, 30, 6)
VEC_QU = vec_vectors(2, 30, 6, unit=True)
VEC_QS = vec_vectors(3, 7, 4)
vec_quant_steps = []
for vec_t in ["int8_hnsw", "int4_hnsw", "int8_flat", "int4_flat"]:
    for vec_sim, vec_docs in [("l2_norm", VEC_Q), ("cosine", VEC_Q), ("dot_product", VEC_QU), ("max_inner_product", VEC_Q)]:
        vec_quant_steps += vec_quantized(f"vec-q-{vec_t.replace('_', '-')}-{vec_sim.replace('_', '-')}", {"type": vec_t}, vec_sim, vec_docs, [vec_docs[3], vec_vectors(9, 1, 6, unit=vec_sim == "dot_product")[0]])
    vec_quant_steps += vec_quantized(f"vec-q-{vec_t.replace('_', '-')}-small", {"type": vec_t}, "l2_norm", VEC_QS, [VEC_QS[0], [1, 2, 3, 4]])
vec_quant_steps += vec_quantized("vec-q-ci", {"type": "int8_hnsw", "confidence_interval": 0.95}, "l2_norm", VEC_Q, [VEC_Q[0]])
vec_quant_steps += vec_quantized("vec-q-ci1", {"type": "int8_flat", "confidence_interval": 1.0}, "cosine", VEC_Q, [VEC_Q[0]])
vec_quant_steps += vec_quantized("vec-q-int4-ci", {"type": "int4_hnsw", "confidence_interval": 0.9}, "l2_norm", VEC_Q, [VEC_Q[0]])
scenario("vec_quantized", vec_quant_steps + [
    ("DELETE", "/vec-q-sim?ignore_unavailable=true"),
    ("PUT", "/vec-q-sim", {"settings": {"number_of_shards": 1}, "mappings": {"properties": {"v": {"type": "dense_vector", "dims": 5, "similarity": "l2_norm", "index_options": {"type": "int8_hnsw"}}, "n": {"type": "keyword"}}}}, ACK),
    ("POST", "/_bulk?refresh=true", [{"index": {"_index": "vec-q-sim", "_id": "1"}}, {"n": "a", "v": [230.0, 300.33, -34.8988, 15.555, -200.0]},
                                     {"index": {"_index": "vec-q-sim", "_id": "2"}}, {"n": "b", "v": [-0.5, 100.0, -13, 14.8, -156.0]},
                                     {"index": {"_index": "vec-q-sim", "_id": "3"}}, {"n": "c", "v": [0.5, 111.3, -13.0, 14.8, -156.0]}], {"pick": lambda r: r["errors"]}),
    ("POST", "/vec-q-sim/_search", {"knn": {"field": "v", "query_vector": [-0.5, 90.0, -10, 14.8, -156.0], "k": 3, "num_candidates": 3, "similarity": 10.3}}, {"pick": vec_scores}),
    ("POST", "/vec-q-sim/_search", {"query": {"knn": {"field": "v", "query_vector": [-0.5, 90.0, -10, 14.8, -156.0], "similarity": 11, "filter": {"term": {"n": "b"}}}}}, {"pick": vec_scores}),
    ("POST", "/vec-q-sim/_search", {"query": {"script_score": {"query": {"match_all": {}}, "script": {"source": "1 / (1 + l2norm(params.q, 'v'))", "params": {"q": [-0.5, 90.0, -10, 14.8, -156.0]}}}}}, {"pick": vec_scores}),
    ("DELETE", "/vec-q-sim"),
])

VEC_FEAT = "/vec-feat/_search"
VEC_FEAT_DOCS = [
    {"t": {"a": 1.5, "b": 0.33333}, "rf": 10, "rfs": {"x": 2.5, "y": 7}, "neg": 3},
    {"t": [{"a": 0.2, "c": 3.1}, {"a": 2.7}], "rf": 0.5, "rfs": {"x": 11}, "neg": 0.25},
    {"t": {"d": 1}, "rf": 100},
    {"t": {}, "rf": "7.5", "rfs": {"y": 0.001}},
    {"t": [], "o": {"s": "z"}},
]


def vec_feat(q):
    return ("POST", VEC_FEAT, {"query": q}, {"pick": lambda r: [(h["_id"], h["_score"], h.get("matched_queries")) for h in r["hits"]["hits"]]})


def vec_doc(body):
    return ("PUT", "/vec-feat/_doc/x", body, {"pick": lambda r: r.get("result")})


scenario("vec_features", setup("vec-feat", {"settings": {"number_of_shards": 1}, "mappings": {"properties": {
    "t": {"type": "sparse_vector"}, "rf": {"type": "rank_feature"}, "rfs": {"type": "rank_features"},
    "neg": {"type": "rank_feature", "positive_score_impact": False}, "txt": {"type": "text"},
    "o": {"properties": {"s": {"type": "keyword"}}}}}}, VEC_FEAT_DOCS) + [
    ("GET", "/vec-feat/_mapping"),
    vec_feat({"rank_feature": {"field": "rf"}}),
    vec_feat({"rank_feature": {"field": "rf", "boost": 2, "_name": "f"}}),
    vec_feat({"rank_feature": {"field": "rf", "saturation": {"pivot": 5}}}),
    vec_feat({"rank_feature": {"field": "rf", "saturation": {}}}),
    vec_feat({"rank_feature": {"field": "rf", "log": {"scaling_factor": 4}}}),
    vec_feat({"rank_feature": {"field": "rf", "sigmoid": {"pivot": 7, "exponent": 0.6}}}),
    vec_feat({"rank_feature": {"field": "rf", "linear": {}}}),
    vec_feat({"rank_feature": {"field": "rfs.x"}}),
    vec_feat({"rank_feature": {"field": "rfs.y", "log": {"scaling_factor": 2}}}),
    vec_feat({"rank_feature": {"field": "rfs.zzz"}}),
    vec_feat({"rank_feature": {"field": "neg"}}),
    vec_feat({"rank_feature": {"field": "neg", "linear": {}}}),
    vec_feat({"rank_feature": {"field": "nope"}}),
    vec_feat({"rank_feature": {"field": "t.a"}}),
    vec_feat({"rank_feature": {"field": "o"}}),
    vec_feat({"rank_feature": {}}),
    vec_feat({"rank_feature": "rf"}),
    vec_feat({"rank_feature": {"field": "rf", "log": {"scaling_factor": 2}, "saturation": {}}}),
    vec_feat({"rank_feature": {"field": "rf", "foo": {}}}),
    vec_feat({"rank_feature": {"field": "rf", "log": {}}}),
    vec_feat({"rank_feature": {"field": "rf", "sigmoid": {"pivot": 5}}}),
    vec_feat({"rank_feature": {"field": "neg", "log": {"scaling_factor": 2}}}),
    vec_feat({"rank_feature": {"field": "rf", "saturation": {"pivot": -1}}}),
    vec_feat({"rank_feature": {"field": "rf", "log": {"scaling_factor": 0.5}}}),
    vec_feat({"rank_feature": {"field": "t"}}),
    vec_feat({"rank_feature": {"field": "rfs"}}),
    vec_feat({"rank_feature": {"field": "txt"}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1, "b": 2, "c": 0.5}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "boost": 3, "_name": "n"}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1, "zz": 5}, "prune": True}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1, "c": 0.1}, "prune": True, "pruning_config": {"tokens_freq_ratio_threshold": 1, "tokens_weight_threshold": 0.5}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1, "c": 0.1}, "prune": True, "pruning_config": {"tokens_freq_ratio_threshold": 1, "tokens_weight_threshold": 0.5, "only_score_pruned_tokens": True}}}),
    vec_feat({"sparse_vector": {"field": "nope", "query_vector": {"a": 1}}}),
    vec_feat({"sparse_vector": {}}),
    vec_feat({"sparse_vector": {"field": "t"}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "inference_id": "m"}}),
    vec_feat({"sparse_vector": {"field": "t", "inference_id": "m"}}),
    vec_feat({"sparse_vector": {"field": "t", "inference_id": "m", "query": "hi"}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": -1}}}),
    vec_feat({"sparse_vector": {"field": "rfs", "query_vector": {"x": 1}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": "x"}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "foo": 1}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "pruning_config": {"tokens_freq_ratio_threshold": 200}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "prune": True, "pruning_config": {"tokens_weight_threshold": 2}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": {"a": 1}}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": [{"a": 1}, {"c": 2}], "boost": 2, "_name": "w"}}}),
    vec_feat({"weighted_tokens": {"rfs": {"tokens": {"x": 1, "y": 0.5}}}}),
    vec_feat({"weighted_tokens": {"t": {}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": {"a": 1}, "pruning_config": {"tokens_freq_ratio_threshold": 200}}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": {"a": 1}, "pruning_config": {"bad": 1}}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": {"a": 1}, "foo": 1}}}),
    vec_feat({"weighted_tokens": {"t": {"tokens": {"a": "x"}}}}),
    vec_feat({"sparse_vector": {"field": "t", "query_vector": {"a": 1}, "pruning_config": {"bad": 2}}}),
    vec_feat({"weighted_tokens": {}}),
    vec_feat({"weighted_tokens": {"nope": {"tokens": {"a": 1}}}}),
    vec_feat({"weighted_tokens": {"txt": {"tokens": {"a": 1}}}}),
    vec_feat({"text_expansion": {"t": {"model_id": "m", "model_text": "x"}}}),
    vec_feat({"text_expansion": {"t": {"model_text": "x"}}}),
    vec_feat({"text_expansion": {"t": {"model_id": "m"}}}),
    vec_feat({"term": {"t": "a"}}),
    vec_feat({"term": {"t": {"value": "a", "boost": 2}}}),
    vec_feat({"term": {"rfs": "x"}}),
    vec_feat({"terms": {"t": ["a", "c"]}}),
    vec_feat({"terms": {"rfs": ["x", "y"], "boost": 3}}),
    vec_feat({"match": {"t": "a"}}),
    vec_feat({"match": {"t": "a c"}}),
    vec_feat({"match": {"t": {"query": "c", "boost": 2}}}),
    vec_feat({"bool": {"should": [{"term": {"t": "a"}}, {"term": {"t": "c"}}]}}),
    vec_feat({"exists": {"field": "t"}}),
    vec_feat({"exists": {"field": "rf"}}),
    vec_feat({"exists": {"field": "rfs"}}),
    vec_feat({"term": {"rf": 10}}),
    vec_feat({"match": {"rf": "1"}}),
    vec_feat({"range": {"rf": {"gte": 1}}}),
    vec_feat({"range": {"t": {"gte": 1}}}),
    vec_feat({"prefix": {"t": "a"}}),
    vec_feat({"wildcard": {"t": "a*"}}),
    vec_feat({"regexp": {"t": "a.*"}}),
    vec_feat({"fuzzy": {"t": "a"}}),
    ("POST", VEC_FEAT, {"fields": ["t", "rf", "rfs", "neg"], "_source": False, "sort": ["_doc"]}, {"pick": lambda r: [h.get("fields") for h in r["hits"]["hits"]]}),
    ("POST", VEC_FEAT, {"docvalue_fields": ["rf"]}),
    ("POST", VEC_FEAT, {"docvalue_fields": ["t"]}),
    ("POST", VEC_FEAT, {"aggs": {"a": {"terms": {"field": "t"}}}, "size": 0}),
    ("POST", VEC_FEAT, {"aggs": {"a": {"avg": {"field": "rf"}}}, "size": 0}),
    ("POST", VEC_FEAT, {"aggs": {"a": {"terms": {"field": "rfs"}}}, "size": 0}),
    ("POST", VEC_FEAT, {"sort": ["rf"]}),
    ("POST", VEC_FEAT, {"sort": [{"t": "desc"}]}),
    # Indexing checks.
    vec_doc({"rf": -1}), vec_doc({"rf": "abc"}), vec_doc({"rf": [1, 2]}), vec_doc({"rf": True}),
    vec_doc({"rf": {"a": 1}}), vec_doc({"rf": 0}), vec_doc({"rf": 1e-40}), vec_doc({"rf": None}), vec_doc({"rf": "5"}),
    vec_doc({"rfs": {"x": -1}}), vec_doc({"rfs": {"x": "a"}}), vec_doc({"rfs": 5}), vec_doc({"rfs": {"x": [1, 2]}}),
    vec_doc({"rfs": {"x": 0}}), vec_doc({"rfs": {"x.y": 1}}), vec_doc({"rfs": [{"x": 1}, {"x": 2}]}),
    vec_doc({"rfs": [{"x": 1}, {"y": 2}]}), vec_doc({"rfs": {"x": True}}),
    vec_doc({"t": {"a": -1}}), vec_doc({"t": {"a": "2"}}), vec_doc({"t": {"a": 0}}), vec_doc({"t": {"a.b": 1}}),
    vec_doc({"t": 5}), vec_doc({"t": "x"}), vec_doc({"t": {"a": [1, 2]}}), vec_doc({"t": {"a": {"b": 1}}}),
    vec_doc({"t": [[{"a": 1}]]}), vec_doc({"t": {"a": 1e-40}}), vec_doc({"t": {"a": None}}), vec_doc({"t": {"a": True}}),
    ("DELETE", "/vec-feat"),
])

scenario("vec_semantic", [
    ("DELETE", "/vec-sem?ignore_unavailable=true"),
    ("DELETE", "/vec-sem2?ignore_unavailable=true"),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"s": {"type": "semantic_text"}}}}),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"o": {"properties": {"s": {"type": "semantic_text"}}}}}}),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"s": {"type": "semantic_text", "inference_id": "x", "foo": 1}}}}),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"rf": {"type": "rank_feature", "positive_score_impact": "maybe"}}}}),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"rf": {"type": "rank_feature", "foo": 1}}}}),
    ("PUT", "/vec-sem2", {"mappings": {"properties": {"t": {"type": "sparse_vector", "store": True}}}}),
    ("PUT", "/vec-sem", {"mappings": {"properties": {"s": {"type": "semantic_text", "inference_id": "nope"}, "t": {"type": "text"}}}}, ACK),
    ("GET", "/vec-sem/_mapping"),
    ("PUT", "/vec-sem/_doc/1?refresh=true", {"s": "hello", "t": "x"}),
    ("PUT", "/vec-sem/_doc/2?refresh=true", {"t": "x"}, {"pick": lambda r: r.get("result")}),
    ("POST", "/vec-sem/_search", {"query": {"semantic": {"field": "s", "query": "hi"}}}),
    ("POST", "/vec-sem/_search", {"query": {"semantic": {"field": "t", "query": "hi"}}}),
    ("POST", "/vec-sem/_search", {"query": {"semantic": {"field": "nope", "query": "hi"}}}, {"pick": vec_scores}),
    ("POST", "/vec-sem/_search", {"query": {"semantic": {"query": "hi"}}}),
    ("POST", "/vec-sem/_search", {"query": {"semantic": {"field": "s", "query": "hi", "x": 1}}}),
    ("POST", "/vec-sem/_search", {"query": {"match": {"s": "hi"}}}),
    ("POST", "/vec-sem/_search", {"retriever": {"text_similarity_reranker": 1}}),
    ("DELETE", "/vec-sem"),
])


def vec_bit_score(f, q):
    return ("POST", "/vec-bit/_search", {"query": {"script_score": {"query": {"match_all": {}}, "script": {"source": f"{f}(params.q, 'b') + 100", "params": {"q": q}}}}}, {"pick": vec_scores})


scenario("vec_bits", [
    ("DELETE", "/vec-bit?ignore_unavailable=true"),
    ("PUT", "/vec-bit", {"mappings": {"properties": {"b": {"type": "dense_vector", "dims": 16, "element_type": "bit"}}}}, ACK),
    ("PUT", "/vec-bit/_doc/1?refresh=true", {"b": [5, -3]}, {"pick": lambda r: r.get("result")}),
] + [vec_bit_score(f, q) for f in ["hamming", "l1norm", "l2norm", "dotProduct", "cosineSimilarity"]
     for q in [[1, 2], "0a0b", [0.5, 1.5, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]]] + [
    ("DELETE", "/vec-bit"),
])


# --- Synthetic `_source` (`"_source": {"mode": "synthetic"}`) -------------

SYN = {"mode": "synthetic"}


def syn_docs(index, docs):
    """Index each doc (id = position) and GET it back."""
    steps = []
    for i, d in enumerate(docs):
        steps.append(("PUT", f"/{index}/_doc/{i}?refresh=true", d, {"pick": lambda r: r.get("result")}))
        steps.append(("GET", f"/{index}/_doc/{i}", None, {"pick": lambda r: r.get("_source")}))
    return steps


def src_hits(r):
    return [(h["_id"], h.get("_source")) for h in r["hits"]["hits"]]


scenario("synth_leaf_types", [
    ("DELETE", "/syn-leaf?ignore_unavailable=true"),
    ("PUT", "/syn-leaf", {"mappings": {"_source": SYN, "properties": {
        "k": {"type": "keyword"}, "kia": {"type": "keyword", "ignore_above": 3},
        "ks": {"type": "keyword", "store": True, "doc_values": False, "ignore_above": 3},
        "kn": {"type": "keyword", "doc_values": False},
        "knv": {"type": "keyword", "null_value": "NULL"},
        "l": {"type": "long"}, "i": {"type": "integer"}, "sh": {"type": "short"},
        "f": {"type": "float"}, "h": {"type": "half_float"}, "d": {"type": "double"},
        "sf": {"type": "scaled_float", "scaling_factor": 100}, "ul": {"type": "unsigned_long"},
        "ln": {"type": "long", "doc_values": False},
        "b": {"type": "boolean"}, "ip": {"type": "ip"},
        "ipm": {"type": "ip", "ignore_malformed": True},
        "im": {"type": "integer", "ignore_malformed": True},
        "dt": {"type": "date"}, "dtf": {"type": "date", "format": "yyyy/MM/dd||epoch_millis"},
        "dte": {"type": "date", "format": "epoch_millis"}, "dn": {"type": "date_nanos"},
        "t": {"type": "text", "store": True},
        "tk": {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 4}}},
        "w": {"type": "wildcard"}, "v": {"type": "version"}, "mot": {"type": "match_only_text"},
        "gp": {"type": "geo_point"}, "dv": {"type": "dense_vector", "dims": 3},
        "fl": {"type": "flattened"}, "flia": {"type": "flattened", "ignore_above": 3},
        "am": {"type": "aggregate_metric_double", "metrics": ["min", "max"], "default_metric": "max"},
        "cp": {"type": "completion"},
    }}}),
] + syn_docs("syn-leaf", [
    {"k": ["b", "a", "b", "c"], "kia": ["long", "b", "a", "long"], "ks": ["zz", "longer", "aa", "zz"]},
    {"kn": ["b", "a", "b"], "knv": [None, "a"], "k": None},
    {"l": [3, 1, 2, 1, "5", 1.7], "i": [2.9, -2.9], "sh": "7", "ln": ["3", 1.7, 1]},
    {"f": [1.1, 2.5, 1.1], "h": [1.1, 3, 0.1], "d": [1.1, "2.5", 1e300], "sf": [1.234, 5, 1.005]},
    {"ul": [18446744073709551615, 1, "5"], "b": ["true", False, True, "false"]},
    {"ip": ["10.0.0.2", "10.0.0.1", "::ffff:10.0.0.1", "2001:db8::1", "::1", "fe80::1"]},
    {"ipm": ["10.0.0.1", "garbage", "1.1.1.1", 7], "im": [5, "x", False, 1]},
    {"dt": ["2017-09-01", 1504224000000, "2017-09-01T10:20:30.123+01:00", "2017"],
     "dtf": ["2017/09/01", 1504224000000], "dte": ["1504224000123", 5]},
    {"dn": ["2017-09-01T00:00:00.123456789Z", "2017-09-01T00:00:00.1Z", "2017-09-01T00:00:00Z", 1504224000000]},
    {"t": ["b c", "a", "b c"], "tk": ["b c", "a", "b c", "longer text", "a"]},
    {"w": ["b", "a", "b"], "v": ["1.10.0", "1.2.0", "1.2.0", "1.2.0-beta"], "mot": ["b", "a"]},
    {"gp": [{"lat": 1.5, "lon": 2.5}, "41.12,-71.34", [-71.34, 41.12], "POINT (2 1)"]},
    {"gp": {"lat": 41.12, "lon": -71.34}, "dv": [1, 2.5, 300.33]},
    {"fl": {"b": [3, True, None, "x", 1.5], "a": {"c": None}, "e": [], "d": {}, "z": {"y": ["2", "10", "2"]}}},
    {"fl": [{"x": 1}, {"x": 2, "y": 0}], "flia": {"a": "abcd", "b": ["x", "abcd", "y"]}},
    {"am": {"min": 1, "max": 5}, "cp": ["b", "a"]},
    {"k": ["a"], "l": [7], "f": [], "x_unmapped": [3, 1], "_doc_count": 2},
]) + [
    ("POST", "/syn-leaf/_search", {"query": {"ids": {"values": [0, 2]}}, "_source": ["k", "l", "i"]}, {"pick": src_hits}),
    ("POST", "/syn-leaf/_search", {"query": {"ids": {"values": ["0", "3"]}}, "fields": ["k", "f", "h"], "_source": False},
     {"pick": lambda r: [(h["_id"], h.get("fields")) for h in r["hits"]["hits"]]}),
    ("DELETE", "/syn-leaf"),
])

scenario("synth_objects", [
    ("DELETE", "/syn-obj?ignore_unavailable=true"),
    ("PUT", "/syn-obj", {"mappings": {"_source": SYN, "properties": {
        "o": {"properties": {"a": {"type": "long"}, "b": {"type": "keyword"},
                             "in": {"properties": {"x": {"type": "long"}}},
                             "dis": {"enabled": False}, "kn": {"type": "keyword", "doc_values": False}}},
        "n": {"type": "nested", "properties": {"a": {"type": "long"}, "nn": {"type": "nested"}}},
        "dis": {"type": "object", "enabled": False},
        "df": {"dynamic": False, "properties": {"m": {"type": "keyword"}}},
        "dr": {"dynamic": "runtime", "properties": {"m": {"type": "keyword"}}},
        "sas": {"store_array_source": True, "properties": {"a": {"type": "long"}, "b": {"type": "keyword"}}},
        "so": {"type": "object", "subobjects": False, "properties": {"a.b": {"type": "keyword"}}},
        "z": {"type": "keyword"},
    }}}),
] + syn_docs("syn-obj", [
    {"o": [{"a": 2, "b": "y"}, {"a": 1, "b": "x", "in": [{"x": 5}, {"x": 4}]}]},
    {"o": [{"a": 1}], "o.b": "q", "o.in.x": 3},
    {"o": [{"dis": {"b": 2}}, {"dis": {"a": 1}}, {"a": 5}]},
    {"o": [{"kn": "b"}, {"kn": "a"}], "z": "1"},
    {"n": [{"a": [3, 1]}, {}, {"a": 2, "nn": [{"q": [3, 2]}, {"q": 1}]}]},
    {"n": {"a": 1}}, {"n": []}, {"n": [None, {"a": 1}]},
    {"dis": {"b": 2, "a": [3, 1, {"c": None}], "x.y": 1}, "z": "x"},
    {"dis": [{"b": 2}, {"a": 1}]}, {"dis": 5},
    {"df": {"m": ["b", "a"], "zz": [3, 1, {"q": 1}], "y": {"w": 2}, "y.x": 1}},
    {"df": [{"m": "x", "u": 1}, {"m": "y", "u": 2}]},
    {"dr": {"m": ["b", "a"], "zz": [3, 1]}}, {"dr": [{"m": "b"}, {"m": "a"}]}, {"dr": {"m": "b", "x": 1}},
    {"sas": [{"b": "z", "a": 2}, {"a": 1}]}, {"sas": {"b": "z", "a": [2, 1]}},
    {"so": {"a.b": ["y", "x"], "c.d": 5, "e": {"f": 1}}}, {"so.a.b": "q"},
    {"o": {"a": None, "b": None}, "z": []}, {"o": [{}, {}]},
    {"a.b.c": 1, "a": {"b": {"d": [2, 1]}}, "new_obj": [{"p": 2}, {"p": 1}]},
]) + [
    ("GET", "/syn-obj/_mapping"),
    ("POST", "/syn-obj/_search", {"query": {"ids": {"values": ["0", "4", "11"]}}}, {"pick": lambda r: sorted(src_hits(r), key=lambda h: h[0])}),
    ("DELETE", "/syn-obj"),
    # A disabled root keeps everything as sent.
    ("DELETE", "/syn-root?ignore_unavailable=true"),
    ("PUT", "/syn-root", {"mappings": {"_source": SYN, "enabled": False}}),
] + syn_docs("syn-root", [{"name": "aaaa", "b": [3, 1], "a.very.deeply": "AAAA", "n": None}]) + [
    ("GET", "/syn-root/_mapping"),
    ("DELETE", "/syn-root"),
    # Beyond the total fields limit, dynamic fields are kept unmapped.
    ("DELETE", "/syn-limit?ignore_unavailable=true"),
    ("PUT", "/syn-limit", {"settings": {"index.mapping.total_fields.limit": 3,
                                        "index.mapping.total_fields.ignore_dynamic_beyond_limit": True},
                           "mappings": {"_source": SYN, "properties": {"name": {"type": "keyword"}}}}),
] + syn_docs("syn-limit", [
    {"name": "x", "p_int": 1000, "q_double": 123.456789, "r_str": "AaAa", "s.very.deep": "A"},
    {"name": "y", "arr": [3, 1, 2], "objs": [{"v": 2}, {"v": 1}]},
]) + [
    ("GET", "/syn-limit/_mapping"),
    ("DELETE", "/syn-limit"),
])

scenario("synth_ranges", [
    ("DELETE", "/syn-range?ignore_unavailable=true"),
    ("PUT", "/syn-range", {"mappings": {"_source": SYN, "properties": {
        "ir": {"type": "integer_range"}, "lr": {"type": "long_range"}, "fr": {"type": "float_range"},
        "dr": {"type": "double_range"}, "dtr": {"type": "date_range"},
        "dtf": {"type": "date_range", "format": "yyyy-MM-dd"}, "ipr": {"type": "ip_range"}}}}),
] + syn_docs("syn-range", [
    {"ir": [{"gte": 1, "lte": 2}, {"gte": 1, "lte": 2}, {"lte": 0}, {"gte": None}]},
    {"ir": {"gt": 1.5, "lt": 3.7}, "lr": {"gte": "5", "lte": "10"}},
    {"fr": {"gt": 1.0, "lt": 2.0}, "dr": [{"gte": 4, "lte": 8}, {"gte": 4, "lte": 7}]},
    {"fr": {"gte": 1.1}, "dr": {"gt": 1.5}},
    {"dtr": {"gt": "2017-09-01", "lt": "2017-09-05"}, "dtf": {"gte": "2017-09-01", "lt": "2017-09-03"}},
    {"dtr": {"gte": 1504224000000, "lte": "2017-09-05T03:04:05.789Z"}},
    {"ipr": "74.125.227.0/25"}, {"ipr": {"gt": "2001:db8::", "lt": "200a:100::"}},
    {"ipr": {"gte": "::ffff:1.2.3.4", "lte": "1.2.3.5"}}, {"ir": None},
]) + [("DELETE", "/syn-range")])


def syn_create(fields, index="syn-bad", mode=SYN, settings=None):
    body = {"mappings": {"_source": mode, "properties": fields}}
    if settings:
        body["settings"] = settings
    return [("PUT", f"/{index}", body, {"pick": lambda r: r.get("acknowledged")}),
            ("DELETE", f"/{index}?ignore_unavailable=true")]


scenario("synth_mapping_errors", [("DELETE", "/syn-bad?ignore_unavailable=true")]
  + syn_create({"t": {"type": "text"}})
  + syn_create({"t": {"type": "text", "store": True}})
  + syn_create({"t": {"type": "text", "fields": {"raw": {"type": "keyword"}}}})
  + syn_create({"t": {"type": "text", "fields": {"raw": {"type": "keyword", "normalizer": "lowercase"}}}})
  + syn_create({"t": {"type": "text", "fields": {"raw": {"type": "keyword", "doc_values": False}}}})
  + syn_create({"b": {"type": "binary"}})
  + syn_create({"b": {"type": "binary", "doc_values": True}})
  + syn_create({"k": {"type": "keyword", "normalizer": "lowercase"}})
  + syn_create({"k": {"type": "keyword", "copy_to": "other"}, "other": {"type": "keyword"}})
  + syn_create({"k": {"type": "boolean", "doc_values": False}})
  + syn_create({"k": {"type": "ip", "doc_values": False}})
  + syn_create({"k": {"type": "date", "doc_values": False}})
  + syn_create({"k": {"type": "geo_point", "doc_values": False}})
  + syn_create({"k": {"type": "flattened", "doc_values": False}})
  + syn_create({"k": {"type": "integer_range", "doc_values": False}})
  + syn_create({"k": {"type": "keyword", "doc_values": False}})
  + syn_create({"k": {"type": "long", "doc_values": False}})
  + syn_create({"o": {"properties": {"z": {"type": "text"}, "b": {"type": "binary"}}}})
  + syn_create({"n": {"type": "nested", "properties": {"t": {"type": "text"}}}})
  + syn_create({"t": {"type": "text"}}, settings={"index.mode": "logsdb"})
  + syn_create({"t": {"type": "text"}}, mode={"mode": "stored"})
  + syn_create({}, mode={"mode": "synthetic", "includes": ["a"]})
  + syn_create({}, mode={"mode": "synthetic", "enabled": False})
  + syn_create({}, mode={"mode": "bogus"})
  + [
    ("PUT", "/syn-bad", {"mappings": {"_source": SYN}}),
    ("PUT", "/syn-bad/_mapping", {"properties": {"t": {"type": "text"}}}),
    ("PUT", "/syn-bad/_mapping", {"properties": {"t": {"type": "keyword", "normalizer": "lowercase"}}}),
    ("PUT", "/syn-bad/_mapping", {"_source": {"mode": "stored"}}),
    ("PUT", "/syn-bad/_mapping", {"_source": {"enabled": True}}),
    ("GET", "/syn-bad/_mapping"),
    ("PUT", "/syn-bad/_mapping", {"_source": {"excludes": ["a"]}}),
    ("PUT", "/syn-bad/_mapping", {"_source": {"mode": "synthetic"}}),
    ("PUT", "/syn-bad/_mapping", {"_source": {}}),
    ("GET", "/syn-bad/_mapping"),
    ("DELETE", "/syn-bad"),
    ("PUT", "/syn-bad", {"mappings": {"_source": {"mode": "stored"}}}),
    ("GET", "/syn-bad/_mapping"),
    ("PUT", "/syn-bad/_mapping", {"_source": SYN}),
    ("PUT", "/syn-bad/_doc/1?refresh=true", {"a.b": [3, 1]}, {"pick": lambda r: r.get("result")}),
    ("GET", "/syn-bad/_doc/1", None, {"pick": lambda r: r.get("_source")}),
    ("DELETE", "/syn-bad"),
])

scenario("synth_force", [
    ("DELETE", "/syn-f1?ignore_unavailable=true"), ("DELETE", "/syn-f2?ignore_unavailable=true"),
    ("PUT", "/syn-f1", {"mappings": {"properties": {"text": {"type": "text"}, "o": {"properties": {"k": {"type": "keyword"}}}}}}),
    ("PUT", "/syn-f1/_doc/1?refresh=true", {"text": "foo", "o.k": ["b", "a"]}),
    ("GET", "/syn-f1/_doc/1?force_synthetic_source=true"),
    ("GET", "/syn-f1/_doc/1"),
    ("POST", "/syn-f1/_mget?force_synthetic_source=true", {"ids": ["1", "2"]}),
    ("POST", "/syn-f1/_search?force_synthetic_source=true", {}),
    ("PUT", "/syn-f2", {"mappings": {"properties": {"o": {"properties": {"k": {"type": "keyword"}}}}}}),
    ("PUT", "/syn-f2/_doc/1?refresh=true", {"o.k": ["b", "a"], "x": 1.5, "s": "str", "n": [3, 1]}),
    ("GET", "/syn-f2/_doc/1?force_synthetic_source=true"),
    ("GET", "/syn-f2/_doc/1?force_synthetic_source=false"),
    ("GET", "/syn-f2/_doc/1?force_synthetic_source=bogus"),
    ("GET", "/syn-f2/_doc/1?force_synthetic_source=true&realtime=false"),
    ("POST", "/syn-f2/_mget?force_synthetic_source=true", {"ids": ["1"]}),
    ("POST", "/syn-f2/_search?force_synthetic_source=true", {}, {"pick": src_hits}),
    ("POST", "/syn-f2/_search", {}, {"pick": src_hits}),
    ("DELETE", "/syn-f1"), ("DELETE", "/syn-f2"),
])

scenario("synth_update", [
    ("DELETE", "/syn-up?ignore_unavailable=true"),
    ("PUT", "/syn-up", {"settings": {"index": {"refresh_interval": "-1"}},
                        "mappings": {"_source": SYN, "properties": {"k": {"type": "keyword"}, "n": {"type": "long"}}}}),
    ("PUT", "/syn-up/_doc/1", {"k": ["b", "a"], "n": "3", "o.p": 1}),
    ("GET", "/syn-up/_doc/1"),
    ("GET", "/syn-up/_doc/1?realtime=false"),
    ("POST", "/syn-up/_update/1", {"doc": {"x": 1}, "_source": True}),
    ("GET", "/syn-up/_doc/1"),
    ("POST", "/syn-up/_update/1", {"doc": {"k": ["a", "b"]}}),
    ("POST", "/syn-up/_update/1", {"script": {"source": "ctx._source.n += 1"}}),
    ("GET", "/syn-up/_doc/1"),
    ("GET", "/syn-up/_source/1"),
    ("GET", "/syn-up/_doc/1?_source_includes=k"),
    ("POST", "/syn-up/_refresh"),
    ("POST", "/syn-up/_search", {"query": {"ids": {"values": ["1"]}}, "fields": ["k", "n", "o.p"]}),
    ("DELETE", "/syn-up"),
])

scenario("synth_index_misc", [
    ("DELETE", "/syn-misc?ignore_unavailable=true"),
    ("PUT", "/syn-misc", {"mappings": {"properties": {"": {"type": "keyword"}}}}),
    ("PUT", "/syn-misc", {"mappings": {"properties": {"a": {"properties": {"": {"type": "keyword"}}}}}}),
    ("PUT", "/syn-misc", {"mappings": {"properties": {" ": {"type": "keyword"}}}}),
    ("PUT", "/syn-misc", {"mappings": {"properties": {"a..b": {"type": "keyword"}}}}),
    ("PUT", "/syn-misc", {"settings": {"soft_deletes.enabled": False}}),
    ("PUT", "/syn-misc", {"settings": {"index.soft_deletes.enabled": "false"}}),
    ("PUT", "/%3Csyn-misc-%7B2022-12-31%7C%7C%2Fd%7Byyyy-MM-dd%7D%7D%3E",
     {"aliases": {"<syn-misc-alias-{2022-12-31||/M{yyyy.MM}}>": {}}}),
    ("PUT", "/%3Csyn-misc-%7B2022-12-31%7C%7C%2Fd%7D%3E"),
    ("PUT", "/%3Csyn-misc-%7Bnow%2Fd%7Byyyy%7D%3E"),
    ("PUT", "/%3Csyn-misc-%7B2022-12-31%7C%7C%2Fd%7Byyyy-MM-dd%7D%7D%3E",
     {"aliases": {"<syn-misc-alias-{2022-12-31||/M{yyyy-MM-dd}}>": {}}}),
    ("GET", "/syn-misc-2022-12-31/_alias"),
    ("HEAD", "/_alias/syn-misc-alias-2022-12-01"),
    ("GET", "/%3Csyn-misc-%7B2022-12-31%7C%7C%2Fd%7Byyyy-MM-dd%7D%7D%3E/_count"),
    ("POST", "/_aliases", {"actions": [{"add": {"index": "<syn-misc-{2022-12-31||/d{yyyy-MM-dd}}>",
                                                "alias": "<syn-misc-al2-{2022-12-31||+1d{yyyy-MM-dd}}>"}}]}),
    ("GET", "/syn-misc-2022-12-31/_alias"),
    ("DELETE", "/syn-misc-2022-12-31"),
    # Index sorting.
    ("PUT", "/syn-misc", {"settings": {"index.sort.field": "nested_field.foo"},
                          "mappings": {"properties": {"nested_field": {"type": "nested", "properties": {"foo": {"type": "keyword"}}}}}}),
    ("PUT", "/syn-misc", {"settings": {"index.sort.field": "nope"}}),
    ("PUT", "/syn-misc", {"settings": {"index.sort.field": "t"}, "mappings": {"properties": {"t": {"type": "text"}}}}),
    ("PUT", "/syn-misc", {"settings": {"index.sort.field": ["a", "b"], "index.sort.order": ["desc"]},
                          "mappings": {"properties": {"a": {"type": "long"}, "b": {"type": "long"}}}}),
    ("PUT", "/syn-misc", {"settings": {"index.sort.field": ["a", "b"], "index.sort.order": ["desc", "asc"],
                                       "index.sort.missing": ["_first", "_last"], "index.refresh_interval": "-1"},
                          "mappings": {"properties": {"a": {"type": "long"}, "b": {"type": "keyword"}}}}),
    ("POST", "/_bulk?refresh=true", [
        {"index": {"_index": "syn-misc", "_id": "1"}}, {"a": 1, "b": "x"},
        {"index": {"_index": "syn-misc", "_id": "2"}}, {"a": 3, "b": "y"},
        {"index": {"_index": "syn-misc", "_id": "3"}}, {"b": "z"},
        {"index": {"_index": "syn-misc", "_id": "4"}}, {"a": 3, "b": "a"},
        {"index": {"_index": "syn-misc", "_id": "5"}}, {"a": [1, 5]}], {"pick": lambda r: r["errors"]}),
    ("POST", "/syn-misc/_search", {"sort": ["_doc"]}, {"pick": ids}),
    ("POST", "/syn-misc/_search?scroll=1m", {"track_total_hits": False}),
    ("POST", "/syn-misc/_search?scroll=1m", {"track_total_hits": 5}),
    ("DELETE", "/syn-misc"),
    # Values a field can't take.
    ("PUT", "/syn-misc", {"mappings": {"properties": {
        "l": {"type": "long"}, "i": {"type": "integer"}, "b": {"type": "boolean"}, "ip": {"type": "ip"},
        "k": {"type": "keyword"}, "t": {"type": "text"}, "f": {"type": "float"},
        "lim": {"type": "long", "ignore_malformed": True}, "o": {"properties": {"s": {"type": "short"}}}}}}),
] + [("PUT", "/syn-misc/_doc/1", d, {"pick": lambda r: r.get("result")}) for d in [
    {"l": "abc"}, {"l": True}, {"l": {"a": 1}}, {"l": [1, "x"]}, {"i": 3000000000}, {"l": 1.5e30},
    {"b": "yes"}, {"b": 1}, {"ip": "x"}, {"ip": 7}, {"k": {"a": 1}}, {"t": {"a": 1}}, {"f": "NaN"},
    {"lim": {"a": 1}}, {"lim": [{"a": 1}]}, {"lim": "x"}, {"k": [{"a": 1}]}, {"l": ""}, {"ip": ""}, {"b": ""},
    {"l": []}, {"l": [[1, 2]]}, {"k": [["a"]]}, {"o.s": 40000}, {"o": [{"s": 1}, {"s": "x"}]},
    {"l": "1.5", "i": 2.9, "b": "true", "ip": "::1", "k": 5, "f": "2.5"}, {"new": [1, "x"]},
]] + [
    ("POST", "/_bulk", [{"index": {"_index": "syn-misc", "_id": "2"}}, {"l": "abc"},
                        {"index": {"_index": "syn-misc", "_id": "3"}}, {"l": 3}],
     {"pick": lambda r: [(list(i.values())[0]["status"], list(i.values())[0].get("error", {}).get("type")) for i in r["items"]]}),
    ("DELETE", "/syn-misc"),
])

failures = 0
for name, steps in SCENARIOS.items():
    if ONLY and name not in ONLY:
        continue
    real, ours = Ctx(REAL), Ctx(OURS)
    print(f"== {name}")
    for i, step in enumerate(steps):
        a, b = run_step(real, step), run_step(ours, step)
        label = f"{step[0]} {step[1]}"
        if len(step) > 2 and step[2] is not None and not isinstance(step[2], (bytes, list)):
            label += " " + json.dumps(step[2])[:150]
        if a == b:
            print(f"  ok   {i:2} {label}")
            if os.environ.get("ES_EDGES_VERBOSE"):
                print(f"         both: {json.dumps(a)[:int(os.environ.get('ES_EDGES_WIDTH', '600'))]}")
        else:
            failures += 1
            print(f"  DIFF {i:2} {label}\n         real: {json.dumps(a)[:int(os.environ.get('ES_EDGES_WIDTH', '600'))]}\n        noida: {json.dumps(b)[:int(os.environ.get('ES_EDGES_WIDTH', '600'))]}")
print(f"\n{failures} differing steps")
sys.exit(min(failures, 100))
