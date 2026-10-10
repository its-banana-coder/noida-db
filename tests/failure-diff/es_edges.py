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
