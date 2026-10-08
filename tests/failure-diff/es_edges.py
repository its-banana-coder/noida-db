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
                print(f"         both: {json.dumps(a)[:600]}")
        else:
            failures += 1
            print(f"  DIFF {i:2} {label}\n         real: {json.dumps(a)[:600]}\n        noida: {json.dumps(b)[:600]}")
print(f"\n{failures} differing steps")
sys.exit(min(failures, 100))
