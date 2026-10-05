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
import sys
import urllib.error
import urllib.request

REAL, OURS = sys.argv[1], sys.argv[2]
ONLY = set(sys.argv[3:])
VOLATILE = {"took", "_shards", "pit_id", "_scroll_id", "uuid", "creation_date", "id"}


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

MAPPING = {
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
    steps = [("DELETE", f"/{index}"), ("PUT", f"/{index}", mapping)]
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
LONG_MAPPING = {"mappings": {"properties": {"text": {"type": "text"}, "tags": {"type": "keyword"},
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

NESTED_MAPPING = {"mappings": {"properties": {
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
    ("GET", "/edge/_stats", None, {"pick": lambda r: r["indices"]["edge"]["primaries"]["docs"]}),
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
        else:
            failures += 1
            print(f"  DIFF {i:2} {label}\n         real: {json.dumps(a)[:600]}\n        noida: {json.dumps(b)[:600]}")
print(f"\n{failures} differing steps")
sys.exit(min(failures, 100))
