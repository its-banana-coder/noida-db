"""elasticsearch-py 8.x smoke test against noida-db."""

import os

from elasticsearch import Elasticsearch


url = os.environ["ELASTICSEARCH_URL"]
index = "noida_python_client"
client = Elasticsearch(url)

info = client.info()
assert info["version"]["number"] == "8.15.3", info
client.options(ignore_status=404).indices.delete(index=index)
created = client.indices.create(index=index)
assert created["acknowledged"] is True, created
written = client.index(index=index, id="book-1", document={"title": "Dune", "pages": 412})
assert written["result"] == "created", written
found = client.get(index=index, id="book-1")
assert found["_source"] == {"title": "Dune", "pages": 412}, found
client.indices.delete(index=index)
print("elasticsearch-py: info/index/get passed")

# --- The flows real applications drive through the client ---------------
from elasticsearch import helpers  # noqa: E402

idx = "noida_python_flows"
client.options(ignore_status=404).indices.delete(index=idx)
client.indices.create(
    index=idx,
    mappings={"properties": {"title": {"type": "text"}, "n": {"type": "integer"},
                             "at": {"type": "date"}, "tag": {"type": "keyword"}}},
)
health = client.cluster.health(index=idx, wait_for_status="yellow", timeout="5s")
assert health["status"] in ("green", "yellow"), health

docs = ({"_index": idx, "_id": str(i), "_source": {
    "title": f"document number {i}" + (" special" if i % 10 == 0 else ""),
    "n": i, "at": f"2024-0{1 + i % 3}-15T00:00:00Z", "tag": "even" if i % 2 == 0 else "odd"}}
    for i in range(250))
ok, errors = helpers.bulk(client, docs, refresh=True)
assert ok == 250 and not errors, (ok, errors)
assert client.count(index=idx)["count"] == 250

# helpers.scan pages with a scroll and clears it at the end.
scanned = sorted(int(h["_id"]) for h in helpers.scan(client, index=idx, size=40, query={"query": {"match_all": {}}}))
assert scanned == list(range(250)), scanned[:5]

# Point in time + search_after, the recommended deep-pagination pattern.
pit = client.open_point_in_time(index=idx, keep_alive="1m")["id"]
seen, after = [], None
while True:
    body = {"size": 100, "sort": [{"n": "asc"}], "pit": {"id": pit, "keep_alive": "1m"}}
    if after:
        body["search_after"] = after
    page = client.search(**body)["hits"]["hits"]
    if not page:
        break
    seen.extend(h["_source"]["n"] for h in page)
    after = page[-1]["sort"]
client.close_point_in_time(id=pit)
assert seen == list(range(250)), (len(seen), seen[:5])

# Scripted update_by_query, then the documents it changed.
r = client.update_by_query(index=idx, refresh=True, query={"term": {"tag": "even"}},
                           script={"source": "ctx._source.n += params.k", "params": {"k": 1000}})
assert r["updated"] == 125, r
assert client.search(index=idx, query={"range": {"n": {"gte": 1000}}}, size=0)["hits"]["total"]["value"] == 125

# Highlighting, query_string, aggregations.
r = client.search(index=idx, query={"query_string": {"query": "special AND tag:even"}},
                  highlight={"fields": {"title": {}}}, size=3)
assert r["hits"]["total"]["value"] == 25, r["hits"]["total"]
assert "<em>special</em>" in r["hits"]["hits"][0]["highlight"]["title"][0]
r = client.search(index=idx, size=0, aggs={"m": {"date_histogram": {"field": "at", "calendar_interval": "month"}}})
assert [b["doc_count"] for b in r["aggregations"]["m"]["buckets"]] == [84, 83, 83], r["aggregations"]

# Scripted single-document update with an upsert.
client.update(index=idx, id="counter", script={"source": "ctx._source.hits += 1"}, upsert={"hits": 0})
client.update(index=idx, id="counter", script={"source": "ctx._source.hits += 1"}, upsert={"hits": 0})
assert client.get(index=idx, id="counter")["_source"] == {"hits": 1}

client.indices.delete(index=idx)
print("elasticsearch-py: bulk/scan/pit/update_by_query/highlight/aggs passed")
