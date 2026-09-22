# Elasticsearch: noida spec

- **Module:** `src/elasticsearch/`, Cargo feature `elasticsearch`, branch
  `svc/elasticsearch`
- **Port:** 9200 (HTTP/1.1, JSON)
- **Target:** Elasticsearch 8.15 with security disabled
  (`xpack.security.enabled=false`), the usual local-dev setup
- **Reference server:** not installed locally (no Docker). CI uses
  `docker.elastic.co/elasticsearch/elasticsearch:8.15.3` with
  `discovery.type=single-node`, `xpack.security.enabled=false`,
  `ES_JAVA_OPTS=-Xms512m -Xmx512m`. Env var:
  `NOIDA_ELASTICSEARCH_REF=http://host:port`.

## 1. Purpose

Apps using the official clients (Java API client, elasticsearch-py,
@elastic/elasticsearch, Go, .NET), Spring Data Elasticsearch, and tools
that speak the REST API (curl scripts, Logstash-style bulk loaders) work
unchanged. Elasticsearch idles at 1–2 GB; this is one of noida's biggest
wins.

## 2. HTTP layer (P0)

- HTTP/1.1 with keep-alive and chunked request bodies; gzip request bodies
  (`Content-Encoding: gzip`) and gzip responses when requested.
- **Every response carries `X-Elastic-Product: Elasticsearch`** (8.x clients
  refuse to work without it).
- Accept `application/json`, `application/x-ndjson`, and
  `application/vnd.elasticsearch+json; compatible-with=8` (and 7 for the
  compatibility headers some clients send); reply with the matching content
  type.
- Query parameters common to all APIs: `pretty`, `human`, `filter_path`,
  `error_trace`, `format` (for `_cat`), `timeout`, `master_timeout`.
- Errors use Elasticsearch's exact JSON shape and status codes:
  `{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no
  such index [x]","index_uuid":"_na_","resource.type":"index_or_alias",
  "resource.id":"x","index":"x"}],"type":…,"reason":…,…},"status":404}`,
  plus `version_conflict_engine_exception` (409),
  `resource_already_exists_exception` (400), `mapper_parsing_exception`,
  `parsing_exception`, `illegal_argument_exception`,
  `document_missing_exception`, `x_content_parse_exception`.
- A HTTP framework is allowed but must be small; a hand-written HTTP/1.1
  server on threads is fine (performance is not a goal).

## 3. APIs

### P0
- `GET /` (name, cluster_name "docker-cluster", cluster_uuid, version
  {number "8.15.3", build_flavor "default", build_type "docker",
  lucene_version, minimum_wire_compatibility_version,
  minimum_index_compatibility_version}, tagline "You Know, for Search"),
  `HEAD /`.
- Index management: `PUT /{index}` (settings incl. number_of_shards/replicas
  accepted and reported, analysis settings, mappings, aliases), `DELETE`,
  `HEAD`, `GET /{index}`, `GET/PUT /{index}/_mapping`, `GET/PUT
  /{index}/_settings`, `POST /{index}/_refresh`, `_flush` (no-op),
  `_open`/`_close`, index name validation rules and wildcards/comma lists,
  `GET /_alias`, `POST /_aliases`, `PUT /{index}/_alias/{name}`.
- Index templates: `PUT/GET/DELETE /_index_template/{name}` and component
  templates (Spring Data, Logstash and ILM-style setups create them); legacy
  `_template` (P1).
- Documents: `PUT/POST /{index}/_doc/{id}`, `POST /{index}/_doc`
  (auto id: base64url 20 chars like ES), `PUT /{index}/_create/{id}`,
  `GET/HEAD/DELETE /{index}/_doc/{id}`, `GET /{index}/_source/{id}`,
  `POST /{index}/_update/{id}` (partial doc, `doc_as_upsert`, `upsert`,
  `detect_noop`; scripted updates → P2), `_mget`, `_bulk` (NDJSON, all four
  actions, per-item results and `errors` flag, exactly like ES), query
  params `refresh` (true/false/wait_for), `routing` (accepted),
  `version`/`version_type`, `if_seq_no`/`if_primary_term` optimistic
  concurrency, `op_type`, `_source` includes/excludes on GET.
  Responses include `_index`, `_id`, `_version`, `result`, `_shards`
  {total 2, successful 1, failed 0 — as a 1-node cluster with 1 replica
  configured reports}, `_seq_no`, `_primary_term`.
- Dynamic mapping exactly as ES: strings → `text` with a `keyword`
  sub-field (`ignore_above: 256`); integers → `long`; floats → `float`;
  booleans → `boolean`; objects → `object`; date detection
  (`strict_date_optional_time||epoch_millis`) for strings that look like
  dates; `dynamic: strict/false/true/runtime` honoured; mapping conflicts
  rejected with ES's errors.
- Field types: text, keyword, long/integer/short/byte, double/float/
  half_float/scaled_float, boolean, date (formats), date_nanos, object,
  nested (P1 queries), flattened (P1), ip, binary, geo_point (P2 queries),
  dense_vector (P2).
- `_search` / `_count` (GET and POST, on index, comma list, wildcard,
  alias, or all):
  - Query DSL: `match_all`, `match_none`, `match` (operator, fuzziness P1,
    minimum_should_match), `match_phrase`, `match_phrase_prefix`,
    `multi_match` (best_fields, most_fields, cross_fields P1, phrase),
    `term`, `terms`, `range` (incl. date math like `now-1d/d`), `exists`,
    `prefix`, `wildcard`, `regexp` (P1), `ids`, `bool` (must, should,
    must_not, filter, minimum_should_match), `constant_score`,
    `query_string` and `simple_query_string` (common syntax), `nested` (P1),
    `function_score` (P2).
  - `from`/`size` (max_result_window 10000 → same error), `sort` (fields,
    `_score`, `_doc`, missing, order, mode), `search_after`, `_source`
    filtering, `fields`, `docvalue_fields`, `stored_fields`,
    `track_total_hits` (default: exact up to 10000 with `relation`),
    `highlight` (plain highlighter on text fields: pre/post tags,
    fragment_size, number_of_fragments), `min_score`, `explain: false`
    only.
  - **Aggregations:** `terms` (size, order, min_doc_count, missing,
    `doc_count_error_upper_bound` 0, `sum_other_doc_count`), `date_histogram`
    (calendar_interval/fixed_interval, format, time_zone, min_doc_count,
    extended_bounds), `histogram`, `range`, `date_range`, `filter`,
    `filters`, `missing`, `avg`, `sum`, `min`, `max`, `stats`,
    `extended_stats` (P1), `value_count`, `cardinality` (exact for local
    data sizes; values must equal ES's for the same data where ES is exact),
    `percentiles` (P1: ES uses TDigest — document the difference or port
    it), `top_hits`, nested sub-aggregations, `size: 0` searches.
  - Scroll (`scroll=1m`, `_search/scroll`, clear scroll) and point in time
    (`POST /{index}/_pit`, `pit` in search, `DELETE /_pit`).
- `_cluster/health` (status green for indices with 0 replicas, yellow when
  replicas > 0, like a single node), `_cluster/state` (minimal),
  `_nodes` (one node), `_cat/indices`, `_cat/health`, `_cat/count`,
  `_cat/aliases`, `_cat/nodes` (text table output with `v`, `h`, `format`).
- `_analyze` with the built-in analyzers below.

### Text analysis (P0)
Analyzers `standard` (Unicode word segmentation per UAX#29 + lowercase;
must tokenize like Lucene's StandardTokenizer for the common cases),
`simple`, `whitespace`, `keyword`, `stop`, and `english` (P1, Porter-style
stemming as Lucene's EnglishAnalyzer). Custom analyzers built from
tokenizers `standard`, `whitespace`, `keyword`, `letter`, `ngram`,
`edge_ngram` (P1) and filters `lowercase`, `uppercase`, `asciifolding`,
`stop`, `trim`, `synonym` (P1), `stemmer` (P1). Normalizers for keyword
fields (lowercase, asciifolding).

### Relevance (P0)
BM25 with ES defaults (k1 = 1.2, b = 0.75) computed the way Lucene does,
**including Lucene's lossy norm encoding of field length** (SmallFloat
`intToByte4`) and 32-bit float arithmetic, on a single shard, so `_score`
values and ranking match real ES for the same data. The differential tests
compare scores to a small tolerance (1e-5 relative) and ranking exactly.
Real ES spreads documents over shards only if `number_of_shards > 1`; the
CI reference indices use 1 shard.

### P1
`_update_by_query`, `_delete_by_query`, `_reindex` (local), `_msearch`,
`_mtermvectors`/`_termvectors`, `_field_caps`, `_validate/query`, `_explain`
(minimal), runtime fields, `_search/template` with mustache (simple),
`_sql` (P2), ILM/data streams APIs (`_data_stream`, `_ilm/policy` accepted
with minimal behaviour), `_snapshot` (P2: accept, error on create).

## 4. Storage

Per index: stored `_source` documents (in insertion/`_id` order), an
in-memory inverted index per text field (postings with term frequencies
and positions for phrases), doc values for keyword/numeric/date fields for
sorting and aggregations. Near-real-time semantics: documents become
searchable on refresh (`refresh_interval` default 1s; `refresh=true`/
`wait_for` honoured), while GET by id is real-time — tests depend on both.
One lock. In-memory acceptable for the first milestone; persistence to the
data dir follows.

Dependency note: Tantivy would add ~10–15MB and doesn't reproduce Lucene's
exact scoring; the default expectation is a small purpose-built index.
Justify any alternative with size and score-compatibility numbers.

## 5. Client matrix

Scenario: create index with explicit mapping, bulk index 1000 docs, get by
id, update, delete, search with bool + range + match, sort + search_after
pagination, terms + date_histogram aggregations, highlight, scroll, a
version conflict error, index template + alias.

| Client | How to run |
|---|---|
| Rust (`reqwest` blocking or raw HTTP in tests) | `tests/elasticsearch_client.rs` |
| elasticsearch-py 8.x | pip |
| @elastic/elasticsearch 8.x | npm |
| Java API client 8.x (+ Jackson) | jars from Maven Central, `javac` |
| Spring Data Elasticsearch, Go client | P1, CI |

Commit test apps under `tests/clients/elasticsearch/` with a runner script.

## 6. Differential tests

`tests/elasticsearch_diff.rs` sends the same HTTP requests to real ES and
noida and compares status codes and JSON bodies, normalizing only `took`,
`_shards` timing details, auto-generated `_id`s, `cluster_uuid`, node
names/ids, and build hashes; `_score` compared with tolerance. Cover every
P0 API, query type and aggregation, and the error shapes. CI: the ES 8.15
service. Print the number of compared requests.

## 7. Non-goals

Clustering/shards/replication behaviour beyond reporting, security
(xpack auth, API keys, TLS) — requests with credentials are accepted and
ignored like an unsecured node, ML, Kibana, Painless scripting (scripted
requests get ES's error for disabled scripting), percolator, cross-cluster
search, snapshots to real repositories, searchable snapshots, ingest
pipelines (P2: accept `pipeline` param only if the pipeline exists; support
simple processors later), `_nodes/stats`/`_stats` performance metrics
(minimal shape only).

## 8. Milestones

1. HTTP layer, `GET /`, index CRUD, document CRUD, `_bulk`, dynamic mapping;
   the Python and Node clients' `info()`/index/get pass.
2. Analysis (standard analyzer) + inverted index + `match`/`term`/`bool`/
   `range` + BM25 scores matching ES; diff suite for these.
3. Sorting, pagination, search_after, highlight, aggregations P0.
4. Scroll/PIT, templates, aliases, `_cat`/`_cluster`; client matrix green;
   `elasticsearch` in default features.
5. P1 items.
