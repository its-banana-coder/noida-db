//! Engine-level Elasticsearch compatibility tests. These call `dispatch`
//! directly so response shapes are pinned independently of HTTP framing.

use serde_json::{Value, json};

use super::engine::Engine;

fn call(engine: &Engine, method: &str, path: &str, body: &str) -> (u16, Value) {
    engine.dispatch(method, path, "", body.as_bytes())
}

#[test]
fn missing_index_error_is_the_elasticsearch_shape_byte_for_byte() {
    let engine = Engine::default();
    let (status, body) = call(&engine, "GET", "/missing", "");

    assert_eq!(status, 404);
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [missing]","index_uuid":"_na_","resource.type":"index_or_alias","resource.id":"missing","index":"missing"}],"type":"index_not_found_exception","reason":"no such index [missing]","index_uuid":"_na_","resource.type":"index_or_alias","resource.id":"missing","index":"missing"},"status":404}"#,
    );
}

#[test]
fn index_and_document_crud_preserve_elasticsearch_response_fields() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    assert_eq!(call(&engine, "HEAD", "/books", "").0, 200);

    let (status, created) =
        call(&engine, "PUT", "/books/_doc/1", r#"{"title":"Dune","pages":412,"published":true}"#);
    assert_eq!(status, 201);
    assert_eq!(created["_index"], "books");
    assert_eq!(created["_id"], "1");
    assert_eq!(created["_version"], 1);
    assert_eq!(created["result"], "created");
    assert_eq!(created["_shards"], json!({"total":2,"successful":1,"failed":0}));

    let (status, fetched) = call(&engine, "GET", "/books/_doc/1", "");
    assert_eq!(status, 200);
    assert_eq!(fetched["found"], true);
    assert_eq!(fetched["_source"], json!({"title":"Dune","pages":412,"published":true}));

    let (status, updated) = call(&engine, "POST", "/books/_update/1", r#"{"doc":{"pages":413}}"#);
    assert_eq!(status, 200);
    assert_eq!(updated["_version"], 2);
    assert_eq!(updated["result"], "updated");

    let (status, deleted) = call(&engine, "DELETE", "/books/_doc/1", "");
    assert_eq!(status, 200);
    assert_eq!(deleted["result"], "deleted");
    assert_eq!(
        call(&engine, "GET", "/books/_doc/1", "").1,
        json!({"_index":"books","_id":"1","found":false,"_source":null})
    );
}

#[test]
fn bulk_and_dynamic_mapping_match_the_milestone_contract() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/events", "").0, 200);
    let body = concat!(
        "{\"index\":{\"_id\":\"one\"}}\n",
        "{\"message\":\"hello\",\"count\":2,\"active\":true,\"meta\":{\"source\":\"test\"}}\n",
        "{\"create\":{\"_id\":\"two\"}}\n",
        "{\"message\":\"world\"}\n",
    );
    let (status, bulk) = call(&engine, "POST", "/events/_bulk", body);
    assert_eq!(status, 200);
    assert_eq!(bulk["errors"], false);
    assert_eq!(bulk["items"][0]["index"]["status"], 201);
    assert_eq!(bulk["items"][1]["create"]["status"], 201);

    let (status, mapping) = call(&engine, "GET", "/events/_mapping", "");
    assert_eq!(status, 200);
    let properties = &mapping["events"]["mappings"]["properties"];
    assert_eq!(properties["message"]["type"], "text");
    assert_eq!(
        properties["message"]["fields"]["keyword"],
        json!({"type":"keyword","ignore_above":256})
    );
    assert_eq!(properties["count"]["type"], "long");
    assert_eq!(properties["active"]["type"], "boolean");
    assert_eq!(properties["meta"]["type"], "object");
}

#[test]
fn explicit_mapping_and_settings_round_trip() {
    let engine = Engine::default();
    let create = r#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}},"mappings":{"properties":{"isbn":{"type":"keyword"}}}}"#;
    assert_eq!(call(&engine, "PUT", "/catalog", create).0, 200);
    assert_eq!(
        call(&engine, "PUT", "/catalog/_mapping", r#"{"properties":{"title":{"type":"text"}}}"#).0,
        200
    );
    assert_eq!(
        call(&engine, "PUT", "/catalog/_settings", r#"{"index":{"refresh_interval":"5s"}}"#).0,
        200
    );

    let (_, mapping) = call(&engine, "GET", "/catalog/_mapping", "");
    assert_eq!(mapping["catalog"]["mappings"]["properties"]["isbn"]["type"], "keyword");
    assert_eq!(mapping["catalog"]["mappings"]["properties"]["title"]["type"], "text");
    let (_, settings) = call(&engine, "GET", "/catalog/_settings", "");
    assert_eq!(settings["catalog"]["settings"]["index"]["refresh_interval"], "5s");
}

#[test]
fn search_is_near_real_time_until_refresh() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_doc/1", r#"{"title":"Dune"}"#);

    // Not yet refreshed: the doc exists (real-time GET) but isn't searchable.
    assert_eq!(call(&engine, "GET", "/books/_doc/1", "").0, 200);
    let (_, before) = call(&engine, "POST", "/books/_search", r#"{"query":{"match_all":{}}}"#);
    assert_eq!(before["hits"]["total"]["value"], 0);

    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);
    let (_, after) = call(&engine, "POST", "/books/_search", r#"{"query":{"match_all":{}}}"#);
    assert_eq!(after["hits"]["total"]["value"], 1);
    assert_eq!(after["hits"]["hits"][0]["_id"], "1");
    assert_eq!(after["hits"]["hits"][0]["_source"], json!({"title":"Dune"}));
}

#[test]
fn write_with_refresh_true_is_immediately_searchable() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    engine.dispatch("PUT", "/books/_doc/1", "refresh=true", br#"{"title":"Dune"}"#);
    let (_, resp) = call(&engine, "GET", "/books/_search", "");
    assert_eq!(resp["hits"]["total"]["value"], 1);
}

#[test]
fn search_ranks_by_bm25_and_supports_bool_range_and_count() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_doc/1", r#"{"title":"A quick fox","pages":100,"tag":"a"}"#);
    call(
        &engine,
        "PUT",
        "/books/_doc/2",
        r#"{"title":"quick quick fox fox","pages":300,"tag":"a"}"#,
    );
    call(&engine, "PUT", "/books/_doc/3", r#"{"title":"an unrelated book","pages":50,"tag":"b"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (_, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"match":{"title":"quick"}}}"#);
    let hits = resp["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0]["_id"], "2");
    assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());

    let (_, resp) = call(
        &engine,
        "POST",
        "/books/_search",
        r#"{"query":{"bool":{"filter":[{"term":{"tag":"a"}}],"must_not":[{"range":{"pages":{"gt":200}}}]}}}"#,
    );
    assert_eq!(resp["hits"]["total"]["value"], 1);
    assert_eq!(resp["hits"]["hits"][0]["_id"], "1");

    let (status, resp) =
        call(&engine, "POST", "/books/_count", r#"{"query":{"exists":{"field":"tag"}}}"#);
    assert_eq!(status, 200);
    assert_eq!(resp["count"], 3);
}

#[test]
fn analyze_uses_the_standard_analyzer_by_default() {
    let engine = Engine::default();
    let (status, resp) = call(&engine, "POST", "/_analyze", r#"{"text":"The Quick Fox!"}"#);
    assert_eq!(status, 200);
    let tokens: Vec<&str> =
        resp["tokens"].as_array().unwrap().iter().map(|t| t["token"].as_str().unwrap()).collect();
    assert_eq!(tokens, vec!["the", "quick", "fox"]);
}

fn ids(resp: &Value) -> Vec<String> {
    resp["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_id"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn match_phrase_requires_consecutive_terms_in_order() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_doc/1", r#"{"body":"the quick brown fox jumps"}"#);
    call(&engine, "PUT", "/books/_doc/2", r#"{"body":"a fox that is quick and brown"}"#);
    call(&engine, "PUT", "/books/_doc/3", r#"{"body":"quick brown fox"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (status, resp) = call(
        &engine,
        "POST",
        "/books/_search",
        r#"{"query":{"match_phrase":{"body":"quick brown fox"}}}"#,
    );
    assert_eq!(status, 200);
    let mut got = ids(&resp);
    got.sort();
    assert_eq!(got, vec!["1", "3"]);

    // Same terms, wrong order/non-adjacent — must not match doc 2.
    let (_, resp) = call(
        &engine,
        "POST",
        "/books/_search",
        r#"{"query":{"match_phrase":{"body":"fox quick brown"}}}"#,
    );
    assert_eq!(resp["hits"]["total"]["value"], 0);
}

#[test]
fn multi_match_best_fields_scores_by_the_best_field() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    // doc 1: term only in title. doc 2: term repeated in body (higher BM25
    // there than doc 1's single title hit), plus once in title.
    call(&engine, "PUT", "/books/_doc/1", r#"{"title":"rust programming","body":"a book"}"#);
    call(
        &engine,
        "PUT",
        "/books/_doc/2",
        r#"{"title":"a book","body":"rust rust rust systems programming"}"#,
    );
    call(&engine, "PUT", "/books/_doc/3", r#"{"title":"cooking","body":"no matches here"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (status, resp) = call(
        &engine,
        "POST",
        "/books/_search",
        r#"{"query":{"multi_match":{"query":"rust","fields":["title","body"]}}}"#,
    );
    assert_eq!(status, 200);
    let mut got = ids(&resp);
    got.sort();
    assert_eq!(got, vec!["1", "2"]);
    // doc 2's best field (body, tf=3) should outscore doc 1's best field
    // (title, tf=1).
    let hits = resp["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits[0]["_id"], "2");
    assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());
}

#[test]
fn multi_match_operator_and_requires_every_term_in_the_same_field() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_doc/1", r#"{"title":"quick fox","body":"nothing"}"#);
    call(&engine, "PUT", "/books/_doc/2", r#"{"title":"quick","body":"a fox runs"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (_, resp) = call(
        &engine,
        "POST",
        "/books/_search",
        r#"{"query":{"multi_match":{"query":"quick fox","fields":["title","body"],"operator":"and"}}}"#,
    );
    // Only doc 1 has both terms in a single field ("title"); doc 2 has
    // "quick" in title and "fox" in body but neither field alone has both.
    assert_eq!(ids(&resp), vec!["1"]);
}

#[test]
fn wildcard_query_matches_glob_patterns_on_keyword_fields() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_mapping", r#"{"properties":{"sku":{"type":"keyword"}}}"#);
    call(&engine, "PUT", "/books/_doc/1", r#"{"sku":"BOOK-1234"}"#);
    call(&engine, "PUT", "/books/_doc/2", r#"{"sku":"BOOK-5678"}"#);
    call(&engine, "PUT", "/books/_doc/3", r#"{"sku":"DISC-1234"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (status, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"wildcard":{"sku":"BOOK-*"}}}"#);
    assert_eq!(status, 200);
    let mut got = ids(&resp);
    got.sort();
    assert_eq!(got, vec!["1", "2"]);
    // Constant score, like the other structured queries.
    assert!(resp["hits"]["hits"].as_array().unwrap().iter().all(|h| h["_score"] == 1.0));

    let (_, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"wildcard":{"sku":"BOOK-1?34"}}}"#);
    assert_eq!(ids(&resp), vec!["1"]);

    let (_, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"wildcard":{"sku":"*-1234"}}}"#);
    let mut got = ids(&resp);
    got.sort();
    assert_eq!(got, vec!["1", "3"]);
}

#[test]
fn regexp_query_matches_on_keyword_fields() {
    let engine = Engine::default();
    assert_eq!(call(&engine, "PUT", "/books", "").0, 200);
    call(&engine, "PUT", "/books/_mapping", r#"{"properties":{"sku":{"type":"keyword"}}}"#);
    call(&engine, "PUT", "/books/_doc/1", r#"{"sku":"AB-100"}"#);
    call(&engine, "PUT", "/books/_doc/2", r#"{"sku":"AB-250"}"#);
    call(&engine, "PUT", "/books/_doc/3", r#"{"sku":"CD-100"}"#);
    assert_eq!(call(&engine, "POST", "/books/_refresh", "").0, 200);

    let (status, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"regexp":{"sku":"AB-[0-9]+"}}}"#);
    assert_eq!(status, 200);
    let mut got = ids(&resp);
    got.sort();
    assert_eq!(got, vec!["1", "2"]);

    // Elasticsearch's regexp anchors the whole term: this pattern must not
    // match "AB-100" as a substring of something longer.
    let (_, resp) =
        call(&engine, "POST", "/books/_search", r#"{"query":{"regexp":{"sku":"AB-1[0-9]{2}"}}}"#);
    assert_eq!(ids(&resp), vec!["1"]);
}

/// Found via extensive real-client testing before a public release: an
/// unrecognized query clause (`query_string`, `nested`, `fuzzy`,
/// `function_score`, ...) used to silently fall through to "0 hits" --
/// a perfectly well-formed, confidently WRONG successful response rather
/// than an error. Violates this project's own stated principle (see
/// docs/specs/README.md: "never silently wrong"). Fixed to return a real
/// 400 instead.
#[test]
fn unsupported_query_type_errors_instead_of_silently_matching_nothing() {
    let engine = Engine::default();
    call(&engine, "PUT", "/docs", "");
    call(&engine, "PUT", "/docs/_doc/1", r#"{"name":"alice"}"#);
    call(&engine, "POST", "/docs/_refresh", "");

    let (status, resp) =
        call(&engine, "POST", "/docs/_search", r#"{"query":{"query_string":{"query":"alice"}}}"#);
    assert_eq!(status, 400);
    assert_eq!(resp["error"]["type"], "parsing_exception");

    let (status, _) =
        call(&engine, "POST", "/docs/_count", r#"{"query":{"query_string":{"query":"alice"}}}"#);
    assert_eq!(status, 400);
}

/// Real Elasticsearch auto-creates an index on its first write
/// (`action.auto_create_index`, on by default) -- found missing via
/// testing before a public release, confirmed to affect both a single
/// document `PUT` and `_bulk`. `GET`/`DELETE` against a genuinely missing
/// index must still 404, unaffected by this fix.
#[test]
fn put_doc_auto_creates_a_missing_index() {
    let engine = Engine::default();
    let (status, resp) = call(&engine, "PUT", "/newindex/_doc/1", r#"{"name":"alice"}"#);
    assert_eq!(status, 201);
    assert_eq!(resp["result"], "created");

    let (status, _) = call(&engine, "GET", "/newindex/_doc/1", "");
    assert_eq!(status, 200);

    // A real miss (GET on a genuinely never-written index) still 404s.
    let (status, _) = call(&engine, "GET", "/nevercreated/_doc/1", "");
    assert_eq!(status, 404);
}

#[test]
fn bulk_auto_creates_a_missing_index() {
    let engine = Engine::default();
    let body = "{\"index\":{\"_index\":\"bulked\",\"_id\":\"1\"}}\n{\"name\":\"a\"}\n";
    let (status, resp) = call(&engine, "POST", "/bulked/_bulk", body);
    assert_eq!(status, 200);
    assert_eq!(resp["errors"], false);
}

/// The global `/_bulk` endpoint (no index in the URL, `_index` set per
/// action) wasn't routed at all before this fix -- confirmed missing via
/// testing before a public release. This is the form most real
/// bulk-ingestion tooling actually uses.
#[test]
fn global_bulk_endpoint_is_routed() {
    let engine = Engine::default();
    let body = concat!(
        "{\"index\":{\"_index\":\"globalbulk\",\"_id\":\"1\"}}\n",
        "{\"name\":\"a\"}\n",
        "{\"index\":{\"_index\":\"globalbulk\",\"_id\":\"2\"}}\n",
        "{\"name\":\"b\"}\n",
    );
    let (status, resp) = call(&engine, "POST", "/_bulk", body);
    assert_eq!(status, 200);
    assert_eq!(resp["errors"], false);
    assert_eq!(resp["items"].as_array().unwrap().len(), 2);

    let (status, _) = call(&engine, "GET", "/globalbulk/_doc/1", "");
    assert_eq!(status, 200);
}

/// Silently-wrong behavior found by testing the official Python client
/// before a public release.
#[test]
fn arrays_multi_fields_and_document_apis_behave_like_elasticsearch() {
    let engine = Engine::default();
    let q = |method: &str, path: &str, query: &str, body: &str| {
        engine.dispatch(method, path, query, body.as_bytes())
    };
    q(
        "PUT",
        "/p",
        "",
        r#"{"mappings":{"properties":{"name":{"type":"text","fields":{"raw":{"type":"keyword"}}},"tags":{"type":"keyword"},"price":{"type":"float"}}}}"#,
    );
    let bulk = "{\"index\":{\"_index\":\"p\",\"_id\":\"1\"}}\n{\"name\":\"Dell XPS\",\"tags\":[\"laptop\",\"win\"],\"price\":1200}\n\
                {\"index\":{\"_index\":\"p\",\"_id\":\"2\"}}\n{\"name\":\"Mac Pro\",\"tags\":[\"laptop\"],\"price\":2500}\n\
                {\"update\":{\"_index\":\"p\",\"_id\":\"2\"}}\n{\"doc\":{\"price\":2400}}\n";
    let (_, r) = q("POST", "/_bulk", "refresh=true", bulk);
    assert_eq!(r["errors"], false);
    // Bulk `update` merges; it used to store `{"doc": ...}` as the document.
    let (_, d) = q("GET", "/p/_doc/2", "", "");
    assert_eq!(d["_source"], json!({"name":"Mac Pro","tags":["laptop"],"price":2400}));

    // Array fields match per element (used to never match).
    let (_, r) = q("POST", "/p/_count", "", r#"{"query":{"term":{"tags":"win"}}}"#);
    assert_eq!(r["count"], 1);
    let (_, r) =
        q("POST", "/p/_search", "", r#"{"size":0,"aggs":{"t":{"terms":{"field":"tags"}}}}"#);
    assert_eq!(r["aggregations"]["t"]["buckets"][0], json!({"key":"laptop","doc_count":2}));

    // Multi-field sub-fields resolve to their parent's values.
    let (_, r) = q("POST", "/p/_count", "", r#"{"query":{"term":{"name.raw":"Dell XPS"}}}"#);
    assert_eq!(r["count"], 1);

    // Range buckets are always keyed; filter-only bool scores 0.
    let (_, r) = q(
        "POST",
        "/p/_search",
        "",
        r#"{"size":0,"aggs":{"r":{"range":{"field":"price","ranges":[{"to":2000}]}}}}"#,
    );
    assert_eq!(r["aggregations"]["r"]["buckets"][0]["key"], "*-2000.0");
    let (_, r) =
        q("POST", "/p/_search", "", r#"{"query":{"bool":{"filter":{"term":{"tags":"win"}}}}}"#);
    assert_eq!(r["hits"]["hits"][0]["_score"], 0.0);

    // `_source_includes` URL parameter, `_mget` ids shorthand, mapping shape.
    let (_, r) =
        q("POST", "/p/_search", "_source_includes=price", r#"{"query":{"ids":{"values":["1"]}}}"#);
    assert_eq!(r["hits"]["hits"][0]["_source"], json!({"price":1200}));
    let (_, r) = q("POST", "/p/_mget", "", r#"{"ids":["1","nope"]}"#);
    assert_eq!(r["docs"][0]["found"], true);
    assert_eq!(r["docs"][1]["found"], false);
    let (_, r) = q("GET", "/p/_mapping", "", "");
    assert_eq!(r["p"]["mappings"]["properties"]["tags"]["type"], "keyword");

    // _delete_by_query, per-index alias routes, templates on auto-create.
    let (_, r) =
        q("POST", "/p/_delete_by_query", "refresh=true", r#"{"query":{"term":{"tags":"win"}}}"#);
    assert_eq!(r["deleted"], 1);
    assert_eq!(q("PUT", "/p/_alias/shop", "", "").0, 200);
    let (_, r) = q("POST", "/shop/_count", "", "");
    assert_eq!(r["count"], 1);
    q(
        "PUT",
        "/_index_template/logs",
        "",
        r#"{"index_patterns":["logs-*"],"template":{"mappings":{"properties":{"level":{"type":"keyword"}}}}}"#,
    );
    q("POST", "/logs-1/_doc", "", r#"{"level":"WARN","at":"2024-01-01T10:00:00Z"}"#);
    let (_, r) = q("GET", "/logs-1/_mapping", "", "");
    assert_eq!(r["logs-1"]["mappings"]["properties"]["level"]["type"], "keyword");
    assert_eq!(r["logs-1"]["mappings"]["properties"]["at"]["type"], "date");
}
