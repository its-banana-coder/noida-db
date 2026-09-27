//! SORT and SORT_RO (Redis 7.2 semantics, from sort.c).

use super::*;

const NOT_DOUBLE: &str = "ERR One or more scores can't be converted into double";

/// Three ids with weights and names, the classic example.
fn setup(t: &mut T) {
    t.run("RPUSH ids 1 2 3");
    t.run("MSET w_1 3 w_2 1 w_3 2 o_1 one o_2 two o_3 three");
}

#[test]
fn sorts_numerically_by_default() {
    let mut t = T::new();
    t.run("RPUSH l 3 1 2");
    assert_eq!(t.run("SORT l"), bulks(&["1", "2", "3"]));
    assert_eq!(t.run("SORT l DESC"), bulks(&["3", "2", "1"]));
    assert_eq!(t.run("SORT l ASC DESC ASC"), bulks(&["1", "2", "3"]));
    assert_eq!(t.run("SORT nokey"), arr(vec![]));
}

#[test]
fn works_on_lists_sets_and_sorted_sets_only() {
    let mut t = T::new();
    t.run("SADD st 3 1 2");
    t.run("ZADD z 9 3 8 1 7 2");
    assert_eq!(t.run("SORT st"), bulks(&["1", "2", "3"]));
    // Sorted-set members sort as values, not by score.
    assert_eq!(t.run("SORT z"), bulks(&["1", "2", "3"]));
    t.run("SET s v");
    t.run("HSET h a 1");
    for key in ["s", "h"] {
        assert_eq!(
            t.run(&format!("SORT {key}")),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}

#[test]
fn alpha_sorts_bytes_and_numeric_sort_rejects_text() {
    let mut t = T::new();
    t.run("RPUSH s b a c B");
    assert_eq!(t.run("SORT s ALPHA"), bulks(&["B", "a", "b", "c"]));
    assert_eq!(t.run("SORT s ALPHA DESC"), bulks(&["c", "b", "a", "B"]));
    assert_eq!(t.run("SORT s"), err(NOT_DOUBLE));
}

#[test]
fn numbers_are_parsed_like_strtod() {
    let mut t = T::new();
    t.run("RPUSH m 10 \" 5\" 1e2 inf -inf 0x10 \"\"");
    assert_eq!(
        t.run("SORT m"),
        bulks(&["-inf", "", " 5", "10", "0x10", "1e2", "inf"]),
        "an empty string is 0, leading spaces and hex are accepted"
    );
    for bad in ["5x", "\"5 \"", "nan", "1.2.3", "e5"] {
        t.run("DEL bad");
        t.run(&format!("RPUSH bad 1 {bad}"));
        assert_eq!(t.run("SORT bad"), err(NOT_DOUBLE), "{bad}");
    }
}

#[test]
fn equal_scores_fall_back_to_a_byte_comparison() {
    let mut t = T::new();
    t.run("RPUSH t 1 1.0 01");
    assert_eq!(t.run("SORT t"), bulks(&["01", "1", "1.0"]));
    assert_eq!(t.run("SORT t DESC"), bulks(&["1.0", "1", "01"]));
}

#[test]
fn limit_windows_the_result() {
    let mut t = T::new();
    t.run("RPUSH n 1 2 3 4 5");
    assert_eq!(t.run("SORT n LIMIT 1 2"), bulks(&["2", "3"]));
    assert_eq!(t.run("SORT n LIMIT 0 -1"), bulks(&["1", "2", "3", "4", "5"]));
    assert_eq!(t.run("SORT n LIMIT -1 2"), bulks(&["1", "2"]));
    assert_eq!(t.run("SORT n LIMIT 3 100"), bulks(&["4", "5"]));
    assert_eq!(t.run("SORT n LIMIT 10 2"), arr(vec![]));
    assert_eq!(t.run("SORT n LIMIT 1 0"), arr(vec![]));
    assert_eq!(t.run("SORT n DESC LIMIT 0 2"), bulks(&["5", "4"]));
}

#[test]
fn by_a_pattern_and_get_patterns() {
    let mut t = T::new();
    setup(&mut t);
    assert_eq!(t.run("SORT ids BY w_*"), bulks(&["2", "3", "1"]));
    assert_eq!(t.run("SORT ids BY w_* DESC"), bulks(&["1", "3", "2"]));
    assert_eq!(t.run("SORT ids BY w_* GET o_*"), bulks(&["two", "three", "one"]));
    assert_eq!(
        t.run("SORT ids BY w_* GET # GET o_*"),
        bulks(&["2", "two", "3", "three", "1", "one"])
    );
    // A GET on a missing key gives nil; a pattern without '*' always does.
    assert_eq!(t.run("SORT ids BY w_* GET x_*"), arr(vec![nil(), nil(), nil()]));
    assert_eq!(t.run("SORT ids GET fixed"), arr(vec![nil(), nil(), nil()]));
    assert_eq!(t.run("SORT ids BY w_* LIMIT 1 1 GET o_*"), bulks(&["three"]));
}

#[test]
fn patterns_can_reach_into_hashes() {
    let mut t = T::new();
    t.run("RPUSH ids 1 2 3");
    t.run("HSET u_1 rank 30 name ann");
    t.run("HSET u_2 rank 10 name bob");
    t.run("HSET u_3 rank 20 name cy");
    assert_eq!(t.run("SORT ids BY u_*->rank"), bulks(&["2", "3", "1"]));
    assert_eq!(t.run("SORT ids BY u_*->rank GET u_*->name"), bulks(&["bob", "cy", "ann"]));
    // A field on a missing hash or a non-hash gives nil.
    t.run("SET u_4 plain");
    t.run("RPUSH more 4 5");
    assert_eq!(t.run("SORT more GET u_*->name"), arr(vec![nil(), nil()]));
    // "->" at the very end is part of the key name, not a field reference.
    t.run("SET k_1-> tail");
    t.run("RPUSH one 1");
    assert_eq!(t.run("SORT one GET k_*->"), bulks(&["tail"]));
}

#[test]
fn missing_by_values_count_as_zero_or_sort_first() {
    let mut t = T::new();
    t.run("RPUSH ids 1 2 3");
    t.run("MSET w_1 5 w_3 -1");
    assert_eq!(t.run("SORT ids BY w_*"), bulks(&["3", "2", "1"]), "w_2 is missing: score 0");
    t.run("MSET a_1 b a_3 a");
    assert_eq!(t.run("SORT ids BY a_* ALPHA"), bulks(&["2", "3", "1"]), "no value sorts first");
}

#[test]
fn by_nosort_keeps_the_natural_order() {
    let mut t = T::new();
    t.run("RPUSH l 3 1 2");
    t.run("ZADD z 1 a 2 b 3 c");
    assert_eq!(t.run("SORT l BY nosort"), bulks(&["3", "1", "2"]));
    assert_eq!(t.run("SORT l BY nosort DESC"), bulks(&["2", "1", "3"]));
    assert_eq!(t.run("SORT l BY nosort LIMIT 1 1"), bulks(&["1"]));
    assert_eq!(t.run("SORT z BY nosort"), bulks(&["a", "b", "c"]));
    assert_eq!(t.run("SORT z BY nosort DESC"), bulks(&["c", "b", "a"]));
    assert_eq!(t.run("SORT z BY nosort LIMIT 1 1"), bulks(&["b"]));
    assert_eq!(t.run("SORT z BY nosort DESC LIMIT 0 2"), bulks(&["c", "b"]));
}

#[test]
fn sets_with_nosort_are_forced_alpha_when_stored_or_in_scripts() {
    let mut t = T::new();
    t.run("SADD st 10 9 100");
    // The set's own order (integers ascend)...
    assert_eq!(t.run("SORT st BY nosort"), bulks(&["9", "10", "100"]));
    // ...but stored, or called from a script, it is sorted as text so the
    // result is repeatable.
    assert_eq!(t.run("SORT st BY nosort STORE out"), int(3));
    assert_eq!(t.run("LRANGE out 0 -1"), bulks(&["10", "100", "9"]));
    assert_eq!(
        t.run("EVAL \"return redis.call('sort','st','by','nosort')\" 0"),
        bulks(&["10", "100", "9"])
    );
}

#[test]
fn store_writes_a_list_and_replies_with_its_length() {
    let mut t = T::new();
    setup(&mut t);
    t.run("SET out old");
    assert_eq!(t.run("SORT ids BY w_* STORE out"), int(3));
    assert_eq!(t.run("TYPE out"), simple("list"));
    assert_eq!(t.run("LRANGE out 0 -1"), bulks(&["2", "3", "1"]));
    // With GET, missing values are stored as empty strings.
    assert_eq!(t.run("SORT ids BY w_* GET o_* GET x_* STORE out2"), int(6));
    assert_eq!(t.run("LRANGE out2 0 -1"), bulks(&["two", "", "three", "", "one", ""]));
    // An empty result deletes the destination.
    assert_eq!(t.run("SORT nokey STORE out"), int(0));
    assert_eq!(t.run("EXISTS out"), int(0));
    // STORE may write over the source.
    assert_eq!(t.run("SORT ids DESC STORE ids"), int(3));
    assert_eq!(t.run("LRANGE ids 0 -1"), bulks(&["3", "2", "1"]));
}

#[test]
fn sort_ro_reads_but_never_stores() {
    let mut t = T::new();
    t.run("RPUSH l 2 1");
    assert_eq!(t.run("SORT_RO l"), bulks(&["1", "2"]));
    assert_eq!(t.run("SORT_RO l DESC ALPHA LIMIT 0 1"), bulks(&["2"]));
    assert_eq!(t.run("SORT_RO l STORE out"), err(SYNTAX));
}

#[test]
fn bad_options_are_syntax_errors() {
    let mut t = T::new();
    t.run("RPUSH n 1 2");
    for line in
        ["SORT n LIMIT 1", "SORT n LIMIT", "SORT n FOO", "SORT n BY", "SORT n GET", "SORT n STORE"]
    {
        assert_eq!(t.run(line), err(SYNTAX), "{line}");
    }
    assert_eq!(t.run("SORT n LIMIT a 1"), err(NOT_INT));
    assert_eq!(t.run("SORT n LIMIT 1 b"), err(NOT_INT));
}

#[test]
fn expired_lookup_keys_count_as_missing() {
    let mut t = T::new();
    t.run("RPUSH ids 1 2");
    t.run("SET w_1 5 PX 100");
    t.run("SET w_2 3");
    t.advance(200);
    assert_eq!(t.run("SORT ids BY w_* GET w_*"), arr(vec![nil(), bulk("3")]));
}
