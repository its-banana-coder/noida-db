//! PFADD, PFCOUNT and PFMERGE. The expected counts, sizes and bytes come
//! from a real Redis: HyperLogLog is a string with a precise binary layout,
//! so a match means the same algorithm, hash and encoding.

use super::*;

const INVALID: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
const CORRUPT: &str = "INVALIDOBJ Corrupted HLL object detected";
const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn add_range(t: &mut T, key: &str, n: usize) -> Value {
    let elems: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    t.run(&format!("PFADD {key} {}", elems.join(" ")))
}

fn get_bytes(t: &mut T, key: &str) -> Vec<u8> {
    match t.run(&format!("GET {key}")) {
        Value::Bulk(b) => b,
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_first_element_makes_a_sparse_hll() {
    let mut t = T::new();
    assert_eq!(t.run("PFADD h foo"), int(1));
    assert_eq!(t.run("TYPE h"), simple("string"));
    // "HYLL", sparse, 3 unused bytes, an invalid cached cardinality, then run
    // lengths: 7348 zeros, register value 5, 9035 zeros (= 16384 registers).
    let mut want = b"HYLL\x01\0\0\0".to_vec();
    want.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0x80]);
    want.extend_from_slice(&[0x5c, 0xb3, 0x90, 0x63, 0x4a]);
    assert_eq!(get_bytes(&mut t, "h"), want);
    assert_eq!(t.run("STRLEN h"), int(21));
    // PFCOUNT caches its answer in the header.
    assert_eq!(t.run("PFCOUNT h"), int(1));
    let cached = get_bytes(&mut t, "h");
    assert_eq!(&cached[8..16], &[1, 0, 0, 0, 0, 0, 0, 0]);
    // Adding the same element again changes nothing.
    assert_eq!(t.run("PFADD h foo"), int(0));
    assert_eq!(&get_bytes(&mut t, "h")[8..16], &[1, 0, 0, 0, 0, 0, 0, 0], "the cache stays valid");
}

#[test]
fn counts_and_sizes_match_real_redis() {
    // (elements, PFCOUNT, STRLEN): sparse below Redis's 3000 byte limit, then dense.
    let table = [
        (1, 1, 21),
        (10, 10, 47),
        (100, 100, 287),
        (500, 503, 1061),
        (1000, 1001, 1922),
        (2000, 2006, 12304),
        (3000, 3005, 12304),
        (5000, 4985, 12304),
        (20000, 19891, 12304),
    ];
    for (n, count, len) in table {
        let mut t = T::new();
        assert_eq!(add_range(&mut t, "big", n), int(1), "n={n}");
        assert_eq!(t.run("PFCOUNT big"), int(count), "count for n={n}");
        assert_eq!(t.run("STRLEN big"), int(len), "size for n={n}");
        let encoding = get_bytes(&mut t, "big")[4];
        assert_eq!(encoding, if len == 12304 { 0 } else { 1 }, "encoding for n={n}");
    }
}

#[test]
fn a_few_elements() {
    let mut t = T::new();
    assert_eq!(t.run("PFADD h a b c d e f g"), int(1));
    assert_eq!(t.run("PFCOUNT h"), int(7));
    assert_eq!(t.run("STRLEN h"), int(38));
    assert_eq!(t.run("PFADD h a b c"), int(0));
    assert_eq!(t.run("PFADD h a z"), int(1));
    assert_eq!(t.run("PFCOUNT h"), int(8));
}

#[test]
fn pfadd_with_no_elements_creates_an_empty_hll() {
    let mut t = T::new();
    assert_eq!(t.run("PFADD e"), int(1));
    assert_eq!(t.run("PFCOUNT e"), int(0));
    assert_eq!(t.run("PFADD e"), int(0));
    assert_eq!(t.run("EXISTS e"), int(1));
}

#[test]
fn pfcount_over_several_keys_counts_the_union() {
    let mut t = T::new();
    t.run("PFADD h1 a b c");
    t.run("PFADD h2 c d e");
    assert_eq!(t.run("PFCOUNT h1"), int(3));
    assert_eq!(t.run("PFCOUNT h1 h2"), int(5));
    assert_eq!(t.run("PFCOUNT h1 nokey h2"), int(5), "a missing key is an empty HLL");
    assert_eq!(t.run("PFCOUNT nokey"), int(0));
    assert_eq!(t.run("PFCOUNT nokey other"), int(0));
    // The union of a sparse and a dense HLL.
    add_range(&mut t, "dense", 2000);
    let Value::Integer(union) = t.run("PFCOUNT h1 dense") else { panic!() };
    assert!(union >= 2000, "{union}");
}

#[test]
fn pfmerge_unions_into_the_destination() {
    let mut t = T::new();
    t.run("PFADD h1 a b c");
    t.run("PFADD h2 c d e");
    assert_eq!(t.run("PFMERGE dest h1 h2"), ok());
    assert_eq!(t.run("PFCOUNT dest"), int(5));
    // The destination counts as a source too.
    t.run("PFADD h3 x y");
    assert_eq!(t.run("PFMERGE dest h3"), ok());
    assert_eq!(t.run("PFCOUNT dest"), int(7));
    // No sources: an empty HLL is created.
    assert_eq!(t.run("PFMERGE empty"), ok());
    assert_eq!(t.run("PFCOUNT empty"), int(0));
    assert_eq!(t.run("STRLEN empty"), int(18), "sparse header plus one 2-byte run of 16384 zeros");
    // A dense source makes the destination dense.
    add_range(&mut t, "dense", 2000);
    assert_eq!(t.run("PFMERGE d2 h1 dense"), ok());
    assert_eq!(t.run("STRLEN d2"), int(12304));
    // Missing sources are skipped.
    assert_eq!(t.run("PFMERGE d3 nokey h1"), ok());
    assert_eq!(t.run("PFCOUNT d3"), int(3));
}

#[test]
fn values_that_are_not_hlls_are_refused() {
    let mut t = T::new();
    t.run("SET s hello");
    t.run("SET short abc");
    t.run("LPUSH l a");
    t.run("PFADD good a");
    for key in ["s", "short"] {
        assert_eq!(t.run(&format!("PFADD {key} x")), err(INVALID), "{key}");
        assert_eq!(t.run(&format!("PFCOUNT {key}")), err(INVALID), "{key}");
        assert_eq!(t.run(&format!("PFCOUNT good {key}")), err(INVALID), "{key}");
        assert_eq!(t.run(&format!("PFMERGE d {key}")), err(INVALID), "{key}");
        assert_eq!(t.run(&format!("PFMERGE {key} good")), err(INVALID), "{key}");
    }
    // Other types get the usual error.
    assert_eq!(t.run("PFADD l x"), err(WRONGTYPE));
    assert_eq!(t.run("PFCOUNT l"), err(WRONGTYPE));
}

#[test]
fn a_corrupt_body_is_detected() {
    let mut t = T::new();
    t.run("PFADD h a b c");
    // Overwrite the run lengths with a value that runs past 16384 registers.
    t.run("SETRANGE h 16 \"\\x7f\\xff\"");
    assert_eq!(t.run("PFCOUNT h"), err(CORRUPT));
    // PFADD doesn't walk the whole body, so (as in Redis) it doesn't notice:
    // the first opcode still covers the register it touches.
    assert_eq!(t.run("PFADD h z"), int(1));
}

#[test]
fn pfadd_keeps_the_ttl_and_works_after_dense_promotion() {
    let mut t = T::new();
    t.run("PFADD h a");
    t.run("EXPIRE h 100");
    t.run("PFADD h b");
    assert_eq!(t.run("TTL h"), int(100));
    add_range(&mut t, "big", 3000);
    let before = t.run("PFCOUNT big");
    assert_eq!(t.run("PFADD big new-element-1 new-element-2"), int(1));
    assert_ne!(t.run("PFCOUNT big"), before);
    assert_eq!(t.run("STRLEN big"), int(12304));
}

#[test]
fn one_at_a_time_matches_all_at_once() {
    // Sparse updates merge and split run lengths; the result must not depend
    // on how the elements arrive.
    let mut t = T::new();
    add_range(&mut t, "bulk", 300);
    for i in 1..=300 {
        t.run(&format!("PFADD single {i}"));
    }
    assert_eq!(t.run("PFCOUNT bulk"), t.run("PFCOUNT single"));
    let (a, b) = (get_bytes(&mut t, "bulk"), get_bytes(&mut t, "single"));
    assert_eq!(a[16..], b[16..], "same registers, same sparse encoding");
}
