//! BM25 relevance scoring, matching Lucene 9's `BM25Similarity` and its
//! lossy per-document field-length norm encoding (`SmallFloat`), so
//! `_score` values line up with real Elasticsearch for the same data.
//!
//! Ported from Apache Lucene (Apache License 2.0):
//! `lucene/core/src/java/org/apache/lucene/util/SmallFloat.java` and
//! `lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java`.
//! See `THIRD_PARTY.md`.

/// `SmallFloat.longToInt4`: a 4-significant-bit encoding of a non-negative
/// integer that preserves ordering.
fn long_to_int4(i: u64) -> u32 {
    let num_bits = 64 - i.leading_zeros();
    if num_bits < 4 {
        i as u32
    } else {
        let shift = num_bits - 4;
        let mut encoded = (i >> shift) as u32;
        encoded &= 0x07;
        encoded |= (shift + 1) << 3;
        encoded
    }
}

/// `SmallFloat.int4ToLong`: decodes a value encoded by [`long_to_int4`].
fn int4_to_long(i: u32) -> u64 {
    let bits = (i & 0x07) as u64;
    let shift = (i >> 3) as i32 - 1;
    if shift == -1 { bits } else { (bits | 0x08) << shift }
}

fn max_int4() -> u32 {
    long_to_int4(i32::MAX as u64)
}

fn num_free_values() -> u32 {
    255 - max_int4()
}

/// `SmallFloat.intToByte4`: Lucene's one-byte encoding of a field's term
/// count (the "norm"), used the same way here for doc length.
pub fn int_to_byte4(i: u32) -> u8 {
    let free = num_free_values();
    if i < free { i as u8 } else { (free + long_to_int4((i - free) as u64)) as u8 }
}

/// `SmallFloat.byte4ToInt`: decodes a norm byte back to an approximate term
/// count (exact for counts below `num_free_values()`, lossy above it).
pub fn byte4_to_int(b: u8) -> u32 {
    let i = b as u32;
    let free = num_free_values();
    if i < free { i } else { (free as u64 + int4_to_long(i - free)) as u32 }
}

/// BM25 defaults used by Elasticsearch: k1 = 1.2, b = 0.75.
pub const K1: f32 = 1.2;
pub const B: f32 = 0.75;

/// `BM25Similarity.idf`: `log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5))`.
pub fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    (1.0 + (doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5)).ln() as f32
}

/// `BM25DocScorer.score`: `idf * (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * doclen / avgdl))`.
pub fn score(term_freq: u32, doc_len: u32, avg_doc_len: f32, doc_freq: u64, doc_count: u64) -> f32 {
    let idf = idf(doc_freq, doc_count);
    let freq = term_freq as f32;
    let norm = K1 * ((1.0 - B) + B * doc_len as f32 / avg_doc_len);
    idf * (freq * (K1 + 1.0)) / (freq + norm)
}

/// Encodes a field's term count into its lossy per-document norm, the way
/// Lucene stores it, then immediately decodes it back — callers store and
/// score against the decoded (lossy) length, exactly as real ES does.
pub fn norm_doc_len(num_terms: u32) -> u32 {
    byte4_to_int(int_to_byte4(num_terms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_round_trip_exactly() {
        for n in 0..16 {
            assert_eq!(norm_doc_len(n), n);
        }
    }

    #[test]
    fn large_values_round_trip_lossily_but_monotonically() {
        let mut prev = 0;
        for n in [16, 100, 1000, 10_000, 1_000_000] {
            let decoded = norm_doc_len(n);
            assert!(decoded >= prev, "norm decoding must not decrease with n");
            // 4 significant bits: within one part in 16 of the true value.
            assert!(
                (decoded as f64 - n as f64).abs() <= n as f64 / 8.0 + 1.0,
                "n={n} decoded={decoded}"
            );
            prev = decoded;
        }
    }

    #[test]
    fn idf_is_higher_for_rarer_terms() {
        assert!(idf(1, 1000) > idf(500, 1000));
    }

    #[test]
    fn identical_single_term_docs_score_equally() {
        let s1 = score(1, 5, 5.0, 1, 10);
        let s2 = score(1, 5, 5.0, 1, 10);
        assert_eq!(s1, s2);
        assert!(s1 > 0.0);
    }
}
