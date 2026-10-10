//! Lucene's scalar quantization (`ScalarQuantizer`, Lucene 9.11), which an
//! `int8_hnsw`, `int8_flat`, `int4_hnsw` or `int4_flat` `dense_vector`
//! field searches over: kNN scores on such a field are computed on the
//! quantized vectors, so they differ (a little) from the exact ones, and
//! a `similarity` threshold sees those scores.
//!
//! A segment's quantizer is fitted to its vectors: the lower and upper
//! quantiles of their values (a fixed confidence interval for int8; for
//! int4 the pair, from a small grid, whose quantized scores best track
//! the float scores of the vectors' nearest neighbours). Each value maps
//! to `round((clamp(v) - lower) * (2^bits - 1) / (upper - lower))`, with a
//! corrective offset per vector for dot-product scores. noida treats an
//! index's vectors as one segment (what a refresh or force-merge leaves
//! behind), in document order. Floating-point steps follow Lucene's
//! order of operations so that scores match to the last bit where they
//! can.

use super::vectors::Sim;

/// Vectors gathered per quantile estimate, and the most vectors sampled.
const SCRATCH_SIZE: usize = 20;
const SAMPLE_SIZE: usize = 25_000;
const AUTO_SAMPLE_SIZE: usize = 1000;
const MINIMUM_CONFIDENCE_INTERVAL: f32 = 0.9;

/// How a field quantizes: `bits` (7 for int8, 4 for int4) and its
/// `confidence_interval` (`None`: the default for the dimensions; `0`:
/// dynamic, fitted to the data).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Options {
    pub bits: u8,
    pub confidence: Option<f32>,
}

/// Lucene's `ScalarQuantizer`.
#[derive(Clone, Copy, Debug)]
struct Quantizer {
    lower: f32,
    upper: f32,
    scale: f32,
    alpha: f32,
}

impl Quantizer {
    fn new(lower: f32, upper: f32, bits: u8) -> Self {
        let divisor = ((1u32 << bits) - 1) as f32;
        Quantizer {
            lower,
            upper,
            scale: divisor / (upper - lower),
            alpha: (upper - lower) / divisor,
        }
    }

    fn constant(&self) -> f32 {
        self.alpha * self.alpha
    }

    /// The quantized values of `v`, and its corrective offset (zero for
    /// Euclidean scores).
    fn quantize(&self, v: &[f32], sim: Sim) -> (Vec<u8>, f32) {
        let mut out = Vec::with_capacity(v.len());
        let mut correction = 0f32;
        for &x in v {
            let dx = x - self.lower;
            let dxc = self.lower.max(self.upper.min(x)) - self.lower;
            // Java's Math.round (NaN rounds to 0).
            let rounded = (self.scale * dxc).round();
            let q = if rounded.is_nan() { 0 } else { rounded as i32 };
            let dxq = q as f32 * self.alpha;
            out.push(q as u8);
            correction += self.lower * (x - self.lower / 2.0) + (dx - dxq) * dxq;
        }
        (out, if sim == Sim::L2 { 0.0 } else { correction })
    }

    /// `ScalarQuantizedVectorSimilarity#score`.
    fn score(&self, sim: Sim, q: &[u8], q_offset: f32, v: &[u8], v_offset: f32) -> f32 {
        if sim == Sim::L2 {
            let d: i32 = q.iter().zip(v).map(|(a, b)| (*a as i32 - *b as i32).pow(2)).sum();
            return 1.0 / (1.0 + d as f32 * self.constant());
        }
        let dot: i32 = q.iter().zip(v).map(|(a, b)| *a as i32 * *b as i32).sum();
        let adjusted = dot as f32 * self.constant() + q_offset + v_offset;
        match sim {
            Sim::Mip => scale_max_inner_product(adjusted),
            _ => ((1.0 + adjusted) / 2.0).max(0.0),
        }
    }
}

fn scale_max_inner_product(x: f32) -> f32 {
    if x < 0.0 { 1.0 / (1.0 + -x) } else { x + 1.0 }
}

/// Lucene's float `VectorSimilarityFunction#compare` (cosine having been
/// turned into dot product on normalized vectors by now).
fn float_score(sim: Sim, a: &[f32], b: &[f32]) -> f32 {
    match sim {
        Sim::L2 => {
            let mut d = 0f32;
            for (x, y) in a.iter().zip(b) {
                let diff = x - y;
                d += diff * diff;
            }
            1.0 / (1.0 + d)
        }
        _ => {
            let mut dot = 0f32;
            for (x, y) in a.iter().zip(b) {
                dot += x * y;
            }
            if sim == Sim::Mip {
                scale_max_inner_product(dot)
            } else {
                ((1.0 + dot) / 2.0).max(0.0)
            }
        }
    }
}

/// `VectorUtil.l2normalize`, as Lucene normalizes cosine vectors.
fn normalized(v: &[f32]) -> Vec<f32> {
    let mut dot = 0f32;
    for x in v {
        dot += x * x;
    }
    if ((dot as f64) - 1.0).abs() <= 1e-5 {
        return v.to_vec();
    }
    let norm = (dot as f64).sqrt() as f32;
    v.iter().map(|x| x / norm).collect()
}

/// The `confidence_interval` Lucene uses when none is configured.
fn default_confidence(dims: usize) -> f32 {
    MINIMUM_CONFIDENCE_INTERVAL.max(1.0 - 1.0 / (dims + 1) as f32)
}

/// The lower and upper quantile of `values` for a confidence interval.
fn quantiles(values: &mut [f32], confidence: f32) -> (f32, f32) {
    values.sort_by(f32::total_cmp);
    let n = values.len();
    if n <= 2 {
        return (values[0], values[n - 1]);
    }
    let skip = (n as f32 * (1.0 - confidence) / 2.0 + 0.5) as usize;
    let kept = &values[skip.min(n)..n.saturating_sub(skip).max(skip.min(n))];
    let lower = kept.iter().copied().fold(f32::INFINITY, f32::min);
    let upper = kept.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    (lower, upper)
}

/// `java.util.Random`, for Lucene's seeded reservoir sampling.
struct JavaRandom(u64);

impl JavaRandom {
    const MULT: u64 = 0x5DEECE66D;
    const MASK: u64 = (1 << 48) - 1;

    fn new(seed: u64) -> Self {
        JavaRandom((seed ^ Self::MULT) & Self::MASK)
    }

    fn next(&mut self, bits: u32) -> i32 {
        self.0 = (self.0.wrapping_mul(Self::MULT).wrapping_add(0xB)) & Self::MASK;
        (self.0 >> (48 - bits)) as i32
    }

    fn next_int(&mut self, bound: i32) -> i32 {
        let mut r = self.next(31);
        let m = bound - 1;
        if bound & m == 0 {
            return ((bound as i64 * r as i64) >> 31) as i32;
        }
        let mut u = r;
        loop {
            r = u % bound;
            if u.wrapping_sub(r).wrapping_add(m) >= 0 {
                return r;
            }
            u = self.next(31);
        }
    }
}

/// Lucene's `reservoirSampleIndices`: which of `total` vectors to sample.
fn reservoir(total: usize, sample: usize) -> Vec<usize> {
    let mut random = JavaRandom::new(42);
    let mut take: Vec<usize> = (0..sample).collect();
    for i in sample..total {
        let j = random.next_int((i + 1) as i32) as usize;
        if j < sample {
            take[j] = i;
        }
    }
    take.sort_unstable();
    take
}

/// The vectors quantiles are gathered from, `SCRATCH_SIZE` at a time (a
/// partial last batch is left out), with every one of `confidences`
/// averaged over the batches: per confidence, (lower, upper).
fn gathered(
    vectors: &[&[f32]],
    sample: usize,
    confidences: &[f32],
) -> (Vec<(f32, f32)>, Vec<usize>) {
    let total = vectors.len();
    let chosen: Vec<usize> =
        if total <= sample { (0..total).collect() } else { reservoir(total, sample) };
    let batch = SCRATCH_SIZE.min(total);
    let mut sums = vec![(0f64, 0f64); confidences.len()];
    let mut count = 0usize;
    let mut scratch: Vec<f32> = Vec::new();
    let mut in_batch = 0;
    for &i in &chosen {
        scratch.extend_from_slice(vectors[i]);
        in_batch += 1;
        if in_batch == batch {
            for (c, sum) in confidences.iter().zip(sums.iter_mut()) {
                let (lo, up) = quantiles(&mut scratch.clone(), *c);
                sum.0 += lo as f64;
                sum.1 += up as f64;
            }
            scratch.clear();
            in_batch = 0;
            count += 1;
        }
    }
    let out = sums
        .iter()
        .map(|(lo, up)| (*lo as f32 / count as f32, *up as f32 / count as f32))
        .collect();
    (out, chosen)
}

/// `ScalarQuantizer.fromVectors`: quantiles at a fixed confidence.
fn fixed(vectors: &[&[f32]], confidence: f32, bits: u8) -> Quantizer {
    if vectors.is_empty() {
        return Quantizer::new(0.0, 0.0, bits);
    }
    if confidence == 1.0 {
        let all = vectors.iter().flat_map(|v| v.iter().copied());
        let lower = all.clone().fold(f32::INFINITY, f32::min);
        let upper = all.fold(f32::NEG_INFINITY, f32::max);
        return Quantizer::new(lower, upper, bits);
    }
    let (q, _) = gathered(vectors, SAMPLE_SIZE, &[confidence]);
    Quantizer::new(q[0].0, q[0].1, bits)
}

/// Welford's online mean and variance (Lucene's `OnlineMeanAndVar`).
#[derive(Default)]
struct MeanVar {
    mean: f64,
    var: f64,
    n: u32,
}

impl MeanVar {
    fn add(&mut self, x: f64) {
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.var += delta * (x - self.mean);
    }

    fn var(&self) -> f64 {
        self.var / (self.n as f64 - 1.0)
    }
}

/// Each sampled vector's (up to) 10 nearest neighbours among the others,
/// best first, with the variance of their scores.
fn nearest(vectors: &[&[f32]], sim: Sim) -> Vec<(Vec<(usize, f32)>, f64)> {
    (0..vectors.len())
        .map(|i| {
            let mut near: Vec<(usize, f32)> = (0..vectors.len())
                .filter(|j| *j != i)
                .map(|j| (j, float_score(sim, vectors[i], vectors[j])))
                .collect();
            near.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            near.truncate(10);
            let mut mv = MeanVar::default();
            for (_, s) in near.iter().rev() {
                mv.add(*s as f64);
            }
            let var = mv.var();
            (near, var)
        })
        .collect()
}

/// How well a quantizer's scores track the float scores: the mean, over
/// the sampled vectors, of 1 minus the error variance over the score
/// variance of their nearest neighbours.
fn correlation(
    q: &Quantizer,
    sim: Sim,
    vectors: &[&[f32]],
    near: &[(Vec<(usize, f32)>, f64)],
) -> f64 {
    let mut corr = MeanVar::default();
    for (i, (neighbours, variance)) in near.iter().enumerate() {
        let (query, q_offset) = q.quantize(vectors[i], sim);
        let mut errors = MeanVar::default();
        for (j, s) in neighbours {
            let (v, v_offset) = q.quantize(vectors[*j], sim);
            errors.add((q.score(sim, &query, q_offset, &v, v_offset) - s) as f64);
        }
        corr.add(1.0 - errors.var() / variance);
    }
    if corr.mean.is_nan() { 0.0 } else { corr.mean }
}

/// `ScalarQuantizer.fromVectorsAutoInterval`: the quantile pair (from a
/// grid between two confidence intervals' quantiles) whose quantized
/// scores correlate best with the float ones.
fn dynamic(vectors: &[&[f32]], sim: Sim, bits: u8) -> Quantizer {
    if vectors.is_empty() {
        return Quantizer::new(0.0, 0.0, bits);
    }
    let dims = vectors[0].len();
    let confidences =
        [1.0 - 32f32.min(dims as f32 / 10.0) / (dims + 1) as f32, 1.0 - 1.0 / (dims + 1) as f32];
    let (q, chosen) = gathered(vectors, AUTO_SAMPLE_SIZE, &confidences);
    let sampled: Vec<&[f32]> = chosen.iter().map(|i| vectors[*i]).collect();
    let ((au, bl), (al, bu)) = (q[0], q[1]);
    let mut lowers = [0f32; 16];
    let mut uppers = [0f32; 16];
    for (idx, step) in (0..32).step_by(2).enumerate() {
        let i = step as f32;
        lowers[idx] = al + i * (au - al) / 32.0;
        uppers[idx] = bl + i * (bu - bl) / 32.0;
    }
    let near = nearest(&sampled, sim);
    let mut best = (f64::NEG_INFINITY, 0f32, 0f32);
    let (mut best_i, mut best_j) = (0, 0);
    let consider = |i: usize, j: usize, best: &mut (f64, f32, f32)| -> bool {
        let (lower, upper) = (lowers[i], uppers[j]);
        if !lower.is_finite() || !upper.is_finite() || upper <= lower {
            return false;
        }
        let c = correlation(&Quantizer::new(lower, upper, bits), sim, &sampled, &near);
        if c > best.0 {
            *best = (c, lower, upper);
            return true;
        }
        false
    };
    for i in (0..16).step_by(4) {
        for j in (0..16).step_by(4) {
            if consider(i, j, &mut best) {
                (best_i, best_j) = (i, j);
            }
        }
    }
    for i in best_i + 1..best_i + 4 {
        for j in best_j + 1..best_j + 4 {
            consider(i, j, &mut best);
        }
    }
    Quantizer::new(best.1, best.2, bits)
}

/// The kNN scores of `query` against each of `vectors` (one segment's,
/// in document order) on a field quantized with `opts`.
pub fn scores(opts: Options, sim: Sim, query: &[f32], vectors: &[&[f32]]) -> Vec<f32> {
    // Cosine is a dot product of normalized vectors.
    let (sim, owned): (Sim, Option<Vec<Vec<f32>>>) = if sim == Sim::Cosine {
        (Sim::Dot, Some(vectors.iter().map(|v| normalized(v)).collect()))
    } else {
        (sim, None)
    };
    let vectors: Vec<&[f32]> = match &owned {
        Some(o) => o.iter().map(Vec::as_slice).collect(),
        None => vectors.to_vec(),
    };
    let query = if owned.is_some() { normalized(query) } else { query.to_vec() };
    let dims = query.len();
    let quantizer = match opts.confidence {
        Some(c) if c == 0.0 => dynamic(&vectors, sim, opts.bits),
        Some(c) => fixed(&vectors, c, opts.bits),
        None => fixed(&vectors, default_confidence(dims), opts.bits),
    };
    let (q, q_offset) = quantizer.quantize(&query, sim);
    vectors
        .iter()
        .map(|v| {
            let (b, offset) = quantizer.quantize(v, sim);
            quantizer.score(sim, &q, q_offset, &b, offset)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOCS5: [[f32; 5]; 3] = [
        [230.0, 300.33, -34.8988, 15.555, -200.0],
        [-0.5, 100.0, -13.0, 14.8, -156.0],
        [0.5, 111.3, -13.0, 14.8, -156.0],
    ];

    // The scores Elasticsearch 8.15 gives these documents (the YAML
    // suite's quantized kNN tests).
    #[test]
    fn int8_scores_match_elasticsearch() {
        let docs: Vec<&[f32]> = DOCS5.iter().map(|d| d.as_slice()).collect();
        let q = [-0.5, 90.0, -10.0, 14.8, -156.0];
        let s = scores(Options { bits: 7, confidence: None }, Sim::L2, &q, &docs);
        assert_eq!(s, [1.3606054e-5, 0.010_709_194_5, 0.002_160_347_5]);
    }

    #[test]
    fn int4_scores_match_elasticsearch() {
        let docs: Vec<&[f32]> = DOCS5.iter().map(|d| &d[..4]).collect();
        let q = [-0.5, 90.0, -10.0, 14.8];
        let s = scores(Options { bits: 4, confidence: Some(0.0) }, Sim::L2, &q, &docs);
        assert_eq!(s, [1.191_048_8e-5, 1.0, 0.002_684_576]);
    }

    #[test]
    fn java_random_matches() {
        // new Random(42).nextInt(10) == 0, then 3, 8.
        let mut r = JavaRandom::new(42);
        assert_eq!([r.next_int(10), r.next_int(10), r.next_int(10)], [0, 3, 8]);
    }

    #[test]
    fn degenerate_inputs_do_not_panic() {
        let one: [f32; 2] = [1.0, 1.0];
        let s = scores(Options { bits: 7, confidence: None }, Sim::Cosine, &[1.0, 2.0], &[&one]);
        assert_eq!(s.len(), 1);
        let s = scores(Options { bits: 4, confidence: Some(0.0) }, Sim::Mip, &[1.0, 2.0], &[&one]);
        assert!(s[0].is_finite());
    }
}
