//! The token stream every analysis component reads and writes: Lucene's
//! term, offset, position-increment, position-length, type and keyword
//! attributes, as a plain struct.

/// One token. Offsets are character (Unicode scalar) indices into the
/// original text, already corrected for any char filters; positions are
/// relative (`pos_inc`), as Lucene's `PositionIncrementAttribute`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub term: String,
    pub start: usize,
    pub end: usize,
    pub pos_inc: u32,
    pub pos_len: u32,
    pub ty: String,
    pub keyword: bool,
}

impl Token {
    pub fn new(term: impl Into<String>, start: usize, end: usize, ty: &str) -> Self {
        Token {
            term: term.into(),
            start,
            end,
            pos_inc: 1,
            pos_len: 1,
            ty: ty.to_string(),
            keyword: false,
        }
    }

    /// A copy carrying a different term (offsets, type, position kept).
    pub fn with_term(&self, term: impl Into<String>) -> Self {
        Token { term: term.into(), ..self.clone() }
    }
}

/// The absolute position of each token (the first token's increment
/// counts from -1, as Lucene's `position` does).
pub fn positions(tokens: &[Token]) -> Vec<i64> {
    let mut pos = -1i64;
    tokens
        .iter()
        .map(|t| {
            pos += i64::from(t.pos_inc);
            pos.max(0)
        })
        .collect()
}

/// The token "type" names the tokenizers use.
pub const WORD: &str = "word";
pub const ALPHANUM: &str = "<ALPHANUM>";
pub const NUM: &str = "<NUM>";
pub const SYNONYM: &str = "SYNONYM";
pub const SHINGLE: &str = "shingle";
