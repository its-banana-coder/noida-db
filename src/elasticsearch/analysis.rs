//! Text analysis: the built-in analyzers used to turn a `text` field's
//! value into index terms. `standard` approximates Lucene's
//! `StandardTokenizer` (Unicode word segmentation, UAX#29) for the common
//! ASCII/word cases the spec asks for, without pulling in a full UAX#29
//! implementation.

/// Splits `text` into lowercased word tokens the way the `standard`
/// analyzer does for common cases: runs of alphanumerics (Unicode-aware),
/// with `'` and internal `.`/`,` inside numbers not splitting a token
/// (e.g. "don't", "3.14"), everything else is a separator.
pub fn standard(text: &str) -> Vec<String> {
    standard_with_offsets(text).into_iter().map(|(t, _, _)| t).collect()
}

/// `standard` tokens with the byte range each came from in `text` (what a
/// highlighter needs to put tags back around the original words).
pub fn standard_with_offsets(text: &str) -> Vec<(String, usize, usize)> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let byte_at = |i: usize| chars.get(i).map_or(text.len(), |c| c.0);
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if !is_word_char(chars[i].1) {
            i += 1;
            continue;
        }
        let start = i;
        while i < chars.len()
            && (is_word_char(chars[i].1)
                || (is_word_glue(chars[i].1)
                    && i + 1 < chars.len()
                    && is_word_char(chars[i + 1].1)
                    && i > start))
        {
            i += 1;
        }
        let mut end = i;
        while end > start && !chars[end - 1].1.is_alphanumeric() {
            end -= 1;
        }
        if end > start {
            let token: String = chars[start..end].iter().map(|c| c.1).collect();
            tokens.push((token.to_lowercase(), byte_at(start), byte_at(end)));
        }
    }
    tokens
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric()
}

fn is_word_glue(c: char) -> bool {
    matches!(c, '\'' | '.' | ',')
}

/// `simple` analyzer: splits on anything that isn't a letter, lowercases.
pub fn simple(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphabetic())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// `whitespace` analyzer: splits on whitespace only, case preserved.
pub fn whitespace(text: &str) -> Vec<String> {
    text.split_whitespace().map(str::to_string).collect()
}

/// `keyword` analyzer / field type: the whole input is a single token,
/// unchanged.
pub fn keyword(text: &str) -> Vec<String> {
    if text.is_empty() { Vec::new() } else { vec![text.to_string()] }
}

/// The default English stopword list Lucene's `StopAnalyzer` and ES's
/// `stop` filter use.
pub const ENGLISH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// `stop` analyzer: `simple` tokenization with English stopwords removed.
pub fn stop(text: &str) -> Vec<String> {
    simple(text).into_iter().filter(|t| !ENGLISH_STOPWORDS.contains(&t.as_str())).collect()
}

/// Analyzes `text` by name, defaulting to `standard` (ES's index default)
/// for anything unrecognized.
pub fn analyze(analyzer: &str, text: &str) -> Vec<String> {
    match analyzer {
        "simple" => simple(text),
        "whitespace" => whitespace(text),
        "keyword" => keyword(text),
        "stop" => stop(text),
        _ => standard(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_lowercases_and_splits_on_punctuation() {
        assert_eq!(standard("The Quick-Brown Fox!"), vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn standard_keeps_apostrophes_and_decimals_inside_tokens() {
        assert_eq!(standard("don't split 3.14 please"), vec!["don't", "split", "3.14", "please"]);
    }

    #[test]
    fn whitespace_preserves_case_and_punctuation() {
        assert_eq!(whitespace("Hello, World!"), vec!["Hello,", "World!"]);
    }

    #[test]
    fn keyword_is_a_single_token() {
        assert_eq!(keyword("New York"), vec!["New York"]);
    }

    #[test]
    fn stop_removes_common_words() {
        assert_eq!(stop("the quick fox and the dog"), vec!["quick", "fox", "dog"]);
    }
}
