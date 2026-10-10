//! Building analyzers, tokenizers, token filters, char filters and
//! normalizers from their definitions: an index's `analysis` settings
//! (where every value is a string), inline `_analyze` definitions (typed
//! JSON), and the built-in names.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::Value;

use super::char_filters::{self, CharFilter};
use super::filters::{LowerLang, Shingle, Stem, TokenFilter, WordDelimiter};
use super::jregex::JPattern;
use super::stemmers;
use super::stopwords;
use super::synonyms::{self, ParseOptions, RuleError};
use super::token::Token;
use super::tokenizers::{TokenChars, Tokenizer};
use super::{AnalysisError, Analyzer};

/// Where names are looked up: the index's `analysis` settings (if any),
/// its other settings (limits), and the synonym sets.
pub struct Defs<'a> {
    pub analysis: &'a Value,
    pub index_settings: &'a Value,
    pub synonym_sets: &'a dyn Fn(&str) -> Option<Vec<String>>,
}

fn iae(reason: impl Into<String>) -> AnalysisError {
    AnalysisError::new("illegal_argument_exception", reason)
}

/// A parameter as text (index settings store everything as strings).
fn p_str<'v>(o: &'v Value, k: &str) -> Option<&'v str> {
    o.get(k).and_then(Value::as_str)
}

fn p_usize(o: &Value, k: &str, default: usize) -> Result<usize, AnalysisError> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|n| n as usize)
            .ok_or_else(|| iae(format!("Failed to parse value [{n}] for setting [{k}]"))),
        Some(Value::String(s)) => s
            .trim()
            .parse::<usize>()
            .map_err(|_| iae(format!("Failed to parse value [{s}] for setting [{k}]"))),
        Some(other) => Err(iae(format!("Failed to parse value [{other}] for setting [{k}]"))),
    }
}

fn p_i64(o: &Value, k: &str, default: i64) -> Result<i64, AnalysisError> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Number(n)) => {
            n.as_i64().ok_or_else(|| iae(format!("Failed to parse value [{n}] for setting [{k}]")))
        }
        Some(Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| iae(format!("Failed to parse value [{s}] for setting [{k}]"))),
        Some(other) => Err(iae(format!("Failed to parse value [{other}] for setting [{k}]"))),
    }
}

fn p_bool(o: &Value, k: &str, default: bool) -> Result<bool, AnalysisError> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) if s == "true" => Ok(true),
        Some(Value::String(s)) if s == "false" => Ok(false),
        Some(other) => {
            let s = other.as_str().map_or_else(|| other.to_string(), str::to_string);
            Err(iae(format!("Failed to parse value [{s}] as only [true] or [false] are allowed.")))
        }
    }
}

/// A list parameter: an array, or a comma-separated string (as
/// `Settings.getAsList` reads one).
fn p_list(o: &Value, k: &str) -> Option<Vec<String>> {
    match o.get(k)? {
        Value::Array(a) => Some(
            a.iter().map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string)).collect(),
        ),
        Value::String(s) => {
            Some(s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
        }
        Value::Null => None,
        other => Some(vec![other.to_string()]),
    }
}

/// A word set: a predefined `_lang_` list, or explicit words.
fn word_set(o: &Value, k: &str, default: &[&str]) -> HashSet<String> {
    match o.get(k) {
        Some(Value::String(s)) if s.starts_with('_') && s.ends_with('_') => {
            match stopwords::predefined(s) {
                Some(list) => list.into_iter().collect(),
                None => [s.clone()].into_iter().collect(),
            }
        }
        Some(_) => p_list(o, k).unwrap_or_default().into_iter().collect(),
        None => default.iter().map(|w| w.to_string()).collect(),
    }
}

fn token_chars(list: &[String], custom: Option<&str>) -> Result<TokenChars, AnalysisError> {
    let mut tc = TokenChars::default();
    for c in list {
        match c.trim() {
            "letter" => tc.letter = true,
            "digit" => tc.digit = true,
            "whitespace" => tc.whitespace = true,
            "punctuation" => tc.punctuation = true,
            "symbol" => tc.symbol = true,
            "custom" => {
                let chars = custom.ok_or_else(|| {
                    iae("Token type: [custom] requires setting `custom_token_chars`")
                })?;
                tc.custom = chars.chars().collect();
            }
            other => {
                return Err(iae(format!(
                    "Unknown token type: '{other}', must be one of [symbol, private_use, paragraph_separator, start_punctuation, unassigned, enclosing_mark, connector_punctuation, letter_number, other_number, math_symbol, lowercase_letter, space_separator, surrogate, initial_quote_punctuation, decimal_digit_number, digit, other_punctuation, dash_punctuation, currency_symbol, non_spacing_mark, format, modifier_letter, control, uppercase_letter, other_symbol, end_punctuation, modifier_symbol, other_letter, line_separator, titlecase_letter, letter, punctuation, combining_spacing_mark, final_quote_punctuation, whitespace]"
                )));
            }
        }
    }
    Ok(tc)
}

fn regex(pattern: &str, flags: &str) -> Result<JPattern, AnalysisError> {
    JPattern::compile(pattern, flags).map_err(|m| AnalysisError::new("pattern_syntax_exception", m))
}

/// Built-in tokenizer names.
fn builtin_tokenizer(name: &str) -> Option<Tokenizer> {
    Some(match name {
        "standard" => Tokenizer::Standard { max_len: 255 },
        "classic" => Tokenizer::Classic { max_len: 255 },
        "uax_url_email" => Tokenizer::UaxUrlEmail { max_len: 255 },
        "whitespace" => Tokenizer::Whitespace { max_len: 255 },
        "letter" => Tokenizer::Letter { max_len: 255, lowercase: false },
        "lowercase" => Tokenizer::Letter { max_len: 255, lowercase: true },
        "keyword" => Tokenizer::Keyword,
        "ngram" => Tokenizer::NGram { min: 1, max: 2, chars: TokenChars::default(), edge: false },
        "edge_ngram" => {
            Tokenizer::NGram { min: 1, max: 2, chars: TokenChars::default(), edge: true }
        }
        "pattern" => Tokenizer::Pattern { re: JPattern::compile(r"\W+", "").ok()?, group: -1 },
        "path_hierarchy" | "PathHierarchy" => {
            Tokenizer::PathHierarchy { delimiter: '/', replacement: '/', skip: 0, reverse: false }
        }
        "thai" => Tokenizer::Thai,
        _ => return None,
    })
}

/// Language analyzers (Lucene's per-language analyzers, approximated
/// beyond English by Snowball stemming).
const LANGUAGES: &[&str] = &[
    "english",
    "arabic",
    "armenian",
    "basque",
    "bengali",
    "brazilian",
    "bulgarian",
    "catalan",
    "cjk",
    "czech",
    "danish",
    "dutch",
    "estonian",
    "finnish",
    "french",
    "galician",
    "german",
    "greek",
    "hindi",
    "hungarian",
    "indonesian",
    "irish",
    "italian",
    "latvian",
    "lithuanian",
    "norwegian",
    "persian",
    "portuguese",
    "romanian",
    "russian",
    "sorani",
    "spanish",
    "swedish",
    "turkish",
    "thai",
];

impl Defs<'_> {
    fn section(&self, kind: &str) -> Option<&serde_json::Map<String, Value>> {
        self.analysis.get(kind).and_then(Value::as_object)
    }

    fn max_ngram_diff(&self) -> usize {
        let v = &self.index_settings["max_ngram_diff"];
        v.as_u64()
            .map(|n| n as usize)
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(1)
    }

    fn max_shingle_diff(&self) -> usize {
        let v = &self.index_settings["max_shingle_diff"];
        v.as_u64()
            .map(|n| n as usize)
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(3)
    }

    // ---- tokenizers ----

    /// A tokenizer by name (index definitions first, then built-ins).
    pub fn tokenizer_named(&self, name: &str) -> Option<Result<Tokenizer, AnalysisError>> {
        if let Some(def) = self.section("tokenizer").and_then(|s| s.get(name)) {
            return Some(self.tokenizer_def(name, def));
        }
        builtin_tokenizer(name).map(Ok)
    }

    pub fn tokenizer_def(&self, name: &str, def: &Value) -> Result<Tokenizer, AnalysisError> {
        let Some(ty) = p_str(def, "type") else {
            return Err(iae(format!(
                "tokenizer [{name}] must specify either an analyzer type, or a tokenizer"
            )));
        };
        let max_len = p_usize(def, "max_token_length", 255)?;
        Ok(match ty {
            "standard" => Tokenizer::Standard { max_len },
            "classic" => Tokenizer::Classic { max_len },
            "uax_url_email" => Tokenizer::UaxUrlEmail { max_len },
            "whitespace" => Tokenizer::Whitespace { max_len },
            "letter" => Tokenizer::Letter { max_len, lowercase: false },
            "lowercase" => Tokenizer::Letter { max_len, lowercase: true },
            "keyword" => Tokenizer::Keyword,
            "thai" => Tokenizer::Thai,
            "ngram" | "nGram" | "edge_ngram" | "edgeNGram" => {
                let edge = ty.starts_with('e');
                let min = p_usize(def, "min_gram", 1)?;
                let max = p_usize(def, "max_gram", 2)?;
                if !edge && max.saturating_sub(min) > self.max_ngram_diff() {
                    return Err(iae(format!(
                        "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: [{}] but was [{}]. This limit can be set by changing the [index.max_ngram_diff] index level setting.",
                        self.max_ngram_diff(),
                        max - min
                    )));
                }
                let chars = token_chars(
                    &p_list(def, "token_chars").unwrap_or_default(),
                    p_str(def, "custom_token_chars"),
                )?;
                Tokenizer::NGram { min, max, chars, edge }
            }
            "pattern" => {
                let pattern = p_str(def, "pattern").unwrap_or(r"\W+");
                let flags = p_str(def, "flags").unwrap_or("");
                Tokenizer::Pattern { re: regex(pattern, flags)?, group: p_i64(def, "group", -1)? }
            }
            "simple_pattern" => {
                Tokenizer::SimplePattern { re: regex(p_str(def, "pattern").unwrap_or(""), "")? }
            }
            "simple_pattern_split" => Tokenizer::SimplePatternSplit {
                re: regex(p_str(def, "pattern").unwrap_or(""), "")?,
            },
            "char_group" => {
                let mut tc = TokenChars::default();
                for item in p_list(def, "tokenize_on_chars").unwrap_or_default() {
                    match item.as_str() {
                        "letter" => tc.letter = true,
                        "digit" => tc.digit = true,
                        "whitespace" => tc.whitespace = true,
                        "punctuation" => tc.punctuation = true,
                        "symbol" => tc.symbol = true,
                        s => {
                            let unescaped = match s {
                                "\\n" => "\n".to_string(),
                                "\\r" => "\r".to_string(),
                                "\\t" => "\t".to_string(),
                                "\\f" => "\u{c}".to_string(),
                                other => other.to_string(),
                            };
                            let mut cs = unescaped.chars();
                            match (cs.next(), cs.next()) {
                                (Some(c), None) => tc.custom.push(c),
                                _ => {
                                    return Err(iae(format!("Invalid escaped char in [{s}]")));
                                }
                            }
                        }
                    }
                }
                Tokenizer::CharGroup { split_on: tc, max_len }
            }
            "path_hierarchy" | "PathHierarchy" => {
                let one_char = |k: &str, default: char| -> Result<char, AnalysisError> {
                    match p_str(def, k) {
                        None => Ok(default),
                        Some(s) => {
                            let mut it = s.chars();
                            match (it.next(), it.next()) {
                                (Some(c), None) => Ok(c),
                                _ => Err(iae(format!("{k} must be a one char value"))),
                            }
                        }
                    }
                };
                let delimiter = one_char("delimiter", '/')?;
                Tokenizer::PathHierarchy {
                    delimiter,
                    replacement: one_char("replacement", delimiter)?,
                    skip: p_usize(def, "skip", 0)?,
                    reverse: p_bool(def, "reverse", false)?,
                }
            }
            other => return Err(iae(format!("Unknown tokenizer type [{other}] for [{name}]"))),
        })
    }

    // ---- char filters ----

    pub fn char_filter_named(&self, name: &str) -> Option<Result<CharFilter, AnalysisError>> {
        if let Some(def) = self.section("char_filter").and_then(|s| s.get(name)) {
            return Some(self.char_filter_def(name, def));
        }
        match name {
            "html_strip" => Some(Ok(CharFilter::HtmlStrip { escaped: HashSet::new() })),
            _ => None,
        }
    }

    pub fn char_filter_def(&self, name: &str, def: &Value) -> Result<CharFilter, AnalysisError> {
        let Some(ty) = p_str(def, "type") else {
            return Err(iae(format!(
                "char_filter [{name}] must specify either an analyzer type, or a tokenizer"
            )));
        };
        Ok(match ty {
            "html_strip" => CharFilter::HtmlStrip {
                escaped: p_list(def, "escaped_tags")
                    .unwrap_or_default()
                    .into_iter()
                    .map(|t| t.to_ascii_lowercase())
                    .collect(),
            },
            "mapping" => {
                let Some(rules) = p_list(def, "mappings") else {
                    if def.get("mappings_path").is_some() {
                        return Err(iae(
                            "IOException while reading mappings_path: files are not supported",
                        ));
                    }
                    return Err(iae(
                        "mapping requires either `mappings` or `mappings_path` to be configured",
                    ));
                };
                CharFilter::Mapping {
                    rules: char_filters::parse_mapping_rules(&rules).map_err(iae)?,
                }
            }
            "pattern_replace" => {
                let Some(pattern) = p_str(def, "pattern") else {
                    return Err(iae(format!(
                        "pattern is missing for [{name}] char filter of type 'pattern_replace'"
                    )));
                };
                CharFilter::PatternReplace {
                    re: regex(pattern, p_str(def, "flags").unwrap_or(""))?,
                    replacement: p_str(def, "replacement").unwrap_or("").to_string(),
                }
            }
            other => return Err(iae(format!("Unknown char_filter type [{other}] for [{name}]"))),
        })
    }

    // ---- token filters ----

    /// A token filter by name. `prev` is the analysis chain before it
    /// (tokenizer + earlier filters), which synonym filters analyze their
    /// rules with.
    pub fn filter_named(
        &self,
        name: &str,
        prev: &Chain,
    ) -> Option<Result<TokenFilter, AnalysisError>> {
        if let Some(def) = self.section("filter").and_then(|s| s.get(name)) {
            return Some(self.filter_def(name, def, prev));
        }
        builtin_filter(name).map(Ok)
    }

    pub fn filter_def(
        &self,
        name: &str,
        def: &Value,
        prev: &Chain,
    ) -> Result<TokenFilter, AnalysisError> {
        let Some(ty) = p_str(def, "type") else {
            return Err(iae(format!(
                "filter [{name}] must specify either an analyzer type, or a tokenizer"
            )));
        };
        Ok(match ty {
            "lowercase" => TokenFilter::Lowercase(match p_str(def, "language") {
                None => LowerLang::Default,
                Some("greek") => LowerLang::Greek,
                Some("irish") => LowerLang::Irish,
                Some("turkish") => LowerLang::Turkish,
                Some(other) => {
                    return Err(iae(format!("language [{other}] not support for lower case")));
                }
            }),
            "uppercase" => TokenFilter::Uppercase,
            "asciifolding" => {
                TokenFilter::AsciiFolding { preserve: p_bool(def, "preserve_original", false)? }
            }
            "stop" => TokenFilter::Stop {
                words: word_set(def, "stopwords", stopwords::ENGLISH),
                ignore_case: p_bool(def, "ignore_case", false)?,
                remove_trailing: p_bool(def, "remove_trailing", true)?,
            },
            "porter_stem" => TokenFilter::Stem(Stem::Porter),
            "kstem" => TokenFilter::Stem(Stem::KStem),
            "stemmer" => {
                let lang =
                    p_str(def, "language").or_else(|| p_str(def, "name")).unwrap_or("english");
                stemmer_filter(lang)?
            }
            "snowball" => {
                let lang = p_str(def, "language").unwrap_or("English");
                if lang.eq_ignore_ascii_case("porter") {
                    TokenFilter::Stem(Stem::Porter)
                } else {
                    match stemmers::snowball_algorithm(lang) {
                        Some(a) => TokenFilter::Stem(Stem::Snowball(a)),
                        None => return Err(invalid_stemmer(lang)),
                    }
                }
            }
            "stemmer_override" => {
                let rules = p_list(def, "rules").ok_or_else(|| {
                    iae("stemmer override filter requires either `rules` or `rules_path` to be configured")
                })?;
                let mut out = Vec::new();
                for r in rules {
                    let Some((from, to)) = r.split_once("=>") else {
                        return Err(iae(format!("Invalid Keyword override Rule:{r}")));
                    };
                    let to = to.trim().to_string();
                    for f in from.split(',') {
                        out.push((f.trim().to_string(), to.clone()));
                    }
                }
                TokenFilter::StemmerOverride(out)
            }
            "shingle" => {
                let min = p_usize(def, "min_shingle_size", 2)?;
                let max = p_usize(def, "max_shingle_size", 2)?;
                let output_unigrams = p_bool(def, "output_unigrams", true)?;
                let diff = max.saturating_sub(min) + usize::from(output_unigrams);
                if diff > self.max_shingle_diff() {
                    return Err(iae(format!(
                        "In Shingle TokenFilter the difference between max_shingle_size and min_shingle_size (and +1 if outputting unigrams) must be less than or equal to: [{}] but was [{diff}]. This limit can be set by changing the [index.max_shingle_diff] index level setting.",
                        self.max_shingle_diff()
                    )));
                }
                TokenFilter::Shingle(Shingle {
                    min,
                    max,
                    output_unigrams,
                    output_unigrams_if_no_shingles: p_bool(
                        def,
                        "output_unigrams_if_no_shingles",
                        false,
                    )?,
                    separator: p_str(def, "token_separator").unwrap_or(" ").to_string(),
                    filler: p_str(def, "filler_token").unwrap_or("_").to_string(),
                })
            }
            "ngram" | "nGram" => {
                let min = p_usize(def, "min_gram", 1)?;
                let max = p_usize(def, "max_gram", 2)?;
                if max.saturating_sub(min) > self.max_ngram_diff() {
                    return Err(iae(format!(
                        "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: [{}] but was [{}]. This limit can be set by changing the [index.max_ngram_diff] index level setting.",
                        self.max_ngram_diff(),
                        max - min
                    )));
                }
                TokenFilter::NGram {
                    min,
                    max,
                    preserve: p_bool(def, "preserve_original", false)?,
                    edge: false,
                }
            }
            "edge_ngram" | "edgeNGram" => TokenFilter::NGram {
                min: p_usize(def, "min_gram", 1)?,
                max: p_usize(def, "max_gram", 2)?,
                preserve: p_bool(def, "preserve_original", false)?,
                edge: true,
            },
            "word_delimiter" | "word_delimiter_graph" => {
                TokenFilter::WordDelimiter(Box::new(WordDelimiter {
                    graph: ty == "word_delimiter_graph",
                    generate_word_parts: p_bool(def, "generate_word_parts", true)?,
                    generate_number_parts: p_bool(def, "generate_number_parts", true)?,
                    catenate_words: p_bool(def, "catenate_words", false)?,
                    catenate_numbers: p_bool(def, "catenate_numbers", false)?,
                    catenate_all: p_bool(def, "catenate_all", false)?,
                    split_on_case_change: p_bool(def, "split_on_case_change", true)?,
                    split_on_numerics: p_bool(def, "split_on_numerics", true)?,
                    preserve_original: p_bool(def, "preserve_original", false)?,
                    stem_english_possessive: p_bool(def, "stem_english_possessive", true)?,
                    protected: p_list(def, "protected_words")
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                }))
            }
            "length" => TokenFilter::Length {
                min: p_usize(def, "min", 0)?,
                max: p_usize(def, "max", i32::MAX as usize)?,
            },
            "truncate" => {
                let n = p_usize(def, "length", 10)?;
                if n == 0 {
                    return Err(iae(format!("length parameter must be provided for [{name}]")));
                }
                TokenFilter::Truncate(n)
            }
            "unique" => {
                TokenFilter::Unique { same_position: p_bool(def, "only_on_same_position", false)? }
            }
            "remove_duplicates" => TokenFilter::RemoveDuplicates,
            "trim" => TokenFilter::Trim,
            "reverse" => TokenFilter::Reverse,
            "elision" => {
                let articles = p_list(def, "articles")
                    .map_or_else(default_articles, |v| v.into_iter().collect());
                let case = p_bool(def, "articles_case", false)?;
                let articles = if case {
                    articles
                } else {
                    articles.into_iter().map(|a| a.to_lowercase()).collect()
                };
                TokenFilter::Elision { articles, ignore_case: !case }
            }
            "apostrophe" => TokenFilter::Apostrophe,
            "synonym" | "synonym_graph" => {
                let map = self.synonym_map(name, def, prev)?;
                TokenFilter::Synonym { map: Rc::new(map), graph: ty == "synonym_graph" }
            }
            "keyword_marker" => {
                let pattern = p_str(def, "keywords_pattern");
                let words = p_list(def, "keywords");
                if pattern.is_some() && words.is_some() {
                    return Err(iae(
                        "cannot specify both `keywords_pattern` and `keywords` or `keywords_path`",
                    ));
                }
                if pattern.is_none() && words.is_none() && def.get("keywords_path").is_none() {
                    return Err(iae(
                        "keyword filter requires either `keywords`, `keywords_path`, or `keywords_pattern` to be configured",
                    ));
                }
                let ignore_case = p_bool(def, "ignore_case", false)?;
                TokenFilter::KeywordMarker {
                    words: words
                        .unwrap_or_default()
                        .into_iter()
                        .map(|w| if ignore_case { w.to_lowercase() } else { w })
                        .collect(),
                    pattern: pattern.map(|p| regex(p, "")).transpose()?,
                    ignore_case,
                }
            }
            "keyword_repeat" => TokenFilter::KeywordRepeat,
            "pattern_replace" => {
                let Some(pattern) = p_str(def, "pattern") else {
                    return Err(iae(format!(
                        "pattern is missing for [{name}] token filter of type 'pattern_replace'"
                    )));
                };
                TokenFilter::PatternReplace {
                    re: regex(pattern, p_str(def, "flags").unwrap_or(""))?,
                    replacement: p_str(def, "replacement").unwrap_or("").to_string(),
                    all: p_bool(def, "all", true)?,
                }
            }
            "limit" => TokenFilter::Limit { max: p_usize(def, "max_token_count", 1)? },
            "decimal_digit" => TokenFilter::DecimalDigit,
            "cjk_width" => TokenFilter::CjkWidth,
            "cjk_bigram" => {
                TokenFilter::CjkBigram { output_unigrams: p_bool(def, "output_unigrams", false)? }
            }
            "fingerprint" => TokenFilter::Fingerprint {
                separator: p_str(def, "separator").unwrap_or(" ").to_string(),
                max_output: p_usize(def, "max_output_size", 255)?,
            },
            "classic" => TokenFilter::Classic,
            "german_normalization" => TokenFilter::GermanNormalization,
            "delimited_payload" => TokenFilter::DelimitedPayload {
                delimiter: p_str(def, "delimiter").and_then(|s| s.chars().next()).unwrap_or('|'),
            },
            "keep" => {
                if def.get("keep_words").is_none() && def.get("keep_words_path").is_none() {
                    return Err(iae(
                        "keep requires either `keep_words` or `keep_words_path` to be configured",
                    ));
                }
                let case = p_bool(def, "keep_words_case", false)?;
                TokenFilter::Keep {
                    words: p_list(def, "keep_words")
                        .unwrap_or_default()
                        .into_iter()
                        .map(|w| if case { w.to_lowercase() } else { w })
                        .collect(),
                    case_sensitive: !case,
                }
            }
            "keep_types" => {
                let Some(types) = p_list(def, "types") else {
                    return Err(iae("keep_types requires `types` to be configured"));
                };
                let include = match p_str(def, "mode").unwrap_or("include") {
                    "include" => true,
                    "exclude" => false,
                    m => {
                        return Err(iae(format!(
                            "`mode` must be `include` or `exclude` for token filter [{name}] but was [{m}]"
                        )));
                    }
                };
                TokenFilter::KeepTypes { types: types.into_iter().collect(), include }
            }
            "pattern_capture" => {
                let Some(pats) = p_list_raw(def, "patterns") else {
                    return Err(iae(format!(
                        "required setting 'patterns' is missing for token filter [{name}]"
                    )));
                };
                let mut patterns = Vec::new();
                for p in &pats {
                    patterns.push(regex(p, "")?);
                }
                TokenFilter::PatternCapture {
                    patterns,
                    preserve: p_bool(def, "preserve_original", true)?,
                }
            }
            "common_grams" => {
                let words = p_list(def, "common_words").unwrap_or_default();
                if words.is_empty() && def.get("common_words_path").is_none() {
                    return Err(iae(
                        "missing or empty [common_words] or [common_words_path] configuration for common_grams token filter",
                    ));
                }
                TokenFilter::CommonGrams {
                    words: words.into_iter().collect(),
                    ignore_case: p_bool(def, "ignore_case", false)?,
                    query_mode: p_bool(def, "query_mode", false)?,
                }
            }
            "multiplexer" => {
                let mut chains = Vec::new();
                for entry in p_list_raw(def, "filters").unwrap_or_default() {
                    let mut chain = Vec::new();
                    for f in entry.split(',').map(str::trim).filter(|f| !f.is_empty()) {
                        match self.filter_named(f, prev) {
                            Some(r) => chain.push(r?),
                            None => {
                                return Err(iae(format!("Unknown token filter type [{f}]")));
                            }
                        }
                    }
                    chains.push(chain);
                }
                TokenFilter::Multiplexer {
                    chains,
                    preserve: p_bool(def, "preserve_original", true)?,
                }
            }
            "dictionary_decompounder" => {
                let words = p_list(def, "word_list").unwrap_or_default();
                if words.is_empty() && def.get("word_list_path").is_none() {
                    return Err(iae(format!(
                        "word_list must be provided for [{name}], either as a path to a file, or directly"
                    )));
                }
                TokenFilter::Decompounder {
                    words: words.into_iter().map(|w| w.to_lowercase()).collect(),
                    min_word: p_usize(def, "min_word_size", 5)?,
                    min_sub: p_usize(def, "min_subword_size", 2)?,
                    max_sub: p_usize(def, "max_subword_size", 15)?,
                    longest_only: p_bool(def, "only_longest_match", false)?,
                }
            }
            "scandinavian_folding" => TokenFilter::ScandinavianFolding,
            "scandinavian_normalization" => TokenFilter::ScandinavianNormalization,
            "flatten_graph" | "type_as_payload" | "standard" => TokenFilter::Identity,
            other => return Err(iae(format!("Unknown filter type [{other}] for [{name}]"))),
        })
    }

    fn synonym_map(
        &self,
        name: &str,
        def: &Value,
        prev: &Chain,
    ) -> Result<synonyms::SynonymMap, AnalysisError> {
        let rules: Vec<String> = if let Some(set) = p_str(def, "synonyms_set") {
            match (self.synonym_sets)(set) {
                Some(r) => r,
                None => {
                    return Err(AnalysisError::new(
                        "resource_not_found_exception",
                        format!("synonyms set [{set}] not found"),
                    ));
                }
            }
        } else if let Some(r) = p_list_raw(def, "synonyms") {
            r
        } else if def.get("synonyms_path").is_some() {
            return Err(iae(format!(
                "IOException while reading synonyms_path_path: {}",
                p_str(def, "synonyms_path").unwrap_or("")
            )));
        } else {
            return Err(iae(
                "synonym requires either `synonyms`, `synonyms_set` or `synonyms_path` to be configured",
            ));
        };
        let opts = ParseOptions {
            wordnet: p_str(def, "format") == Some("wordnet"),
            expand: p_bool(def, "expand", true)?,
            lenient: p_bool(def, "lenient", false)?,
        };
        let analyze = |s: &str| -> Vec<(String, u32)> {
            prev.run(s).into_iter().map(|t| (t.term, t.pos_inc)).collect()
        };
        synonyms::build(&rules, &opts, &analyze).map_err(|RuleError::Line(line, reason)| {
            let mut e = iae(format!("failed to build synonyms from ['{name}' analyzer settings]"));
            let mut parse = AnalysisError::new(
                "parse_exception",
                format!("Invalid synonym rule at line {line}"),
            );
            parse.caused_by = Some(Box::new(iae(reason)));
            e.caused_by = Some(Box::new(parse));
            e
        })
    }

    // ---- analyzers ----

    /// An analyzer by name (index definitions first, then built-ins).
    pub fn analyzer_named(&self, name: &str) -> Option<Result<Analyzer, AnalysisError>> {
        if let Some(def) = self.section("analyzer").and_then(|s| s.get(name)) {
            return Some(self.analyzer_def(name, def));
        }
        builtin_analyzer(name).map(Ok)
    }

    pub fn analyzer_def(&self, name: &str, def: &Value) -> Result<Analyzer, AnalysisError> {
        let ty = p_str(def, "type");
        let gap = p_usize(def, "position_increment_gap", 100)? as u32;
        match ty {
            Some("custom") | None => {
                if ty.is_none() && def.get("tokenizer").is_none() {
                    return Err(iae(format!(
                        "analyzer [{name}] must specify either an analyzer type, or a tokenizer"
                    )));
                }
                let Some(tok_name) = p_str(def, "tokenizer") else {
                    return Err(iae(format!(
                        "Custom Analyzer [{name}] must be configured with a tokenizer"
                    )));
                };
                let tokenizer = match self.tokenizer_named(tok_name) {
                    Some(t) => t?,
                    None => {
                        return Err(iae(format!(
                            "Custom Analyzer [{name}] failed to find tokenizer under name [{tok_name}]"
                        )));
                    }
                };
                let mut chain = Chain::new(tok_name, tokenizer);
                for cf in p_list(def, "char_filter").unwrap_or_default() {
                    match self.char_filter_named(&cf) {
                        Some(c) => chain.char_filters.push((cf.clone(), c?)),
                        None => {
                            return Err(iae(format!(
                                "Custom Analyzer [{name}] failed to find char_filter under name [{cf}]"
                            )));
                        }
                    }
                }
                for f in p_list(def, "filter").unwrap_or_default() {
                    match self.filter_named(&f, &chain) {
                        Some(tf) => chain.filters.push((f.clone(), tf?)),
                        None => {
                            return Err(iae(format!(
                                "Custom Analyzer [{name}] failed to find filter under name [{f}]"
                            )));
                        }
                    }
                }
                Ok(chain.into_analyzer(name, true, gap))
            }
            Some(t) => {
                let mut a = self.typed_analyzer(name, t, def)?;
                a.custom = false;
                a.position_increment_gap = p_usize(def, "position_increment_gap", 0)? as u32;
                Ok(a)
            }
        }
    }

    /// A non-custom analyzer type with its parameters.
    fn typed_analyzer(&self, name: &str, ty: &str, def: &Value) -> Result<Analyzer, AnalysisError> {
        let max_len = p_usize(def, "max_token_length", 255)?;
        let stop = |default: &[&str]| TokenFilter::Stop {
            words: word_set(def, "stopwords", default),
            ignore_case: false,
            remove_trailing: true,
        };
        let chain = match ty {
            "standard" => Chain::new("standard", Tokenizer::Standard { max_len })
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Default))
                .with("stop", stop(&[])),
            "simple" => {
                Chain::new("lowercase", Tokenizer::Letter { max_len: 255, lowercase: true })
            }
            "whitespace" => Chain::new("whitespace", Tokenizer::Whitespace { max_len }),
            "keyword" => Chain::new("keyword", Tokenizer::Keyword),
            "stop" => Chain::new("lowercase", Tokenizer::Letter { max_len: 255, lowercase: true })
                .with("stop", stop(stopwords::ENGLISH)),
            "pattern" => {
                let re = regex(
                    p_str(def, "pattern").unwrap_or(r"\W+"),
                    p_str(def, "flags").unwrap_or(""),
                )?;
                let mut c = Chain::new("pattern", Tokenizer::Pattern { re, group: -1 });
                if p_bool(def, "lowercase", true)? {
                    c = c.with("lowercase", TokenFilter::Lowercase(LowerLang::Default));
                }
                c.with("stop", stop(&[]))
            }
            "fingerprint" => Chain::new("standard", Tokenizer::Standard { max_len: 255 })
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Default))
                .with("asciifolding", TokenFilter::AsciiFolding { preserve: false })
                .with("stop", stop(&[]))
                .with(
                    "fingerprint",
                    TokenFilter::Fingerprint {
                        separator: p_str(def, "separator").unwrap_or(" ").to_string(),
                        max_output: p_usize(def, "max_output_size", 255)?,
                    },
                ),
            "snowball" => {
                let lang = p_str(def, "language").unwrap_or("English");
                let stem = if lang.eq_ignore_ascii_case("porter") {
                    Stem::Porter
                } else {
                    Stem::Snowball(
                        stemmers::snowball_algorithm(lang)
                            .ok_or_else(|| iae(format!("Unknown snowball language [{lang}]")))?,
                    )
                };
                let default_stop: &[&str] =
                    if lang.eq_ignore_ascii_case("english") { stopwords::ENGLISH } else { &[] };
                Chain::new("standard", Tokenizer::Standard { max_len: 255 })
                    .with("lowercase", TokenFilter::Lowercase(LowerLang::Default))
                    .with("stop", stop(default_stop))
                    .with("snowball", TokenFilter::Stem(stem))
            }
            "classic" => Chain::new("classic", Tokenizer::Classic { max_len: 255 })
                .with("classic", TokenFilter::Classic)
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Default))
                .with("stop", stop(stopwords::ENGLISH)),
            lang if LANGUAGES.contains(&lang) => language_analyzer(lang, def)?,
            other => return Err(iae(format!("Unknown analyzer type [{other}] for [{name}]"))),
        };
        Ok(chain.into_analyzer(name, false, 0))
    }

    // ---- normalizers ----

    pub fn normalizer_named(&self, name: &str) -> Option<Result<Analyzer, AnalysisError>> {
        if let Some(def) = self.section("normalizer").and_then(|s| s.get(name)) {
            return Some(self.normalizer_def(name, def));
        }
        match name {
            "lowercase" => Some(Ok(Chain::new("keyword", Tokenizer::Keyword)
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Default))
                .into_analyzer(name, true, 0))),
            _ => None,
        }
    }

    pub fn normalizer_def(&self, name: &str, def: &Value) -> Result<Analyzer, AnalysisError> {
        if let Some(t) = p_str(def, "type").filter(|t| *t != "custom") {
            return Err(iae(format!("Unknown normalizer type [{t}] for [{name}]")));
        }
        let mut chain = Chain::new("keyword", Tokenizer::Keyword);
        for cf in p_list(def, "char_filter").unwrap_or_default() {
            let c = match self.char_filter_named(&cf) {
                Some(c) => c?,
                None => {
                    return Err(iae(format!(
                        "Custom normalizer [{name}] failed to find char_filter under name [{cf}]"
                    )));
                }
            };
            if matches!(c, CharFilter::HtmlStrip { .. }) {
                return Err(iae(format!(
                    "Custom normalizer [{name}] may not use char filter [{cf}]"
                )));
            }
            chain.char_filters.push((cf, c));
        }
        for f in p_list(def, "filter").unwrap_or_default() {
            let tf = match self.filter_named(&f, &chain) {
                Some(t) => t?,
                None => {
                    return Err(iae(format!(
                        "Custom normalizer [{name}] failed to find filter under name [{f}]"
                    )));
                }
            };
            let allowed = matches!(
                tf,
                TokenFilter::Lowercase(_)
                    | TokenFilter::Uppercase
                    | TokenFilter::AsciiFolding { .. }
                    | TokenFilter::CjkWidth
                    | TokenFilter::DecimalDigit
                    | TokenFilter::Elision { .. }
                    | TokenFilter::GermanNormalization
                    | TokenFilter::PatternReplace { .. }
                    | TokenFilter::Trim
            );
            if !allowed {
                return Err(iae(format!("Custom normalizer [{name}] may not use filter [{f}]")));
            }
            chain.filters.push((f, tf));
        }
        Ok(chain.into_analyzer(name, true, 0))
    }

    /// Builds every component the index defines (Elasticsearch does so
    /// when the index is created, unused ones included).
    pub fn validate(&self) -> Result<(), AnalysisError> {
        if let Some(s) = self.section("tokenizer") {
            for (n, d) in s {
                self.tokenizer_def(n, d)?;
            }
        }
        if let Some(s) = self.section("char_filter") {
            for (n, d) in s {
                self.char_filter_def(n, d)?;
            }
        }
        let empty = Chain::new("standard", Tokenizer::Standard { max_len: 255 });
        if let Some(s) = self.section("filter") {
            for (n, d) in s {
                // Synonym filters are checked within their analyzers.
                if matches!(p_str(d, "type"), Some("synonym" | "synonym_graph")) {
                    continue;
                }
                self.filter_def(n, d, &empty)?;
            }
        }
        if let Some(s) = self.section("analyzer") {
            for (n, d) in s {
                if !d.is_object() {
                    return Err(AnalysisError::with_status(
                        "settings_exception",
                        format!(
                            "Failed to get setting group for [index.analysis.analyzer.] setting prefix and setting [index.analysis.analyzer.{n}] because of a missing '.'"
                        ),
                        500,
                    ));
                }
                self.analyzer_def(n, d)?;
            }
        }
        if let Some(s) = self.section("normalizer") {
            for (n, d) in s {
                self.normalizer_def(n, d)?;
            }
        }
        Ok(())
    }

    /// The synonym sets (`synonyms_set`) this index's filters use, with
    /// the analyzers using each through an `updateable` filter.
    pub fn synonym_set_users(&self) -> HashMap<String, Vec<String>> {
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        let filters = self.section("filter");
        if let Some(analyzers) = self.section("analyzer") {
            for (aname, adef) in analyzers {
                for f in p_list(adef, "filter").unwrap_or_default() {
                    if let Some(fdef) = filters.and_then(|s| s.get(&f))
                        && let Some(set) = p_str(fdef, "synonyms_set")
                    {
                        let users = out.entry(set.to_string()).or_default();
                        if !users.contains(aname) {
                            users.push(aname.clone());
                        }
                    }
                }
            }
        }
        for v in out.values_mut() {
            v.sort();
        }
        out
    }

    /// Every synonym set the index's filters name.
    pub fn synonym_sets_used(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .section("filter")
            .map(|s| {
                s.values().filter_map(|d| p_str(d, "synonyms_set")).map(str::to_string).collect()
            })
            .unwrap_or_default();
        out.sort();
        out.dedup();
        out
    }

    /// Filters an analyzer uses that only work at search time
    /// (`updateable: true`).
    pub fn updateable_filters(&self, analyzer: &str) -> Vec<String> {
        let Some(adef) = self.section("analyzer").and_then(|s| s.get(analyzer)) else {
            return Vec::new();
        };
        let filters = self.section("filter");
        p_list(adef, "filter")
            .unwrap_or_default()
            .into_iter()
            .filter(|f| {
                filters
                    .and_then(|s| s.get(f))
                    .is_some_and(|d| p_bool(d, "updateable", false).unwrap_or(false))
            })
            .collect()
    }
}

/// Synonym rules as given: an array, or one string (split on commas, as
/// a settings list is).
fn p_list_raw(o: &Value, k: &str) -> Option<Vec<String>> {
    p_list(o, k)
}

fn default_articles() -> HashSet<String> {
    ["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn invalid_stemmer(lang: &str) -> AnalysisError {
    let mut c = lang.chars();
    let cap: String = c.next().map(|f| f.to_uppercase().chain(c).collect()).unwrap_or_default();
    AnalysisError::new(
        "illegal_argument_exception",
        format!("Invalid stemmer class specified: {cap}"),
    )
}

fn stemmer_filter(lang: &str) -> Result<TokenFilter, AnalysisError> {
    let l = lang.to_ascii_lowercase();
    Ok(TokenFilter::Stem(match l.as_str() {
        "english" | "porter" => Stem::Porter,
        "light_english" | "kstem" => Stem::KStem,
        "minimal_english" => Stem::MinimalEnglish,
        "possessive_english" => Stem::Possessive,
        "porter2" => Stem::Snowball(rust_stemmers::Algorithm::English),
        "light_french" | "minimal_french" => Stem::Light(stemmers::Light::French),
        "light_german" | "minimal_german" => Stem::Light(stemmers::Light::German),
        "light_spanish" => Stem::Light(stemmers::Light::Spanish),
        "light_italian" => Stem::Light(stemmers::Light::Italian),
        "light_portuguese" | "minimal_portuguese" => Stem::Light(stemmers::Light::Portuguese),
        other => {
            let base = other
                .trim_start_matches("light_")
                .trim_start_matches("minimal_")
                .trim_end_matches("_rslp")
                .trim_end_matches("2");
            match stemmers::snowball_algorithm(base) {
                Some(a) => Stem::Snowball(a),
                None => return Err(invalid_stemmer(lang)),
            }
        }
    }))
}

/// Built-in token filter names.
fn builtin_filter(name: &str) -> Option<TokenFilter> {
    let empty = Value::Null;
    let _ = &empty;
    Some(match name {
        "lowercase" => TokenFilter::Lowercase(LowerLang::Default),
        "uppercase" => TokenFilter::Uppercase,
        "asciifolding" => TokenFilter::AsciiFolding { preserve: false },
        "stop" => TokenFilter::Stop {
            words: stopwords::ENGLISH.iter().map(|w| w.to_string()).collect(),
            ignore_case: false,
            remove_trailing: true,
        },
        "porter_stem" => TokenFilter::Stem(Stem::Porter),
        "scandinavian_folding" => TokenFilter::ScandinavianFolding,
        "scandinavian_normalization" => TokenFilter::ScandinavianNormalization,
        // Only legacy names verified against Elasticsearch; the others (french,
        // german, brazilian, czech, persian) stay unknown rather than approximate.
        "arabic_stem" | "dutch_stem" | "russian_stem" => {
            return stemmer_filter(name.trim_end_matches("_stem")).ok();
        }
        "kstem" => TokenFilter::Stem(Stem::KStem),
        "stemmer" => TokenFilter::Stem(Stem::Porter),
        "snowball" => TokenFilter::Stem(Stem::Snowball(rust_stemmers::Algorithm::English)),
        "shingle" => TokenFilter::Shingle(Shingle {
            min: 2,
            max: 2,
            output_unigrams: true,
            output_unigrams_if_no_shingles: false,
            separator: " ".into(),
            filler: "_".into(),
        }),
        "ngram" | "nGram" => TokenFilter::NGram { min: 1, max: 2, preserve: false, edge: false },
        "edge_ngram" | "edgeNGram" => {
            TokenFilter::NGram { min: 1, max: 1, preserve: false, edge: true }
        }
        "word_delimiter" | "word_delimiter_graph" => {
            TokenFilter::WordDelimiter(Box::new(WordDelimiter {
                graph: name == "word_delimiter_graph",
                generate_word_parts: true,
                generate_number_parts: true,
                catenate_words: false,
                catenate_numbers: false,
                catenate_all: false,
                split_on_case_change: true,
                split_on_numerics: true,
                preserve_original: false,
                stem_english_possessive: true,
                protected: HashSet::new(),
            }))
        }
        "length" => TokenFilter::Length { min: 0, max: i32::MAX as usize },
        "truncate" => TokenFilter::Truncate(10),
        "unique" => TokenFilter::Unique { same_position: false },
        "remove_duplicates" => TokenFilter::RemoveDuplicates,
        "trim" => TokenFilter::Trim,
        "reverse" => TokenFilter::Reverse,
        "elision" => TokenFilter::Elision { articles: default_articles(), ignore_case: true },
        "apostrophe" => TokenFilter::Apostrophe,
        "keyword_repeat" => TokenFilter::KeywordRepeat,
        "limit" => TokenFilter::Limit { max: 1 },
        "decimal_digit" => TokenFilter::DecimalDigit,
        "cjk_width" => TokenFilter::CjkWidth,
        "cjk_bigram" => TokenFilter::CjkBigram { output_unigrams: false },
        "fingerprint" => TokenFilter::Fingerprint { separator: " ".into(), max_output: 255 },
        "classic" => TokenFilter::Classic,
        "german_normalization" => TokenFilter::GermanNormalization,
        "delimited_payload" => TokenFilter::DelimitedPayload { delimiter: '|' },
        "flatten_graph" | "type_as_payload" => TokenFilter::Identity,
        _ => return None,
    })
}

/// A built-in analyzer by name.
pub fn builtin_analyzer(name: &str) -> Option<Analyzer> {
    let defs =
        Defs { analysis: &Value::Null, index_settings: &Value::Null, synonym_sets: &|_| None };
    let ty = match name {
        "default" => "standard",
        n => n,
    };
    if !matches!(
        ty,
        "standard"
            | "simple"
            | "whitespace"
            | "keyword"
            | "stop"
            | "pattern"
            | "fingerprint"
            | "snowball"
            | "classic"
    ) && !LANGUAGES.contains(&ty)
    {
        return None;
    }
    let mut a = defs.typed_analyzer(name, ty, &Value::Null).ok()?;
    a.custom = false;
    a.position_increment_gap = 0;
    Some(a)
}

/// Lucene's per-language analyzers: their tokenizer, normalization,
/// stopwords and stemmer (Snowball where Lucene uses a stemmer of its
/// own this doesn't have).
fn language_analyzer(lang: &str, def: &Value) -> Result<Chain, AnalysisError> {
    let defaults = stopwords::for_language(lang);
    let default_refs: Vec<&str> = defaults.iter().map(String::as_str).collect();
    let stop = TokenFilter::Stop {
        words: word_set(
            def,
            "stopwords",
            if lang == "english" { stopwords::ENGLISH } else { &default_refs },
        ),
        ignore_case: false,
        remove_trailing: true,
    };
    let exclusion: HashSet<String> =
        p_list(def, "stem_exclusion").unwrap_or_default().into_iter().collect();
    let lower = TokenFilter::Lowercase(LowerLang::Default);
    let mut chain = if lang == "thai" {
        Chain::new("thai", Tokenizer::Thai)
    } else {
        Chain::new("standard", Tokenizer::Standard { max_len: 255 })
    };
    let elision = |articles: &[&str]| TokenFilter::Elision {
        articles: articles.iter().map(|a| a.to_string()).collect(),
        ignore_case: true,
    };
    match lang {
        "english" => {
            chain = chain
                .with("possessive", TokenFilter::Stem(Stem::Possessive))
                .with("lowercase", lower)
        }
        "french" => {
            chain = chain
                .with(
                    "elision",
                    TokenFilter::Elision { articles: default_articles(), ignore_case: true },
                )
                .with("lowercase", lower)
        }
        "italian" => {
            chain = chain
                .with(
                    "elision",
                    elision(&[
                        "c", "l", "all", "dall", "dell", "nell", "sull", "coll", "pell", "gl",
                        "agl", "dagl", "degl", "negl", "sugl", "un", "m", "t", "s", "v", "d",
                    ]),
                )
                .with("lowercase", lower)
        }
        "catalan" => {
            chain = chain
                .with("elision", elision(&["d", "l", "m", "n", "s", "t"]))
                .with("lowercase", lower)
        }
        "irish" => {
            chain = chain
                .with("elision", elision(&["d", "m", "b"]))
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Irish))
        }
        "turkish" => {
            chain = chain
                .with("apostrophe", TokenFilter::Apostrophe)
                .with("lowercase", TokenFilter::Lowercase(LowerLang::Turkish))
        }
        "greek" => chain = chain.with("lowercase", TokenFilter::Lowercase(LowerLang::Greek)),
        "cjk" => {
            return Ok(chain
                .with("cjk_width", TokenFilter::CjkWidth)
                .with("lowercase", lower)
                .with("cjk_bigram", TokenFilter::CjkBigram { output_unigrams: false })
                .with("stop", stop));
        }
        "arabic" | "persian" | "sorani" | "bengali" | "hindi" => {
            chain = chain.with("lowercase", lower).with("decimal_digit", TokenFilter::DecimalDigit)
        }
        _ => chain = chain.with("lowercase", lower),
    }
    chain = chain.with("stop", stop);
    if !exclusion.is_empty() {
        chain = chain.with(
            "keyword_marker",
            TokenFilter::KeywordMarker { words: exclusion, pattern: None, ignore_case: false },
        );
    }
    if lang == "german" {
        chain = chain.with("german_normalization", TokenFilter::GermanNormalization);
    }
    let stem = match lang {
        "english" => Some(Stem::Porter),
        "french" => Some(Stem::Light(stemmers::Light::French)),
        "german" => Some(Stem::Light(stemmers::Light::German)),
        "spanish" => Some(Stem::Light(stemmers::Light::Spanish)),
        "italian" => Some(Stem::Light(stemmers::Light::Italian)),
        "portuguese" => Some(Stem::Light(stemmers::Light::Portuguese)),
        "brazilian" => stemmers::snowball_algorithm("portuguese").map(Stem::Snowball),
        "thai" | "cjk" => None,
        other => stemmers::snowball_algorithm(other).map(Stem::Snowball),
    };
    if let Some(s) = stem {
        chain = chain.with("stemmer", TokenFilter::Stem(s));
    }
    Ok(chain)
}

/// An analysis chain under construction.
#[derive(Clone)]
pub struct Chain {
    pub char_filters: Vec<(String, CharFilter)>,
    pub tokenizer: (String, Tokenizer),
    pub filters: Vec<(String, TokenFilter)>,
}

impl Chain {
    pub fn new(name: &str, t: Tokenizer) -> Self {
        Chain { char_filters: Vec::new(), tokenizer: (name.to_string(), t), filters: Vec::new() }
    }

    fn with(mut self, name: &str, f: TokenFilter) -> Self {
        self.filters.push((name.to_string(), f));
        self
    }

    /// The chain's tokens for `text` (as built so far).
    pub fn run(&self, text: &str) -> Vec<Token> {
        self.clone().into_analyzer("", true, 0).tokens(text)
    }

    pub fn into_analyzer(self, name: &str, custom: bool, gap: u32) -> Analyzer {
        Analyzer {
            name: name.to_string(),
            custom,
            char_filters: self.char_filters,
            tokenizer: self.tokenizer,
            filters: self.filters,
            position_increment_gap: gap,
        }
    }
}
