//! Synonym rules (Solr and WordNet formats) and the `synonym` /
//! `synonym_graph` token filters: Lucene's `SolrSynonymParser`,
//! `WordnetSynonymParser`, `SynonymFilter` and `SynonymGraphFilter`.

use std::collections::HashMap;

use super::token::{SYNONYM, Token};

/// Input term sequence -> (output term sequences, keep the original).
#[derive(Debug, Clone, Default)]
pub struct SynonymMap {
    entries: HashMap<Vec<String>, (Vec<Vec<String>>, bool)>,
    max_input: usize,
}

impl SynonymMap {
    fn add(&mut self, input: &[String], output: &[String], keep_orig: bool) {
        self.max_input = self.max_input.max(input.len());
        let e = self.entries.entry(input.to_vec()).or_insert_with(|| (Vec::new(), false));
        if !e.0.iter().any(|o| o == output) {
            e.0.push(output.to_vec());
        }
        e.1 |= keep_orig;
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Lucene's `SolrSynonymParser.split`: empty pieces are dropped, a
/// backslash escapes the next character (and is kept for `unescape`).
fn split(s: &str, sep: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    let sep: Vec<char> = sep.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i..].starts_with(&sep) {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            i += sep.len();
            continue;
        }
        let c = chars[i];
        i += 1;
        if c == '\\' {
            cur.push(c);
            if i >= chars.len() {
                break;
            }
            cur.push(chars[i]);
            i += 1;
            continue;
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn unescape(s: &str) -> String {
    let mut out = String::new();
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            if let Some(n) = it.next() {
                out.push(n);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Why a rule set failed to parse.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleError {
    /// `Invalid synonym rule at line N`, with the underlying reason.
    Line(usize, String),
}

/// A rule's phrases as (inputs, explicit outputs); `Err` with Lucene's
/// reason when the rule is malformed.
pub fn parse_solr_rule(rule: &str) -> Result<(Vec<String>, Option<Vec<String>>), String> {
    let sides = split(rule, "=>");
    if sides.len() > 2 {
        return Err("more than one explicit mapping specified on the same line".into());
    }
    let pieces = |s: &str| {
        split(s, ",").into_iter().map(|p| unescape(&p).trim().to_string()).collect::<Vec<_>>()
    };
    if sides.len() == 2 {
        Ok((pieces(&sides[0]), Some(pieces(&sides[1]))))
    } else if rule.contains("=>") && sides.len() == 1 {
        // `=> x` or `x =>`: one side is missing entirely.
        Ok((pieces(sides.first().map_or("", String::as_str)), Some(vec![String::new()])))
    } else {
        Ok((pieces(sides.first().map_or("", String::as_str)), None))
    }
}

/// Groups WordNet prolog lines (`s(id,n,'word',type,sense,tag).`) by
/// synset into equivalence rules.
fn wordnet_groups(rules: &[String]) -> Result<Vec<(usize, Vec<String>)>, RuleError> {
    let mut groups: Vec<(usize, String, Vec<String>)> = Vec::new();
    for (n, line) in rules.iter().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let bad = || RuleError::Line(n + 1, format!("Invalid synonym rule at line {}", n + 1));
        let body = line.strip_prefix("s(").ok_or_else(bad)?;
        let (id, rest) = body.split_once(',').ok_or_else(bad)?;
        let q1 = rest.find('\'').ok_or_else(bad)?;
        let rest2 = &rest[q1 + 1..];
        // The word ends at a quote not doubled ('' escapes a quote).
        let chars: Vec<char> = rest2.chars().collect();
        let mut word = String::new();
        let mut i = 0;
        loop {
            match chars.get(i) {
                Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                    word.push('\'');
                    i += 2;
                }
                Some('\'') => break,
                Some(c) => {
                    word.push(*c);
                    i += 1;
                }
                None => return Err(bad()),
            }
        }
        match groups.last_mut() {
            Some((_, gid, words)) if gid == id => words.push(word),
            _ => groups.push((n + 1, id.to_string(), vec![word])),
        }
    }
    Ok(groups.into_iter().map(|(line, _, w)| (line, w)).collect())
}

pub struct ParseOptions {
    pub wordnet: bool,
    pub expand: bool,
    pub lenient: bool,
}

/// Builds the map, analyzing each phrase with `analyze` (the analysis
/// chain before the synonym filter). `analyze` returns the terms and
/// whether any of them was stacked (position increment 0).
pub fn build(
    rules: &[String],
    opts: &ParseOptions,
    analyze: &dyn Fn(&str) -> Vec<(String, u32)>,
) -> Result<SynonymMap, RuleError> {
    let mut map = SynonymMap::default();
    let phrase = |s: &str| -> Result<Vec<String>, String> {
        let toks = analyze(s);
        if toks.is_empty() {
            return Err(format!("term: {s} was completely eliminated by analyzer"));
        }
        if let Some((t, _)) = toks.iter().skip(1).find(|t| t.1 == 0) {
            return Err(format!(
                "term: {s} analyzed to a token ({t}) with position increment != 1 (got: 0)"
            ));
        }
        Ok(toks.into_iter().map(|t| t.0).collect())
    };
    let mut groups: Vec<(usize, Vec<String>, Option<Vec<String>>)> = Vec::new();
    if opts.wordnet {
        for (line, words) in wordnet_groups(rules)? {
            groups.push((line, words, None));
        }
    } else {
        for (n, rule) in rules.iter().enumerate() {
            // Lines aren't trimmed: a trailing empty term is an error.
            let r = rule.as_str();
            if r.trim().is_empty() || r.starts_with('#') {
                continue;
            }
            let (inputs, outputs) = parse_solr_rule(r).map_err(|e| RuleError::Line(n + 1, e))?;
            groups.push((n + 1, inputs, outputs));
        }
    }
    for (line, inputs, outputs) in groups {
        let analyze_all = |xs: &[String]| -> Result<Vec<Vec<String>>, String> {
            let mut out = Vec::new();
            for x in xs {
                match phrase(x) {
                    Ok(p) => out.push(p),
                    Err(e) if opts.lenient => {
                        let _ = e;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(out)
        };
        let ins = analyze_all(&inputs).map_err(|e| RuleError::Line(line, e))?;
        match outputs {
            Some(outs) => {
                let outs = analyze_all(&outs).map_err(|e| RuleError::Line(line, e))?;
                for i in &ins {
                    for o in &outs {
                        map.add(i, o, false);
                    }
                }
            }
            None if opts.expand => {
                for (a, i) in ins.iter().enumerate() {
                    for (b, o) in ins.iter().enumerate() {
                        if a != b {
                            map.add(i, o, true);
                        }
                    }
                }
            }
            None => {
                if let Some(first) = ins.first() {
                    for i in &ins {
                        map.add(i, first, false);
                    }
                }
            }
        }
    }
    Ok(map)
}

/// The longest rule input matching `tokens[i..]` (consecutive tokens,
/// each one position after the previous).
fn longest_match<'a>(
    map: &'a SynonymMap,
    tokens: &[Token],
    i: usize,
) -> Option<(usize, &'a (Vec<Vec<String>>, bool))> {
    let mut best = None;
    let mut key: Vec<String> = Vec::new();
    for (k, t) in tokens.iter().enumerate().skip(i).take(map.max_input) {
        if k > i && t.pos_inc != 1 {
            break;
        }
        key.push(t.term.clone());
        if let Some(e) = map.entries.get(&key) {
            best = Some((k - i + 1, e));
        }
    }
    best
}

/// `synonym_graph`: each match becomes a proper graph (every output and
/// the kept original as parallel paths, positionLength spanning the
/// longest path).
pub fn apply_graph(map: &SynonymMap, tokens: Vec<Token>) -> Vec<Token> {
    if map.is_empty() {
        return tokens;
    }
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let Some((n, (outputs, keep))) = longest_match(map, &tokens, i) else {
            out.push(tokens[i].clone());
            i += 1;
            continue;
        };
        let inputs = &tokens[i..i + n];
        let (start, end) = (inputs[0].start, inputs[n - 1].end);
        // Paths: outputs first, then the original.
        let mut paths: Vec<Vec<Token>> = outputs
            .iter()
            .map(|o| {
                o.iter()
                    .map(|w| Token {
                        ty: SYNONYM.to_string(),
                        ..Token::new(w.clone(), start, end, SYNONYM)
                    })
                    .collect()
            })
            .collect();
        if *keep {
            paths.push(inputs.to_vec());
        }
        let span = paths.iter().map(Vec::len).max().unwrap_or(1).max(1);
        let first_inc = inputs[0].pos_inc;
        for pos in 0..span {
            let mut first_here = true;
            for p in &paths {
                if let Some(t) = p.get(pos) {
                    let mut t = t.clone();
                    t.pos_len = if pos + 1 == p.len() { (span - p.len() + 1) as u32 } else { 1 };
                    t.pos_inc = if first_here { if pos == 0 { first_inc } else { 1 } } else { 0 };
                    first_here = false;
                    out.push(t);
                }
            }
        }
        i += n;
    }
    out
}

/// `synonym` (Lucene's `SynonymFilter`): outputs are laid over the
/// following positions, mixing with the input tokens there.
pub fn apply_flat(map: &SynonymMap, tokens: Vec<Token>) -> Vec<Token> {
    if map.is_empty() {
        return tokens;
    }
    // (term, explicit end offset, position length) per future position.
    let mut future: std::collections::VecDeque<Vec<(String, Option<usize>, u32)>> =
        std::collections::VecDeque::new();
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    let mut match_end = 0;
    let mut match_keep = true;
    let mut last = (0usize, 0usize);
    while i < tokens.len() || !future.is_empty() {
        if i < tokens.len() {
            let tok = &tokens[i];
            if i >= match_end
                && let Some((n, (outputs, keep))) = longest_match(map, &tokens, i)
            {
                let end = tokens[i + n - 1].end;
                for o in outputs {
                    for (k, w) in o.iter().enumerate() {
                        while future.len() <= k {
                            future.push_back(Vec::new());
                        }
                        let single = o.len() == 1;
                        let pos_len = if single && *keep { n as u32 } else { 1 };
                        future[k].push((w.clone(), single.then_some(end), pos_len));
                    }
                }
                match_end = i + n;
                match_keep = *keep;
            }
            let keep_input = i >= match_end || match_keep;
            let mut first = true;
            if keep_input {
                out.push(tok.clone());
                first = false;
            }
            for (term, end, pos_len) in future.pop_front().unwrap_or_default() {
                let mut t = Token::new(term, tok.start, end.unwrap_or(tok.end), SYNONYM);
                t.pos_len = pos_len;
                t.pos_inc = if first { tok.pos_inc } else { 0 };
                first = false;
                out.push(t);
            }
            if first {
                // The input was replaced and nothing landed here: its
                // position is still taken by what follows.
            }
            last = (tok.start, tok.end);
            i += 1;
        } else {
            let mut first = true;
            for (term, end, pos_len) in future.pop_front().unwrap_or_default() {
                let mut t = Token::new(term, last.0, end.unwrap_or(last.1), SYNONYM);
                t.pos_len = pos_len;
                t.pos_inc = u32::from(first);
                first = false;
                out.push(t);
            }
        }
    }
    out
}

/// The synonyms API's check of one rule (`PUT _synonyms`): `Err` with
/// the validation message.
pub fn validate_api_rule(rule: &str) -> Result<(), String> {
    if rule.is_empty() {
        return Err("[synonyms] field can't be empty".into());
    }
    let opts = ParseOptions { wordnet: false, expand: true, lenient: false };
    let analyze = |s: &str| -> Vec<(String, u32)> {
        s.split_whitespace().map(|w| (w.to_lowercase(), 1)).collect()
    };
    match build(&[rule.to_string()], &opts, &analyze) {
        Ok(_) => Ok(()),
        Err(RuleError::Line(_, msg)) if msg.starts_with("more than one explicit") => Err(format!(
            "More than one explicit mapping specified in the same synonyms rule: [{rule}]"
        )),
        Err(_) => Err(format!("Incorrect syntax for [synonyms]: [{rule}]")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(s: &str) -> Vec<(String, u32)> {
        s.split_whitespace().map(|w| (w.to_lowercase(), 1)).collect()
    }

    fn toks(s: &str) -> Vec<Token> {
        let mut out = Vec::new();
        let mut off = 0;
        for w in s.split(' ') {
            out.push(Token::new(w, off, off + w.len(), "<ALPHANUM>"));
            off += w.len() + 1;
        }
        out
    }

    fn show(ts: &[Token]) -> Vec<(String, i64, u32)> {
        let pos = super::super::token::positions(ts);
        ts.iter().zip(pos).map(|(t, p)| (t.term.clone(), p, t.pos_len)).collect()
    }

    fn map(rules: &[&str]) -> SynonymMap {
        let rules: Vec<String> = rules.iter().map(|s| s.to_string()).collect();
        build(&rules, &ParseOptions { wordnet: false, expand: true, lenient: false }, &ws).unwrap()
    }

    #[test]
    fn graph_puts_synonyms_before_the_original() {
        let m = map(&["quick, fast, speedy", "new york, ny"]);
        let got = show(&apply_graph(&m, toks("the quick fox")));
        let terms: Vec<&str> = got.iter().map(|t| t.0.as_str()).collect();
        assert_eq!(terms, ["the", "fast", "speedy", "quick", "fox"]);
        let got = show(&apply_graph(&m, toks("ny")));
        assert_eq!(got, [("new".into(), 0, 1), ("ny".into(), 0, 2), ("york".into(), 1, 1)]);
    }

    #[test]
    fn flat_lays_outputs_over_following_positions() {
        let m = map(&["x => a b c", "y => d"]);
        let got = show(&apply_flat(&m, toks("x y")));
        assert_eq!(
            got,
            [("a".into(), 0, 1), ("b".into(), 1, 1), ("d".into(), 1, 1), ("c".into(), 2, 1)]
        );
    }

    #[test]
    fn api_rule_validation() {
        assert!(validate_api_rule("hello, hi").is_ok());
        assert!(validate_api_rule("a,,b").is_ok());
        assert_eq!(
            validate_api_rule("bye => => goodbye").unwrap_err(),
            "More than one explicit mapping specified in the same synonyms rule: [bye => => goodbye]"
        );
        assert!(validate_api_rule(" => goodbye").unwrap_err().starts_with("Incorrect syntax"));
        assert!(validate_api_rule("bye => ").unwrap_err().starts_with("Incorrect syntax"));
        assert!(validate_api_rule("bye, goodbye,  ").unwrap_err().starts_with("Incorrect syntax"));
    }

    #[test]
    fn wordnet_groups_by_synset() {
        let rules: Vec<String> =
            ["s(1,1,'hello',n,1,0).", "s(1,2,'hi',n,1,0).", "s(2,1,'bye',v,1,0)."]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let m = build(&rules, &ParseOptions { wordnet: true, expand: true, lenient: false }, &ws)
            .unwrap();
        let got = show(&apply_graph(&m, toks("hello")));
        assert_eq!(got.len(), 2);
    }
}
