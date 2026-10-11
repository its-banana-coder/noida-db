//! A small backtracking matcher for the Java regular expressions that
//! `regex-lite` can't run: lookahead / lookbehind, backreferences, atomic
//! groups and possessive quantifiers (as in Elasticsearch's documented
//! `(?<=\p{Lower})(?=\p{Upper})` camel-case splitting). It works on
//! characters and reports character offsets.

use super::chars::{is_letter, lower};

type Pred = fn(char) -> bool;

#[derive(Debug, Clone)]
enum Item {
    Range(char, char),
    Pred(Pred, bool),
    Set(Set),
}

#[derive(Debug, Clone)]
struct Set {
    neg: bool,
    items: Vec<Item>,
}

impl Set {
    fn matches(&self, c: char, icase: bool) -> bool {
        let hit = |c: char| {
            self.items.iter().any(|it| match it {
                Item::Range(a, b) => *a <= c && c <= *b,
                Item::Pred(p, neg) => p(c) != *neg,
                Item::Set(s) => s.matches(c, false),
            })
        };
        let m = hit(c) || (icase && (hit(lower(c)) || hit(c.to_uppercase().next().unwrap_or(c))));
        m != self.neg
    }
}

#[derive(Debug, Clone)]
enum Node {
    Lit(char, bool),
    Set(Set, bool),
    Any(bool),
    Bol(bool),
    Eol(bool),
    WordB(bool),
    TextStart,
    TextEnd(bool),
    Group(Box<Node>, Option<usize>),
    Atomic(Box<Node>),
    Alt(Vec<Node>),
    Cat(Vec<Node>),
    Rep { node: Box<Node>, min: usize, max: usize, greedy: bool, possessive: bool },
    Look { node: Box<Node>, ahead: bool, neg: bool },
    Backref(usize, bool),
}

#[derive(Clone, Copy)]
struct Flags {
    icase: bool,
    dotall: bool,
    multiline: bool,
    comments: bool,
}

/// A compiled pattern.
#[derive(Debug, Clone)]
pub struct Program {
    root: Node,
    groups: usize,
    pub names: Vec<(String, usize)>,
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{B}' | '\u{C}' | '\r')
}

/// `\p{..}` property predicates (Java's POSIX classes are ASCII-only).
fn property(name: &str) -> Option<Pred> {
    let n = name.strip_prefix("Is").unwrap_or(name);
    Some(match n {
        "Lower" => |c: char| c.is_ascii_lowercase(),
        "Upper" => |c: char| c.is_ascii_uppercase(),
        "ASCII" => |c: char| c.is_ascii(),
        "Alpha" => |c: char| c.is_ascii_alphabetic(),
        "Digit" => |c: char| c.is_ascii_digit(),
        "Alnum" => |c: char| c.is_ascii_alphanumeric(),
        "Punct" => |c: char| c.is_ascii_punctuation(),
        "Graph" => |c: char| c.is_ascii_graphic(),
        "Print" => |c: char| c.is_ascii_graphic() || c == ' ',
        "Blank" => |c: char| c == ' ' || c == '\t',
        "Cntrl" => |c: char| c.is_ascii_control(),
        "XDigit" => |c: char| c.is_ascii_hexdigit(),
        "Space" | "White_Space" | "WhiteSpace" => |c: char| c.is_whitespace(),
        "javaLowerCase" | "Ll" | "Lowercase" | "LowerCase" => |c: char| c.is_lowercase(),
        "javaUpperCase" | "Lu" | "Uppercase" | "UpperCase" => |c: char| c.is_uppercase(),
        "javaWhitespace" => super::chars::is_java_whitespace,
        "L" | "Letter" | "Alphabetic" | "javaLetter" => is_letter,
        "N" | "Nd" | "javaDigit" => super::chars::is_digit,
        "P" | "Punctuation" => super::chars::is_punctuation,
        "S" => super::chars::is_symbol,
        "Z" | "Zs" => |c: char| c.is_whitespace(),
        "javaLetterOrDigit" => |c: char| c.is_alphanumeric(),
        _ => return None,
    })
}

struct Parser<'a> {
    p: &'a [char],
    i: usize,
    groups: usize,
    names: Vec<(String, usize)>,
    src: &'a str,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> String {
        format!("{what} near index {}\n{}", self.i, self.src)
    }

    fn peek(&self) -> Option<char> {
        self.p.get(self.i).copied()
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn skip_comments(&mut self, f: Flags) {
        if !f.comments {
            return;
        }
        loop {
            match self.peek() {
                Some(c) if c.is_whitespace() => self.i += 1,
                Some('#') => {
                    while let Some(c) = self.peek() {
                        self.i += 1;
                        if c == '\n' {
                            break;
                        }
                    }
                }
                _ => return,
            }
        }
    }

    fn alt(&mut self, f: &mut Flags) -> Result<Node, String> {
        let mut alts = vec![self.cat(f)?];
        while self.eat('|') {
            alts.push(self.cat(f)?);
        }
        Ok(if alts.len() == 1 { alts.pop().unwrap() } else { Node::Alt(alts) })
    }

    fn cat(&mut self, f: &mut Flags) -> Result<Node, String> {
        let mut items = Vec::new();
        loop {
            self.skip_comments(*f);
            match self.peek() {
                None | Some('|') | Some(')') => break,
                _ => {
                    let atom = self.atom(f)?;
                    let atom = self.quant(atom, *f)?;
                    items.push(atom);
                }
            }
        }
        Ok(if items.len() == 1 { items.pop().unwrap() } else { Node::Cat(items) })
    }

    fn number(&mut self) -> Option<usize> {
        let s = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        if self.i == s { None } else { self.p[s..self.i].iter().collect::<String>().parse().ok() }
    }

    fn quant(&mut self, atom: Node, f: Flags) -> Result<Node, String> {
        self.skip_comments(f);
        let (min, max) = match self.peek() {
            Some('*') => (0, usize::MAX),
            Some('+') => (1, usize::MAX),
            Some('?') => (0, 1),
            Some('{') => {
                let save = self.i;
                self.i += 1;
                let Some(min) = self.number() else {
                    self.i = save;
                    return Ok(atom);
                };
                let max = if self.eat(',') { self.number().unwrap_or(usize::MAX) } else { min };
                if !self.eat('}') {
                    return Err(self.err("Unclosed counted closure"));
                }
                self.i -= 1;
                (min, max)
            }
            _ => return Ok(atom),
        };
        self.i += 1;
        let (greedy, possessive) = if self.eat('?') {
            (false, false)
        } else if self.eat('+') {
            (true, true)
        } else {
            (true, false)
        };
        if matches!(atom, Node::Bol(_) | Node::Eol(_)) && min == 0 {
            return Ok(Node::Cat(vec![]));
        }
        Ok(Node::Rep { node: Box::new(atom), min, max, greedy, possessive })
    }

    fn atom(&mut self, f: &mut Flags) -> Result<Node, String> {
        let c = self.peek().ok_or_else(|| self.err("Unexpected end"))?;
        self.i += 1;
        Ok(match c {
            '.' => Node::Any(f.dotall),
            '^' => Node::Bol(f.multiline),
            '$' => Node::Eol(f.multiline),
            '[' => Node::Set(self.class(*f)?, f.icase),
            '(' => self.group(f)?,
            '\\' => self.escape(*f, false)?,
            '*' | '+' | '?' => return Err(self.err(&format!("Dangling meta character '{c}'"))),
            c => Node::Lit(c, f.icase),
        })
    }

    fn group(&mut self, f: &mut Flags) -> Result<Node, String> {
        let mut inner_flags = *f;
        let node = if self.eat('?') {
            match self.peek() {
                Some(':') => {
                    self.i += 1;
                    Node::Group(Box::new(self.alt(&mut inner_flags)?), None)
                }
                Some('=') | Some('!') => {
                    let neg = self.peek() == Some('!');
                    self.i += 1;
                    Node::Look { node: Box::new(self.alt(&mut inner_flags)?), ahead: true, neg }
                }
                Some('>') => {
                    self.i += 1;
                    Node::Atomic(Box::new(self.alt(&mut inner_flags)?))
                }
                Some('<') if matches!(self.p.get(self.i + 1), Some('=') | Some('!')) => {
                    let neg = self.p[self.i + 1] == '!';
                    self.i += 2;
                    Node::Look { node: Box::new(self.alt(&mut inner_flags)?), ahead: false, neg }
                }
                Some('<') => {
                    self.i += 1;
                    let s = self.i;
                    while self.peek().is_some_and(|c| c.is_ascii_alphanumeric()) {
                        self.i += 1;
                    }
                    let name: String = self.p[s..self.i].iter().collect();
                    if !self.eat('>') {
                        return Err(self.err("named capturing group is missing trailing '>'"));
                    }
                    self.groups += 1;
                    let g = self.groups;
                    self.names.push((name, g));
                    Node::Group(Box::new(self.alt(&mut inner_flags)?), Some(g))
                }
                _ => {
                    // Inline flags: `(?i)` or `(?i-s:...)`.
                    let mut on = true;
                    let mut nf = *f;
                    loop {
                        match self.peek() {
                            Some('-') => on = false,
                            Some('i') => nf.icase = on,
                            Some('s') => nf.dotall = on,
                            Some('m') => nf.multiline = on,
                            Some('x') => nf.comments = on,
                            Some('u') | Some('U') | Some('d') => {}
                            Some(')') => {
                                self.i += 1;
                                *f = nf;
                                return Ok(Node::Cat(vec![]));
                            }
                            Some(':') => {
                                self.i += 1;
                                let mut gf = nf;
                                let n = Node::Group(Box::new(self.alt(&mut gf)?), None);
                                if !self.eat(')') {
                                    return Err(self.err("Unclosed group"));
                                }
                                return Ok(n);
                            }
                            _ => return Err(self.err("Unknown inline modifier")),
                        }
                        self.i += 1;
                    }
                }
            }
        } else {
            self.groups += 1;
            let g = self.groups;
            Node::Group(Box::new(self.alt(&mut inner_flags)?), Some(g))
        };
        if !self.eat(')') {
            return Err(self.err("Unclosed group"));
        }
        Ok(node)
    }

    fn hex(&mut self, n: usize) -> Result<char, String> {
        let s: String =
            self.p.get(self.i..self.i + n).map(|c| c.iter().collect()).unwrap_or_default();
        self.i += n;
        u32::from_str_radix(&s, 16)
            .ok()
            .and_then(char::from_u32)
            .ok_or_else(|| self.err("Illegal hexadecimal escape sequence"))
    }

    /// An escape after `\`: a node (outside classes) or a set item.
    fn escape(&mut self, f: Flags, in_class: bool) -> Result<Node, String> {
        let c = self.peek().ok_or_else(|| self.err("Unexpected internal error"))?;
        self.i += 1;
        let pred = |p: Pred, neg: bool| {
            Node::Set(Set { neg: false, items: vec![Item::Pred(p, neg)] }, false)
        };
        Ok(match c {
            'd' => pred(|c| c.is_ascii_digit(), false),
            'D' => pred(|c| c.is_ascii_digit(), true),
            'w' => pred(is_word, false),
            'W' => pred(is_word, true),
            's' => pred(is_space, false),
            'S' => pred(is_space, true),
            'h' => pred(
                |c| {
                    matches!(
                        c,
                        ' ' | '\t' | '\u{A0}' | '\u{1680}' | '\u{2000}'
                            ..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}'
                    )
                },
                false,
            ),
            'p' | 'P' => {
                let name: String = if self.eat('{') {
                    let s = self.i;
                    while self.peek().is_some_and(|c| c != '}') {
                        self.i += 1;
                    }
                    let n = self.p[s..self.i].iter().collect();
                    if !self.eat('}') {
                        return Err(self.err("Unclosed character family"));
                    }
                    n
                } else {
                    let n = self.peek().map(String::from).unwrap_or_default();
                    self.i += 1;
                    n
                };
                let p = property(&name).ok_or_else(|| {
                    self.err(&format!("Unknown character property name {{{name}}}"))
                })?;
                pred(p, c == 'P')
            }
            'b' if !in_class => Node::WordB(false),
            'B' if !in_class => Node::WordB(true),
            'A' if !in_class => Node::TextStart,
            'z' if !in_class => Node::TextEnd(false),
            'Z' if !in_class => Node::TextEnd(true),
            't' => Node::Lit('\t', false),
            'n' => Node::Lit('\n', false),
            'r' => Node::Lit('\r', false),
            'f' => Node::Lit('\u{C}', false),
            'e' => Node::Lit('\u{1B}', false),
            'a' => Node::Lit('\u{7}', false),
            'x' => {
                let ch = if self.eat('{') {
                    let s = self.i;
                    while self.peek().is_some_and(|c| c != '}') {
                        self.i += 1;
                    }
                    let h: String = self.p[s..self.i].iter().collect();
                    self.i += 1;
                    u32::from_str_radix(&h, 16)
                        .ok()
                        .and_then(char::from_u32)
                        .ok_or_else(|| self.err("Illegal hexadecimal escape sequence"))?
                } else {
                    self.hex(2)?
                };
                Node::Lit(ch, f.icase)
            }
            'u' => Node::Lit(self.hex(4)?, f.icase),
            '0' => {
                let s = self.i;
                while self.i < s + 3 && self.peek().is_some_and(|c| ('0'..='7').contains(&c)) {
                    self.i += 1;
                }
                let o: String = self.p[s..self.i].iter().collect();
                Node::Lit(
                    u32::from_str_radix(&o, 8).ok().and_then(char::from_u32).unwrap_or('\0'),
                    f.icase,
                )
            }
            '1'..='9' if !in_class => {
                let mut n = c.to_digit(10).unwrap_or(0) as usize;
                while let Some(d) = self.peek().and_then(|c| c.to_digit(10)) {
                    let next = n * 10 + d as usize;
                    if next > self.groups {
                        break;
                    }
                    n = next;
                    self.i += 1;
                }
                Node::Backref(n, f.icase)
            }
            'k' if !in_class && self.peek() == Some('<') => {
                self.i += 1;
                let s = self.i;
                while self.peek().is_some_and(|c| c != '>') {
                    self.i += 1;
                }
                let name: String = self.p[s..self.i].iter().collect();
                self.i += 1;
                let g = self.names.iter().find(|(n, _)| *n == name).map(|(_, g)| *g).ok_or_else(
                    || self.err(&format!("named capturing group <{name}> does not exist")),
                )?;
                Node::Backref(g, f.icase)
            }
            'Q' if !in_class => {
                let mut lits = Vec::new();
                while self.i < self.p.len() {
                    if self.p[self.i] == '\\' && self.p.get(self.i + 1) == Some(&'E') {
                        self.i += 2;
                        break;
                    }
                    lits.push(Node::Lit(self.p[self.i], f.icase));
                    self.i += 1;
                }
                Node::Cat(lits)
            }
            c if c.is_ascii_alphabetic() => {
                return Err(self.err("Illegal/unsupported escape sequence"));
            }
            c => Node::Lit(c, f.icase),
        })
    }

    fn class(&mut self, f: Flags) -> Result<Set, String> {
        let neg = self.eat('^');
        let mut items = Vec::new();
        let mut first = true;
        loop {
            let c = self.peek().ok_or_else(|| self.err("Unclosed character class"))?;
            if c == ']' && !first {
                self.i += 1;
                break;
            }
            first = false;
            if c == '[' {
                self.i += 1;
                items.push(Item::Set(self.class(f)?));
                continue;
            }
            if c == '&' && self.p.get(self.i + 1) == Some(&'&') {
                // Intersection: approximated as union of what follows.
                self.i += 2;
                continue;
            }
            let lo = if c == '\\' {
                self.i += 1;
                match self.escape(f, true)? {
                    Node::Lit(ch, _) => ch,
                    Node::Set(s, _) => {
                        items.extend(s.items);
                        continue;
                    }
                    _ => return Err(self.err("Illegal escape in character class")),
                }
            } else {
                self.i += 1;
                c
            };
            if self.peek() == Some('-') && self.p.get(self.i + 1).is_some_and(|c| *c != ']') {
                self.i += 1;
                let hc = self.peek().unwrap_or(lo);
                self.i += 1;
                let hi = if hc == '\\' {
                    match self.escape(f, true)? {
                        Node::Lit(ch, _) => ch,
                        _ => return Err(self.err("Illegal character range")),
                    }
                } else {
                    hc
                };
                if hi < lo {
                    return Err(self.err("Illegal character range"));
                }
                items.push(Item::Range(lo, hi));
            } else {
                items.push(Item::Range(lo, lo));
            }
        }
        Ok(Set { neg, items })
    }
}

/// Compiles a Java pattern with Java flag names.
pub fn compile(pattern: &str, flags: &str) -> Result<Program, String> {
    let mut f = Flags { icase: false, dotall: false, multiline: false, comments: false };
    for name in flags.split('|').map(str::trim) {
        match name {
            "CASE_INSENSITIVE" => f.icase = true,
            "COMMENTS" => f.comments = true,
            "MULTILINE" => f.multiline = true,
            "DOTALL" => f.dotall = true,
            _ => {}
        }
    }
    let chars: Vec<char> = pattern.chars().collect();
    let mut p = Parser { p: &chars, i: 0, groups: 0, names: Vec::new(), src: pattern };
    let root = p.alt(&mut f)?;
    if p.i < chars.len() {
        return Err(p.err("Unmatched closing ')'"));
    }
    Ok(Program { root, groups: p.groups, names: p.names })
}

/// (min, max) width of what a node matches (`None`: unbounded).
fn width(n: &Node) -> (usize, Option<usize>) {
    match n {
        Node::Lit(..) | Node::Set(..) | Node::Any(_) => (1, Some(1)),
        Node::Bol(_)
        | Node::Eol(_)
        | Node::WordB(_)
        | Node::TextStart
        | Node::TextEnd(_)
        | Node::Look { .. } => (0, Some(0)),
        Node::Group(n, _) | Node::Atomic(n) => width(n),
        Node::Backref(..) => (0, None),
        Node::Alt(v) => {
            let ws: Vec<_> = v.iter().map(width).collect();
            let min = ws.iter().map(|w| w.0).min().unwrap_or(0);
            let max = ws.iter().try_fold(0usize, |m, w| w.1.map(|x| m.max(x)));
            (min, max)
        }
        Node::Cat(v) => v
            .iter()
            .map(width)
            .fold((0, Some(0)), |(a, b), (c, d)| (a + c, b.and_then(|b| d.map(|d| b + d)))),
        Node::Rep { node, min, max, .. } => {
            let (a, b) = width(node);
            let hi = if *max == usize::MAX {
                if b == Some(0) { Some(0) } else { None }
            } else {
                b.map(|b| b * max)
            };
            (a * min, hi)
        }
    }
}

type Caps = Vec<Option<(usize, usize)>>;

struct Matcher<'a> {
    t: &'a [char],
}

impl Matcher<'_> {
    fn single(&self, n: &Node, i: usize) -> bool {
        let Some(&c) = self.t.get(i) else { return false };
        match n {
            Node::Lit(l, icase) => c == *l || (*icase && lower(c) == lower(*l)),
            Node::Set(s, icase) => s.matches(c, *icase),
            Node::Any(dotall) => {
                *dotall || !matches!(c, '\n' | '\r' | '\u{85}' | '\u{2028}' | '\u{2029}')
            }
            _ => false,
        }
    }

    fn m(
        &self,
        n: &Node,
        i: usize,
        c: &mut Caps,
        k: &mut dyn FnMut(usize, &mut Caps) -> bool,
    ) -> bool {
        let len = self.t.len();
        match n {
            Node::Lit(..) | Node::Set(..) | Node::Any(_) => self.single(n, i) && k(i + 1, c),
            Node::Bol(ml) => (i == 0 || (*ml && self.t[i - 1] == '\n')) && k(i, c),
            Node::Eol(ml) => {
                let at = i == len
                    || (*ml && self.t[i] == '\n')
                    || (!*ml && i + 1 == len && self.t[i] == '\n');
                at && k(i, c)
            }
            Node::WordB(neg) => {
                let w = |j: usize| self.t.get(j).is_some_and(|c| c.is_alphanumeric() || *c == '_');
                let b = (i > 0 && w(i - 1)) != w(i);
                b != *neg && k(i, c)
            }
            Node::TextStart => i == 0 && k(i, c),
            Node::TextEnd(z) => (i == len || (*z && i + 1 == len && self.t[i] == '\n')) && k(i, c),
            Node::Cat(v) => self.seq(v, 0, i, c, k),
            Node::Alt(v) => v.iter().any(|a| self.m(a, i, c, k)),
            Node::Group(inner, None) => self.m(inner, i, c, k),
            Node::Group(inner, Some(g)) => {
                let g = *g;
                self.m(inner, i, c, &mut |e, c2: &mut Caps| {
                    let old = c2[g];
                    c2[g] = Some((i, e));
                    if k(e, c2) {
                        true
                    } else {
                        c2[g] = old;
                        false
                    }
                })
            }
            Node::Atomic(inner) => {
                let mut end = None;
                let mut c2 = c.clone();
                self.m(inner, i, &mut c2, &mut |e, cc: &mut Caps| {
                    end = Some((e, cc.clone()));
                    true
                });
                if let Some((e, cc)) = end {
                    let saved = std::mem::replace(c, cc);
                    if k(e, c) {
                        return true;
                    }
                    *c = saved;
                }
                false
            }
            Node::Look { node, ahead: true, neg } => {
                let mut c2 = c.clone();
                let ok = self.m(node, i, &mut c2, &mut |_, _| true);
                if *neg {
                    !ok && k(i, c)
                } else if ok {
                    let saved = std::mem::replace(c, c2);
                    if k(i, c) {
                        true
                    } else {
                        *c = saved;
                        false
                    }
                } else {
                    false
                }
            }
            Node::Look { node, ahead: false, neg } => {
                let (lo, hi) = width(node);
                let from = hi.map_or(0, |h| i.saturating_sub(h));
                let to = i.checked_sub(lo);
                let ok = to.is_some_and(|to| {
                    (from..=to).rev().any(|s| {
                        let mut c2 = c.clone();
                        self.m(node, s, &mut c2, &mut |e, _| e == i)
                    })
                });
                ok != *neg && k(i, c)
            }
            Node::Backref(g, icase) => {
                let Some(Some((s, e))) = c.get(*g).copied() else { return false };
                let n = e - s;
                if i + n > len {
                    return false;
                }
                let same = (0..n).all(|j| {
                    let (a, b) = (self.t[s + j], self.t[i + j]);
                    a == b || (*icase && lower(a) == lower(b))
                });
                same && k(i + n, c)
            }
            Node::Rep { node, min, max, greedy, possessive } => {
                if matches!(**node, Node::Lit(..) | Node::Set(..) | Node::Any(_)) {
                    // One character at a time: no recursion per repetition.
                    let mut run = 0;
                    while run < *max && self.single(node, i + run) {
                        run += 1;
                    }
                    if run < *min {
                        return false;
                    }
                    if *possessive {
                        return k(i + run, c);
                    }
                    if *greedy {
                        (*min..=run).rev().any(|n| k(i + n, c))
                    } else {
                        (*min..=run).any(|n| k(i + n, c))
                    }
                } else if *possessive {
                    let mut pos = i;
                    let mut count = 0;
                    while count < *max {
                        let mut next = None;
                        let mut c2 = c.clone();
                        self.m(node, pos, &mut c2, &mut |e, cc: &mut Caps| {
                            next = Some((e, cc.clone()));
                            true
                        });
                        match next {
                            Some((e, cc)) if e != pos => {
                                pos = e;
                                *c = cc;
                                count += 1;
                            }
                            _ => break,
                        }
                    }
                    count >= *min && k(pos, c)
                } else {
                    self.rep(node, *min, *max, *greedy, 0, i, c, k)
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn rep(
        &self,
        node: &Node,
        min: usize,
        max: usize,
        greedy: bool,
        count: usize,
        i: usize,
        c: &mut Caps,
        k: &mut dyn FnMut(usize, &mut Caps) -> bool,
    ) -> bool {
        if !greedy && count >= min && k(i, c) {
            return true;
        }
        if count < max {
            let more = self.m(node, i, c, &mut |e, c2: &mut Caps| {
                (e != i || count < min) && self.rep(node, min, max, greedy, count + 1, e, c2, k)
            });
            if more {
                return true;
            }
        }
        greedy && count >= min && k(i, c)
    }

    fn seq(
        &self,
        v: &[Node],
        j: usize,
        i: usize,
        c: &mut Caps,
        k: &mut dyn FnMut(usize, &mut Caps) -> bool,
    ) -> bool {
        if j == v.len() {
            return k(i, c);
        }
        self.m(&v[j], i, c, &mut |e, c2: &mut Caps| self.seq(v, j + 1, e, c2, k))
    }
}

impl Program {
    /// Every non-overlapping match, left to right, as group spans
    /// (group 0 the whole match) in character offsets.
    pub fn captures_all(&self, text: &[char]) -> Vec<Caps> {
        let mt = Matcher { t: text };
        let mut out = Vec::new();
        let mut start = 0;
        while start <= text.len() {
            let mut found: Option<Caps> = None;
            for s in start..=text.len() {
                let mut c: Caps = vec![None; self.groups + 1];
                if mt.m(&self.root, s, &mut c, &mut |e, cc: &mut Caps| {
                    let mut got = cc.clone();
                    got[0] = Some((s, e));
                    found = Some(got);
                    true
                }) {
                    break;
                }
            }
            let Some(caps) = found else { break };
            let (s, e) = caps[0].unwrap_or((start, start));
            start = if e == s { e + 1 } else { e };
            out.push(caps);
        }
        out
    }

    /// Whether the whole text matches.
    pub fn full_match(&self, text: &[char]) -> bool {
        let mt = Matcher { t: text };
        let mut c: Caps = vec![None; self.groups + 1];
        mt.m(&self.root, 0, &mut c, &mut |e, _| e == text.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans(p: &str, t: &str) -> Vec<(usize, usize)> {
        let chars: Vec<char> = t.chars().collect();
        compile(p, "").unwrap().captures_all(&chars).into_iter().filter_map(|c| c[0]).collect()
    }

    #[test]
    fn lookaround_and_groups() {
        assert_eq!(spans(r"(?<=\p{Lower})(?=\p{Upper})", "fooBarBaz"), [(3, 3), (6, 6)]);
        assert_eq!(spans(r"(\d+)-(?=\d)", "123-456-789"), [(0, 4), (4, 8)]);
        assert_eq!(spans(r"(a)\1", "xaay"), [(1, 3)]);
        assert_eq!(spans(r"a++b", "aaab"), [(0, 4)]);
        assert_eq!(spans(r"(?i)ab", "xAB"), [(1, 3)]);
        assert_eq!(spans(r"\w+", "hi there"), [(0, 2), (3, 8)]);
        assert_eq!(spans(r"(?:ab)+?", "ababx"), [(0, 2), (2, 4)]);
        assert!(compile(r"([^\p{L}\d]+)|(?<=\D)(?=\d)|(?<=\d)(?=\D)", "").is_ok());
    }
}
