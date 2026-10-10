//! Stemmers: Lucene's `PorterStemmer` (the `english` analyzer's and the
//! `porter_stem` filter's), `EnglishMinimalStemmer`, the possessive
//! filter, an approximation of KStem, and Snowball (`rust-stemmers`) for
//! the other languages.

/// Porter's original algorithm, as Lucene's `PorterStemmer` implements
/// it (including its `bli`/`logi` departures).
pub fn porter(word: &str) -> String {
    let chars: Vec<char> = word.chars().collect();
    if chars.len() <= 2 {
        return word.to_string();
    }
    let mut b = chars.clone();
    b.resize(chars.len() + 8, '\0');
    let mut p = Porter { b, k: chars.len() as isize - 1, j: 0 };
    p.step1ab();
    p.step1c();
    p.step2();
    p.step3();
    p.step4();
    p.step5();
    p.b[..=(p.k as usize)].iter().collect()
}

/// The C/Java implementation's state: the buffer, the end of the word
/// `k`, and the end of the stem `j` (signed, as `j` may be -1).
struct Porter {
    b: Vec<char>,
    k: isize,
    j: isize,
}

impl Porter {
    fn at(&self, i: isize) -> char {
        self.b[i as usize]
    }

    fn cons(&self, i: isize) -> bool {
        match self.at(i) {
            'a' | 'e' | 'i' | 'o' | 'u' => false,
            'y' => i == 0 || !self.cons(i - 1),
            _ => true,
        }
    }

    /// The number of consonant sequences between 0 and j.
    fn m(&self) -> usize {
        let mut n = 0;
        let mut i = 0;
        loop {
            if i > self.j {
                return n;
            }
            if !self.cons(i) {
                break;
            }
            i += 1;
        }
        i += 1;
        loop {
            loop {
                if i > self.j {
                    return n;
                }
                if self.cons(i) {
                    break;
                }
                i += 1;
            }
            i += 1;
            n += 1;
            loop {
                if i > self.j {
                    return n;
                }
                if !self.cons(i) {
                    break;
                }
                i += 1;
            }
            i += 1;
        }
    }

    fn vowel_in_stem(&self) -> bool {
        (0..=self.j).any(|i| !self.cons(i))
    }

    fn doublec(&self, j: isize) -> bool {
        j >= 1 && self.at(j) == self.at(j - 1) && self.cons(j)
    }

    fn cvc(&self, i: isize) -> bool {
        if i < 2 || !self.cons(i) || self.cons(i - 1) || !self.cons(i - 2) {
            return false;
        }
        !matches!(self.at(i), 'w' | 'x' | 'y')
    }

    fn ends(&mut self, s: &str) -> bool {
        let s: Vec<char> = s.chars().collect();
        let l = s.len() as isize;
        if l > self.k + 1 {
            return false;
        }
        let from = (self.k + 1 - l) as usize;
        if self.b[from..=(self.k as usize)] != s[..] {
            return false;
        }
        self.j = self.k - l;
        true
    }

    fn setto(&mut self, s: &str) {
        let mut i = (self.j + 1) as usize;
        for c in s.chars() {
            if i >= self.b.len() {
                self.b.push(c);
            } else {
                self.b[i] = c;
            }
            i += 1;
        }
        self.k = self.j + s.chars().count() as isize;
    }

    fn r(&mut self, s: &str) {
        if self.m() > 0 {
            self.setto(s);
        }
    }

    fn step1ab(&mut self) {
        if self.at(self.k) == 's' {
            if self.ends("sses") {
                self.k -= 2;
            } else if self.ends("ies") {
                self.setto("i");
            } else if self.at(self.k - 1) != 's' {
                self.k -= 1;
            }
        }
        if self.ends("eed") {
            if self.m() > 0 {
                self.k -= 1;
            }
        } else if (self.ends("ed") || self.ends("ing")) && self.vowel_in_stem() {
            self.k = self.j;
            if self.ends("at") {
                self.setto("ate");
            } else if self.ends("bl") {
                self.setto("ble");
            } else if self.ends("iz") {
                self.setto("ize");
            } else if self.doublec(self.k) {
                self.k -= 1;
                if matches!(self.at(self.k), 'l' | 's' | 'z') {
                    self.k += 1;
                }
            } else if self.m() == 1 && self.cvc(self.k) {
                self.setto("e");
            }
        }
    }

    fn step1c(&mut self) {
        if self.ends("y") && self.vowel_in_stem() {
            let k = self.k as usize;
            self.b[k] = 'i';
        }
    }

    fn step2(&mut self) {
        if self.k == 0 {
            return;
        }
        let rules: &[(&str, &str)] = match self.at(self.k - 1) {
            'a' => &[("ational", "ate"), ("tional", "tion")],
            'c' => &[("enci", "ence"), ("anci", "ance")],
            'e' => &[("izer", "ize")],
            'l' => {
                &[("bli", "ble"), ("alli", "al"), ("entli", "ent"), ("eli", "e"), ("ousli", "ous")]
            }
            'o' => &[("ization", "ize"), ("ation", "ate"), ("ator", "ate")],
            's' => &[("alism", "al"), ("iveness", "ive"), ("fulness", "ful"), ("ousness", "ous")],
            't' => &[("aliti", "al"), ("iviti", "ive"), ("biliti", "ble")],
            'g' => &[("logi", "log")],
            _ => &[],
        };
        for (suffix, rep) in rules {
            if self.ends(suffix) {
                self.r(rep);
                break;
            }
        }
    }

    fn step3(&mut self) {
        let rules: &[(&str, &str)] = match self.at(self.k) {
            'e' => &[("icate", "ic"), ("ative", ""), ("alize", "al")],
            'i' => &[("iciti", "ic")],
            'l' => &[("ical", "ic"), ("ful", "")],
            's' => &[("ness", "")],
            _ => &[],
        };
        for (suffix, rep) in rules {
            if self.ends(suffix) {
                self.r(rep);
                break;
            }
        }
    }

    fn step4(&mut self) {
        if self.k == 0 {
            return;
        }
        let hit = match self.at(self.k - 1) {
            'a' => self.ends("al"),
            'c' => self.ends("ance") || self.ends("ence"),
            'e' => self.ends("er"),
            'i' => self.ends("ic"),
            'l' => self.ends("able") || self.ends("ible"),
            'n' => self.ends("ant") || self.ends("ement") || self.ends("ment") || self.ends("ent"),
            'o' => {
                (self.ends("ion") && self.j >= 0 && matches!(self.at(self.j), 's' | 't'))
                    || self.ends("ou")
            }
            's' => self.ends("ism"),
            't' => self.ends("ate") || self.ends("iti"),
            'u' => self.ends("ous"),
            'v' => self.ends("ive"),
            'z' => self.ends("ize"),
            _ => false,
        };
        if hit && self.m() > 1 {
            self.k = self.j;
        }
    }

    fn step5(&mut self) {
        self.j = self.k;
        if self.at(self.k) == 'e' {
            let a = self.m();
            if a > 1 || (a == 1 && !self.cvc(self.k - 1)) {
                self.k -= 1;
            }
        }
        if self.at(self.k) == 'l' && self.doublec(self.k) && self.m() > 1 {
            self.k -= 1;
        }
    }
}

/// Lucene's `EnglishMinimalStemmer`: plurals only.
pub fn minimal_english(word: &str) -> String {
    let s: Vec<char> = word.chars().collect();
    let len = s.len();
    if len < 3 || s[len - 1] != 's' {
        return word.to_string();
    }
    let keep = |n: usize| s[..n].iter().collect::<String>();
    match s[len - 2] {
        'u' | 's' => word.to_string(),
        'e' => {
            if len > 3 && s[len - 3] == 'i' && s[len - 4] != 'a' && s[len - 4] != 'e' {
                let mut out = keep(len - 3);
                out.push('y');
                return out;
            }
            if matches!(s[len - 3], 'i' | 'a' | 'o' | 'e') {
                return word.to_string();
            }
            keep(len - 1)
        }
        _ => keep(len - 1),
    }
}

/// Lucene's `EnglishPossessiveFilter`: a trailing `'s`.
pub fn possessive(word: &str) -> String {
    let s: Vec<char> = word.chars().collect();
    let n = s.len();
    if n >= 2 && matches!(s[n - 1], 's' | 'S') && matches!(s[n - 2], '\'' | '\u{2019}' | '\u{FF07}')
    {
        s[..n - 2].iter().collect()
    } else {
        word.to_string()
    }
}

/// An approximation of Krovetz's KStem (`kstem`, `light_english`): its
/// inflectional rules without the dictionary that vets them.
pub fn kstem(word: &str) -> String {
    let w = word;
    if w.len() < 3 || !w.chars().all(|c| c.is_ascii_lowercase()) {
        return w.to_string();
    }
    let vowel = |c: char| "aeiou".contains(c);
    let ends = |s: &str| w.ends_with(s);
    let stem = |n: usize| w[..w.len() - n].to_string();
    if ends("ies") && w.len() > 4 {
        return format!("{}y", stem(3));
    }
    if ends("sses") || ends("xes") || ends("ches") || ends("shes") || ends("zes") {
        return stem(2);
    }
    if ends("ss") || ends("us") || ends("is") {
        return w.to_string();
    }
    if ends("s") && w.len() > 3 {
        return stem(1);
    }
    for suffix in ["ing", "ed"] {
        if ends(suffix) && w.len() > suffix.len() + 2 {
            let base = stem(suffix.len());
            if !base.chars().any(vowel) {
                return w.to_string();
            }
            let b: Vec<char> = base.chars().collect();
            let n = b.len();
            if n >= 2 && b[n - 1] == b[n - 2] && !"lsz".contains(b[n - 1]) {
                return b[..n - 1].iter().collect();
            }
            if n >= 2 && !vowel(b[n - 1]) && vowel(b[n - 2]) && !"wxy".contains(b[n - 1]) {
                return format!("{base}e");
            }
            if n >= 2 && "cgsvz".contains(b[n - 1]) {
                return format!("{base}e");
            }
            return base;
        }
    }
    w.to_string()
}

/// Lucene's "light" stemmers (Savoy's): the French, German, Spanish,
/// Italian and Portuguese language analyzers' stemmers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Light {
    French,
    German,
    Spanish,
    Italian,
    Portuguese,
}

pub fn light(lang: Light, word: &str) -> String {
    let mut s: Vec<char> = word.chars().collect();
    let len = match lang {
        Light::French => french_light(&mut s),
        Light::German => german_light(&mut s),
        Light::Spanish => spanish_light(&mut s),
        Light::Italian => italian_light(&mut s),
        Light::Portuguese => portuguese_light(&mut s),
    };
    s[..len.min(s.len())].iter().collect()
}

fn ends(s: &[char], len: usize, suffix: &str) -> bool {
    let suf: Vec<char> = suffix.chars().collect();
    len >= suf.len() && s[len - suf.len()..len] == suf[..]
}

fn fold_vowels(s: &mut [char], len: usize, extra: bool) {
    for c in s.iter_mut().take(len) {
        *c = match *c {
            'à' | 'á' | 'â' | 'ä' => 'a',
            'ã' if extra => 'a',
            'ò' | 'ó' | 'ô' | 'ö' => 'o',
            'õ' if extra => 'o',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ç' if extra => 'c',
            c => c,
        };
    }
}

fn french_light(s: &mut Vec<char>) -> usize {
    let mut len = s.len();
    if len > 5 && s[len - 1] == 'x' {
        if s[len - 3] == 'a' && s[len - 2] == 'u' && s[len - 4] != 'e' {
            s[len - 2] = 'l';
        }
        len -= 1;
    }
    if len > 3 && s[len - 1] == 'x' {
        len -= 1;
    }
    if len > 3 && s[len - 1] == 's' {
        len -= 1;
    }
    if len > 9 && ends(s, len, "issement") {
        len -= 6;
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 8 && ends(s, len, "issant") {
        len -= 4;
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 6 && ends(s, len, "ement") {
        len -= 4;
        if len > 3 && ends(s, len, "ive") {
            len -= 1;
            s[len - 1] = 'f';
        }
        return french_norm(s, len);
    }
    if len > 11 && ends(s, len, "ficatrice") {
        len -= 5;
        s[len - 2] = 'e';
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 10 && ends(s, len, "ficateur") {
        len -= 4;
        s[len - 2] = 'e';
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 9 && ends(s, len, "catrice") {
        len -= 3;
        s[len - 4] = 'q';
        s[len - 3] = 'u';
        s[len - 2] = 'e';
        return french_norm(s, len);
    }
    if len > 8 && ends(s, len, "cateur") {
        len -= 2;
        s[len - 4] = 'q';
        s[len - 3] = 'u';
        s[len - 2] = 'e';
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 8 && ends(s, len, "atrice") {
        len -= 4;
        s[len - 2] = 'e';
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 7 && ends(s, len, "ateur") {
        len -= 3;
        s[len - 2] = 'e';
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 6 && ends(s, len, "trice") {
        len -= 1;
        s[len - 3] = 'e';
        s[len - 2] = 'u';
        s[len - 1] = 'r';
    }
    if len > 5 && ends(s, len, "ième") {
        return french_norm(s, len - 4);
    }
    if len > 7 && ends(s, len, "teuse") {
        len -= 2;
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 6 && ends(s, len, "teur") {
        len -= 1;
        s[len - 1] = 'r';
        return french_norm(s, len);
    }
    if len > 5 && ends(s, len, "euse") {
        return french_norm(s, len - 2);
    }
    if len > 8 && ends(s, len, "ère") {
        len -= 1;
        s[len - 2] = 'e';
        return french_norm(s, len);
    }
    if len > 7 && ends(s, len, "ive") {
        len -= 1;
        s[len - 1] = 'f';
        return french_norm(s, len);
    }
    if len > 4 && (ends(s, len, "folle") || ends(s, len, "molle")) {
        len -= 2;
        s[len - 1] = 'u';
        return french_norm(s, len);
    }
    if len > 9 && ends(s, len, "nnelle") {
        return french_norm(s, len - 5);
    }
    if len > 9 && ends(s, len, "nnel") {
        return french_norm(s, len - 3);
    }
    if len > 4 && ends(s, len, "ète") {
        len -= 1;
        s[len - 2] = 'e';
    }
    if len > 8 && ends(s, len, "ique") {
        len -= 4;
    }
    if len > 8 && ends(s, len, "esse") {
        return french_norm(s, len - 3);
    }
    if len > 7 && ends(s, len, "inage") {
        return french_norm(s, len - 3);
    }
    if len > 9 && ends(s, len, "isation") {
        len -= 7;
        if len > 5 && ends(s, len, "ual") {
            s[len - 2] = 'e';
        }
        return french_norm(s, len);
    }
    if len > 9 && ends(s, len, "isateur") {
        return french_norm(s, len - 7);
    }
    if len > 8 && ends(s, len, "ation") {
        return french_norm(s, len - 5);
    }
    if len > 8 && ends(s, len, "ition") {
        return french_norm(s, len - 5);
    }
    french_norm(s, len)
}

fn french_norm(s: &mut Vec<char>, mut len: usize) -> usize {
    if len > 4 {
        for c in s.iter_mut().take(len) {
            *c = match *c {
                'à' | 'á' | 'â' => 'a',
                'ô' => 'o',
                'è' | 'é' | 'ê' => 'e',
                'ù' | 'û' => 'u',
                'î' => 'i',
                'ç' => 'c',
                c => c,
            };
        }
        // Repeated letters collapse ("pp" -> "p").
        let mut i = 1;
        let mut ch = s[0];
        while i < len {
            if s[i] == ch && ch.is_alphabetic() {
                s.remove(i);
                len -= 1;
            } else {
                ch = s[i];
                i += 1;
            }
        }
    }
    if len > 4 && ends(s, len, "ie") {
        len -= 2;
    }
    if len > 4 {
        if s[len - 1] == 'r' {
            len -= 1;
        }
        if s[len - 1] == 'e' {
            len -= 1;
        }
        if s[len - 1] == 'e' {
            len -= 1;
        }
        if s[len - 1] == s[len - 2] && s[len - 1].is_alphabetic() {
            len -= 1;
        }
    }
    len
}

fn german_light(s: &mut [char]) -> usize {
    let len = s.len();
    for c in s.iter_mut() {
        *c = match *c {
            'ä' | 'à' | 'á' | 'â' => 'a',
            'ö' | 'ò' | 'ó' | 'ô' => 'o',
            'ï' | 'ì' | 'í' | 'î' => 'i',
            'ü' | 'ù' | 'ú' | 'û' => 'u',
            c => c,
        };
    }
    let st_ending = |c: char| matches!(c, 'b' | 'd' | 'f' | 'g' | 'h' | 'k' | 'l' | 'm' | 'n' | 't');
    let step1 = |s: &[char], len: usize| -> usize {
        if len > 5 && s[len - 3] == 'e' && s[len - 2] == 'r' && s[len - 1] == 'n' {
            return len - 3;
        }
        if len > 4 && s[len - 2] == 'e' && matches!(s[len - 1], 'm' | 'n' | 'r' | 's') {
            return len - 2;
        }
        if len > 3 && s[len - 1] == 'e' {
            return len - 1;
        }
        if len > 3 && s[len - 1] == 's' && st_ending(s[len - 2]) {
            return len - 1;
        }
        len
    };
    let len = step1(s, len);
    if len > 5 && s[len - 3] == 'e' && s[len - 2] == 's' && s[len - 1] == 't' {
        return len - 3;
    }
    if len > 4 && s[len - 2] == 'e' && (s[len - 1] == 'r' || s[len - 1] == 'n') {
        return len - 2;
    }
    if len > 4 && s[len - 2] == 's' && s[len - 1] == 't' && st_ending(s[len - 3]) {
        return len - 2;
    }
    len
}

fn spanish_light(s: &mut [char]) -> usize {
    let len = s.len();
    if len < 5 {
        return len;
    }
    fold_vowels(s, len, false);
    match s[len - 1] {
        'o' | 'a' | 'e' => len - 1,
        's' => {
            if s[len - 2] == 'e' && s[len - 3] == 's' && s[len - 4] == 'e' {
                return len - 2;
            }
            if s[len - 2] == 'e' && s[len - 3] == 'c' {
                s[len - 3] = 'z';
                return len - 2;
            }
            if matches!(s[len - 2], 'o' | 'a' | 'e') {
                return len - 2;
            }
            len
        }
        _ => len,
    }
}

fn italian_light(s: &mut [char]) -> usize {
    let len = s.len();
    if len < 6 {
        return len;
    }
    fold_vowels(s, len, false);
    match s[len - 1] {
        'e' | 'i' if matches!(s[len - 2], 'i' | 'h') => len - 2,
        'a' | 'o' if s[len - 2] == 'i' => len - 2,
        'e' | 'i' | 'a' | 'o' => len - 1,
        _ => len,
    }
}

fn portuguese_light(s: &mut [char]) -> usize {
    let mut len = s.len();
    if len < 4 {
        return len;
    }
    len = portuguese_suffix(s, len);
    if len > 3 && s[len - 1] == 'a' {
        len = portuguese_feminine(s, len);
    }
    if len > 4 && matches!(s[len - 1], 'e' | 'a' | 'o') {
        len -= 1;
    }
    fold_vowels(s, len, true);
    len
}

fn portuguese_suffix(s: &mut [char], len: usize) -> usize {
    if len > 4 && ends(s, len, "es") && matches!(s[len - 3], 'r' | 's' | 'l' | 'z') {
        return len - 2;
    }
    if len > 3 && ends(s, len, "ns") {
        s[len - 2] = 'm';
        return len - 1;
    }
    if len > 4 && (ends(s, len, "eis") || ends(s, len, "éis")) {
        s[len - 3] = 'e';
        s[len - 2] = 'l';
        return len - 1;
    }
    if len > 4 && ends(s, len, "ais") {
        s[len - 2] = 'l';
        return len - 1;
    }
    if len > 4 && ends(s, len, "óis") {
        s[len - 3] = 'o';
        s[len - 2] = 'l';
        return len - 1;
    }
    if len > 4 && ends(s, len, "is") {
        s[len - 1] = 'l';
        return len;
    }
    if len > 3 && (ends(s, len, "ões") || ends(s, len, "ães")) {
        let len = len - 1;
        s[len - 2] = 'ã';
        s[len - 1] = 'o';
        return len;
    }
    if len > 6 && ends(s, len, "mente") {
        return len - 5;
    }
    if len > 3 && s[len - 1] == 's' {
        return len - 1;
    }
    len
}

fn portuguese_feminine(s: &mut [char], len: usize) -> usize {
    if len > 7 && (ends(s, len, "inha") || ends(s, len, "iaca") || ends(s, len, "eira")) {
        s[len - 1] = 'o';
        return len;
    }
    if len > 6 {
        if ["osa", "ica", "ida", "ada", "iva", "ama"].iter().any(|x| ends(s, len, x)) {
            s[len - 1] = 'o';
            return len;
        }
        if ends(s, len, "ona") {
            s[len - 3] = 'ã';
            s[len - 2] = 'o';
            return len - 1;
        }
        if ends(s, len, "ora") {
            return len - 1;
        }
        if ends(s, len, "esa") {
            s[len - 3] = 'ê';
            return len - 1;
        }
        if ends(s, len, "na") {
            s[len - 1] = 'o';
            return len;
        }
    }
    len
}

/// The Snowball stemmer for a (case-insensitive) language name.
pub fn snowball_algorithm(lang: &str) -> Option<rust_stemmers::Algorithm> {
    use rust_stemmers::Algorithm::*;
    Some(match lang.to_ascii_lowercase().as_str() {
        "arabic" => Arabic,
        "danish" => Danish,
        "dutch" => Dutch,
        "english" | "porter2" => English,
        "finnish" => Finnish,
        "french" => French,
        "german" | "german2" => German,
        "greek" => Greek,
        "hungarian" => Hungarian,
        "italian" => Italian,
        "norwegian" => Norwegian,
        "portuguese" => Portuguese,
        "romanian" => Romanian,
        "russian" => Russian,
        "spanish" => Spanish,
        "swedish" => Swedish,
        "tamil" => Tamil,
        "turkish" => Turkish,
        _ => return None,
    })
}

pub fn snowball(alg: rust_stemmers::Algorithm, word: &str) -> String {
    rust_stemmers::Stemmer::create(alg).stem(word).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn porter_matches_lucene() {
        let cases = [
            ("caresses", "caress"),
            ("ponies", "poni"),
            ("ties", "ti"),
            ("cats", "cat"),
            ("agreed", "agre"),
            ("plastered", "plaster"),
            ("motoring", "motor"),
            ("sing", "sing"),
            ("conflated", "conflat"),
            ("troubled", "troubl"),
            ("sized", "size"),
            ("hopping", "hop"),
            ("falling", "fall"),
            ("hissing", "hiss"),
            ("filing", "file"),
            ("happy", "happi"),
            ("sky", "sky"),
            ("relational", "relat"),
            ("conformabli", "conform"),
            ("radicalli", "radic"),
            ("vietnamization", "vietnam"),
            ("electriciti", "electr"),
            ("generalizations", "gener"),
            ("oscillators", "oscil"),
            ("dancing", "danc"),
            ("stars", "star"),
            ("lazy", "lazi"),
            ("toys", "toi"),
            ("knives", "knive"),
            ("controll", "control"),
            ("roll", "roll"),
            ("cease", "ceas"),
            ("rate", "rate"),
            ("is", "is"),
            ("ed", "ed"),
            ("bed", "bed"),
            ("shining", "shine"),
        ];
        for (w, s) in cases {
            assert_eq!(porter(w), s, "{w}");
        }
    }

    #[test]
    fn minimal_and_possessive() {
        assert_eq!(minimal_english("flies"), "fly");
        assert_eq!(minimal_english("boxes"), "boxe");
        assert_eq!(minimal_english("abilities"), "ability");
        assert_eq!(possessive("John's"), "John");
    }

    #[test]
    fn light_stemmers() {
        assert_eq!(light(Light::French, "chevaux"), "cheval");
        assert_eq!(light(Light::French, "étudiants"), "etudiant");
        assert_eq!(light(Light::French, "rapide"), "rapid");
        assert_eq!(light(Light::German, "hauser"), "haus");
        assert_eq!(light(Light::German, "strasse"), "strass");
        assert_eq!(light(Light::Spanish, "niños"), "niñ");
        assert_eq!(light(Light::Spanish, "rápidamente"), "rapidament");
        assert_eq!(light(Light::Portuguese, "foxes"), "foxe");
    }
}
