//! Checks noida's software `long double` against real x87 hardware: a
//! small C program does what Redis's INCRBYFLOAT does (string2ld, +,
//! ld2string human) and every result must match byte for byte.
//!
//! Needs a C compiler (`cc`) on x86-64; skipped otherwise.

use std::io::Write;
use std::process::{Command, Stdio};

use noida::redis::longdouble::parse;

const ORACLE: &str = r#"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Redis's string2ld. */
static int string2ld(const char *s, long double *dp) {
    char *eptr;
    size_t slen = strlen(s);
    if (slen == 0 || slen >= 5*1024) return 0;
    errno = 0;
    long double value = strtold(s, &eptr);
    if (isspace(s[0]) || eptr[0] != '\0' ||
        (errno == ERANGE && (value == HUGE_VAL || value == -HUGE_VAL || fpclassify(value) == FP_ZERO)) ||
        isnan(value)) return 0;
    *dp = value;
    return 1;
}

int main(void) {
    char a[512], b[512];
    static char buf[5*1024];
    while (scanf("%511s %511s", a, b) == 2) {
        long double x, y;
        if (!string2ld(a, &x) || !string2ld(b, &y)) { puts("INVALID"); continue; }
        long double v = x + y;
        if (isnan(v) || isinf(v)) { puts("NANINF"); continue; }
        int l = snprintf(buf, sizeof(buf), "%.17Lf", v);
        if (l + 1 > (int)sizeof(buf)) { puts(""); continue; }
        if (strchr(buf, '.')) {
            char *p = buf + l - 1;
            while (*p == '0') { p--; l--; }
            if (*p == '.') l--;
        }
        if (l == 2 && buf[0] == '-' && buf[1] == '0') { buf[0] = '0'; l = 1; }
        buf[l] = '\0';
        puts(buf);
    }
    return 0;
}
"#;

/// A small deterministic PRNG, so failures reproduce.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn digits(&mut self, n: u64) -> String {
        (0..n).map(|_| char::from(b'0' + self.below(10) as u8)).collect()
    }

    fn number(&mut self) -> String {
        let sign = ["", "-", "+"][self.below(3) as usize];
        let kind = self.below(10);
        let (n1, n2, n3) = (1 + self.below(4), 1 + self.below(12), 1 + self.below(25));
        let (d1, d2, d3) = (self.digits(n1), self.digits(n2), self.digits(n3));
        let small_exp = self.below(700) as i64 - 350;
        let wide_exp = self.below(9900) as i64 - 4950;
        let (n4, n5, n6) = (1 + self.below(19), 1 + self.below(40), 1 + self.below(30));
        let (one, long) = (self.digits(1), self.digits(n4));
        let frac40 = self.digits(n5);
        let int30 = self.digits(n6);
        let odd =
            ["inf", "-inf", "nan", "abc", "1e", ".", "1.2.3", "0x1p3", "1e99999", "4.9e-4960"]
                [self.below(10) as usize];
        let n = self.below(1000);
        match kind {
            0 => format!("{sign}{}", n % 100),
            1 => format!("{sign}{d1}.{d1}"),
            2 => format!("{sign}{d2}.{d3}"),
            3 => format!("{sign}{d1}e{small_exp}"),
            4 => format!("{sign}{one}.{long}e{wide_exp}"),
            5 => format!("{sign}0.{frac40}"),
            6 => format!("{sign}{int30}"),
            7 => odd.into(),
            8 => format!("{sign}.{d1}"),
            _ => format!("{sign}{n}.5"),
        }
    }
}

#[test]
fn incrbyfloat_arithmetic_matches_x87() {
    if !cfg!(target_arch = "x86_64") {
        eprintln!("SKIPPED: the reference is x87 long double (x86-64 only)");
        return;
    }
    let dir = std::env::temp_dir().join(format!("noida-ld-oracle-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("oracle.c");
    let bin = dir.join("oracle");
    std::fs::write(&src, ORACLE).unwrap();
    let compiled = Command::new("cc").arg("-O1").arg("-o").arg(&bin).arg(&src).arg("-lm").status();
    if !compiled.is_ok_and(|s| s.success()) {
        eprintln!("SKIPPED: no C compiler");
        return;
    }

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    // Big-number work is slow unoptimized; debug builds check fewer pairs.
    let n = if cfg!(debug_assertions) { 3_000 } else { 20_000 };
    let pairs: Vec<(String, String)> = (0..n).map(|_| (rng.number(), rng.number())).collect();
    let input: String = pairs.iter().map(|(a, b)| format!("{a} {b}\n")).collect();

    let mut child =
        Command::new(&bin).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    // Feed stdin from another thread so the pipes can't deadlock.
    let mut stdin = child.stdin.take().unwrap();
    let feeder = std::thread::spawn(move || stdin.write_all(input.as_bytes()).unwrap());
    let out = child.wait_with_output().unwrap();
    feeder.join().unwrap();
    let expected: Vec<String> =
        String::from_utf8(out.stdout).unwrap().lines().map(String::from).collect();
    assert_eq!(expected.len(), pairs.len());

    let mut failures = Vec::new();
    for ((a, b), want) in pairs.iter().zip(&expected) {
        let got = match (parse(a.as_bytes()), parse(b.as_bytes())) {
            (Ok(x), Ok(y)) => {
                let v = x.plus(y);
                if v.is_finite() { v.to_human() } else { "NANINF".into() }
            }
            _ => "INVALID".into(),
        };
        if &got != want {
            failures.push(format!("{a} + {b}\n    x87:   {want}\n    noida: {got}"));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        failures.is_empty(),
        "{} of {} differ, first ones:\n  {}",
        failures.len(),
        pairs.len(),
        failures.iter().take(10).cloned().collect::<Vec<_>>().join("\n  ")
    );
}
