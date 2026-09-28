//! A hand-rolled parser for the slice of ClickHouse's SQL dialect this
//! milestone covers: scalar `SELECT` expressions, `system.one`, the
//! `numbers(N)` table function, `LIMIT` and `FORMAT`. ClickHouse's dialect
//! differs enough from Postgres's (table functions, different literal and
//! type-inference rules) that reusing `sqlparser`'s generic dialect isn't a
//! good fit here; this grows the same way the rest of noida-db's protocol
//! layers do, one milestone at a time. See docs/specs/clickhouse.md.

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Int(i64),
    Str(String),
    Call(String, Vec<Expr>),
    Ident(String),
    Star,
}

impl Expr {
    /// The column name ClickHouse gives this expression when it has no alias.
    pub fn default_name(&self) -> String {
        match self {
            Expr::Int(n) => n.to_string(),
            Expr::Str(s) => format!("'{s}'"),
            Expr::Call(name, args) => {
                let args: Vec<String> = args.iter().map(Expr::default_name).collect();
                format!("{name}({})", args.join(", "))
            }
            Expr::Ident(name) => name.clone(),
            Expr::Star => "*".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Table {
    /// No `FROM`: the select list is evaluated once, as scalars.
    None,
    SystemOne,
    SystemNumbers(u64),
    Unknown {
        database: String,
        table: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub items: Vec<Item>,
    pub table: Table,
    pub limit: Option<u64>,
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlError(pub String);

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
}

fn tokenize(sql: &str) -> Result<Vec<Token>, SqlError> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            ';' => i += 1,
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '\'' => {
                let mut j = i + 1;
                let mut s = String::new();
                loop {
                    if j >= chars.len() {
                        return Err(SqlError("unterminated string literal".into()));
                    }
                    if chars[j] == '\'' {
                        if chars.get(j + 1) == Some(&'\'') {
                            s.push('\'');
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    s.push(chars[j]);
                    j += 1;
                }
                tokens.push(Token::Str(s));
                i = j + 1;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                tokens.push(Token::Number(chars[start..i].iter().collect()));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                tokens.push(Token::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(SqlError(format!("unexpected character '{other}'"))),
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat(&mut self, t: &Token) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if let Some(Token::Ident(s)) = self.peek()
            && s.eq_ignore_ascii_case(kw)
        {
            self.pos += 1;
            return true;
        }
        false
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), SqlError> {
        if self.eat_keyword(kw) { Ok(()) } else { Err(SqlError(format!("expected {kw}"))) }
    }

    fn parse_ident(&mut self) -> Result<String, SqlError> {
        match self.next() {
            Some(Token::Ident(s)) => Ok(s),
            other => Err(SqlError(format!("expected an identifier, found {other:?}"))),
        }
    }

    fn parse_uint(&mut self) -> Result<u64, SqlError> {
        match self.next() {
            Some(Token::Number(n)) => {
                n.parse().map_err(|_| SqlError(format!("invalid number '{n}'")))
            }
            other => Err(SqlError(format!("expected a number, found {other:?}"))),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, SqlError> {
        match self.next() {
            Some(Token::Star) => Ok(Expr::Star),
            Some(Token::Number(n)) => {
                n.parse().map(Expr::Int).map_err(|_| SqlError(format!("invalid number '{n}'")))
            }
            Some(Token::Str(s)) => Ok(Expr::Str(s)),
            Some(Token::Ident(name)) => {
                if self.eat(&Token::LParen) {
                    let args = self.parse_arg_list()?;
                    Ok(Expr::Call(name, args))
                } else {
                    Ok(Expr::Ident(name))
                }
            }
            other => Err(SqlError(format!("unexpected token {other:?}"))),
        }
    }

    /// Parses comma-separated expressions up to a closing `)`, already
    /// having consumed the opening one.
    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, SqlError> {
        let mut args = Vec::new();
        if self.peek() != Some(&Token::RParen) {
            loop {
                args.push(self.parse_expr()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&Token::RParen) {
            return Err(SqlError("expected ')'".into()));
        }
        Ok(args)
    }

    fn parse_items(&mut self) -> Result<Vec<Item>, SqlError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if self.eat_keyword("AS") { Some(self.parse_ident()?) } else { None };
            items.push(Item { expr, alias });
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_table(&mut self) -> Result<Table, SqlError> {
        let first = self.parse_ident()?;
        let (database, name) =
            if self.eat(&Token::Dot) { (Some(first), self.parse_ident()?) } else { (None, first) };
        let args = if self.eat(&Token::LParen) { Some(self.parse_arg_list()?) } else { None };

        // `numbers(N)` is a table function, independent of database; it's
        // also reachable as `system.numbers`, but real usage almost always
        // calls it bare.
        if name.eq_ignore_ascii_case("numbers") {
            if let Some(args) = &args
                && let [Expr::Int(n)] = args.as_slice()
                && *n >= 0
            {
                return Ok(Table::SystemNumbers(*n as u64));
            }
            return Err(SqlError("numbers() expects one non-negative integer argument".into()));
        }
        if database.as_deref() == Some("system") && name == "one" && args.is_none() {
            return Ok(Table::SystemOne);
        }
        Ok(Table::Unknown { database: database.unwrap_or_else(|| "default".into()), table: name })
    }
}

/// Parses a single `SELECT` statement.
pub fn parse(sql: &str) -> Result<Select, SqlError> {
    let tokens = tokenize(sql)?;
    let mut p = Parser { tokens: &tokens, pos: 0 };
    p.expect_keyword("SELECT")?;
    let items = p.parse_items()?;
    let table = if p.eat_keyword("FROM") { p.parse_table()? } else { Table::None };
    let limit = if p.eat_keyword("LIMIT") { Some(p.parse_uint()?) } else { None };
    let format = if p.eat_keyword("FORMAT") { Some(p.parse_ident()?) } else { None };
    if p.pos != p.tokens.len() {
        return Err(SqlError("unexpected trailing input".into()));
    }
    Ok(Select { items, table, limit, format })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_one() {
        let s = parse("SELECT 1").unwrap();
        assert_eq!(s.items, [Item { expr: Expr::Int(1), alias: None }]);
        assert_eq!(s.table, Table::None);
        assert_eq!(s.limit, None);
        assert_eq!(s.format, None);
    }

    #[test]
    fn select_version_call() {
        let s = parse("SELECT version()").unwrap();
        assert_eq!(s.items, [Item { expr: Expr::Call("version".into(), vec![]), alias: None }]);
    }

    #[test]
    fn select_from_system_one() {
        let s = parse("SELECT * FROM system.one").unwrap();
        assert_eq!(s.items, [Item { expr: Expr::Star, alias: None }]);
        assert_eq!(s.table, Table::SystemOne);
    }

    #[test]
    fn select_from_numbers() {
        let s = parse("SELECT number FROM numbers(10) LIMIT 5").unwrap();
        assert_eq!(s.table, Table::SystemNumbers(10));
        assert_eq!(s.limit, Some(5));
    }

    #[test]
    fn select_with_format() {
        let s = parse("SELECT 1 FORMAT JSON").unwrap();
        assert_eq!(s.format, Some("JSON".into()));
    }

    #[test]
    fn select_unknown_table() {
        let s = parse("SELECT * FROM nope").unwrap();
        assert_eq!(s.table, Table::Unknown { database: "default".into(), table: "nope".into() });
    }

    #[test]
    fn select_with_alias() {
        let s = parse("SELECT 1 AS one").unwrap();
        assert_eq!(s.items, [Item { expr: Expr::Int(1), alias: Some("one".into()) }]);
    }

    #[test]
    fn trailing_semicolon_is_ignored() {
        assert!(parse("SELECT 1;").is_ok());
    }

    #[test]
    fn syntax_error_on_garbage() {
        assert!(parse("not sql").is_err());
        assert!(parse("SELECT 1 FROM").is_err());
        assert!(parse("SELECT 1,").is_err());
    }

    /// `SELECT FROM` parses fine (`FROM` reads as a bare column name, since
    /// keywords aren't reserved at the tokenizer level) — it's the engine
    /// that later rejects the unknown identifier. See engine::tests.
    #[test]
    fn select_from_as_bare_identifier() {
        let s = parse("SELECT FROM").unwrap();
        assert_eq!(s.items, [Item { expr: Expr::Ident("FROM".into()), alias: None }]);
        assert_eq!(s.table, Table::None);
    }

    #[test]
    fn default_names() {
        assert_eq!(Expr::Int(1).default_name(), "1");
        assert_eq!(Expr::Call("version".into(), vec![]).default_name(), "version()");
    }
}
