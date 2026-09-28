//! A hand-rolled parser for the slice of ClickHouse's SQL dialect this
//! project covers: scalar and table `SELECT` (with `WHERE`/`GROUP BY`/
//! `ORDER BY`/`LIMIT`/`FORMAT`), `CREATE TABLE ... ENGINE = Memory|
//! MergeTree`, `INSERT INTO ... VALUES`, `DROP TABLE`, `system.one`, the
//! `numbers(N)` table function and `system.tables`. ClickHouse's dialect
//! differs enough from Postgres's (table functions, different literal and
//! type-inference rules) that reusing `sqlparser`'s generic dialect isn't a
//! good fit here; this grows the same way the rest of noida-db's protocol
//! layers do, one milestone at a time. See docs/specs/clickhouse.md.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl BinOp {
    fn symbol(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Eq => "=",
            BinOp::Ne => "!=",
            BinOp::Lt => "<",
            BinOp::Le => "<=",
            BinOp::Gt => ">",
            BinOp::Ge => ">=",
            BinOp::And => "AND",
            BinOp::Or => "OR",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Call(String, Vec<Expr>),
    Ident(String),
    Star,
    BinOp(BinOp, Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    /// The column name ClickHouse gives this expression when it has no alias.
    pub fn default_name(&self) -> String {
        match self {
            Expr::Int(n) => n.to_string(),
            Expr::Float(f) => f.to_string(),
            Expr::Str(s) => format!("'{s}'"),
            Expr::Bool(b) => b.to_string(),
            Expr::Call(name, args) => {
                let args: Vec<String> = args.iter().map(Expr::default_name).collect();
                format!("{name}({})", args.join(", "))
            }
            Expr::Ident(name) => name.clone(),
            Expr::Star => "*".to_string(),
            Expr::BinOp(op, l, r) => {
                format!("{} {} {}", l.default_name(), op.symbol(), r.default_name())
            }
            Expr::Neg(e) => format!("-{}", e.default_name()),
            Expr::Not(e) => format!("NOT {}", e.default_name()),
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
    SystemTables,
    Named {
        database: Option<String>,
        table: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub items: Vec<Item>,
    pub table: Table,
    pub where_: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<u64>,
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub database: Option<String>,
    pub table: String,
    pub columns: Vec<(String, String)>,
    pub engine: String,
    pub order_by: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub database: Option<String>,
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTable {
    pub if_exists: bool,
    pub database: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Select),
    CreateTable(CreateTable),
    Insert(Insert),
    DropTable(DropTable),
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
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
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
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '!' if chars.get(i + 1) == Some(&'=') => {
                tokens.push(Token::Ne);
                i += 2;
            }
            '<' if chars.get(i + 1) == Some(&'>') => {
                tokens.push(Token::Ne);
                i += 2;
            }
            '<' if chars.get(i + 1) == Some(&'=') => {
                tokens.push(Token::Le);
                i += 2;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
            }
            '>' if chars.get(i + 1) == Some(&'=') => {
                tokens.push(Token::Ge);
                i += 2;
            }
            '>' => {
                tokens.push(Token::Gt);
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
                if chars.get(i) == Some(&'.') && chars.get(i + 1).is_some_and(char::is_ascii_digit)
                {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
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

    fn expect(&mut self, t: &Token) -> Result<(), SqlError> {
        if self.eat(t) { Ok(()) } else { Err(SqlError(format!("expected {t:?}"))) }
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

    /// A possibly `db.`-qualified name.
    fn parse_qualified_name(&mut self) -> Result<(Option<String>, String), SqlError> {
        let first = self.parse_ident()?;
        if self.eat(&Token::Dot) {
            Ok((Some(first), self.parse_ident()?))
        } else {
            Ok((None, first))
        }
    }

    // Expressions, lowest to highest precedence: OR, AND, NOT, comparison
    // (non-chaining), +/-, `*`//`%`, unary minus, primary.

    fn parse_expr(&mut self) -> Result<Expr, SqlError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, SqlError> {
        let mut lhs = self.parse_and()?;
        while self.eat_keyword("OR") {
            let rhs = self.parse_and()?;
            lhs = Expr::BinOp(BinOp::Or, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr, SqlError> {
        let mut lhs = self.parse_not()?;
        while self.eat_keyword("AND") {
            let rhs = self.parse_not()?;
            lhs = Expr::BinOp(BinOp::And, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Expr, SqlError> {
        if self.eat_keyword("NOT") {
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Expr, SqlError> {
        let lhs = self.parse_add()?;
        let op = match self.peek() {
            Some(Token::Eq) => BinOp::Eq,
            Some(Token::Ne) => BinOp::Ne,
            Some(Token::Lt) => BinOp::Lt,
            Some(Token::Le) => BinOp::Le,
            Some(Token::Gt) => BinOp::Gt,
            Some(Token::Ge) => BinOp::Ge,
            _ => return Ok(lhs),
        };
        self.pos += 1;
        let rhs = self.parse_add()?;
        Ok(Expr::BinOp(op, Box::new(lhs), Box::new(rhs)))
    }

    fn parse_add(&mut self) -> Result<Expr, SqlError> {
        let mut lhs = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinOp::Add,
                Some(Token::Minus) => BinOp::Sub,
                _ => break,
            };
            self.pos += 1;
            let rhs = self.parse_mul()?;
            lhs = Expr::BinOp(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_mul(&mut self) -> Result<Expr, SqlError> {
        let mut lhs = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinOp::Mul,
                Some(Token::Slash) => BinOp::Div,
                Some(Token::Percent) => BinOp::Mod,
                _ => break,
            };
            self.pos += 1;
            let rhs = self.parse_unary()?;
            lhs = Expr::BinOp(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<Expr, SqlError> {
        if self.eat(&Token::Minus) {
            return Ok(Expr::Neg(Box::new(self.parse_unary()?)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, SqlError> {
        match self.next() {
            Some(Token::Star) => Ok(Expr::Star),
            Some(Token::Number(n)) => {
                if n.contains('.') {
                    n.parse()
                        .map(Expr::Float)
                        .map_err(|_| SqlError(format!("invalid number '{n}'")))
                } else {
                    n.parse().map(Expr::Int).map_err(|_| SqlError(format!("invalid number '{n}'")))
                }
            }
            Some(Token::Str(s)) => Ok(Expr::Str(s)),
            Some(Token::LParen) => {
                let e = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(e)
            }
            Some(Token::Ident(name)) if name.eq_ignore_ascii_case("true") => Ok(Expr::Bool(true)),
            Some(Token::Ident(name)) if name.eq_ignore_ascii_case("false") => Ok(Expr::Bool(false)),
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

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, SqlError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.eat(&Token::Comma) {
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    fn parse_order_list(&mut self) -> Result<Vec<(Expr, bool)>, SqlError> {
        let mut out = Vec::new();
        loop {
            let e = self.parse_expr()?;
            let desc = if self.eat_keyword("DESC") {
                true
            } else {
                self.eat_keyword("ASC");
                false
            };
            out.push((e, desc));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(out)
    }

    fn parse_table(&mut self) -> Result<Table, SqlError> {
        let (database, name) = self.parse_qualified_name()?;
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
        if database.as_deref() == Some("system") && args.is_none() {
            match name.as_str() {
                "one" => return Ok(Table::SystemOne),
                "tables" => return Ok(Table::SystemTables),
                _ => {}
            }
        }
        Ok(Table::Named { database, table: name })
    }

    /// A column type name: an identifier, optionally with parenthesized
    /// arguments (`Decimal(10, 2)`, `Nullable(String)`) which are parsed
    /// (so the syntax is accepted) but discarded — those types aren't
    /// supported yet, so resolving the bare name later reports
    /// `NOT_IMPLEMENTED` rather than silently ignoring the wrapper.
    fn parse_type_name(&mut self) -> Result<String, SqlError> {
        let name = self.parse_ident()?;
        if self.eat(&Token::LParen) {
            let _ = self.parse_arg_list()?;
        }
        Ok(name)
    }

    fn parse_select(&mut self) -> Result<Select, SqlError> {
        let items = self.parse_items()?;
        let table = if self.eat_keyword("FROM") { self.parse_table()? } else { Table::None };
        let where_ = if self.eat_keyword("WHERE") { Some(self.parse_expr()?) } else { None };
        let group_by = if self.eat_keyword("GROUP") {
            self.expect_keyword("BY")?;
            self.parse_expr_list()?
        } else {
            vec![]
        };
        let order_by = if self.eat_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.parse_order_list()?
        } else {
            vec![]
        };
        let limit = if self.eat_keyword("LIMIT") { Some(self.parse_uint()?) } else { None };
        let format = if self.eat_keyword("FORMAT") { Some(self.parse_ident()?) } else { None };
        Ok(Select { items, table, where_, group_by, order_by, limit, format })
    }

    fn parse_create_table(&mut self) -> Result<CreateTable, SqlError> {
        self.expect_keyword("TABLE")?;
        let mut if_not_exists = false;
        if self.eat_keyword("IF") {
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }
        let (database, table) = self.parse_qualified_name()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.parse_ident()?;
            let type_name = self.parse_type_name()?;
            columns.push((col_name, type_name));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        self.expect_keyword("ENGINE")?;
        self.expect(&Token::Eq)?;
        let engine = self.parse_ident()?;
        if self.eat(&Token::LParen) {
            let _ = self.parse_arg_list()?;
        }
        let order_by = if self.eat_keyword("ORDER") {
            self.expect_keyword("BY")?;
            if self.eat(&Token::LParen) {
                let mut cols = Vec::new();
                loop {
                    cols.push(self.parse_ident()?);
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                }
                self.expect(&Token::RParen)?;
                cols
            } else {
                vec![self.parse_ident()?]
            }
        } else {
            vec![]
        };
        Ok(CreateTable { if_not_exists, database, table, columns, engine, order_by })
    }

    fn parse_insert(&mut self) -> Result<Insert, SqlError> {
        self.expect_keyword("INTO")?;
        let (database, table) = self.parse_qualified_name()?;
        let columns = if self.eat(&Token::LParen) {
            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_ident()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        self.expect_keyword("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_expr()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(Insert { database, table, columns, rows })
    }

    fn parse_drop_table(&mut self) -> Result<DropTable, SqlError> {
        self.expect_keyword("TABLE")?;
        let mut if_exists = false;
        if self.eat_keyword("IF") {
            self.expect_keyword("EXISTS")?;
            if_exists = true;
        }
        let (database, table) = self.parse_qualified_name()?;
        Ok(DropTable { if_exists, database, table })
    }
}

/// Parses a single statement.
pub fn parse(sql: &str) -> Result<Statement, SqlError> {
    let tokens = tokenize(sql)?;
    let mut p = Parser { tokens: &tokens, pos: 0 };
    let stmt = if p.eat_keyword("SELECT") {
        Statement::Select(p.parse_select()?)
    } else if p.eat_keyword("CREATE") {
        Statement::CreateTable(p.parse_create_table()?)
    } else if p.eat_keyword("INSERT") {
        Statement::Insert(p.parse_insert()?)
    } else if p.eat_keyword("DROP") {
        Statement::DropTable(p.parse_drop_table()?)
    } else {
        return Err(SqlError("expected SELECT, CREATE TABLE, INSERT or DROP TABLE".into()));
    };
    if p.pos != p.tokens.len() {
        return Err(SqlError("unexpected trailing input".into()));
    }
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select(sql: &str) -> Select {
        match parse(sql).unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    #[test]
    fn select_one() {
        let s = select("SELECT 1");
        assert_eq!(s.items, [Item { expr: Expr::Int(1), alias: None }]);
        assert_eq!(s.table, Table::None);
        assert_eq!(s.limit, None);
        assert_eq!(s.format, None);
    }

    #[test]
    fn select_version_call() {
        let s = select("SELECT version()");
        assert_eq!(s.items, [Item { expr: Expr::Call("version".into(), vec![]), alias: None }]);
    }

    #[test]
    fn select_from_system_one() {
        let s = select("SELECT * FROM system.one");
        assert_eq!(s.items, [Item { expr: Expr::Star, alias: None }]);
        assert_eq!(s.table, Table::SystemOne);
    }

    #[test]
    fn select_from_system_tables() {
        let s = select("SELECT * FROM system.tables");
        assert_eq!(s.table, Table::SystemTables);
    }

    #[test]
    fn select_from_numbers() {
        let s = select("SELECT number FROM numbers(10) LIMIT 5");
        assert_eq!(s.table, Table::SystemNumbers(10));
        assert_eq!(s.limit, Some(5));
    }

    #[test]
    fn select_with_format() {
        let s = select("SELECT 1 FORMAT JSON");
        assert_eq!(s.format, Some("JSON".into()));
    }

    #[test]
    fn select_unknown_table() {
        let s = select("SELECT * FROM nope");
        assert_eq!(s.table, Table::Named { database: None, table: "nope".into() });
    }

    #[test]
    fn select_with_alias() {
        let s = select("SELECT 1 AS one");
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
        let s = select("SELECT FROM");
        assert_eq!(s.items, [Item { expr: Expr::Ident("FROM".into()), alias: None }]);
        assert_eq!(s.table, Table::None);
    }

    #[test]
    fn default_names() {
        assert_eq!(Expr::Int(1).default_name(), "1");
        assert_eq!(Expr::Call("version".into(), vec![]).default_name(), "version()");
    }

    #[test]
    fn expression_precedence() {
        let s = select("SELECT 1 + 2 * 3");
        assert_eq!(
            s.items[0].expr,
            Expr::BinOp(
                BinOp::Add,
                Box::new(Expr::Int(1)),
                Box::new(Expr::BinOp(BinOp::Mul, Box::new(Expr::Int(2)), Box::new(Expr::Int(3))))
            )
        );
    }

    #[test]
    fn parenthesized_expression() {
        let s = select("SELECT (1 + 2) * 3");
        assert_eq!(
            s.items[0].expr,
            Expr::BinOp(
                BinOp::Mul,
                Box::new(Expr::BinOp(BinOp::Add, Box::new(Expr::Int(1)), Box::new(Expr::Int(2)))),
                Box::new(Expr::Int(3))
            )
        );
    }

    #[test]
    fn unary_minus_and_not() {
        let s = select("SELECT -1, NOT true");
        assert_eq!(s.items[0].expr, Expr::Neg(Box::new(Expr::Int(1))));
        assert_eq!(s.items[1].expr, Expr::Not(Box::new(Expr::Bool(true))));
    }

    #[test]
    fn float_literal() {
        let s = select("SELECT 1.5");
        assert_eq!(s.items[0].expr, Expr::Float(1.5));
    }

    #[test]
    fn where_group_by_order_by_limit() {
        let s = select("SELECT k, count(*) FROM t WHERE k > 1 GROUP BY k ORDER BY k DESC LIMIT 2");
        assert!(s.where_.is_some());
        assert_eq!(s.group_by, [Expr::Ident("k".into())]);
        assert_eq!(s.order_by, [(Expr::Ident("k".into()), true)]);
        assert_eq!(s.limit, Some(2));
    }

    #[test]
    fn create_table_memory() {
        let stmt = parse("CREATE TABLE t (id UInt32, name String) ENGINE = Memory").unwrap();
        let Statement::CreateTable(c) = stmt else { panic!("expected CREATE TABLE") };
        assert_eq!(c.table, "t");
        assert_eq!(
            c.columns,
            [("id".to_string(), "UInt32".to_string()), ("name".to_string(), "String".to_string())]
        );
        assert_eq!(c.engine, "Memory");
        assert!(c.order_by.is_empty());
    }

    #[test]
    fn create_table_merge_tree_with_order_by() {
        let stmt =
            parse("CREATE TABLE IF NOT EXISTS db.t (id UInt32) ENGINE = MergeTree() ORDER BY (id)")
                .unwrap();
        let Statement::CreateTable(c) = stmt else { panic!("expected CREATE TABLE") };
        assert!(c.if_not_exists);
        assert_eq!(c.database, Some("db".to_string()));
        assert_eq!(c.order_by, ["id".to_string()]);
    }

    #[test]
    fn create_table_column_type_with_args_is_captured_but_discarded() {
        let stmt = parse("CREATE TABLE t (id UInt32, n Nullable(String)) ENGINE = Memory").unwrap();
        let Statement::CreateTable(c) = stmt else { panic!("expected CREATE TABLE") };
        assert_eq!(c.columns[1], ("n".to_string(), "Nullable".to_string()));
    }

    #[test]
    fn insert_values() {
        let stmt = parse("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')").unwrap();
        let Statement::Insert(i) = stmt else { panic!("expected INSERT") };
        assert_eq!(i.table, "t");
        assert_eq!(i.columns, Some(vec!["id".to_string(), "name".to_string()]));
        assert_eq!(i.rows.len(), 2);
        assert_eq!(i.rows[0], [Expr::Int(1), Expr::Str("a".into())]);
    }

    #[test]
    fn insert_without_column_list() {
        let stmt = parse("INSERT INTO t VALUES (1, 'a')").unwrap();
        let Statement::Insert(i) = stmt else { panic!("expected INSERT") };
        assert_eq!(i.columns, None);
    }

    #[test]
    fn drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS t").unwrap();
        let Statement::DropTable(d) = stmt else { panic!("expected DROP TABLE") };
        assert!(d.if_exists);
        assert_eq!(d.table, "t");
    }
}
