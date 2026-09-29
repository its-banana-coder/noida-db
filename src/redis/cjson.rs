//! The `cjson` library Redis exposes to scripts: `cjson.encode` and
//! `cjson.decode`, matching Lua CJSON's behaviour closely enough for the
//! clients that use it (BullMQ, Sidekiq and friends).

use mlua::{Lua, LuaString, Table, Value as Lv};

pub fn table(lua: &Lua) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.raw_set("encode", lua.create_function(|lua, v: Lv| encode_value(lua, &v, 0))?)?;
    t.raw_set(
        "decode",
        lua.create_function(|lua, s: LuaString| {
            let bytes = s.as_bytes().to_vec();
            let text = String::from_utf8_lossy(&bytes).to_string();
            let mut p = Parser { s: text.as_bytes(), i: 0 };
            p.skip_ws();
            let v = p.value(lua)?;
            p.skip_ws();
            if p.i != p.s.len() {
                return Err(mlua::Error::RuntimeError(format!(
                    "Expected the end but found invalid token at character {}",
                    p.i + 1
                )));
            }
            Ok(v)
        })?,
    )?;
    // cjson.null is a light userdata, as in Lua CJSON.
    t.raw_set("null", Lv::LightUserData(mlua::LightUserData(std::ptr::null_mut())))?;
    Ok(t)
}

fn encode_value(lua: &Lua, v: &Lv, depth: usize) -> mlua::Result<String> {
    if depth > 100 {
        return Err(mlua::Error::RuntimeError("Cannot serialise, excessive nesting".into()));
    }
    Ok(match v {
        Lv::Nil | Lv::LightUserData(_) => "null".into(),
        Lv::Boolean(b) => b.to_string(),
        Lv::Integer(n) => n.to_string(),
        Lv::Number(n) if !n.is_finite() => {
            return Err(mlua::Error::RuntimeError(format!(
                "Cannot serialise number: must not be NaN or Infinity ({n})"
            )));
        }
        // Lua's own %.14g formatting, so numbers look as they do in Redis.
        Lv::Number(_) => lua
            .coerce_string(v.clone())?
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default(),
        Lv::String(s) => quote(&s.as_bytes()),
        Lv::Table(t) => encode_table(lua, t, depth)?,
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "Cannot serialise {}: type not supported",
                other.type_name()
            )));
        }
    })
}

fn encode_table(lua: &Lua, t: &Table, depth: usize) -> mlua::Result<String> {
    let len = t.raw_len();
    if len > 0 {
        let mut parts = Vec::with_capacity(len);
        for i in 1..=len {
            let v: Lv = t.raw_get(i)?;
            parts.push(encode_value(lua, &v, depth + 1)?);
        }
        return Ok(format!("[{}]", parts.join(",")));
    }
    // An empty table encodes as an object, as Lua CJSON does.
    let mut parts = Vec::new();
    for entry in t.clone().pairs::<Lv, Lv>() {
        let (k, v) = entry?;
        let key = match &k {
            Lv::String(s) => quote(&s.as_bytes()),
            Lv::Integer(_) | Lv::Number(_) => {
                let s = lua.coerce_string(k.clone())?;
                quote(s.map(|s| s.as_bytes().to_vec()).unwrap_or_default().as_slice())
            }
            other => {
                return Err(mlua::Error::RuntimeError(format!(
                    "Cannot serialise {}: table key must be a number or string",
                    other.type_name()
                )));
            }
        };
        parts.push(format!("{key}:{}", encode_value(lua, &v, depth + 1)?));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

fn quote(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for &b in bytes {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x08 => out.push_str("\\b"),
            0x0c => out.push_str("\\f"),
            0..=0x1f | 0x7f => out.push_str(&format!("\\u{b:04x}")),
            _ => out.push(b as char),
        }
    }
    out.push('"');
    out
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.s.get(self.i).is_some_and(|c| c.is_ascii_whitespace()) {
            self.i += 1;
        }
    }

    fn fail<T>(&self, what: &str) -> mlua::Result<T> {
        Err(mlua::Error::RuntimeError(format!(
            "Expected value but found {what} at character {}",
            self.i + 1
        )))
    }

    fn literal(&mut self, word: &str) -> bool {
        if self.s[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            return true;
        }
        false
    }

    fn value(&mut self, lua: &Lua) -> mlua::Result<Lv> {
        self.skip_ws();
        match self.s.get(self.i) {
            None => self.fail("T_END"),
            Some(b'n') if self.literal("null") => {
                Ok(Lv::LightUserData(mlua::LightUserData(std::ptr::null_mut())))
            }
            Some(b't') if self.literal("true") => Ok(Lv::Boolean(true)),
            Some(b'f') if self.literal("false") => Ok(Lv::Boolean(false)),
            Some(b'"') => Ok(Lv::String(lua.create_string(self.string()?)?)),
            Some(b'[') => {
                self.i += 1;
                let t = lua.create_table()?;
                self.skip_ws();
                if self.s.get(self.i) == Some(&b']') {
                    self.i += 1;
                    return Ok(Lv::Table(t));
                }
                let mut n = 1;
                loop {
                    let v = self.value(lua)?;
                    t.raw_set(n, v)?;
                    n += 1;
                    self.skip_ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            return Ok(Lv::Table(t));
                        }
                        _ => return self.fail("invalid token"),
                    }
                }
            }
            Some(b'{') => {
                self.i += 1;
                let t = lua.create_table()?;
                self.skip_ws();
                if self.s.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    return Ok(Lv::Table(t));
                }
                loop {
                    self.skip_ws();
                    let key = self.string()?;
                    self.skip_ws();
                    if self.s.get(self.i) != Some(&b':') {
                        return self.fail("invalid token");
                    }
                    self.i += 1;
                    let v = self.value(lua)?;
                    t.raw_set(lua.create_string(key)?, v)?;
                    self.skip_ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            return Ok(Lv::Table(t));
                        }
                        _ => return self.fail("invalid token"),
                    }
                }
            }
            Some(_) => self.number(),
        }
    }

    fn string(&mut self) -> mlua::Result<Vec<u8>> {
        if self.s.get(self.i) != Some(&b'"') {
            return self.fail("invalid token");
        }
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return self.fail("unterminated string");
            };
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return self.fail("unterminated string");
                    };
                    self.i += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'/' => out.push(b'/'),
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'u' => {
                            let hex = self.s.get(self.i..self.i + 4).unwrap_or_default();
                            let code = std::str::from_utf8(hex)
                                .ok()
                                .and_then(|h| u32::from_str_radix(h, 16).ok());
                            let Some(code) = code else {
                                return self.fail("invalid escape");
                            };
                            self.i += 4;
                            let ch = char::from_u32(code).unwrap_or('\u{fffd}');
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return self.fail("invalid escape"),
                    }
                }
                _ => out.push(c),
            }
        }
    }

    fn number(&mut self) -> mlua::Result<Lv> {
        let start = self.i;
        while self
            .s
            .get(self.i)
            .is_some_and(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
        match text.parse::<f64>() {
            Ok(n) if self.i > start => Ok(Lv::Number(n)),
            _ => {
                self.i = start;
                self.fail("invalid token")
            }
        }
    }
}
