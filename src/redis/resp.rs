//! RESP, the Redis serialization protocol: reading client commands and
//! encoding replies.

use std::io::{self, BufRead};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Value>),
    NullArray,
    /// RESP3 map; sent as a flat array on RESP2.
    Map(Vec<(Value, Value)>),
    /// RESP3 set; sent as an array on RESP2.
    Set(Vec<Value>),
    /// RESP3 verbatim string (format, text); sent as a bulk string on RESP2.
    Verbatim(&'static str, Vec<u8>),
    /// Nothing is sent (CLIENT REPLY OFF/SKIP).
    NoReply,
}

impl Value {
    pub fn ok() -> Value {
        Value::Simple("OK".into())
    }

    pub fn err(msg: impl Into<String>) -> Value {
        Value::Error(msg.into())
    }

    pub fn bulk(b: impl AsRef<[u8]>) -> Value {
        Value::Bulk(b.as_ref().to_vec())
    }
}

/// A malformed request. The server replies `-ERR Protocol error: <msg>` and
/// closes the connection, as Redis does.
#[derive(Debug, PartialEq)]
pub struct ProtocolError(pub String);

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    Protocol(ProtocolError),
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

fn protocol(msg: impl Into<String>) -> ReadError {
    ReadError::Protocol(ProtocolError(msg.into()))
}

/// Encodes a reply for a client speaking RESP `proto` (2 or 3).
pub fn encode(v: &Value, proto: u8, out: &mut Vec<u8>) {
    let resp3 = proto >= 3;
    match v {
        Value::Simple(s) => line(out, b'+', s.as_bytes()),
        Value::Error(s) => line(out, b'-', s.as_bytes()),
        Value::Integer(n) => line(out, b':', n.to_string().as_bytes()),
        Value::Bulk(b) => bulk(out, b'$', b),
        Value::Null | Value::NullArray if resp3 => out.extend_from_slice(b"_\r\n"),
        Value::Null => out.extend_from_slice(b"$-1\r\n"),
        Value::NullArray => out.extend_from_slice(b"*-1\r\n"),
        Value::Array(items) => aggregate(out, b'*', items, proto),
        Value::Set(items) => aggregate(out, if resp3 { b'~' } else { b'*' }, items, proto),
        Value::Map(pairs) => {
            let len = if resp3 { pairs.len() } else { pairs.len() * 2 };
            line(out, if resp3 { b'%' } else { b'*' }, len.to_string().as_bytes());
            for (k, v) in pairs {
                encode(k, proto, out);
                encode(v, proto, out);
            }
        }
        Value::Verbatim(format, text) if resp3 => {
            let mut body = format!("{format}:").into_bytes();
            body.extend_from_slice(text);
            bulk(out, b'=', &body);
        }
        Value::Verbatim(_, text) => bulk(out, b'$', text),
        Value::NoReply => {}
    }
}

fn bulk(out: &mut Vec<u8>, prefix: u8, b: &[u8]) {
    line(out, prefix, b.len().to_string().as_bytes());
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

fn aggregate(out: &mut Vec<u8>, prefix: u8, items: &[Value], proto: u8) {
    line(out, prefix, items.len().to_string().as_bytes());
    for item in items {
        encode(item, proto, out);
    }
}

fn line(out: &mut Vec<u8>, prefix: u8, body: &[u8]) {
    out.push(prefix);
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
}

const MAX_MULTIBULK: i64 = 1024 * 1024;
const MAX_BULK: i64 = 512 * 1024 * 1024;

/// Reads one line without its trailing `\r\n` (or `\n`). `None` on EOF.
fn read_line<R: BufRead>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    if r.read_until(b'\n', &mut buf)? == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    } else {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(Some(buf))
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn read_exact_bulk<R: BufRead>(r: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; len + 2];
    r.read_exact(&mut buf)?;
    buf.truncate(len);
    Ok(buf)
}

/// Reads one client command: a multibulk array of bulk strings, or an inline
/// command line. Returns `None` on a clean EOF.
pub fn read_command<R: BufRead>(r: &mut R) -> Result<Option<Vec<Vec<u8>>>, ReadError> {
    let Some(header) = read_line(r)? else {
        return Ok(None);
    };
    if header.first() != Some(&b'*') {
        return split_inline(&header).map(Some);
    }
    let count = match parse_i64(&header[1..]) {
        Some(n) if n <= MAX_MULTIBULK => n,
        _ => return Err(protocol("invalid multibulk length")),
    };
    let mut args = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let line = read_line(r)?.ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
        match line.first() {
            Some(b'$') => {}
            Some(&c) => {
                return Err(protocol(format!("expected '$', got '{}'", c as char)));
            }
            None => return Err(protocol("expected '$', got ' '")),
        }
        let len = match parse_i64(&line[1..]) {
            Some(n) if (0..=MAX_BULK).contains(&n) => n as usize,
            _ => return Err(protocol("invalid bulk length")),
        };
        args.push(read_exact_bulk(r, len)?);
    }
    Ok(Some(args))
}

/// Splits an inline command the way Redis's `sdssplitargs` does.
pub(crate) fn split_inline(line: &[u8]) -> Result<Vec<Vec<u8>>, ReadError> {
    let unbalanced = || protocol("unbalanced quotes in request");
    let mut args = Vec::new();
    let mut i = 0;
    loop {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == line.len() {
            return Ok(args);
        }
        let mut arg = Vec::new();
        let mut in_dq = false;
        let mut in_sq = false;
        loop {
            if in_dq {
                match line.get(i) {
                    None => return Err(unbalanced()),
                    Some(b'\\')
                        if line.get(i + 1) == Some(&b'x')
                            && i + 3 < line.len()
                            && line[i + 2].is_ascii_hexdigit()
                            && line[i + 3].is_ascii_hexdigit() =>
                    {
                        let hex = std::str::from_utf8(&line[i + 2..i + 4]).unwrap();
                        arg.push(u8::from_str_radix(hex, 16).unwrap());
                        i += 3;
                    }
                    Some(b'\\') if i + 1 < line.len() => {
                        i += 1;
                        arg.push(match line[i] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 8,
                            b'a' => 7,
                            c => c,
                        });
                    }
                    Some(b'"') => {
                        // A closing quote must be followed by a space or the end.
                        if line.get(i + 1).is_some_and(|c| !c.is_ascii_whitespace()) {
                            return Err(unbalanced());
                        }
                        in_dq = false;
                    }
                    Some(&c) => arg.push(c),
                }
            } else if in_sq {
                match line.get(i) {
                    None => return Err(unbalanced()),
                    Some(b'\\') if line.get(i + 1) == Some(&b'\'') => {
                        i += 1;
                        arg.push(b'\'');
                    }
                    Some(b'\'') => {
                        if line.get(i + 1).is_some_and(|c| !c.is_ascii_whitespace()) {
                            return Err(unbalanced());
                        }
                        in_sq = false;
                    }
                    Some(&c) => arg.push(c),
                }
            } else {
                match line.get(i) {
                    None => break,
                    Some(c) if c.is_ascii_whitespace() => break,
                    Some(b'"') => in_dq = true,
                    Some(b'\'') => in_sq = true,
                    Some(&c) => arg.push(c),
                }
            }
            i += 1;
        }
        args.push(arg);
    }
}

/// Reads any RESP2 or RESP3 value (used by clients and tests to read replies).
pub fn read_value<R: BufRead>(r: &mut R) -> Result<Option<Value>, ReadError> {
    let Some(line) = read_line(r)? else {
        return Ok(None);
    };
    let Some((&kind, rest)) = line.split_first() else {
        return Err(protocol("empty reply line"));
    };
    let text = || String::from_utf8_lossy(rest).into_owned();
    let int = || parse_i64(rest).ok_or_else(|| protocol("invalid length"));
    let v = match kind {
        b'+' => Value::Simple(text()),
        b'-' => Value::Error(text()),
        b':' => Value::Integer(int()?),
        b'$' => match int()? {
            -1 => Value::Null,
            n => Value::Bulk(read_exact_bulk(r, n as usize)?),
        },
        b'*' => match int()? {
            -1 => Value::NullArray,
            n => {
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let item =
                        read_value(r)?.ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
                    items.push(item);
                }
                Value::Array(items)
            }
        },
        b'_' => Value::Null,
        b'%' => {
            let mut pairs = Vec::new();
            for _ in 0..int()? {
                let k = read_value(r)?.ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
                let v = read_value(r)?.ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
                pairs.push((k, v));
            }
            Value::Map(pairs)
        }
        b'~' => {
            let mut items = Vec::new();
            for _ in 0..int()? {
                items.push(read_value(r)?.ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?);
            }
            Value::Set(items)
        }
        b'=' => {
            let body = read_exact_bulk(r, int()? as usize)?;
            let format = match &body[..4.min(body.len())] {
                b"txt:" => "txt",
                b"mkd:" => "mkd",
                _ => return Err(protocol("bad verbatim format")),
            };
            Value::Verbatim(format, body[4..].to_vec())
        }
        c => return Err(protocol(format!("unknown reply type '{}'", c as char))),
    };
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: Value) -> String {
        enc_proto(v, 2)
    }

    fn enc_proto(v: Value, proto: u8) -> String {
        let mut out = Vec::new();
        encode(&v, proto, &mut out);
        String::from_utf8(out).unwrap()
    }

    fn cmd(input: &str) -> Result<Option<Vec<String>>, String> {
        let mut r = input.as_bytes();
        match read_command(&mut r) {
            Ok(v) => {
                Ok(v.map(|args| args.into_iter().map(|a| String::from_utf8(a).unwrap()).collect()))
            }
            Err(ReadError::Protocol(ProtocolError(m))) => Err(m),
            Err(ReadError::Io(e)) => panic!("io error: {e}"),
        }
    }

    fn strs(v: &[&str]) -> Option<Vec<String>> {
        Some(v.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn encodes_every_type() {
        assert_eq!(enc(Value::ok()), "+OK\r\n");
        assert_eq!(enc(Value::err("ERR nope")), "-ERR nope\r\n");
        assert_eq!(enc(Value::Integer(-42)), ":-42\r\n");
        assert_eq!(enc(Value::bulk("hi")), "$2\r\nhi\r\n");
        assert_eq!(enc(Value::bulk("")), "$0\r\n\r\n");
        assert_eq!(enc(Value::Null), "$-1\r\n");
        assert_eq!(enc(Value::NullArray), "*-1\r\n");
        assert_eq!(enc(Value::Array(vec![])), "*0\r\n");
        assert_eq!(
            enc(Value::Array(vec![Value::Integer(1), Value::bulk("a")])),
            "*2\r\n:1\r\n$1\r\na\r\n"
        );
    }

    #[test]
    fn reads_multibulk() {
        assert_eq!(cmd("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"), Ok(strs(&["GET", "k"])));
    }

    #[test]
    fn bulk_strings_are_binary_safe() {
        let mut r: &[u8] = b"*1\r\n$4\r\na\r\nb\r\n";
        assert_eq!(read_command(&mut r).unwrap(), Some(vec![b"a\r\nb".to_vec()]));
    }

    #[test]
    fn reads_pipelined_commands() {
        let mut r: &[u8] = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        assert!(read_command(&mut r).unwrap().is_some());
        assert!(read_command(&mut r).unwrap().is_some());
        assert!(read_command(&mut r).unwrap().is_none());
    }

    #[test]
    fn reads_inline_commands() {
        assert_eq!(cmd("PING\r\n"), Ok(strs(&["PING"])));
        assert_eq!(cmd("SET  k   v\n"), Ok(strs(&["SET", "k", "v"])));
        assert_eq!(cmd("SET k \"hello world\\n\"\r\n"), Ok(strs(&["SET", "k", "hello world\n"])));
        assert_eq!(cmd("SET k 'a b'\r\n"), Ok(strs(&["SET", "k", "a b"])));
    }

    #[test]
    fn empty_requests_are_empty_commands() {
        assert_eq!(cmd("\r\n"), Ok(strs(&[])));
        assert_eq!(cmd("*0\r\n"), Ok(strs(&[])));
        assert_eq!(cmd("*-1\r\n"), Ok(strs(&[])));
    }

    #[test]
    fn eof_is_none() {
        assert_eq!(cmd(""), Ok(None));
    }

    #[test]
    fn protocol_errors_match_redis() {
        assert_eq!(cmd("*abc\r\n"), Err("invalid multibulk length".into()));
        assert_eq!(cmd("*1\r\n:5\r\n"), Err("expected '$', got ':'".into()));
        assert_eq!(cmd("*1\r\n$x\r\n"), Err("invalid bulk length".into()));
        assert_eq!(cmd("SET k \"unterminated\r\n"), Err("unbalanced quotes in request".into()));
    }

    #[test]
    fn read_value_round_trips() {
        let v = Value::Array(vec![
            Value::ok(),
            Value::err("ERR x"),
            Value::Integer(7),
            Value::bulk("b"),
            Value::Null,
            Value::NullArray,
            Value::Array(vec![Value::bulk("")]),
        ]);
        let mut buf = Vec::new();
        encode(&v, 2, &mut buf);
        let mut r = buf.as_slice();
        assert_eq!(read_value(&mut r).unwrap(), Some(v));
    }

    #[test]
    fn resp3_types_and_their_resp2_fallbacks() {
        let m = Value::Map(vec![(Value::bulk("a"), Value::Integer(1))]);
        assert_eq!(enc_proto(m.clone(), 3), "%1\r\n$1\r\na\r\n:1\r\n");
        assert_eq!(enc_proto(m, 2), "*2\r\n$1\r\na\r\n:1\r\n");
        let set = Value::Set(vec![Value::Simple("x".into())]);
        assert_eq!(enc_proto(set.clone(), 3), "~1\r\n+x\r\n");
        assert_eq!(enc_proto(set, 2), "*1\r\n+x\r\n");
        let v = Value::Verbatim("txt", b"hi".to_vec());
        assert_eq!(enc_proto(v.clone(), 3), "=6\r\ntxt:hi\r\n");
        assert_eq!(enc_proto(v, 2), "$2\r\nhi\r\n");
        assert_eq!(enc_proto(Value::Null, 3), "_\r\n");
        assert_eq!(enc_proto(Value::NullArray, 3), "_\r\n");
        assert_eq!(enc_proto(Value::NoReply, 3), "");
    }

    #[test]
    fn read_value_understands_resp3() {
        let v = Value::Map(vec![
            (Value::bulk("s"), Value::Set(vec![Value::Integer(1)])),
            (Value::bulk("v"), Value::Verbatim("txt", b"x".to_vec())),
            (Value::bulk("n"), Value::Null),
        ]);
        let mut buf = Vec::new();
        encode(&v, 3, &mut buf);
        assert_eq!(read_value(&mut buf.as_slice()).unwrap(), Some(v));
    }
}
