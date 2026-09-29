//! RowBinary (+`WithNames`, +`WithNamesAndTypes`): ClickHouse's compact
//! binary row format. Needed by the official Rust `clickhouse` crate, JDBC
//! and clickhouse-connect, all of which default to it over the HTTP
//! interface. Encoding: little-endian fixed-width numbers, LEB128 varints
//! for lengths (ClickHouse's own "compact" varint), UTF-8 strings as a
//! varint length followed by the bytes.

use super::engine::{Columns, QueryResult, Rows};
use super::error::ChError;
use super::types::{Type, Val};

pub fn encode_varint(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, ChError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *data.get(*pos).ok_or_else(|| ChError::syntax("RowBinary: truncated varint"))?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(ChError::syntax("RowBinary: varint too long"));
        }
    }
    Ok(result)
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    encode_varint(s.len() as u64, out);
    out.extend_from_slice(s.as_bytes());
}

fn decode_string(data: &[u8], pos: &mut usize) -> Result<String, ChError> {
    let len = decode_varint(data, pos)? as usize;
    let bytes = take(data, pos, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| ChError::syntax("RowBinary: invalid UTF-8"))
}

fn take<'a>(data: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], ChError> {
    let end = *pos + n;
    let s = data.get(*pos..end).ok_or_else(|| ChError::syntax("RowBinary: truncated value"))?;
    *pos = end;
    Ok(s)
}

fn as_u64(v: &Val) -> Result<u64, ChError> {
    v.as_i128().map(|n| n as u64).ok_or_else(|| ChError::type_mismatch("integer"))
}

fn as_i64(v: &Val) -> Result<i64, ChError> {
    v.as_i128().map(|n| n as i64).ok_or_else(|| ChError::type_mismatch("integer"))
}

fn as_f64(v: &Val) -> Result<f64, ChError> {
    v.as_f64().ok_or_else(|| ChError::type_mismatch("float"))
}

fn encode_value(v: &Val, t: Type, out: &mut Vec<u8>) -> Result<(), ChError> {
    match t {
        Type::Nullable(inner) => {
            if v == &Val::Null {
                out.push(1);
            } else {
                out.push(0);
                encode_value(v, *inner, out)?;
            }
        }
        Type::UInt8 => out.push(as_u64(v)? as u8),
        Type::Int8 => out.push(as_i64(v)? as i8 as u8),
        Type::Bool => out.push(v.is_truthy() as u8),
        Type::UInt16 => out.extend_from_slice(&(as_u64(v)? as u16).to_le_bytes()),
        Type::Int16 => out.extend_from_slice(&(as_i64(v)? as i16).to_le_bytes()),
        Type::UInt32 => out.extend_from_slice(&(as_u64(v)? as u32).to_le_bytes()),
        Type::Int32 => out.extend_from_slice(&(as_i64(v)? as i32).to_le_bytes()),
        Type::UInt64 => out.extend_from_slice(&as_u64(v)?.to_le_bytes()),
        Type::Int64 => out.extend_from_slice(&as_i64(v)?.to_le_bytes()),
        Type::Float32 => out.extend_from_slice(&(as_f64(v)? as f32).to_le_bytes()),
        Type::Float64 => out.extend_from_slice(&as_f64(v)?.to_le_bytes()),
        Type::String => match v {
            Val::Str(s) => encode_string(s, out),
            _ => return Err(ChError::type_mismatch("String")),
        },
    }
    Ok(())
}

fn decode_value(data: &[u8], pos: &mut usize, t: Type) -> Result<Val, ChError> {
    Ok(match t {
        Type::Nullable(inner) => {
            let is_null = take(data, pos, 1)?[0] != 0;
            if is_null { Val::Null } else { decode_value(data, pos, *inner)? }
        }
        Type::UInt8 => Val::UInt(take(data, pos, 1)?[0] as u64),
        Type::Int8 => Val::Int(take(data, pos, 1)?[0] as i8 as i64),
        Type::Bool => Val::Bool(take(data, pos, 1)?[0] != 0),
        Type::UInt16 => {
            Val::UInt(u16::from_le_bytes(take(data, pos, 2)?.try_into().unwrap()) as u64)
        }
        Type::Int16 => Val::Int(i16::from_le_bytes(take(data, pos, 2)?.try_into().unwrap()) as i64),
        Type::UInt32 => {
            Val::UInt(u32::from_le_bytes(take(data, pos, 4)?.try_into().unwrap()) as u64)
        }
        Type::Int32 => Val::Int(i32::from_le_bytes(take(data, pos, 4)?.try_into().unwrap()) as i64),
        Type::UInt64 => Val::UInt(u64::from_le_bytes(take(data, pos, 8)?.try_into().unwrap())),
        Type::Int64 => Val::Int(i64::from_le_bytes(take(data, pos, 8)?.try_into().unwrap())),
        Type::Float32 => {
            Val::Float(f32::from_le_bytes(take(data, pos, 4)?.try_into().unwrap()) as f64)
        }
        Type::Float64 => Val::Float(f64::from_le_bytes(take(data, pos, 8)?.try_into().unwrap())),
        Type::String => Val::Str(decode_string(data, pos)?),
    })
}

/// Encodes a query result as RowBinary, optionally prefixed with a names
/// (and types) header.
pub fn encode(r: &QueryResult, with_names: bool, with_types: bool) -> Result<Vec<u8>, ChError> {
    let mut out = Vec::new();
    if with_names {
        encode_varint(r.columns.len() as u64, &mut out);
        for (name, _) in &r.columns {
            encode_string(name, &mut out);
        }
    }
    if with_types {
        for (_, t) in &r.columns {
            encode_string(&t.name(), &mut out);
        }
    }
    for row in &r.rows {
        for (val, (_, ty)) in row.iter().zip(r.columns.iter()) {
            encode_value(val, ty.clone(), &mut out)?;
        }
    }
    Ok(out)
}

/// Decodes RowBinary-encoded rows against `schema` (the target columns, in
/// stream order). When a header is present, its column names must match
/// `schema` exactly — noida-db never silently reorders or drops columns.
pub fn decode_rows(
    data: &[u8],
    schema: &Columns,
    with_names: bool,
    with_types: bool,
) -> Result<Rows, ChError> {
    let mut pos = 0;
    if with_names {
        let n = decode_varint(data, &mut pos)? as usize;
        if n != schema.len() {
            return Err(ChError::not_implemented(
                "RowBinary column count differs from the target column list",
            ));
        }
        for (name, _) in schema {
            let got = decode_string(data, &mut pos)?;
            if &got != name {
                return Err(ChError::not_implemented(
                    "RowBinary column order must match the target column list",
                ));
            }
        }
    }
    if with_types {
        for (_, t) in schema {
            let got = decode_string(data, &mut pos)?;
            if got != t.name() {
                return Err(ChError::type_mismatch(&got));
            }
        }
    }
    let mut rows = Vec::new();
    while pos < data.len() {
        let mut row = Vec::with_capacity(schema.len());
        for (_, t) in schema {
            row.push(decode_value(data, &mut pos, t.clone())?);
        }
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Columns {
        vec![("n".into(), Type::UInt32), ("s".into(), Type::String)]
    }

    fn result() -> QueryResult {
        QueryResult {
            columns: schema(),
            rows: vec![
                vec![Val::UInt(1), Val::Str("a".into())],
                vec![Val::UInt(2), Val::Str("bb".into())],
            ],
        }
    }

    #[test]
    fn varint_roundtrip() {
        for n in [0u64, 1, 127, 128, 300, 16384, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(n, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_varint(&buf, &mut pos).unwrap(), n);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn plain_row_binary_roundtrip() {
        let encoded = encode(&result(), false, false).unwrap();
        let decoded = decode_rows(&encoded, &schema(), false, false).unwrap();
        assert_eq!(decoded, result().rows);
    }

    #[test]
    fn with_names_and_types_roundtrip() {
        let encoded = encode(&result(), true, true).unwrap();
        let decoded = decode_rows(&encoded, &schema(), true, true).unwrap();
        assert_eq!(decoded, result().rows);
    }

    #[test]
    fn mismatched_header_names_are_rejected() {
        let encoded = encode(&result(), true, false).unwrap();
        let wrong_schema = vec![("other".into(), Type::UInt32), ("s".into(), Type::String)];
        assert!(decode_rows(&encoded, &wrong_schema, true, false).is_err());
    }

    #[test]
    fn truncated_data_is_a_syntax_error_not_a_panic() {
        let mut encoded = encode(&result(), false, false).unwrap();
        encoded.truncate(2);
        assert!(decode_rows(&encoded, &schema(), false, false).is_err());
    }

    #[test]
    fn nullable_roundtrip_mixes_null_and_real_values() {
        let schema: Columns = vec![("s".into(), Type::Nullable(Box::new(Type::String)))];
        let r = QueryResult {
            columns: schema.clone(),
            rows: vec![vec![Val::Null], vec![Val::Str("hi".into())]],
        };
        let encoded = encode(&r, false, false).unwrap();
        let decoded = decode_rows(&encoded, &schema, false, false).unwrap();
        assert_eq!(decoded, r.rows);
    }
}
