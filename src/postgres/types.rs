//! Postgres data types: identities (OIDs, names, categories), runtime
//! values, and their text and binary wire formats.

use std::cmp::Ordering;

use super::datetime::{self, Ctx, DtErr, Interval};
use super::error::{PgError, PgResult, code};
use super::json::{self, Json};
use super::numeric::{Dec, NumError, Numeric, cmp_num};
use super::tz::Zone;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub enum Base {
    Bool,
    Bytea,
    Char,
    Name,
    Int8,
    Int2,
    Int2Vector,
    Int4,
    Regproc,
    Text,
    Oid,
    Tid,
    Xid,
    Cid,
    OidVector,
    Json,
    Xml,
    PgNodeTree,
    Float4,
    Float8,
    Unknown,
    Money,
    Inet,
    Cidr,
    Macaddr,
    Aclitem,
    Bpchar,
    Varchar,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Interval,
    Timetz,
    Bit,
    Varbit,
    Numeric,
    Refcursor,
    Regprocedure,
    Regoper,
    Regoperator,
    Regclass,
    Regtype,
    Record,
    Cstring,
    Any,
    AnyArray,
    Void,
    Trigger,
    AnyElement,
    AnyNonArray,
    AnyEnum,
    Uuid,
    Jsonb,
    Regnamespace,
    Regrole,
    PgLsn,
    Internal,
    Regconfig,
    Tsvector,
    Tsquery,
    /// A user-defined enum, by its pg_type OID.
    Enum(u32),
}

/// Static facts about a built-in type.
pub struct TypeInfo {
    pub base: Base,
    pub oid: u32,
    pub array_oid: u32,
    pub name: &'static str,
    pub len: i16,
    pub byval: bool,
    /// pg_type.typcategory
    pub category: u8,
    pub preferred: bool,
    /// format_type() spelling.
    pub display: &'static str,
    /// 'b' base, 'p' pseudo.
    pub typtype: u8,
    pub align: u8,
}

macro_rules! ti {
    ($b:ident, $oid:expr, $arr:expr, $name:expr, $len:expr, $byval:expr, $cat:expr, $pref:expr, $disp:expr, $tt:expr, $al:expr) => {
        TypeInfo {
            base: Base::$b,
            oid: $oid,
            array_oid: $arr,
            name: $name,
            len: $len,
            byval: $byval,
            category: $cat,
            preferred: $pref,
            display: $disp,
            typtype: $tt,
            align: $al,
        }
    };
}

pub static TYPES: &[TypeInfo] = &[
    ti!(Bool, 16, 1000, "bool", 1, true, b'B', true, "boolean", b'b', b'c'),
    ti!(Bytea, 17, 1001, "bytea", -1, false, b'U', false, "bytea", b'b', b'i'),
    ti!(Char, 18, 1002, "char", 1, true, b'Z', false, "\"char\"", b'b', b'c'),
    ti!(Name, 19, 1003, "name", 64, false, b'S', false, "name", b'b', b'c'),
    ti!(Int8, 20, 1016, "int8", 8, true, b'N', false, "bigint", b'b', b'd'),
    ti!(Int2, 21, 1005, "int2", 2, true, b'N', false, "smallint", b'b', b's'),
    ti!(Int2Vector, 22, 1006, "int2vector", -1, false, b'A', false, "int2vector", b'b', b'i'),
    ti!(Int4, 23, 1007, "int4", 4, true, b'N', false, "integer", b'b', b'i'),
    ti!(Regproc, 24, 1008, "regproc", 4, true, b'N', false, "regproc", b'b', b'i'),
    ti!(Text, 25, 1009, "text", -1, false, b'S', true, "text", b'b', b'i'),
    ti!(Oid, 26, 1028, "oid", 4, true, b'N', true, "oid", b'b', b'i'),
    ti!(Tid, 27, 1010, "tid", 6, false, b'U', false, "tid", b'b', b's'),
    ti!(Xid, 28, 1011, "xid", 4, true, b'U', false, "xid", b'b', b'i'),
    ti!(Cid, 29, 1012, "cid", 4, true, b'U', false, "cid", b'b', b'i'),
    ti!(OidVector, 30, 1013, "oidvector", -1, false, b'A', false, "oidvector", b'b', b'i'),
    ti!(Json, 114, 199, "json", -1, false, b'U', false, "json", b'b', b'i'),
    ti!(Xml, 142, 143, "xml", -1, false, b'U', false, "xml", b'b', b'i'),
    ti!(PgNodeTree, 194, 0, "pg_node_tree", -1, false, b'Z', false, "pg_node_tree", b'b', b'i'),
    ti!(Float4, 700, 1021, "float4", 4, true, b'N', false, "real", b'b', b'i'),
    ti!(Float8, 701, 1022, "float8", 8, true, b'N', true, "double precision", b'b', b'd'),
    ti!(Unknown, 705, 0, "unknown", -2, false, b'X', false, "unknown", b'p', b'c'),
    ti!(Money, 790, 791, "money", 8, true, b'N', false, "money", b'b', b'd'),
    ti!(Inet, 869, 1041, "inet", -1, false, b'I', true, "inet", b'b', b'i'),
    ti!(Cidr, 650, 651, "cidr", -1, false, b'I', false, "cidr", b'b', b'i'),
    ti!(Macaddr, 829, 1040, "macaddr", 6, false, b'U', false, "macaddr", b'b', b'i'),
    ti!(Aclitem, 1033, 1034, "aclitem", 16, false, b'U', false, "aclitem", b'b', b'd'),
    ti!(Bpchar, 1042, 1014, "bpchar", -1, false, b'S', false, "character", b'b', b'i'),
    ti!(Varchar, 1043, 1015, "varchar", -1, false, b'S', false, "character varying", b'b', b'i'),
    ti!(Date, 1082, 1182, "date", 4, true, b'D', false, "date", b'b', b'i'),
    ti!(Time, 1083, 1183, "time", 8, true, b'D', false, "time without time zone", b'b', b'd'),
    ti!(
        Timestamp,
        1114,
        1115,
        "timestamp",
        8,
        true,
        b'D',
        false,
        "timestamp without time zone",
        b'b',
        b'd'
    ),
    ti!(
        Timestamptz,
        1184,
        1185,
        "timestamptz",
        8,
        true,
        b'D',
        true,
        "timestamp with time zone",
        b'b',
        b'd'
    ),
    ti!(Interval, 1186, 1187, "interval", 16, false, b'T', true, "interval", b'b', b'd'),
    ti!(Timetz, 1266, 1270, "timetz", 12, false, b'D', false, "time with time zone", b'b', b'd'),
    ti!(Bit, 1560, 1561, "bit", -1, false, b'V', false, "bit", b'b', b'i'),
    ti!(Varbit, 1562, 1563, "varbit", -1, false, b'V', true, "bit varying", b'b', b'i'),
    ti!(Numeric, 1700, 1231, "numeric", -1, false, b'N', false, "numeric", b'b', b'i'),
    ti!(Refcursor, 1790, 2201, "refcursor", -1, false, b'U', false, "refcursor", b'b', b'i'),
    ti!(Regprocedure, 2202, 2207, "regprocedure", 4, true, b'N', false, "regprocedure", b'b', b'i'),
    ti!(Regoper, 2203, 2208, "regoper", 4, true, b'N', false, "regoper", b'b', b'i'),
    ti!(Regoperator, 2204, 2209, "regoperator", 4, true, b'N', false, "regoperator", b'b', b'i'),
    ti!(Regclass, 2205, 2210, "regclass", 4, true, b'N', false, "regclass", b'b', b'i'),
    ti!(Regtype, 2206, 2211, "regtype", 4, true, b'N', false, "regtype", b'b', b'i'),
    ti!(Record, 2249, 2287, "record", -1, false, b'P', false, "record", b'p', b'd'),
    ti!(Cstring, 2275, 1263, "cstring", -2, false, b'P', false, "cstring", b'p', b'c'),
    ti!(Any, 2276, 0, "any", 4, true, b'P', false, "\"any\"", b'p', b'i'),
    ti!(AnyArray, 2277, 0, "anyarray", -1, false, b'P', false, "anyarray", b'p', b'd'),
    ti!(Void, 2278, 0, "void", 4, true, b'P', false, "void", b'p', b'i'),
    ti!(Trigger, 2279, 0, "trigger", 4, true, b'P', false, "trigger", b'p', b'i'),
    ti!(AnyElement, 2283, 0, "anyelement", 4, true, b'P', false, "anyelement", b'p', b'i'),
    ti!(AnyNonArray, 2776, 0, "anynonarray", 4, true, b'P', false, "anynonarray", b'p', b'i'),
    ti!(AnyEnum, 3500, 0, "anyenum", 4, true, b'P', false, "anyenum", b'p', b'i'),
    ti!(Uuid, 2950, 2951, "uuid", 16, false, b'U', false, "uuid", b'b', b'c'),
    ti!(Jsonb, 3802, 3807, "jsonb", -1, false, b'U', false, "jsonb", b'b', b'i'),
    ti!(Regnamespace, 4089, 4090, "regnamespace", 4, true, b'N', false, "regnamespace", b'b', b'i'),
    ti!(Regrole, 4096, 4097, "regrole", 4, true, b'N', false, "regrole", b'b', b'i'),
    ti!(PgLsn, 3220, 3221, "pg_lsn", 8, true, b'U', false, "pg_lsn", b'b', b'd'),
    ti!(Internal, 2281, 0, "internal", 8, true, b'P', false, "internal", b'p', b'd'),
    ti!(Regconfig, 3734, 3735, "regconfig", 4, true, b'N', false, "regconfig", b'b', b'i'),
    ti!(Tsvector, 3614, 3643, "tsvector", -1, false, b'U', false, "tsvector", b'b', b'i'),
    ti!(Tsquery, 3615, 3645, "tsquery", -1, false, b'U', false, "tsquery", b'b', b'i'),
];

/// A column/expression type. Postgres arrays of any dimension share a type.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Type {
    pub base: Base,
    pub array: bool,
}

impl Base {
    pub fn info(self) -> Option<&'static TypeInfo> {
        TYPES.iter().find(|t| t.base == self)
    }
}

impl Type {
    pub const fn of(base: Base) -> Type {
        Type { base, array: false }
    }
    pub const fn array_of(base: Base) -> Type {
        Type { base, array: true }
    }
    pub const BOOL: Type = Type::of(Base::Bool);
    pub const INT2: Type = Type::of(Base::Int2);
    pub const INT4: Type = Type::of(Base::Int4);
    pub const INT8: Type = Type::of(Base::Int8);
    pub const FLOAT4: Type = Type::of(Base::Float4);
    pub const FLOAT8: Type = Type::of(Base::Float8);
    pub const NUMERIC: Type = Type::of(Base::Numeric);
    pub const TEXT: Type = Type::of(Base::Text);
    pub const VARCHAR: Type = Type::of(Base::Varchar);
    pub const NAME: Type = Type::of(Base::Name);
    pub const UNKNOWN: Type = Type::of(Base::Unknown);
    pub const OID: Type = Type::of(Base::Oid);
    pub const DATE: Type = Type::of(Base::Date);
    pub const TIMESTAMP: Type = Type::of(Base::Timestamp);
    pub const TIMESTAMPTZ: Type = Type::of(Base::Timestamptz);
    pub const INTERVAL: Type = Type::of(Base::Interval);
    pub const JSON: Type = Type::of(Base::Json);
    pub const JSONB: Type = Type::of(Base::Jsonb);
    pub const BYTEA: Type = Type::of(Base::Bytea);
    pub const UUID: Type = Type::of(Base::Uuid);
    pub const VOID: Type = Type::of(Base::Void);
    pub const RECORD: Type = Type::of(Base::Record);
    pub const CHAR: Type = Type::of(Base::Char);

    pub fn elem(self) -> Type {
        Type::of(self.base)
    }

    pub fn to_array(self) -> Type {
        Type::array_of(self.base)
    }

    pub fn oid(self) -> u32 {
        match self.base {
            // Enum arrays get the OID right after the enum's.
            Base::Enum(oid) => {
                if self.array {
                    oid + 1
                } else {
                    oid
                }
            }
            b => {
                let i = b.info().expect("type info");
                if self.array { i.array_oid } else { i.oid }
            }
        }
    }

    pub fn from_oid(oid: u32) -> Option<Type> {
        for t in TYPES {
            if t.oid == oid {
                return Some(Type::of(t.base));
            }
            if t.array_oid == oid && oid != 0 {
                return Some(Type::array_of(t.base));
            }
        }
        None
    }

    pub fn category(self) -> u8 {
        if self.array {
            return b'A';
        }
        match self.base {
            Base::Enum(_) => b'E',
            b => b.info().map_or(b'U', |i| i.category),
        }
    }

    pub fn is_numeric(self) -> bool {
        !self.array
            && matches!(
                self.base,
                Base::Int2 | Base::Int4 | Base::Int8 | Base::Float4 | Base::Float8 | Base::Numeric
            )
    }

    pub fn is_integer(self) -> bool {
        !self.array && matches!(self.base, Base::Int2 | Base::Int4 | Base::Int8)
    }

    pub fn is_string(self) -> bool {
        !self.array && matches!(self.base, Base::Text | Base::Varchar | Base::Bpchar | Base::Name)
    }

    pub fn is_unknown(self) -> bool {
        self.base == Base::Unknown && !self.array
    }

    pub fn is_reg(self) -> bool {
        !self.array
            && matches!(
                self.base,
                Base::Regclass
                    | Base::Regtype
                    | Base::Regproc
                    | Base::Regprocedure
                    | Base::Regnamespace
                    | Base::Regrole
                    | Base::Regoper
                    | Base::Regoperator
                    | Base::Regconfig
            )
    }

    /// The type's `typname` (arrays as `_elem`).
    pub fn name(self) -> String {
        let base = match self.base {
            Base::Enum(_) => "enum".to_string(),
            b => b.info().map_or("unknown", |i| i.name).to_string(),
        };
        if self.array { format!("_{base}") } else { base }
    }

    /// `format_type()` spelling with an optional typmod.
    pub fn display(self, typmod: i32) -> String {
        let base = match self.base {
            Base::Enum(_) => "enum".to_string(),
            b => {
                let d = b.info().map_or("unknown", |i| i.display);
                match (b, typmod) {
                    (Base::Varchar | Base::Bpchar | Base::Bit | Base::Varbit, m)
                        if m >= 4 || (m >= 0 && matches!(b, Base::Bit | Base::Varbit)) =>
                    {
                        let n = if matches!(b, Base::Bit | Base::Varbit) { m } else { m - 4 };
                        format!("{d}({n})")
                    }
                    (Base::Numeric, m) if m >= 4 => {
                        let m = m - 4;
                        format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff)
                    }
                    (Base::Timestamp, m) if m >= 0 => format!("timestamp({m}) without time zone"),
                    (Base::Timestamptz, m) if m >= 0 => format!("timestamp({m}) with time zone"),
                    (Base::Time, m) if m >= 0 => format!("time({m}) without time zone"),
                    (Base::Timetz, m) if m >= 0 => format!("time({m}) with time zone"),
                    _ => d.to_string(),
                }
            }
        };
        if self.array { format!("{base}[]") } else { base }
    }

    /// pg_type.typlen.
    pub fn typlen(self) -> i16 {
        if self.array {
            return -1;
        }
        match self.base {
            Base::Enum(_) => 4,
            b => b.info().map_or(-1, |i| i.len),
        }
    }
}

// ---------------------------------------------------------------------------
// Values

#[derive(Clone, Debug)]
pub struct Array {
    /// (length, lower bound) per dimension; empty for an empty array.
    pub dims: Vec<(i32, i32)>,
    /// Elements in row-major order.
    pub items: Vec<Value>,
}

impl Array {
    pub fn new(items: Vec<Value>) -> Array {
        if items.is_empty() {
            return Array { dims: vec![], items };
        }
        Array { dims: vec![(items.len() as i32, 1)], items }
    }

    pub fn empty() -> Array {
        Array { dims: vec![], items: vec![] }
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    /// int2/int4/int8/oid and the reg* types.
    Int(i64),
    /// float4/float8 (float4 values are kept rounded to f32).
    Float(f64),
    Num(Numeric),
    /// text, varchar, bpchar, name, "char", json, enums and other text-like types.
    Text(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    /// Microseconds and offset in seconds east of UTC.
    TimeTz(i64, i32),
    /// timestamp (local) or timestamptz (UTC).
    Ts(i64),
    Interval(Interval),
    Uuid([u8; 16]),
    Jsonb(Box<Json>),
    Array(Box<Array>),
    Record(Vec<Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn text(s: impl Into<String>) -> Value {
        Value::Text(s.into())
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 3,
            Value::Num(_) => 4,
            Value::Text(_) => 5,
            Value::Bytes(_) => 6,
            Value::Date(_) => 7,
            Value::Time(_) => 8,
            Value::TimeTz(..) => 9,
            Value::Ts(_) => 10,
            Value::Interval(_) => 11,
            Value::Uuid(_) => 12,
            Value::Jsonb(_) => 13,
            Value::Array(_) => 14,
            Value::Record(_) => 15,
            Value::Null => 255,
        }
    }
}

pub fn cmp_f64(a: f64, b: f64) -> Ordering {
    // Postgres: NaN sorts above everything and equals itself; -0 = 0.
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap(),
    }
}

/// Total order over same-typed values, NULLs last (Postgres's default).
/// Mixed numeric representations compare by value.
pub fn cmp_values(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Greater,
        (_, Null) => Ordering::Less,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => cmp_f64(*x, *y),
        (Num(x), Num(y)) => cmp_num(x, y),
        (Int(x), Num(y)) => cmp_num(&Numeric::from_i64(*x), y),
        (Num(x), Int(y)) => cmp_num(x, &Numeric::from_i64(*y)),
        (Int(x), Float(y)) => cmp_f64(*x as f64, *y),
        (Float(x), Int(y)) => cmp_f64(*x, *y as f64),
        (Num(x), Float(y)) => cmp_f64(x.to_f64(), *y),
        (Float(x), Num(y)) => cmp_f64(*x, y.to_f64()),
        (Text(x), Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        (Time(x), Time(y)) => x.cmp(y),
        (TimeTz(x, ox), TimeTz(y, oy)) => {
            (x - *ox as i64 * 1_000_000).cmp(&(y - *oy as i64 * 1_000_000)).then(oy.cmp(ox))
        }
        (Ts(x), Ts(y)) => x.cmp(y),
        (Interval(x), Interval(y)) => x.span().cmp(&y.span()),
        (Uuid(x), Uuid(y)) => x.cmp(y),
        (Jsonb(x), Jsonb(y)) => json::cmp_jsonb(x, y),
        (Array(x), Array(y)) => {
            for (p, q) in x.items.iter().zip(&y.items) {
                let c = cmp_values(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.items.len().cmp(&y.items.len()).then_with(|| x.dims.len().cmp(&y.dims.len()))
        }
        (Record(x), Record(y)) => {
            for (p, q) in x.iter().zip(y) {
                let c = cmp_values(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => a.rank().cmp(&b.rank()),
    }
}

impl PartialEq for Value {
    fn eq(&self, o: &Value) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(o)
            && cmp_values(self, o) == Ordering::Equal
    }
}

/// Equality as GROUP BY / DISTINCT see it (NULLs equal each other).
pub fn values_equal(a: &Value, b: &Value) -> bool {
    cmp_values(a, b) == Ordering::Equal
}

// ---------------------------------------------------------------------------
// Text output

/// Session settings that affect text output.
#[derive(Clone)]
pub struct FmtCtx {
    pub zone: Zone,
    pub interval_iso: bool,
    pub bytea_escape: bool,
    pub extra_float_digits: i32,
    /// Names for reg* values, filled in when a result has such columns.
    pub reg_names: Option<std::sync::Arc<RegNames>>,
}

/// Object names a `regclass`/`regtype`/... value prints as.
#[derive(Clone, Default, Debug)]
pub struct RegNames {
    pub class: std::collections::HashMap<u32, String>,
    pub types: std::collections::HashMap<u32, String>,
    pub procs: std::collections::HashMap<u32, String>,
    pub namespaces: std::collections::HashMap<u32, String>,
    pub roles: std::collections::HashMap<u32, String>,
}

impl RegNames {
    pub fn lookup(&self, base: Base, oid: u32) -> Option<&String> {
        match base {
            Base::Regclass => self.class.get(&oid),
            Base::Regtype => self.types.get(&oid),
            Base::Regproc | Base::Regprocedure => self.procs.get(&oid),
            Base::Regnamespace => self.namespaces.get(&oid),
            Base::Regrole => self.roles.get(&oid),
            _ => None,
        }
    }
}

impl Default for FmtCtx {
    fn default() -> Self {
        FmtCtx {
            reg_names: None,
            zone: Zone::utc(),
            interval_iso: false,
            bytea_escape: false,
            extra_float_digits: 1,
        }
    }
}

/// Postgres's float output: shortest round-trip digits, exponent form
/// outside [1e-4, 1e15) for float8 or [1e-4, 1e6) for float4.
pub fn format_float(v: f64, float4: bool, efd: i32) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    if efd <= 0 {
        let prec = if float4 { 6 } else { 15 } + efd;
        return super::numeric::format_g(v, prec.max(1) as usize);
    }
    let sci = if float4 { format!("{:e}", v as f32) } else { format!("{v:e}") };
    let (mant, exp) = sci.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant),
    };
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let limit = if float4 { 6 } else { 15 };
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if exp < -4 || exp >= limit {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push_str(&format!("e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs()));
    } else if exp < 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-exp - 1) as usize));
        out.push_str(&digits);
    } else {
        let int_len = exp as usize + 1;
        if digits.len() <= int_len {
            out.push_str(&digits);
            out.push_str(&"0".repeat(int_len - digits.len()));
        } else {
            out.push_str(&digits[..int_len]);
            out.push('.');
            out.push_str(&digits[int_len..]);
        }
    }
    out
}

pub fn format_uuid(u: &[u8; 16]) -> String {
    let h: String = u.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

pub fn format_bytea(b: &[u8], escape: bool) -> String {
    if escape {
        let mut s = String::new();
        for &c in b {
            match c {
                b'\\' => s.push_str("\\\\"),
                0x20..=0x7e => s.push(c as char),
                _ => s.push_str(&format!("\\{c:03o}")),
            }
        }
        return s;
    }
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for c in b {
        s.push_str(&format!("{c:02x}"));
    }
    s
}

/// Text form of a non-null value of type `ty`.
pub fn to_text(v: &Value, ty: Type, f: &FmtCtx) -> String {
    if ty.array {
        return match v {
            Value::Array(a) => array_to_text(a, ty.elem(), f),
            other => to_text(other, ty.elem(), f),
        };
    }
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "t" } else { "f" }.into(),
        Value::Int(i) => match ty.base {
            Base::Bool => if *i != 0 { "t" } else { "f" }.into(),
            b if ty.is_reg() => match f.reg_names.as_ref().and_then(|r| r.lookup(b, *i as u32)) {
                Some(n) => n.clone(),
                None => i.to_string(),
            },
            _ => i.to_string(),
        },
        Value::Float(x) => format_float(*x, ty.base == Base::Float4, f.extra_float_digits),
        Value::Num(n) => n.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => format_bytea(b, f.bytea_escape),
        Value::Date(d) => datetime::format_date(*d),
        Value::Time(t) => datetime::format_time(*t),
        Value::TimeTz(t, o) => datetime::format_timetz(*t, *o),
        Value::Ts(t) => match ty.base {
            Base::Timestamptz => datetime::format_timestamptz(*t, &f.zone),
            _ => datetime::format_timestamp(*t),
        },
        Value::Interval(iv) => {
            if f.interval_iso {
                datetime::format_interval_iso(iv)
            } else {
                datetime::format_interval(iv)
            }
        }
        Value::Uuid(u) => format_uuid(u),
        Value::Jsonb(j) => j.to_jsonb_string(),
        Value::Array(a) => array_to_text(a, ty, f),
        Value::Record(fields) => {
            let mut s = String::from("(");
            for (i, x) in fields.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                if !x.is_null() {
                    let t = to_text(x, value_type_guess(x), f);
                    s.push_str(&quote_record_elem(&t));
                }
            }
            s.push(')');
            s
        }
    }
}

/// Best-effort type for values inside records, where we don't track types.
pub fn value_type_guess(v: &Value) -> Type {
    match v {
        Value::Bool(_) => Type::BOOL,
        Value::Int(_) => Type::INT8,
        Value::Float(_) => Type::FLOAT8,
        Value::Num(_) => Type::NUMERIC,
        Value::Bytes(_) => Type::BYTEA,
        Value::Date(_) => Type::DATE,
        Value::Time(_) => Type::of(Base::Time),
        Value::TimeTz(..) => Type::of(Base::Timetz),
        Value::Ts(_) => Type::TIMESTAMP,
        Value::Interval(_) => Type::INTERVAL,
        Value::Uuid(_) => Type::UUID,
        Value::Jsonb(_) => Type::JSONB,
        Value::Array(a) => a
            .items
            .iter()
            .find(|x| !x.is_null())
            .map_or(Type::array_of(Base::Text), |x| value_type_guess(x).to_array()),
        Value::Record(_) => Type::RECORD,
        Value::Text(_) | Value::Null => Type::TEXT,
    }
}

fn quote_record_elem(s: &str) -> String {
    let needs = s.is_empty()
        || s.chars().any(|c| matches!(c, '"' | '\\' | '(' | ')' | ',') || c.is_whitespace());
    if !needs {
        return s.to_string();
    }
    let mut o = String::from("\"");
    for c in s.chars() {
        if c == '"' || c == '\\' {
            o.push(c);
        }
        o.push(c);
    }
    o.push('"');
    o
}

fn quote_array_elem(s: &str) -> String {
    let needs = s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.chars().any(|c| matches!(c, '"' | '\\' | '{' | '}' | ',') || c.is_ascii_whitespace());
    if !needs {
        return s.to_string();
    }
    let mut o = String::from("\"");
    for c in s.chars() {
        if c == '"' || c == '\\' {
            o.push('\\');
        }
        o.push(c);
    }
    o.push('"');
    o
}

pub fn array_to_text(a: &Array, elem: Type, f: &FmtCtx) -> String {
    if a.dims.is_empty() {
        return "{}".into();
    }
    let mut out = String::new();
    if a.dims.iter().any(|&(_, lb)| lb != 1) {
        for &(len, lb) in &a.dims {
            out.push_str(&format!("[{}:{}]", lb, lb + len - 1));
        }
        out.push('=');
    }
    let mut idx = 0;
    write_array_level(a, 0, &mut idx, elem, f, &mut out);
    out
}

fn write_array_level(
    a: &Array,
    dim: usize,
    idx: &mut usize,
    elem: Type,
    f: &FmtCtx,
    out: &mut String,
) {
    out.push('{');
    let n = a.dims[dim].0;
    for i in 0..n {
        if i > 0 {
            out.push(',');
        }
        if dim + 1 < a.dims.len() {
            write_array_level(a, dim + 1, idx, elem, f, out);
        } else {
            let v = &a.items[*idx];
            *idx += 1;
            if v.is_null() {
                out.push_str("NULL");
            } else {
                out.push_str(&quote_array_elem(&to_text(v, elem, f)));
            }
        }
    }
    out.push('}');
}

// ---------------------------------------------------------------------------
// Text input

pub fn invalid_input(ty: &str, s: &str) -> PgError {
    PgError::new(
        code::INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {ty}: \"{s}\""),
    )
}

fn out_of_range(s: &str, ty: &str) -> PgError {
    PgError::new(
        code::NUMERIC_VALUE_OUT_OF_RANGE,
        format!("value \"{s}\" is out of range for type {ty}"),
    )
}

pub fn dt_error(e: DtErr, ty: &str, s: &str) -> PgError {
    match e {
        DtErr::Syntax => PgError::new(
            code::INVALID_DATETIME_FORMAT,
            format!("invalid input syntax for type {ty}: \"{s}\""),
        ),
        DtErr::Range => PgError::new(
            code::DATETIME_FIELD_OVERFLOW,
            format!("date/time field value out of range: \"{s}\""),
        ),
        DtErr::Zone(z) => {
            PgError::new(code::INVALID_PARAMETER_VALUE, format!("time zone \"{z}\" not recognized"))
        }
    }
}

pub fn parse_int(s: &str, ty: Type) -> PgResult<i64> {
    let name = ty.display(-1);
    let t = s.trim();
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return Err(invalid_input(&name, s));
    }
    let v: i64 = t.parse().map_err(|_| out_of_range(s, &name))?;
    check_int_range(v, ty).map_err(|_| out_of_range(s, &name))
}

/// Range check for int2/int4/oid; the error names the type.
pub fn check_int_range(v: i64, ty: Type) -> PgResult<i64> {
    let ok = match ty.base {
        Base::Int2 => i16::try_from(v).is_ok(),
        Base::Int4 => i32::try_from(v).is_ok(),
        Base::Oid
        | Base::Regclass
        | Base::Regtype
        | Base::Regproc
        | Base::Regnamespace
        | Base::Regrole
        | Base::Xid
        | Base::Cid => (i32::MIN as i64..=u32::MAX as i64).contains(&v),
        _ => true,
    };
    if ok { Ok(v) } else { Err(PgError::new(code::NUMERIC_VALUE_OUT_OF_RANGE, int_range_msg(ty))) }
}

pub fn int_range_msg(ty: Type) -> &'static str {
    match ty.base {
        Base::Int2 => "smallint out of range",
        Base::Int4 => "integer out of range",
        Base::Oid => "OID out of range",
        _ => "bigint out of range",
    }
}

pub fn parse_bool(s: &str) -> Option<bool> {
    let t = s.trim().to_ascii_lowercase();
    if t.is_empty() {
        return None;
    }
    let is_prefix = |w: &str, min: usize| t.len() >= min && w.starts_with(t.as_str());
    if is_prefix("true", 1) || is_prefix("yes", 1) || t == "on" || t == "1" {
        Some(true)
    } else if is_prefix("false", 1) || is_prefix("no", 1) || is_prefix("off", 2) || t == "0" {
        Some(false)
    } else {
        None
    }
}

pub fn parse_float(s: &str, float4: bool) -> PgResult<f64> {
    let name = if float4 { "real" } else { "double precision" };
    let t = s.trim();
    let l = t.to_ascii_lowercase();
    let v = match l.as_str() {
        "nan" => f64::NAN,
        "infinity" | "+infinity" | "inf" | "+inf" => f64::INFINITY,
        "-infinity" | "-inf" => f64::NEG_INFINITY,
        _ => {
            if t.is_empty()
                || !t
                    .bytes()
                    .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                return Err(invalid_input(name, s));
            }
            let v: f64 = t.parse().map_err(|_| invalid_input(name, s))?;
            if v.is_infinite() {
                return Err(out_of_range(t, name));
            }
            v
        }
    };
    if float4 {
        let f = v as f32;
        if f.is_infinite() && v.is_finite() {
            return Err(out_of_range(t, name));
        }
        return Ok(f as f64);
    }
    Ok(v)
}

pub fn parse_numeric(s: &str) -> PgResult<Numeric> {
    Numeric::parse(s).map_err(|e| match e {
        NumError::Overflow => {
            PgError::new(code::NUMERIC_VALUE_OUT_OF_RANGE, "value overflows numeric format")
        }
        _ => invalid_input("numeric", s),
    })
}

pub fn parse_uuid(s: &str) -> PgResult<[u8; 16]> {
    let t = s.trim();
    let t = t.strip_prefix('{').and_then(|x| x.strip_suffix('}')).unwrap_or(t);
    let hex: Vec<u8> = t.bytes().filter(|&c| c != b'-').collect();
    // Hyphens are allowed only after groups of four digits.
    if hex.len() != 32 || t.starts_with('-') || t.ends_with('-') || t.contains("--") {
        return Err(invalid_input("uuid", s));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        let h =
            std::str::from_utf8(&hex[i * 2..i * 2 + 2]).map_err(|_| invalid_input("uuid", s))?;
        out[i] = u8::from_str_radix(h, 16).map_err(|_| invalid_input("uuid", s))?;
    }
    Ok(out)
}

pub fn parse_bytea(s: &str) -> PgResult<Vec<u8>> {
    if let Some(hex) = s.strip_prefix("\\x") {
        let digits: Vec<u8> = hex.bytes().filter(|c| !c.is_ascii_whitespace()).collect();
        if !digits.len().is_multiple_of(2) {
            return Err(PgError::new(
                code::INVALID_TEXT_REPRESENTATION,
                "invalid hexadecimal data: odd number of digits",
            ));
        }
        let mut out = Vec::with_capacity(digits.len() / 2);
        for pair in digits.chunks(2) {
            let h = std::str::from_utf8(pair).unwrap_or("zz");
            let b = u8::from_str_radix(h, 16).map_err(|_| {
                PgError::new(
                    code::INVALID_TEXT_REPRESENTATION,
                    format!("invalid hexadecimal digit: \"{}\"", pair[0] as char),
                )
            })?;
            out.push(b);
        }
        return Ok(out);
    }
    // Escape format.
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            if b.get(i + 1) == Some(&b'\\') {
                out.push(b'\\');
                i += 2;
            } else if i + 3 < b.len()
                && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
                && b[i + 1] <= b'3'
            {
                out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
                i += 4;
            } else {
                return Err(invalid_input("bytea", s));
            }
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    Ok(out)
}

/// Parses text input for `ty` (arrays included).
pub fn from_text(s: &str, ty: Type, ctx: &Ctx) -> PgResult<Value> {
    if ty.array {
        return parse_array(s, ty.elem(), ctx).map(|a| Value::Array(Box::new(a)));
    }
    Ok(match ty.base {
        Base::Bool => Value::Bool(parse_bool(s).ok_or_else(|| invalid_input("boolean", s))?),
        Base::Int2 | Base::Int4 | Base::Int8 | Base::Oid | Base::Xid | Base::Cid => {
            Value::Int(parse_int(s, ty)?)
        }
        Base::Float4 => Value::Float(parse_float(s, true)?),
        Base::Float8 => Value::Float(parse_float(s, false)?),
        Base::Numeric => Value::Num(parse_numeric(s)?),
        Base::Bytea => Value::Bytes(parse_bytea(s)?),
        Base::Date => {
            Value::Date(datetime::parse_date(s, ctx).map_err(|e| dt_error(e, "date", s))?)
        }
        Base::Time => Value::Time(datetime::parse_time(s).map_err(|e| dt_error(e, "time", s))?),
        Base::Timetz => {
            let (t, o) = datetime::parse_timetz(s, ctx)
                .map_err(|e| dt_error(e, "time with time zone", s))?;
            Value::TimeTz(t, o)
        }
        Base::Timestamp => {
            Value::Ts(datetime::parse_timestamp(s, ctx).map_err(|e| dt_error(e, "timestamp", s))?)
        }
        Base::Timestamptz => Value::Ts(
            datetime::parse_timestamptz(s, ctx)
                .map_err(|e| dt_error(e, "timestamp with time zone", s))?,
        ),
        Base::Interval => Value::Interval(datetime::parse_interval(s).map_err(|e| match e {
            DtErr::Range => PgError::new(
                code::DATETIME_FIELD_OVERFLOW,
                format!("interval field value out of range: \"{s}\""),
            ),
            _ => dt_error(e, "interval", s),
        })?),
        Base::Uuid => Value::Uuid(parse_uuid(s)?),
        Base::Json => {
            json::parse(s).map_err(|e| invalid_input("json", s).detail(e.0))?;
            Value::Text(s.to_string())
        }
        Base::Jsonb => Value::Jsonb(Box::new(json::parse_jsonb(s).map_err(|e| {
            if e.0.contains("\\u0000") {
                PgError::new(code::UNTRANSLATABLE_CHARACTER, "unsupported Unicode escape sequence")
                    .detail(e.0)
            } else {
                invalid_input("json", s).detail(e.0)
            }
        })?)),
        Base::Char => Value::Text(s.chars().next().map(String::from).unwrap_or_default()),
        Base::Name => Value::Text(truncate_name(s)),
        Base::Record => {
            return Err(PgError::new(
                code::FEATURE_NOT_SUPPORTED,
                "input of anonymous composite types is not implemented",
            ));
        }
        Base::Void => Value::Null,
        _ => Value::Text(s.to_string()),
    })
}

/// Identifiers are truncated to NAMEDATALEN-1 = 63 bytes.
pub fn truncate_name(s: &str) -> String {
    if s.len() <= 63 {
        return s.to_string();
    }
    let mut end = 63;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

fn malformed_array(s: &str, detail: &str) -> PgError {
    PgError::new(code::INVALID_TEXT_REPRESENTATION, format!("malformed array literal: \"{s}\""))
        .detail(detail.to_string())
}

pub fn parse_array(s: &str, elem: Type, ctx: &Ctx) -> PgResult<Array> {
    let t = s.trim();
    let mut rest = t;
    let mut explicit_dims: Option<Vec<(i32, i32)>> = None;
    if rest.starts_with('[') {
        let eq = rest
            .find('=')
            .ok_or_else(|| malformed_array(s, "Missing \"=\" after array dimensions."))?;
        let mut dims = vec![];
        for part in rest[..eq].trim().split(']').filter(|p| !p.is_empty()) {
            let p = part
                .trim()
                .strip_prefix('[')
                .ok_or_else(|| malformed_array(s, "Missing \"]\" after array dimensions."))?;
            let (lo, hi) = p.split_once(':').unwrap_or(("1", p));
            let lo: i32 = lo
                .trim()
                .parse()
                .map_err(|_| malformed_array(s, "Missing array dimension value."))?;
            let hi: i32 = hi
                .trim()
                .parse()
                .map_err(|_| malformed_array(s, "Missing array dimension value."))?;
            dims.push((hi - lo + 1, lo));
        }
        explicit_dims = Some(dims);
        rest = rest[eq + 1..].trim();
    }
    if !rest.starts_with('{') {
        return Err(malformed_array(
            s,
            "Array value must start with \"{\" or dimension information.",
        ));
    }
    let b = rest.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    let mut items: Vec<Option<String>> = vec![];
    // Element counts per depth for validating rectangular shape.
    let mut counts: Vec<i32> = vec![];
    let mut dim_len: Vec<Option<i32>> = vec![];
    let mut max_depth = 0;
    let mut expect_elem = true;
    while i < b.len() {
        match b[i] {
            b'{' => {
                depth += 1;
                if counts.len() < depth {
                    counts.push(0);
                    dim_len.push(None);
                }
                counts[depth - 1] = 0;
                max_depth = max_depth.max(depth);
                i += 1;
                expect_elem = true;
            }
            b'}' => {
                if depth == 0 {
                    return Err(malformed_array(s, "Unexpected \"}\" character."));
                }
                let n = counts[depth - 1];
                match dim_len[depth - 1] {
                    None => dim_len[depth - 1] = Some(n),
                    Some(m) if m != n => {
                        return Err(malformed_array(
                            s,
                            "Multidimensional arrays must have sub-arrays with matching dimensions.",
                        ));
                    }
                    _ => {}
                }
                depth -= 1;
                if depth > 0 {
                    counts[depth - 1] += 1;
                }
                i += 1;
                expect_elem = false;
                if depth == 0 {
                    if rest[i..].trim().is_empty() {
                        break;
                    }
                    return Err(malformed_array(s, "Junk after closing right brace."));
                }
            }
            b',' => {
                i += 1;
                expect_elem = true;
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                if !expect_elem || depth == 0 {
                    return Err(malformed_array(s, "Unexpected array element."));
                }
                if depth < max_depth {
                    return Err(malformed_array(
                        s,
                        "Multidimensional arrays must have sub-arrays with matching dimensions.",
                    ));
                }
                // An element: quoted or bare.
                let mut val = Vec::new();
                let mut quoted = false;
                if b[i] == b'"' {
                    quoted = true;
                    i += 1;
                    loop {
                        match b.get(i) {
                            None => return Err(malformed_array(s, "Unexpected end of input.")),
                            Some(b'"') => {
                                i += 1;
                                break;
                            }
                            Some(b'\\') => {
                                if let Some(&c) = b.get(i + 1) {
                                    val.push(c);
                                }
                                i += 2;
                            }
                            Some(&c) => {
                                val.push(c);
                                i += 1;
                            }
                        }
                    }
                } else {
                    while i < b.len() && !matches!(b[i], b',' | b'}' | b'{') {
                        if b[i] == b'\\' {
                            if let Some(&c) = b.get(i + 1) {
                                val.push(c);
                            }
                            i += 2;
                            continue;
                        }
                        val.push(b[i]);
                        i += 1;
                    }
                    while val.last().is_some_and(|c| c.is_ascii_whitespace()) {
                        val.pop();
                    }
                }
                let v = String::from_utf8(val).map_err(|_| malformed_array(s, "invalid UTF-8"))?;
                if !quoted && v.eq_ignore_ascii_case("null") {
                    items.push(None);
                } else {
                    items.push(Some(v));
                }
                counts[depth - 1] += 1;
                expect_elem = false;
            }
        }
    }
    if depth != 0 {
        return Err(malformed_array(s, "Unexpected end of input."));
    }
    let mut values = Vec::with_capacity(items.len());
    for it in items {
        values.push(match it {
            None => Value::Null,
            Some(t) => from_text(&t, elem, ctx)?,
        });
    }
    if values.is_empty() {
        return Ok(Array::empty());
    }
    let mut dims: Vec<(i32, i32)> = dim_len.iter().map(|d| (d.unwrap_or(0), 1)).collect();
    if let Some(ed) = explicit_dims {
        if ed.len() != dims.len() || ed.iter().zip(&dims).any(|(a, b)| a.0 != b.0) {
            return Err(malformed_array(
                s,
                "Specified array dimensions do not match array contents.",
            ));
        }
        dims = ed;
    }
    Ok(Array { dims, items: values })
}

// ---------------------------------------------------------------------------
// Typmods

/// `varchar(n)` etc. typmod → declared length.
pub fn typmod_len(typmod: i32) -> Option<usize> {
    (typmod >= 4).then(|| (typmod - 4) as usize)
}

/// Coerces a value to a column's typmod. `explicit` casts truncate
/// strings; assignments raise `string_data_right_truncation`.
pub fn apply_typmod(v: Value, ty: Type, typmod: i32, explicit: bool) -> PgResult<Value> {
    if typmod < 0 || v.is_null() {
        return Ok(v);
    }
    if ty.array {
        return match v {
            Value::Array(mut a) => {
                let items = std::mem::take(&mut a.items);
                a.items = items
                    .into_iter()
                    .map(|x| apply_typmod(x, ty.elem(), typmod, explicit))
                    .collect::<PgResult<_>>()?;
                Ok(Value::Array(a))
            }
            other => Ok(other),
        };
    }
    match (ty.base, v) {
        (Base::Varchar | Base::Bpchar, Value::Text(s)) => {
            let n = typmod_len(typmod).unwrap_or(usize::MAX);
            let len = s.chars().count();
            let mut s = s;
            if len > n {
                let cut: String = s.chars().take(n).collect();
                if !explicit && !s.chars().skip(n).all(|c| c == ' ') {
                    let what =
                        if ty.base == Base::Varchar { "character varying" } else { "character" };
                    return Err(PgError::new(
                        code::STRING_DATA_RIGHT_TRUNCATION,
                        format!("value too long for type {what}({n})"),
                    ));
                }
                s = cut;
            }
            if ty.base == Base::Bpchar {
                let len = s.chars().count();
                if len < n {
                    s.push_str(&" ".repeat(n - len));
                }
            }
            Ok(Value::Text(s))
        }
        (Base::Numeric, Value::Num(n)) => {
            let m = typmod - 4;
            let (p, sc) = ((m >> 16) & 0xffff, (m & 0xffff) as i16);
            n.apply_typmod(p as i64, sc as i64).map(Value::Num).map_err(|_| {
                let lim = p - sc as i32;
                PgError::new(code::NUMERIC_VALUE_OUT_OF_RANGE, "numeric field overflow").detail(if lim > 0 {
                    format!("A field with precision {p}, scale {sc} must round to an absolute value less than 10^{lim}.")
                } else {
                    format!("A field with precision {p}, scale {sc} must round to an absolute value less than 1.")
                })
            })
        }
        (Base::Timestamp | Base::Timestamptz, Value::Ts(t)) => {
            Ok(Value::Ts(datetime::round_micros(t, typmod)))
        }
        (Base::Time, Value::Time(t)) => Ok(Value::Time(datetime::round_micros(t, typmod))),
        (_, v) => Ok(v),
    }
}

/// Encodes `varchar(n)` / `numeric(p,s)` / `timestamp(p)` typmods.
pub fn encode_typmod(base: Base, args: &[i64]) -> PgResult<i32> {
    match base {
        Base::Varchar | Base::Bpchar => {
            let n = args.first().copied().unwrap_or(1);
            if n < 1 {
                let name = if base == Base::Varchar { "varchar" } else { "char" };
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    format!("length for type {name} must be at least 1"),
                ));
            }
            if n > 10_485_760 {
                let name = if base == Base::Varchar { "varchar" } else { "char" };
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    format!("length for type {name} cannot exceed 10485760"),
                ));
            }
            Ok(n as i32 + 4)
        }
        Base::Numeric => {
            let p = args[0];
            let s = args.get(1).copied().unwrap_or(0);
            if !(1..=1000).contains(&p) {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    format!("NUMERIC precision {p} must be between 1 and 1000"),
                ));
            }
            if s < 0 || s > p {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    format!("NUMERIC scale {s} must be between 0 and precision {p}"),
                ));
            }
            Ok((((p << 16) | s) + 4) as i32)
        }
        Base::Timestamp | Base::Timestamptz | Base::Time | Base::Timetz | Base::Interval => {
            Ok(args.first().copied().unwrap_or(6).clamp(0, 6) as i32)
        }
        _ => Ok(-1),
    }
}

// ---------------------------------------------------------------------------
// Binary format

fn numeric_send(n: &Numeric, out: &mut Vec<u8>) {
    let (sign, dscale, digits, weight): (u16, u16, Vec<i16>, i16) = match n {
        Numeric::NaN => (0xC000, 0, vec![], 0),
        Numeric::Inf(false) => (0xD000, 0, vec![], 0),
        Numeric::Inf(true) => (0xF000, 0, vec![], 0),
        Numeric::Fin(d) => {
            let sign = if d.neg { 0x4000 } else { 0 };
            let s = d.scale as usize;
            // Split into integer and fraction decimal digits, pad to groups of 4.
            let mut all = d.digits.clone();
            if all.len() < s {
                let mut z = vec![0u8; s - all.len()];
                z.extend(all);
                all = z;
            }
            let int_part = &all[..all.len() - s];
            let frac_part = &all[all.len() - s..];
            let mut int_groups = vec![];
            let mut ip: Vec<u8> = int_part.to_vec();
            while !ip.len().is_multiple_of(4) {
                ip.insert(0, 0);
            }
            for c in ip.chunks(4) {
                int_groups.push(c.iter().fold(0i16, |a, &x| a * 10 + x as i16));
            }
            let mut fp = frac_part.to_vec();
            while !fp.len().is_multiple_of(4) {
                fp.push(0);
            }
            let frac_groups: Vec<i16> =
                fp.chunks(4).map(|c| c.iter().fold(0i16, |a, &x| a * 10 + x as i16)).collect();
            let mut weight = int_groups.len() as i16 - 1;
            let mut groups: Vec<i16> = int_groups.into_iter().chain(frac_groups).collect();
            while groups.first() == Some(&0) {
                groups.remove(0);
                weight -= 1;
            }
            while groups.last() == Some(&0) {
                groups.pop();
            }
            if groups.is_empty() {
                weight = 0;
            }
            (sign, d.scale as u16, groups, weight)
        }
    };
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for g in digits {
        out.extend_from_slice(&g.to_be_bytes());
    }
}

fn numeric_recv(b: &[u8]) -> PgResult<Numeric> {
    let bad = || PgError::new(code::INVALID_BINARY_REPRESENTATION, "invalid numeric binary value");
    if b.len() < 8 {
        return Err(bad());
    }
    let nd = i16::from_be_bytes([b[0], b[1]]) as usize;
    let weight = i16::from_be_bytes([b[2], b[3]]) as i64;
    let sign = u16::from_be_bytes([b[4], b[5]]);
    let dscale = u16::from_be_bytes([b[6], b[7]]) as u32;
    match sign {
        0xC000 => return Ok(Numeric::NaN),
        0xD000 => return Ok(Numeric::Inf(false)),
        0xF000 => return Ok(Numeric::Inf(true)),
        0 | 0x4000 => {}
        _ => return Err(bad()),
    }
    if b.len() < 8 + nd * 2 {
        return Err(bad());
    }
    let mut s = String::new();
    // Value = sum(d_i * 10000^(weight - i)).
    let groups: Vec<i16> =
        (0..nd).map(|i| i16::from_be_bytes([b[8 + i * 2], b[9 + i * 2]])).collect();
    if groups.is_empty() {
        return Ok(Numeric::Fin(Dec { neg: false, digits: vec![], scale: dscale }));
    }
    let int_groups = (weight + 1).max(0) as usize;
    for i in 0..int_groups {
        let g = groups.get(i).copied().unwrap_or(0);
        s.push_str(&format!("{g:04}"));
    }
    if s.is_empty() {
        s.push('0');
    }
    s.push('.');
    if weight < -1 {
        s.push_str(&"0000".repeat((-weight - 1) as usize));
    }
    for g in groups.iter().skip(int_groups) {
        s.push_str(&format!("{g:04}"));
    }
    let n = Numeric::parse(&s).map_err(|_| bad())?;
    let n = if sign == 0x4000 { n.neg() } else { n };
    Ok(n.round(dscale as i64))
}

/// Binary (send) form of a non-null value.
pub fn to_binary(v: &Value, ty: Type, f: &FmtCtx) -> Vec<u8> {
    let mut out = Vec::new();
    if ty.array {
        if let Value::Array(a) = v {
            let elem = ty.elem();
            out.extend_from_slice(&(a.dims.len() as i32).to_be_bytes());
            let has_null = a.items.iter().any(Value::is_null) as i32;
            out.extend_from_slice(&has_null.to_be_bytes());
            out.extend_from_slice(&elem.oid().to_be_bytes());
            for &(len, lb) in &a.dims {
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&lb.to_be_bytes());
            }
            for it in &a.items {
                if it.is_null() {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    let b = to_binary(it, elem, f);
                    out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                    out.extend_from_slice(&b);
                }
            }
        }
        return out;
    }
    match (ty.base, v) {
        (_, Value::Bool(b)) => out.push(*b as u8),
        (Base::Int2, Value::Int(i)) => out.extend_from_slice(&(*i as i16).to_be_bytes()),
        (Base::Int8, Value::Int(i)) => out.extend_from_slice(&i.to_be_bytes()),
        (Base::Money, Value::Int(i)) => out.extend_from_slice(&i.to_be_bytes()),
        (_, Value::Int(i)) => out.extend_from_slice(&(*i as i32).to_be_bytes()),
        (Base::Float4, Value::Float(x)) => out.extend_from_slice(&(*x as f32).to_be_bytes()),
        (_, Value::Float(x)) => out.extend_from_slice(&x.to_be_bytes()),
        (_, Value::Num(n)) => numeric_send(n, &mut out),
        (Base::Jsonb, Value::Text(s)) => {
            out.push(1);
            out.extend_from_slice(s.as_bytes());
        }
        (_, Value::Text(s)) => out.extend_from_slice(s.as_bytes()),
        (_, Value::Bytes(b)) => out.extend_from_slice(b),
        (_, Value::Date(d)) => out.extend_from_slice(&d.to_be_bytes()),
        (_, Value::Time(t)) => out.extend_from_slice(&t.to_be_bytes()),
        (_, Value::TimeTz(t, o)) => {
            out.extend_from_slice(&t.to_be_bytes());
            out.extend_from_slice(&(-o).to_be_bytes());
        }
        (_, Value::Ts(t)) => out.extend_from_slice(&t.to_be_bytes()),
        (_, Value::Interval(iv)) => {
            out.extend_from_slice(&iv.micros.to_be_bytes());
            out.extend_from_slice(&iv.days.to_be_bytes());
            out.extend_from_slice(&iv.months.to_be_bytes());
        }
        (_, Value::Uuid(u)) => out.extend_from_slice(u),
        (_, Value::Jsonb(j)) => {
            out.push(1);
            out.extend_from_slice(j.to_jsonb_string().as_bytes());
        }
        (_, Value::Record(fields)) => {
            out.extend_from_slice(&(fields.len() as i32).to_be_bytes());
            for x in fields {
                let t = value_type_guess(x);
                out.extend_from_slice(&t.oid().to_be_bytes());
                if x.is_null() {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    let b = to_binary(x, t, f);
                    out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                    out.extend_from_slice(&b);
                }
            }
        }
        (_, other) => out.extend_from_slice(to_text(other, ty, f).as_bytes()),
    }
    out
}

fn bin_err(ty: Type) -> PgError {
    PgError::new(
        code::INVALID_BINARY_REPRESENTATION,
        format!("incorrect binary data format in bind parameter of type {}", ty.display(-1)),
    )
}

/// Decodes a binary (recv) parameter value.
pub fn from_binary(b: &[u8], ty: Type) -> PgResult<Value> {
    let arr = |n: usize| -> PgResult<&[u8]> { if b.len() == n { Ok(b) } else { Err(bin_err(ty)) } };
    if ty.array {
        let rd = |at: usize| -> PgResult<i32> {
            Ok(i32::from_be_bytes(
                b.get(at..at + 4).ok_or_else(|| bin_err(ty))?.try_into().unwrap(),
            ))
        };
        let ndim = rd(0)? as usize;
        let elem_oid = rd(8)? as u32;
        let elem = Type::from_oid(elem_oid).unwrap_or(ty.elem());
        let mut dims = vec![];
        let mut p = 12;
        for _ in 0..ndim {
            dims.push((rd(p)?, rd(p + 4)?));
            p += 8;
        }
        let total: i32 = if ndim == 0 { 0 } else { dims.iter().map(|d| d.0).product() };
        let mut items = vec![];
        for _ in 0..total {
            let len = rd(p)?;
            p += 4;
            if len < 0 {
                items.push(Value::Null);
            } else {
                let data = b.get(p..p + len as usize).ok_or_else(|| bin_err(ty))?;
                items.push(from_binary(data, elem)?);
                p += len as usize;
            }
        }
        return Ok(Value::Array(Box::new(Array { dims, items })));
    }
    let text = || std::str::from_utf8(b).map(str::to_string).map_err(|_| bin_err(ty));
    Ok(match ty.base {
        Base::Bool => Value::Bool(arr(1)?[0] != 0),
        Base::Int2 => Value::Int(i16::from_be_bytes(arr(2)?.try_into().unwrap()) as i64),
        Base::Int4 => Value::Int(i32::from_be_bytes(arr(4)?.try_into().unwrap()) as i64),
        Base::Oid
        | Base::Regclass
        | Base::Regtype
        | Base::Regproc
        | Base::Xid
        | Base::Cid
        | Base::Regnamespace => Value::Int(u32::from_be_bytes(arr(4)?.try_into().unwrap()) as i64),
        Base::Int8 => Value::Int(i64::from_be_bytes(arr(8)?.try_into().unwrap())),
        Base::Float4 => Value::Float(f32::from_be_bytes(arr(4)?.try_into().unwrap()) as f64),
        Base::Float8 => Value::Float(f64::from_be_bytes(arr(8)?.try_into().unwrap())),
        Base::Numeric => Value::Num(numeric_recv(b)?),
        Base::Bytea => Value::Bytes(b.to_vec()),
        Base::Date => Value::Date(i32::from_be_bytes(arr(4)?.try_into().unwrap())),
        Base::Time => Value::Time(i64::from_be_bytes(arr(8)?.try_into().unwrap())),
        Base::Timetz => {
            let x = arr(12)?;
            Value::TimeTz(
                i64::from_be_bytes(x[..8].try_into().unwrap()),
                -i32::from_be_bytes(x[8..].try_into().unwrap()),
            )
        }
        Base::Timestamp | Base::Timestamptz => {
            Value::Ts(i64::from_be_bytes(arr(8)?.try_into().unwrap()))
        }
        Base::Interval => {
            let x = arr(16)?;
            Value::Interval(Interval {
                micros: i64::from_be_bytes(x[..8].try_into().unwrap()),
                days: i32::from_be_bytes(x[8..12].try_into().unwrap()),
                months: i32::from_be_bytes(x[12..].try_into().unwrap()),
            })
        }
        Base::Uuid => Value::Uuid(arr(16)?.try_into().unwrap()),
        Base::Jsonb => {
            if b.first() != Some(&1) {
                return Err(PgError::new(
                    code::INVALID_BINARY_REPRESENTATION,
                    "unsupported jsonb version number",
                ));
            }
            let s = std::str::from_utf8(&b[1..]).map_err(|_| bin_err(ty))?;
            Value::Jsonb(Box::new(
                json::parse_jsonb(s).map_err(|e| invalid_input("json", s).detail(e.0))?,
            ))
        }
        Base::Json => {
            let s = text()?;
            json::parse(&s).map_err(|e| invalid_input("json", &s).detail(e.0))?;
            Value::Text(s)
        }
        Base::Char => Value::Text(b.first().map(|&c| (c as char).to_string()).unwrap_or_default()),
        _ => Value::Text(text()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> (i64, Zone) {
        (0, Zone::utc())
    }

    #[test]
    fn float_output() {
        let f = |v: f64| format_float(v, false, 1);
        assert_eq!(f(1e20), "1e+20");
        assert_eq!(f(1e15), "1e+15");
        assert_eq!(f(123456789012345.0), "123456789012345");
        assert_eq!(f(0.0001), "0.0001");
        assert_eq!(f(0.00001), "1e-05");
        assert_eq!(f(100.0), "100");
        assert_eq!(f(1.5), "1.5");
        assert_eq!(f(-0.0), "-0");
        assert_eq!(f(1.0e-10), "1e-10");
        assert_eq!(f(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(format_float(1e6, true, 1), "1e+06");
        assert_eq!(format_float(123456.0, true, 1), "123456");
        assert_eq!(format_float(0.1f32 as f64, true, 1), "0.1");
        assert_eq!(format_float(0.1 + 0.2, false, 0), "0.3");
    }

    #[test]
    fn arrays_roundtrip() {
        let (now, z) = ctx();
        let c = Ctx { now, zone: &z };
        let f = FmtCtx::default();
        let a = parse_array(r#"{"a b","","NULL",NULL,"x\"y","{","a\\b"}"#, Type::TEXT, &c).unwrap();
        assert_eq!(
            array_to_text(&a, Type::TEXT, &f),
            r#"{"a b","","NULL",NULL,"x\"y","{","a\\b"}"#
        );
        let a = parse_array("[0:1]={1,2}", Type::INT4, &c).unwrap();
        assert_eq!(array_to_text(&a, Type::INT4, &f), "[0:1]={1,2}");
        let a = parse_array("{{1,2},{3,4}}", Type::INT4, &c).unwrap();
        assert_eq!(a.dims, vec![(2, 1), (2, 1)]);
        assert_eq!(array_to_text(&a, Type::INT4, &f), "{{1,2},{3,4}}");
        assert!(parse_array("{{1,2},{3}}", Type::INT4, &c).is_err());
        assert!(parse_array("{1,2", Type::INT4, &c).is_err());
        assert_eq!(
            array_to_text(&parse_array("{}", Type::INT4, &c).unwrap(), Type::INT4, &f),
            "{}"
        );
    }

    #[test]
    fn scalar_input() {
        assert_eq!(parse_int(" 12 ", Type::INT4).unwrap(), 12);
        assert_eq!(
            parse_int("1.5", Type::INT4).unwrap_err().code,
            code::INVALID_TEXT_REPRESENTATION
        );
        assert_eq!(
            parse_int("99999999999", Type::INT4).unwrap_err().code,
            code::NUMERIC_VALUE_OUT_OF_RANGE
        );
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("of"), Some(false));
        assert_eq!(parse_bool("x"), None);
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bytea("\\x0aff").unwrap(), vec![10, 255]);
        assert_eq!(parse_bytea("a\\\\b\\001").unwrap(), b"a\\b\x01".to_vec());
        let u = parse_uuid("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11").unwrap();
        assert_eq!(format_uuid(&u), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    }

    #[test]
    fn typmods() {
        let v = apply_typmod(Value::text("ab"), Type::of(Base::Bpchar), 8, false).unwrap();
        assert_eq!(v.as_str(), Some("ab  "));
        let e = apply_typmod(Value::text("abc"), Type::VARCHAR, 6, false).unwrap_err();
        assert_eq!(e.code, code::STRING_DATA_RIGHT_TRUNCATION);
        let v = apply_typmod(Value::text("abc"), Type::VARCHAR, 6, true).unwrap();
        assert_eq!(v.as_str(), Some("ab"));
        let tm = encode_typmod(Base::Numeric, &[5, 2]).unwrap();
        assert_eq!(Type::NUMERIC.display(tm), "numeric(5,2)");
        assert_eq!(Type::VARCHAR.display(259), "character varying(255)");
    }

    #[test]
    fn numeric_binary_roundtrip() {
        for s in ["0", "1", "-12345.678", "0.0001", "10000", "123456789.000", "0.00000123"] {
            let n = Numeric::parse(s).unwrap();
            let mut b = vec![];
            numeric_send(&n, &mut b);
            assert_eq!(numeric_recv(&b).unwrap().to_string(), n.to_string(), "{s}");
        }
    }
}
