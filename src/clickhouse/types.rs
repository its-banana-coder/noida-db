//! The concrete column types milestone 2 supports, and a deliberately
//! simplified value representation. Performance is not a goal (see
//! docs/specs/README.md's "small and simple" rule): `Val` has one variant
//! per value *class* (unsigned integer, signed integer, float, string,
//! bool), not one per declared width, so arithmetic is computed in
//! u64/i64/f64 rather than tracking exact per-width overflow the way real
//! ClickHouse does. Result types of expressions are therefore an
//! approximation of ClickHouse's promotion rules — documented in
//! docs/LIMITATIONS.md.

use super::error::ChError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
    Nullable(Box<Type>),
}

impl Type {
    pub fn name(&self) -> String {
        match self {
            Type::UInt8 => "UInt8".to_string(),
            Type::UInt16 => "UInt16".to_string(),
            Type::UInt32 => "UInt32".to_string(),
            Type::UInt64 => "UInt64".to_string(),
            Type::Int8 => "Int8".to_string(),
            Type::Int16 => "Int16".to_string(),
            Type::Int32 => "Int32".to_string(),
            Type::Int64 => "Int64".to_string(),
            Type::Float32 => "Float32".to_string(),
            Type::Float64 => "Float64".to_string(),
            Type::String => "String".to_string(),
            Type::Bool => "Bool".to_string(),
            Type::Nullable(t) => format!("Nullable({})", t.name()),
        }
    }

    /// Parses a ClickHouse type name. `None` for anything not supported yet
    /// (`Array(...)`, `Decimal`, `Date`, `UUID`, ...) —
    /// callers turn that into `NOT_IMPLEMENTED`, never a silent guess.
    pub fn parse(name: &str) -> Option<Type> {
        if let Some(inner) = name.strip_prefix("Nullable(")
            && let Some(inner) = inner.strip_suffix(")")
        {
            return Type::parse(inner).map(|t| Type::Nullable(Box::new(t)));
        }
        match name {
            "UInt8" => Some(Type::UInt8),
            "UInt16" => Some(Type::UInt16),
            "UInt32" => Some(Type::UInt32),
            "UInt64" => Some(Type::UInt64),
            "Int8" => Some(Type::Int8),
            "Int16" => Some(Type::Int16),
            "Int32" => Some(Type::Int32),
            "Int64" => Some(Type::Int64),
            "Float32" => Some(Type::Float32),
            "Float64" => Some(Type::Float64),
            "String" => Some(Type::String),
            "Bool" | "Boolean" => Some(Type::Bool),
            _ => None,
        }
    }

    pub fn is_unsigned(&self) -> bool {
        match self {
            Type::Nullable(t) => t.is_unsigned(),
            Type::UInt8 | Type::UInt16 | Type::UInt32 | Type::UInt64 => true,
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            Type::Nullable(t) => t.is_float(),
            Type::Float32 | Type::Float64 => true,
            _ => false,
        }
    }

    pub fn is_integer(&self) -> bool {
        if self.is_unsigned() {
            return true;
        }
        match self {
            Type::Nullable(t) => t.is_integer(),
            Type::Int8 | Type::Int16 | Type::Int32 | Type::Int64 => true,
            _ => false,
        }
    }

    /// `(min, max)`, wide enough (`i128`) for both signed and unsigned
    /// ranges. Only meaningful for integer types.
    fn int_range(&self) -> (i128, i128) {
        match self {
            Type::UInt8 => (0, u8::MAX as i128),
            Type::UInt16 => (0, u16::MAX as i128),
            Type::UInt32 => (0, u32::MAX as i128),
            Type::UInt64 => (0, u64::MAX as i128),
            Type::Int8 => (i8::MIN as i128, i8::MAX as i128),
            Type::Int16 => (i16::MIN as i128, i16::MAX as i128),
            Type::Int32 => (i32::MIN as i128, i32::MAX as i128),
            Type::Int64 => (i64::MIN as i128, i64::MAX as i128),
            Type::Nullable(t) => t.int_range(),
            Type::Float32 | Type::Float64 | Type::String | Type::Bool => (i128::MIN, i128::MAX),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    UInt(u64),
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl Val {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Val::UInt(n) => Some(*n as f64),
            Val::Int(n) => Some(*n as f64),
            Val::Float(n) => Some(*n),
            Val::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Val::Str(_) | Val::Null => None,
        }
    }

    pub fn as_i128(&self) -> Option<i128> {
        match self {
            Val::UInt(n) => Some(*n as i128),
            Val::Int(n) => Some(*n as i128),
            Val::Bool(b) => Some(*b as i128),
            Val::Float(_) | Val::Str(_) | Val::Null => None,
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Val::Bool(b) => *b,
            Val::UInt(n) => *n != 0,
            Val::Int(n) => *n != 0,
            Val::Float(n) => *n != 0.0,
            Val::Str(s) => !s.is_empty(),
            Val::Null => false,
        }
    }

    /// The type a bare value (no declared column) would be given, e.g. for
    /// an aggregate's result.
    pub fn natural_type(&self) -> Type {
        match self {
            Val::UInt(_) => Type::UInt64,
            Val::Int(_) => Type::Int64,
            Val::Float(_) => Type::Float64,
            Val::Str(_) => Type::String,
            Val::Bool(_) => Type::Bool,
            Val::Null => Type::Nullable(Box::new(Type::String)), // Type of NULL literal is Nullable(String)
        }
    }
}

/// The value ClickHouse gives a column an `INSERT` doesn't mention.
pub fn zero_value(t: &Type) -> Val {
    match t {
        Type::Nullable(_) => Val::Null,
        Type::String => Val::Str(String::new()),
        Type::Bool => Val::Bool(false),
        t if t.is_float() => Val::Float(0.0),
        t if t.is_unsigned() => Val::UInt(0),
        _ => Val::Int(0),
    }
}

/// Coerces `v` into `t`, range-checking integers (approximating
/// ClickHouse's `ARGUMENT_OUT_OF_BOUND`).
pub fn coerce(v: &Val, t: &Type) -> Result<Val, ChError> {
    if v == &Val::Null {
        if let Type::Nullable(_) = t {
            return Ok(Val::Null);
        }
        return Err(ChError::type_mismatch(&t.name()));
    }
    match t {
        Type::Nullable(inner) => coerce(v, inner),
        Type::String => match v {
            Val::Str(s) => Ok(Val::Str(s.clone())),
            _ => Err(ChError::type_mismatch(&t.name())),
        },
        Type::Bool => match v {
            Val::Bool(b) => Ok(Val::Bool(*b)),
            Val::UInt(n) => Ok(Val::Bool(*n != 0)),
            Val::Int(n) => Ok(Val::Bool(*n != 0)),
            _ => Err(ChError::type_mismatch(&t.name())),
        },
        t if t.is_float() => {
            v.as_f64().map(Val::Float).ok_or_else(|| ChError::type_mismatch(&t.name()))
        }
        t => {
            // Aggregates like sum()/avg() are computed in f64 (see the
            // module doc), so a value bound for an integer column may
            // arrive as a Float that's numerically a whole number — round
            // it rather than rejecting it as a type mismatch.
            let n = v
                .as_i128()
                .or_else(|| v.as_f64().map(|f| f.round() as i128))
                .ok_or_else(|| ChError::type_mismatch(&t.name()))?;
            let (min, max) = t.int_range();
            if n < min || n > max {
                return Err(ChError::out_of_range(&t.name()));
            }
            Ok(if t.is_unsigned() { Val::UInt(n as u64) } else { Val::Int(n as i64) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_type_names() {
        assert_eq!(Type::parse("UInt32"), Some(Type::UInt32));
        assert_eq!(Type::parse("String"), Some(Type::String));
        assert_eq!(Type::parse("Nullable"), None);
    }

    #[test]
    fn coerce_rejects_out_of_range_ints() {
        assert!(coerce(&Val::Int(300), &Type::UInt8).is_err());
        assert!(coerce(&Val::Int(-1), &Type::UInt8).is_err());
        assert_eq!(coerce(&Val::Int(200), &Type::UInt8).unwrap(), Val::UInt(200));
    }

    #[test]
    fn coerce_rejects_type_mismatches() {
        assert!(coerce(&Val::Str("x".into()), &Type::UInt8).is_err());
        assert!(coerce(&Val::Int(1), &Type::String).is_err());
    }

    #[test]
    fn coerce_int_to_float() {
        assert_eq!(coerce(&Val::Int(3), &Type::Float64).unwrap(), Val::Float(3.0));
    }

    #[test]
    fn nullable_parses_and_names() {
        assert_eq!(Type::parse("Nullable(String)"), Some(Type::Nullable(Box::new(Type::String))));
        assert_eq!(Type::parse("Nullable(UInt32)"), Some(Type::Nullable(Box::new(Type::UInt32))));
        assert_eq!(Type::Nullable(Box::new(Type::String)).name(), "Nullable(String)");
    }

    #[test]
    fn nullable_coerces_null_and_passes_through_to_inner() {
        assert_eq!(coerce(&Val::Null, &Type::Nullable(Box::new(Type::UInt32))).unwrap(), Val::Null);
        assert_eq!(
            coerce(&Val::Int(5), &Type::Nullable(Box::new(Type::UInt32))).unwrap(),
            Val::UInt(5)
        );
        // A non-Nullable column must still reject NULL.
        assert!(coerce(&Val::Null, &Type::UInt32).is_err());
    }

    #[test]
    fn nullable_zero_value_is_null() {
        assert_eq!(zero_value(&Type::Nullable(Box::new(Type::String))), Val::Null);
    }

    #[test]
    fn coerce_float_to_int_rounds() {
        // sum()/avg() are computed in f64 (see the module doc); a whole
        // number arriving as a Float must still fit an integer column.
        assert_eq!(coerce(&Val::Float(3.0), &Type::UInt64).unwrap(), Val::UInt(3));
        assert_eq!(coerce(&Val::Float(2.6), &Type::Int32).unwrap(), Val::Int(3));
        assert!(coerce(&Val::Float(1e30), &Type::UInt64).is_err());
    }
}
