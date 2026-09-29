use crate::sql::json::Json;
use crate::sql::numeric::{Numeric, cmp_num};
use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Num(Numeric),
    Text(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    Ts(i64),
    Json(Box<Json>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Num(a), Value::Num(b)) => cmp_num(a, b) == Ordering::Equal,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Ts(a), Value::Ts(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            _ => false,
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}
