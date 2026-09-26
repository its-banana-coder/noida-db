//! Built-in function signatures and overload resolution.

use std::sync::OnceLock;

use super::casts::{CastCtx, cast_context};
use super::error::{PgError, PgResult, code};
use super::types::{Base, Type};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Scalar,
    Agg,
    Window,
    /// Set-returning; column names/types for multi-column results.
    Srf,
}

#[derive(Debug)]
pub struct Sig {
    pub name: &'static str,
    pub args: Vec<Type>,
    pub variadic: bool,
    pub ret: Type,
    pub kind: Kind,
    pub strict: bool,
    /// Output columns of a multi-column SRF.
    pub cols: Vec<(&'static str, Type)>,
    pub oid: u32,
}

/// `[flags]name(args)ret`. Flags: `a:` aggregate, `w:` window, `s:` set-returning
/// (its `ret` is the element type, or `(col type, ...)` for several columns),
/// `!` not strict. `...t` marks a variadic last argument. SRF rets may be
/// `(col type, ...)`.
const SIGS: &str = r#"
abs(int2)int2 abs(int4)int4 abs(int8)int8 abs(float4)float4 abs(float8)float8 abs(numeric)numeric
ceil(numeric)numeric ceil(float8)float8 ceiling(numeric)numeric ceiling(float8)float8
floor(numeric)numeric floor(float8)float8
round(numeric)numeric round(float8)float8 round(numeric,int4)numeric
trunc(numeric)numeric trunc(float8)float8 trunc(numeric,int4)numeric
sign(numeric)numeric sign(float8)float8
sqrt(numeric)numeric sqrt(float8)float8 cbrt(float8)float8
exp(numeric)numeric exp(float8)float8 ln(numeric)numeric ln(float8)float8
log(numeric)numeric log(float8)float8 log(numeric,numeric)numeric log10(numeric)numeric log10(float8)float8
power(numeric,numeric)numeric power(float8,float8)float8 pow(numeric,numeric)numeric pow(float8,float8)float8
mod(int2,int2)int2 mod(int4,int4)int4 mod(int8,int8)int8 mod(numeric,numeric)numeric
div(numeric,numeric)numeric gcd(int4,int4)int4 gcd(int8,int8)int8 lcm(int4,int4)int4 lcm(int8,int8)int8
pi()float8 random()float8 degrees(float8)float8 radians(float8)float8
sin(float8)float8 cos(float8)float8 tan(float8)float8 asin(float8)float8 acos(float8)float8 atan(float8)float8 atan2(float8,float8)float8 cot(float8)float8
scale(numeric)int4 min_scale(numeric)int4 trim_scale(numeric)numeric
width_bucket(float8,float8,float8,int4)int4 width_bucket(numeric,numeric,numeric,int4)int4
factorial(int8)numeric setseed(float8)void
lower(text)text upper(text)text initcap(text)text
length(text)int4 length(bytea)int4 char_length(text)int4 character_length(text)int4 octet_length(text)int4 octet_length(bytea)int4 bit_length(text)int4
substr(text,int4)text substr(text,int4,int4)text substring(text,int4)text substring(text,int4,int4)text substring(text,text)text substring(text,text,text)text
substr(bytea,int4)bytea substr(bytea,int4,int4)bytea substring(bytea,int4)bytea substring(bytea,int4,int4)bytea
strpos(text,text)int4 position(text,text)int4 replace(text,text,text)text translate(text,text,text)text
btrim(text)text btrim(text,text)text ltrim(text)text ltrim(text,text)text rtrim(text)text rtrim(text,text)text
lpad(text,int4)text lpad(text,int4,text)text rpad(text,int4)text rpad(text,int4,text)text
left(text,int4)text right(text,int4)text repeat(text,int4)text reverse(text)text
!concat(...any)text !concat_ws(text,...any)text split_part(text,text,int4)text
md5(text)text md5(bytea)text sha256(bytea)bytea ascii(text)int4 chr(int4)text
!format(text)text !format(text,...any)text quote_ident(text)text quote_literal(anyelement)text !quote_nullable(anyelement)text
regexp_replace(text,text,text)text regexp_replace(text,text,text,text)text
regexp_match(text,text)_text regexp_match(text,text,text)_text
regexp_like(text,text)bool regexp_like(text,text,text)bool regexp_count(text,text)int4
regexp_split_to_array(text,text)_text regexp_split_to_array(text,text,text)_text
s:regexp_matches(text,text)_text s:regexp_matches(text,text,text)_text
s:regexp_split_to_table(text,text)text s:regexp_split_to_table(text,text,text)text
!string_to_array(text,text)_text !string_to_array(text,text,text)_text
!array_to_string(anyarray,text)text !array_to_string(anyarray,text,text)text
starts_with(text,text)bool to_hex(int4)text to_hex(int8)text
encode(bytea,text)text decode(text,text)bytea convert_to(text,text)bytea convert_from(bytea,text)text
to_ascii(text)text unistr(text)text
gen_random_uuid()uuid uuid_generate_v4()uuid
now()timestamptz transaction_timestamp()timestamptz statement_timestamp()timestamptz clock_timestamp()timestamptz timeofday()text
date_trunc(text,timestamp)timestamp date_trunc(text,timestamptz)timestamptz date_trunc(text,timestamptz,text)timestamptz date_trunc(text,interval)interval
date_part(text,timestamp)float8 date_part(text,timestamptz)float8 date_part(text,date)float8 date_part(text,interval)float8 date_part(text,time)float8
extract(text,timestamp)numeric extract(text,timestamptz)numeric extract(text,date)numeric extract(text,interval)numeric extract(text,time)numeric
age(timestamp,timestamp)interval age(timestamptz,timestamptz)interval age(timestamp)interval age(timestamptz)interval
to_char(timestamp,text)text to_char(timestamptz,text)text to_char(interval,text)text to_char(numeric,text)text to_char(int4,text)text to_char(int8,text)text to_char(float8,text)text to_char(date,text)text
to_date(text,text)date to_timestamp(text,text)timestamptz to_timestamp(float8)timestamptz to_number(text,text)numeric
make_date(int4,int4,int4)date make_time(int4,int4,float8)time make_timestamp(int4,int4,int4,int4,int4,float8)timestamp
make_timestamptz(int4,int4,int4,int4,int4,float8)timestamptz make_timestamptz(int4,int4,int4,int4,int4,float8,text)timestamptz
make_interval(int4,int4,int4,int4,int4,int4,float8)interval make_interval()interval make_interval(int4)interval make_interval(int4,int4)interval make_interval(int4,int4,int4)interval make_interval(int4,int4,int4,int4)interval make_interval(int4,int4,int4,int4,int4)interval make_interval(int4,int4,int4,int4,int4,int4)interval make_interval(int4,int4,int4,int4,int4,int4,float8)interval
justify_days(interval)interval justify_hours(interval)interval justify_interval(interval)interval
isfinite(date)bool isfinite(timestamp)bool isfinite(timestamptz)bool isfinite(interval)bool
timezone(text,timestamptz)timestamp timezone(text,timestamp)timestamptz timezone(interval,timestamptz)timestamp
date_bin(interval,timestamp,timestamp)timestamp date_bin(interval,timestamptz,timestamptz)timestamptz
!to_json(anyelement)json !to_jsonb(anyelement)jsonb !array_to_json(anyarray)json !row_to_json(record)json
!row_to_json(record,_text)json !to_jsonb(record,_text)jsonb
!json_build_object(...any)json !jsonb_build_object(...any)jsonb !json_build_array(...any)json !jsonb_build_array(...any)jsonb
!json_build_object()json !jsonb_build_object()jsonb !json_build_array()json !jsonb_build_array()jsonb
json_object(_text)json jsonb_object(_text)jsonb json_object(_text,_text)json jsonb_object(_text,_text)jsonb
jsonb_typeof(jsonb)text json_typeof(json)text jsonb_array_length(jsonb)int4 json_array_length(json)int4
jsonb_extract_path(jsonb,..._text)jsonb jsonb_extract_path_text(jsonb,..._text)text json_extract_path(json,..._text)json json_extract_path_text(json,..._text)text
jsonb_set(jsonb,_text,jsonb)jsonb jsonb_set(jsonb,_text,jsonb,bool)jsonb jsonb_set_lax(jsonb,_text,jsonb)jsonb jsonb_insert(jsonb,_text,jsonb)jsonb jsonb_insert(jsonb,_text,jsonb,bool)jsonb
jsonb_pretty(jsonb)text jsonb_strip_nulls(jsonb)jsonb json_strip_nulls(json)json
jsonb_exists(jsonb,text)bool jsonb_concat(jsonb,jsonb)jsonb
s:jsonb_array_elements(jsonb)jsonb s:json_array_elements(json)json
s:jsonb_array_elements_text(jsonb)text s:json_array_elements_text(json)text
s:jsonb_each(jsonb)(key text,value jsonb) s:json_each(json)(key text,value json)
s:jsonb_each_text(jsonb)(key text,value text) s:json_each_text(json)(key text,value text)
s:jsonb_object_keys(jsonb)text s:json_object_keys(json)text
s:jsonb_path_query(jsonb,text)jsonb
s:_pg_expandarray(anyarray)(x anyelement,n int4)
_pg_char_max_length(oid,int4)int4 _pg_numeric_precision(oid,int4)int4 _pg_numeric_scale(oid,int4)int4
_pg_datetime_precision(oid,int4)int4 _pg_truetypid(pg_node_tree,oid)oid _pg_truetypmod(pg_node_tree,oid)int4
record_field(record,int4)anyelement
array_length(anyarray,int4)int4 array_upper(anyarray,int4)int4 array_lower(anyarray,int4)int4 cardinality(anyarray)int4
array_ndims(anyarray)int4 array_dims(anyarray)text
!array_append(anyarray,anyelement)anyarray !array_prepend(anyelement,anyarray)anyarray !array_cat(anyarray,anyarray)anyarray
!array_remove(anyarray,anyelement)anyarray !array_replace(anyarray,anyelement,anyelement)anyarray
!array_position(anyarray,anyelement)int4 !array_positions(anyarray,anyelement)_int4
array_fill(anyelement,_int4)anyarray trim_array(anyarray,int4)anyarray
s:unnest(anyarray)anyelement s:generate_subscripts(anyarray,int4)int4
s:generate_series(int4,int4)int4 s:generate_series(int4,int4,int4)int4
s:generate_series(int8,int8)int8 s:generate_series(int8,int8,int8)int8
s:generate_series(numeric,numeric)numeric s:generate_series(numeric,numeric,numeric)numeric
s:generate_series(timestamp,timestamp,interval)timestamp s:generate_series(timestamptz,timestamptz,interval)timestamptz
!num_nulls(...any)int4 !num_nonnulls(...any)int4
!format_type(oid,int4)text version()text current_database()name current_schema()name current_schemas(bool)_name
pg_backend_pid()int4 pg_typeof(any)regtype
!pg_get_expr(pg_node_tree,oid)text !pg_get_expr(pg_node_tree,oid,bool)text pg_get_expr(text,oid)text pg_get_expr(text,oid,bool)text
pg_table_is_visible(oid)bool pg_type_is_visible(oid)bool pg_function_is_visible(oid)bool
has_table_privilege(text,text)bool has_table_privilege(oid,text)bool has_table_privilege(text,text,text)bool has_table_privilege(name,oid,text)bool
has_schema_privilege(text,text)bool has_schema_privilege(text,text,text)bool has_schema_privilege(oid,text)bool has_schema_privilege(name,oid,text)bool
has_database_privilege(text,text)bool has_database_privilege(text,text,text)bool has_column_privilege(text,text,text)bool has_column_privilege(oid,int2,text)bool
has_sequence_privilege(text,text)bool has_function_privilege(oid,text)bool has_any_column_privilege(oid,text)bool
pg_has_role(name,text)bool pg_has_role(name,name,text)bool pg_has_role(oid,text)bool
pg_get_userbyid(oid)name pg_encoding_to_char(int4)name pg_char_to_encoding(name)int4
!obj_description(oid,name)text !obj_description(oid)text !col_description(oid,int4)text !shobj_description(oid,name)text
pg_get_constraintdef(oid)text pg_get_constraintdef(oid,bool)text pg_get_indexdef(oid)text pg_get_indexdef(oid,int4,bool)text
pg_get_viewdef(oid)text pg_get_viewdef(oid,bool)text pg_get_viewdef(text)text pg_get_viewdef(text,bool)text
pg_get_serial_sequence(text,text)text pg_get_triggerdef(oid)text pg_get_triggerdef(oid,bool)text
pg_get_functiondef(oid)text pg_get_function_arguments(oid)text pg_get_function_result(oid)text pg_get_function_identity_arguments(oid)text
pg_get_partkeydef(oid)text pg_get_statisticsobjdef_columns(oid)text pg_get_ruledef(oid)text
pg_relation_size(regclass)int8 pg_total_relation_size(regclass)int8 pg_table_size(regclass)int8 pg_indexes_size(regclass)int8 pg_database_size(name)int8 pg_database_size(oid)int8 pg_size_pretty(int8)text pg_size_pretty(numeric)text
pg_relation_filenode(regclass)oid pg_relation_is_publishable(regclass)bool
txid_current()int8 pg_current_xact_id()xid8 txid_current_if_assigned()int8 pg_is_in_recovery()bool pg_postmaster_start_time()timestamptz pg_conf_load_time()timestamptz
inet_server_addr()inet inet_server_port()int4 inet_client_addr()inet inet_client_port()int4
current_setting(text)text current_setting(text,bool)text !set_config(text,text,bool)text
nextval(regclass)int8 currval(regclass)int8 setval(regclass,int8)int8 setval(regclass,int8,bool)int8 lastval()int8
pg_sleep(float8)void pg_advisory_lock(int8)void pg_advisory_lock(int4,int4)void pg_advisory_unlock(int8)bool pg_advisory_unlock(int4,int4)bool
pg_try_advisory_lock(int8)bool pg_try_advisory_lock(int4,int4)bool pg_advisory_xact_lock(int8)void pg_advisory_xact_lock(int4,int4)void pg_try_advisory_xact_lock(int8)bool
pg_advisory_unlock_all()void pg_advisory_lock_shared(int8)void pg_advisory_unlock_shared(int8)bool
pg_cancel_backend(int4)bool pg_terminate_backend(int4)bool pg_reload_conf()bool
!pg_notify(text,text)void
to_regclass(text)regclass to_regtype(text)regtype to_regproc(text)regproc to_regnamespace(text)regnamespace to_regrole(text)regrole
pg_get_keywords()(word text,catcode char,barelabel bool,catdesc text,baredesc text)
pg_column_size(any)int4 pg_tablespace_location(oid)text pg_collation_for(any)text
pg_jit_available()bool pg_listening_channels()text pg_trigger_depth()int4
txid_snapshot_xmin(txid_snapshot)int8 pg_client_encoding()name
pg_input_is_valid(text,text)bool
a:count()int8 a:count(any)int8
a:sum(int2)int8 a:sum(int4)int8 a:sum(int8)numeric a:sum(numeric)numeric a:sum(float4)float4 a:sum(float8)float8 a:sum(interval)interval a:sum(money)money
a:avg(int2)numeric a:avg(int4)numeric a:avg(int8)numeric a:avg(numeric)numeric a:avg(float4)float8 a:avg(float8)float8 a:avg(interval)interval
a:min(anyelement)anyelement a:max(anyelement)anyelement
a:bool_and(bool)bool a:bool_or(bool)bool a:every(bool)bool
a:bit_and(int4)int4 a:bit_and(int8)int8 a:bit_or(int4)int4 a:bit_or(int8)int8 a:bit_and(int2)int2 a:bit_or(int2)int2
a:!string_agg(text,text)text a:!string_agg(bytea,bytea)bytea
a:!array_agg(anynonarray)anyarray a:!array_agg(anyarray)anyarray
a:!json_agg(anyelement)json a:!jsonb_agg(anyelement)jsonb a:!json_object_agg(any,any)json a:!jsonb_object_agg(any,any)jsonb
a:stddev(float8)float8 a:stddev(numeric)numeric a:stddev_samp(float8)float8 a:stddev_samp(numeric)numeric a:stddev_pop(float8)float8 a:stddev_pop(numeric)numeric
a:variance(float8)float8 a:variance(numeric)numeric a:var_samp(float8)float8 a:var_samp(numeric)numeric a:var_pop(float8)float8 a:var_pop(numeric)numeric
a:stddev(int4)numeric a:stddev(int8)numeric a:variance(int4)numeric a:variance(int8)numeric a:stddev_samp(int4)numeric a:stddev_pop(int4)numeric a:var_samp(int4)numeric a:var_pop(int4)numeric
a:percentile_cont(float8)float8 a:percentile_disc(float8)anyelement a:mode()anyelement
w:row_number()int8 w:rank()int8 w:dense_rank()int8 w:percent_rank()float8 w:cume_dist()float8 w:ntile(int4)int4
w:!lag(anyelement)anyelement w:!lag(anyelement,int4)anyelement w:!lag(anyelement,int4,anyelement)anyelement
w:!lead(anyelement)anyelement w:!lead(anyelement,int4)anyelement w:!lead(anyelement,int4,anyelement)anyelement
w:!first_value(anyelement)anyelement w:!last_value(anyelement)anyelement w:!nth_value(anyelement,int4)anyelement
"#;

fn parse_type(s: &str) -> Type {
    let s = s.trim();
    let (arr, name) = match s.strip_prefix('_') {
        Some(n) => (true, n),
        None => (false, s),
    };
    let base = match name {
        "anyelement" | "anycompatible" => Base::AnyElement,
        "anyarray" | "anycompatiblearray" => return Type::array_of(Base::AnyArray),
        "anynonarray" => Base::AnyNonArray,
        "any" => Base::Any,
        "record" => Base::Record,
        "xid8" | "txid_snapshot" => Base::Int8,
        other => super::types::TYPES
            .iter()
            .find(|t| t.name == other)
            .map(|t| t.base)
            .unwrap_or(Base::Text),
    };
    Type { base, array: arr }
}

fn all() -> &'static Vec<Sig> {
    static S: OnceLock<Vec<Sig>> = OnceLock::new();
    S.get_or_init(|| {
        let mut out = vec![];
        for (n, tok) in split_sigs(SIGS).into_iter().enumerate() {
            let oid = 50_000u32 + n as u32;
            let mut t = tok;
            let mut kind = Kind::Scalar;
            if let Some(r) = t.strip_prefix("a:") {
                kind = Kind::Agg;
                t = r;
            } else if let Some(r) = t.strip_prefix("w:") {
                kind = Kind::Window;
                t = r;
            } else if let Some(r) = t.strip_prefix("s:") {
                kind = Kind::Srf;
                t = r;
            }
            let strict = !t.starts_with('!');
            let t = t.trim_start_matches('!');
            let open = t.find('(').unwrap();
            let close = open + t[open..].find(')').unwrap();
            let name: &'static str = &t[..open];
            let mut variadic = false;
            let args: Vec<Type> = t[open + 1..close]
                .split(',')
                .filter(|a| !a.is_empty())
                .map(|a| {
                    if let Some(v) = a.strip_prefix("...") {
                        variadic = true;
                        parse_type(v)
                    } else {
                        parse_type(a)
                    }
                })
                .collect();
            let ret_s = &t[close + 1..];
            let mut cols = vec![];
            let ret = if ret_s.starts_with('(') {
                kind = Kind::Srf;
                for c in ret_s.trim_matches(|c| c == '(' || c == ')').split(',') {
                    let (n, ty) = c.trim().split_once(' ').unwrap();
                    cols.push((n, parse_type(ty)));
                }
                Type::RECORD
            } else {
                parse_type(ret_s)
            };
            out.push(Sig { name, args, variadic, ret, kind, strict, cols, oid });
        }
        out
    })
}

/// Splits the DSL on whitespace that isn't inside parentheses.
fn split_sigs(s: &'static str) -> Vec<&'static str> {
    let mut out = vec![];
    let mut depth = 0;
    let mut start = None;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c.is_whitespace() && depth == 0 => {
                if let Some(st) = start.take() {
                    out.push(&s[st..i]);
                }
                continue;
            }
            _ => {}
        }
        if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(st) = start {
        out.push(&s[st..]);
    }
    out
}

pub fn exists(name: &str) -> bool {
    all().iter().any(|s| s.name == name)
}

pub fn kind_of(name: &str) -> Option<Kind> {
    all().iter().find(|s| s.name == name).map(|s| s.kind)
}

pub fn all_sigs() -> &'static [Sig] {
    all()
}

/// A resolved call: target argument types and the result type.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub sig: &'static Sig,
    pub arg_tys: Vec<Type>,
    pub ret: Type,
}

fn is_poly(t: Type) -> bool {
    matches!(t.base, Base::AnyElement | Base::AnyArray | Base::AnyNonArray)
}

/// Picks the best overload for `name` given argument types
/// (Postgres's func_select_candidate, simplified).
pub fn resolve(name: &str, args: &[Type]) -> PgResult<Resolved> {
    let cands: Vec<&'static Sig> = all()
        .iter()
        .filter(|s| s.name == name)
        .filter(|s| {
            if s.variadic {
                args.len() >= s.args.len().saturating_sub(1)
            } else {
                s.args.len() == args.len()
            }
        })
        .collect();
    let mut best: Vec<(i32, i32, Resolved)> = vec![];
    for sig in cands {
        let params: Vec<Type> = (0..args.len())
            .map(|i| {
                if sig.variadic && i >= sig.args.len() - 1 {
                    let v = *sig.args.last().unwrap();
                    // `..._text` means variadic text.
                    if v.array && v.base != Base::AnyArray { v.elem() } else { v }
                } else {
                    sig.args[i]
                }
            })
            .collect();
        // Polymorphic binding.
        let mut elem: Option<Type> = None;
        let mut ok = true;
        for (a, p) in args.iter().zip(&params) {
            if a.is_unknown() {
                continue;
            }
            match p.base {
                Base::AnyElement | Base::AnyNonArray if !p.array => {
                    if p.base == Base::AnyNonArray && a.array {
                        ok = false;
                    }
                    elem = Some(merge_poly(elem, *a));
                }
                Base::AnyArray => match super::casts::vector_as_array(*a) {
                    Some(arr) => elem = Some(merge_poly(elem, arr.elem())),
                    None if a.array => elem = Some(merge_poly(elem, a.elem())),
                    None => ok = false,
                },
                _ => {}
            }
        }
        if !ok {
            continue;
        }
        let elem_t = elem.unwrap_or(Type::TEXT);
        let concrete = |p: Type| -> Type {
            match p.base {
                Base::AnyElement | Base::AnyNonArray if !p.array => elem_t,
                Base::AnyArray => elem_t.to_array(),
                _ => p,
            }
        };
        let mut exact = 0;
        let mut preferred = 0;
        let mut fits = true;
        let mut arg_tys = vec![];
        for (a, p) in args.iter().zip(&params) {
            let target = if p.base == Base::Any { *a } else { concrete(*p) };
            arg_tys.push(target);
            if a.is_unknown() {
                if target.category() == b'S' {
                    preferred += 1;
                }
                continue;
            }
            if *a == target || p.base == Base::Any {
                exact += 1;
                continue;
            }
            if is_poly(*p) {
                // Polymorphic args must match the bound element type.
                if cast_context(*a, target) != Some(CastCtx::Implicit) {
                    fits = false;
                }
                continue;
            }
            match cast_context(*a, target) {
                Some(CastCtx::Implicit) => {
                    if target.base.info().is_some_and(|i| i.preferred) {
                        preferred += 1;
                    }
                }
                _ => fits = false,
            }
        }
        if !fits {
            continue;
        }
        let ret = concrete(sig.ret);
        best.push((exact, preferred, Resolved { sig, arg_tys, ret }));
    }
    if best.is_empty() {
        let argl: Vec<String> = args.iter().map(|t| t.display(-1)).collect();
        let exists = all().iter().any(|s| s.name == name);
        let _ = exists;
        return Err(PgError::new(
            code::UNDEFINED_FUNCTION,
            format!("function {}({}) does not exist", name, argl.join(", ")),
        )
        .hint("No function matches the given name and argument types. You might need to add explicit type casts."));
    }
    best.sort_by_key(|c| (-c.0, -c.1));
    if best.len() > 1 && (best[0].0, best[0].1) == (best[1].0, best[1].1) {
        // Ties between numeric overloads with unknown args: prefer numeric/text.
        let top = (best[0].0, best[0].1);
        let tied: Vec<&(i32, i32, Resolved)> = best.iter().filter(|b| (b.0, b.1) == top).collect();
        let pick = tied
            .iter()
            .find(|b| {
                b.2.arg_tys
                    .iter()
                    .all(|t| matches!(t.base, Base::Numeric | Base::Text | Base::Float8))
            })
            .or_else(|| tied.iter().find(|b| b.2.arg_tys.iter().any(|t| t.base == Base::Text)));
        if let Some(p) = pick {
            return Ok(p.2.clone());
        }
        if args.iter().any(|a| a.is_unknown()) || tied.len() > 1 {
            let argl: Vec<String> = args.iter().map(|t| t.display(-1)).collect();
            // Postgres prefers the candidate accepting the preferred type; failing that it's ambiguous.
            if tied
                .iter()
                .map(|b| &b.2.arg_tys)
                .collect::<Vec<_>>()
                .windows(2)
                .any(|w| w[0] != w[1])
            {
                return Err(PgError::new(
                    code::AMBIGUOUS_FUNCTION,
                    format!("function {}({}) is not unique", name, argl.join(", ")),
                )
                .hint("Could not choose a best candidate function. You might need to add explicit type casts."));
            }
        }
    }
    Ok(best.swap_remove(0).2)
}

fn merge_poly(cur: Option<Type>, t: Type) -> Type {
    match cur {
        None => t,
        Some(c) if c == t => c,
        Some(c) => {
            if cast_context(t, c) == Some(CastCtx::Implicit) {
                c
            } else {
                t
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution() {
        let r = resolve("abs", &[Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::INT4);
        let r = resolve("round", &[Type::NUMERIC, Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::NUMERIC);
        let r = resolve("round", &[Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::FLOAT8);
        let r = resolve("length", &[Type::UNKNOWN]).unwrap();
        assert_eq!(r.arg_tys, vec![Type::TEXT]);
        let r = resolve("sum", &[Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::INT8);
        let r = resolve("max", &[Type::TEXT]).unwrap();
        assert_eq!(r.ret, Type::TEXT);
        let r = resolve("array_append", &[Type::array_of(Base::Int4), Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::array_of(Base::Int4));
        let r = resolve("concat", &[Type::INT4, Type::TEXT, Type::UNKNOWN]).unwrap();
        assert_eq!(r.ret, Type::TEXT);
        assert_eq!(resolve("nosuch", &[]).unwrap_err().code, code::UNDEFINED_FUNCTION);
        assert_eq!(resolve("lower", &[Type::INT4]).unwrap_err().code, code::UNDEFINED_FUNCTION);
        let r = resolve("generate_series", &[Type::INT4, Type::INT4]).unwrap();
        assert_eq!(r.ret, Type::INT4);
        assert_eq!(r.sig.kind, Kind::Srf);
        let r = resolve("jsonb_each", &[Type::JSONB]).unwrap();
        assert_eq!(r.sig.cols.len(), 2);
    }
}
