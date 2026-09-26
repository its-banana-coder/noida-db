//! Postgres keywords (from Postgres 14's pg_get_keywords()), used for
//! identifier quoting and `pg_get_keywords()`.

/// Column-name keywords (category C).
pub const COL_NAME: &str = "between bigint bit boolean char character coalesce dec decimal exists extract float greatest \
grouping inout int integer interval least national nchar none normalize nullif numeric out overlay position precision \
real row setof smallint substring time timestamp treat trim values varchar xmlattributes xmlconcat xmlelement xmlexists \
xmlforest xmlnamespaces xmlparse xmlpi xmlroot xmlserialize xmltable";

/// Fully reserved keywords (category R).
pub const RESERVED: &str = "all analyse analyze and any array as asc asymmetric both case cast check collate column \
constraint create current_catalog current_date current_role current_time current_timestamp current_user default \
deferrable desc distinct do else end except false fetch for foreign from grant group having in initially intersect into \
lateral leading limit localtime localtimestamp not null offset on only or order placing primary references returning \
select session_user some symmetric table then to trailing true union unique user using variadic when where window with";

/// Type/function-name keywords (category T).
pub const TYPE_FUNC: &str = "authorization binary collation concurrently cross current_schema freeze full ilike inner is \
isnull join left like natural notnull outer overlaps right similar tablesample verbose";

/// Whether quote_ident() must quote this word (every non-unreserved keyword).
pub fn is_reserved(w: &str) -> bool {
    [COL_NAME, RESERVED, TYPE_FUNC].iter().any(|list| list.split_ascii_whitespace().any(|k| k == w))
}
