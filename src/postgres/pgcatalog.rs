//! `pg_catalog` and `information_schema` relations, generated on demand
//! from the live catalog.

use std::sync::OnceLock;

use super::catalog::{ConstraintKind, DbState, PG_CATALOG_NS, Row};
use super::error::PgResult;
use super::exec::Ctx;
use super::plan::OutCol;
use super::types::{Array, Base, Type, Value};

/// `relation: col type, col type, ...`
const RELATIONS: &str = "
pg_namespace: oid oid, nspname name, nspowner oid, nspacl _aclitem
pg_class: oid oid, relname name, relnamespace oid, reltype oid, reloftype oid, relowner oid, relam oid, \
relfilenode oid, reltablespace oid, relpages int4, reltuples float4, relallvisible int4, reltoastrelid oid, \
relhasindex bool, relisshared bool, relpersistence char, relkind char, relnatts int2, relchecks int2, \
relhasrules bool, relhastriggers bool, relhassubclass bool, relrowsecurity bool, relforcerowsecurity bool, \
relispopulated bool, relreplident char, relispartition bool, relrewrite oid, relfrozenxid xid, relminmxid xid, \
relacl _aclitem, reloptions _text, relpartbound pg_node_tree
pg_attribute: attrelid oid, attname name, atttypid oid, attstattarget int4, attlen int2, attnum int2, \
attndims int4, attcacheoff int4, atttypmod int4, attbyval bool, attalign char, attstorage char, \
attcompression char, attnotnull bool, atthasdef bool, atthasmissing bool, attidentity char, attgenerated char, \
attisdropped bool, attislocal bool, attinhcount int4, attcollation oid, attacl _aclitem, attoptions _text, \
attfdwoptions _text, attmissingval anyarray
pg_type: oid oid, typname name, typnamespace oid, typowner oid, typlen int2, typbyval bool, typtype char, \
typcategory char, typispreferred bool, typisdefined bool, typdelim char, typrelid oid, typsubscript regproc, \
typelem oid, typarray oid, typinput regproc, typoutput regproc, typreceive regproc, typsend regproc, \
typmodin regproc, typmodout regproc, typanalyze regproc, typalign char, typstorage char, typnotnull bool, \
typbasetype oid, typtypmod int4, typndims int4, typcollation oid, typdefaultbin pg_node_tree, typdefault text, \
typacl _aclitem
pg_index: indexrelid oid, indrelid oid, indnatts int2, indnkeyatts int2, indisunique bool, \
indnullsnotdistinct bool, indisprimary bool, indisexclusion bool, indimmediate bool, indisclustered bool, \
indisvalid bool, indcheckxmin bool, indisready bool, indislive bool, indisreplident bool, indkey int2vector, \
indcollation oidvector, indclass oidvector, indoption int2vector, indexprs pg_node_tree, indpred pg_node_tree
pg_constraint: oid oid, conname name, connamespace oid, contype char, condeferrable bool, condeferred bool, \
convalidated bool, conrelid oid, contypid oid, conindid oid, conparentid oid, confrelid oid, confupdtype char, \
confdeltype char, confmatchtype char, conislocal bool, coninhcount int4, connoinherit bool, conkey _int2, \
confkey _int2, conpfeqop _oid, conppeqop _oid, conffeqop _oid, confdelsetcols _int2, conexclop _oid, \
conbin pg_node_tree
pg_attrdef: oid oid, adrelid oid, adnum int2, adbin pg_node_tree
pg_database: oid oid, datname name, datdba oid, encoding int4, datlocprovider char, datistemplate bool, \
datallowconn bool, datconnlimit int4, datfrozenxid xid, datminmxid xid, dattablespace oid, datcollate name, \
datctype name, daticulocale text, daticurules text, datcollversion text, datacl _aclitem
pg_proc: oid oid, proname name, pronamespace oid, proowner oid, prolang oid, procost float4, prorows float4, \
provariadic oid, prosupport regproc, prokind char, prosecdef bool, proleakproof bool, proisstrict bool, \
proretset bool, provolatile char, proparallel char, pronargs int2, pronargdefaults int2, prorettype oid, \
proargtypes oidvector, proallargtypes _oid, proargmodes _char, proargnames _text, proargdefaults pg_node_tree, \
protrftypes _oid, prosrc text, probin text, prosqlbody pg_node_tree, proconfig _text, proacl _aclitem
pg_enum: oid oid, enumtypid oid, enumsortorder float4, enumlabel name
pg_description: objoid oid, classoid oid, objsubid int4, description text
pg_shdescription: objoid oid, classoid oid, description text
pg_am: oid oid, amname name, amhandler regproc, amtype char
pg_roles: rolname name, rolsuper bool, rolinherit bool, rolcreaterole bool, rolcreatedb bool, rolcanlogin bool, \
rolreplication bool, rolconnlimit int4, rolpassword text, rolvaliduntil timestamptz, rolbypassrls bool, \
rolconfig _text, oid oid
pg_authid: oid oid, rolname name, rolsuper bool, rolinherit bool, rolcreaterole bool, rolcreatedb bool, \
rolcanlogin bool, rolreplication bool, rolbypassrls bool, rolconnlimit int4, rolpassword text, \
rolvaliduntil timestamptz
pg_user: usename name, usesysid oid, usecreatedb bool, usesuper bool, userepl bool, usebypassrls bool, \
passwd text, valuntil timestamptz, useconfig _text
pg_settings: name text, setting text, unit text, category text, short_desc text, extra_desc text, \
context text, vartype text, source text, min_val text, max_val text, enumvals _text, boot_val text, \
reset_val text, sourcefile text, sourceline int4, pending_restart bool
pg_tables: schemaname name, tablename name, tableowner name, tablespace name, hasindexes bool, hasrules bool, \
hastriggers bool, rowsecurity bool
pg_views: schemaname name, viewname name, viewowner name, definition text
pg_matviews: schemaname name, matviewname name, matviewowner name, tablespace name, hasindexes bool, \
ispopulated bool, definition text
pg_indexes: schemaname name, tablename name, indexname name, tablespace name, indexdef text
pg_sequences: schemaname name, sequencename name, sequenceowner name, data_type regtype, start_value int8, \
min_value int8, max_value int8, increment_by int8, cycle bool, cache_size int8, last_value int8
pg_sequence: seqrelid oid, seqtypid oid, seqstart int8, seqincrement int8, seqmax int8, seqmin int8, \
seqcache int8, seqcycle bool
pg_collation: oid oid, collname name, collnamespace oid, collowner oid, collprovider char, collisdeterministic bool, \
collencoding int4, collcollate name, collctype name, colliculocale text, collicurules text, collversion text
pg_depend: classid oid, objid oid, objsubid int4, refclassid oid, refobjid oid, refobjsubid int4, deptype char
pg_inherits: inhrelid oid, inhparent oid, inhseqno int4, inhdetachpending bool
pg_extension: oid oid, extname name, extowner oid, extnamespace oid, extrelocatable bool, extversion text, \
extconfig _oid, extcondition _text
pg_available_extensions: name name, default_version text, installed_version text, comment text
pg_tablespace: oid oid, spcname name, spcowner oid, spcacl _aclitem, spcoptions _text
pg_trigger: oid oid, tgrelid oid, tgparentid oid, tgname name, tgfoid oid, tgtype int2, tgenabled char, \
tgisinternal bool, tgconstrrelid oid, tgconstrindid oid, tgconstraint oid, tgdeferrable bool, tginitdeferred bool, \
tgnargs int2, tgattr int2vector, tgargs bytea, tgqual pg_node_tree, tgoldtable name, tgnewtable name
pg_rewrite: oid oid, rulename name, ev_class oid, ev_type char, ev_enabled char, is_instead bool, \
ev_qual pg_node_tree, ev_action pg_node_tree
pg_locks: locktype text, database oid, relation oid, page int4, tuple int2, virtualxid text, \
transactionid xid, classid oid, objid oid, objsubid int2, virtualtransaction text, pid int4, mode text, \
granted bool, fastpath bool
pg_stat_activity: datid oid, datname name, pid int4, leader_pid int4, usesysid oid, usename name, \
application_name text, client_addr inet, client_hostname text, client_port int4, backend_start timestamptz, \
xact_start timestamptz, query_start timestamptz, state_change timestamptz, wait_event_type text, \
wait_event text, state text, backend_xid xid, backend_xmin xid, query_id int8, query text, backend_type text
pg_stat_user_tables: relid oid, schemaname name, relname name, seq_scan int8, seq_tup_read int8, \
idx_scan int8, idx_tup_fetch int8, n_tup_ins int8, n_tup_upd int8, n_tup_del int8, n_tup_hot_upd int8, \
n_live_tup int8, n_dead_tup int8, n_mod_since_analyze int8, n_ins_since_vacuum int8, last_vacuum timestamptz, \
last_autovacuum timestamptz, last_analyze timestamptz, last_autoanalyze timestamptz, vacuum_count int8, \
autovacuum_count int8, analyze_count int8, autoanalyze_count int8
pg_stat_all_tables: relid oid, schemaname name, relname name, seq_scan int8, n_live_tup int8
pg_statio_user_tables: relid oid, schemaname name, relname name, heap_blks_read int8, heap_blks_hit int8
pg_language: oid oid, lanname name, lanowner oid, lanispl bool, lanpltrusted bool, lanplcallfoid oid, \
laninline oid, lanvalidator oid, lanacl _aclitem
pg_operator: oid oid, oprname name, oprnamespace oid, oprowner oid, oprkind char, oprcanmerge bool, \
oprcanhash bool, oprleft oid, oprright oid, oprresult oid, oprcom oid, oprnegate oid, oprcode regproc, \
oprrest regproc, oprjoin regproc
pg_opclass: oid oid, opcmethod oid, opcname name, opcnamespace oid, opcowner oid, opcfamily oid, \
opcintype oid, opcdefault bool, opckeytype oid
pg_range: rngtypid oid, rngsubtype oid, rngmultitypid oid, rngcollation oid, rngsubopc oid, rngcanonical regproc, \
rngsubdiff regproc
pg_partitioned_table: partrelid oid, partstrat char, partnatts int2, partdefid oid, partattrs int2vector, \
partclass oidvector, partcollation oidvector, partexprs pg_node_tree
pg_publication: oid oid, pubname name, pubowner oid, puballtables bool, pubinsert bool, pubupdate bool, \
pubdelete bool, pubtruncate bool, pubviaroot bool
pg_foreign_table: ftrelid oid, ftserver oid, ftoptions _text
pg_foreign_server: oid oid, srvname name, srvowner oid, srvfdw oid, srvtype text, srvversion text, \
srvacl _aclitem, srvoptions _text
pg_event_trigger: oid oid, evtname name, evtevent name, evtowner oid, evtfoid oid, evtenabled char, evttags _text
pg_policy: oid oid, polname name, polrelid oid, polcmd char, polpermissive bool, polroles _oid, \
polqual pg_node_tree, polwithcheck pg_node_tree
pg_cast: oid oid, castsource oid, casttarget oid, castfunc oid, castcontext char, castmethod char
pg_conversion: oid oid, conname name, connamespace oid, conowner oid, conforencoding int4, \
contoencoding int4, conproc regproc, condefault bool
pg_statistic_ext: oid oid, stxrelid oid, stxname name, stxnamespace oid, stxowner oid, stxstattarget int4, \
stxkeys int2vector, stxkind _char
pg_prepared_statements: name text, statement text, prepare_time timestamptz, parameter_types _regtype, \
result_types _regtype, from_sql bool, generic_plans int8, custom_plans int8
pg_prepared_xacts: transaction xid, gid text, prepared timestamptz, owner name, database name
pg_replication_slots: slot_name name, plugin name, slot_type text, datoid oid, database name, temporary bool, \
active bool, active_pid int4, xmin xid, catalog_xmin xid, restart_lsn pg_lsn, confirmed_flush_lsn pg_lsn
pg_stat_replication: pid int4, usesysid oid, usename name, application_name text, client_addr inet, state text
pg_timezone_names: name text, abbrev text, utc_offset interval, is_dst bool
pg_timezone_abbrevs: abbrev text, utc_offset interval, is_dst bool
pg_publication_rel: oid oid, prpubid oid, prrelid oid
pg_publication_namespace: oid oid, pnpubid oid, pnnspid oid
pg_subscription: oid oid, subdbid oid, subname name, subowner oid, subenabled bool, subconninfo text, subslotname name, subsynccommit text, subpublications _text
pg_subscription_rel: srsubid oid, srrelid oid, srsubstate char, srsublsn pg_lsn
pg_statistic: starelid oid, staattnum int2, stainherit bool, stanullfrac float4, stawidth int4, stadistinct float4
pg_stats: schemaname name, tablename name, attname name, inherited bool, null_frac float4, avg_width int4, n_distinct float4, most_common_vals _text, most_common_freqs _float4, histogram_bounds _text, correlation float4
pg_user_mapping: oid oid, umuser oid, umserver oid, umoptions _text
pg_default_acl: oid oid, defaclrole oid, defaclnamespace oid, defaclobjtype char, defaclacl _aclitem
pg_init_privs: objoid oid, classoid oid, objsubid int4, privtype char, initprivs _aclitem
pg_largeobject: loid oid, pageno int4, data bytea
pg_largeobject_metadata: oid oid, lomowner oid, lomacl _aclitem
pg_seclabel: objoid oid, classoid oid, objsubid int4, provider text, label text
pg_shseclabel: objoid oid, classoid oid, provider text, label text
pg_ts_config: oid oid, cfgname name, cfgnamespace oid, cfgowner oid, cfgparser oid
pg_ts_dict: oid oid, dictname name, dictnamespace oid, dictowner oid, dicttemplate oid, dictinitoption text
pg_ts_parser: oid oid, prsname name, prsnamespace oid, prsstart regproc, prstoken regproc, prsend regproc, prsheadline regproc, prslextype regproc
pg_ts_template: oid oid, tmplname name, tmplnamespace oid, tmplinit regproc, tmpllexize regproc
pg_transform: oid oid, trftype oid, trflang oid, trffromsql regproc, trftosql regproc
pg_aggregate: aggfnoid regproc, aggkind char, aggnumdirectargs int2, aggtransfn regproc, aggfinalfn regproc, aggcombinefn regproc, aggtranstype oid, agginitval text
pg_amop: oid oid, amopfamily oid, amoplefttype oid, amoprighttype oid, amopstrategy int2, amoppurpose char, amopopr oid, amopmethod oid, amopsortfamily oid
pg_amproc: oid oid, amprocfamily oid, amproclefttype oid, amprocrighttype oid, amprocnum int2, amproc regproc
pg_opfamily: oid oid, opfmethod oid, opfname name, opfnamespace oid, opfowner oid
pg_auth_members: oid oid, roleid oid, member oid, grantor oid, admin_option bool, inherit_option bool, set_option bool
pg_db_role_setting: setdatabase oid, setrole oid, setconfig _text
pg_stat_database: datid oid, datname name, numbackends int4, xact_commit int8, xact_rollback int8, blks_read int8, blks_hit int8, tup_returned int8, tup_fetched int8, tup_inserted int8, tup_updated int8, tup_deleted int8, conflicts int8, temp_files int8, temp_bytes int8, deadlocks int8, stats_reset timestamptz
pg_stat_gssapi: pid int4, gss_authenticated bool, principal text, encrypted bool
pg_file_settings: sourcefile text, sourceline int4, seqno int4, name text, setting text, applied bool, error text
pg_hba_file_rules: line_number int4, type text, database _text, user_name _text, address text, netmask text, auth_method text, options _text, error text
pg_config: name text, setting text
pg_cursors: name text, statement text, is_holdable bool, is_binary bool, is_scrollable bool, creation_time timestamptz
information_schema.schemata: catalog_name name, schema_name name, schema_owner name, \
default_character_set_catalog name, default_character_set_schema name, default_character_set_name name, \
sql_path varchar
information_schema.tables: table_catalog name, table_schema name, table_name name, table_type varchar, \
self_referencing_column_name name, reference_generation varchar, user_defined_type_catalog name, \
user_defined_type_schema name, user_defined_type_name name, is_insertable_into varchar, is_typed varchar, \
commit_action varchar
information_schema.columns: table_catalog name, table_schema name, table_name name, column_name name, \
ordinal_position int4, column_default text, is_nullable varchar, data_type varchar, character_maximum_length int4, \
character_octet_length int4, numeric_precision int4, numeric_precision_radix int4, numeric_scale int4, \
datetime_precision int4, interval_type varchar, interval_precision int4, character_set_catalog name, \
character_set_schema name, character_set_name name, collation_catalog name, collation_schema name, \
collation_name name, domain_catalog name, domain_schema name, domain_name name, udt_catalog name, \
udt_schema name, udt_name name, scope_catalog name, scope_schema name, scope_name name, \
maximum_cardinality int4, dtd_identifier name, is_self_referencing varchar, is_identity varchar, \
identity_generation varchar, identity_start varchar, identity_increment varchar, identity_maximum varchar, \
identity_minimum varchar, identity_cycle varchar, is_generated varchar, generation_expression varchar, is_updatable varchar
information_schema.views: table_catalog name, table_schema name, table_name name, view_definition varchar, \
check_option varchar, is_updatable varchar, is_insertable_into varchar, is_trigger_updatable varchar, \
is_trigger_deletable varchar, is_trigger_insertable_into varchar
information_schema.table_constraints: constraint_catalog name, constraint_schema name, constraint_name name, \
table_catalog name, table_schema name, table_name name, constraint_type varchar, is_deferrable varchar, \
initially_deferred varchar, enforced varchar, nulls_distinct varchar
information_schema.key_column_usage: constraint_catalog name, constraint_schema name, constraint_name name, \
table_catalog name, table_schema name, table_name name, column_name name, ordinal_position int4, \
position_in_unique_constraint int4
information_schema.constraint_column_usage: table_catalog name, table_schema name, table_name name, \
column_name name, constraint_catalog name, constraint_schema name, constraint_name name
information_schema.referential_constraints: constraint_catalog name, constraint_schema name, \
constraint_name name, unique_constraint_catalog name, unique_constraint_schema name, unique_constraint_name name, \
match_option varchar, update_rule varchar, delete_rule varchar
information_schema.check_constraints: constraint_catalog name, constraint_schema name, constraint_name name, \
check_clause varchar
information_schema.sequences: sequence_catalog name, sequence_schema name, sequence_name name, data_type varchar, \
numeric_precision int4, numeric_precision_radix int4, numeric_scale int4, start_value varchar, minimum_value varchar, \
maximum_value varchar, increment varchar, cycle_option varchar
information_schema.routines: specific_catalog name, specific_schema name, specific_name name, \
routine_catalog name, routine_schema name, routine_name name, routine_type varchar, data_type varchar, \
routine_body varchar, routine_definition varchar, external_language varchar, is_deterministic varchar
information_schema.parameters: specific_catalog name, specific_schema name, specific_name name, \
ordinal_position int4, parameter_mode varchar, parameter_name name, data_type varchar
information_schema.domains: domain_catalog name, domain_schema name, domain_name name, data_type varchar
information_schema.table_privileges: grantor name, grantee name, table_catalog name, table_schema name, \
table_name name, privilege_type varchar, is_grantable varchar, with_hierarchy varchar
information_schema.column_privileges: grantor name, grantee name, table_catalog name, table_schema name, \
table_name name, column_name name, privilege_type varchar, is_grantable varchar
information_schema.role_table_grants: grantor name, grantee name, table_catalog name, table_schema name, \
table_name name, privilege_type varchar, is_grantable varchar, with_hierarchy varchar
information_schema.enabled_roles: role_name name
information_schema.applicable_roles: grantee name, role_name name, is_grantable varchar
information_schema.character_sets: character_set_catalog name, character_set_schema name, \
character_set_name name, character_repertoire name, form_of_use name, default_collate_catalog name, \
default_collate_schema name, default_collate_name name
information_schema.element_types: object_catalog name, object_schema name, object_name name, object_type varchar, \
collection_type_identifier name, data_type varchar, udt_catalog name, udt_schema name, udt_name name
";

struct Rel {
    name: &'static str,
    cols: Vec<OutCol>,
}

fn rels() -> &'static Vec<Rel> {
    static R: OnceLock<Vec<Rel>> = OnceLock::new();
    R.get_or_init(|| {
        let mut out = vec![];
        for line in RELATIONS.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (name, cols) = line.split_once(':').expect("catalog relation");
            let cols = cols
                .split(',')
                .map(|c| {
                    let (n, t) = c.trim().split_once(' ').expect("catalog column");
                    OutCol::new(n.trim().to_string(), parse_type(t.trim()))
                })
                .collect();
            out.push(Rel { name: name.trim(), cols });
        }
        out
    })
}

fn parse_type(s: &str) -> Type {
    let (array, name) = match s.strip_prefix('_') {
        Some(n) => (true, n),
        None => (false, s),
    };
    if name == "anyarray" {
        return Type::array_of(Base::Text);
    }
    let base =
        super::types::TYPES.iter().find(|t| t.name == name).map(|t| t.base).unwrap_or(Base::Text);
    Type { base, array }
}

fn find(name: &str) -> Option<&'static Rel> {
    let lower = name.to_ascii_lowercase();
    rels().iter().find(|r| {
        r.name == lower || r.name.strip_prefix("pg_catalog.").is_some_and(|n| n == lower) || {
            // `information_schema.tables` is also reachable unqualified in that schema.
            r.name.rsplit('.').next() == Some(lower.as_str())
                && r.name.starts_with("information_schema")
        }
    })
}

/// Whether `schema.name` names a system catalog relation.
pub fn is_catalog_relation(schema: Option<&str>, name: &str) -> bool {
    match schema {
        Some("pg_catalog") => rels().iter().any(|r| r.name == name.to_ascii_lowercase()),
        Some("information_schema") => rels()
            .iter()
            .any(|r| r.name == format!("information_schema.{}", name.to_ascii_lowercase())),
        Some(_) => false,
        // pg_catalog is implicitly first on the search path.
        None => {
            rels().iter().any(|r| r.name == name.to_ascii_lowercase()) && name.starts_with("pg_")
        }
    }
}

/// Resolves the relation a FROM item refers to, qualified or not.
pub fn columns(name: &str) -> Vec<OutCol> {
    find(name).map(|r| r.cols.clone()).unwrap_or_default()
}

fn qualified(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some("information_schema") => format!("information_schema.{}", name.to_ascii_lowercase()),
        _ => name.to_ascii_lowercase(),
    }
}

pub fn relation_key(schema: Option<&str>, name: &str) -> String {
    qualified(schema, name)
}

fn n(v: i64) -> Value {
    Value::Int(v)
}

fn t(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}

fn ch(c: char) -> Value {
    Value::Text(c.to_string())
}

fn b(v: bool) -> Value {
    Value::Bool(v)
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(Box::new(Array::new(items)))
}

const NULL: Value = Value::Null;

/// Builds the rows of a catalog relation.
pub fn rows(name: &str, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    let db: &DbState = ctx.db;
    let user = ctx.rt.user.clone();
    let database = ctx.rt.database.clone();
    let mut out = vec![];
    match name.to_ascii_lowercase().as_str() {
        "pg_namespace" => {
            for s in db.schemas.values() {
                out.push(vec![n(s.oid as i64), t(&s.name), n(10), NULL]);
            }
        }
        "pg_class" => {
            for tb in db.tables.values() {
                out.push(pg_class_row(
                    tb.oid,
                    &tb.name,
                    tb.schema,
                    tb.relkind(),
                    tb.columns.iter().filter(|c| !c.dropped).count() as i64,
                    tb.constraints
                        .iter()
                        .filter(|c| matches!(c.kind, ConstraintKind::Check(_)))
                        .count() as i64,
                    !tb.indexes.is_empty(),
                    tb.rows.len() as i64,
                    tb.type_oid,
                ));
                for idx in &tb.indexes {
                    out.push(pg_class_row(
                        idx.oid,
                        &idx.name,
                        tb.schema,
                        'i',
                        idx.cols.len() as i64,
                        0,
                        false,
                        tb.rows.len() as i64,
                        0,
                    ));
                }
            }
            for s in db.sequences.values() {
                out.push(pg_class_row(s.oid, &s.name, s.schema, 'S', 3, 0, false, 1, 0));
            }
        }
        "pg_attribute" => {
            for tb in db.tables.values() {
                for (i, c) in tb.columns.iter().enumerate() {
                    let ty = c.ty;
                    out.push(vec![
                        n(tb.oid as i64),
                        t(&c.name),
                        n(ty.oid() as i64),
                        n(-1),
                        n(ty.typlen() as i64),
                        n(i as i64 + 1),
                        n(if ty.array { 1 } else { 0 }),
                        n(-1),
                        n(c.typmod as i64),
                        b(ty.base.info().map(|i| i.byval).unwrap_or(false) && !ty.array),
                        ch(ty.base.info().map(|i| i.align as char).unwrap_or('i')),
                        ch(if ty.typlen() == -1 { 'x' } else { 'p' }),
                        ch('\0'),
                        b(c.not_null),
                        b(c.default.is_some()),
                        b(false),
                        ch(match c.identity {
                            Some((true, _)) => 'a',
                            Some((false, _)) => 'd',
                            None => '\0',
                        }),
                        ch(if c.generated.is_some() { 's' } else { '\0' }),
                        b(c.dropped),
                        b(true),
                        n(0),
                        n(if ty.is_string() { 100 } else { 0 }),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                    ]);
                }
            }
        }
        "pg_type" => {
            for ti in super::types::TYPES {
                let ty = Type::of(ti.base);
                out.push(pg_type_row(
                    ti.oid,
                    ti.name,
                    PG_CATALOG_NS,
                    ti.len,
                    ti.byval,
                    ti.typtype as char,
                    ti.category as char,
                    ti.preferred,
                    0,
                    ti.array_oid,
                    ti.align as char,
                    ty,
                ));
                if ti.array_oid != 0 {
                    out.push(pg_type_row(
                        ti.array_oid,
                        &format!("_{}", ti.name),
                        PG_CATALOG_NS,
                        -1,
                        false,
                        'b',
                        'A',
                        false,
                        ti.oid,
                        0,
                        'i',
                        Type::array_of(ti.base),
                    ));
                }
            }
            for e in db.enums.values() {
                out.push(pg_type_row(
                    e.oid,
                    &e.name,
                    e.schema,
                    4,
                    true,
                    'e',
                    'E',
                    false,
                    0,
                    e.oid + 1,
                    'i',
                    Type::INT4,
                ));
                out.push(pg_type_row(
                    e.oid + 1,
                    &format!("_{}", e.name),
                    e.schema,
                    -1,
                    false,
                    'b',
                    'A',
                    false,
                    e.oid,
                    0,
                    'i',
                    Type::INT4,
                ));
            }
            // Composite types for tables.
            for tb in db.tables.values() {
                if tb.type_oid != 0 {
                    let mut r = pg_type_row(
                        tb.type_oid,
                        &tb.name,
                        tb.schema,
                        -1,
                        false,
                        'c',
                        'C',
                        false,
                        0,
                        0,
                        'd',
                        Type::RECORD,
                    );
                    r[11] = n(tb.oid as i64);
                    out.push(r);
                }
            }
        }
        "pg_index" => {
            for tb in db.tables.values() {
                for idx in &tb.indexes {
                    let keys: Vec<Value> =
                        idx.cols.iter().map(|c| n(c.map_or(0, |x| x as i64 + 1))).collect();
                    out.push(vec![
                        n(idx.oid as i64),
                        n(tb.oid as i64),
                        n(idx.cols.len() as i64),
                        n(idx.cols.len() as i64),
                        b(idx.unique),
                        b(idx.nulls_not_distinct),
                        b(idx.primary),
                        b(false),
                        b(true),
                        b(false),
                        b(true),
                        b(false),
                        b(true),
                        b(true),
                        b(false),
                        arr(keys),
                        arr(idx.cols.iter().map(|_| n(0)).collect()),
                        arr(idx.cols.iter().map(|_| n(0)).collect()),
                        arr(idx.desc.iter().map(|d| n(if *d { 1 } else { 0 })).collect()),
                        NULL,
                        idx.predicate.clone().map_or(NULL, t),
                    ]);
                }
            }
        }
        "pg_constraint" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    let (confrelid, confkey, upd, del) = match &c.kind {
                        ConstraintKind::ForeignKey {
                            ref_table,
                            ref_cols,
                            on_update,
                            on_delete,
                        } => (
                            *ref_table as i64,
                            arr(ref_cols.iter().map(|x| n(*x as i64 + 1)).collect()),
                            on_update.code(),
                            on_delete.code(),
                        ),
                        _ => (0, NULL, ' ', ' '),
                    };
                    out.push(vec![
                        n(c.oid as i64),
                        t(&c.name),
                        n(tb.schema as i64),
                        ch(c.contype()),
                        b(c.deferrable),
                        b(false),
                        b(true),
                        n(tb.oid as i64),
                        n(0),
                        n(c.index_oid.unwrap_or(0) as i64),
                        n(0),
                        n(confrelid),
                        ch(upd),
                        ch(del),
                        ch('s'),
                        b(true),
                        n(0),
                        b(false),
                        if c.cols.is_empty() {
                            NULL
                        } else {
                            arr(c.cols.iter().map(|x| n(*x as i64 + 1)).collect())
                        },
                        confkey,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        match &c.kind {
                            ConstraintKind::Check(sql) => t(sql),
                            _ => NULL,
                        },
                    ]);
                }
            }
        }
        "pg_attrdef" => {
            for tb in db.tables.values() {
                for (i, c) in tb.columns.iter().enumerate() {
                    if let Some(d) = &c.default {
                        out.push(vec![
                            n((tb.oid as i64) * 1000 + i as i64),
                            n(tb.oid as i64),
                            n(i as i64 + 1),
                            t(d),
                        ]);
                    }
                }
            }
        }
        "pg_database" => {
            out.push(vec![
                n(super::catalog::DATABASE_OID as i64),
                t(&database),
                n(10),
                n(6),
                ch('c'),
                b(false),
                b(true),
                n(-1),
                n(1),
                n(1),
                n(1663),
                t("C"),
                t("C"),
                NULL,
                NULL,
                NULL,
                NULL,
            ]);
        }
        "pg_proc" => {
            for s in super::sigs::all_sigs() {
                out.push(vec![
                    n(s.oid as i64),
                    t(s.name),
                    n(PG_CATALOG_NS as i64),
                    n(10),
                    n(12),
                    Value::Float(1.0),
                    Value::Float(0.0),
                    n(0),
                    n(0),
                    ch(match s.kind {
                        super::sigs::Kind::Agg => 'a',
                        super::sigs::Kind::Window => 'w',
                        _ => 'f',
                    }),
                    b(false),
                    b(false),
                    b(s.strict),
                    b(s.kind == super::sigs::Kind::Srf),
                    ch('i'),
                    ch('s'),
                    n(s.args.len() as i64),
                    n(0),
                    n(s.ret.oid() as i64),
                    arr(s.args.iter().map(|a| n(a.oid() as i64)).collect()),
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    t(s.name),
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                ]);
            }
        }
        "pg_enum" => {
            for e in db.enums.values() {
                for (order, label, oid) in &e.labels {
                    out.push(vec![
                        n(*oid as i64),
                        n(e.oid as i64),
                        Value::Float(*order as f64),
                        t(label),
                    ]);
                }
            }
        }
        "pg_description" => {
            for tb in db.tables.values() {
                if let Some(c) = &tb.comment {
                    out.push(vec![n(tb.oid as i64), n(1259), n(0), t(c)]);
                }
                for (i, col) in tb.columns.iter().enumerate() {
                    if let Some(c) = &col.comment {
                        out.push(vec![n(tb.oid as i64), n(1259), n(i as i64 + 1), t(c)]);
                    }
                }
                for con in &tb.constraints {
                    if let Some(c) = &con.comment {
                        out.push(vec![n(con.oid as i64), n(2606), n(0), t(c)]);
                    }
                }
            }
            for s in db.schemas.values() {
                if let Some(c) = &s.comment {
                    out.push(vec![n(s.oid as i64), n(2615), n(0), t(c)]);
                }
            }
        }
        "pg_shdescription" => {
            if let Some(c) = &db.db_comment {
                out.push(vec![n(super::catalog::DATABASE_OID as i64), n(1262), t(c)]);
            }
        }
        "pg_am" => {
            for (oid, name, kind) in [
                (2, "heap", 't'),
                (403, "btree", 'i'),
                (405, "hash", 'i'),
                (783, "gist", 'i'),
                (2742, "gin", 'i'),
            ] {
                out.push(vec![n(oid), t(name), n(0), ch(kind)]);
            }
        }
        "pg_roles" => {
            out.push(vec![
                t(&user),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                n(-1),
                t("********"),
                NULL,
                b(true),
                NULL,
                n(10),
            ]);
        }
        "pg_authid" => {
            out.push(vec![
                n(10),
                t(&user),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                n(-1),
                NULL,
                NULL,
            ]);
        }
        "pg_user" => {
            out.push(vec![
                t(&user),
                n(10),
                b(true),
                b(true),
                b(true),
                b(true),
                t("********"),
                NULL,
                NULL,
            ]);
        }
        "pg_settings" => {
            for (k, v) in ctx.rt.settings.all() {
                out.push(vec![
                    t(&k),
                    t(&v),
                    NULL,
                    t("Client Connection Defaults"),
                    t(""),
                    NULL,
                    t("user"),
                    t(if v == "on" || v == "off" { "bool" } else { "string" }),
                    t("default"),
                    NULL,
                    NULL,
                    NULL,
                    t(&v),
                    t(&v),
                    NULL,
                    NULL,
                    b(false),
                ]);
            }
        }
        "pg_tables" => {
            for tb in db.tables.values().filter(|t| t.kind == super::catalog::RelKind::Table) {
                out.push(vec![
                    t(db.schema_name(tb.schema)),
                    t(&tb.name),
                    t(&user),
                    NULL,
                    b(!tb.indexes.is_empty()),
                    b(false),
                    b(false),
                    b(false),
                ]);
            }
        }
        "pg_views" => {
            for tb in db.tables.values().filter(|t| t.kind == super::catalog::RelKind::View) {
                out.push(vec![
                    t(db.schema_name(tb.schema)),
                    t(&tb.name),
                    t(&user),
                    t(tb.view_sql.clone().unwrap_or_default()),
                ]);
            }
        }
        "pg_matviews" => {}
        "pg_indexes" => {
            for tb in db.tables.values() {
                for idx in &tb.indexes {
                    out.push(vec![
                        t(db.schema_name(tb.schema)),
                        t(&tb.name),
                        t(&idx.name),
                        NULL,
                        index_def(db, idx.oid).map_or(NULL, t),
                    ]);
                }
            }
        }
        "pg_sequences" => {
            for s in db.sequences.values() {
                let last =
                    ctx.seqs.get(&s.oid).filter(|v| v.is_called).map(|v| n(v.last)).unwrap_or(NULL);
                out.push(vec![
                    t(db.schema_name(s.schema)),
                    t(&s.name),
                    t(&user),
                    n(s.ty.oid() as i64),
                    n(s.start),
                    n(s.min),
                    n(s.max),
                    n(s.increment),
                    b(s.cycle),
                    n(s.cache),
                    last,
                ]);
            }
        }
        "pg_sequence" => {
            for s in db.sequences.values() {
                out.push(vec![
                    n(s.oid as i64),
                    n(s.ty.oid() as i64),
                    n(s.start),
                    n(s.increment),
                    n(s.max),
                    n(s.min),
                    n(s.cache),
                    b(s.cycle),
                ]);
            }
        }
        "pg_collation" => {
            out.push(vec![
                n(100),
                t("default"),
                n(PG_CATALOG_NS as i64),
                n(10),
                ch('d'),
                b(true),
                n(-1),
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
            ]);
            out.push(vec![
                n(950),
                t("C"),
                n(PG_CATALOG_NS as i64),
                n(10),
                ch('c'),
                b(true),
                n(-1),
                t("C"),
                t("C"),
                NULL,
                NULL,
                NULL,
            ]);
        }
        "pg_extension" => {
            out.push(vec![
                n(13000),
                t("plpgsql"),
                n(10),
                n(PG_CATALOG_NS as i64),
                b(false),
                t("1.0"),
                NULL,
                NULL,
            ]);
        }
        "pg_available_extensions" => {
            out.push(vec![t("plpgsql"), t("1.0"), t("1.0"), t("PL/pgSQL procedural language")]);
        }
        "pg_tablespace" => {
            out.push(vec![n(1663), t("pg_default"), n(10), NULL, NULL]);
            out.push(vec![n(1664), t("pg_global"), n(10), NULL, NULL]);
        }
        "pg_language" => {
            for (oid, name) in [(12, "internal"), (13, "c"), (14, "sql"), (13000, "plpgsql")] {
                out.push(vec![
                    n(oid),
                    t(name),
                    n(10),
                    b(oid == 13000),
                    b(oid == 13000),
                    n(0),
                    n(0),
                    n(0),
                    NULL,
                ]);
            }
        }
        "pg_stat_activity" => {
            out.push(vec![
                n(super::catalog::DATABASE_OID as i64),
                t(&database),
                n(ctx.rt.pid as i64),
                NULL,
                n(10),
                t(&user),
                t(ctx.rt.settings.get("application_name").unwrap_or_default()),
                t("127.0.0.1"),
                NULL,
                n(0),
                Value::Ts(ctx.rt.now),
                Value::Ts(ctx.rt.now),
                Value::Ts(ctx.rt.stmt_now),
                Value::Ts(ctx.rt.stmt_now),
                NULL,
                NULL,
                t("active"),
                NULL,
                NULL,
                NULL,
                t("SELECT"),
                t("client backend"),
            ]);
        }
        "pg_stat_user_tables" | "pg_stat_all_tables" | "pg_statio_user_tables" => {
            let full = name.eq_ignore_ascii_case("pg_stat_user_tables");
            for tb in db.tables.values().filter(|t| t.kind == super::catalog::RelKind::Table) {
                let mut r = vec![n(tb.oid as i64), t(db.schema_name(tb.schema)), t(&tb.name)];
                let ncols = columns(name).len();
                if full {
                    r.push(n(0));
                    r.push(n(0));
                    r.push(n(0));
                    r.push(n(0));
                    r.push(n(tb.rows.len() as i64));
                    r.push(n(0));
                    r.push(n(0));
                    r.push(n(0));
                    r.push(n(tb.rows.len() as i64));
                }
                while r.len() < ncols {
                    r.push(if columns(name)[r.len()].ty.base == Base::Timestamptz {
                        NULL
                    } else {
                        n(0)
                    });
                }
                out.push(r);
            }
        }
        "pg_timezone_names" => {}
        "pg_timezone_abbrevs" => {}
        // information_schema
        "information_schema.schemata" => {
            for s in db.schemas.values() {
                out.push(vec![t(&database), t(&s.name), t(&user), NULL, NULL, NULL, NULL]);
            }
        }
        "information_schema.tables" => {
            for tb in db.tables.values() {
                let kind = match tb.kind {
                    super::catalog::RelKind::View => "VIEW",
                    _ => "BASE TABLE",
                };
                out.push(vec![
                    t(&database),
                    t(db.schema_name(tb.schema)),
                    t(&tb.name),
                    t(kind),
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    t(if kind == "VIEW" { "NO" } else { "YES" }),
                    t("NO"),
                    NULL,
                ]);
            }
        }
        "information_schema.columns" => {
            for tb in db.tables.values() {
                for (i, c) in tb.live_columns() {
                    let ty = c.ty;
                    let (prec, radix, scale) = numeric_info(ty, c.typmod);
                    out.push(vec![
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&tb.name),
                        t(&c.name),
                        n(i as i64 + 1),
                        c.default.clone().map_or(NULL, t),
                        t(if c.not_null { "NO" } else { "YES" }),
                        t(data_type_name(ty)),
                        char_max_len(ty, c.typmod),
                        NULL,
                        prec,
                        radix,
                        scale,
                        datetime_precision(ty, c.typmod),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        t(&database),
                        t("pg_catalog"),
                        t(ty.name()),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        t(format!("{}", i + 1)),
                        t("NO"),
                        t(if c.identity.is_some() { "YES" } else { "NO" }),
                        match c.identity {
                            Some((true, _)) => t("ALWAYS"),
                            Some((false, _)) => t("BY DEFAULT"),
                            None => NULL,
                        },
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        t(if c.generated.is_some() { "ALWAYS" } else { "NEVER" }),
                        c.generated.clone().map_or(NULL, t),
                        t("YES"),
                    ]);
                }
            }
        }
        "information_schema.views" => {
            for tb in db.tables.values().filter(|t| t.kind == super::catalog::RelKind::View) {
                out.push(vec![
                    t(&database),
                    t(db.schema_name(tb.schema)),
                    t(&tb.name),
                    t(tb.view_sql.clone().unwrap_or_default()),
                    t("NONE"),
                    t("NO"),
                    t("NO"),
                    t("NO"),
                    t("NO"),
                    t("NO"),
                ]);
            }
        }
        "information_schema.table_constraints" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    let kind = match c.kind {
                        ConstraintKind::PrimaryKey => "PRIMARY KEY",
                        ConstraintKind::Unique => "UNIQUE",
                        ConstraintKind::Check(_) => "CHECK",
                        ConstraintKind::ForeignKey { .. } => "FOREIGN KEY",
                    };
                    out.push(vec![
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&c.name),
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&tb.name),
                        t(kind),
                        t(if c.deferrable { "YES" } else { "NO" }),
                        t("NO"),
                        t("YES"),
                        NULL,
                    ]);
                }
                // NOT NULL constraints appear as CHECK constraints.
                for (_, col) in tb.live_columns().filter(|(_, c)| c.not_null) {
                    out.push(vec![
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(format!("{}_{}_not_null", tb.name, col.name)),
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&tb.name),
                        t("CHECK"),
                        t("NO"),
                        t("NO"),
                        t("YES"),
                        NULL,
                    ]);
                }
            }
        }
        "information_schema.key_column_usage" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    if !matches!(
                        c.kind,
                        ConstraintKind::PrimaryKey
                            | ConstraintKind::Unique
                            | ConstraintKind::ForeignKey { .. }
                    ) {
                        continue;
                    }
                    for (pos, &col) in c.cols.iter().enumerate() {
                        out.push(vec![
                            t(&database),
                            t(db.schema_name(tb.schema)),
                            t(&c.name),
                            t(&database),
                            t(db.schema_name(tb.schema)),
                            t(&tb.name),
                            t(&tb.columns[col].name),
                            n(pos as i64 + 1),
                            match c.kind {
                                ConstraintKind::ForeignKey { .. } => n(pos as i64 + 1),
                                _ => NULL,
                            },
                        ]);
                    }
                }
            }
        }
        "information_schema.constraint_column_usage" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    let (target, cols) = match &c.kind {
                        ConstraintKind::ForeignKey { ref_table, ref_cols, .. } => {
                            (*ref_table, ref_cols.clone())
                        }
                        _ => (tb.oid, c.cols.clone()),
                    };
                    let Some(tt) = db.table(target) else { continue };
                    for col in cols {
                        out.push(vec![
                            t(&database),
                            t(db.schema_name(tt.schema)),
                            t(&tt.name),
                            t(&tt.columns[col].name),
                            t(&database),
                            t(db.schema_name(tb.schema)),
                            t(&c.name),
                        ]);
                    }
                }
            }
        }
        "information_schema.referential_constraints" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    let ConstraintKind::ForeignKey { ref_table, ref_cols, on_update, on_delete } =
                        &c.kind
                    else {
                        continue;
                    };
                    let unique = db.table(*ref_table).and_then(|p| {
                        p.constraints
                            .iter()
                            .find(|pc| {
                                matches!(
                                    pc.kind,
                                    ConstraintKind::PrimaryKey | ConstraintKind::Unique
                                ) && pc.cols == *ref_cols
                            })
                            .map(|pc| (db.schema_name(p.schema).to_string(), pc.name.clone()))
                    });
                    out.push(vec![
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&c.name),
                        unique.as_ref().map_or(NULL, |_| t(&database)),
                        unique.as_ref().map_or(NULL, |(s, _)| t(s)),
                        unique.as_ref().map_or(NULL, |(_, n)| t(n)),
                        t("NONE"),
                        t(on_update.sql()),
                        t(on_delete.sql()),
                    ]);
                }
            }
        }
        "information_schema.check_constraints" => {
            for tb in db.tables.values() {
                for c in &tb.constraints {
                    if let ConstraintKind::Check(sql) = &c.kind {
                        out.push(vec![
                            t(&database),
                            t(db.schema_name(tb.schema)),
                            t(&c.name),
                            t(sql),
                        ]);
                    }
                }
            }
        }
        "information_schema.sequences" => {
            for s in db.sequences.values() {
                out.push(vec![
                    t(&database),
                    t(db.schema_name(s.schema)),
                    t(&s.name),
                    t(data_type_name(s.ty)),
                    n(64),
                    n(2),
                    n(0),
                    t(s.start.to_string()),
                    t(s.min.to_string()),
                    t(s.max.to_string()),
                    t(s.increment.to_string()),
                    t(if s.cycle { "YES" } else { "NO" }),
                ]);
            }
        }
        "information_schema.enabled_roles" => out.push(vec![t(&user)]),
        "information_schema.applicable_roles" => {}
        "information_schema.character_sets" => {
            out.push(vec![
                NULL,
                NULL,
                t("UTF8"),
                t("UCS"),
                t("UTF8"),
                NULL,
                t("pg_catalog"),
                t("C"),
            ]);
        }
        "information_schema.table_privileges" | "information_schema.role_table_grants" => {
            for tb in db.tables.values() {
                for p in
                    ["INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
                {
                    out.push(vec![
                        t(&user),
                        t(&user),
                        t(&database),
                        t(db.schema_name(tb.schema)),
                        t(&tb.name),
                        t(p),
                        t("YES"),
                        t(if p == "SELECT" { "YES" } else { "NO" }),
                    ]);
                }
            }
        }
        "information_schema.routines"
        | "information_schema.parameters"
        | "information_schema.domains"
        | "information_schema.column_privileges"
        | "information_schema.element_types" => {}
        _ => {}
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn pg_class_row(
    oid: u32,
    name: &str,
    schema: u32,
    kind: char,
    natts: i64,
    nchecks: i64,
    has_index: bool,
    ntuples: i64,
    type_oid: u32,
) -> Row {
    vec![
        n(oid as i64),
        t(name),
        n(schema as i64),
        n(type_oid as i64),
        n(0),
        n(10),
        n(if kind == 'i' { 403 } else { 2 }),
        n(oid as i64),
        n(0),
        n((ntuples + 99) / 100),
        Value::Float(ntuples as f64),
        n(0),
        n(0),
        b(has_index),
        b(false),
        ch('p'),
        ch(kind),
        n(natts),
        n(nchecks),
        b(false),
        b(false),
        b(false),
        b(false),
        b(false),
        b(true),
        ch('d'),
        b(false),
        n(0),
        n(1),
        n(1),
        NULL,
        NULL,
        NULL,
    ]
}

#[allow(clippy::too_many_arguments)]
fn pg_type_row(
    oid: u32,
    name: &str,
    schema: u32,
    len: i16,
    byval: bool,
    typtype: char,
    category: char,
    preferred: bool,
    elem: u32,
    array: u32,
    align: char,
    _ty: Type,
) -> Row {
    vec![
        n(oid as i64),
        t(name),
        n(schema as i64),
        n(10),
        n(len as i64),
        b(byval),
        ch(typtype),
        ch(category),
        b(preferred),
        b(true),
        ch(','),
        n(0),
        n(0),
        n(elem as i64),
        n(array as i64),
        n(0),
        n(0),
        n(0),
        n(0),
        n(0),
        n(0),
        n(0),
        ch(align),
        ch(if len == -1 { 'x' } else { 'p' }),
        b(false),
        n(0),
        n(-1),
        n(0),
        // Collatable types use the default collation (100).
        n(if matches!(_ty.base, Base::Text | Base::Varchar | Base::Bpchar | Base::Name) {
            100
        } else {
            0
        }),
        NULL,
        NULL,
        NULL,
    ]
}

/// information_schema.columns.data_type.
fn data_type_name(ty: Type) -> String {
    if ty.array {
        return "ARRAY".into();
    }
    match ty.base {
        Base::Enum(_) => "USER-DEFINED".into(),
        _ => ty.display(-1),
    }
}

fn char_max_len(ty: Type, typmod: i32) -> Value {
    match (ty.base, super::types::typmod_len(typmod)) {
        (Base::Varchar | Base::Bpchar, Some(l)) => n(l as i64),
        _ => NULL,
    }
}

fn numeric_info(ty: Type, typmod: i32) -> (Value, Value, Value) {
    match ty.base {
        Base::Int2 => (n(16), n(2), n(0)),
        Base::Int4 => (n(32), n(2), n(0)),
        Base::Int8 => (n(64), n(2), n(0)),
        Base::Float4 => (n(24), n(2), NULL),
        Base::Float8 => (n(53), n(2), NULL),
        Base::Numeric => {
            if typmod >= 4 {
                let m = typmod - 4;
                (n(((m >> 16) & 0xffff) as i64), n(10), n((m & 0xffff) as i64))
            } else {
                (NULL, n(10), NULL)
            }
        }
        _ => (NULL, NULL, NULL),
    }
}

fn datetime_precision(ty: Type, typmod: i32) -> Value {
    match ty.base {
        Base::Date => n(0),
        Base::Time | Base::Timetz | Base::Timestamp | Base::Timestamptz | Base::Interval => {
            n(if typmod >= 0 { typmod as i64 } else { 6 })
        }
        _ => NULL,
    }
}

/// Resolves a `reg*` name to an OID, as a cast from text does.
pub fn resolve_reg(
    db: &DbState,
    search_path: &[String],
    user: &str,
    base: Base,
    name: &str,
) -> Option<i64> {
    let n = name.trim();
    // A numeric literal is an OID, the way Postgres's regclassin reads it.
    if let Ok(oid) = n.parse::<u32>() {
        return Some(oid as i64);
    }
    let (schema, base_name) = match n.rsplit_once('.') {
        Some((s, b)) => (Some(s.trim_matches('"').to_string()), b.trim_matches('"').to_string()),
        None => (None, n.trim_matches('"').to_string()),
    };
    match base {
        Base::Regtype => super::types::TYPES
            .iter()
            .find(|t| t.name == base_name || t.display == base_name)
            .map(|t| t.oid as i64)
            .or_else(|| db.enums.values().find(|e| e.name == base_name).map(|e| e.oid as i64)),
        Base::Regnamespace => db.schema_by_name(&base_name).map(|o| o as i64),
        Base::Regproc | Base::Regprocedure => {
            super::sigs::all_sigs().iter().find(|s| s.name == base_name).map(|s| s.oid as i64)
        }
        Base::Regrole => (base_name == "postgres" || base_name == user).then_some(10),
        _ => {
            let schemas: Vec<u32> = match &schema {
                Some(s) => db.schema_by_name(s).into_iter().collect(),
                None => search_path.iter().filter_map(|s| db.schema_by_name(s)).collect(),
            };
            schemas.iter().find_map(|&sid| {
                db.find_table(sid, &base_name)
                    .map(|t| t.oid as i64)
                    .or_else(|| db.find_sequence(sid, &base_name).map(|s| s.oid as i64))
                    .or_else(|| db.find_index(sid, &base_name).map(|(_, i)| i.oid as i64))
            })
        }
    }
}

/// The error a failed `reg*` lookup raises.
pub fn undefined_reg(base: Base, name: &str) -> super::error::PgError {
    use super::error::{PgError, code};
    match base {
        Base::Regtype => {
            PgError::new(code::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist"))
        }
        Base::Regnamespace => {
            PgError::new(code::INVALID_SCHEMA_NAME, format!("schema \"{name}\" does not exist"))
        }
        Base::Regproc | Base::Regprocedure => {
            PgError::new(code::UNDEFINED_FUNCTION, format!("function \"{name}\" does not exist"))
        }
        Base::Regrole => {
            PgError::new(code::UNDEFINED_OBJECT, format!("role \"{name}\" does not exist"))
        }
        _ => PgError::new(code::UNDEFINED_TABLE, format!("relation \"{name}\" does not exist")),
    }
}

/// `pg_get_constraintdef`.
pub fn constraint_def(db: &DbState, oid: u32) -> Option<String> {
    for tb in db.tables.values() {
        let Some(c) = tb.constraints.iter().find(|c| c.oid == oid) else { continue };
        let cols = |idx: &[usize], t: &super::catalog::Table| -> String {
            idx.iter()
                .map(|&i| super::funcs::quote_ident(&t.columns[i].name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Some(match &c.kind {
            ConstraintKind::PrimaryKey => format!("PRIMARY KEY ({})", cols(&c.cols, tb)),
            ConstraintKind::Unique => format!("UNIQUE ({})", cols(&c.cols, tb)),
            ConstraintKind::Check(sql) => format!("CHECK (({sql}))"),
            ConstraintKind::ForeignKey { ref_table, ref_cols, on_update, on_delete } => {
                let parent = db.table(*ref_table)?;
                let mut s = format!(
                    "FOREIGN KEY ({}) REFERENCES {}({})",
                    cols(&c.cols, tb),
                    super::funcs::quote_ident(&parent.name),
                    cols(ref_cols, parent)
                );
                if *on_update != super::catalog::FkAction::NoAction {
                    s.push_str(&format!(" ON UPDATE {}", on_update.sql()));
                }
                if *on_delete != super::catalog::FkAction::NoAction {
                    s.push_str(&format!(" ON DELETE {}", on_delete.sql()));
                }
                s
            }
        });
    }
    None
}

/// `pg_get_indexdef`.
pub fn index_def(db: &DbState, oid: u32) -> Option<String> {
    for tb in db.tables.values() {
        let Some(i) = tb.indexes.iter().find(|i| i.oid == oid) else { continue };
        let mut keys = vec![];
        let mut exprs = i.exprs.iter();
        for (k, c) in i.cols.iter().enumerate() {
            let mut s = match c {
                Some(idx) => super::funcs::quote_ident(&tb.columns[*idx].name),
                None => format!("({})", exprs.next().cloned().unwrap_or_default()),
            };
            if i.desc.get(k).copied().unwrap_or(false) {
                s.push_str(" DESC");
            }
            keys.push(s);
        }
        let mut s = format!(
            "CREATE {}INDEX {} ON {}.{} USING {} ({})",
            if i.unique { "UNIQUE " } else { "" },
            super::funcs::quote_ident(&i.name),
            super::funcs::quote_ident(db.schema_name(tb.schema)),
            super::funcs::quote_ident(&tb.name),
            i.method,
            keys.join(", ")
        );
        if let Some(p) = &i.predicate {
            s.push_str(&format!(" WHERE {p}"));
        }
        return Some(s);
    }
    None
}
