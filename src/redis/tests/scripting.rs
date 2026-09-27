//! EVAL and friends. Replies and error strings are Redis 7.2's.

use super::*;

#[test]
fn eval_converts_lua_values_to_replies() {
    let mut t = T::new();
    assert_eq!(t.run("EVAL \"return 1\" 0"), int(1));
    assert_eq!(t.run("EVAL \"return 'hello'\" 0"), bulk("hello"));
    assert_eq!(t.run("EVAL \"return true\" 0"), int(1));
    assert_eq!(t.run("EVAL \"return false\" 0"), nil());
    assert_eq!(t.run("EVAL \"return 3.9\" 0"), int(3));
    assert_eq!(t.run("EVAL \"return -3.9\" 0"), int(-3));
    assert_eq!(
        t.run("EVAL \"return {1,2,'three',nil,5}\" 0"),
        arr(vec![int(1), int(2), bulk("three")])
    );
    assert_eq!(t.run("EVAL \"return {ok='fine'}\" 0"), simple("fine"));
    assert_eq!(t.run("EVAL \"return {err='My Error'}\" 0"), err("My Error"));
    assert_eq!(t.run("EVAL \"return redis.error_reply('bad')\" 0"), err("ERR bad"));
    assert_eq!(t.run("EVAL \"return redis.error_reply('WRONGTYPE bad')\" 0"), err("WRONGTYPE bad"));
    assert_eq!(t.run("EVAL \"return redis.status_reply('YES')\" 0"), simple("YES"));
    assert_eq!(
        t.run("EVAL \"return redis.sha1hex('')\" 0"),
        bulk("da39a3ee5e6b4b0d3255bfef95601890afd80709")
    );
    assert_eq!(
        t.run("EVAL \"return {1,2,{3,'hello'}}\" 0"),
        arr(vec![int(1), int(2), arr(vec![int(3), bulk("hello")])])
    );
}

#[test]
fn keys_and_argv() {
    let mut t = T::new();
    assert_eq!(t.run("EVAL \"return #KEYS\" 2 a b"), int(2));
    assert_eq!(t.run("EVAL \"return {KEYS[1],KEYS[2],ARGV[1]}\" 2 a b c"), bulks(&["a", "b", "c"]));
    assert_eq!(t.run("EVAL \"return ARGV[1] == nil\" 0"), int(1));
    assert_eq!(t.run("EVAL \"return 1\" -1"), err("ERR Number of keys can't be negative"));
    assert_eq!(
        t.run("EVAL \"return 1\" 2 a"),
        err("ERR Number of keys can't be greater than number of args")
    );
    assert_eq!(t.run("EVAL \"return 1\" abc"), err(NOT_INT));
}

#[test]
fn redis_call_runs_commands() {
    let mut t = T::new();
    t.run("SET s v");
    assert_eq!(t.run("EVAL \"return redis.call('get','s')\" 0"), bulk("v"));
    assert_eq!(t.run("EVAL \"return redis.call('get','nope')\" 0"), nil());
    assert_eq!(t.run("EVAL \"return redis.call('set',KEYS[1],ARGV[1])\" 1 k v"), ok());
    assert_eq!(t.run("GET k"), bulk("v"));
    assert_eq!(
        t.run("EVAL \"redis.call('rpush','l','a','b') return redis.call('lrange','l',0,-1)\" 0"),
        bulks(&["a", "b"])
    );
    assert_eq!(t.run("EVAL \"return redis.call('echo', 3)\" 0"), bulk("3"));
    assert_eq!(t.run("EVAL \"return redis.call('echo', 3.7)\" 0"), bulk("3.7"));
    // A blocking command doesn't block inside a script.
    assert_eq!(t.run("EVAL \"return redis.call('blpop','nolist',0)\" 0"), nil());
}

#[test]
fn script_errors_carry_the_script_and_line() {
    let mut t = T::new();
    t.run("SET s v");
    let sha = "c243b9f46db13ba8f69da41b45a77708832babed";
    assert_eq!(
        t.run("EVAL \"return redis.call('incr','s')\" 0"),
        err(&format!(
            "ERR value is not an integer or out of range script: {sha}, on @user_script:1."
        ))
    );
    assert_eq!(
        t.run("EVAL \"return redis.pcall('incr','s')\" 0"),
        err("ERR value is not an integer or out of range")
    );
    assert_eq!(
        t.run("EVAL \"local o = redis.pcall('incr','s') return o.err\" 0"),
        bulk("ERR value is not an integer or out of range")
    );
    let Value::Error(e) = t.run("EVAL \"return redis.call('nosuchcmd')\" 0") else { panic!() };
    assert!(e.starts_with("ERR Unknown Redis command called from script script: "), "{e}");
    assert!(e.ends_with(", on @user_script:1."), "{e}");
    let Value::Error(e) = t.run("EVAL \"return redis.call('set','x')\" 0") else { panic!() };
    assert!(e.starts_with("ERR Wrong number of args calling Redis command from script "), "{e}");
    let Value::Error(e) = t.run("EVAL \"return redis.call('subscribe','x')\" 0") else { panic!() };
    assert!(e.starts_with("ERR This Redis command is not allowed from script "), "{e}");
    let Value::Error(e) = t.run("EVAL \"return redis.call('echo', {})\" 0") else { panic!() };
    assert!(
        e.starts_with("ERR Lua redis lib command arguments must be strings or integers "),
        "{e}"
    );
    assert_eq!(
        t.run("EVAL \"return redis.pcall('nosuchcmd')\" 0"),
        err("ERR Unknown Redis command called from script")
    );
    assert_eq!(
        t.run("EVAL \"return redis.pcall()\" 0"),
        err("ERR Please specify at least one argument for this redis lib call")
    );
}

#[test]
fn the_sandbox_matches_redis() {
    let mut t = T::new();
    let Value::Error(e) = t.run("EVAL \"return nosuchglobal\" 0") else { panic!() };
    assert!(
        e.starts_with(
            "ERR user_script:1: Script attempted to access nonexistent global variable \
             'nosuchglobal' script: "
        ),
        "{e}"
    );
    let Value::Error(e) = t.run("EVAL \"x = 5\" 0") else { panic!() };
    assert!(e.starts_with("ERR user_script:1: Attempt to modify a readonly table script: "), "{e}");
    let Value::Error(e) = t.run("EVAL \"return type(print)\" 0") else { panic!() };
    assert!(e.contains("nonexistent global variable 'print'"), "{e}");
    assert_eq!(
        t.run("EVAL \"this is not lua\" 0"),
        err("ERR Error compiling script (new function): user_script:1: '=' expected near 'is'")
    );
    // The libraries scripts rely on are there.
    assert_eq!(t.run("EVAL \"return type(cjson)\" 0"), bulk("table"));
    assert_eq!(t.run("EVAL \"return type(string.format)\" 0"), bulk("function"));
    assert_eq!(t.run("EVAL \"return type(table.remove)\" 0"), bulk("function"));
    assert_eq!(t.run("EVAL \"return type(unpack)\" 0"), bulk("function"));
    assert_eq!(t.run("EVAL \"return type(pcall)\" 0"), bulk("function"));
    assert_eq!(t.run("EVAL \"return redis.REDIS_VERSION\" 0"), bulk(crate::redis::REDIS_VERSION));
    assert_eq!(t.run("EVAL \"return redis.replicate_commands()\" 0"), int(1));
}

#[test]
fn cjson_encodes_and_decodes() {
    let mut t = T::new();
    assert_eq!(t.run("EVAL \"return cjson.encode({1,2,'x'})\" 0"), bulk("[1,2,\"x\"]"));
    assert_eq!(t.run("EVAL \"return cjson.encode({})\" 0"), bulk("{}"));
    assert_eq!(t.run("EVAL \"return cjson.encode(1.5)\" 0"), bulk("1.5"));
    assert_eq!(t.run("EVAL \"return cjson.encode('a')\" 0"), bulk("\"a\""));
    assert_eq!(t.run("EVAL \"return cjson.decode('[1,\\\"a\\\",true,null]')[2]\" 0"), bulk("a"));
    assert_eq!(t.run("EVAL \"return cjson.decode('3')\" 0"), int(3));
    assert_eq!(t.run("EVAL \"return cjson.decode('{\\\"a\\\":7}').a\" 0"), int(7));
    assert_eq!(t.run("EVAL \"return type(cjson.decode('{}'))\" 0"), bulk("table"));
}

#[test]
fn eval_ro_refuses_writes() {
    let mut t = T::new();
    t.run("SET s v");
    assert_eq!(t.run("EVAL_RO \"return redis.call('get','s')\" 1 s"), bulk("v"));
    let Value::Error(e) = t.run("EVAL_RO \"return redis.call('set','a','b')\" 1 a") else {
        panic!()
    };
    assert!(
        e.starts_with("ERR Write commands are not allowed from read-only scripts. script: "),
        "{e}"
    );
}

#[test]
fn script_cache() {
    let mut t = T::new();
    assert_eq!(
        t.run("EVALSHA ffffffffffffffffffffffffffffffffffffffff 0"),
        err("NOSCRIPT No matching script. Please use EVAL.")
    );
    let sha = "e0e1f9fabfc9d4800c877a703b823ac0578ff8db";
    assert_eq!(t.run("SCRIPT LOAD \"return 1\""), bulk(sha));
    assert_eq!(t.run(&format!("EVALSHA {sha} 0")), int(1));
    assert_eq!(t.run(&format!("EVALSHA {} 0", sha.to_uppercase())), int(1));
    assert_eq!(
        t.run(&format!("SCRIPT EXISTS {sha} ffffffffffffffffffffffffffffffffffffffff")),
        arr(vec![int(1), int(0)])
    );
    // EVAL caches the script too.
    t.run("EVAL \"return 2\" 0");
    assert_eq!(t.run("SCRIPT EXISTS 7f923f79fe76194c868d7e1d0820de36700eb649"), arr(vec![int(1)]));
    assert_eq!(t.run("SCRIPT FLUSH"), ok());
    assert_eq!(t.run(&format!("SCRIPT EXISTS {sha}")), arr(vec![int(0)]));
    assert_eq!(t.run("SCRIPT FLUSH ASYNC"), ok());
    assert_eq!(t.run("SCRIPT FLUSH bogus"), err("ERR SCRIPT FLUSH only support SYNC|ASYNC option"));
    assert_eq!(t.run("SCRIPT KILL"), err("NOTBUSY No scripts in execution right now."));
    assert_eq!(
        t.run("SCRIPT LOAD \"this is not lua\""),
        err("ERR Error compiling script (new function): user_script:1: '=' expected near 'is'")
    );
}

#[test]
fn an_undefined_global_reports_the_scripts_own_line() {
    let mut t = T::new();
    let Value::Error(e) = t.run("EVAL \"return nosuchglobal\" 0") else { panic!() };
    assert!(
        e.starts_with("ERR user_script:1: Script attempted to access nonexistent global variable 'nosuchglobal' script: "),
        "{e}"
    );
    assert!(e.ends_with(", on @user_script:1."), "{e}");
}

#[test]
fn cmsgpack_packs_and_unpacks() {
    let mut t = T::new();
    assert_eq!(
        t.run("EVAL \"return cmsgpack.pack(1,-1,200)\" 0"),
        bulk_bytes(&[0x01, 0xff, 0xcc, 0xc8])
    );
    assert_eq!(
        t.run("EVAL \"return cmsgpack.pack('a',true,nil)\" 0"),
        bulk_bytes(&[0xa1, b'a', 0xc3, 0xc0])
    );
    assert_eq!(t.run("EVAL \"return cmsgpack.pack({1,2})\" 0"), bulk_bytes(&[0x92, 1, 2]));
    assert_eq!(t.run("EVAL \"return cmsgpack.pack({a=1})\" 0"), bulk_bytes(&[0x81, 0xa1, b'a', 1]));
    assert_eq!(
        t.run("EVAL \"return cmsgpack.pack(1.5)\" 0"),
        bulk_bytes(&[0xca, 0x3f, 0xc0, 0, 0])
    );
    assert_eq!(
        t.run("EVAL \"return {cmsgpack.unpack(cmsgpack.pack(1,'a',{2}))}\" 0"),
        arr(vec![int(1), bulk("a"), arr(vec![int(2)])])
    );
    assert_eq!(
        t.run("EVAL \"return {cmsgpack.unpack_one(cmsgpack.pack(7,8,9))}\" 0"),
        arr(vec![int(1), int(7)]),
        "unpack_one returns the next offset first"
    );
    assert_eq!(
        t.run("EVAL \"return {cmsgpack.unpack_one(cmsgpack.pack(7,8,9), 2)}\" 0"),
        arr(vec![int(-1), int(9)]),
        "and -1 once the input is used up"
    );
}

#[test]
fn cmsgpack_reports_bad_input() {
    let mut t = T::new();
    let msg = |t: &mut T, code: &str| match t.run(&format!("EVAL \"{code}\" 0")) {
        Value::Error(e) => e,
        other => panic!("expected an error, got {other:?}"),
    };
    assert!(
        msg(&mut t, "return cmsgpack.unpack(string.char(145))").contains("Missing bytes in input.")
    );
    assert!(
        msg(&mut t, "return cmsgpack.unpack(string.char(193))")
            .contains("Bad data format in input.")
    );
    assert!(msg(&mut t, "return cmsgpack.pack()").contains("MessagePack pack needs input."));
}

#[test]
fn lua_numbers_reach_commands_at_full_precision() {
    // Millisecond timestamps must not round to Lua's 14 significant digits
    // (BullMQ's delayed jobs depend on it).
    let mut t = T::new();
    t.run("EVAL \"return redis.call('set','k',1790482289286.5)\" 0");
    assert_eq!(t.run("GET k"), bulk("1790482289286.5"));
    t.run("EVAL \"return redis.call('set','k',7333814913605631)\" 0");
    assert_eq!(t.run("GET k"), bulk("7333814913605631"));
    t.run("EVAL \"return redis.call('set','k',0.1)\" 0");
    assert_eq!(t.run("GET k"), bulk("0.1"));
    t.run("EVAL \"return redis.call('set','k',1e19)\" 0");
    assert_eq!(t.run("GET k"), bulk("1e+19"));
}

#[test]
fn a_resp3_client_still_gets_resp2_replies_inside_scripts() {
    let mut t = T::new();
    t.run("ZADD z 1 a");
    let mut c = t.connect();
    t.run_as(&mut c, "HELLO 3");
    // The script sees the flat RESP2 shape, as in Redis, unless it asks for RESP3.
    assert_eq!(
        t.run_as(&mut c, "EVAL \"return redis.call('zrange','z',0,-1,'withscores')\" 0"),
        arr(vec![bulk("a"), bulk("1")])
    );
    assert_eq!(
        t.run_as(
            &mut c,
            "EVAL \"redis.setresp(3); return #redis.call('zrange','z',0,-1,'withscores')\" 0"
        ),
        int(1)
    );
    // ... and the client itself is still on RESP3 afterwards.
    assert!(matches!(t.run_as(&mut c, "ZRANGE z 0 -1 WITHSCORES"), Value::Array(_)));
    assert_eq!(c.resp, 3);
}
