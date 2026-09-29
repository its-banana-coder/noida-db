//! Bitmaps: SETBIT/GETBIT, BITCOUNT, BITPOS, BITOP and BITFIELD.

use super::*;

#[test]
fn setbit_getbit_and_growth() {
    let mut t = T::new();
    assert_eq!(t.run("SETBIT b 7 1"), int(0));
    assert_eq!(t.run("GET b"), bulk("\u{1}"));
    assert_eq!(t.run("SETBIT b 7 1"), int(1));
    assert_eq!(t.run("SETBIT b 100 1"), int(0));
    assert_eq!(t.run("STRLEN b"), int(13));
    assert_eq!(t.run("GETBIT b 100"), int(1));
    assert_eq!(t.run("GETBIT b 999"), int(0));
    assert_eq!(t.run("GETBIT nokey 1"), int(0));
    assert_eq!(t.run("SETBIT b 0 2"), err("ERR bit is not an integer or out of range"));
    assert_eq!(t.run("SETBIT b -1 1"), err("ERR bit offset is not an integer or out of range"));
    assert_eq!(
        t.run("SETBIT b 4294967296 1"),
        err("ERR bit offset is not an integer or out of range")
    );
    t.run("LPUSH l x");
    assert_eq!(t.run("SETBIT l 0 1"), err(WRONGTYPE));
    assert_eq!(t.run("GETBIT l 0"), err(WRONGTYPE));
}

#[test]
fn bitcount_and_bitpos() {
    let mut t = T::new();
    t.run("SET s foobar");
    assert_eq!(t.run("BITCOUNT s"), int(26));
    assert_eq!(t.run("BITCOUNT s 0 0"), int(4));
    assert_eq!(t.run("BITCOUNT s 1 1"), int(6));
    assert_eq!(t.run("BITCOUNT s 5 30 BIT"), int(17));
    assert_eq!(t.run("BITCOUNT s 2 1"), int(0));
    assert_eq!(t.run("BITCOUNT nokey"), int(0));
    assert_eq!(t.run("BITCOUNT s 0 0 FOO"), err(SYNTAX));
    assert_eq!(t.run("BITPOS s 1"), int(1));
    assert_eq!(t.run("BITPOS s 0"), int(0));
    assert_eq!(t.run("BITPOS s 1 2"), int(17));
    assert_eq!(t.run("BITPOS s 1 2 -1 BIT"), int(2));
    assert_eq!(t.run("BITPOS nokey 1"), int(-1));
    assert_eq!(t.run("BITPOS nokey 0"), int(0));
    assert_eq!(t.run("BITPOS s 2"), err("ERR The bit argument must be 1 or 0."));
    // All-ones: the zero is "past the end" only when no end was given.
    t.run("SET o \u{7f}");
    assert_eq!(t.run("BITPOS o 1"), int(1));
    t.run("SETBIT o 0 1");
    assert_eq!(t.run("BITPOS o 0"), int(8));
    assert_eq!(t.run("BITPOS o 0 0 -1"), int(-1));
}

#[test]
fn bitop_combines_strings() {
    let mut t = T::new();
    t.run("SET a abc");
    t.run("SET b abdff");
    assert_eq!(t.run("BITOP AND dst a b"), int(5));
    assert_eq!(t.run("GET dst"), bulk("ab`\0\0"));
    assert_eq!(t.run("BITOP OR dst a b"), int(5));
    assert_eq!(t.run("GET dst"), bulk("abgff"));
    assert_eq!(t.run("BITOP XOR dst a b"), int(5));
    assert_eq!(t.run("BITOP NOT dst a"), int(3));
    assert_eq!(
        t.run("BITOP NOT dst a b"),
        err("ERR BITOP NOT must be called with a single source key.")
    );
    assert_eq!(t.run("BITOP AND dst nokey nokey2"), int(0));
    assert_eq!(t.run("EXISTS dst"), int(0));
    assert_eq!(t.run("BITOP FOO dst a"), err(SYNTAX));
    t.run("LPUSH l x");
    assert_eq!(t.run("BITOP AND dst a l"), err(WRONGTYPE));
}

#[test]
fn bitfield_types_and_overflow() {
    let mut t = T::new();
    assert_eq!(t.run("BITFIELD bf SET u8 0 255 GET u8 0"), arr(vec![int(0), int(255)]));
    assert_eq!(t.run("BITFIELD bf OVERFLOW SAT INCRBY u8 0 10"), arr(vec![int(255)]));
    assert_eq!(t.run("BITFIELD bf OVERFLOW FAIL INCRBY u8 0 10"), arr(vec![nil()]));
    assert_eq!(t.run("BITFIELD bf OVERFLOW WRAP INCRBY u8 0 1"), arr(vec![int(0)]));
    assert_eq!(t.run("BITFIELD bf SET i8 0 -128 GET i8 0"), arr(vec![int(0), int(-128)]));
    assert_eq!(t.run("BITFIELD bf OVERFLOW SAT INCRBY i8 0 -1"), arr(vec![int(-128)]));
    assert_eq!(t.run("BITFIELD bf OVERFLOW WRAP INCRBY i8 0 -1"), arr(vec![int(127)]));
    // #<n> counts in field-sized steps.
    assert_eq!(t.run("BITFIELD bf SET u8 #1 7 GET u8 8"), arr(vec![int(0), int(7)]));
    assert_eq!(
        t.run("BITFIELD bf GET u64 0"),
        err(
            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
        )
    );
    assert_eq!(
        t.run("BITFIELD bf OVERFLOW BAD INCRBY u8 0 1"),
        err("ERR Invalid OVERFLOW type specified")
    );
    assert_eq!(t.run("BITFIELD bf FOO u8 0"), err(SYNTAX));
    assert_eq!(t.run("BITFIELD nokey GET u8 0"), arr(vec![int(0)]));
    assert_eq!(t.run("EXISTS nokey"), int(0));
    assert_eq!(
        t.run("BITFIELD_RO bf SET u8 0 1"),
        err("ERR BITFIELD_RO only supports the GET subcommand")
    );
    assert_eq!(t.run("BITFIELD_RO bf GET i8 0"), arr(vec![int(127)]));
}
