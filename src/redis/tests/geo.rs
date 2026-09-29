//! Geo commands. The scores are Redis's 52-bit geohashes, so the values
//! here are the ones a real Redis stores and reports.

use super::*;

#[test]
fn geoadd_stores_geohash_scores() {
    let mut t = T::new();
    assert_eq!(
        t.run("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania"),
        int(2)
    );
    assert_eq!(t.run("ZSCORE Sicily Palermo"), Value::Double(3479099956230698.0));
    assert_eq!(t.run("ZSCORE Sicily Catania"), Value::Double(3479447370796909.0));
    assert_eq!(t.run("TYPE Sicily"), simple("zset"));
    assert_eq!(t.run("GEOADD Sicily XX 1 2 nosuchmember"), int(0));
    assert_eq!(t.run("GEOADD Sicily NX XX 1 2 m"), err(SYNTAX));
    assert_eq!(t.run("GEOADD Sicily 1 2 m extra"), err(SYNTAX));
    assert_eq!(
        t.run("GEOADD Sicily 181 38 bad"),
        err("ERR invalid longitude,latitude pair 181.000000,38.000000")
    );
    t.run("SET str v");
    assert_eq!(t.run("GEOADD str 1 2 m"), err(WRONGTYPE));
}

#[test]
fn positions_distances_and_hashes() {
    let mut t = T::new();
    t.run("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania");
    assert_eq!(
        t.run("GEOPOS Sicily Palermo nosuch"),
        arr(vec![
            arr(vec![bulk("13.36138933897018433"), bulk("38.11555639549629859")]),
            Value::NullArray
        ])
    );
    assert_eq!(t.run("GEODIST Sicily Palermo Catania"), bulk("166274.1516"));
    assert_eq!(t.run("GEODIST Sicily Palermo Catania km"), bulk("166.2742"));
    assert_eq!(t.run("GEODIST Sicily Palermo Catania mi"), bulk("103.3182"));
    assert_eq!(t.run("GEODIST Sicily Palermo nosuch"), nil());
    assert_eq!(
        t.run("GEODIST Sicily Palermo Catania yards"),
        err("ERR unsupported unit provided. please use M, KM, FT, MI")
    );
    assert_eq!(
        t.run("GEOHASH Sicily Palermo Catania nosuch"),
        arr(vec![bulk("sqc8b49rny0"), bulk("sqdtr74hyu0"), nil()])
    );
}

#[test]
fn searching_by_radius_and_box() {
    let mut t = T::new();
    t.run(
        "GEOADD k 13.361389 38.115556 Palermo 15.087269 37.502669 Catania 12.758489 38.788135 edge",
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 200 km ASC"),
        bulks(&["Catania", "Palermo"])
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 200 km DESC"),
        bulks(&["Palermo", "Catania"])
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1"),
        bulks(&["Catania"])
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHDIST"),
        arr(vec![
            arr(vec![bulk("Catania"), bulk("56.4413")]),
            arr(vec![bulk("Palermo"), bulk("190.4424")]),
        ])
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMMEMBER Palermo BYBOX 400 400 km ASC"),
        bulks(&["Palermo", "edge", "Catania"])
    );
    assert_eq!(t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 1 m"), arr(vec![]));
    assert_eq!(t.run("GEOSEARCH nokey FROMLONLAT 15 37 BYRADIUS 200 km"), arr(vec![]));
    assert_eq!(
        t.run("GEOSEARCH k FROMMEMBER nosuch BYRADIUS 200 km"),
        err("ERR could not decode requested zset member")
    );
    assert_eq!(
        t.run("GEOSEARCH k BYRADIUS 200 km COUNT 1"),
        err("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch")
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 COUNT 1 ASC"),
        err("ERR exactly one of BYRADIUS and BYBOX can be specified for geosearch")
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS -1 km"),
        err("ERR radius cannot be negative")
    );
    assert_eq!(
        t.run("GEOSEARCH k FROMLONLAT 15 37 BYRADIUS 200 km ANY"),
        err("ERR the ANY argument requires COUNT argument")
    );
}

#[test]
fn searches_that_store() {
    let mut t = T::new();
    t.run("GEOADD k 13.361389 38.115556 Palermo 15.087269 37.502669 Catania");
    assert_eq!(t.run("GEOSEARCHSTORE dst k FROMLONLAT 15 37 BYRADIUS 200 km ASC"), int(2));
    assert_eq!(t.run("ZSCORE dst Catania"), Value::Double(3479447370796909.0));
    assert_eq!(t.run("GEOSEARCHSTORE dst k FROMLONLAT 15 37 BYRADIUS 200 km STOREDIST"), int(2));
    // STOREDIST keeps the distance in the unit the search used (km here).
    let Value::Double(d) = t.run("ZSCORE dst Catania") else { panic!() };
    assert!((d - 56.4413).abs() < 0.001, "{d}");
    assert_eq!(t.run("GEOSEARCHSTORE dst k FROMLONLAT 15 37 BYRADIUS 1 m"), int(0));
    assert_eq!(t.run("EXISTS dst"), int(0));
    assert_eq!(t.run("GEORADIUS k 15 37 200 km STORE out"), int(2));
    assert_eq!(t.run("ZCARD out"), int(2));
    assert_eq!(t.run("GEORADIUS_RO k 15 37 200 km STORE out"), err(SYNTAX));
    assert_eq!(
        t.run("GEOSEARCHSTORE dst k FROMLONLAT 15 37 BYRADIUS 200 km WITHDIST"),
        err("ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
    );
}
