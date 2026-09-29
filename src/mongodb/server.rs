use bson::{Document, doc};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use super::engine::Engine;
use super::wire::{MsgHeader, OP_MSG, OP_QUERY, OP_REPLY};

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    let engine = Arc::new(Mutex::new(Engine::new()));

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let _ = handle_client(stream, engine);
            });
        }
    });

    Ok(local_addr)
}

fn handle_client(mut stream: TcpStream, engine: Arc<Mutex<Engine>>) -> io::Result<()> {
    loop {
        let header = match MsgHeader::read_from(&mut stream) {
            Ok(h) => h,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut body = vec![0u8; (header.message_length - 16) as usize];
        stream.read_exact(&mut body)?;

        match header.op_code {
            OP_QUERY => {
                // Read flags (4), collection name (cstring), skip/limit (4+4)
                let mut offset = 4;
                while offset < body.len() && body[offset] != 0 {
                    offset += 1;
                }
                offset += 1 + 4 + 4; // null byte + skip + limit

                let query_doc = if offset < body.len() {
                    let doc_slice = &body[offset..];
                    Document::from_reader(doc_slice).unwrap_or_else(|_| doc! {})
                } else {
                    doc! {}
                };

                let is_hello = query_doc.contains_key("hello")
                    || query_doc.contains_key("ismaster")
                    || query_doc.contains_key("isMaster");
                let reply_doc = if is_hello {
                    hello_reply()
                } else {
                    doc! {
                        "ok": 0.0,
                        "errmsg": "OP_QUERY is no longer supported. The client should update its driver and use OP_MSG.",
                        "code": 59,
                        "codeName": "CommandNotFound"
                    }
                };

                let mut doc_buf = Vec::new();
                reply_doc.to_writer(&mut doc_buf).unwrap();

                let reply_header = MsgHeader {
                    message_length: 16 + 4 + 8 + 4 + 4 + doc_buf.len() as i32,
                    request_id: 1, // generated
                    response_to: header.request_id,
                    op_code: OP_REPLY,
                };

                reply_header.write_to(&mut stream)?;
                stream.write_all(&0i32.to_le_bytes())?; // flags
                stream.write_all(&0i64.to_le_bytes())?; // cursor id
                stream.write_all(&0i32.to_le_bytes())?; // starting from
                stream.write_all(&1i32.to_le_bytes())?; // number returned
                stream.write_all(&doc_buf)?;
            }
            OP_MSG => {
                let mut offset = 0;
                let _flag_bits = u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap());
                offset += 4;

                if offset < body.len() && body[offset] == 0 {
                    offset += 1; // kind 0
                    let doc_slice = &body[offset..];
                    let cmd_doc = Document::from_reader(doc_slice).unwrap_or_else(|_| doc! {});

                    let is_hello = cmd_doc.contains_key("hello")
                        || cmd_doc.contains_key("ismaster")
                        || cmd_doc.contains_key("isMaster");
                    let reply_doc = if is_hello {
                        hello_reply()
                    } else if cmd_doc.contains_key("buildInfo") || cmd_doc.contains_key("buildinfo")
                    {
                        buildinfo_reply()
                    } else if cmd_doc.contains_key("ping") {
                        doc! { "ok": 1.0 }
                    } else if cmd_doc.contains_key("insert") {
                        let coll = cmd_doc.get_str("insert").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let docs = cmd_doc
                            .get_array("documents")
                            .map(|a| a.iter().map(|d| d.as_document().unwrap().clone()).collect())
                            .unwrap_or_default();
                        let mut eng = engine.lock().unwrap();
                        match eng.insert(db, coll, docs) {
                            Ok(n) => doc! { "n": n, "ok": 1.0 },
                            Err(e) => {
                                doc! { "ok": 1.0, "n": 0, "writeErrors": [{"index": 0, "code": 11000, "errmsg": e}] }
                            }
                        }
                    } else if cmd_doc.contains_key("createIndexes") {
                        let coll = cmd_doc.get_str("createIndexes").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let indexes = cmd_doc.get_array("indexes").unwrap();
                        let mut eng = engine.lock().unwrap();
                        let mut err = None;
                        for idx in indexes {
                            let idx_doc = idx.as_document().unwrap();
                            let name = idx_doc.get_str("name").unwrap();
                            let keys = idx_doc.get_document("key").unwrap().clone();
                            let unique = idx_doc.get_bool("unique").unwrap_or(false);
                            if let Err(e) = eng.create_index(db, coll, name, keys, unique) {
                                err = Some(e);
                                break;
                            }
                        }
                        if let Some(e) = err {
                            doc! { "ok": 0.0, "code": 11000, "codeName": "DuplicateKey", "errmsg": e }
                        } else {
                            doc! { "ok": 1.0 }
                        }
                    } else if cmd_doc.contains_key("find") {
                        let coll = cmd_doc.get_str("find").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let filter = cmd_doc.get_document("filter").unwrap_or(&doc! {}).clone();
                        let eng = engine.lock().unwrap();
                        let docs = eng.find(db, coll, &filter);
                        let first_batch =
                            bson::Bson::Array(docs.into_iter().map(bson::Bson::Document).collect());
                        doc! {
                            "cursor": {
                                "id": 0i64,
                                "ns": format!("{}.{}", db, coll),
                                "firstBatch": first_batch
                            },
                            "ok": 1.0
                        }
                    } else if cmd_doc.contains_key("update") {
                        let coll = cmd_doc.get_str("update").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let updates = cmd_doc.get_array("updates").unwrap();
                        let mut n_matched = 0;
                        let mut n_modified = 0;
                        let mut eng = engine.lock().unwrap();
                        for u in updates {
                            let u_doc = u.as_document().unwrap();
                            let q = u_doc.get_document("q").unwrap();
                            let u_obj = u_doc.get_document("u").unwrap();
                            let multi = u_doc.get_bool("multi").unwrap_or(false);
                            let upsert = u_doc.get_bool("upsert").unwrap_or(false);
                            let (m, modif) = eng.update(db, coll, q, u_obj, multi, upsert);
                            n_matched += m;
                            n_modified += modif;
                        }
                        doc! { "n": n_matched, "nModified": n_modified, "ok": 1.0 }
                    } else if cmd_doc.contains_key("delete") {
                        let coll = cmd_doc.get_str("delete").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let deletes = cmd_doc.get_array("deletes").unwrap();
                        let mut eng = engine.lock().unwrap();
                        let mut n = 0;
                        for d in deletes {
                            let d_doc = d.as_document().unwrap();
                            let q = d_doc.get_document("q").unwrap();
                            let limit = d_doc.get_i32("limit").unwrap_or(0);
                            n += eng.delete(db, coll, q, limit);
                        }
                        doc! { "n": n, "ok": 1.0 }
                    } else if cmd_doc.contains_key("aggregate") {
                        let coll = cmd_doc.get_str("aggregate").unwrap();
                        let db = cmd_doc.get_str("$db").unwrap_or("test");
                        let pipeline = cmd_doc.get_array("pipeline").unwrap_or(&Vec::new()).clone();
                        let eng = engine.lock().unwrap();
                        match eng.aggregate(db, coll, &pipeline) {
                            Ok(docs) => {
                                let first_batch = bson::Bson::Array(
                                    docs.into_iter().map(bson::Bson::Document).collect(),
                                );
                                doc! {
                                    "cursor": {
                                        "id": 0i64,
                                        "ns": format!("{}.{}", db, coll),
                                        "firstBatch": first_batch
                                    },
                                    "ok": 1.0
                                }
                            }
                            Err(e) => {
                                doc! {
                                    "ok": 0.0,
                                    "errmsg": e,
                                    "code": 14,
                                    "codeName": "TypeMismatch"
                                }
                            }
                        }
                    } else {
                        let cmd_name =
                            cmd_doc.keys().next().cloned().unwrap_or_else(|| "".to_string());
                        doc! {
                            "ok": 0.0,
                            "errmsg": format!("no such command: '{}'", cmd_name),
                            "code": 59,
                            "codeName": "CommandNotFound"
                        }
                    };

                    let mut doc_buf = Vec::new();
                    reply_doc.to_writer(&mut doc_buf).unwrap();

                    let reply_header = MsgHeader {
                        message_length: 16 + 4 + 1 + doc_buf.len() as i32,
                        request_id: 2,
                        response_to: header.request_id,
                        op_code: OP_MSG,
                    };

                    reply_header.write_to(&mut stream)?;
                    stream.write_all(&0u32.to_le_bytes())?; // flagBits
                    stream.write_all(&[0])?; // kind 0
                    stream.write_all(&doc_buf)?;
                }
            }
            _ => {
                // Drop connection on unknown opcode
                return Ok(());
            }
        }
    }
}

fn hello_reply() -> Document {
    doc! {
        "isWritablePrimary": true,
        "ismaster": true,
        "topologyVersion": {
            "processId": bson::oid::ObjectId::new(),
            "counter": 0i64
        },
        "maxBsonObjectSize": 16777216i32,
        "maxMessageSizeBytes": 48000000i32,
        "maxWriteBatchSize": 100000i32,
        "localTime": bson::DateTime::now(),
        "logicalSessionTimeoutMinutes": 30i32,
        "connectionId": 1i32,
        "minWireVersion": 0i32,
        "maxWireVersion": 21i32,
        "readOnly": false,
        "setName": "rs0",
        "hosts": vec!["127.0.0.1:27017"],
        "me": "127.0.0.1:27017",
        "primary": "127.0.0.1:27017",
        "setVersion": 1i32,
        "electionId": bson::oid::ObjectId::new(),
        "lastWrite": {
            "opTime": {
                "ts": bson::Timestamp { time: 0, increment: 0 },
                "t": 0i64
            },
            "lastWriteDate": bson::DateTime::now(),
            "majorityOpTime": {
                "ts": bson::Timestamp { time: 0, increment: 0 },
                "t": 0i64
            },
            "majorityWriteDate": bson::DateTime::now()
        },
        "ok": 1.0
    }
}

fn buildinfo_reply() -> Document {
    doc! {
        "version": "7.0.14",
        "versionArray": vec![7i32, 0, 14, 0],
        "gitVersion": "1234567890abcdef",
        "modules": vec![] as Vec<String>,
        "allocator": "tcmalloc",
        "javascriptEngine": "mozjs",
        "sysInfo": "deprecated",
        "versionEnvironment": {
            "os": "linux"
        },
        "bits": 64i32,
        "debug": false,
        "maxBsonObjectSize": 16777216i32,
        "storageEngines": vec!["wiredTiger"],
        "ok": 1.0
    }
}
