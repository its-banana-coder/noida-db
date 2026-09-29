#[cfg(feature = "mongodb")]
mod diff {
    use bson::{Document, doc};
    use noida::mongodb::server::spawn;
    use noida::mongodb::wire::{MsgHeader, OP_MSG};
    use std::env;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn send_op_msg(stream: &mut TcpStream, req_id: i32, cmd: &Document) -> Document {
        let mut doc_buf = Vec::new();
        cmd.to_writer(&mut doc_buf).unwrap();

        let header = MsgHeader {
            message_length: 16 + 4 + 1 + doc_buf.len() as i32,
            request_id: req_id,
            response_to: 0,
            op_code: OP_MSG,
        };

        header.write_to(&mut *stream).unwrap();
        stream.write_all(&0u32.to_le_bytes()).unwrap(); // flagBits
        stream.write_all(&[0]).unwrap(); // kind 0
        stream.write_all(&doc_buf).unwrap();

        let header = MsgHeader::read_from(&mut *stream).unwrap();
        let mut body = vec![0u8; (header.message_length - 16) as usize];
        stream.read_exact(&mut body).unwrap();

        assert_eq!(header.op_code, OP_MSG);
        let mut offset = 4; // flagBits
        assert_eq!(body[offset], 0); // kind 0
        offset += 1;

        Document::from_reader(&body[offset..]).unwrap()
    }

    #[test]
    fn test_diff() {
        let ref_addr = env::var("NOIDA_MONGODB_REF");
        if ref_addr.is_err() {
            println!("SKIPPED");
            return;
        }
        let ref_addr = ref_addr.unwrap();

        let local_addr = spawn("127.0.0.1:0").unwrap();

        let mut local_stream = TcpStream::connect(local_addr).unwrap();
        let mut ref_stream = TcpStream::connect(ref_addr).unwrap();

        let commands = [
            doc! { "ping": 1 },
            doc! { "buildInfo": 1 },
            doc! { "insert": "test_diff_coll", "documents": [doc! {"_id": 1, "a": 1}] },
            doc! { "find": "test_diff_coll", "filter": doc! {"a": 1} },
            doc! { "update": "test_diff_coll", "updates": [doc! {"q": {"a": 1}, "u": {"$set": {"a": 2}}}] },
            doc! { "find": "test_diff_coll", "filter": doc! {"a": 2} },
            doc! { "delete": "test_diff_coll", "deletes": [doc! {"q": {"a": 2}, "limit": 1}] },
        ];

        let mut compared = 0;
        for (i, cmd) in commands.iter().enumerate() {
            let mut local_cmd = cmd.clone();
            local_cmd.insert("$db", "test");
            let local_reply = send_op_msg(&mut local_stream, i as i32, &local_cmd);

            let mut ref_cmd = cmd.clone();
            ref_cmd.insert("$db", "test");
            let ref_reply = send_op_msg(&mut ref_stream, i as i32, &ref_cmd);

            // Normalize differences
            let mut local_normalized = local_reply.clone();
            let mut ref_normalized = ref_reply.clone();

            for doc in [&mut local_normalized, &mut ref_normalized] {
                doc.remove("$clusterTime");
                doc.remove("operationTime");
                doc.remove("topologyVersion");
                doc.remove("connectionId");
                doc.remove("localTime");
                doc.remove("electionId");

                if doc.contains_key("version") {
                    doc.insert("version", "NORMALIZED");
                    doc.insert("versionArray", "NORMALIZED");
                    doc.insert("gitVersion", "NORMALIZED");
                }
            }

            assert_eq!(local_normalized, ref_normalized, "Mismatch on command {}: {:?}", i, cmd);
            compared += 1;
        }

        println!("Compared {} replies", compared);
    }
}
