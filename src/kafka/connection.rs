//! TCP Connection handler for Kafka.

use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, CreateTopicsRequest, FetchRequest, InitProducerIdRequest,
    ListOffsetsRequest, MetadataRequest, ProduceRequest, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use std::io::{BufReader, BufWriter};
use std::net::TcpStream;

use super::codec::{read_frame, write_frame};
use super::engine::Engine;

pub fn handle_connection(stream: TcpStream, engine: Engine) {
    // Best-effort: this connection's own write path already batches a
    // response's length header and payload into one `BufWriter` flush,
    // so it doesn't show the specific ~40ms Nagle/delayed-ACK stall
    // `src/mysql/server.rs` was measured hitting, but there's no reason
    // to leave Nagle's algorithm enabled for a local-dev tool either.
    let _ = stream.set_nodelay(true);
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    loop {
        let frame = match read_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => break,
        };

        let mut buf = frame;
        // Determine header version dynamically based on request header version
        let header_version = peek_header_version(&buf).unwrap_or(2); // fallback

        let mut buf_decode = buf.clone();
        let header = match RequestHeader::decode(&mut buf_decode, header_version) {
            Ok(h) => {
                buf = buf_decode;
                h
            }
            Err(_) => {
                // Fallback attempt with v1 if v2 decoding failed
                let mut buf_fallback = buf;
                match RequestHeader::decode(&mut buf_fallback, 1) {
                    Ok(h) => {
                        buf = buf_fallback;
                        h
                    }
                    Err(_) => break,
                }
            }
        };

        let api_key = match ApiKey::try_from(header.request_api_key) {
            Ok(k) => k,
            Err(_) => break,
        };

        let response_buf = match api_key {
            ApiKey::ApiVersions => {
                use kafka_protocol::messages::api_versions_response::ApiVersion;
                let version = header.request_api_version;
                // A version this broker doesn't speak: like Kafka, answer
                // UNSUPPORTED_VERSION in the v0 format every client can
                // read, listing the ApiVersions versions it does speak so
                // the client can retry with one of them.
                if !(0..=3).contains(&version) {
                    let mut resp = kafka_protocol::messages::ApiVersionsResponse::default();
                    resp.error_code = 35; // UNSUPPORTED_VERSION
                    let mut v = ApiVersion::default();
                    v.api_key = ApiKey::ApiVersions as i16;
                    v.min_version = 0;
                    v.max_version = 3;
                    resp.api_keys.push(v);
                    encode_response(&header, &resp, 0, 0)
                } else {
                    let req = match ApiVersionsRequest::decode(&mut buf, version) {
                        Ok(r) => r,
                        Err(_) => break,
                    };
                    // v3+ names the client software; Kafka rejects a name
                    // or version that isn't [a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?.
                    let valid = |s: &str| {
                        let b = s.as_bytes();
                        !b.is_empty()
                            && b[0].is_ascii_alphanumeric()
                            && b[b.len() - 1].is_ascii_alphanumeric()
                            && b.iter()
                                .all(|c| c.is_ascii_alphanumeric() || *c == b'-' || *c == b'.')
                    };
                    let resp = if version >= 3
                        && !(valid(req.client_software_name.as_str())
                            && valid(req.client_software_version.as_str()))
                    {
                        let mut resp = kafka_protocol::messages::ApiVersionsResponse::default();
                        resp.error_code = 42; // INVALID_REQUEST
                        resp
                    } else {
                        engine.handle_api_versions(version)
                    };
                    encode_response(&header, &resp, resp_header_version(api_key, version), version)
                }
            }
            ApiKey::Metadata => {
                let req = match MetadataRequest::decode(&mut buf, header.request_api_version) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_metadata(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::CreateTopics => {
                let req = match CreateTopicsRequest::decode(&mut buf, header.request_api_version) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_create_topics(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::InitProducerId => {
                let req = match InitProducerIdRequest::decode(&mut buf, header.request_api_version)
                {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_init_producer_id(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::Produce => {
                let req = match ProduceRequest::decode(&mut buf, header.request_api_version) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_produce(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::Fetch => {
                let req = match FetchRequest::decode(&mut buf, header.request_api_version) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_fetch(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::ListOffsets => {
                let req = match ListOffsetsRequest::decode(&mut buf, header.request_api_version) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_list_offsets(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DeleteTopics => {
                let req = match kafka_protocol::messages::DeleteTopicsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_delete_topics(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::CreatePartitions => {
                let req = match kafka_protocol::messages::CreatePartitionsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_create_partitions(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::FindCoordinator => {
                let req = match kafka_protocol::messages::FindCoordinatorRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_find_coordinator(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::JoinGroup => {
                let req = match kafka_protocol::messages::JoinGroupRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_join_group(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::SyncGroup => {
                let req = match kafka_protocol::messages::SyncGroupRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_sync_group(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::Heartbeat => {
                let req = match kafka_protocol::messages::HeartbeatRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_heartbeat(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::LeaveGroup => {
                let req = match kafka_protocol::messages::LeaveGroupRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_leave_group(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::OffsetCommit => {
                let req = match kafka_protocol::messages::OffsetCommitRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_offset_commit(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::OffsetFetch => {
                let req = match kafka_protocol::messages::OffsetFetchRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_offset_fetch(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeGroups => {
                let req = match kafka_protocol::messages::DescribeGroupsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_groups(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::ListGroups => {
                let req = match kafka_protocol::messages::ListGroupsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_list_groups(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DeleteGroups => {
                let req = match kafka_protocol::messages::DeleteGroupsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_delete_groups(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeConfigs => {
                let req = match kafka_protocol::messages::DescribeConfigsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_configs(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeCluster => {
                let req = match kafka_protocol::messages::DescribeClusterRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_cluster(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::OffsetForLeaderEpoch => {
                let req = match kafka_protocol::messages::OffsetForLeaderEpochRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_offset_for_leader_epoch(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::AddPartitionsToTxn => {
                let req = match kafka_protocol::messages::AddPartitionsToTxnRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_add_partitions_to_txn(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::AddOffsetsToTxn => {
                let req = match kafka_protocol::messages::AddOffsetsToTxnRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_add_offsets_to_txn(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::EndTxn => {
                let req = match kafka_protocol::messages::EndTxnRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_end_txn(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::TxnOffsetCommit => {
                let req = match kafka_protocol::messages::TxnOffsetCommitRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_txn_offset_commit(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::AlterConfigs => {
                let req = match kafka_protocol::messages::AlterConfigsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_alter_configs(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::IncrementalAlterConfigs => {
                let req = match kafka_protocol::messages::IncrementalAlterConfigsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp =
                    engine.handle_incremental_alter_configs(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DeleteRecords => {
                let req = match kafka_protocol::messages::DeleteRecordsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_delete_records(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::OffsetDelete => {
                let req = match kafka_protocol::messages::OffsetDeleteRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_offset_delete(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeTransactions => {
                let req = match kafka_protocol::messages::DescribeTransactionsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_transactions(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::ListTransactions => {
                let req = match kafka_protocol::messages::ListTransactionsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_list_transactions(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeProducers => {
                let req = match kafka_protocol::messages::DescribeProducersRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_producers(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::DescribeLogDirs => {
                let req = match kafka_protocol::messages::DescribeLogDirsRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_describe_log_dirs(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::SaslHandshake => {
                let req = match kafka_protocol::messages::SaslHandshakeRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_sasl_handshake(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            ApiKey::SaslAuthenticate => {
                let req = match kafka_protocol::messages::SaslAuthenticateRequest::decode(
                    &mut buf,
                    header.request_api_version,
                ) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let resp = engine.handle_sasl_authenticate(&req, header.request_api_version);
                encode_response(
                    &header,
                    &resp,
                    resp_header_version(api_key, header.request_api_version),
                    header.request_api_version,
                )
            }
            _ => break,
        };

        if let Ok(resp_bytes) = response_buf {
            if write_frame(&mut writer, &resp_bytes).is_err() {
                break;
            }
        } else {
            break;
        }
    }
}

fn peek_header_version(buf: &[u8]) -> Option<i16> {
    if buf.len() < 4 {
        return None;
    }
    let api_key_num = i16::from_be_bytes([buf[0], buf[1]]);
    let api_version = i16::from_be_bytes([buf[2], buf[3]]);
    let api_key = ApiKey::try_from(api_key_num).ok()?;
    Some(api_key.request_header_version(api_version))
}

fn resp_header_version(api_key: ApiKey, api_version: i16) -> i16 {
    api_key.response_header_version(api_version)
}

fn encode_response<R: Encodable>(
    req_header: &RequestHeader,
    response: &R,
    header_version: i16,
    api_version: i16,
) -> Result<Vec<u8>, ()> {
    let mut header = ResponseHeader::default();
    header.correlation_id = req_header.correlation_id;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version).map_err(|_| ())?;
    response.encode(&mut buf, api_version).map_err(|_| ())?;

    Ok(buf.to_vec())
}
