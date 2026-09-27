use kafka_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{FindCoordinatorRequest, GroupId, HeartbeatRequest};
use kafka_protocol::protocol::StrBytes;

use super::T;

#[test]
fn test_find_coordinator() {
    let t = T::new();

    // v3 (key only)
    let mut req_v3 = FindCoordinatorRequest::default();
    req_v3.key = StrBytes::from_static_str("my-group");
    let resp_v3 = t.engine.handle_find_coordinator(&req_v3, 3);
    assert_eq!(resp_v3.error_code, 0);
    assert_eq!(resp_v3.node_id.0, 1);

    // v4 (batched)
    let mut req_v4 = FindCoordinatorRequest::default();
    req_v4.coordinator_keys.push(StrBytes::from_static_str("my-group"));
    let resp_v4 = t.engine.handle_find_coordinator(&req_v4, 4);
    assert_eq!(resp_v4.coordinators.len(), 1);
    assert_eq!(resp_v4.coordinators[0].error_code, 0);
    assert_eq!(resp_v4.coordinators[0].node_id.0, 1);
}

#[test]
fn test_join_group_member_id_required_round_trip() {
    let t = T::new();

    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("round-trip-group"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    join_req.session_timeout_ms = 30000;
    join_req.rebalance_timeout_ms = 60000;

    let mut proto = JoinGroupRequestProtocol::default();
    proto.name = StrBytes::from_static_str("range");
    proto.metadata = bytes::Bytes::from("metadata-v1");
    join_req.protocols.push(proto);

    // 1. Initial join with empty member_id returns MEMBER_ID_REQUIRED (79)
    let resp1 = t.engine.handle_join_group(&join_req, 5);
    assert_eq!(resp1.error_code, 79);
    assert!(!resp1.member_id.is_empty());
    assert_eq!(resp1.generation_id, -1);

    // 2. Second join with assigned member_id succeeds
    join_req.member_id = resp1.member_id.clone();
    let resp2 = t.engine.handle_join_group(&join_req, 5);
    assert_eq!(resp2.error_code, 0);
    assert_eq!(resp2.generation_id, 1);
    assert_eq!(resp2.leader, resp1.member_id);
    assert_eq!(resp2.protocol_name.as_ref().map(|s| s.as_str()), Some("range"));
    assert_eq!(resp2.members.len(), 1);
    assert_eq!(resp2.members[0].member_id, resp1.member_id);
}

#[test]
fn test_join_group_unknown_member_and_inconsistent_protocol() {
    let t = T::new();

    // Joining with an unrecognized member_id -> UNKNOWN_MEMBER_ID (25)
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("test-grp"));
    join_req.member_id = StrBytes::from_static_str("completely-bogus-member-id");
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let resp = t.engine.handle_join_group(&join_req, 5);
    assert_eq!(resp.error_code, 25);

    // Join valid member with protocol_type "consumer"
    let mut valid_req = JoinGroupRequest::default();
    valid_req.group_id = GroupId(StrBytes::from_static_str("proto-grp"));
    valid_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&valid_req, 5);
    assert_eq!(step1.error_code, 79);
    valid_req.member_id = step1.member_id;
    let step2 = t.engine.handle_join_group(&valid_req, 5);
    assert_eq!(step2.error_code, 0);

    // Now try to join with protocol_type "connect" -> INCONSISTENT_GROUP_PROTOCOL (23)
    let mut bad_proto_req = JoinGroupRequest::default();
    bad_proto_req.group_id = GroupId(StrBytes::from_static_str("proto-grp"));
    bad_proto_req.protocol_type = StrBytes::from_static_str("connect");
    let step1_bad = t.engine.handle_join_group(&bad_proto_req, 5);
    assert_eq!(step1_bad.error_code, 79);
    bad_proto_req.member_id = step1_bad.member_id;
    let step2_bad = t.engine.handle_join_group(&bad_proto_req, 5);
    assert_eq!(step2_bad.error_code, 23);
}

#[test]
fn test_sync_group_and_heartbeat_happy_path() {
    let t = T::new();

    // 1. Join
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("sync-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    join_req.session_timeout_ms = 10000;
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;
    let gen_id = step2.generation_id;

    // 2. Leader SyncGroup distributes assignments
    let mut sync_req = SyncGroupRequest::default();
    sync_req.group_id = GroupId(StrBytes::from_static_str("sync-grp"));
    sync_req.member_id = member_id.clone();
    sync_req.generation_id = gen_id;
    let mut assign = SyncGroupRequestAssignment::default();
    assign.member_id = member_id.clone();
    assign.assignment = bytes::Bytes::from("partition-assignment-bytes");
    sync_req.assignments.push(assign);

    let sync_resp = t.engine.handle_sync_group(&sync_req, 3);
    assert_eq!(sync_resp.error_code, 0);
    assert_eq!(sync_resp.assignment.as_ref(), b"partition-assignment-bytes");

    // 3. Heartbeat succeeds in Stable state
    let mut hb_req = HeartbeatRequest::default();
    hb_req.group_id = GroupId(StrBytes::from_static_str("sync-grp"));
    hb_req.member_id = member_id;
    hb_req.generation_id = gen_id;

    let hb_resp = t.engine.handle_heartbeat(&hb_req, 4);
    assert_eq!(hb_resp.error_code, 0);
}

#[test]
fn test_sync_group_errors() {
    let t = T::new();

    // Join
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("sync-err-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;
    let gen_id = step2.generation_id;

    // Illegal generation -> ILLEGAL_GENERATION (22)
    let mut sync_wrong_gen = SyncGroupRequest::default();
    sync_wrong_gen.group_id = GroupId(StrBytes::from_static_str("sync-err-grp"));
    sync_wrong_gen.member_id = member_id.clone();
    sync_wrong_gen.generation_id = gen_id + 99;
    let resp_wrong_gen = t.engine.handle_sync_group(&sync_wrong_gen, 3);
    assert_eq!(resp_wrong_gen.error_code, 22);

    // Unknown member -> UNKNOWN_MEMBER_ID (25)
    let mut sync_unknown_m = SyncGroupRequest::default();
    sync_unknown_m.group_id = GroupId(StrBytes::from_static_str("sync-err-grp"));
    sync_unknown_m.member_id = StrBytes::from_static_str("unknown-member");
    sync_unknown_m.generation_id = gen_id;
    let resp_unknown_m = t.engine.handle_sync_group(&sync_unknown_m, 3);
    assert_eq!(resp_unknown_m.error_code, 25);
}

#[test]
fn test_heartbeat_clock_session_timeout() {
    let t = T::new();

    // Join with session timeout = 5000 ms
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("timeout-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    join_req.session_timeout_ms = 5000;
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;
    let gen_id = step2.generation_id;

    // Sync
    let mut sync_req = SyncGroupRequest::default();
    sync_req.group_id = GroupId(StrBytes::from_static_str("timeout-grp"));
    sync_req.member_id = member_id.clone();
    sync_req.generation_id = gen_id;
    t.engine.handle_sync_group(&sync_req, 3);

    // Heartbeat before timeout (at 3000ms) succeeds
    t.advance(3000);
    let mut hb_req = HeartbeatRequest::default();
    hb_req.group_id = GroupId(StrBytes::from_static_str("timeout-grp"));
    hb_req.member_id = member_id.clone();
    hb_req.generation_id = gen_id;
    let hb_resp1 = t.engine.handle_heartbeat(&hb_req, 4);
    assert_eq!(hb_resp1.error_code, 0);

    // Advance clock past session timeout (> 5000ms since last heartbeat)
    t.advance(6000);
    let hb_resp2 = t.engine.handle_heartbeat(&hb_req, 4);
    assert_eq!(hb_resp2.error_code, 25); // UNKNOWN_MEMBER_ID (expired)
}

#[test]
fn test_leave_group() {
    let t = T::new();

    // Join
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("leave-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;

    // Leave group
    let mut leave_req = LeaveGroupRequest::default();
    leave_req.group_id = GroupId(StrBytes::from_static_str("leave-grp"));
    leave_req.member_id = member_id.clone();
    let leave_resp = t.engine.handle_leave_group(&leave_req, 2);
    assert_eq!(leave_resp.error_code, 0);

    // Subsequent heartbeat fails with UNKNOWN_MEMBER_ID (25)
    let mut hb_req = HeartbeatRequest::default();
    hb_req.group_id = GroupId(StrBytes::from_static_str("leave-grp"));
    hb_req.member_id = member_id;
    hb_req.generation_id = step2.generation_id;
    let hb_resp = t.engine.handle_heartbeat(&hb_req, 4);
    assert_eq!(hb_resp.error_code, 25);
}

#[test]
fn test_rebalance_trigger_returns_rebalance_in_progress() {
    let t = T::new();

    // Consumer 1 joins and syncs
    let mut c1_join = JoinGroupRequest::default();
    c1_join.group_id = GroupId(StrBytes::from_static_str("dynamic-rebalance-grp"));
    c1_join.protocol_type = StrBytes::from_static_str("consumer");
    let c1_step1 = t.engine.handle_join_group(&c1_join, 5);
    c1_join.member_id = c1_step1.member_id.clone();
    let c1_step2 = t.engine.handle_join_group(&c1_join, 5);
    assert_eq!(c1_step2.generation_id, 1);

    let mut c1_sync = SyncGroupRequest::default();
    c1_sync.group_id = GroupId(StrBytes::from_static_str("dynamic-rebalance-grp"));
    c1_sync.member_id = c1_step2.member_id.clone();
    c1_sync.generation_id = 1;
    let mut assign = SyncGroupRequestAssignment::default();
    assign.member_id = c1_step2.member_id.clone();
    assign.assignment = bytes::Bytes::from("p0");
    c1_sync.assignments.push(assign);
    t.engine.handle_sync_group(&c1_sync, 3);

    // Group is now Stable. Now Consumer 2 joins with empty member_id
    let mut c2_join = JoinGroupRequest::default();
    c2_join.group_id = GroupId(StrBytes::from_static_str("dynamic-rebalance-grp"));
    c2_join.protocol_type = StrBytes::from_static_str("consumer");
    let c2_step1 = t.engine.handle_join_group(&c2_join, 5);
    assert_eq!(c2_step1.error_code, 79);

    // Consumer 2 sends JoinGroup with assigned ID -> triggers rebalance!
    c2_join.member_id = c2_step1.member_id.clone();
    let _ = t.engine.handle_join_group(&c2_join, 5);

    // Now Consumer 1 sends Heartbeat with old generation -> REBALANCE_IN_PROGRESS (27) or ILLEGAL_GENERATION (22)
    let mut hb_req = HeartbeatRequest::default();
    hb_req.group_id = GroupId(StrBytes::from_static_str("dynamic-rebalance-grp"));
    hb_req.member_id = c1_step2.member_id.clone();
    hb_req.generation_id = 1; // old generation!
    let hb_resp = t.engine.handle_heartbeat(&hb_req, 4);
    // Generation was incremented when rebalance was triggered
    assert!(hb_resp.error_code == 22 || hb_resp.error_code == 27);
}
