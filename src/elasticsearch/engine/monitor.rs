//! Routes the monitoring and cluster-administration endpoints (stats,
//! `_cat` tables, `_nodes`, `_cluster`, tasks, features, `_internal`) to
//! their handlers; everything else is left to the main router.

use super::cluster;
use super::*;

impl Engine {
    pub(super) fn monitoring_route(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
        path: &str,
    ) -> Option<(u16, Value)> {
        let get = method == "GET";
        let r = match segments {
            ["_cluster", ..] => return self.cluster_route(method, segments, q, body, path),
            ["_cat", ..] if get => return self.cat_tables(segments, q),
            ["_nodes", ..] => self.nodes_route(method, segments, q, body, path),
            ["_stats"] if get => self.indices_stats("_all", None, q, path),
            ["_stats", metric] if get => self.indices_stats("_all", Some(metric), q, path),
            [idx, "_stats"] if get => self.indices_stats(idx, None, q, path),
            [idx, "_stats", metric] if get => self.indices_stats(idx, Some(metric), q, path),
            ["_segments"] if get => self.segments_api("_all", q),
            [idx, "_segments"] if get => self.segments_api(idx, q),
            ["_recovery"] if get => self.recovery_api("_all", q),
            [idx, "_recovery"] if get => self.recovery_api(idx, q),
            ["_shard_stores"] if get => self.shard_stores_api("_all", q),
            [idx, "_shard_stores"] if get => self.shard_stores_api(idx, q),
            [idx, "_disk_usage"] if method == "POST" => self.disk_usage_api(idx, q),
            [idx, "_field_usage_stats"] if get => self.field_usage_api(idx, q),
            ["_info"] if get => self.cluster_info(None, path),
            ["_info", target] if get => self.cluster_info(Some(target), path),
            ["_health_report"] if get => self.health_report(None, q),
            ["_health_report", feature] if get => self.health_report(Some(feature), q),
            ["_remote", "info"] if get => (200, json!({})),
            ["_tasks", ..] => self.tasks_api(method, segments, q),
            ["_features"] if get => (200, cluster::features()),
            ["_features", "_reset"] if method == "POST" => (200, cluster::reset_features()),
            ["_capabilities"] if get => cluster::capabilities(q),
            ["_internal", ..] => return self.internal_api(method, segments, q, body),
            _ => return None,
        };
        Some(r)
    }
}
