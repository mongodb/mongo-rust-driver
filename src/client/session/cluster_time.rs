use bson::{Document, TimeStamp};
use serde::{Deserialize, Serialize};

/// Struct modeling a cluster time reported by the server.
///
/// See [the MongoDB documentation](https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/)
/// for more information.
#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterTime {
    cluster_time: TimeStamp,
    signature: Document,
}

impl std::cmp::PartialEq for ClusterTime {
    fn eq(&self, other: &ClusterTime) -> bool {
        self.cluster_time == other.cluster_time
    }
}

impl std::cmp::Eq for ClusterTime {}

impl std::cmp::Ord for ClusterTime {
    fn cmp(&self, other: &ClusterTime) -> std::cmp::Ordering {
        let lhs = (self.cluster_time.t, self.cluster_time.i);
        let rhs = (other.cluster_time.t, other.cluster_time.i);
        lhs.cmp(&rhs)
    }
}

impl std::cmp::PartialOrd for ClusterTime {
    fn partial_cmp(&self, other: &ClusterTime) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
